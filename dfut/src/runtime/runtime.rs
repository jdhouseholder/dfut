use std::collections::{hash_map::Entry, HashMap};
use std::future::Future;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use metrics::{counter, histogram};
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{
    broadcast::{channel, Receiver, Sender},
    watch::{channel as watch_channel, Receiver as WatchRx, Sender as WatchTx},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::{
    transport::{Channel, Endpoint, Server},
    Status,
};

use crate::{
    consts::DFUT_RETRIES,
    d_fut::DFut,
    d_scheduler::{DScheduler, Where},
    d_store::{DStore, DStoreId, ValueTrait},
    gaps::AddressToGaps,
    peer_work::PeerWorkerClient,
    retry::retry,
    rpc_context::RpcContext,
    seq::Seq,
    services::{
        d_store_service::d_store_service_server::DStoreServiceServer,
        global_scheduler_service::{
            global_scheduler_service_client::GlobalSchedulerServiceClient,
            heart_beat_response::HeartBeatResponseType, BadRequest, HeartBeat, HeartBeatRequest,
            HeartBeatResponse, NotLeader, RequestId, RuntimeInfo, TaskFailure,
        },
        worker_service::{
            worker_service_server::{WorkerService, WorkerServiceServer},
            DoWorkResponse, ParentInfo,
        },
    },
    sleep::sleep_with_jitter,
    stopwatch::Stopwatch,
    work::{IntoWork, Work},
    DResult, Error,
};

#[derive(Debug, Clone, Default)]
pub struct WorkerServerConfig {
    pub local_server_address: String,
    pub global_scheduler_address: String,
    pub fn_names: Vec<String>,
}

pub trait WorkerServiceExt: WorkerService {
    fn new(root_runtime: RootRuntime) -> Self;
}

#[derive(Debug)]
struct SharedRuntimeState {
    task_tracker: TaskTracker,

    local_server_address: String,
    dfut_retries: usize,

    d_scheduler: DScheduler,
    peer_worker_client: PeerWorkerClient,
    d_store: Arc<DStore>,

    address_to_gaps: AddressToGaps,
    address_to_runtime_info: Mutex<HashMap<String, RuntimeInfo>>,
    lifetime_id: Arc<AtomicU64>,
    next_task_id: AtomicU64,
    next_request_id: Arc<Mutex<Seq>>,

    task_failures: Mutex<Vec<TaskFailure>>,
}

#[derive(Debug, Clone)]
pub struct RootRuntime {
    shared_runtime_state: Arc<SharedRuntimeState>,
}

pub struct RootRuntimeHandle {
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
    root_runtime: RootRuntime,
}

impl RootRuntimeHandle {
    pub async fn wait(&self) {
        self.task_tracker.wait().await;
    }

    pub fn emit_debug_output(&self) {
        tracing::error!(
            "{:#?}",
            self.root_runtime
                .shared_runtime_state
                .address_to_runtime_info
                .lock()
                .unwrap()
        );
        self.root_runtime
            .shared_runtime_state
            .d_store
            .emit_debug_output();
    }

    pub async fn shutdown(&self) {
        self.cancellation_token.cancel();
        self.task_tracker.close();
        self.task_tracker.wait().await;
    }
}

impl RootRuntime {
    fn filter_fn_names(available_fn_names: Vec<String>, fn_names: Vec<String>) -> Vec<String> {
        if fn_names.len() > 0 {
            for fn_name in &fn_names {
                if !available_fn_names.contains(fn_name) {
                    panic!("Worker doesn't have d-fn: \"{fn_name}\".");
                }
            }
            fn_names
        } else {
            available_fn_names
        }
    }

    pub async fn serve<T>(
        cfg: WorkerServerConfig,
        available_fn_names: Vec<String>,
    ) -> RootRuntimeHandle
    where
        T: WorkerServiceExt,
    {
        let WorkerServerConfig {
            local_server_address,
            global_scheduler_address,
            fn_names,
        } = cfg;

        let fn_names = Self::filter_fn_names(available_fn_names, fn_names);

        let lifetime_id = Arc::new(AtomicU64::new(0));

        let address_to_gaps = AddressToGaps::default();

        let d_store = Arc::new(
            DStore::new(&local_server_address, &lifetime_id, address_to_gaps.clone()).await,
        );

        let cancellation_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();

        let root_runtime = Self {
            shared_runtime_state: Arc::new(SharedRuntimeState {
                task_tracker: task_tracker.clone(),
                local_server_address: local_server_address.to_string(),
                // TODO: pass in through config
                dfut_retries: DFUT_RETRIES,

                d_scheduler: DScheduler::new(fn_names),
                peer_worker_client: PeerWorkerClient::new(),
                d_store: Arc::clone(&d_store),

                address_to_gaps,
                address_to_runtime_info: Mutex::default(),
                lifetime_id,
                next_task_id: AtomicU64::new(0),
                next_request_id: Arc::default(),

                task_failures: Mutex::default(),
            }),
        };

        let (heart_beat_timeout_tx, mut heart_beat_timeout_rx) = watch_channel(0);
        heart_beat_timeout_rx.mark_unchanged();

        let endpoint: Endpoint = global_scheduler_address.parse().unwrap();
        let channel = endpoint.connect_lazy();
        let mut client = GlobalSchedulerServiceClient::new(channel);
        let mut request_id = 0u64;
        let mut heart_beat_timeout = 100;

        while !root_runtime
            .heart_beat_once(
                &mut client,
                &mut request_id,
                &mut heart_beat_timeout,
                &heart_beat_timeout_tx,
            )
            .await
        {
            sleep_with_jitter(heart_beat_timeout / 3).await;
        }

        tracing::info!("valid heartbeat");

        let heart_beat_fut = task_tracker.spawn({
            let root_runtime = root_runtime.clone();
            async move {
                root_runtime
                    .heart_beat_forever(
                        &mut client,
                        &mut request_id,
                        &mut heart_beat_timeout,
                        &heart_beat_timeout_tx,
                    )
                    .await;
            }
        });

        let expire_fut = task_tracker.spawn({
            let root_runtime = root_runtime.clone();
            async move {
                root_runtime.expire_forever(heart_beat_timeout_rx).await;
            }
        });

        let serve_fut = Server::builder()
            .add_service(
                DStoreServiceServer::new(Arc::clone(&d_store))
                    .max_encoding_message_size(usize::MAX)
                    .max_decoding_message_size(usize::MAX),
            )
            .add_service(
                WorkerServiceServer::new(T::new(root_runtime.clone()))
                    .max_encoding_message_size(usize::MAX)
                    .max_decoding_message_size(usize::MAX),
            )
            .serve(
                local_server_address
                    .strip_prefix("http://")
                    .unwrap()
                    .to_string()
                    .parse()
                    .unwrap(),
            );

        task_tracker.spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                tokio::select! {
                    r = serve_fut => r.unwrap(),
                    _ = heart_beat_fut => {},
                    _ = expire_fut => {},
                    _ = cancellation_token.cancelled() => {},
                }
            }
        });

        RootRuntimeHandle {
            cancellation_token,
            task_tracker,
            root_runtime,
        }
    }

    fn new_child(&self, parent_info: Vec<ParentInfo>) -> Runtime {
        Runtime {
            shared_runtime_state: Arc::clone(&self.shared_runtime_state),

            lifetime_id: self.shared_runtime_state.lifetime_id.load(Ordering::SeqCst),
            parent_info: Arc::new(parent_info),
            task_id: self
                .shared_runtime_state
                .next_task_id
                .fetch_add(1, Ordering::SeqCst),
            inner: Arc::default(),

            requests: Arc::default(),
        }
    }

    fn validate_lifetime_id(
        &self,
        parent_address: &str,
        parent_lifetime_id: u64,
    ) -> Result<(), Status> {
        let address_to_runtime_info = self
            .shared_runtime_state
            .address_to_runtime_info
            .lock()
            .unwrap();
        if let Some(runtime_info) = address_to_runtime_info.get(parent_address) {
            if parent_lifetime_id < runtime_info.lifetime_id {
                return Err(Status::invalid_argument("lifetime id too old."));
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, parent_info, f))]
    pub fn do_local_work<F, T, FutFn>(
        &self,
        parent_info: Vec<ParentInfo>,
        fn_name: &str,
        f: FutFn,
    ) -> Result<DoWorkResponse, Status>
    where
        F: Future<Output = DResult<T>> + Send + 'static,
        T: Serialize + std::fmt::Debug + Send + Sync + 'static,
        FutFn: FnOnce(Runtime) -> F,
    {
        let parent_task = parent_info.last().unwrap();

        let parent_address = &parent_task.address;
        let parent_lifetime_id = parent_task.lifetime_id;
        let parent_task_id = parent_task.task_id;
        let request_id = parent_task.request_id.unwrap();

        // TODO: remove this counter.
        counter!(
           "do_local_work",
           "from" => parent_address.to_string(),
           "to" => self.shared_runtime_state.local_server_address.to_string(),
           "depth" => parent_info.len().to_string(),
        )
        .increment(1);

        self.validate_lifetime_id(parent_address, parent_lifetime_id)?;

        if self
            .shared_runtime_state
            .address_to_gaps
            .have_seen_request_id(parent_address, parent_lifetime_id, request_id)
        {
            return self
                .shared_runtime_state
                .d_store
                .parent_info_to_id(
                    parent_address,
                    parent_lifetime_id,
                    parent_task_id,
                    request_id,
                )
                .map(|d_store_id| d_store_id.into())
                .ok_or(Status::not_found("Result has already been deallocated"));
        }

        let runtime = self.new_child(parent_info);
        let fut = f(runtime.clone());
        let d_store_id = runtime.do_local_work(fn_name, fut);
        Ok(d_store_id.into())
    }

    async fn expire_forever(&self, mut heart_beat_timeout_rx: WatchRx<u64>) {
        heart_beat_timeout_rx.changed().await.unwrap();

        let mut last_task_failures = Vec::new();
        loop {
            let heart_beat_timeout = *heart_beat_timeout_rx.borrow();
            sleep_with_jitter(heart_beat_timeout / 3).await;

            {
                let mut task_failures = self.shared_runtime_state.task_failures.lock().unwrap();

                *task_failures = task_failures
                    .clone()
                    .into_iter()
                    .filter(|tf| !last_task_failures.contains(tf))
                    .collect();

                last_task_failures = task_failures.clone();
            }
        }
    }

    async fn heart_beat_once(
        &self,
        client: &mut GlobalSchedulerServiceClient<Channel>,
        request_id: &mut u64,
        next_heart_beat_timeout: &mut u64,
        heart_beat_timeout_tx: &WatchTx<u64>,
    ) -> bool {
        let local_lifetime_id = self.shared_runtime_state.lifetime_id.load(Ordering::SeqCst);

        let task_failures = {
            self.shared_runtime_state
                .task_failures
                .lock()
                .unwrap()
                .clone()
        };

        let stats = self.shared_runtime_state.d_scheduler.output_stats();

        tracing::trace!(
            "{}: Attempting to HeartBeat",
            self.shared_runtime_state.local_server_address
        );

        let HeartBeatResponse {
            heart_beat_response_type,
        } = retry(client, |mut client| {
            let request_id = *request_id;
            let address = self.shared_runtime_state.local_server_address.to_string();
            let stats = stats.clone();
            let task_failures = task_failures.clone();
            async move {
                client
                    .heart_beat(HeartBeatRequest {
                        request_id,
                        address,
                        current_runtime_info: Some(RuntimeInfo {
                            lifetime_id: local_lifetime_id,
                            stats: Some(stats),
                            task_failures,
                        }),
                    })
                    .await
            }
        })
        .await
        .unwrap();

        let HeartBeat {
            lifetime_id,
            heart_beat_timeout,
            next_expected_request_id,
            address_to_runtime_info,
            ..
        } = match heart_beat_response_type {
            Some(HeartBeatResponseType::HeartBeat(heart_beat)) => heart_beat,
            Some(HeartBeatResponseType::BadRequest(BadRequest {
                lifetime_id,
                next_expected_request_id,
            })) => {
                if lifetime_id != self.shared_runtime_state.lifetime_id.load(Ordering::SeqCst) {
                    tracing::info!(
                        "{}: lifetime id change: {} to {}",
                        self.shared_runtime_state.local_server_address,
                        local_lifetime_id,
                        lifetime_id
                    );
                    self.shared_runtime_state
                        .lifetime_id
                        .store(lifetime_id, Ordering::SeqCst);
                    self.shared_runtime_state.d_store.clear();
                }
                *request_id = next_expected_request_id;
                return false;
            }
            Some(HeartBeatResponseType::NotLeader(NotLeader { leader_address })) => {
                match leader_address {
                    Some(leader_address) => {
                        let endpoint: Endpoint = leader_address.parse().unwrap();
                        let channel = endpoint.connect_lazy();
                        *client = GlobalSchedulerServiceClient::new(channel);
                        return false;
                    }
                    None => {
                        return false;
                    }
                }
            }
            None => panic!(),
        };

        tracing::trace!(
            "{}: Good HeartBeat",
            self.shared_runtime_state.local_server_address
        );

        *next_heart_beat_timeout = heart_beat_timeout;
        heart_beat_timeout_tx.send(heart_beat_timeout).unwrap();
        *request_id = next_expected_request_id;

        self.shared_runtime_state.d_scheduler.update(
            &self.shared_runtime_state.local_server_address,
            &address_to_runtime_info,
        );

        {
            let mut local_address_to_runtime_info = self
                .shared_runtime_state
                .address_to_runtime_info
                .lock()
                .unwrap();

            // worker_failure
            for (address, runtime_info) in &address_to_runtime_info {
                if let Some(previous_runtime_info) = local_address_to_runtime_info.get(address) {
                    if previous_runtime_info.lifetime_id < runtime_info.lifetime_id {
                        self.shared_runtime_state
                            .d_store
                            .worker_failure(address, runtime_info.lifetime_id);
                        self.shared_runtime_state
                            .address_to_gaps
                            .try_reset(address, runtime_info.lifetime_id);
                    }
                }
            }

            // task_failure
            for (address, runtime_info) in &address_to_runtime_info {
                for task_failure in &runtime_info.task_failures {
                    self.shared_runtime_state.d_store.task_failure(
                        address,
                        task_failure.lifetime_id,
                        task_failure.task_id,
                    );

                    for request in &task_failure.requests {
                        if request.address == self.shared_runtime_state.local_server_address {
                            self.shared_runtime_state
                                .address_to_gaps
                                .have_seen_request_id(
                                    address,
                                    runtime_info.lifetime_id,
                                    request.request_id,
                                );
                        }
                    }
                }
            }

            *local_address_to_runtime_info = address_to_runtime_info;
        }

        if lifetime_id != local_lifetime_id {
            tracing::info!(
                "{}: lifetime id change: {} to {}",
                self.shared_runtime_state.local_server_address,
                local_lifetime_id,
                lifetime_id
            );
            // If we hit this case it means we didn't renew our lifetime lease, so either the
            // global scheduler died or there has been a network partition. So we can actually
            // continue to compute the current values and put them into a temporary store.
            // This way we can serve old dfuts if they haven't been d_awaited, but also care to
            // not block up the d_store with d_futs that won't be resolved.
            //
            // New tasks will now be associated with this lifetime.
            //
            // To simplify this logic we will simply clear the d_store and fail all d_futs that
            // this worker will resolve if they have a lifetime_id that is older than the new
            // one.
            self.shared_runtime_state
                .lifetime_id
                .store(lifetime_id, Ordering::SeqCst);
            // We can simply clear the d_store after incrementing the lifetime_id since we
            // don't reset the object_id.
            self.shared_runtime_state.d_store.clear();
        }
        true
    }

    async fn heart_beat_forever(
        &self,
        client: &mut GlobalSchedulerServiceClient<Channel>,
        request_id: &mut u64,
        next_heart_beat_timeout: &mut u64,
        heart_beat_timeout_tx: &WatchTx<u64>,
    ) {
        loop {
            self.heart_beat_once(
                client,
                request_id,
                next_heart_beat_timeout,
                &heart_beat_timeout_tx,
            )
            .await;
            sleep_with_jitter(*next_heart_beat_timeout / 3).await;
        }
    }
}

#[allow(unused)]
#[derive(Debug)]
enum Call {
    // We don't need to be able to recover local calls they are owned by the remote owner.
    Local { fn_name: String },
    // If the remote task fails then we will recover.
    Remote { work: Work },
    Retrying { tx: Sender<Arc<dyn ValueTrait>> },
    RetriedOk { v: Arc<dyn ValueTrait> },
    RetriedErr,
}

#[derive(Debug, Default)]
struct InnerRuntime {
    calls: HashMap<DStoreId, Call>,
    stopwatch: Stopwatch,
}

#[derive(Debug, Clone)]
pub struct Runtime {
    shared_runtime_state: Arc<SharedRuntimeState>,

    lifetime_id: u64,

    parent_info: Arc<Vec<ParentInfo>>,

    task_id: u64,
    inner: Arc<Mutex<InnerRuntime>>,
    requests: Arc<Mutex<Vec<RequestId>>>,
}

impl Runtime {
    fn is_valid_id(&self, d_store_id: &DStoreId) -> Option<bool> {
        self.shared_runtime_state
            .address_to_runtime_info
            .lock()
            .unwrap()
            .get(&d_store_id.address)
            .map(|v| d_store_id.lifetime_id >= v.lifetime_id)
    }

    fn start_stopwatch(&self) -> Instant {
        self.inner.lock().unwrap().stopwatch.start()
    }

    fn stop_stopwatch(&self) {
        self.inner.lock().unwrap().stopwatch.stop();
    }

    fn finish_local_work(&self, fn_name: &str, ret_size: usize) -> Duration {
        let elapsed = { self.inner.lock().unwrap().stopwatch.elapsed() };

        self.shared_runtime_state
            .d_scheduler
            .finish_local_work(fn_name, elapsed, ret_size);

        elapsed
    }

    fn check_runtime_state(&self) -> DResult<()> {
        if self.lifetime_id != self.shared_runtime_state.lifetime_id.load(Ordering::SeqCst) {
            return Err(Error::System);
        }

        let address_to_runtime_info = self
            .shared_runtime_state
            .address_to_runtime_info
            .lock()
            .unwrap();
        for parent_info in self.parent_info.iter() {
            if let Some(parent_runtime_info) = address_to_runtime_info.get(&parent_info.address) {
                if parent_runtime_info.lifetime_id < parent_info.lifetime_id {
                    return Err(Error::System);
                }

                if parent_runtime_info.lifetime_id == parent_info.lifetime_id {
                    for task_failure in &parent_runtime_info.task_failures {
                        if task_failure.lifetime_id == parent_info.lifetime_id
                            && task_failure.task_id == parent_info.task_id
                        {
                            counter!("check_runtime_state::fail").increment(1);
                            return Err(Error::System);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn rpc_context(&self) -> RpcContext {
        RpcContext {
            local_address: self.shared_runtime_state.local_server_address.clone(),
            lifetime_id: self.lifetime_id,
            next_request_id: Arc::clone(&self.shared_runtime_state.next_request_id),
            requests: Arc::clone(&self.requests),
        }
    }

    fn next_parent_info(&self, address: &str) -> Vec<ParentInfo> {
        let request_id = self
            .shared_runtime_state
            .next_request_id
            .lock()
            .unwrap()
            .next(address);

        self.requests.lock().unwrap().push(RequestId {
            address: address.to_string(),
            request_id,
        });

        let mut parent_info = (*self.parent_info).clone();

        parent_info.push(ParentInfo {
            address: self.shared_runtime_state.local_server_address.clone(),
            lifetime_id: self.lifetime_id,
            task_id: self.task_id,
            request_id: Some(request_id),
        });

        parent_info
    }

    async fn try_retry_dfut<T>(&self, d_store_id: &DStoreId) -> DResult<T>
    where
        T: Serialize + DeserializeOwned + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        enum RetryState {
            Sender(Work, Sender<Arc<dyn ValueTrait>>),
            Receiver(Receiver<Arc<dyn ValueTrait>>),
        }

        let r = {
            let mut inner = self.inner.lock().unwrap();
            let mut e = inner.calls.entry(d_store_id.clone());
            match e {
                Entry::Occupied(ref mut o) => match o.get() {
                    Call::Remote { .. } => {
                        let (tx, _rx) = channel(1);
                        let v = o.insert(Call::Retrying { tx: tx.clone() });
                        let Call::Remote { work } = v else {
                            unreachable!()
                        };
                        RetryState::Sender(work, tx)
                    }
                    Call::Retrying { tx } => RetryState::Receiver(tx.subscribe()),
                    Call::RetriedOk { v } => {
                        let t: Arc<T> = Arc::clone(&v).as_any().downcast().unwrap();
                        return Ok((*t).clone());
                    }
                    Call::Local { .. } | Call::RetriedErr => return Err(Error::System),
                },
                Entry::Vacant(_) => unreachable!(),
            }
        };
        match r {
            RetryState::Sender(work, tx) => {
                for _ in 0..self.shared_runtime_state.dfut_retries {
                    if let Err(_) = self.check_runtime_state() {
                        self.inner
                            .lock()
                            .unwrap()
                            .calls
                            .insert(d_store_id.clone(), Call::RetriedErr);

                        return Err(Error::System);
                    }

                    let address = self
                        .shared_runtime_state
                        .d_scheduler
                        .schedule_fn(&work.fn_name)
                        .ok_or(Error::System)?;

                    let parent_info = self.next_parent_info(&address);

                    let d_store_id = self
                        .shared_runtime_state
                        .peer_worker_client
                        .do_work(&parent_info, &address, work.clone())
                        .await?;

                    let t: DResult<T> = self
                        .shared_runtime_state
                        .d_store
                        .get_or_watch(&self.rpc_context(), d_store_id.clone())
                        .await;

                    if let Ok(t) = t {
                        let v: Arc<dyn ValueTrait> = Arc::new(t.clone());
                        self.inner
                            .lock()
                            .unwrap()
                            .calls
                            .insert(d_store_id.clone(), Call::RetriedOk { v: Arc::clone(&v) });
                        let _ = tx.send(v);

                        return Ok(t);
                    }
                }

                self.inner
                    .lock()
                    .unwrap()
                    .calls
                    .insert(d_store_id.clone(), Call::RetriedErr);

                return Err(Error::System);
            }
            RetryState::Receiver(mut rx) => {
                let v = match rx.recv().await {
                    Ok(v) => v,
                    Err(_) => return Err(Error::System),
                };
                let t: Arc<T> = v.as_any().downcast().unwrap();
                return Ok((*t).clone());
            }
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn wait<T>(&self, d_fut: DFut<T>) -> DResult<T>
    where
        T: Serialize + DeserializeOwned + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        self.stop_stopwatch();

        if let Some(valid) = self.is_valid_id(&d_fut.d_store_id) {
            if !valid {
                return Err(Error::System);
            }
        }

        let t = self
            .shared_runtime_state
            .d_store
            .get_or_watch(&self.rpc_context(), d_fut.d_store_id.clone())
            .await;

        if let Err(_) = &t {
            return self.try_retry_dfut(&d_fut.d_store_id).await;
        }

        self.start_stopwatch();

        t
    }

    #[tracing::instrument(skip_all)]
    pub async fn cancel<T>(&self, d_fut: DFut<T>) -> DResult<()> {
        self.stop_stopwatch();

        if let Some(valid) = self.is_valid_id(&d_fut.d_store_id) {
            if !valid {
                self.start_stopwatch();
                // Ignore failure.
                return Ok(());
            }
        }

        self.shared_runtime_state
            .d_store
            .decrement_or_remove(&self.rpc_context(), d_fut.d_store_id, 1)
            .await?;

        self.start_stopwatch();

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn share<T>(&self, d_fut: &DFut<T>) -> DResult<DFut<T>> {
        self.stop_stopwatch();

        if let Some(valid) = self.is_valid_id(&d_fut.d_store_id) {
            if !valid {
                return Err(Error::System);
            }
        }

        self.shared_runtime_state
            .d_store
            .share(&self.rpc_context(), &d_fut.d_store_id, 1)
            .await?;

        self.start_stopwatch();

        Ok(d_fut.share())
    }

    #[tracing::instrument(skip_all)]
    pub async fn share_n<T>(&self, d_fut: &DFut<T>, n: u64) -> DResult<Vec<DFut<T>>> {
        self.stop_stopwatch();

        if let Some(valid) = self.is_valid_id(&d_fut.d_store_id) {
            if !valid {
                return Err(Error::System);
            }
        }

        self.shared_runtime_state
            .d_store
            .share(&self.rpc_context(), &d_fut.d_store_id, n)
            .await?;

        self.start_stopwatch();

        Ok((0..n).map(|_| d_fut.share()).collect())
    }

    #[tracing::instrument(skip_all)]
    pub async fn d_box<T>(&self, t: T) -> DResult<DFut<T>>
    where
        T: Serialize + std::fmt::Debug + Send + Sync + 'static,
    {
        self.stop_stopwatch();
        let d_store_id = self.next_d_store_id();
        self.shared_runtime_state
            .d_store
            .publish(d_store_id.clone(), t)?;
        self.start_stopwatch();
        Ok(d_store_id.into())
    }
}

impl Runtime {
    fn new_local_child(&self, parent_info: Vec<ParentInfo>) -> Runtime {
        Runtime {
            shared_runtime_state: Arc::clone(&self.shared_runtime_state),

            lifetime_id: self.lifetime_id,
            parent_info: Arc::new(parent_info),
            task_id: self
                .shared_runtime_state
                .next_task_id
                .fetch_add(1, Ordering::SeqCst),
            inner: Arc::default(),

            requests: Arc::default(),
        }
    }

    pub fn schedule_work(&self, fn_name: &str, arg_size: usize) -> DResult<Where> {
        self.shared_runtime_state
            .d_scheduler
            .accept_local_work(fn_name, arg_size)
            .ok_or(Error::System)
    }

    fn track_task_failure(&self, d_store_id: DStoreId) {
        self.shared_runtime_state
            .d_store
            .local_task_failure(d_store_id);

        // Only push to failed tasks if we are on the current lifetime_id, otherwise the task fail
        // will be handled by the lifetime failover logic.
        if self.lifetime_id == self.shared_runtime_state.lifetime_id.load(Ordering::SeqCst) {
            counter!("task_failures::push").increment(1);

            self.shared_runtime_state
                .task_failures
                .lock()
                .unwrap()
                .push(TaskFailure {
                    task_id: self.task_id,
                    lifetime_id: self.lifetime_id,
                    requests: self.requests.lock().unwrap().clone(),
                });
        }
    }

    // Can we use https://docs.rs/tokio-metrics/0.3.1/tokio_metrics/ to make decisions?
    //
    // We return a DStoreId so that we can just pass it over the network.
    //
    #[tracing::instrument(skip(self, fut))]
    fn do_local_work<F, T>(&self, fn_name: &str, fut: F) -> DStoreId
    where
        F: Future<Output = DResult<T>> + Send + 'static,
        T: Serialize + std::fmt::Debug + Send + Sync + 'static,
    {
        let d_store_id = self.next_d_store_id();

        self.shared_runtime_state.task_tracker.spawn({
            // TODO: I don't like that we pass two references to the same runtime into the spawn:
            // once here and once in the macro. Figure out how to fix this.
            let rt = self.clone();
            let d_store_id = d_store_id.clone();
            let fn_name = fn_name.to_string();
            async move {
                rt.start_stopwatch();

                if rt.check_runtime_state().is_err() {
                    rt.track_task_failure(d_store_id);
                    return;
                }

                let t = match fut.await {
                    Ok(t) => t,
                    Err(Error::System) => {
                        rt.track_task_failure(d_store_id);
                        return;
                    }
                };

                let size = size_ser::to_size(&t).unwrap();
                let took = rt.finish_local_work(&fn_name, size);

                histogram!("do_local_work::duration", "fn_name" => fn_name.clone()).record(took);
                histogram!("do_local_work::size", "fn_name" => fn_name.clone()).record(size as f64);

                if rt.check_runtime_state().is_err() {
                    rt.track_task_failure(d_store_id);
                    return;
                }

                if let Err(_) = rt
                    .shared_runtime_state
                    .d_store
                    .publish(d_store_id.clone(), t)
                {
                    rt.track_task_failure(d_store_id);
                    return;
                }
            }
        });

        d_store_id
    }

    fn track_call(&self, d_store_id: DStoreId, call: Call) {
        self.inner.lock().unwrap().calls.insert(d_store_id, call);
    }

    pub fn do_local_work_fut<F, T, FutFn>(&self, fn_name: &str, f: FutFn) -> DFut<T>
    where
        F: Future<Output = DResult<T>> + Send + 'static,
        T: Serialize + std::fmt::Debug + Send + Sync + 'static,
        FutFn: FnOnce(Runtime) -> F,
    {
        let mut parent_info = (*self.parent_info).clone();
        parent_info.push(ParentInfo {
            address: self.shared_runtime_state.local_server_address.clone(),
            lifetime_id: self.lifetime_id,
            task_id: self.task_id,
            request_id: None,
        });

        let r = self.new_local_child(parent_info);
        let fut = f(r.clone());
        let d_store_id = r.do_local_work(fn_name, fut);

        self.track_call(
            d_store_id.clone(),
            Call::Local {
                fn_name: fn_name.to_string(),
            },
        );

        d_store_id.into()
    }

    fn next_d_store_id(&self) -> DStoreId {
        self.shared_runtime_state
            .d_store
            .take_next_id(Arc::clone(&self.parent_info), self.task_id)
    }

    // TODO: it is possible that the global scheduler routes the work back to
    // the worker requesting remote scheduling. We want to avoid a self to self network
    // transmission.
    //
    //
    // TODO: just generate this for each fn, that way we don't have to use Work
    // for local computation. We only use it for remote computation.
    //
    // Put work into local queue or remote queue.
    #[tracing::instrument(skip(self, iw))]
    pub async fn do_remote_work<I, T>(&self, address: &str, iw: I) -> DResult<DFut<T>>
    where
        I: IntoWork,
    {
        let work = iw.into_work();

        let parent_info = self.next_parent_info(&address);

        let d_store_id = self
            .shared_runtime_state
            .peer_worker_client
            .do_work(&parent_info, &address, work.clone())
            .await?;

        self.track_call(d_store_id.clone(), Call::Remote { work });

        Ok(d_store_id.into())
    }
}
