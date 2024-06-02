use std::collections::{hash_map::Entry, HashMap};
use std::future::Future;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use metrics::histogram;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tonic::{
    transport::{Channel, Endpoint, Server},
    Status,
};

use crate::{
    consts::DFUT_RETRIES,
    d_fut::DFut,
    d_scheduler::{DScheduler, Where},
    d_store::{DStore, DStoreId, ParentInfo, ValueTrait},
    fn_index::FnIndex,
    gaps::AddressToGaps,
    peer_work::PeerWorkerClient,
    retry::retry,
    rpc_context::RpcContext,
    seq::Seq,
    services::{
        d_store_service::d_store_service_server::DStoreServiceServer,
        global_scheduler_service::{
            global_scheduler_service_client::GlobalSchedulerServiceClient, CurrentRuntimeInfo,
            FailedLocalTask, HeartBeatRequest, HeartBeatResponse, RegisterRequest,
            RegisterResponse, RequestId, RuntimeInfo,
        },
        worker_service::{
            worker_service_server::{WorkerService, WorkerServiceServer},
            DoWorkResponse,
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

    fn_name_to_addresses: Mutex<FnIndex>,
    failed_local_tasks: Mutex<Vec<FailedLocalTask>>,
}

#[derive(Debug, Clone)]
pub struct RootRuntime {
    shared_runtime_state: Arc<SharedRuntimeState>,
    heart_beat_timeout: u64,
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

    pub async fn serve<T>(cfg: WorkerServerConfig, available_fn_names: Vec<String>)
    where
        T: WorkerServiceExt,
    {
        let WorkerServerConfig {
            local_server_address,
            global_scheduler_address,
            fn_names,
        } = cfg;

        let endpoint: Endpoint = global_scheduler_address.parse().unwrap();

        let fn_names = Self::filter_fn_names(available_fn_names, fn_names);

        let mut client: Option<GlobalSchedulerServiceClient<Channel>> = None;

        let RegisterResponse {
            lifetime_id,
            heart_beat_timeout,
            ..
        } = retry(&mut client, &endpoint, |mut client| {
            let address = local_server_address.to_string();
            let fn_names = fn_names.clone();
            async move { client.register(RegisterRequest { address, fn_names }).await }
        })
        .await
        .expect("Unable to register.");

        let lifetime_id = Arc::new(AtomicU64::new(lifetime_id));

        let address_to_gaps = AddressToGaps::default();

        let d_store = Arc::new(
            DStore::new(&local_server_address, &lifetime_id, address_to_gaps.clone()).await,
        );

        let root_runtime = Self {
            shared_runtime_state: Arc::new(SharedRuntimeState {
                local_server_address: local_server_address.to_string(),
                // TODO: pass in through config
                dfut_retries: DFUT_RETRIES,

                d_scheduler: DScheduler::default(),
                peer_worker_client: PeerWorkerClient::new(),
                d_store: Arc::clone(&d_store),

                address_to_gaps,
                address_to_runtime_info: Mutex::default(),
                lifetime_id,
                next_task_id: AtomicU64::new(0),
                next_request_id: Arc::default(),

                fn_name_to_addresses: Mutex::new(FnIndex::default()),
                failed_local_tasks: Mutex::default(),
            }),

            heart_beat_timeout,
        };

        let heart_beat_fut = tokio::spawn({
            let root_runtime = root_runtime.clone();
            async move {
                root_runtime.heart_beat_forever(endpoint, client).await;
            }
        });

        let serve_fut = Server::builder()
            .add_service(
                DStoreServiceServer::new(d_store)
                    .max_encoding_message_size(usize::MAX)
                    .max_decoding_message_size(usize::MAX),
            )
            .add_service(
                WorkerServiceServer::new(T::new(root_runtime))
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

        tokio::select! {
            r = serve_fut => r.unwrap(),
            _ = heart_beat_fut => {},
        }
    }

    fn new_child(
        &self,
        parent_address: &str,
        parent_lifetime_id: u64,
        parent_task_id: u64,
        parent_request_id: u64,
    ) -> Runtime {
        Runtime {
            shared_runtime_state: Arc::clone(&self.shared_runtime_state),

            lifetime_id: self.shared_runtime_state.lifetime_id.load(Ordering::SeqCst),
            parent_info: Arc::new(ParentInfo {
                address: parent_address.to_string(),
                lifetime_id: parent_lifetime_id,
                task_id: parent_task_id,
                request_id: parent_request_id,
            }),
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

    pub fn do_local_work<F, T, FutFn>(
        &self,
        parent_address: &str,
        parent_lifetime_id: u64,
        parent_task_id: u64,
        request_id: u64,
        fn_name: &str,
        f: FutFn,
    ) -> Result<DoWorkResponse, Status>
    where
        F: Future<Output = DResult<T>> + Send + 'static,
        T: Serialize + std::fmt::Debug + Send + Sync + 'static,
        FutFn: FnOnce(Runtime) -> F,
    {
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

        let runtime = self.new_child(
            parent_address,
            parent_lifetime_id,
            parent_task_id,
            request_id,
        );
        let fut = f(runtime.clone());
        let d_store_id = runtime.do_local_work(fn_name, fut);
        Ok(d_store_id.into())
    }

    async fn heart_beat_forever(
        &self,
        endpoint: Endpoint,
        mut global_scheduler: Option<GlobalSchedulerServiceClient<Channel>>,
    ) {
        // TODO: shutdown via select.
        let sleep_for = self.heart_beat_timeout / 3;
        let mut request_id = 0u64;
        loop {
            let local_lifetime_id = self.shared_runtime_state.lifetime_id.load(Ordering::SeqCst);

            let failed_local_tasks = {
                self.shared_runtime_state
                    .failed_local_tasks
                    .lock()
                    .unwrap()
                    .clone()
            };

            let HeartBeatResponse {
                lifetime_id,
                address_to_runtime_info,
                ..
            } = retry(&mut global_scheduler, &endpoint, |mut client| {
                let address = self.shared_runtime_state.local_server_address.to_string();
                let failed_local_tasks = failed_local_tasks.clone();
                async move {
                    client
                        .heart_beat(HeartBeatRequest {
                            request_id,
                            address,
                            current_runtime_info: Some(CurrentRuntimeInfo {
                                lifetime_id: local_lifetime_id,
                                stats: None,
                                failed_local_tasks,
                            }),
                        })
                        .await
                }
            })
            .await
            .unwrap();
            // TODO: turn off the worker here so that we can full restart.

            request_id += 1;

            let fn_index = FnIndex::compute(
                &address_to_runtime_info,
                &self.shared_runtime_state.local_server_address,
            );
            {
                *self
                    .shared_runtime_state
                    .fn_name_to_addresses
                    .lock()
                    .unwrap() = fn_index;
            }

            {
                let mut previous_address_to_runtime_info = self
                    .shared_runtime_state
                    .address_to_runtime_info
                    .lock()
                    .unwrap();

                // worker_failure
                for (address, runtime_info) in &address_to_runtime_info {
                    if let Some(previous_runtime_info) =
                        previous_address_to_runtime_info.get(address)
                    {
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
                                    .try_reset(address, runtime_info.lifetime_id);
                            }
                        }
                    }
                }

                *previous_address_to_runtime_info = address_to_runtime_info;
            }

            if lifetime_id != local_lifetime_id {
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

            // TODO: insert tombstone for tasks that are running on this worker

            sleep_with_jitter(sleep_for).await;
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
    parent_info: Arc<ParentInfo>,
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

        if let Some(parent_runtime_info) = self
            .shared_runtime_state
            .address_to_runtime_info
            .lock()
            .unwrap()
            .get(&self.parent_info.address)
        {
            if parent_runtime_info.lifetime_id < self.parent_info.lifetime_id {
                return Err(Error::System);
            }

            if parent_runtime_info.lifetime_id == self.parent_info.lifetime_id {
                let mut failure = false;
                for task_failure in &parent_runtime_info.task_failures {
                    if task_failure.lifetime_id == self.parent_info.lifetime_id
                        && task_failure.task_id == self.parent_info.task_id
                    {
                        failure = true;
                        break;
                    }
                }
                if failure {
                    return Err(Error::System);
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

    fn next_request_id(&self, address: &str) -> u64 {
        let id = self
            .shared_runtime_state
            .next_request_id
            .lock()
            .unwrap()
            .next(address);
        self.requests.lock().unwrap().push(RequestId {
            address: address.to_string(),
            request_id: id,
        });
        id
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
                    let address = {
                        self.shared_runtime_state
                            .fn_name_to_addresses
                            .lock()
                            .unwrap()
                            .schedule_fn(&work.fn_name)
                            .unwrap()
                    };

                    let request_id = self.next_request_id(&address);

                    let d_store_id = self
                        .shared_runtime_state
                        .peer_worker_client
                        .do_work(
                            &self.shared_runtime_state.local_server_address,
                            self.lifetime_id,
                            self.task_id,
                            request_id,
                            &address,
                            work.clone(),
                        )
                        .await?;

                    let t: DResult<T> = self
                        .shared_runtime_state
                        .d_store
                        .get_or_watch(&self.rpc_context(), d_store_id.clone())
                        .await;

                    if let Ok(t) = t {
                        let mut inner = self.inner.lock().unwrap();
                        let v: Arc<dyn ValueTrait> = Arc::new(t.clone());
                        inner
                            .calls
                            .insert(d_store_id.clone(), Call::RetriedOk { v: Arc::clone(&v) });
                        let _ = tx.send(v);

                        return Ok(t);
                    }

                    self.check_runtime_state()?;
                }

                let mut inner = self.inner.lock().unwrap();
                inner.calls.insert(d_store_id.clone(), Call::RetriedErr);

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

    pub async fn wait<T>(&self, d_fut: DFut<T>) -> DResult<T>
    where
        T: Serialize + DeserializeOwned + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        self.stop_stopwatch();

        if let Some(valid) = self.is_valid_id(&d_fut.d_store_id) {
            if !valid {
                return self.try_retry_dfut(&d_fut.d_store_id).await;
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
    fn new_child(&self) -> Runtime {
        Runtime {
            shared_runtime_state: Arc::clone(&self.shared_runtime_state),

            lifetime_id: self.lifetime_id,
            parent_info: Arc::clone(&self.parent_info),
            task_id: self
                .shared_runtime_state
                .next_task_id
                .fetch_add(1, Ordering::SeqCst),
            inner: Arc::default(),

            requests: Arc::default(),
        }
    }

    pub fn schedule_work(&self, fn_name: &str, arg_size: usize) -> Where {
        self.shared_runtime_state
            .d_scheduler
            .accept_local_work(fn_name, arg_size)
    }

    fn track_failed_task(&self, d_store_id: DStoreId) {
        self.shared_runtime_state
            .d_store
            .local_task_failure(d_store_id);

        // Only push to failed tasks if we are on the current lifetime_id, otherwise the task fail
        // will be handled by the lifetime failover logic.
        let mut failed_local_tasks = self.shared_runtime_state.failed_local_tasks.lock().unwrap();
        // Only push to failed tasks if we are on the current lifetime_id, otherwise the task fail
        // will be handled by the lifetime failover logic.
        if self.lifetime_id == self.shared_runtime_state.lifetime_id.load(Ordering::SeqCst) {
            failed_local_tasks.push(FailedLocalTask {
                task_id: self.task_id,
                requests: self.requests.lock().unwrap().clone(),
            });
        }
    }

    // Can we use https://docs.rs/tokio-metrics/0.3.1/tokio_metrics/ to make decisions?
    //
    // We return a DStoreId so that we can just pass it over the network.
    //
    fn do_local_work<F, T>(&self, fn_name: &str, fut: F) -> DStoreId
    where
        F: Future<Output = DResult<T>> + Send + 'static,
        T: Serialize + std::fmt::Debug + Send + Sync + 'static,
    {
        let d_store_id = self.next_d_store_id();

        self.track_call(
            d_store_id.clone(),
            Call::Local {
                fn_name: fn_name.to_string(),
            },
        );

        tokio::spawn({
            // TODO: I don't like that we pass two references to the same runtime into the spawn:
            // once here and once in the macro. Figure out how to fix this.
            let rt = self.clone();
            let d_store_id = d_store_id.clone();
            let fn_name = fn_name.to_string();
            async move {
                rt.start_stopwatch();

                if rt.check_runtime_state().is_err() {
                    rt.track_failed_task(d_store_id);
                    return;
                }

                let t = match fut.await {
                    Ok(t) => t,
                    Err(Error::System) => {
                        rt.track_failed_task(d_store_id);
                        return;
                    }
                };

                let size = size_ser::to_size(&t).unwrap();
                let took = rt.finish_local_work(&fn_name, size);

                histogram!("do_local_work::duration", "fn_name" => fn_name.clone()).record(took);
                histogram!("do_local_work::size", "fn_name" => fn_name.clone()).record(size as f64);

                if rt.check_runtime_state().is_err() {
                    rt.track_failed_task(d_store_id);
                    return;
                }

                // TODO: check parent lifeitime id vs rt.parent_info.lifeitime
                // If the entry was removed then publish will fail. So we can ignore the Error.
                let _ = rt.shared_runtime_state.d_store.publish(d_store_id, t);
            }
        });

        d_store_id
    }

    pub fn do_local_work_fut<F, T, FutFn>(&self, fn_name: &str, f: FutFn) -> DFut<T>
    where
        F: Future<Output = DResult<T>> + Send + 'static,
        T: Serialize + std::fmt::Debug + Send + Sync + 'static,
        FutFn: FnOnce(Runtime) -> F,
    {
        let r = self.new_child();
        let fut = f(r);
        let d_store_id = self.do_local_work(fn_name, fut);
        d_store_id.into()
    }

    fn track_call(&self, d_store_id: DStoreId, call: Call) {
        self.inner.lock().unwrap().calls.insert(d_store_id, call);
    }

    fn next_d_store_id(&self) -> DStoreId {
        // TODO: pass owner info to d_store to allow for parent based cleanup.
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
    pub async fn do_remote_work<I, T>(&self, iw: I) -> DResult<DFut<T>>
    where
        I: IntoWork,
    {
        let work = iw.into_work();

        let address = {
            self.shared_runtime_state
                .fn_name_to_addresses
                .lock()
                .unwrap()
                .schedule_fn(&work.fn_name)
                .unwrap()
        };

        let request_id = self.next_request_id(&address);

        let d_store_id = self
            .shared_runtime_state
            .peer_worker_client
            .do_work(
                &self.shared_runtime_state.local_server_address,
                self.lifetime_id,
                self.task_id,
                request_id,
                &address,
                work.clone(),
            )
            .await?;

        self.track_call(d_store_id.clone(), Call::Remote { work });

        Ok(d_store_id.into())
    }
}
