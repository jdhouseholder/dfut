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
    d_fut::DFut,
    d_scheduler::{DScheduler, Where},
    d_store::{DStore, DStoreClient, DStoreId, ParentInfo, ValueTrait},
    fn_index::FnIndex,
    gaps::LifetimeScopedGaps,
    peer_work::PeerWorkerClient,
    rpc_context::RpcContext,
    seq::Seq,
    services::{
        d_store_service::d_store_service_server::DStoreServiceServer,
        global_scheduler_service::{
            global_scheduler_service_client::GlobalSchedulerServiceClient, CurrentRuntimeInfo,
            FailedLocalTask, HeartBeatRequest, HeartBeatResponse, RegisterClientRequest,
            RegisterClientResponse, RegisterRequest, RegisterResponse, RequestId, RuntimeInfo,
        },
        worker_service::{
            worker_service_server::WorkerService, worker_service_server::WorkerServiceServer,
            DoWorkResponse,
        },
    },
    sleep::sleep_with_jitter,
    stopwatch::Stopwatch,
    work::{IntoWork, Work},
    DResult, Error,
};

const DFUT_RETRIES: usize = 10;

#[derive(Debug)]
struct SharedRuntimeState {
    local_server_address: String,
    dfut_retries: usize,

    global_scheduler: GlobalSchedulerServiceClient<Channel>,
    d_scheduler: DScheduler,
    peer_worker_client: PeerWorkerClient,
    d_store: Arc<DStore>,

    address_to_gaps: Arc<Mutex<HashMap<String, LifetimeScopedGaps>>>,
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
    pub async fn new(
        local_server_address: &str,
        global_scheduler_address: &str,
        fn_names: Vec<String>,
    ) -> Self {
        let endpoint: Endpoint = global_scheduler_address.parse().unwrap();
        let mut global_scheduler = Self::gs_connect(endpoint).await;

        let RegisterResponse {
            lifetime_id,
            heart_beat_timeout,
            ..
        } = global_scheduler
            .register(RegisterRequest {
                address: local_server_address.to_string(),
                fn_names,
            })
            .await
            .unwrap()
            .into_inner();

        let lifetime_id = Arc::new(AtomicU64::new(lifetime_id));
        let next_task_id = AtomicU64::new(0);

        let address_to_gaps = Arc::default();

        let d_store = Arc::new(
            DStore::new(
                local_server_address,
                &lifetime_id,
                Arc::clone(&address_to_gaps),
            )
            .await,
        );

        Self {
            shared_runtime_state: Arc::new(SharedRuntimeState {
                local_server_address: local_server_address.to_string(),
                // TODO: pass in through config
                dfut_retries: DFUT_RETRIES,

                global_scheduler,
                d_scheduler: DScheduler::default(),
                peer_worker_client: PeerWorkerClient::new(),
                d_store,

                address_to_gaps,
                address_to_runtime_info: Mutex::default(),
                lifetime_id,
                next_task_id,
                next_request_id: Arc::default(),

                fn_name_to_addresses: Mutex::new(FnIndex::default()),
                failed_local_tasks: Mutex::default(),
            }),

            heart_beat_timeout,
        }
    }

    async fn gs_connect(endpoint: Endpoint) -> GlobalSchedulerServiceClient<Channel> {
        for i in 0..5 {
            if let Ok(client) = GlobalSchedulerServiceClient::connect(endpoint.clone()).await {
                return client;
            }
            tokio::time::sleep(Duration::from_millis(100 * 2u64.pow(i))).await;
        }
        panic!();
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

    fn have_seen_request_id(
        &self,
        parent_address: &str,
        parent_lifetime_id: u64,
        request_id: u64,
    ) -> bool {
        let mut address_to_gaps = self.shared_runtime_state.address_to_gaps.lock().unwrap();
        let gaps = address_to_gaps
            .entry(parent_address.to_string())
            .or_default();
        if gaps.lifetime_id() < parent_lifetime_id {
            gaps.reset(parent_lifetime_id);
        }
        if gaps.lifetime_id() > parent_lifetime_id {
            // TODO: This would mean that there is a system bug.
            unreachable!()
        }

        gaps.add(request_id).is_seen()
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

        if self.have_seen_request_id(parent_address, parent_lifetime_id, request_id) {
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

    async fn heart_beat_forever(&self) {
        // TODO: shutdown via select.
        let sleep_for = self.heart_beat_timeout / 3;
        loop {
            let local_lifetime_id = self.shared_runtime_state.lifetime_id.load(Ordering::SeqCst);

            let failed_local_tasks = {
                self.shared_runtime_state
                    .failed_local_tasks
                    .lock()
                    .unwrap()
                    .clone()
            };

            // TODO: retry & graceful fail.
            let HeartBeatResponse {
                lifetime_id,
                address_to_runtime_info,
                ..
            } = self
                .shared_runtime_state
                .global_scheduler
                .clone()
                .heart_beat(HeartBeatRequest {
                    address: self.shared_runtime_state.local_server_address.to_string(),
                    current_runtime_info: Some(CurrentRuntimeInfo {
                        lifetime_id: local_lifetime_id,
                        stats: None,
                        failed_local_tasks,
                    }),
                })
                .await
                .unwrap()
                .into_inner();

            let fn_index = compute_fn_index(
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

                            if let Some(gaps) = self
                                .shared_runtime_state
                                .address_to_gaps
                                .lock()
                                .unwrap()
                                .get_mut(address)
                            {
                                gaps.reset(runtime_info.lifetime_id);
                            }
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
                                if let Some(gaps) = self
                                    .shared_runtime_state
                                    .address_to_gaps
                                    .lock()
                                    .unwrap()
                                    .get_mut(address)
                                {
                                    gaps.add(request.request_id);
                                }
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

    pub async fn serve<T>(self, address: &str, worker_service_server: WorkerServiceServer<T>)
    where
        T: WorkerService,
    {
        let address = address
            .strip_prefix("http://")
            .unwrap()
            .to_string()
            .parse()
            .unwrap();

        let d_store = Arc::clone(&self.shared_runtime_state.d_store);

        let heart_beat_fut = tokio::spawn(async move {
            self.heart_beat_forever().await;
        });

        let serve_fut = Server::builder()
            .add_service(
                DStoreServiceServer::new(d_store)
                    .max_encoding_message_size(usize::MAX)
                    .max_decoding_message_size(usize::MAX),
            )
            .add_service(
                worker_service_server
                    .max_encoding_message_size(usize::MAX)
                    .max_decoding_message_size(usize::MAX),
            )
            .serve(address);

        tokio::select! {
            r = serve_fut => r.unwrap(),
            _ = heart_beat_fut => {},
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

fn compute_fn_index(
    address_to_runtime_info: &HashMap<String, RuntimeInfo>,
    current_address: &str,
) -> FnIndex {
    let mut m: HashMap<String, Vec<String>> = HashMap::new();
    for (address, runtime_info) in address_to_runtime_info {
        if address == current_address {
            continue;
        }
        if let Some(stats) = &runtime_info.stats {
            for fn_name in stats.fn_stats.keys() {
                m.entry(fn_name.to_string())
                    .or_default()
                    .push(address.to_string());
            }
        }
    }
    FnIndex::new(m)
}

#[derive(Debug, Default)]
struct SharedRuntimeClientState {
    client_id: String,
    lifetime_id: u64,
    next_task_id: u64,

    fn_name_to_addresses: FnIndex,
}

pub struct RootRuntimeClient {
    shared: Arc<Mutex<SharedRuntimeClientState>>,
    next_request_id: Arc<Mutex<Seq>>,
    peer_worker_client: PeerWorkerClient,
    d_store_client: DStoreClient,
}

impl RootRuntimeClient {
    pub async fn new(global_scheduler_address: &str) -> Self {
        let shared = Arc::new(Mutex::new(SharedRuntimeClientState::default()));
        tokio::spawn({
            let global_scheduler_address = global_scheduler_address.to_string();
            let shared = Arc::clone(&shared);
            async move { Self::heart_beat_forever(&global_scheduler_address, shared).await }
        });
        Self {
            shared,
            next_request_id: Arc::default(),
            peer_worker_client: PeerWorkerClient::new(),
            d_store_client: DStoreClient::new(),
        }
    }

    async fn gs_connect(endpoint: Endpoint) -> GlobalSchedulerServiceClient<Channel> {
        for i in 0..5 {
            if let Ok(client) = GlobalSchedulerServiceClient::connect(endpoint.clone()).await {
                return client;
            }
            tokio::time::sleep(Duration::from_millis(100 * 2u64.pow(i))).await;
        }
        panic!();
    }

    async fn heart_beat_forever(
        global_scheduler_address: &str,
        shared: Arc<Mutex<SharedRuntimeClientState>>,
    ) {
        let endpoint: Endpoint = global_scheduler_address.parse().unwrap();
        let mut global_scheduler = Self::gs_connect(endpoint).await;

        let RegisterClientResponse {
            client_id,
            lifetime_id,
            heart_beat_timeout,
            ..
        } = global_scheduler
            .register_client(RegisterClientRequest {})
            .await
            .unwrap()
            .into_inner();
        {
            let mut shared = shared.lock().unwrap();
            shared.client_id = client_id;
            shared.lifetime_id = lifetime_id;
        }
        let sleep_for = heart_beat_timeout / 3;
        loop {
            let (client_id, lifetime_id) = {
                let shared = shared.lock().unwrap();
                (shared.client_id.clone(), shared.lifetime_id)
            };

            let HeartBeatResponse {
                lifetime_id,
                address_to_runtime_info,
            } = global_scheduler
                .heart_beat(HeartBeatRequest {
                    address: client_id,
                    current_runtime_info: Some(CurrentRuntimeInfo {
                        lifetime_id,
                        // TODO: share failed local tasks.
                        ..Default::default()
                    }),
                })
                .await
                .unwrap()
                .into_inner();

            let i = compute_fn_index(&address_to_runtime_info, "");
            {
                let mut shared = shared.lock().unwrap();
                shared.lifetime_id = lifetime_id;
                shared.fn_name_to_addresses = i;
            }

            sleep_with_jitter(sleep_for).await;
        }
    }

    pub fn new_runtime_client(&self) -> RuntimeClient {
        let mut shared = self.shared.lock().unwrap();
        let task_id = shared.next_task_id;
        shared.next_task_id += 1;

        RuntimeClient {
            shared: Arc::clone(&self.shared),
            next_request_id: Arc::clone(&self.next_request_id),
            peer_worker_client: self.peer_worker_client.clone(),
            d_store_client: self.d_store_client.clone(),
            task_id,

            calls: Mutex::new(HashMap::new()),
            requests: Arc::default(),
            dfut_retries: DFUT_RETRIES,
        }
    }
}

enum ClientCall {
    Remote { work: Work },
    Retrying { tx: Sender<Arc<dyn ValueTrait>> },
    RetriedOk { v: Arc<dyn ValueTrait> },
    RetriedErr,
}

// TODO: Runtime clients need to heartbeat with the global scheduler.
// They must maintain a lifetime and each real client must have a unique (address, lifetime id, task id) triple.
// TODO: retry in the client too.
pub struct RuntimeClient {
    shared: Arc<Mutex<SharedRuntimeClientState>>,
    next_request_id: Arc<Mutex<Seq>>,
    peer_worker_client: PeerWorkerClient,
    d_store_client: DStoreClient,

    task_id: u64,

    calls: Mutex<HashMap<DStoreId, ClientCall>>,
    requests: Arc<Mutex<Vec<RequestId>>>,
    dfut_retries: usize,
}

impl RuntimeClient {
    fn rpc_context(&self) -> RpcContext {
        let shared = self.shared.lock().unwrap();
        RpcContext {
            local_address: shared.client_id.clone(),
            lifetime_id: shared.lifetime_id,
            next_request_id: Arc::clone(&self.next_request_id),
            requests: Arc::clone(&self.requests),
        }
    }

    fn next_request_id(&self, address: &str) -> u64 {
        let id = self.next_request_id.lock().unwrap().next(address);
        self.requests.lock().unwrap().push(RequestId {
            address: address.to_string(),
            request_id: id,
        });
        id
    }

    pub async fn do_remote_work<I, T>(&self, iw: I) -> DResult<DFut<T>>
    where
        I: IntoWork,
    {
        let w = iw.into_work();
        // TODO: fail if we don't eventually schedule?
        let (client_id, address, lifetime_id) = {
            'scheduled: loop {
                {
                    let shared = self.shared.lock().unwrap();
                    if let Some(address) = shared.fn_name_to_addresses.schedule_fn(&w.fn_name) {
                        let client_id = shared.client_id.clone();
                        let lifetime_id = shared.lifetime_id;
                        break 'scheduled (client_id, address, lifetime_id);
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        };

        let request_id = self.next_request_id(&address);

        let d_store_id = self
            .peer_worker_client
            .do_work(
                &client_id,
                lifetime_id,
                self.task_id,
                request_id,
                &address,
                w.clone(),
            )
            .await?;

        self.calls
            .lock()
            .unwrap()
            .insert(d_store_id.clone(), ClientCall::Remote { work: w.clone() });

        Ok(d_store_id.into())
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
            let mut calls = self.calls.lock().unwrap();
            let mut e = calls.entry(d_store_id.clone());
            match e {
                Entry::Occupied(ref mut o) => match o.get() {
                    ClientCall::Remote { .. } => {
                        let (tx, _rx) = channel(1);
                        let v = o.insert(ClientCall::Retrying { tx: tx.clone() });
                        let ClientCall::Remote { work } = v else {
                            unreachable!()
                        };
                        RetryState::Sender(work, tx)
                    }
                    ClientCall::Retrying { tx } => RetryState::Receiver(tx.subscribe()),
                    ClientCall::RetriedOk { v } => {
                        let t: Arc<T> = Arc::clone(&v).as_any().downcast().unwrap();
                        return Ok((*t).clone());
                    }
                    ClientCall::RetriedErr => return Err(Error::System),
                },
                Entry::Vacant(_) => unreachable!(),
            }
        };
        match r {
            RetryState::Sender(work, tx) => {
                for _ in 0..self.dfut_retries {
                    let (client_id, lifetime_id, task_id, address) = {
                        let mut shared = self.shared.lock().unwrap();
                        let address = shared
                            .fn_name_to_addresses
                            .schedule_fn(&work.fn_name)
                            .unwrap();
                        let client_id = shared.client_id.clone();
                        let lifetime_id = shared.lifetime_id;
                        let task_id = shared.next_task_id;
                        shared.next_task_id += 1;
                        (client_id, lifetime_id, task_id, address)
                    };

                    let request_id = self.next_request_id(&address);

                    let d_store_id = self
                        .peer_worker_client
                        .do_work(
                            &client_id,
                            lifetime_id,
                            task_id,
                            request_id,
                            &address,
                            work.clone(),
                        )
                        .await?;

                    let t: DResult<T> = self
                        .d_store_client
                        .get_or_watch(&self.rpc_context(), d_store_id.clone())
                        .await;

                    if let Ok(t) = t {
                        let v: Arc<dyn ValueTrait> = Arc::new(t.clone());
                        self.calls.lock().unwrap().insert(
                            d_store_id.clone(),
                            ClientCall::RetriedOk { v: Arc::clone(&v) },
                        );
                        let _ = tx.send(v);

                        return Ok(t);
                    }
                }

                self.calls
                    .lock()
                    .unwrap()
                    .insert(d_store_id.clone(), ClientCall::RetriedErr);

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
        let t = self
            .d_store_client
            .get_or_watch(&self.rpc_context(), d_fut.d_store_id.clone())
            .await;

        match t {
            Ok(t) => Ok(t),
            Err(_) => self.try_retry_dfut(&d_fut.d_store_id).await,
        }
    }

    pub async fn cancel<T>(&self, d_fut: DFut<T>) -> DResult<()>
    where
        T: DeserializeOwned,
    {
        self.d_store_client
            .decrement_or_remove(&self.rpc_context(), d_fut.d_store_id, 1)
            .await
    }

    pub async fn share<T>(&self, d_fut: &DFut<T>) -> DResult<DFut<T>> {
        self.d_store_client
            .share(&self.rpc_context(), &d_fut.d_store_id, 1)
            .await?;
        Ok(d_fut.share())
    }

    pub async fn share_n<T>(&self, d_fut: &DFut<T>, n: u64) -> DResult<Vec<DFut<T>>> {
        self.d_store_client
            .share(&self.rpc_context(), &d_fut.d_store_id, n)
            .await?;
        Ok((0..n).map(|_| d_fut.share()).collect())
    }

    pub async fn d_box<T>(&self, _t: T) -> DResult<DFut<T>>
    where
        T: Serialize + Send + 'static,
    {
        todo!()
    }
}
