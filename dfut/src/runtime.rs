use std::collections::{hash_map::Entry, HashMap};
use std::future::Future;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use metrics::{counter, histogram};
use rand::seq::SliceRandom;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tonic::transport::{Channel, Endpoint, Server};

use crate::{
    d_fut::{DFut, InnerDFut},
    d_scheduler::DScheduler,
    d_store::{
        d_store_service::d_store_service_server::DStoreServiceServer, DStore, DStoreClient,
        DStoreId, ValueTrait,
    },
    global_scheduler::global_scheduler_service::{
        global_scheduler_service_client::GlobalSchedulerServiceClient, FailedTasks,
        HeartBeatRequest, HeartBeatResponse, RegisterClientRequest, RegisterClientResponse,
        RegisterRequest, RegisterResponse, Stats,
    },
    peer_work::{
        worker_service::{
            worker_service_server::WorkerService, worker_service_server::WorkerServiceServer,
            DoWorkResponse,
        },
        PeerWorkerClient,
    },
    timer::Timer,
    work::{IntoWork, Work},
    DResult, Error,
};

const DFUT_RETRIES: usize = 3;

#[derive(Debug)]
struct SharedRuntimeState {
    local_server_address: String,
    dfut_retries: usize,

    global_scheduler: GlobalSchedulerServiceClient<Channel>,
    d_scheduler: DScheduler,
    peer_worker_client: PeerWorkerClient,
    d_store: Arc<DStore>,
    lifetime_list_id: AtomicU64,
    lifetimes: Mutex<HashMap<String, u64>>,
    lifetime_id: Arc<AtomicU64>,
    next_task_id: AtomicU64,

    fn_name_to_addresses: Mutex<FnIndex>,

    failed_local_tasks: Mutex<Vec<u64>>,
    failed_remote_tasks: Mutex<HashMap<String, FailedTasks>>,
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

        let d_store = Arc::new(DStore::new(local_server_address, &lifetime_id).await);

        Self {
            shared_runtime_state: Arc::new(SharedRuntimeState {
                local_server_address: local_server_address.to_string(),
                // TODO: pass in through config
                dfut_retries: DFUT_RETRIES,

                global_scheduler,
                d_scheduler: DScheduler::new(),
                peer_worker_client: PeerWorkerClient::new(),
                d_store,
                lifetime_list_id: AtomicU64::new(0),
                lifetimes: Mutex::default(),
                lifetime_id,
                next_task_id,

                fn_name_to_addresses: Mutex::new(FnIndex::default()),

                failed_local_tasks: Mutex::default(),
                failed_remote_tasks: Mutex::default(),
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
    ) -> Runtime {
        Runtime {
            shared_runtime_state: Arc::clone(&self.shared_runtime_state),

            lifetime_id: self.shared_runtime_state.lifetime_id.load(Ordering::SeqCst),
            parent_info: Arc::new(ParentInfo {
                address: parent_address.to_string(),
                lifetime_id: parent_lifetime_id,
                task_id: parent_task_id,
            }),
            task_id: self
                .shared_runtime_state
                .next_task_id
                .fetch_add(1, Ordering::SeqCst),
            inner: Arc::default(),
        }
    }

    pub fn do_local_work<F, T, FutFn>(
        &self,
        parent_address: &str,
        parent_lifetime_id: u64,
        parent_task_id: u64,
        fn_name: &str,
        f: FutFn,
    ) -> DoWorkResponse
    where
        F: Future<Output = DResult<T>> + Send + 'static,
        T: Serialize + std::fmt::Debug + Send + Sync + 'static,
        FutFn: FnOnce(Runtime) -> F,
    {
        let runtime = self.new_child(parent_address, parent_lifetime_id, parent_task_id);
        let fut = f(runtime.clone());
        let d_store_id = runtime.do_local_work(fn_name, fut);
        DoWorkResponse {
            address: d_store_id.address,
            lifetime_id: d_store_id.lifetime_id,
            task_id: d_store_id.task_id,
            object_id: d_store_id.object_id,
        }
    }

    async fn heart_beat_forever(&self) {
        // TODO: shutdown via select.
        let sleep_for = Duration::from_secs(self.heart_beat_timeout / 3);
        loop {
            let local_lifetime_id = self.shared_runtime_state.lifetime_id.load(Ordering::SeqCst);

            let local_lifetime_list_id = self
                .shared_runtime_state
                .lifetime_list_id
                .load(Ordering::SeqCst);

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
                lifetime_list_id,
                lifetimes,
                failed_tasks,
                stats,
                ..
            } = self
                .shared_runtime_state
                .global_scheduler
                .clone()
                .heart_beat(HeartBeatRequest {
                    address: self.shared_runtime_state.local_server_address.to_string(),
                    lifetime_id: local_lifetime_id,
                    lifetime_list_id: local_lifetime_list_id,
                    failed_tasks: failed_local_tasks,
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();

            let i = worker_stats_to_index(stats);
            {
                *self
                    .shared_runtime_state
                    .fn_name_to_addresses
                    .lock()
                    .unwrap() = i;
            }

            if lifetime_list_id > local_lifetime_list_id {
                *self.shared_runtime_state.lifetimes.lock().unwrap() = lifetimes;
            }

            {
                let mut failed_remote_tasks = self
                    .shared_runtime_state
                    .failed_remote_tasks
                    .lock()
                    .unwrap();
                if *failed_remote_tasks != failed_tasks {
                    // TODO: clear d_store
                    *failed_remote_tasks = failed_tasks;
                }
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

            tokio::time::sleep(sleep_for).await;
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
    timer: Timer,
}

#[derive(Debug, Clone)]
struct ParentInfo {
    address: String,
    lifetime_id: u64,
    task_id: u64,
}

pub enum Where {
    Remote { address: String },
    Local,
}

#[derive(Debug, Clone)]
pub struct Runtime {
    shared_runtime_state: Arc<SharedRuntimeState>,

    lifetime_id: u64,
    parent_info: Arc<ParentInfo>,
    task_id: u64,
    inner: Arc<Mutex<InnerRuntime>>,
}

impl Runtime {
    fn is_valid_id(&self, d_store_id: &DStoreId) -> Option<bool> {
        self.shared_runtime_state
            .lifetimes
            .lock()
            .unwrap()
            .get(&d_store_id.address)
            .map(|v| d_store_id.lifetime_id >= *v)
    }

    fn start_timer(&self) -> Instant {
        self.inner.lock().unwrap().timer.start()
    }

    fn stop_timer(&self) {
        self.inner.lock().unwrap().timer.stop();
    }

    fn finish_local_work(&self, fn_name: &str, ret_size: usize) -> Duration {
        let elapsed = { self.inner.lock().unwrap().timer.elapsed() };

        self.shared_runtime_state
            .d_scheduler
            .finish_local_work(fn_name, elapsed, ret_size);

        elapsed
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
                        let work = if let Call::Remote { work } = v {
                            work
                        } else {
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
                    let d_store_id = self
                        .shared_runtime_state
                        .peer_worker_client
                        .do_work(&address, self.task_id, work.clone())
                        .await;
                    let t: DResult<T> = self
                        .shared_runtime_state
                        .d_store
                        .get_or_watch(d_store_id.clone())
                        .await;
                    if let Ok(t) = t {
                        let mut inner = self.inner.lock().unwrap();
                        let v: Arc<dyn ValueTrait> = Arc::new(t.clone());
                        inner
                            .calls
                            .insert(d_store_id.clone(), Call::RetriedOk { v: Arc::clone(&v) });
                        tx.send(v).unwrap();

                        return Ok(t);
                    }
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
        match &d_fut.inner {
            InnerDFut::DStore(id) => {
                self.stop_timer();

                if let Some(valid) = self.is_valid_id(&id) {
                    if !valid {
                        return self.try_retry_dfut(id).await;
                    }
                }

                let t = self
                    .shared_runtime_state
                    .d_store
                    .get_or_watch(id.clone())
                    .await;

                if let Err(_) = &t {
                    return self.try_retry_dfut(id).await;
                }

                self.start_timer();

                t
            }
            InnerDFut::Error(err) => Err(err.clone()),
        }
    }

    pub async fn cancel<T>(&self, d_fut: DFut<T>) -> DResult<()> {
        match &d_fut.inner {
            InnerDFut::DStore(id) => {
                self.stop_timer();

                if let Some(valid) = self.is_valid_id(id) {
                    if !valid {
                        self.start_timer();
                        // Ignore failure.
                        return Ok(());
                    }
                }

                self.shared_runtime_state
                    .d_store
                    .decrement_or_remove(id.clone(), 1)
                    .await?;

                self.start_timer();

                Ok(())
            }
            InnerDFut::Error(err) => Err(err.clone()),
        }
    }

    pub async fn share<T>(&self, d_fut: &DFut<T>) -> DResult<DFut<T>> {
        match &d_fut.inner {
            InnerDFut::DStore(id) => {
                self.stop_timer();

                if let Some(valid) = self.is_valid_id(id) {
                    if !valid {
                        return Err(Error::System);
                    }
                }

                self.shared_runtime_state.d_store.share(&id, 1).await?;

                self.start_timer();

                Ok(id.clone().into())
            }
            InnerDFut::Error(err) => Err(err.clone()),
        }
    }

    pub async fn share_n<T>(&self, d_fut: &DFut<T>, n: u64) -> DResult<Vec<DFut<T>>> {
        match &d_fut.inner {
            InnerDFut::DStore(id) => {
                self.stop_timer();

                if let Some(valid) = self.is_valid_id(id) {
                    if !valid {
                        return Err(Error::System);
                    }
                }

                self.shared_runtime_state.d_store.share(&id, n).await?;

                self.start_timer();

                Ok((0..n).map(|_| id.clone().into()).collect())
            }
            InnerDFut::Error(err) => Err(err.clone()),
        }
    }

    pub async fn d_box<T>(&self, t: T) -> DResult<DFut<T>>
    where
        T: Serialize + std::fmt::Debug + Send + Sync + 'static,
    {
        self.stop_timer();
        let d_store_id = self.next_d_store_id();
        self.shared_runtime_state
            .d_store
            .publish(d_store_id.clone(), t)?;
        self.start_timer();
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
        }
    }

    pub fn schedule_work(&self, fn_name: &str, arg_size: usize) -> Where {
        let local = self
            .shared_runtime_state
            .d_scheduler
            .accept_local_work(fn_name, arg_size);
        if local {
            Where::Local
        } else {
            Where::Remote {
                address: "TODO: decide peer here.".to_string(),
            }
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
                rt.start_timer();

                let t = match fut.await {
                    Ok(t) => t,
                    Err(Error::System) => {
                        let mut failed_local_tasks =
                            rt.shared_runtime_state.failed_local_tasks.lock().unwrap();
                        if rt.lifetime_id
                            == rt.shared_runtime_state.lifetime_id.load(Ordering::SeqCst)
                        {
                            failed_local_tasks.push(rt.task_id);
                        }
                        return;
                    }
                };

                let size = size_ser::to_size(&t).unwrap();
                let took = rt.finish_local_work(&fn_name, size);

                histogram!("do_local_work::duration", "fn_name" => fn_name.clone()).record(took);
                histogram!("do_local_work::size", "fn_name" => fn_name.clone()).record(size as f64);

                {
                    let failed_remote_tasks =
                        rt.shared_runtime_state.failed_remote_tasks.lock().unwrap();
                    if let Some(failed_tasks) = failed_remote_tasks.get(&rt.parent_info.address) {
                        if rt.parent_info.lifetime_id == failed_tasks.lifetime_id
                            && failed_tasks.task_id.contains(&rt.parent_info.task_id)
                        {
                            rt.shared_runtime_state
                                .failed_local_tasks
                                .lock()
                                .unwrap()
                                .push(rt.task_id);
                            return;
                        }
                    }
                }

                // TODO: check parent lifeitime id vs rt.parent_info.lifeitime

                // TODO: pass owner info to d_store to allow for parent based cleanup.
                rt.shared_runtime_state
                    .d_store
                    .publish(d_store_id, t)
                    .unwrap();
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
        self.shared_runtime_state.d_store.take_next_id(self.task_id)
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
    pub async fn do_remote_work<I, T>(&self, iw: I) -> DFut<T>
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

        // TODO: make sure this has the current runtime and track the ownership.
        let d_store_id = self
            .shared_runtime_state
            .peer_worker_client
            .do_work(&address, self.task_id, work.clone())
            .await;

        self.track_call(d_store_id.clone(), Call::Remote { work });

        d_store_id.into()
    }
}

#[derive(Debug, Default)]
struct FnIndex {
    m: HashMap<String, Vec<String>>,
}

impl FnIndex {
    fn schedule_fn(&self, fn_name: &str) -> Option<String> {
        self.m
            .get(fn_name)?
            .choose(&mut rand::thread_rng())
            .map(|s| {
                counter!("schedule_fn", "fn_name" => fn_name.to_string(), "choice" => s.clone())
                    .increment(1);
                s.clone()
            })
            .clone()
    }
}

fn worker_stats_to_index(worker_stats: HashMap<String, Stats>) -> FnIndex {
    let mut m: HashMap<String, Vec<String>> = HashMap::new();
    for (address, stats) in worker_stats {
        for fn_name in stats.fn_stats.keys() {
            m.entry(fn_name.to_string())
                .or_default()
                .push(address.to_string());
        }
    }
    FnIndex { m }
}

#[derive(Debug, Default)]
struct SharedRuntimeClientState {
    client_id: String,
    lifetime_id: u64,
    next_task_id: u64,

    fn_name_to_addresses: FnIndex,
}

// TODO: Runtime clients need to heartbeat with the global scheduler.
// They must maintain a lifetime and each real client must have a unique (address, lifetime id, task id) triple.
pub struct RuntimeClient {
    shared: Arc<Mutex<SharedRuntimeClientState>>,
    peer_worker_client: PeerWorkerClient,
    d_store_client: DStoreClient,
}

impl RuntimeClient {
    pub async fn new(global_scheduler_address: &str) -> Self {
        let shared = Arc::new(Mutex::new(SharedRuntimeClientState::default()));
        tokio::spawn({
            let global_scheduler_address = global_scheduler_address.to_string();
            let shared = Arc::clone(&shared);
            async move { Self::heart_beat(&global_scheduler_address, shared).await }
        });
        Self {
            shared,
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

    async fn heart_beat(
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
        let sleep_for = Duration::from_secs(heart_beat_timeout / 3);
        loop {
            let (client_id, lifetime_id) = {
                let shared = shared.lock().unwrap();
                (shared.client_id.clone(), shared.lifetime_id)
            };
            let HeartBeatResponse { stats, .. } = global_scheduler
                .heart_beat(HeartBeatRequest {
                    address: client_id,
                    lifetime_id,
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();

            let i = worker_stats_to_index(stats);
            {
                shared.lock().unwrap().fn_name_to_addresses = i;
            }

            tokio::time::sleep(sleep_for).await;
        }
    }

    pub async fn do_remote_work<I, T>(&self, iw: I) -> DFut<T>
    where
        I: IntoWork,
    {
        let w = iw.into_work();
        let (address, task_id) = {
            'scheduled: loop {
                {
                    let mut shared = self.shared.lock().unwrap();
                    if let Some(address) = shared.fn_name_to_addresses.schedule_fn(&w.fn_name) {
                        let task_id = shared.next_task_id;
                        shared.next_task_id += 1;
                        break 'scheduled (address, task_id);
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        };
        let d_store_id = self
            .peer_worker_client
            .clone()
            .do_work(&address, task_id, w)
            .await;
        d_store_id.into()
    }

    pub async fn wait<T>(&self, d_fut: DFut<T>) -> DResult<T>
    where
        T: Serialize + DeserializeOwned + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        match &d_fut.inner {
            InnerDFut::DStore(id) => self.d_store_client.get_or_watch(id.clone()).await,
            InnerDFut::Error(err) => Err(err.clone()),
        }
    }

    pub async fn cancel<T>(&self, d_fut: DFut<T>) -> DResult<()>
    where
        T: DeserializeOwned,
    {
        match &d_fut.inner {
            InnerDFut::DStore(id) => self.d_store_client.decrement_or_remove(id.clone(), 1).await,
            InnerDFut::Error(err) => Err(err.clone()),
        }
    }

    pub async fn share<T>(&self, d_fut: &DFut<T>) -> DResult<DFut<T>> {
        match &d_fut.inner {
            InnerDFut::DStore(id) => {
                self.d_store_client.share(id, 1).await?;
                Ok(id.clone().into())
            }
            InnerDFut::Error(err) => Err(err.clone()),
        }
    }

    pub async fn share_n<T>(&self, d_fut: &DFut<T>, n: u64) -> DResult<Vec<DFut<T>>> {
        match &d_fut.inner {
            InnerDFut::DStore(id) => {
                self.d_store_client.share(id, n).await?;
                Ok((0..n).map(|_| id.clone().into()).collect())
            }
            InnerDFut::Error(err) => Err(err.clone()),
        }
    }

    pub async fn d_box<T>(&self, _t: T) -> DResult<DFut<T>>
    where
        T: Serialize + Send + 'static,
    {
        todo!()
    }
}
