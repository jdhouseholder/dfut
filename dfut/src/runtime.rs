use std::collections::{hash_map::Entry, HashMap};
use std::future::Future;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tonic::transport::Server;

use crate::{
    d_fut::{DFut, InnerDFut},
    d_scheduler::{
        worker_service::{
            worker_service_server::WorkerService, worker_service_server::WorkerServiceServer,
            DoWorkResponse,
        },
        DScheduler, DSchedulerClient,
    },
    d_store::{
        d_store_service::d_store_service_server::DStoreServiceServer, DStore, DStoreClient,
        DStoreId, ValueTrait,
    },
    global_scheduler::global_scheduler_service::{HeartBeatResponse, RegisterResponse},
    timer::Timer,
    work::{IntoWork, Work},
    Error,
};

const DFUT_RETRIES: usize = 3;

#[derive(Debug)]
struct SharedRuntimeState {
    local_server_address: String,
    dfut_retries: usize,

    d_scheduler: DScheduler,
    d_store: Arc<DStore>,
    lifetime_list_id: AtomicU64,
    lifetimes: Mutex<HashMap<String, u64>>,
    lifetime_id: Arc<AtomicU64>,
    next_task_id: AtomicU64,
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
        let d_scheduler = DScheduler::new(&global_scheduler_address).await;

        let RegisterResponse {
            lifetime_id,
            heart_beat_timeout,
            ..
        } = d_scheduler
            .register(local_server_address, fn_names)
            .await
            .unwrap();

        let lifetime_id = Arc::new(AtomicU64::new(lifetime_id));
        let next_task_id = AtomicU64::new(0);

        let d_store = Arc::new(DStore::new(local_server_address, &lifetime_id).await);

        Self {
            shared_runtime_state: Arc::new(SharedRuntimeState {
                local_server_address: local_server_address.to_string(),
                // TODO: pass in through config
                dfut_retries: DFUT_RETRIES,

                d_scheduler,
                d_store,
                lifetime_list_id: AtomicU64::new(0),
                lifetimes: Mutex::default(),
                lifetime_id,
                next_task_id,
            }),

            heart_beat_timeout,
        }
    }

    pub fn new_child(&self, remote_parent_task_id: u64) -> Runtime {
        Runtime {
            shared_runtime_state: Arc::clone(&self.shared_runtime_state),

            lifetime_id: self.shared_runtime_state.lifetime_id.load(Ordering::SeqCst),
            _parent_task_id: remote_parent_task_id,
            task_id: self
                .shared_runtime_state
                .next_task_id
                .fetch_add(1, Ordering::SeqCst),
            inner: Arc::default(),
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

            // TODO: retry & graceful fail.
            let HeartBeatResponse {
                lifetime_id,
                lifetime_list_id,
                lifetimes,
                ..
            } = self
                .shared_runtime_state
                .d_scheduler
                .heart_beat(
                    &self.shared_runtime_state.local_server_address,
                    local_lifetime_id,
                    local_lifetime_list_id,
                )
                .await
                .unwrap();

            if lifetime_list_id > local_lifetime_list_id {
                *self.shared_runtime_state.lifetimes.lock().unwrap() = lifetimes
                    .into_iter()
                    .map(|l| (l.address, l.lifetime_id))
                    .collect();
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

        tokio::spawn(async move {
            self.heart_beat_forever().await;
        });

        Server::builder()
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
            .serve(address)
            .await
            .unwrap();
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
    Err,
}

#[derive(Debug, Default)]
struct InnerRuntime {
    calls: HashMap<DStoreId, Call>,
    timer: Timer,
}

#[derive(Debug, Clone)]
pub struct Runtime {
    shared_runtime_state: Arc<SharedRuntimeState>,

    lifetime_id: u64,
    _parent_task_id: u64,
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

    fn start_timer(&self) {
        self.inner.lock().unwrap().timer.start();
    }

    fn stop_timer(&self) {
        self.inner.lock().unwrap().timer.stop();
    }

    fn stop_timer_and_finalize(&self, fn_name: &str) {
        let elapsed = {
            let mut inner = self.inner.lock().unwrap();
            inner.timer.stop();
            inner.timer.elapsed()
        };

        self.shared_runtime_state
            .d_scheduler
            .finish_local_work(fn_name, elapsed);
    }

    async fn try_retry_dfut<T>(&self, d_store_id: &DStoreId) -> Result<T, Error>
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
                    Call::Local { .. } | Call::Err => return Err(Error::System),
                },
                Entry::Vacant(_) => unreachable!(),
            }
        };
        match r {
            RetryState::Sender(work, tx) => {
                for _ in 0..self.shared_runtime_state.dfut_retries {
                    let d_store_id = self
                        .shared_runtime_state
                        .d_scheduler
                        .schedule(self.task_id, work.clone())
                        .await;
                    let t: Result<T, Error> = self
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
                inner.calls.insert(d_store_id.clone(), Call::Err);

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

    pub async fn wait<T>(&self, d_fut: DFut<T>) -> Result<T, Error>
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
            InnerDFut::Error(err) => return Err(err.clone()),
        }
    }

    pub async fn cancel<T>(&self, d_fut: DFut<T>) -> Result<(), Error> {
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

    pub async fn share<T>(&self, d_fut: &DFut<T>) -> Result<DFut<T>, Error> {
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

    pub async fn share_n<T>(&self, d_fut: &DFut<T>, n: u64) -> Result<Vec<DFut<T>>, Error> {
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

    pub async fn d_box<T>(&self, t: T) -> Result<DFut<T>, Error>
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
    pub fn new_child(&self) -> Runtime {
        Runtime {
            shared_runtime_state: Arc::clone(&self.shared_runtime_state),

            lifetime_id: self.lifetime_id,
            _parent_task_id: self.task_id,
            task_id: self
                .shared_runtime_state
                .next_task_id
                .fetch_add(1, Ordering::SeqCst),
            inner: Arc::default(),
        }
    }

    pub fn accept_local_work(&self, fn_name: &str) -> bool {
        self.shared_runtime_state
            .d_scheduler
            .accept_local_work(fn_name)
    }

    // Can we use https://docs.rs/tokio-metrics/0.3.1/tokio_metrics/ to make decisions?
    //
    // We return a DStoreId so that we can just pass it over the network.
    //
    fn do_local_work<F, T>(&self, fn_name: &str, fut: F) -> DStoreId
    where
        F: Future<Output = T> + Send + 'static,
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

                let t = fut.await;

                rt.stop_timer_and_finalize(&fn_name);

                rt.shared_runtime_state
                    .d_store
                    .publish(d_store_id, t)
                    .unwrap();
            }
        });

        d_store_id
    }

    pub fn do_local_work_dwr<F, T>(&self, fn_name: &str, fut: F) -> DoWorkResponse
    where
        F: Future<Output = T> + Send + 'static,
        T: Serialize + std::fmt::Debug + Send + Sync + 'static,
    {
        let d_store_id = self.do_local_work(fn_name, fut);

        d_store_id.into()
    }

    pub fn do_local_work_fut<F, T>(&self, fn_name: &str, fut: F) -> DFut<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Serialize + std::fmt::Debug + Send + Sync + 'static,
    {
        let d_store_id = self.do_local_work(fn_name, fut);

        d_store_id.into()
    }

    fn track_call(&self, d_store_id: DStoreId, call: Call) {
        self.inner.lock().unwrap().calls.insert(d_store_id, call);
    }

    fn next_d_store_id(&self) -> DStoreId {
        self.shared_runtime_state.d_store.take_next_id(self.task_id)
    }

    // TODO: just generate this for each fn, that way we don't have to use Work
    // for local computation. We only use it for remote computation.
    //
    // Put work into local queue or remote queue.
    pub async fn do_remote_work<I, T>(&self, iw: I) -> DFut<T>
    where
        I: IntoWork,
    {
        let work = iw.into_work();

        let d_store_id = self
            .shared_runtime_state
            .d_scheduler
            .schedule(self.task_id, work.clone())
            .await;

        self.track_call(d_store_id.clone(), Call::Remote { work });

        d_store_id.into()
    }
}

pub struct RuntimeClient {
    d_scheduler_client: DSchedulerClient,
    d_store_client: DStoreClient,
}

impl RuntimeClient {
    pub async fn new(global_scheduler_address: &str) -> Self {
        Self {
            d_scheduler_client: DSchedulerClient::new(global_scheduler_address).await,
            d_store_client: DStoreClient::new(),
        }
    }

    pub async fn do_remote_work<I, T>(&self, iw: I) -> DFut<T>
    where
        I: IntoWork,
    {
        let w = iw.into_work();
        let d_store_id = self.d_scheduler_client.schedule(0, w).await;
        d_store_id.into()
    }

    pub async fn wait<T>(&self, d_fut: DFut<T>) -> Result<T, Error>
    where
        T: Serialize + DeserializeOwned + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        match &d_fut.inner {
            InnerDFut::DStore(id) => self.d_store_client.get_or_watch(id.clone()).await,
            InnerDFut::Error(err) => return Err(err.clone()),
        }
    }

    pub async fn cancel<T>(&self, d_fut: DFut<T>) -> Result<(), Error>
    where
        T: DeserializeOwned,
    {
        match &d_fut.inner {
            InnerDFut::DStore(id) => self.d_store_client.decrement_or_remove(id.clone(), 1).await,
            InnerDFut::Error(err) => return Err(err.clone()),
        }
    }

    pub async fn share<T>(&self, d_fut: &DFut<T>) -> Result<DFut<T>, Error> {
        match &d_fut.inner {
            InnerDFut::DStore(id) => {
                self.d_store_client.share(id, 1).await?;
                Ok(id.clone().into())
            }
            InnerDFut::Error(err) => Err(err.clone()),
        }
    }

    pub async fn share_n<T>(&self, d_fut: &DFut<T>, n: u64) -> Result<Vec<DFut<T>>, Error> {
        match &d_fut.inner {
            InnerDFut::DStore(id) => {
                self.d_store_client.share(id, n).await?;
                Ok((0..n).map(|_| id.clone().into()).collect())
            }
            InnerDFut::Error(err) => Err(err.clone()),
        }
    }

    pub async fn d_box<T>(&self, _t: T) -> Result<DFut<T>, Error>
    where
        T: Serialize + Send + 'static,
    {
        todo!()
    }
}
