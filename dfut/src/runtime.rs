use std::collections::HashMap;
use std::future::Future;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use serde::{de::DeserializeOwned, Serialize};
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
        DStoreId,
    },
    work::{IntoWork, Work},
    Error,
};

#[derive(Debug, Clone)]
pub struct RootRuntime {
    d_scheduler: Arc<DScheduler>,
    d_store: Arc<DStore>,
    lifetimes: Arc<Mutex<HashMap<String, u64>>>,
    next_task_id: Arc<AtomicU64>,
}

impl RootRuntime {
    pub async fn new(
        local_server_address: &str,
        global_scheduler_address: &str,
        fn_names: Vec<String>,
    ) -> Self {
        let d_scheduler = Arc::new(DScheduler::new(&global_scheduler_address).await);

        let lifetime_id = d_scheduler
            .register(local_server_address, fn_names)
            .await
            .unwrap();

        let d_store = Arc::new(DStore::new(local_server_address, lifetime_id).await);

        let next_task_id = Arc::new(AtomicU64::new(0));

        Self {
            d_scheduler,
            d_store,
            lifetimes: Arc::default(),
            next_task_id,
        }
    }

    pub fn new_child(&self, remote_parent_task_id: u64) -> Runtime {
        Runtime {
            d_scheduler: Arc::clone(&self.d_scheduler),
            d_store: Arc::clone(&self.d_store),
            next_task_id: Arc::clone(&self.next_task_id),

            lifetimes: Arc::clone(&self.lifetimes),

            _parent_task_id: remote_parent_task_id,
            task_id: self.next_task_id.fetch_add(1, Ordering::SeqCst),
            _inner: Arc::default(),
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

        Server::builder()
            .add_service(
                DStoreServiceServer::new(Arc::clone(&self.d_store))
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
    // We don't need to be able to recover local calls they are considered to be owned by the
    // remote owner.
    Local {
        fn_name: String,
        d_store_id: DStoreId,
    },
    // If the remote task fails then we will recover.
    Remote {
        work: Work,
        d_store_id: DStoreId,
    },
}

#[derive(Debug, Default)]
struct InnerRuntime {
    calls: Vec<Call>,
}

#[derive(Debug, Clone)]
pub struct Runtime {
    d_scheduler: Arc<DScheduler>,
    d_store: Arc<DStore>,
    next_task_id: Arc<AtomicU64>,

    // Lifetimes is updated during heartbeats with the global scheduler.
    lifetimes: Arc<Mutex<HashMap<String, u64>>>,

    _parent_task_id: u64,
    task_id: u64,
    _inner: Arc<Mutex<InnerRuntime>>,
}

impl Runtime {
    fn is_valid_id(&self, d_store_id: &DStoreId) -> Option<bool> {
        self.lifetimes
            .lock()
            .unwrap()
            .get(&d_store_id.address)
            .map(|v| d_store_id.lifetime_id >= *v)
    }

    pub async fn wait<T>(&self, d_fut: DFut<T>) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        match &d_fut.inner {
            InnerDFut::DStore(id) => {
                if let Some(valid) = self.is_valid_id(id) {
                    if !valid {
                        // try lineage reconstruction here
                        return Err(Error::System);
                    }
                }

                let t = self.d_store.get_or_watch(id.clone()).await;
                // TODO: n lineage retries on failed wait?
                t
            }
            InnerDFut::Error(err) => return Err(err.clone()),
        }
    }

    pub async fn cancel<T>(&self, d_fut: DFut<T>) -> Result<(), Error> {
        match &d_fut.inner {
            InnerDFut::DStore(id) => {
                if let Some(valid) = self.is_valid_id(id) {
                    if !valid {
                        // Ignore failure.
                        return Ok(());
                    }
                }

                self.d_store.decrement_or_remove(id.clone(), 1).await?;
                Ok(())
            }
            InnerDFut::Error(err) => Err(err.clone()),
        }
    }

    pub async fn share<T>(&self, d_fut: &DFut<T>) -> Result<DFut<T>, Error> {
        match &d_fut.inner {
            InnerDFut::DStore(id) => {
                if let Some(valid) = self.is_valid_id(id) {
                    if !valid {
                        return Err(Error::System);
                    }
                }

                self.d_store.share(&id, 1).await?;
                Ok(id.clone().into())
            }
            InnerDFut::Error(err) => Err(err.clone()),
        }
    }

    pub async fn share_n<T>(&self, d_fut: &DFut<T>, n: u64) -> Result<Vec<DFut<T>>, Error> {
        match &d_fut.inner {
            InnerDFut::DStore(id) => {
                if let Some(valid) = self.is_valid_id(id) {
                    if !valid {
                        return Err(Error::System);
                    }
                }

                self.d_store.share(&id, n).await?;
                Ok((0..n).map(|_| id.clone().into()).collect())
            }
            InnerDFut::Error(err) => Err(err.clone()),
        }
    }

    pub async fn d_box<T>(&self, t: &T) -> Result<DFut<T>, Error>
    where
        T: Serialize + Send + 'static,
    {
        let d_store_id = self.next_d_store_id();
        self.d_store.publish(d_store_id.clone(), t)?;
        Ok(d_store_id.into())
    }
}

impl Runtime {
    pub fn new_child(&self) -> Runtime {
        Runtime {
            d_scheduler: Arc::clone(&self.d_scheduler),
            d_store: Arc::clone(&self.d_store),
            next_task_id: Arc::clone(&self.next_task_id),

            lifetimes: Arc::clone(&self.lifetimes),

            _parent_task_id: self.task_id,
            task_id: self.next_task_id.fetch_add(1, Ordering::SeqCst),
            _inner: Arc::default(),
        }
    }

    pub fn accept_local_work(&self, fn_name: &str) -> bool {
        self.d_scheduler.accept_local_work(fn_name)
    }

    // Can we use https://docs.rs/tokio-metrics/0.3.1/tokio_metrics/ to make decisions?
    //
    // We return a DStoreId so that we can just pass it over the network.
    //
    fn do_local_work<F, T>(&self, fn_name: &str, fut: F) -> DStoreId
    where
        F: Future<Output = T> + Send + 'static,
        T: Serialize + Send + Sync + 'static,
    {
        let d_store_id = self.next_d_store_id();

        self.track_call(Call::Local {
            fn_name: fn_name.to_string(),
            d_store_id: d_store_id.clone(),
        });

        tokio::spawn({
            let rt = self.clone();
            let d_store_id = d_store_id.clone();
            let fn_name = fn_name.to_string();
            async move {
                let token = rt.d_scheduler.start_local_work();

                let t = fut.await;
                rt.d_store.publish(d_store_id, &t).unwrap();

                rt.d_scheduler.finish_local_work(token, &fn_name);
            }
        });

        d_store_id
    }

    pub fn do_local_work_dwr<F, T>(&self, fn_name: &str, fut: F) -> DoWorkResponse
    where
        F: Future<Output = T> + Send + 'static,
        T: Serialize + Send + Sync + 'static,
    {
        let d_store_id = self.do_local_work(fn_name, fut);

        d_store_id.into()
    }

    pub fn do_local_work_fut<F, T>(&self, fn_name: &str, fut: F) -> DFut<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Serialize + Send + Sync + 'static,
    {
        let d_store_id = self.do_local_work(fn_name, fut);

        d_store_id.into()
    }

    fn track_call(&self, call: Call) {
        self._inner.lock().unwrap().calls.push(call);
    }

    fn next_d_store_id(&self) -> DStoreId {
        self.d_store.take_next_id(self.task_id)
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

        let d_store_id = self.d_scheduler.schedule(self.task_id, work.clone()).await;

        self.track_call(Call::Remote {
            work,
            d_store_id: d_store_id.clone(),
        });

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
        T: DeserializeOwned,
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

    pub async fn d_box<T>(&self, _t: &T) -> Result<DFut<T>, Error>
    where
        T: Serialize + Send + 'static,
    {
        todo!()
    }
}
