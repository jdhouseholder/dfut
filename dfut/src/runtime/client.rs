use std::collections::{hash_map::Entry, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tonic::transport::{Channel, Endpoint};

use crate::{
    consts::DFUT_RETRIES,
    d_fut::DFut,
    d_store::{DStoreClient, DStoreId, ValueTrait},
    fn_index::FnIndex,
    peer_work::PeerWorkerClient,
    retry::retry,
    rpc_context::RpcContext,
    seq::Seq,
    services::global_scheduler_service::{
        global_scheduler_service_client::GlobalSchedulerServiceClient, CurrentRuntimeInfo,
        HeartBeatRequest, HeartBeatResponse, RegisterRequest, RegisterResponse, RequestId,
    },
    sleep::sleep_with_jitter,
    work::{IntoWork, Work},
    DResult, Error,
};

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
    pub async fn new(global_scheduler_address: &str, unique_client_id: &str) -> Self {
        let shared = Arc::new(Mutex::new(SharedRuntimeClientState::default()));

        let endpoint: Endpoint = global_scheduler_address.parse().unwrap();

        let mut client: Option<GlobalSchedulerServiceClient<Channel>> = None;

        let RegisterResponse {
            lifetime_id,
            heart_beat_timeout,
            ..
        } = retry(&mut client, &endpoint, |mut client| {
            let address = unique_client_id.to_string();
            async move {
                client
                    .register(RegisterRequest {
                        address,
                        ..Default::default()
                    })
                    .await
            }
        })
        .await
        .expect("Unable to register.");

        {
            let mut shared = shared.lock().unwrap();
            shared.client_id = unique_client_id.to_string();
            shared.lifetime_id = lifetime_id;
        }

        tokio::spawn({
            let shared = Arc::clone(&shared);
            async move { Self::heart_beat_forever(client, endpoint, shared, heart_beat_timeout).await }
        });

        Self {
            shared,
            next_request_id: Arc::default(),
            peer_worker_client: PeerWorkerClient::new(),
            d_store_client: DStoreClient::new(),
        }
    }

    async fn heart_beat_forever(
        mut client: Option<GlobalSchedulerServiceClient<Channel>>,
        endpoint: Endpoint,
        shared: Arc<Mutex<SharedRuntimeClientState>>,
        heart_beat_timeout: u64,
    ) {
        let sleep_for = heart_beat_timeout / 3;
        let mut request_id = 0u64;
        loop {
            let (client_id, lifetime_id) = {
                let shared = shared.lock().unwrap();
                (shared.client_id.clone(), shared.lifetime_id)
            };

            let HeartBeatResponse {
                lifetime_id,
                address_to_runtime_info,
                ..
            } = retry(&mut client, &endpoint, |mut client| {
                let client_id = client_id.clone();
                async move {
                    client
                        .heart_beat(HeartBeatRequest {
                            request_id,
                            address: client_id,
                            current_runtime_info: Some(CurrentRuntimeInfo {
                                lifetime_id,
                                // TODO: share failed local tasks.
                                ..Default::default()
                            }),
                        })
                        .await
                }
            })
            .await
            .unwrap();

            request_id += 1;

            let i = FnIndex::compute(&address_to_runtime_info, "");
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
