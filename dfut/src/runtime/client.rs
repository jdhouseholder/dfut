use std::collections::{hash_map::Entry, HashMap};
use std::sync::{Arc, Mutex};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

use crate::{
    consts::DFUT_RETRIES,
    d_fut::DFut,
    d_scheduler::DScheduler,
    d_store::{DStoreClient, DStoreId, ValueTrait},
    peer_work::PeerWorkerClient,
    retry::retry,
    rpc_context::RpcContext,
    seq::Seq,
    services::{
        global_scheduler_service::{
            global_scheduler_service_client::GlobalSchedulerServiceClient,
            heart_beat_response::HeartBeatResponseType, BadRequest, HeartBeat, HeartBeatRequest,
            HeartBeatResponse, NotLeader, RequestId, RuntimeInfo,
        },
        worker_service::ParentInfo,
    },
    sleep::sleep_with_jitter,
    work::{IntoWork, Work},
    DResult, Error,
};

#[derive(Debug, Default)]
struct SharedRuntimeClientState {
    lifetime_id: u64,
    next_task_id: u64,

    d_scheduler: DScheduler,
}

pub struct RootRuntimeClient {
    cancellation_token: CancellationToken,

    client_id: Arc<String>,
    shared: Arc<Mutex<SharedRuntimeClientState>>,
    next_request_id: Arc<Mutex<Seq>>,
    peer_worker_client: PeerWorkerClient,
    d_store_client: DStoreClient,
}

impl RootRuntimeClient {
    pub async fn new(global_scheduler_address: &str, unique_client_id: &str) -> Self {
        let shared = Arc::new(Mutex::new(SharedRuntimeClientState::default()));

        let cancellation_token = CancellationToken::new();

        let endpoint: Endpoint = global_scheduler_address.parse().unwrap();
        let channel = endpoint.connect_lazy();
        let mut client = GlobalSchedulerServiceClient::new(channel);

        let mut request_id = 0u64;
        let mut heart_beat_timeout = 0u64;

        while !Self::heart_beat_once(
            &mut client,
            &mut request_id,
            &mut heart_beat_timeout,
            unique_client_id,
            &shared,
        )
        .await
        {
            sleep_with_jitter(heart_beat_timeout / 3).await;
        }

        tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            let shared = Arc::clone(&shared);
            let client_id = unique_client_id.to_string();

            async move {
                let fut = Self::heart_beat_forever(
                    &mut client,
                    &mut request_id,
                    &mut heart_beat_timeout,
                    client_id,
                    shared,
                );

                tokio::select! {
                    _ = fut => {},
                    _ = cancellation_token.cancelled() => {},
                };
            }
        });

        Self {
            cancellation_token,

            client_id: Arc::new(unique_client_id.to_string()),
            shared,
            next_request_id: Arc::default(),
            peer_worker_client: PeerWorkerClient::new(),
            d_store_client: DStoreClient::new(),
        }
    }

    pub async fn shutdown(self) {
        self.cancellation_token.cancel();
    }

    async fn heart_beat_once(
        client: &mut GlobalSchedulerServiceClient<Channel>,
        request_id: &mut u64,
        next_heart_beat_timeout: &mut u64,
        client_id: &str,
        shared: &Arc<Mutex<SharedRuntimeClientState>>,
    ) -> bool {
        let lifetime_id = {
            let shared = shared.lock().unwrap();
            shared.lifetime_id
        };

        tracing::trace!("{client_id}: Attempting to HeartBeat");

        let HeartBeatResponse {
            heart_beat_response_type,
        } = retry(client, |mut client| {
            let request_id = *request_id;
            let client_id = client_id.to_string();
            async move {
                client
                    .heart_beat(HeartBeatRequest {
                        request_id,
                        address: client_id,
                        current_runtime_info: Some(RuntimeInfo {
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
                shared.lock().unwrap().lifetime_id = lifetime_id;
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

        tracing::trace!("{client_id}: Good HeartBeat");

        *request_id = next_expected_request_id;
        *next_heart_beat_timeout = heart_beat_timeout;

        {
            let mut shared = shared.lock().unwrap();

            if shared.lifetime_id != lifetime_id {
                shared.lifetime_id = lifetime_id;
                tracing::trace!("client: set lifetime_id to {lifetime_id}");
            }

            shared.d_scheduler.update("", &address_to_runtime_info);
        }

        true
    }

    async fn heart_beat_forever(
        client: &mut GlobalSchedulerServiceClient<Channel>,
        request_id: &mut u64,
        heart_beat_timeout: &mut u64,
        client_id: String,
        shared: Arc<Mutex<SharedRuntimeClientState>>,
    ) {
        loop {
            Self::heart_beat_once(client, request_id, heart_beat_timeout, &client_id, &shared)
                .await;
            sleep_with_jitter(*heart_beat_timeout / 3).await;
        }
    }

    pub fn new_runtime_client(&self) -> RuntimeClient {
        let mut shared = self.shared.lock().unwrap();

        let lifetime_id = shared.lifetime_id;
        let task_id = shared.next_task_id;
        shared.next_task_id += 1;

        RuntimeClient {
            client_id: Arc::clone(&self.client_id),
            shared: Arc::clone(&self.shared),
            next_request_id: Arc::clone(&self.next_request_id),
            peer_worker_client: self.peer_worker_client.clone(),
            d_store_client: self.d_store_client.clone(),

            lifetime_id,
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
    client_id: Arc<String>,
    shared: Arc<Mutex<SharedRuntimeClientState>>,
    next_request_id: Arc<Mutex<Seq>>,
    peer_worker_client: PeerWorkerClient,
    d_store_client: DStoreClient,

    lifetime_id: u64,
    task_id: u64,

    calls: Mutex<HashMap<DStoreId, ClientCall>>,
    requests: Arc<Mutex<Vec<RequestId>>>,
    dfut_retries: usize,
}

impl RuntimeClient {
    fn rpc_context(&self) -> RpcContext {
        let shared = self.shared.lock().unwrap();
        RpcContext {
            local_address: (*self.client_id).clone(),
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

    #[tracing::instrument(skip_all)]
    pub async fn do_remote_work<I, T>(&self, iw: I) -> DResult<DFut<T>>
    where
        I: IntoWork,
    {
        let w = iw.into_work();
        // TODO: fail if we don't eventually schedule?
        let (address, lifetime_id) = {
            'scheduled: loop {
                {
                    let shared = self.shared.lock().unwrap();
                    if let Some(address) = shared.d_scheduler.schedule_fn(&w.fn_name) {
                        let lifetime_id = shared.lifetime_id;
                        break 'scheduled (address, lifetime_id);
                    }
                }
            }
        };

        tracing::info!("scheduled {} for {} {}", w.fn_name, address, lifetime_id);

        let request_id = self.next_request_id(&address);

        let d_store_id = self
            .peer_worker_client
            .do_work(
                &[ParentInfo {
                    address: (*self.client_id).clone(),
                    lifetime_id,
                    task_id: self.task_id,
                    request_id: Some(request_id),
                }],
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

        tracing::error!("retrying");

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
                    ClientCall::RetriedErr => {
                        tracing::error!("already retried");
                        return Err(Error::System);
                    }
                },
                Entry::Vacant(_) => unreachable!(),
            }
        };
        match r {
            RetryState::Sender(work, tx) => {
                for _ in 0..self.dfut_retries {
                    let address = self
                        .shared
                        .lock()
                        .unwrap()
                        .d_scheduler
                        .schedule_fn(&work.fn_name)
                        .ok_or(Error::System)?;

                    let request_id = self.next_request_id(&address);

                    let parent_info = ParentInfo {
                        address: (*self.client_id).clone(),
                        lifetime_id: self.lifetime_id,
                        task_id: self.task_id,
                        request_id: Some(request_id),
                    };

                    let d_store_id = self
                        .peer_worker_client
                        .do_work(&[parent_info.clone()], &address, work.clone())
                        .await?;

                    tracing::error!("retrying {:?} new={:?}", parent_info, d_store_id);

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

    #[tracing::instrument(skip_all)]
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
            Err(e) => {
                tracing::error!("wait: {e:?}");
                self.try_retry_dfut(&d_fut.d_store_id).await
            }
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn cancel<T>(&self, d_fut: DFut<T>) -> DResult<()>
    where
        T: DeserializeOwned,
    {
        self.d_store_client
            .decrement_or_remove(&self.rpc_context(), d_fut.d_store_id, 1)
            .await
    }

    #[tracing::instrument(skip_all)]
    pub async fn share<T>(&self, d_fut: &DFut<T>) -> DResult<DFut<T>> {
        self.d_store_client
            .share(&self.rpc_context(), &d_fut.d_store_id, 1)
            .await?;
        Ok(d_fut.share())
    }

    #[tracing::instrument(skip_all)]
    pub async fn share_n<T>(&self, d_fut: &DFut<T>, n: u64) -> DResult<Vec<DFut<T>>> {
        self.d_store_client
            .share(&self.rpc_context(), &d_fut.d_store_id, n)
            .await?;
        Ok((0..n).map(|_| d_fut.share()).collect())
    }

    #[tracing::instrument(skip_all)]
    pub async fn d_box<T>(&self, _t: T) -> DResult<DFut<T>>
    where
        T: Serialize + Send + 'static,
    {
        todo!()
    }
}
