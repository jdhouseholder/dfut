// TODO: Alternatively we can mux onto one rpc client.
use std::collections::{hash_map::Entry as HashMapEntry, HashMap};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

// TODO: Feature flag lru.
use lru::LruCache;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::broadcast;
use tonic::{transport::Endpoint, Request, Response, Status};

use crate::{d_scheduler::worker_service::DoWorkResponse, Error};

pub(crate) mod d_store_service {
    tonic::include_proto!("d_store_service");
}

use d_store_service::{
    d_store_service_client::DStoreServiceClient, d_store_service_server::DStoreService,
    DecrementOrRemoveRequest, DecrementOrRemoveResponse, GetOrWatchRequest, GetOrWatchResponse,
    ShareNRequest, ShareNResponse,
};

#[derive(Debug, std::hash::Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Clone)]
pub struct DStoreId {
    pub(crate) address: String,
    pub(crate) lifetime_id: u64,
    pub(crate) task_id: u64,
    pub(crate) object_id: u64,
}

impl Into<DoWorkResponse> for DStoreId {
    fn into(self) -> DoWorkResponse {
        DoWorkResponse {
            address: self.address,
            lifetime_id: self.lifetime_id,
            task_id: self.task_id,
            object_id: self.object_id,
        }
    }
}

#[derive(Debug)]
enum Entry {
    Watch {
        tx: broadcast::Sender<Vec<u8>>,
        subscribers: u64,
        ref_count: u64,
    },
    DBlob {
        b: Vec<u8>,
        ref_count: u64,
    },
}

#[derive(Debug, Default)]
struct LocalStore {
    blob_store: Mutex<HashMap<DStoreId, Entry>>,
}

// is the local store simple iff we require an extra call to decrement ref?
// Or do we have to guarantee one req in flight per peer?
impl LocalStore {
    fn insert(&self, key: DStoreId, b: Vec<u8>) {
        let mut m = self.blob_store.lock().unwrap();

        let mut new_ref_count = 1;
        if let Some(Entry::Watch {
            tx,
            subscribers,
            ref_count,
        }) = m.remove(&key)
        {
            // subscribers must be less than or equal to ref_count.
            if subscribers == ref_count {
                // All refs are currently subscribed. We can't have any new gets or shares.
                tx.send(b).unwrap();
                return;
            } else {
                // There are still outstanding refs, we need to store.
                tx.send(b.clone()).unwrap();
                new_ref_count = ref_count - subscribers;
            }
        }
        m.insert(
            key,
            Entry::DBlob {
                b,
                ref_count: new_ref_count,
            },
        );
    }

    async fn get_or_watch(&self, key: DStoreId) -> Vec<u8> {
        let mut rx = {
            let mut m = self.blob_store.lock().unwrap();
            let rx = match m.entry(key) {
                HashMapEntry::Vacant(v) => {
                    let (tx, rx) = broadcast::channel(1);
                    v.insert(Entry::Watch {
                        tx,
                        subscribers: 0,
                        ref_count: 1,
                    });
                    rx
                }
                HashMapEntry::Occupied(mut o) => {
                    let mut remove = false;
                    match o.get_mut() {
                        Entry::Watch { subscribers, .. } => {
                            *subscribers += 1;
                        }
                        Entry::DBlob { b, ref_count } => {
                            *ref_count -= 1;
                            if *ref_count == 0 {
                                remove = true;
                            } else {
                                return b.clone();
                            }
                        }
                    }
                    if remove {
                        match o.remove() {
                            Entry::DBlob { b, .. } => return b,
                            _ => panic!(),
                        }
                    }
                    match o.get() {
                        Entry::Watch { tx, .. } => tx.subscribe(),
                        _ => panic!(),
                    }
                }
            };
            drop(m);
            rx
        };

        rx.recv().await.unwrap()
    }

    async fn share(&self, key: &DStoreId, n: u64) -> Result<(), Error> {
        let mut m = self.blob_store.lock().unwrap();
        match m.get_mut(&key).unwrap() {
            Entry::Watch { ref_count, .. } => {
                *ref_count += n;
            }
            Entry::DBlob { ref_count, .. } => {
                *ref_count += n;
            }
        }
        Ok(())
    }

    async fn decrement_or_remove(&self, key: &DStoreId, _by: u64) -> bool {
        let mut m = self.blob_store.lock().unwrap();
        let mut remove = false;

        match m.get_mut(&key).unwrap() {
            Entry::Watch { ref_count, .. } => {
                *ref_count -= 1;
                if *ref_count == 0 {
                    remove = true;
                }
            }
            Entry::DBlob { ref_count, .. } => {
                *ref_count -= 1;
                if *ref_count == 0 {
                    remove = true;
                }
            }
        }

        if remove {
            m.remove(key);
        }

        remove
    }
}

#[derive(Debug, Default)]
pub(crate) struct DStore {
    current_address: String,
    lifetime_id: u64,
    next_object_id: AtomicU64,

    local_store: Arc<LocalStore>,
    d_store_client: DStoreClient,
}

impl DStore {
    pub(crate) async fn new(current_address: &str, lifetime_id: u64) -> Self {
        Self {
            current_address: current_address.to_string(),
            lifetime_id,
            next_object_id: AtomicU64::new(0),

            local_store: Default::default(),
            d_store_client: DStoreClient::default(),
        }
    }

    pub(crate) fn take_next_id(&self, task_id: u64) -> DStoreId {
        DStoreId {
            address: self.current_address.clone(),
            lifetime_id: self.lifetime_id,
            task_id,
            object_id: self.next_object_id.fetch_add(1, Ordering::SeqCst),
        }
    }

    pub(crate) fn publish<T>(&self, key: DStoreId, t: &T) -> Result<(), Error>
    where
        T: Serialize + Send + 'static,
    {
        let b = serde_json::to_vec(t).unwrap();
        self.local_store.insert(key, b);
        Ok(())
    }

    pub(crate) async fn get_or_watch<T>(&self, key: DStoreId) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        if let Some(b) = {
            let mut lru = self.d_store_client.lru.lock().unwrap();
            lru.get(&key).map(|v| v.to_vec())
        } {
            // MUST decrement remote. We can omit the object transfer.
            self.d_store_client.decrement_or_remove(key, 1).await?;
            return Ok(serde_json::from_slice(&b).unwrap());
        }

        if key.address != self.current_address {
            self.d_store_client.get_or_watch(key).await
        } else {
            let b = self.local_store.get_or_watch(key.clone()).await;
            Ok(serde_json::from_slice(&b).unwrap())
        }
    }

    pub(crate) async fn share(&self, key: &DStoreId, n: u64) -> Result<(), Error> {
        if key.address != self.current_address {
            return self.d_store_client.share(key, n).await;
        }

        self.local_store.share(key, n).await?;

        Ok(())
    }

    pub(crate) async fn decrement_or_remove(&self, key: DStoreId, by: u64) -> Result<(), Error> {
        if key.address != self.current_address {
            return self.d_store_client.decrement_or_remove(key, by).await;
        }

        self.local_store.decrement_or_remove(&key, by).await;

        Ok(())
    }
}

#[derive(Debug)]
pub struct DStoreClient {
    // TODO: LRU cache + in flight tracking goes here.
    lru: Arc<Mutex<LruCache<DStoreId, Vec<u8>>>>,
}

impl Default for DStoreClient {
    fn default() -> Self {
        Self {
            lru: Arc::new(Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(100).unwrap(),
            ))),
        }
    }
}

impl DStoreClient {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn get_or_watch<T>(&self, key: DStoreId) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        let endpoint: Endpoint = key.address.parse().unwrap();
        let mut client = DStoreServiceClient::connect(endpoint)
            .await
            .unwrap()
            .max_encoding_message_size(usize::MAX)
            .max_decoding_message_size(usize::MAX);
        let GetOrWatchResponse { object } = client
            .get_or_watch(GetOrWatchRequest {
                address: key.address.clone(),
                lifetime_id: key.lifetime_id,
                task_id: key.task_id,
                object_id: key.object_id,
            })
            .await
            .unwrap()
            .into_inner();

        // TODO: We can avoid this if we know if the object has been deallocated.
        self.lru.lock().unwrap().put(key, object.clone());

        return Ok(serde_json::from_slice(&object).unwrap());
    }

    pub(crate) async fn share(&self, key: &DStoreId, n: u64) -> Result<(), Error> {
        let endpoint: Endpoint = key.address.parse().unwrap();
        let mut client = DStoreServiceClient::connect(endpoint).await.unwrap();
        let _ = client
            .share_n(ShareNRequest {
                address: key.address.clone(),
                lifetime_id: key.lifetime_id,
                task_id: key.task_id,
                object_id: key.object_id,
                n,
            })
            .await
            .unwrap();
        return Ok(());
    }

    pub(crate) async fn decrement_or_remove(&self, key: DStoreId, by: u64) -> Result<(), Error> {
        let endpoint: Endpoint = key.address.parse().unwrap();
        let mut client = DStoreServiceClient::connect(endpoint).await.unwrap();
        let _ = client
            .decrement_or_remove(DecrementOrRemoveRequest {
                address: key.address,
                lifetime_id: key.lifetime_id,
                task_id: key.task_id,
                object_id: key.object_id,
                by,
            })
            .await
            .unwrap();
        return Ok(());
    }
}

#[tonic::async_trait]
impl DStoreService for Arc<DStore> {
    async fn get_or_watch(
        &self,
        request: Request<GetOrWatchRequest>,
    ) -> Result<Response<GetOrWatchResponse>, Status> {
        let GetOrWatchRequest {
            address,
            lifetime_id,
            task_id,
            object_id,
        } = request.into_inner();

        if lifetime_id < self.lifetime_id {
            return Err(Status::not_found("Lifetime too old".to_string()));
        }

        let id = DStoreId {
            address,
            lifetime_id,
            task_id,
            object_id,
        };

        let b = self.local_store.get_or_watch(id.clone()).await;

        Ok(Response::new(GetOrWatchResponse { object: b }))
    }

    async fn share_n(
        &self,
        request: Request<ShareNRequest>,
    ) -> Result<Response<ShareNResponse>, Status> {
        let ShareNRequest {
            address,
            lifetime_id,
            task_id,
            object_id,
            n,
        } = request.into_inner();

        if lifetime_id < self.lifetime_id {
            return Err(Status::not_found("Lifetime too old".to_string()));
        }

        let id = DStoreId {
            address,
            lifetime_id,
            task_id,
            object_id,
        };

        self.local_store.share(&id, n).await.unwrap();

        Ok(Response::new(ShareNResponse::default()))
    }

    async fn decrement_or_remove(
        &self,
        request: Request<DecrementOrRemoveRequest>,
    ) -> Result<Response<DecrementOrRemoveResponse>, Status> {
        let DecrementOrRemoveRequest {
            address,
            lifetime_id,
            task_id,
            object_id,
            by,
        } = request.into_inner();

        if lifetime_id < self.lifetime_id {
            return Err(Status::not_found("Lifetime too old".to_string()));
        }

        let removed = self
            .local_store
            .decrement_or_remove(
                &DStoreId {
                    address,
                    lifetime_id,
                    task_id,
                    object_id,
                },
                by,
            )
            .await;

        Ok(Response::new(DecrementOrRemoveResponse { removed }))
    }
}
