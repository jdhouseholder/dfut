use std::any::Any;
use std::collections::{hash_map::Entry as HashMapEntry, HashMap};
use std::num::NonZeroUsize;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use lru::LruCache;
use metrics::counter;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::broadcast;
use tonic::{
    transport::{Channel, Endpoint},
    Request, Response, Status,
};

use crate::{d_scheduler::worker_service::DoWorkResponse, Error};

pub(crate) mod d_store_service {
    tonic::include_proto!("d_store_service");
}

use d_store_service::{
    d_store_service_client::DStoreServiceClient, d_store_service_server::DStoreService,
    DecrementOrRemoveRequest, DecrementOrRemoveResponse, GetOrWatchRequest, GetOrWatchResponse,
    ShareNRequest, ShareNResponse,
};

pub(crate) trait ValueTrait:
    erased_serde::Serialize + std::fmt::Debug + Any + Send + Sync + 'static
{
    fn as_serialize(&self) -> &dyn erased_serde::Serialize;
    fn as_any(self: Arc<Self>) -> Arc<(dyn Any + Send + Sync + 'static)>;
}

impl<T> ValueTrait for T
where
    T: erased_serde::Serialize + std::fmt::Debug + Any + Send + Sync + 'static,
{
    fn as_serialize(&self) -> &dyn erased_serde::Serialize {
        self
    }

    fn as_any(self: Arc<Self>) -> Arc<(dyn Any + Send + Sync + 'static)> {
        self
    }
}

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
        tx: broadcast::Sender<Option<Arc<dyn ValueTrait>>>,
        subscribers: u64,
        ref_count: u64,
    },
    // TODO: we can store Box<dyn Any> and lazy encode then store the Vec<u8>.
    // This means we avoid encode/decode in the local work case.
    Here {
        t: Arc<dyn ValueTrait>,
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
    fn insert<T>(&self, key: DStoreId, t: T)
    where
        T: ValueTrait,
    {
        counter!("local_store::insert").increment(1);

        let t: Arc<dyn ValueTrait> = Arc::new(t);

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
                tx.send(Some(t)).unwrap();
                return;
            } else {
                // There are still outstanding refs, we need to store.
                tx.send(Some(Arc::clone(&t))).unwrap();
                new_ref_count = ref_count - subscribers;
            }
        }
        m.insert(
            key,
            Entry::Here {
                t,
                ref_count: new_ref_count,
            },
        );
    }

    async fn get_or_watch(&self, key: DStoreId) -> Option<Arc<dyn ValueTrait>> {
        counter!("local_store::get_or_watch").increment(1);

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
                        Entry::Here { t, ref_count } => {
                            *ref_count -= 1;
                            if *ref_count == 0 {
                                remove = true;
                            } else {
                                return Some(Arc::clone(&t));
                            }
                        }
                    }
                    if remove {
                        match o.remove() {
                            Entry::Here { t, .. } => return Some(t),
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
        counter!("local_store::share").increment(1);

        let mut m = self.blob_store.lock().unwrap();
        match m.get_mut(&key).unwrap() {
            Entry::Watch { ref_count, .. } | Entry::Here { ref_count, .. } => {
                *ref_count += n;
            }
        }
        Ok(())
    }

    async fn decrement_or_remove(&self, key: &DStoreId, by: u64) -> bool {
        counter!("local_store::decrement_or_remove").increment(1);

        let mut m = self.blob_store.lock().unwrap();
        let mut remove = false;

        match m.get_mut(&key).unwrap() {
            Entry::Watch { ref_count, .. } | Entry::Here { ref_count, .. } => {
                *ref_count -= by;
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

    fn clear(&self) {
        counter!("local_store::clear").increment(1);

        let mut m = self.blob_store.lock().unwrap();
        for e in m.values() {
            match e {
                Entry::Watch { tx, .. } => {
                    tx.send(None).unwrap();
                }
                Entry::Here { .. } => {}
            }
        }
        m.clear();
    }
}

#[derive(Debug, Default)]
pub(crate) struct DStore {
    current_address: String,
    lifetime_id: Arc<AtomicU64>,
    next_object_id: AtomicU64,

    local_store: LocalStore,
    d_store_client: DStoreClient,
}

impl DStore {
    pub(crate) async fn new(current_address: &str, lifetime_id: &Arc<AtomicU64>) -> Self {
        Self {
            current_address: current_address.to_string(),
            lifetime_id: Arc::clone(&lifetime_id),
            next_object_id: AtomicU64::new(0),

            local_store: Default::default(),
            d_store_client: DStoreClient::default(),
        }
    }

    pub(crate) fn take_next_id(&self, task_id: u64) -> DStoreId {
        DStoreId {
            address: self.current_address.clone(),
            lifetime_id: self.lifetime_id.load(Ordering::SeqCst),
            task_id,
            object_id: self.next_object_id.fetch_add(1, Ordering::SeqCst),
        }
    }

    pub(crate) fn publish<T>(&self, key: DStoreId, t: T) -> Result<(), Error>
    where
        T: Serialize + std::fmt::Debug + Send + Sync + 'static,
    {
        self.local_store.insert(key, t);
        Ok(())
    }

    pub(crate) async fn get_or_watch<T>(&self, key: DStoreId) -> Result<T, Error>
    where
        T: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        if key.address != self.current_address {
            let t: T = self.d_store_client.get_or_watch(key).await?;
            Ok(t)
        } else {
            let t: Arc<T> = self
                .local_store
                .get_or_watch(key.clone())
                .await
                .ok_or(Error::System)?
                .as_any()
                .downcast()
                .unwrap();
            Ok((*t).clone())
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

    pub(crate) fn clear(&self) {
        self.local_store.clear()
    }
}

#[derive(Debug)]
pub struct DStoreClient {
    lru: Arc<Mutex<LruCache<DStoreId, Arc<dyn Any + Send + Sync + 'static>>>>,
    d_store_service_client_cache: Arc<Mutex<LruCache<String, DStoreServiceClient<Channel>>>>,
}

impl Default for DStoreClient {
    fn default() -> Self {
        Self {
            lru: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(100).unwrap()))),
            d_store_service_client_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(100).unwrap(),
            ))),
        }
    }
}

impl DStoreClient {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    async fn get_or_connect(&self, address: &str) -> DStoreServiceClient<Channel> {
        let maybe_client = self
            .d_store_service_client_cache
            .lock()
            .unwrap()
            .get(address)
            .map(|client| client.clone())
            .clone();

        match maybe_client {
            Some(client) => client,
            None => {
                let endpoint: Endpoint = address.parse().unwrap();
                let client = DStoreServiceClient::connect(endpoint)
                    .await
                    .unwrap()
                    .max_encoding_message_size(usize::MAX)
                    .max_decoding_message_size(usize::MAX);
                self.d_store_service_client_cache
                    .lock()
                    .unwrap()
                    .put(address.to_string(), client.clone());
                client
            }
        }
    }

    pub(crate) async fn get_or_watch<T>(&self, key: DStoreId) -> Result<T, Error>
    where
        T: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        if let Some(v) = {
            let v: Option<Arc<T>> = self
                .lru
                .lock()
                .unwrap()
                .get(&key)
                .map(|a| Arc::clone(&a).downcast().unwrap());
            v
        } {
            // MUST decrement remote.
            self.decrement_or_remove(key, 1).await?;
            return Ok((*v).clone());
        }

        let mut client = self.get_or_connect(&key.address).await;
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

        let t: T = bincode::deserialize(&object).unwrap();

        // TODO: We can avoid this if we know if the object has been deallocated.
        self.lru.lock().unwrap().put(key, Arc::new(t.clone()));

        Ok(t)
    }

    pub(crate) async fn share(&self, key: &DStoreId, n: u64) -> Result<(), Error> {
        let mut client = self.get_or_connect(&key.address).await;
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
        let mut client = self.get_or_connect(&key.address).await;
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

        if lifetime_id < self.lifetime_id.load(Ordering::SeqCst) {
            return Err(Status::not_found("Lifetime too old".to_string()));
        }

        let id = DStoreId {
            address,
            lifetime_id,
            task_id,
            object_id,
        };

        let t: Arc<dyn ValueTrait> = self
            .local_store
            .get_or_watch(id.clone())
            .await
            .ok_or_else(|| Status::not_found("DFut canceled."))?;

        let b = bincode::serialize(t.as_serialize()).unwrap();
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

        if lifetime_id < self.lifetime_id.load(Ordering::SeqCst) {
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

        if lifetime_id < self.lifetime_id.load(Ordering::SeqCst) {
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

#[cfg(test)]
mod local_store_test {
    use super::*;

    #[tokio::test]
    async fn insert_then_get() {
        let local_store = LocalStore::default();
        let key = DStoreId {
            address: "address".to_string(),
            lifetime_id: 0,
            task_id: 0,
            object_id: 0,
        };
        let want = vec![1];

        local_store.insert(key.clone(), want.clone());
        let got: Arc<Vec<u8>> = local_store
            .get_or_watch(key)
            .await
            .unwrap()
            .as_any()
            .downcast()
            .unwrap();

        assert_eq!(*got, want);
    }

    #[tokio::test]
    async fn insert_then_watch() {
        let local_store = Arc::new(LocalStore::default());
        let key = DStoreId {
            address: "address".to_string(),
            lifetime_id: 0,
            task_id: 0,
            object_id: 0,
        };
        let want = vec![1];

        let jh = tokio::spawn({
            let local_store = Arc::clone(&local_store);
            let key = key.clone();
            let want = want.clone();
            async move {
                let got: Arc<Vec<u8>> = local_store
                    .get_or_watch(key)
                    .await
                    .unwrap()
                    .as_any()
                    .downcast()
                    .unwrap();
                assert_eq!(*got, want);
            }
        });

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        local_store.insert(key.clone(), want.clone());

        let _ = jh.await.unwrap();
    }
}
