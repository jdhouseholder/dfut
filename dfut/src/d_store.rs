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

use crate::{services::worker_service::DoWorkResponse, Error};

use crate::services::d_store_service::{
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

impl From<DoWorkResponse> for DStoreId {
    fn from(v: DoWorkResponse) -> Self {
        Self {
            address: v.address,
            lifetime_id: v.lifetime_id,
            task_id: v.task_id,
            object_id: v.object_id,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ParentInfo {
    pub(crate) address: String,
    pub(crate) lifetime_id: u64,
    pub(crate) task_id: u64,
}

#[derive(Debug)]
struct Value {
    parent_info: Arc<ParentInfo>,
    ref_count: u64,
    value_state: ValueState,
}

#[derive(Debug)]
enum ValueState {
    Watch {
        tx: broadcast::Sender<Option<Arc<dyn ValueTrait>>>,
        subscribers: u64,
    },
    Here {
        t: Arc<dyn ValueTrait>,
    },
}

#[derive(Debug, Default)]
struct LocalStore {
    m: Mutex<HashMap<DStoreId, Value>>,
}

// is the local store simple iff we require an extra call to decrement ref?
// Or do we have to guarantee one req in flight per peer?
impl LocalStore {
    fn reserve(&self, parent_info: Arc<ParentInfo>, key: DStoreId) {
        let (tx, _) = broadcast::channel(1);
        let mut m = self.m.lock().unwrap();
        m.insert(
            key,
            Value {
                parent_info,
                ref_count: 1,
                value_state: ValueState::Watch { tx, subscribers: 0 },
            },
        );
    }

    fn insert<T>(&self, key: DStoreId, t: T) -> Result<(), Error>
    where
        T: ValueTrait,
    {
        counter!("local_store::insert").increment(1);

        let t: Arc<dyn ValueTrait> = Arc::new(t);

        let mut m = self.m.lock().unwrap();

        let parent_info;
        let new_ref_count;
        match m.entry(key.clone()) {
            HashMapEntry::Occupied(o) => {
                let (ref_count, tx, subscribers) = {
                    let value = o.remove();
                    match value.value_state {
                        ValueState::Watch { tx, subscribers } => {
                            parent_info = value.parent_info;
                            (value.ref_count, tx, subscribers)
                        }
                        _ => return Err(Error::System),
                    }
                };
                // subscribers must be less than or equal to ref_count.
                if subscribers > 0 && subscribers == ref_count {
                    // All refs are currently subscribed. We can't have any new gets or shares.
                    tx.send(Some(t)).unwrap();
                    return Ok(());
                }
                // There are still outstanding refs, we need to store.
                if subscribers > 0 {
                    tx.send(Some(Arc::clone(&t))).unwrap();
                }
                new_ref_count = ref_count - subscribers;
            }
            HashMapEntry::Vacant(_) => {
                return Err(Error::System);
            }
        }

        m.insert(
            key,
            Value {
                parent_info,
                ref_count: new_ref_count,
                value_state: ValueState::Here { t },
            },
        );

        Ok(())
    }

    async fn get_or_watch(&self, key: DStoreId) -> Result<Arc<dyn ValueTrait>, Error> {
        counter!("local_store::get_or_watch").increment(1);

        let mut rx = {
            let mut m = self.m.lock().unwrap();
            let rx = match m.entry(key) {
                HashMapEntry::Vacant(_) => return Err(Error::System),
                HashMapEntry::Occupied(mut o) => {
                    let mut remove = false;
                    let value = o.get_mut();
                    match &mut value.value_state {
                        ValueState::Watch { subscribers, .. } => {
                            *subscribers += 1;
                        }
                        ValueState::Here { t, .. } => {
                            value.ref_count -= 1;
                            if value.ref_count == 0 {
                                remove = true;
                            } else {
                                return Ok(Arc::clone(&t));
                            }
                        }
                    }
                    if remove {
                        match o.remove().value_state {
                            ValueState::Here { t, .. } => return Ok(t),
                            _ => panic!(),
                        }
                    }
                    match &o.get().value_state {
                        ValueState::Watch { tx, .. } => tx.subscribe(),
                        _ => panic!(),
                    }
                }
            };
            drop(m);
            rx
        };

        rx.recv().await.unwrap().ok_or(Error::System)
    }

    async fn share(&self, key: &DStoreId, n: u64) -> Result<(), Error> {
        counter!("local_store::share").increment(1);

        let mut m = self.m.lock().unwrap();
        let value = m.get_mut(&key).unwrap();
        value.ref_count += n;
        Ok(())
    }

    async fn decrement_or_remove(&self, key: &DStoreId, by: u64) -> bool {
        counter!("local_store::decrement_or_remove").increment(1);

        let mut m = self.m.lock().unwrap();
        let mut remove = false;

        let value = m.get_mut(&key).unwrap();

        value.ref_count -= by;
        if value.ref_count == 0 {
            remove = true;
        }

        if remove {
            m.remove(key);
        }

        remove
    }

    fn worker_failure(&self, address: &str, new_lifetime_id: u64) {
        let mut m = self.m.lock().unwrap();

        let mut remove = Vec::new();
        for (id, entry) in m.iter() {
            if entry.parent_info.address == address
                && entry.parent_info.lifetime_id < new_lifetime_id
            {
                remove.push(id.clone());
            }
        }

        for id in remove {
            if let Some(v) = m.remove(&id) {
                if let ValueState::Watch { tx, .. } = v.value_state {
                    let _ = tx.send(None);
                }
            }
        }
    }

    fn task_failure(&self, address: &str, new_lifetime_id: u64, task_id: u64) {
        let mut m = self.m.lock().unwrap();

        let mut remove = Vec::new();
        for (id, entry) in m.iter() {
            if entry.parent_info.address == address
                && entry.parent_info.lifetime_id == new_lifetime_id
                && entry.parent_info.task_id == task_id
            {
                remove.push(id.clone());
            }
        }

        for id in remove {
            if let Some(v) = m.remove(&id) {
                if let ValueState::Watch { tx, .. } = v.value_state {
                    let _ = tx.send(None);
                }
            }
        }
    }

    fn local_task_failure(&self, d_store_id: DStoreId) {
        let mut m = self.m.lock().unwrap();
        if let Some(v) = m.remove(&d_store_id) {
            match &v.value_state {
                ValueState::Watch { tx, .. } => {
                    let _ = tx.send(None);
                }
                ValueState::Here { .. } => {}
            }
        }
    }

    fn clear(&self) {
        counter!("local_store::clear").increment(1);

        let mut m = self.m.lock().unwrap();
        for e in m.values() {
            match &e.value_state {
                ValueState::Watch { tx, .. } => {
                    let _ = tx.send(None);
                }
                ValueState::Here { .. } => {}
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

    pub(crate) fn take_next_id(&self, parent_info: Arc<ParentInfo>, task_id: u64) -> DStoreId {
        let key = DStoreId {
            address: self.current_address.clone(),
            lifetime_id: self.lifetime_id.load(Ordering::SeqCst),
            task_id,
            object_id: self.next_object_id.fetch_add(1, Ordering::SeqCst),
        };
        self.local_store.reserve(parent_info, key.clone());
        key
    }

    pub(crate) fn publish<T>(&self, key: DStoreId, t: T) -> Result<(), Error>
    where
        T: Serialize + std::fmt::Debug + Send + Sync + 'static,
    {
        self.local_store.insert(key, t)
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
                .await?
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

    pub(crate) fn worker_failure(&self, address: &str, new_lifetime_id: u64) {
        self.local_store.worker_failure(address, new_lifetime_id);
    }

    pub(crate) fn task_failure(&self, address: &str, new_lifetime_id: u64, task_id: u64) {
        self.local_store
            .task_failure(address, new_lifetime_id, task_id);
    }
    pub(crate) fn local_task_failure(&self, d_store_id: DStoreId) {
        self.local_store.local_task_failure(d_store_id);
    }

    pub(crate) fn clear(&self) {
        self.local_store.clear()
    }
}

#[derive(Debug, Clone)]
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
            .map(|client| client.clone());

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
            // TODO: only don't error on not found.
            let _ = self.decrement_or_remove(key, 1).await;
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
            .map_err(|e| {
                tracing::error!("{e:?}");
                Error::System
            })?
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
        let DStoreId {
            address,
            lifetime_id,
            task_id,
            object_id,
        } = key;
        let _ = client
            .decrement_or_remove(DecrementOrRemoveRequest {
                address,
                lifetime_id,
                task_id,
                object_id,
                by,
            })
            .await
            .unwrap();
        return Ok(());
    }
}

const LIFE_TIME_TOO_OLD: &str = "Lifetime too old.";

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
            return Err(Status::not_found(LIFE_TIME_TOO_OLD.to_string()));
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
            .map_err(|_| Status::not_found("get_or_watch"))?;

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
            return Err(Status::not_found(LIFE_TIME_TOO_OLD.to_string()));
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
            return Err(Status::not_found(LIFE_TIME_TOO_OLD.to_string()));
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

        local_store.insert(key.clone(), want.clone()).unwrap();
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
        local_store.insert(key.clone(), want.clone()).unwrap();

        let _ = jh.await.unwrap();
    }
}
