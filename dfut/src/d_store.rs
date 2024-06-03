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
use tonic::{transport::Channel, Request, Response, Status};

use crate::{
    client_pool::ClientPool,
    gaps::AddressToGaps,
    retry::retry_from_pool,
    rpc_context::RpcContext,
    services::{
        d_store_service::{
            d_store_service_client::DStoreServiceClient, d_store_service_server::DStoreService,
            DecrementOrRemoveRequest, DecrementOrRemoveResponse, GetOrWatchRequest,
            GetOrWatchResponse, ShareNRequest, ShareNResponse,
        },
        worker_service::{DoWorkResponse, ParentInfo},
    },
    Error,
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

#[derive(Debug)]
struct Value {
    parent_info: Arc<Vec<ParentInfo>>,
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
    fn reserve(&self, parent_info: Arc<Vec<ParentInfo>>, key: DStoreId) {
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
                    let _ = tx.send(Some(t));
                    return Ok(());
                }
                // There are still outstanding refs, we need to store.
                if subscribers > 0 {
                    let _ = tx.send(Some(Arc::clone(&t)));
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
        let value = m.get_mut(&key).ok_or(Error::System)?;
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

    fn exists(&self, id: &DStoreId) -> bool {
        self.m.lock().unwrap().contains_key(id)
    }

    fn worker_failure(&self, address: &str, new_lifetime_id: u64) {
        let mut m = self.m.lock().unwrap();

        let mut remove = Vec::new();
        for (id, entry) in m.iter() {
            for parent_info in entry.parent_info.iter() {
                if parent_info.address == address && parent_info.lifetime_id < new_lifetime_id {
                    remove.push(id.clone());
                }
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
            for parent_info in entry.parent_info.iter() {
                if parent_info.address == address
                    && parent_info.lifetime_id == new_lifetime_id
                    && parent_info.task_id == task_id
                {
                    remove.push(id.clone());
                }
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

    fn parent_info_to_id(
        &self,
        parent_address: &str,
        parent_lifetime_id: u64,
        parent_task_id: u64,
        request_id: u64,
    ) -> Option<DStoreId> {
        let m = self.m.lock().unwrap();
        for (k, v) in m.iter() {
            if v.parent_info.last().unwrap().address == parent_address
                && v.parent_info.last().unwrap().lifetime_id == parent_lifetime_id
                && v.parent_info.last().unwrap().task_id == parent_task_id
                && v.parent_info.last().unwrap().request_id == request_id
            {
                return Some(k.clone());
            }
        }

        None
    }
}

#[derive(Debug, Default)]
pub(crate) struct DStore {
    current_address: String,
    lifetime_id: Arc<AtomicU64>,
    next_object_id: AtomicU64,
    address_to_gaps: AddressToGaps,

    local_store: LocalStore,
    d_store_client: DStoreClient,
}

impl DStore {
    pub(crate) async fn new(
        current_address: &str,
        lifetime_id: &Arc<AtomicU64>,
        address_to_gaps: AddressToGaps,
    ) -> Self {
        Self {
            current_address: current_address.to_string(),
            lifetime_id: Arc::clone(&lifetime_id),
            next_object_id: AtomicU64::new(0),
            address_to_gaps,

            local_store: Default::default(),
            d_store_client: DStoreClient::default(),
        }
    }

    pub(crate) fn take_next_id(&self, parent_info: Arc<Vec<ParentInfo>>, task_id: u64) -> DStoreId {
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

    pub(crate) async fn get_or_watch<T>(
        &self,
        rpc_context: &RpcContext,
        key: DStoreId,
    ) -> Result<T, Error>
    where
        T: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        if key.address != self.current_address {
            let t: T = self.d_store_client.get_or_watch(rpc_context, key).await?;
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

    pub(crate) async fn share(
        &self,
        rpc_context: &RpcContext,
        key: &DStoreId,
        n: u64,
    ) -> Result<(), Error> {
        if key.address != self.current_address {
            return self.d_store_client.share(rpc_context, key, n).await;
        }

        self.local_store.share(key, n).await?;

        Ok(())
    }

    pub(crate) async fn decrement_or_remove(
        &self,
        rpc_context: &RpcContext,
        key: DStoreId,
        by: u64,
    ) -> Result<(), Error> {
        if key.address != self.current_address {
            return self
                .d_store_client
                .decrement_or_remove(rpc_context, key, by)
                .await;
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

    pub(crate) fn parent_info_to_id(
        &self,
        parent_address: &str,
        parent_lifetime_id: u64,
        parent_task_id: u64,
        request_id: u64,
    ) -> Option<DStoreId> {
        self.local_store.parent_info_to_id(
            parent_address,
            parent_lifetime_id,
            parent_task_id,
            request_id,
        )
    }
}

#[derive(Debug, Clone)]
pub struct DStoreClient {
    lru: Arc<Mutex<LruCache<DStoreId, Arc<dyn Any + Send + Sync + 'static>>>>,
    pool: ClientPool<DStoreServiceClient<Channel>>,
}

impl Default for DStoreClient {
    fn default() -> Self {
        Self {
            lru: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(100).unwrap()))),
            pool: ClientPool::new(),
        }
    }
}

impl DStoreClient {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn get_or_watch<T>(
        &self,
        rpc_context: &RpcContext,
        key: DStoreId,
    ) -> Result<T, Error>
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
            let _ = self.decrement_or_remove(rpc_context, key, 1).await;
            return Ok((*v).clone());
        }

        let request_id = rpc_context.next_request_id(&key.address);
        let GetOrWatchResponse { object } =
            retry_from_pool(&self.pool, &key.address, |mut client| {
                let key = key.clone();
                async move {
                    client
                        .get_or_watch(GetOrWatchRequest {
                            remote_address: rpc_context.local_address.clone(),
                            remote_lifetime_id: rpc_context.lifetime_id,
                            request_id,

                            address: key.address,
                            lifetime_id: key.lifetime_id,
                            task_id: key.task_id,
                            object_id: key.object_id,
                        })
                        .await
                }
            })
            .await?;

        let t: T = bincode::deserialize(&object).unwrap();

        // TODO: We can avoid this if we know if the object has been deallocated.
        self.lru.lock().unwrap().put(key, Arc::new(t.clone()));

        Ok(t)
    }

    pub(crate) async fn share(
        &self,
        rpc_context: &RpcContext,
        key: &DStoreId,
        n: u64,
    ) -> Result<(), Error> {
        let request_id = rpc_context.next_request_id(&key.address);
        retry_from_pool(&self.pool, &key.address, |mut client| {
            let key = key.clone();
            async move {
                client
                    .share_n(ShareNRequest {
                        remote_address: rpc_context.local_address.clone(),
                        remote_lifetime_id: rpc_context.lifetime_id,
                        request_id,

                        address: key.address,
                        lifetime_id: key.lifetime_id,
                        task_id: key.task_id,
                        object_id: key.object_id,
                        n,
                    })
                    .await
            }
        })
        .await?;
        return Ok(());
    }

    pub(crate) async fn decrement_or_remove(
        &self,
        rpc_context: &RpcContext,
        key: DStoreId,
        by: u64,
    ) -> Result<(), Error> {
        let request_id = rpc_context.next_request_id(&key.address);
        retry_from_pool(&self.pool, &key.address, |mut client| {
            let key = key.clone();
            async move {
                client
                    .decrement_or_remove(DecrementOrRemoveRequest {
                        remote_address: rpc_context.local_address.clone(),
                        remote_lifetime_id: rpc_context.lifetime_id,
                        request_id,

                        address: key.address,
                        lifetime_id: key.lifetime_id,
                        task_id: key.task_id,
                        object_id: key.object_id,
                        by,
                    })
                    .await
            }
        })
        .await?;
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
            remote_address,
            remote_lifetime_id,
            request_id,

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

        if self.address_to_gaps.have_seen_request_id(
            &remote_address,
            remote_lifetime_id,
            request_id,
        ) {
            if self.local_store.share(&id, 1).await.is_err() {
                return Err(Status::not_found("Invalid id"));
            }
        }

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
            remote_address,
            remote_lifetime_id,
            request_id,

            address,
            lifetime_id,
            task_id,
            object_id,
            n,
        } = request.into_inner();

        if lifetime_id < self.lifetime_id.load(Ordering::SeqCst) {
            return Err(Status::not_found(LIFE_TIME_TOO_OLD.to_string()));
        }

        if self.address_to_gaps.have_seen_request_id(
            &remote_address,
            remote_lifetime_id,
            request_id,
        ) {
            return Ok(Response::new(ShareNResponse::default()));
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
            remote_address,
            remote_lifetime_id,
            request_id,

            address,
            lifetime_id,
            task_id,
            object_id,
            by,
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

        if self.address_to_gaps.have_seen_request_id(
            &remote_address,
            remote_lifetime_id,
            request_id,
        ) {
            let exists = self.local_store.exists(&id);
            return Ok(Response::new(DecrementOrRemoveResponse { removed: exists }));
        }

        let removed = self.local_store.decrement_or_remove(&id, by).await;

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

        local_store.reserve(
            Arc::new(vec![ParentInfo {
                address: "address".to_string(),
                lifetime_id: 0,
                task_id: 0,
                request_id: 0,
            }]),
            key.clone(),
        );
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

        local_store.reserve(
            Arc::new(vec![ParentInfo {
                address: "address".to_string(),
                lifetime_id: 0,
                task_id: 0,
                request_id: 0,
            }]),
            key.clone(),
        );
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        local_store.insert(key.clone(), want.clone()).unwrap();

        let _ = jh.await.unwrap();
    }
}
