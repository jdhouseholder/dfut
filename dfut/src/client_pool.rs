use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use lru::LruCache;
use tonic::transport::{Channel, Endpoint, Error};

use crate::services::{
    d_store_service::d_store_service_client::DStoreServiceClient,
    worker_service::worker_service_client::WorkerServiceClient,
};

const CLIENT_CACHE_SIZE: usize = 20;

pub trait Connect: Clone {
    fn connect(endpoint: Endpoint) -> impl std::future::Future<Output = Result<Self, Error>>;
}

impl Connect for WorkerServiceClient<Channel> {
    fn connect(endpoint: Endpoint) -> impl std::future::Future<Output = Result<Self, Error>> {
        async move {
            let client = WorkerServiceClient::connect(endpoint).await?;
            Ok(client
                .max_encoding_message_size(usize::MAX)
                .max_decoding_message_size(usize::MAX))
        }
    }
}

impl Connect for DStoreServiceClient<Channel> {
    fn connect(endpoint: Endpoint) -> impl std::future::Future<Output = Result<Self, Error>> {
        async move {
            let client = DStoreServiceClient::connect(endpoint).await?;
            Ok(client
                .max_encoding_message_size(usize::MAX)
                .max_decoding_message_size(usize::MAX))
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientPool<T> {
    worker_service_client_cache: Arc<Mutex<LruCache<String, T>>>,
}

impl<T> ClientPool<T>
where
    T: Connect,
{
    pub fn new() -> Self {
        Self {
            worker_service_client_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(CLIENT_CACHE_SIZE).unwrap(),
            ))),
        }
    }

    pub async fn connect(&self, address: &str) -> Result<T, Error> {
        let endpoint: Endpoint = address.parse().unwrap();
        match T::connect(endpoint).await {
            Ok(client) => {
                self.worker_service_client_cache
                    .lock()
                    .unwrap()
                    .put(address.to_string(), client.clone());
                Ok(client)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_client(&self, address: &str) -> Result<T, Error> {
        if let Some(client) = self
            .worker_service_client_cache
            .lock()
            .unwrap()
            .get(address)
        {
            return Ok(client.clone());
        }
        self.connect(address).await
    }
}
