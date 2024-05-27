use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lru::LruCache;
use tonic::transport::{Channel, Endpoint};

use crate::{d_store::DStoreId, work::Work};

pub(crate) mod worker_service {
    tonic::include_proto!("worker_service");
}

use worker_service::{worker_service_client::WorkerServiceClient, DoWorkRequest, DoWorkResponse};

#[derive(Debug, Clone)]
pub(crate) struct PeerWorkerClient {
    worker_service_client_cache: Arc<Mutex<LruCache<String, WorkerServiceClient<Channel>>>>,
}

impl PeerWorkerClient {
    pub(crate) fn new() -> Self {
        Self {
            worker_service_client_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(20).unwrap(),
            ))),
        }
    }

    async fn worker_service_connect(endpoint: Endpoint) -> WorkerServiceClient<Channel> {
        for i in 0..5 {
            if let Ok(client) = WorkerServiceClient::connect(endpoint.clone()).await {
                return client
                    .max_encoding_message_size(usize::MAX)
                    .max_decoding_message_size(usize::MAX);
            }
            tokio::time::sleep(Duration::from_millis(100 * 2u64.pow(i))).await;
        }
        panic!();
    }

    pub(crate) async fn do_work(
        &self,
        current_address: &str,
        current_lifetime_id: u64,
        current_task_id: u64,
        address: &str,
        w: Work,
    ) -> DStoreId {
        let maybe_client = {
            self.worker_service_client_cache
                .lock()
                .unwrap()
                .get(address)
                .map(|client| client.clone())
                .clone()
        };

        let mut client = match maybe_client {
            Some(client) => client,
            None => {
                let endpoint: Endpoint = address.parse().unwrap();
                let client = Self::worker_service_connect(endpoint).await;
                self.worker_service_client_cache
                    .lock()
                    .unwrap()
                    .put(address.to_string(), client.clone());
                client
            }
        };

        let DoWorkResponse {
            address,
            lifetime_id,
            task_id,
            object_id,
        } = client
            .do_work(DoWorkRequest {
                parent_address: current_address.to_string(),
                parent_lifetime_id: current_lifetime_id,
                parent_task_id: current_task_id,
                fn_name: w.fn_name,
                args: w.args,
            })
            .await
            .unwrap()
            .into_inner();

        DStoreId {
            address,
            lifetime_id,
            task_id,
            object_id,
        }
    }
}
