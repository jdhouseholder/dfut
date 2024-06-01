use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lru::LruCache;
use tonic::{
    transport::{Channel, Endpoint},
    Code,
};

use crate::{d_store::DStoreId, work::Work, DResult, Error};

use crate::services::worker_service::{worker_service_client::WorkerServiceClient, DoWorkRequest};

const WORKER_SERVER_CLIENT_CACHE_SIZE: usize = 20;

#[derive(Debug, Clone)]
pub(crate) struct PeerWorkerClient {
    worker_service_client_cache: Arc<Mutex<LruCache<String, WorkerServiceClient<Channel>>>>,
    peer_to_next_request_id: Arc<Mutex<HashMap<String, u64>>>,
}

impl PeerWorkerClient {
    pub(crate) fn new() -> Self {
        Self {
            worker_service_client_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(WORKER_SERVER_CLIENT_CACHE_SIZE).unwrap(),
            ))),
            peer_to_next_request_id: Arc::default(),
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

    fn take_next_request_id(&self, address: &str) -> u64 {
        let mut m = self.peer_to_next_request_id.lock().unwrap();
        match m.get_mut(address) {
            Some(next_request_id) => {
                let id = *next_request_id;
                *next_request_id += 1;
                id
            }
            None => {
                m.insert(address.to_string(), 1);
                0
            }
        }
    }

    pub(crate) async fn do_work(
        &self,
        current_address: &str,
        current_lifetime_id: u64,
        current_task_id: u64,
        address: &str,
        w: Work,
    ) -> DResult<DStoreId> {
        let maybe_client = {
            self.worker_service_client_cache
                .lock()
                .unwrap()
                .get(address)
                .map(|client| client.clone())
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

        let request_id = self.take_next_request_id(&address);

        for i in 0..5 {
            match client
                .do_work(DoWorkRequest {
                    parent_address: current_address.to_string(),
                    parent_lifetime_id: current_lifetime_id,
                    parent_task_id: current_task_id,
                    request_id,
                    fn_name: w.fn_name.clone(),
                    args: w.args.clone(),
                })
                .await
            {
                Ok(resp) => return Ok(resp.into_inner().into()),
                Err(e) => match e.code() {
                    Code::Internal => {
                        /* TODO: confirm that this means we need to reconnect. */
                        let endpoint: Endpoint = address.parse().unwrap();
                        client = Self::worker_service_connect(endpoint).await;
                        self.worker_service_client_cache
                            .lock()
                            .unwrap()
                            .put(address.to_string(), client.clone());
                    }
                    Code::NotFound | Code::InvalidArgument => return Err(Error::System),
                    _ => tokio::time::sleep(Duration::from_millis(100 * 2u64.pow(i))).await,
                },
            }
        }

        Err(Error::System)
    }
}
