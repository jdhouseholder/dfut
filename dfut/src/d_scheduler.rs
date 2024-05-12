use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};

use crate::{
    d_store::DStoreId,
    global_scheduler::global_scheduler_service::{
        global_scheduler_service_client::GlobalSchedulerServiceClient, RegisterRequest,
        RegisterResponse, ScheduleRequest, ScheduleResponse,
    },
    work::Work,
};

pub(crate) mod worker_service {
    tonic::include_proto!("worker_service");
}

use worker_service::{worker_service_client::WorkerServiceClient, DoWorkResponse};

#[derive(Debug, Default, Clone)]
struct FnStats {
    dur: Vec<Duration>,
}

impl FnStats {
    fn track(&mut self, dur: std::time::Duration) {
        self.dur.push(dur);
    }
}

#[derive(Debug, Default, Clone)]
struct Stats {
    fn_stats: HashMap<String, FnStats>,
}

pub(crate) struct LocalWorkToken {
    start: Instant,
}

#[derive(Debug)]
pub(crate) struct DScheduler {
    d_scheduler_client: DSchedulerClient,
    stats: StdMutex<Stats>,
}

impl DScheduler {
    pub(crate) async fn new(address: &str) -> Self {
        Self {
            d_scheduler_client: DSchedulerClient::new(address).await,
            stats: StdMutex::default(),
        }
    }

    pub(crate) async fn register(
        &self,
        address: &str,
        fn_names: Vec<String>,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let mut global_scheduler = self.d_scheduler_client.global_scheduler.lock().await;
        let RegisterResponse { lifetime_id } = global_scheduler
            .register(RegisterRequest {
                address: address.to_string(),
                fn_names,
            })
            .await?
            .into_inner();
        Ok(lifetime_id)
    }

    pub(crate) fn accept_local_work(&self, _fn_name: &str) -> bool {
        // Check local stats.
        // Decide if we have enough of the DFuts locally or if they are mainly on another peer.
        rand::random()
    }

    pub(crate) fn start_local_work(&self) -> LocalWorkToken {
        LocalWorkToken {
            start: Instant::now(),
        }
    }

    pub(crate) fn finish_local_work(&self, local_work_token: LocalWorkToken, fn_name: &str) {
        let mut stats = self.stats.lock().unwrap();

        let dur = local_work_token.start.elapsed();

        stats
            .fn_stats
            .entry(fn_name.to_string())
            .or_default()
            .track(dur);
    }

    pub(crate) async fn schedule(&self, task_id: u64, w: Work) -> DStoreId {
        self.d_scheduler_client.schedule(task_id, w).await
    }
}

// TODO: Do real retries.
#[derive(Debug, Clone)]
pub(crate) struct DSchedulerClient {
    global_scheduler: Arc<Mutex<GlobalSchedulerServiceClient<Channel>>>,
}

impl DSchedulerClient {
    async fn gs_connect(endpoint: Endpoint) -> GlobalSchedulerServiceClient<Channel> {
        for i in 0..5 {
            if let Ok(client) = GlobalSchedulerServiceClient::connect(endpoint.clone()).await {
                return client;
            }
            tokio::time::sleep(Duration::from_millis(100 * 2u64.pow(i))).await;
        }
        panic!();
    }

    pub(crate) async fn new(global_scheduler_address: &str) -> Self {
        let endpoint: Endpoint = global_scheduler_address.parse().unwrap();
        let global_scheduler = Self::gs_connect(endpoint).await;
        Self {
            global_scheduler: Arc::new(Mutex::new(global_scheduler)),
        }
    }

    async fn schedule_with_retry(&self, w: &Work) -> String {
        for i in 0..5 {
            let mut global_scheduler = self.global_scheduler.lock().await;
            let req: ScheduleRequest = w.into();
            let ScheduleResponse { address } =
                global_scheduler.schedule(req).await.unwrap().into_inner();
            if let Some(address) = address {
                return address;
            }
            tokio::time::sleep(Duration::from_millis(100 * 2u64.pow(i))).await;
        }
        panic!("unable to find worker that advertises {}", w.fn_name);
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

    pub(crate) async fn schedule(&self, task_id: u64, w: Work) -> DStoreId {
        let address = self.schedule_with_retry(&w).await;
        let endpoint: Endpoint = address.parse().unwrap();
        let mut client = Self::worker_service_connect(endpoint).await;
        let req = w.into_do_work_request(task_id);
        let DoWorkResponse {
            address,
            lifetime_id,
            task_id,
            object_id,
        } = client.do_work(req).await.unwrap().into_inner();
        DStoreId {
            address,
            lifetime_id,
            task_id,
            object_id,
        }
    }
}
