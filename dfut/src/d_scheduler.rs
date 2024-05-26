use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lru::LruCache;
use metrics::counter;
use tonic::transport::{Channel, Endpoint};

use crate::{
    d_store::DStoreId,
    global_scheduler::global_scheduler_service::{
        global_scheduler_service_client::GlobalSchedulerServiceClient, HeartBeatRequest,
        HeartBeatResponse, RegisterRequest, RegisterResponse,
    },
    work::Work,
};

pub(crate) mod worker_service {
    tonic::include_proto!("worker_service");
}

use worker_service::{worker_service_client::WorkerServiceClient, DoWorkRequest, DoWorkResponse};

const STAY_LOCAL_THRESHOLD: usize = 2 << 16; // 64KiB // 2 << 32; // 1GiB

#[derive(Debug, Default, Clone)]
struct FnStats {
    v: Vec<(Duration, usize)>,
}

impl FnStats {
    fn track(&mut self, dur: std::time::Duration, ret_size: usize) {
        self.v.push((dur, ret_size));
    }
}

#[derive(Debug, Default, Clone)]
struct Stats {
    fn_stats: HashMap<String, FnStats>,
}

#[derive(Debug)]
pub(crate) struct DScheduler {
    d_scheduler_client: DSchedulerClient,
    // TODO: When it is fun performance time we move this out of a mutex and
    // use a lock free data structure. We don't have any benchmarks/analysis to
    // justify not using this mutex so we'll do the simple thing first.
    stats: Mutex<Stats>,
}

impl DScheduler {
    pub(crate) async fn new(address: &str) -> Self {
        Self {
            d_scheduler_client: DSchedulerClient::new(address).await,
            stats: Mutex::default(),
        }
    }

    pub(crate) async fn register(
        &self,
        address: &str,
        fn_names: Vec<String>,
    ) -> Result<RegisterResponse, Box<dyn std::error::Error>> {
        let mut global_scheduler = self.d_scheduler_client.global_scheduler.clone();
        Ok(global_scheduler
            .register(RegisterRequest {
                address: address.to_string(),
                fn_names,
            })
            .await?
            .into_inner())
    }

    pub(crate) async fn heart_beat(
        &self,
        address: &str,
        lifetime_id: u64,
        lifetime_list_id: u64,
        failed_tasks: Vec<u64>,
    ) -> Result<HeartBeatResponse, Box<dyn std::error::Error>> {
        let mut global_scheduler = self.d_scheduler_client.global_scheduler.clone();
        Ok(global_scheduler
            .heart_beat(HeartBeatRequest {
                address: address.to_string(),
                lifetime_id,
                lifetime_list_id,
                failed_tasks,
                ..Default::default()
            })
            .await?
            .into_inner())
    }

    pub(crate) fn accept_local_work(&self, fn_name: &str, arg_size: usize) -> bool {
        // Check local stats.
        // Decide if we have enough of the DFuts locally or if they are mainly on another peer.
        let last_stats = {
            self.stats
                .lock()
                .unwrap()
                .fn_stats
                .get(fn_name)
                .map(|s| {
                    s.v.last()
                        .map(|(duration, ret_size)| (duration.as_secs_f64(), *ret_size))
                })
                .flatten()
        };

        // 1. Increase local probability if large args or ret.
        // 2. Decrease local probability if the historical duration is large.
        let p = if let Some((duration, ret_size)) = last_stats {
            let s = if arg_size + ret_size >= STAY_LOCAL_THRESHOLD {
                0.8
            } else {
                0.5
            };
            let d = if duration > 1. {
                1. / (2. + duration)
            } else {
                0.
            };
            s - d
        } else {
            if arg_size >= STAY_LOCAL_THRESHOLD {
                0.8
            } else {
                0.5
            }
        };

        let f: f64 = rand::random();
        let local = f < p;
        counter!(
            "accept_local_work",
             "fn_name" => fn_name.to_string(),
             "choice" => if local { "local" } else { "remote" },
        )
        .increment(1);
        local
    }

    pub(crate) fn finish_local_work(&self, fn_name: &str, took: Duration, ret_size: usize) {
        let mut stats = self.stats.lock().unwrap();

        stats
            .fn_stats
            .entry(fn_name.to_string())
            .or_default()
            .track(took, ret_size);
    }

    pub(crate) async fn do_work(&self, address: &str, task_id: u64, w: Work) -> DStoreId {
        self.d_scheduler_client.do_work(address, task_id, w).await
    }
}

// TODO: Do real retries.
#[derive(Debug, Clone)]
pub(crate) struct DSchedulerClient {
    global_scheduler: GlobalSchedulerServiceClient<Channel>,
    worker_service_client_cache: Arc<Mutex<LruCache<String, WorkerServiceClient<Channel>>>>,
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
            global_scheduler,
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

    pub(crate) async fn do_work(&self, address: &str, task_id: u64, w: Work) -> DStoreId {
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
                parent_address: "TODO".to_string(),
                parent_lifetime_id: 1,
                parent_task_id: task_id,
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
