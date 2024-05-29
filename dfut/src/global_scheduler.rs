use std::collections::{hash_map::Entry, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tonic::{transport::Server, Request, Response, Status};
use tracing::error;

use crate::services::global_scheduler_service::{
    global_scheduler_service_server::{GlobalSchedulerService, GlobalSchedulerServiceServer},
    FnStats, HeartBeatRequest, HeartBeatResponse, RaftStepRequest, RaftStepResponse,
    RegisterClientRequest, RegisterClientResponse, RegisterRequest, RegisterResponse, RuntimeInfo,
    Stats, TaskFailure,
};

const DEFAULT_HEARTBEAT_TIMEOUT: u64 = 10; // TODO: pass through config

#[derive(Debug, Serialize, Deserialize)]
enum Proposal {
    // Adds worker to pool of available workers.
    Add {
        address: String,
        lifetime_id: u64,
        fns: Vec<String>,
    },
    // Updates heartbeat and scheduler state.
    KeepAlive {
        address: String,
        lifetime_id: u64,
    },
    // Keeps scheduler stats approx. in sync.
    SchedulerSync {
        fns: HashMap<String, ()>,
    },
    // Removes worker from pool of available workers.
    // Either due to:
    // * Lifetime expiry.
    // * Clean removal.
    Remove {
        address: String,
        lifetime_id: u64,
    },
}

#[derive(Debug)]
struct LifetimeLease {
    id: u64,
    at: Instant,
}

#[derive(Debug, Default)]
struct InnerGlobalScheduler {
    lifetimes: HashMap<String, LifetimeLease>,
    address_to_runtime_info: HashMap<String, RuntimeInfo>,
    next_client_id: u64,
}

// TODO: Rename to Global Coordinator Service (or Global Control Service)
#[derive(Debug, Default)]
pub struct GlobalScheduler {
    inner: Mutex<InnerGlobalScheduler>,
    lifetime_timeout: Duration,
}

impl GlobalScheduler {
    async fn expire_lifetimes(self: &Arc<Self>) {
        // Proposal that expires lifetime ids based on timeout.
        // On commit: remove addresses that have expired from fn_availability.
    }

    pub async fn serve(address: &str, _peers: Vec<String>, lifetime_timeout: Duration) {
        let global_scheduler = Arc::new(Self {
            lifetime_timeout,
            ..Default::default()
        });
        let address = address
            .strip_prefix("http://")
            .unwrap()
            .to_string()
            .parse()
            .unwrap();

        let jh = tokio::spawn({
            let global_scheduler = Arc::clone(&global_scheduler);
            async move {
                loop {
                    global_scheduler.expire_lifetimes().await;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });

        let serve = Server::builder()
            .add_service(GlobalSchedulerServiceServer::new(global_scheduler))
            .serve(address);

        tokio::select! {
            _ = jh => {}
            r = serve => r.unwrap(),
        }
    }
}

#[tonic::async_trait]
impl GlobalSchedulerService for Arc<GlobalScheduler> {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let RegisterRequest { address, fn_names } = request.into_inner();

        let mut inner = self.inner.lock().unwrap();

        // TODO: store address -> lifetime_id ~forever.
        let lifetime_id = match inner.lifetimes.entry(address.clone()) {
            Entry::Occupied(ref mut e) => {
                let l = e.get_mut();
                l.at = Instant::now();
                l.id
            }
            Entry::Vacant(v) => {
                v.insert(LifetimeLease {
                    id: 0,
                    at: Instant::now(),
                });
                0
            }
        };

        let runtime_info = inner
            .address_to_runtime_info
            .entry(address.clone())
            .or_insert_with(|| {
                let fn_stats: HashMap<String, FnStats> = fn_names
                    .into_iter()
                    .map(|fn_name| (fn_name, FnStats::default()))
                    .collect();
                RuntimeInfo {
                    stats: Some(Stats {
                        fn_stats,
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            });

        runtime_info.lifetime_id = lifetime_id;

        Ok(Response::new(RegisterResponse {
            lifetime_id,
            heart_beat_timeout: DEFAULT_HEARTBEAT_TIMEOUT,
        }))
    }

    async fn register_client(
        &self,
        request: Request<RegisterClientRequest>,
    ) -> Result<Response<RegisterClientResponse>, Status> {
        let RegisterClientRequest {} = request.into_inner();

        let mut inner = self.inner.lock().unwrap();

        let id = inner.next_client_id;
        inner.next_client_id += 1;

        let address = format!("client-id-{id}");

        let lifetime_id = match inner.lifetimes.entry(address.clone()) {
            Entry::Occupied(ref mut e) => {
                let l = e.get_mut();
                l.at = Instant::now();
                l.id
            }
            Entry::Vacant(v) => {
                v.insert(LifetimeLease {
                    id: 0,
                    at: Instant::now(),
                });
                0
            }
        };

        inner
            .address_to_runtime_info
            .entry(address.clone())
            .or_insert_with(|| RuntimeInfo {
                lifetime_id,
                ..Default::default()
            });

        Ok(Response::new(RegisterClientResponse {
            client_id: address,
            lifetime_id,
            heart_beat_timeout: DEFAULT_HEARTBEAT_TIMEOUT,
        }))
    }

    async fn heart_beat(
        &self,
        request: Request<HeartBeatRequest>,
    ) -> Result<Response<HeartBeatResponse>, Status> {
        let HeartBeatRequest {
            address,
            current_runtime_info,
        } = request.into_inner();
        let Some(current_runtime_info) = current_runtime_info else {
            return Err(Status::invalid_argument("missing runtime_info"));
        };

        let mut inner = self.inner.lock().unwrap();

        let current_lifetime_id = current_runtime_info.lifetime_id;

        match inner.address_to_runtime_info.entry(address.clone()) {
            Entry::Occupied(ref mut o) => {
                // TODO: merge
                let stored_runtime_info = o.get_mut();

                if current_runtime_info.failed_task_ids.len() > 0 {
                    let new_task_failures: Vec<_> = current_runtime_info
                        .failed_task_ids
                        .into_iter()
                        .map(|task_id| TaskFailure {
                            lifetime_id: current_runtime_info.lifetime_id,
                            task_id,
                        })
                        .collect();
                    stored_runtime_info.task_failures.extend(new_task_failures);
                    stored_runtime_info.task_failures.dedup();
                }
            }
            Entry::Vacant(_) => return Err(Status::not_found("Address is not registered.")),
        }

        // TODO: new failed_tasks have a ttl of 2 * HBTO and can then be removed.
        // TODO: use low watermark from worker to avoid having to failed_tasks forever.

        let lifetime_id = match inner.lifetimes.entry(address.clone()) {
            Entry::Occupied(ref mut o) => {
                let now = Instant::now();

                let lifetime_lease = o.get_mut();

                if current_lifetime_id > lifetime_lease.id {
                    todo!("Error: Workers cannot have a higher lifetime id than the lease, there is a bug.");
                }

                let expired_lifetime_id = current_lifetime_id < lifetime_lease.id;
                let dur_since_last_heart_beat =
                    now.checked_duration_since(lifetime_lease.at).unwrap();
                let lifetime_id_timeout = dur_since_last_heart_beat > self.lifetime_timeout;

                if expired_lifetime_id || lifetime_id_timeout {
                    if expired_lifetime_id {
                        error!(
                            "expired_lifetime_id: got={}, want={}",
                            current_lifetime_id, lifetime_lease.id
                        );
                    }
                    if lifetime_id_timeout {
                        error!(
                            "lifetime_id_timeout: dur_since_last_heart_beat={:?}",
                            dur_since_last_heart_beat
                        );
                    }
                    lifetime_lease.id += 1;
                }

                lifetime_lease.at = now;
                lifetime_lease.id
            }
            Entry::Vacant(_) => {
                return Err(Status::invalid_argument(format!(
                    "{} does not have a lifetime lease",
                    address
                )));
            }
        };

        Ok(Response::new(HeartBeatResponse {
            lifetime_id,
            address_to_runtime_info: inner.address_to_runtime_info.clone(),
        }))
    }

    async fn raft_step(
        &self,
        request: Request<RaftStepRequest>,
    ) -> Result<Response<RaftStepResponse>, Status> {
        let _ = request.into_inner();
        Ok(Response::new(RaftStepResponse::default()))
    }
}
