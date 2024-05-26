use std::collections::{hash_map::Entry, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use metrics::counter;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use tonic::{transport::Server, Request, Response, Status};
use tracing::error;

pub(crate) mod global_scheduler_service {
    tonic::include_proto!("global_scheduler_service");
}

use global_scheduler_service::{
    global_scheduler_service_server::{GlobalSchedulerService, GlobalSchedulerServiceServer},
    HeartBeatRequest, HeartBeatResponse, RaftStepRequest, RaftStepResponse, RegisterRequest,
    RegisterResponse, ScheduleRequest, ScheduleResponse, UnRegisterRequest, UnRegisterResponse,
};

const DEFAULT_HEARTBEAT_TIMEOUT: u64 = 5; // TODO: pass through config
const DEFAULT_LIFETIME_ID: u64 = 1;

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
struct Lifetimes {
    list_id: u64,
    m: HashMap<String, LifetimeLease>,
}

#[derive(Debug, Default)]
struct InnerGlobalScheduler {
    fn_availability: HashMap<String, Vec<String>>,
    lifetimes: Lifetimes,
}

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

    fn schedule_fn(&self, fn_name: &str) -> Option<String> {
        self.inner
            .lock()
            .unwrap()
            .fn_availability
            .get(fn_name)?
            .choose(&mut rand::thread_rng())
            .map(|s| {
                counter!("schedule_fn", "fn_name" => fn_name.to_string(), "choice" => s.clone())
                    .increment(1);
                s.clone()
            })
            .clone()
    }

    fn remove_address_from_index(&self, address: &str) {
        let mut inner = self.inner.lock().unwrap();
        for v in inner.fn_availability.values_mut() {
            if let Some(i) = v.iter().position(|s| s == &address) {
                v.remove(i);
                continue;
            }
        }
    }

    pub async fn serve(address: &str, _peers: Vec<String>) {
        let global_scheduler = Arc::new(Self {
            lifetime_timeout: Duration::from_secs(5),
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
        for fn_name in fn_names {
            let p = inner.fn_availability.entry(fn_name.clone()).or_default();
            p.push(address.clone());
            p.dedup();
        }

        // TODO: store address -> lifetime_id ~forever.
        let lifetime_id = match inner.lifetimes.m.entry(address) {
            Entry::Occupied(ref mut e) => {
                let l = e.get_mut();
                l.at = Instant::now();
                l.id
            }
            Entry::Vacant(v) => {
                v.insert(LifetimeLease {
                    id: DEFAULT_LIFETIME_ID,
                    at: Instant::now(),
                });
                DEFAULT_LIFETIME_ID
            }
        };

        Ok(Response::new(RegisterResponse {
            leader_redirect: None,
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
            lifetime_id,
            lifetime_list_id,
            ..
        } = request.into_inner();

        let mut inner = self.inner.lock().unwrap();

        if lifetime_list_id == inner.lifetimes.list_id {
            return Ok(Response::new(HeartBeatResponse {
                leader_redirect: None,
                lifetime_id,
                lifetime_list_id: inner.lifetimes.list_id,
                lifetimes: HashMap::new(),
                stats: HashMap::new(),
                failed_tasks: HashMap::new(),
            }));
        }

        let lifetime_id = match inner.lifetimes.m.entry(address.clone()) {
            Entry::Occupied(ref mut o) => {
                let now = Instant::now();

                let lifetime_lease = o.get_mut();

                if lifetime_id > lifetime_lease.id {
                    todo!("Error: Workers cannot have a higher lifetime id than the lease, there is a bug.");
                }

                let expired_lifetime_id = lifetime_id < lifetime_lease.id;
                let dur_since_last_heart_beat =
                    now.checked_duration_since(lifetime_lease.at).unwrap();
                let lifetime_id_timeout = dur_since_last_heart_beat > self.lifetime_timeout;

                if expired_lifetime_id || lifetime_id_timeout {
                    if expired_lifetime_id {
                        error!(
                            "expired_lifetime_id: got={}, want={}",
                            lifetime_id, lifetime_lease.id
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

        let lifetime_list_id = inner.lifetimes.list_id;
        let lifetimes: HashMap<_, _> = inner
            .lifetimes
            .m
            .iter()
            .map(|(address, lifetime_lease)| (address.to_string(), lifetime_lease.id))
            .collect();

        Ok(Response::new(HeartBeatResponse {
            leader_redirect: None,
            lifetime_id,
            lifetime_list_id,
            lifetimes,
            stats: HashMap::new(),
            failed_tasks: HashMap::new(),
        }))
    }

    async fn schedule(
        &self,
        request: Request<ScheduleRequest>,
    ) -> Result<Response<ScheduleResponse>, Status> {
        let ScheduleRequest { fn_name } = request.into_inner();

        let address = self.schedule_fn(&fn_name);

        Ok(Response::new(ScheduleResponse {
            leader_redirect: None,
            address,
        }))
    }

    async fn un_register(
        &self,
        request: Request<UnRegisterRequest>,
    ) -> Result<Response<UnRegisterResponse>, Status> {
        let UnRegisterRequest { address } = request.into_inner();

        self.remove_address_from_index(&address);

        Ok(Response::new(UnRegisterResponse::default()))
    }

    async fn raft_step(
        &self,
        request: Request<RaftStepRequest>,
    ) -> Result<Response<RaftStepResponse>, Status> {
        let _ = request.into_inner();
        Ok(Response::new(RaftStepResponse::default()))
    }
}
