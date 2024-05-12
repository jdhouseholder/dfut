use std::collections::{hash_map::Entry, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::seq::SliceRandom;
use tonic::{transport::Server, Request, Response, Status};

pub(crate) mod global_scheduler_service {
    tonic::include_proto!("global_scheduler_service");
}

use global_scheduler_service::{
    global_scheduler_service_server::{GlobalSchedulerService, GlobalSchedulerServiceServer},
    HeartBeatRequest, HeartBeatResponse, Lifetime, RegisterRequest, RegisterResponse,
    ScheduleRequest, ScheduleResponse, UnRegisterRequest, UnRegisterResponse,
};

#[derive(Debug)]
struct LifetimeLease {
    id: u64,
    at: Instant,
}

#[derive(Debug, Default)]
pub struct GlobalScheduler {
    ids: Mutex<HashMap<String, AtomicU64>>,
    fn_pools: Mutex<HashMap<String, Vec<String>>>,
    lifetimes: Mutex<HashMap<String, LifetimeLease>>,
    lifetime_timeout: Duration,
}

impl GlobalScheduler {
    fn schedule_fn(&self, fn_name: &str) -> Option<String> {
        self.fn_pools
            .lock()
            .unwrap()
            .get(fn_name)?
            .choose(&mut rand::thread_rng())
            .map(|s| s.clone())
            .clone()
    }

    pub async fn serve(address: &str) {
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

        Server::builder()
            .add_service(GlobalSchedulerServiceServer::new(global_scheduler))
            .serve(address)
            .await
            .unwrap();
    }
}

#[tonic::async_trait]
impl GlobalSchedulerService for Arc<GlobalScheduler> {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let RegisterRequest { address, fn_names } = request.into_inner();

        let mut p = self.fn_pools.lock().unwrap();
        for fn_name in fn_names {
            let p = p.entry(fn_name.clone()).or_default();
            p.push(address.clone());
            p.dedup();
        }

        // store address -> lifetime_id ~forever.
        let lifetime_id = self
            .ids
            .lock()
            .unwrap()
            .entry(address)
            .or_default()
            .fetch_add(1, Ordering::SeqCst);

        Ok(Response::new(RegisterResponse { lifetime_id }))
    }

    async fn heart_beat(
        &self,
        request: Request<HeartBeatRequest>,
    ) -> Result<Response<HeartBeatResponse>, Status> {
        let HeartBeatRequest {
            address,
            lifetime_id,
        } = request.into_inner();

        let mut lifetimes = self.lifetimes.lock().unwrap();

        let lifetime_id = match lifetimes.entry(address.clone()) {
            Entry::Occupied(ref mut o) => {
                let now = Instant::now();

                let lifetime_lease = o.get_mut();

                if lifetime_id > lifetime_lease.id {
                    todo!("Error: Workers cannot have a higher lifetime id than the lease, there is a bug.");
                }
                if lifetime_id < lifetime_lease.id
                    || now.checked_duration_since(lifetime_lease.at).unwrap()
                        > self.lifetime_timeout
                {
                    lifetime_lease.id += 1;
                }

                lifetime_lease.at = now;
                lifetime_lease.id
            }
            Entry::Vacant(_) => {
                return Err(Status::invalid_argument(format!(
                    "{} does not have a lifetime lease.",
                    address
                )));
            }
        };

        let lifetimes = lifetimes
            .iter()
            .map(|(address, lifetime_lease)| Lifetime {
                address: address.to_string(),
                lifetime_id: lifetime_lease.id,
            })
            .collect();

        Ok(Response::new(HeartBeatResponse {
            lifetime_id,
            lifetimes,
        }))
    }

    async fn schedule(
        &self,
        request: Request<ScheduleRequest>,
    ) -> Result<Response<ScheduleResponse>, Status> {
        let ScheduleRequest { fn_name } = request.into_inner();

        let address = self.schedule_fn(&fn_name);

        Ok(Response::new(ScheduleResponse { address }))
    }

    async fn un_register(
        &self,
        request: Request<UnRegisterRequest>,
    ) -> Result<Response<UnRegisterResponse>, Status> {
        let _ = request;

        // Remove address from pool

        Ok(Response::new(UnRegisterResponse::default()))
    }
}
