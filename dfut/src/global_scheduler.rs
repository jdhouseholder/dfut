use std::collections::{hash_map::Entry, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status};
use tracing::error;

use crate::services::global_scheduler_service::{
    global_scheduler_service_server::{GlobalSchedulerService, GlobalSchedulerServiceServer},
    HeartBeatRequest, HeartBeatResponse, RaftStepRequest, RaftStepResponse, RuntimeInfo,
};

#[derive(Debug)]
struct LifetimeLease {
    id: u64,
    at: Instant,
}

#[derive(Debug, Default)]
struct InnerGlobalScheduler {
    max_request_id: HashMap<String, u64>,
    lifetimes: HashMap<String, LifetimeLease>,
    address_to_runtime_info: HashMap<String, RuntimeInfo>,
}

// TODO: Rename to Global Coordinator Service (or Global Control Service)
#[derive(Debug, Default)]
pub struct GlobalScheduler {
    inner: Mutex<InnerGlobalScheduler>,
    lifetime_timeout: Duration,
    heart_beat_timeout: u64,
}

pub struct GlobalSchedulerHandle {
    cancellation_token: CancellationToken,
}

impl GlobalSchedulerHandle {
    pub fn shutdown(&self) {
        self.cancellation_token.cancel();
    }
}

impl GlobalScheduler {
    async fn expire_lifetimes(self: &Arc<Self>) {
        // Proposal that expires lifetime ids based on timeout.
        // On commit: remove addresses that have expired from fn_availability.
        let mut inner = self.inner.lock().unwrap();

        let now = Instant::now();
        for lifetime_lease in inner.lifetimes.values_mut() {
            let dur_since_last_heart_beat = now.checked_duration_since(lifetime_lease.at).unwrap();
            let lifetime_id_timeout = dur_since_last_heart_beat > self.lifetime_timeout;
            if lifetime_id_timeout {
                lifetime_lease.id += 1;
            }
        }
    }

    pub async fn serve(
        address: &str,
        _peers: Vec<String>,
        lifetime_timeout: Duration,
    ) -> GlobalSchedulerHandle {
        let global_scheduler = Arc::new(Self {
            lifetime_timeout,
            heart_beat_timeout: lifetime_timeout.as_millis() as u64,
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

        let cancellation_token = CancellationToken::new();

        tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                tokio::select! {
                    _ = jh => {}
                    r = serve => r.unwrap(),
                    _ = cancellation_token.cancelled() => {}
                }
            }
        });

        GlobalSchedulerHandle { cancellation_token }
    }
}

#[tonic::async_trait]
impl GlobalSchedulerService for Arc<GlobalScheduler> {
    async fn heart_beat(
        &self,
        request: Request<HeartBeatRequest>,
    ) -> Result<Response<HeartBeatResponse>, Status> {
        let HeartBeatRequest {
            request_id,
            address,
            current_runtime_info,
        } = request.into_inner();
        let Some(current_runtime_info) = current_runtime_info else {
            return Err(Status::invalid_argument("missing runtime_info"));
        };

        let mut inner = self.inner.lock().unwrap();

        // TODO: track max request_id and if < max then return error.
        let max_request_id = inner.max_request_id.entry(address.clone()).or_default();
        if request_id < *max_request_id {
            return Err(Status::cancelled("Old request id."));
        }
        *max_request_id = request_id;
        let next_expected_request_id = *max_request_id + 1;

        let current_lifetime_id = current_runtime_info.lifetime_id;

        inner
            .address_to_runtime_info
            .insert(address.clone(), current_runtime_info);

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
            Entry::Vacant(v) => {
                v.insert(LifetimeLease {
                    id: 0,
                    at: Instant::now(),
                });
                0
            }
        };

        Ok(Response::new(HeartBeatResponse {
            lifetime_id,
            next_expected_request_id,
            heart_beat_timeout: self.heart_beat_timeout,
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
