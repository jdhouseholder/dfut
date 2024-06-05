use std::cmp::Ordering;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

use protobuf::Message as MessageTrait;
use raft::{
    eraftpb::{
        ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, Entry as RaftEntry,
        EntryType, Message, Snapshot,
    },
    raw_node::RawNode,
    storage::MemStorage,
    Config, StateRole,
};
use serde::{Deserialize, Serialize};
use slog::{o, Drain};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status};
use tracing::error;

use crate::services::global_scheduler_service::{
    global_scheduler_service_client::GlobalSchedulerServiceClient,
    global_scheduler_service_server::{GlobalSchedulerService, GlobalSchedulerServiceServer},
    raft_step_request::Inner,
    HeartBeatRequest, HeartBeatResponse, RaftStepRequest, RaftStepResponse, RuntimeInfo,
};

// Since heart beat timeouts are large (on the order of seconds) we can probably just use epoch
// time to simplify failover.
fn now_epoch_time_ms() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn checked_duration_since(a: u128, b: u128) -> u128 {
    a.checked_sub(b).unwrap()
}

enum Proposal {
    State(Vec<u8>),
    ConfChange(Vec<u8>),
    Message(Vec<u8>),
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
enum At {
    Instant(u128),
    #[default]
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LifetimeLease {
    id: u64,
    at: At,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ReplicatedGlobalSchedulerState {
    max_request_id: HashMap<String, u64>,
    lifetimes: HashMap<String, LifetimeLease>,
    address_to_runtime_info: HashMap<String, RuntimeInfo>,
}

#[derive(Debug, Clone, Default)]
enum LeaderState {
    IAm,
    TheyAre(String),
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Default)]
struct InnerGlobalScheduler {
    leader_state: LeaderState,
    replicated: ReplicatedGlobalSchedulerState,
}

// TODO: Rename to Global Coordinator Service (or Global Control Service)
#[derive(Debug)]
pub struct GlobalScheduler {
    id: u64,
    address: String,
    inner: Mutex<InnerGlobalScheduler>,
    lifetime_timeout: u128,
    heart_beat_timeout: u64,
    raft_tx: Sender<Proposal>,
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
    fn maybe_propose(&self, inner: &MutexGuard<'_, InnerGlobalScheduler>) {
        match inner.leader_state {
            LeaderState::IAm => {
                let b = bincode::serialize(&inner.replicated).unwrap();
                let _ = self.raft_tx.try_send(Proposal::State(b));
            }
            LeaderState::TheyAre(_) => {}
            LeaderState::Unknown => {}
        }
    }

    async fn expire_lifetimes_forever(self: &Arc<Self>) {
        loop {
            // Proposal that expires lifetime ids based on timeout.
            // On commit: remove addresses that have expired from fn_availability.
            {
                let mut inner = self.inner.lock().unwrap();

                let mut changes = false;

                let now = now_epoch_time_ms();
                for lifetime_lease in inner.replicated.lifetimes.values_mut() {
                    match lifetime_lease.at {
                        At::Expired => {}
                        At::Instant(epoch_time_ms) => {
                            let dur_since_last_heart_beat =
                                checked_duration_since(now, epoch_time_ms);
                            if dur_since_last_heart_beat > self.lifetime_timeout {
                                lifetime_lease.id += 1;
                                lifetime_lease.at = At::Expired;

                                changes = true;
                            }
                        }
                    }
                }
                if changes {
                    self.maybe_propose(&inner);
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    pub async fn serve(
        id: u64,
        address: &str,
        peers: HashMap<u64, String>,
        lifetime_timeout: Duration,
    ) -> GlobalSchedulerHandle {
        let (tx, rx) = channel::<Proposal>(100);

        let global_scheduler = Arc::new(Self {
            id,
            address: address.to_string(),
            inner: Mutex::default(),
            lifetime_timeout: lifetime_timeout.as_millis(),
            heart_beat_timeout: lifetime_timeout.as_millis() as u64,
            raft_tx: tx,
        });

        let address = address
            .strip_prefix("http://")
            .unwrap()
            .to_string()
            .parse()
            .unwrap();

        let cancellation_token = CancellationToken::new();

        let _expire_jh = tokio::spawn({
            let global_scheduler = Arc::clone(&global_scheduler);
            let cancellation_token = cancellation_token.clone();
            async move {
                tokio::select! {
                    _ = global_scheduler.expire_lifetimes_forever() => {}
                    _ = cancellation_token.cancelled() => {}
                }
            }
        });

        if peers.is_empty() {
            let mut inner = global_scheduler.inner.lock().unwrap();
            inner.leader_state = LeaderState::IAm;
        } else {
            let _raft_jh = tokio::spawn({
                let global_scheduler = Arc::clone(&global_scheduler);
                let peers = peers.clone();
                let cancellation_token = cancellation_token.clone();
                async move {
                    tokio::select! {
                        _ = global_scheduler.raft_forever(id, peers, rx) => {}
                        _ = cancellation_token.cancelled() => {}
                    }
                }
            });
        }

        let _serve_jh = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                tokio::select! {
                _ = Server::builder()
                    .add_service(GlobalSchedulerServiceServer::new(global_scheduler))
                    .serve(address) => {},
                _ = cancellation_token.cancelled() => {}
                }
            }
        });

        GlobalSchedulerHandle { cancellation_token }
    }

    // TODO: cache + parallel
    async fn send_to_peers(&self, peers: &HashMap<u64, String>, msg: Message) {
        let msg = msg.write_to_bytes().unwrap();

        for peer in peers.values() {
            println!("Sending from {} to {}", self.id, peer);

            let endpoint: tonic::transport::Endpoint = peer.parse().unwrap();
            let mut client = GlobalSchedulerServiceClient::connect(endpoint)
                .await
                .unwrap();
            client
                .raft_step(RaftStepRequest {
                    inner: Some(Inner::Msg(msg.clone())),
                })
                .await
                .unwrap();
        }
    }

    async fn handle_messages(&self, peers: &HashMap<u64, String>, msgs: Vec<Message>) {
        for msg in msgs {
            self.send_to_peers(peers, msg).await;
        }
    }

    fn handle_committed_entries(
        &self,
        node: &mut RawNode<MemStorage>,
        committed_entries: Vec<RaftEntry>,
    ) {
        let mut _last_apply_index = 0;
        for entry in committed_entries {
            // Mostly, you need to save the last apply index to resume applying
            // after restart. Here we just ignore this because we use a Memory storage.
            _last_apply_index = entry.index;

            if entry.data.is_empty() {
                // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    let state: ReplicatedGlobalSchedulerState =
                        bincode::deserialize(entry.data.as_ref()).unwrap();
                    self.inner.lock().unwrap().replicated = state;
                }
                EntryType::EntryConfChange => todo!("handle_conf_change(entry)"),
                EntryType::EntryConfChangeV2 => {
                    let mut cc = ConfChangeV2::default();
                    cc.transition = ConfChangeTransition::Explicit;
                    cc.merge_from_bytes(&entry.data).unwrap();
                    let cs = node.apply_conf_change(&cc).unwrap();
                    node.mut_store().wl().set_conf_state(cs);
                }
            }
        }
    }

    async fn raft_forever(&self, id: u64, peers: HashMap<u64, String>, mut rx: Receiver<Proposal>) {
        let logger = slog::Logger::root(slog_stdlog::StdLog.fuse(), o!());

        let config = Config {
            id,
            ..Default::default()
        };
        config.validate().unwrap();

        let storage = MemStorage::new();

        if id == 1 {
            let mut s = Snapshot::default();
            s.mut_metadata().index = 1;
            s.mut_metadata().term = 1;
            s.mut_metadata().mut_conf_state().voters = vec![1];
            storage.wl().apply_snapshot(s).unwrap();
        } else {
            tokio::spawn({
                let peers = peers.clone();
                async move {
                    let mut cc = ConfChangeV2::default();
                    for id in peers.keys() {
                        if *id != 1 {
                            let mut ccs = ConfChangeSingle::new();
                            ccs.change_type = ConfChangeType::AddNode;
                            ccs.node_id = *id;
                            cc.changes.push(ccs);
                        }
                    }
                    let b = cc.write_to_bytes().unwrap();
                    for peer in peers.values() {
                        let endpoint: tonic::transport::Endpoint = peer.parse().unwrap();
                        let mut client = GlobalSchedulerServiceClient::connect(endpoint)
                            .await
                            .unwrap();
                        loop {
                            if let Ok(_) = client
                                .raft_step(RaftStepRequest {
                                    inner: Some(Inner::Cc(b.clone())),
                                })
                                .await
                            {
                                break;
                            }
                        }
                    }
                }
            });
        }

        let mut node = RawNode::new(&config, storage, &logger).unwrap();

        let timeout = Duration::from_millis(100);
        let mut remaining_timeout = timeout;

        loop {
            let now = Instant::now();

            match tokio::time::timeout(remaining_timeout, rx.recv()).await {
                Ok(Some(Proposal::State(local))) => {
                    let _ = node.propose(vec![], local);
                }
                Ok(Some(Proposal::ConfChange(bytes))) => {
                    let cc = ConfChangeV2::parse_from_bytes(&bytes).unwrap();
                    let _ = node.propose_conf_change(vec![], cc);
                }
                Ok(Some(Proposal::Message(bytes))) => {
                    let m = Message::parse_from_bytes(&bytes).unwrap();
                    node.step(m).unwrap()
                }
                Ok(None) | Err(_) => {}
            }

            let elapsed = now.elapsed();
            if elapsed >= remaining_timeout {
                remaining_timeout = timeout;
                // We drive Raft every 100ms.
                node.tick();
            } else {
                remaining_timeout -= elapsed;
            }

            {
                let mut inner = self.inner.lock().unwrap();
                //println!("{:?}: {:?}", self.id, inner);
                tracing::error!("{id}: {:?}", inner);
                if node.raft.state == StateRole::Leader {
                    //println!("{id} IAM THE LEADER");
                    inner.leader_state = LeaderState::IAm;
                } else {
                    let id = node.raft.leader_id;
                    if let Some(address) = peers.get(&id) {
                        inner.leader_state = LeaderState::TheyAre(address.to_string());
                    }
                }
            }

            if node.has_ready() {
                let mut ready = node.ready();
                if !ready.messages().is_empty() {
                    self.handle_messages(&peers, ready.take_messages()).await;
                }
                if !ready.snapshot().is_empty() {
                    // This is a snapshot, we need to apply the snapshot at first.
                    node.mut_store()
                        .wl()
                        .apply_snapshot(ready.snapshot().clone())
                        .unwrap();
                }

                self.handle_committed_entries(&mut node, ready.take_committed_entries());

                if !ready.entries().is_empty() {
                    // Append entries to the Raft log
                    node.mut_store().wl().append(ready.entries()).unwrap();
                }

                if let Some(hs) = ready.hs() {
                    // Raft HardState changed, and we need to persist it.
                    node.mut_store().wl().set_hardstate(hs.clone());
                }

                if !ready.persisted_messages().is_empty() {
                    self.handle_messages(&peers, ready.take_persisted_messages())
                        .await;
                }

                let mut light_rd = node.advance(ready);
                if let Some(commit) = light_rd.commit_index() {
                    node.mut_store().wl().mut_hard_state().set_commit(commit);
                }

                self.handle_messages(&peers, light_rd.take_messages()).await;
                self.handle_committed_entries(&mut node, light_rd.take_committed_entries());

                node.advance_apply();
            }
        }
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

        let leader_address = match &inner.leader_state {
            LeaderState::TheyAre(address) => {
                return Ok(Response::new(HeartBeatResponse {
                    leader_address: Some(address.to_string()),
                    ..Default::default()
                }));
            }
            LeaderState::IAm => None,
            LeaderState::Unknown => Some(self.address.to_string()),
        };

        // TODO: track max request_id and if < max then return error.
        let max_request_id = inner
            .replicated
            .max_request_id
            .entry(address.clone())
            .or_default();
        if request_id < *max_request_id {
            return Err(Status::cancelled("Old request id."));
        }
        *max_request_id = request_id;
        let next_expected_request_id = *max_request_id + 1;

        let current_lifetime_id = current_runtime_info.lifetime_id;

        inner
            .replicated
            .address_to_runtime_info
            .insert(address.clone(), current_runtime_info);

        // TODO: new failed_tasks have a ttl of 2 * HBTO and can then be removed.
        // TODO: use low watermark from worker to avoid having to failed_tasks forever.

        let lifetime_id = match inner.replicated.lifetimes.entry(address.clone()) {
            Entry::Occupied(ref mut o) => {
                let now = now_epoch_time_ms();

                let lifetime_lease = o.get_mut();

                match u64::cmp(&current_lifetime_id, &lifetime_lease.id) {
                    Ordering::Greater => {
                        todo!("Error: Workers cannot have a higher lifetime id than the lease, there is a bug.");
                    }
                    Ordering::Less => {
                        error!(
                            "expired_lifetime_id: got={}, want={}",
                            current_lifetime_id, lifetime_lease.id
                        );
                        lifetime_lease.id
                    }
                    Ordering::Equal => {
                        match lifetime_lease.at {
                            At::Instant(epoch_time_ms) => {
                                let dur_since_last_heart_beat =
                                    checked_duration_since(now, epoch_time_ms);
                                let lifetime_id_timeout =
                                    dur_since_last_heart_beat > self.lifetime_timeout;

                                if lifetime_id_timeout {
                                    error!(
                                        "lifetime_id_timeout: dur_since_last_heart_beat={:?}",
                                        dur_since_last_heart_beat
                                    );
                                    lifetime_lease.id += 1;
                                }

                                lifetime_lease.at = At::Instant(now_epoch_time_ms());
                            }
                            At::Expired => {}
                        }
                        lifetime_lease.id
                    }
                }
            }
            Entry::Vacant(v) => {
                v.insert(LifetimeLease {
                    id: 0,
                    at: At::Instant(now_epoch_time_ms()),
                });
                0
            }
        };

        self.maybe_propose(&inner);

        Ok(Response::new(HeartBeatResponse {
            leader_address,
            lifetime_id,
            next_expected_request_id,
            heart_beat_timeout: self.heart_beat_timeout,
            address_to_runtime_info: inner.replicated.address_to_runtime_info.clone(),
        }))
    }

    async fn raft_step(
        &self,
        request: Request<RaftStepRequest>,
    ) -> Result<Response<RaftStepResponse>, Status> {
        println!("raft_step");
        let RaftStepRequest { inner } = request.into_inner();
        match inner {
            Some(Inner::Msg(msg)) => {
                self.raft_tx.send(Proposal::Message(msg)).await.unwrap();
            }
            Some(Inner::Cc(cc)) => {
                self.raft_tx.send(Proposal::ConfChange(cc)).await.unwrap();
            }
            None => {}
        }
        Ok(Response::new(RaftStepResponse::default()))
    }
}
