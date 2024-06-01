use crate::{seq::Seq, services::global_scheduler_service::RequestId};
use std::sync::{Arc, Mutex};

pub struct RpcContext {
    pub local_address: String,
    pub lifetime_id: u64,
    pub next_request_id: Arc<Mutex<Seq>>,
    pub requests: Arc<Mutex<Vec<RequestId>>>,
}

impl RpcContext {
    pub fn next_request_id(&self, address: &str) -> u64 {
        let id = self.next_request_id.lock().unwrap().next(address);
        self.requests.lock().unwrap().push(RequestId {
            address: address.to_string(),
            request_id: id,
        });
        id
    }
}
