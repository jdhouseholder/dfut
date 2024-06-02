use tonic::transport::Channel;

use crate::{
    client_pool::ClientPool,
    d_store::DStoreId,
    retry::rpc_with_retry,
    services::worker_service::{worker_service_client::WorkerServiceClient, DoWorkRequest},
    work::Work,
    DResult,
};

#[derive(Debug, Clone)]
pub(crate) struct PeerWorkerClient {
    pool: ClientPool<WorkerServiceClient<Channel>>,
}

impl PeerWorkerClient {
    pub(crate) fn new() -> Self {
        Self {
            pool: ClientPool::new(),
        }
    }

    pub(crate) async fn do_work(
        &self,
        current_address: &str,
        current_lifetime_id: u64,
        current_task_id: u64,
        request_id: u64,
        address: &str,
        w: Work,
    ) -> DResult<DStoreId> {
        rpc_with_retry(&self.pool, address, |mut client| {
            let w = w.clone();
            async move {
                client
                    .do_work(DoWorkRequest {
                        parent_address: current_address.to_string(),
                        parent_lifetime_id: current_lifetime_id,
                        parent_task_id: current_task_id,
                        request_id,
                        fn_name: w.fn_name,
                        args: w.args,
                    })
                    .await
            }
        })
        .await
        .map(|resp| resp.into())
    }
}
