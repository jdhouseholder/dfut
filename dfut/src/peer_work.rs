use tonic::transport::Channel;

use crate::{
    client_pool::ClientPool,
    d_store::DStoreId,
    retry::retry_from_pool,
    services::worker_service::{
        worker_service_client::WorkerServiceClient, DoWorkRequest, ParentInfo,
    },
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
        parent_info: &[ParentInfo],
        address: &str,
        w: Work,
    ) -> DResult<DStoreId> {
        retry_from_pool(&self.pool, address, |mut client| {
            let w = w.clone();
            async move {
                client
                    .do_work(DoWorkRequest {
                        parent_info: parent_info.to_vec(),
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
