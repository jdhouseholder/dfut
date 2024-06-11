use std::{future::Future, time::Duration};

use rand::Rng;
use tokio::time::sleep;
use tonic::{Code, Response, Status};
use tracing::{error, instrument};

use crate::{
    client_pool::{ClientPool, Connect},
    Error,
};

const RETRIES: u32 = 10;

#[instrument(skip_all)]
pub async fn retry_from_pool<C, F, Fut, T>(
    pool: &ClientPool<C>,
    address: &str,
    f: F,
) -> Result<T, Error>
where
    C: Connect,
    F: Fn(C) -> Fut,
    Fut: Future<Output = Result<Response<T>, Status>>,
{
    for i in 0..RETRIES {
        if let Ok(client) = pool.get_client(address).await {
            match f(client).await {
                Ok(resp) => return Ok(resp.into_inner()),
                Err(e) => match e.code() {
                    Code::NotFound | Code::InvalidArgument => {
                        error!("rpc_with_retry: {e:?}");
                        return Err(Error::System);
                    }
                    _ => error!("rpc_with_retry: {e:?}"),
                },
            }
        }

        let jitter = rand::thread_rng().gen_range(0..100);
        sleep(Duration::from_millis(100 * 2u64.pow(i) - jitter)).await;

        tracing::trace!("retrying from pool to {address}");
    }

    error!("rpc_with_retry: too many retries");

    Err(Error::System)
}

#[instrument(skip_all)]
pub async fn retry<C, F, Fut, T>(client: &C, f: F) -> Result<T, Error>
where
    C: Clone,
    F: Fn(C) -> Fut,
    Fut: Future<Output = Result<Response<T>, Status>>,
{
    for i in 0..RETRIES {
        match f(client.clone()).await {
            Ok(resp) => return Ok(resp.into_inner()),
            Err(e) => match e.code() {
                Code::NotFound | Code::InvalidArgument => {
                    error!("rpc_with_retry: {e:?}");
                    return Err(Error::System);
                }
                _ => error!("rpc_with_retry: {e:?}"),
            },
        }

        let jitter = rand::thread_rng().gen_range(0..100);
        sleep(Duration::from_millis(100 * 2u64.pow(i) - jitter)).await;

        tracing::trace!("retrying");
    }

    error!("retry: too many retries");

    Err(Error::System)
}
