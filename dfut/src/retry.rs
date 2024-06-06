use std::{future::Future, time::Duration};

use rand::Rng;
use tokio::time::sleep;
use tonic::{transport::Endpoint, Code, Response, Status};
use tracing::{error, trace};

use crate::{
    client_pool::{ClientPool, Connect},
    Error,
};

const RETRIES: u32 = 5;

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
    let mut maybe_client = pool.get_client(address).await;

    for i in 0..RETRIES {
        if let Ok(client) = maybe_client {
            match f(client).await {
                Ok(resp) => return Ok(resp.into_inner()),
                Err(e) => match e.code() {
                    Code::NotFound | Code::InvalidArgument => {
                        trace!("rpc_with_retry: {e:?}");
                        return Err(Error::System);
                    }
                    _ => error!("rpc_with_retry: {e:?}"),
                },
            }
        }

        let jitter = rand::thread_rng().gen_range(0..100);
        sleep(Duration::from_millis(100 * 2u64.pow(i) - jitter)).await;

        maybe_client = pool.connect(address).await;
    }

    Err(Error::System)
}

pub async fn retry<C, F, Fut, T>(
    client: &mut Option<C>,
    endpoint: &Endpoint,
    f: F,
) -> Result<T, Error>
where
    C: Connect,
    F: Fn(C) -> Fut,
    Fut: Future<Output = Result<Response<T>, Status>>,
{
    if client.is_none() {
        *client = C::connect(endpoint.clone()).await.ok();
    }
    for i in 0..RETRIES {
        if let Some(client) = client {
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
        }

        let jitter = rand::thread_rng().gen_range(0..100);
        sleep(Duration::from_millis(100 * 2u64.pow(i) - jitter)).await;

        *client = C::connect(endpoint.clone()).await.ok();
    }

    Err(Error::System)
}
