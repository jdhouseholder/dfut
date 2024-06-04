#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Error {
    System,
}

mod client_pool;
mod consts;
mod d_fut;
mod d_scheduler;
mod d_store;
mod fn_index;
mod gaps;
mod global_scheduler;
mod peer_work;
mod retry;
mod rpc_context;
mod runtime;
mod seq;
mod services;
mod sleep;
mod stopwatch;
mod work;

// Try is experimental so we can't create our own type that usese the ? operator.
pub type DResult<T> = Result<T, Error>;

pub use d_fut::DFut;
pub use d_scheduler::Where;
pub use global_scheduler::GlobalScheduler;
pub use runtime::{
    client::{RootRuntimeClient, RuntimeClient},
    runtime::{RootRuntime, RootRuntimeHandle, Runtime, WorkerServerConfig, WorkerServiceExt},
};
pub use services::worker_service::{
    worker_service_client::WorkerServiceClient,
    worker_service_server::{WorkerService, WorkerServiceServer},
    DoWorkRequest, DoWorkResponse,
};
pub use work::{IntoWork, Work};

pub use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub use bincode;
pub use serde;
pub use size_ser;
pub use tonic;

extern crate dfut_macro;
pub use dfut_macro::{d_await, d_box, d_cancel, into_dfut};
