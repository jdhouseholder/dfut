#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Error {
    System,
}

#[derive(Debug, Clone, Default)]
pub struct WorkerServerConfig {
    pub local_server_address: String,
    pub global_scheduler_address: String,
    pub fn_names: Vec<String>,
}

mod d_fut;
mod d_scheduler;
mod d_store;
mod gaps;
mod global_scheduler;
mod peer_work;
mod runtime;
mod services;
mod sleep;
mod stopwatch;
mod work;

// Try is experimental so we can't create our own type that usese the ? operator.
pub type DResult<T> = Result<T, Error>;

pub use d_fut::DFut;
pub use d_scheduler::Where;
pub use global_scheduler::GlobalScheduler;
pub use runtime::{RootRuntime, RootRuntimeClient, Runtime, RuntimeClient};
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
