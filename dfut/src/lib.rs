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
mod global_scheduler;
mod runtime;
mod timer;
mod work;

pub use d_fut::DFut;
pub use d_scheduler::worker_service::{
    worker_service_client::WorkerServiceClient,
    worker_service_server::{WorkerService, WorkerServiceServer},
    DoWorkRequest, DoWorkResponse,
};
pub use global_scheduler::GlobalScheduler;
pub use runtime::{RootRuntime, Runtime, RuntimeClient};
pub use work::{IntoWork, Work};

pub use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub use bincode;
pub use serde;
pub use size_ser;
pub use tonic;

extern crate dfut_macro;
pub use dfut_macro::{d_await, d_box, d_cancel, into_dfut};
