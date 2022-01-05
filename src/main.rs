#[macro_use]
mod api;
mod controller;
mod datastore;
mod executor;
mod log;
mod model;

use tokio::sync::{mpsc, Mutex, RwLock};

use dotenv::dotenv;
use std::env;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tracing::{event, Level};
use tracing_subscriber::EnvFilter;

use executor::{ActionExecutor, Executor};
use model::Action;

fn main() {
    dotenv().ok();
    let env_filter = EnvFilter::try_from_env("TASKQUEUE_LOG");
    log::setup(false, env_filter, &None);

    event!(Level::INFO, "Starting TaskQueue: {}", env!("FULL_VERSION"));

    let (tx_action, rx_action) = mpsc::channel::<Action>(32);
    let mut executor = ActionExecutor::new();
    let (rx_execution_status, _executor_runtime) = executor.start(rx_action).unwrap();

    let storage = crate::datastore::MemoryTaskStorage::new();
    let datastore = datastore::HashMapStorage::new(storage);
    let shared_datastore = Arc::new(RwLock::new(datastore));
    let mut controller =
        controller::TaskController::start(shared_datastore.clone(), tx_action, rx_execution_status);
    let term = Arc::new(AtomicBool::new(false));

    while !term.load(Ordering::Acquire) {
        std::thread::park();
    }
    controller.stop().unwrap();
    executor.stop().unwrap();
}

#[cfg(all(test, feature = "e2e"))]
mod e2e_tests;
