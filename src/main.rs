#[macro_use]
mod config;
mod controller;
mod crash_handler;
mod datastore;
mod executor;
mod log;
mod model;

use tokio::sync::{mpsc, RwLock};

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

use crate::controller::ServerConfig;

fn main() -> Result<(), std::io::Error> {
    dotenv().ok();
    let env_filter = EnvFilter::try_from_env("TASKQUEUE_LOG");
    log::setup(env_filter);

    let term = Arc::new(AtomicBool::new(false));
    crash_handler::setup_panic_handler();

    event!(Level::INFO, "Starting TaskQueue: {}", env!("FULL_VERSION"));

    let (tx_action, rx_action) = mpsc::channel::<Action>(config::CHANNEL_SIZE);
    let mut executor = ActionExecutor::new();
    let (rx_execution_status, _executor_runtime) = executor.start(rx_action).unwrap();

    let storage = crate::datastore::MemoryTaskStorage::new();
    let datastore = datastore::HashMapStorage::new(storage);
    let shared_datastore = Arc::new(RwLock::new(datastore));
    let mut controller = controller::TaskController::start(
        shared_datastore.clone(),
        tx_action,
        rx_execution_status,
        ServerConfig {
            port: 8001,
            concurrent: Some(1000),
            timeout: Some(30),
        },
    );

    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&term))?;
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&term))?;

    while !term.load(Ordering::Relaxed) {
        // std::thread::park();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    event!(Level::INFO, "Shutting down TaskQueue.");

    controller.stop().unwrap();
    Ok(())
}

#[cfg(all(test, feature = "e2e"))]
mod e2e_tests;
