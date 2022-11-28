pub mod api;
mod callback;
mod dispatcher;
mod error;
mod storage;

use std::sync::Arc;

use tokio::runtime::Runtime;
use tokio::sync::mpsc::channel;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    RwLock,
};

pub use api::ServerConfig;
pub use error::ControllerError;
use tracing::instrument;

use crate::config::CHANNEL_SIZE;
use crate::datastore::TaskDataStore;
use crate::executor::ActionExecutionOutcome;
use crate::model::{Action, Task};

use self::storage::RequestResponse;

type Shared<D> = Arc<RwLock<D>>;

#[derive(Debug)]
pub enum ControllerRequest {
    SubmitTask(Task, oneshot::Sender<Result<TaskId, ControllerError>>),
    CancelTask(TaskId, oneshot::Sender<Result<TaskId, ControllerError>>),
    RetryTask(TaskId, oneshot::Sender<Result<TaskId, ControllerError>>),
    CloneTask(TaskId, oneshot::Sender<Result<TaskId, ControllerError>>),
}

pub type TaskId = String;
pub type ControllerRequestPublisher = mpsc::Sender<ControllerRequest>;

pub struct TaskController<D>
where
    D: TaskDataStore,
{
    _datastore: Shared<D>,
    _storage_runtime: Runtime,
    _api_runtime: Runtime,
    _callback_runtime: Runtime,
    _dispatcher_runtime: Runtime,
}

impl<D> TaskController<D>
where
    D: TaskDataStore + std::marker::Send + 'static + std::marker::Sync,
{
    #[instrument(skip(datastore, tx_action, rx_execution_status, cfg))]
    pub fn start(
        datastore: Shared<D>,
        tx_action: Sender<Action>,
        rx_execution_status: Receiver<ActionExecutionOutcome>,
        cfg: ServerConfig,
    ) -> Self {
        let (tx_controller_request, rx_controller_request) =
            mpsc::channel::<ControllerRequest>(CHANNEL_SIZE);

        let (tx_storage, rx_storage) = channel::<RequestResponse>(CHANNEL_SIZE);
        let storage_service = storage::StorageService::new(datastore.clone(), rx_storage);
        let dispatcher_service = dispatcher::DispatcherService::new(
            rx_controller_request,
            tx_action.clone(),
            tx_storage.clone(),
        );

        Self {
            _datastore: datastore.clone(),
            _storage_runtime: storage_service.build_runtime(),
            _api_runtime: api::build_runtime(
                cfg,
                tx_controller_request.clone(),
                tx_storage.clone(),
            ),
            _callback_runtime: callback::build_runtime(
                tx_storage.clone(),
                rx_execution_status,
                tx_action.clone(),
            ),
            _dispatcher_runtime: dispatcher_service.build_runtime(),
        }
    }

    pub fn stop(self) -> Result<(), ControllerError> {
        self._api_runtime.shutdown_background();
        self._dispatcher_runtime
            .shutdown_timeout(std::time::Duration::from_millis(20));
        self._callback_runtime
            .shutdown_timeout(std::time::Duration::from_millis(100));
        std::thread::sleep(std::time::Duration::from_millis(100));
        self._storage_runtime
            .shutdown_timeout(std::time::Duration::from_millis(10));
        Ok(())
    }
}
