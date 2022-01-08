mod error;

use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::{Builder, Runtime};
use tokio::sync::{mpsc, oneshot};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        RwLock,
    },
    time,
};

use anyhow::Result;

pub use error::ControllerError;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::api::Server;
use crate::datastore::TaskDataStore;
use crate::executor::{self, ActionExecutionOutcome};
use crate::model::{Action, Task, TaskState};

type Shared<D> = Arc<RwLock<D>>;

#[derive(Debug)]
pub enum ControllerRequest {
    SubmitTask(Task, oneshot::Sender<Result<TaskId>>),
    CancelTask(TaskId, oneshot::Sender<Result<TaskId>>),
}

pub type TaskId = String;
pub type ControllerRequestPublisher = mpsc::Sender<ControllerRequest>;

pub struct TaskController<D>
where
    D: TaskDataStore,
{
    _datastore: Shared<D>,
    _datastore_runtime: Runtime,
    _api_runtime: Runtime,
    _callback_runtime: Runtime,
    _dispatcher_runtime: Runtime,
}

impl<D> TaskController<D>
where
    D: TaskDataStore + std::marker::Send + 'static + std::marker::Sync,
{
    pub fn start(
        datastore: Shared<D>,
        tx_action: Sender<Action>,
        rx_execution_status: Receiver<executor::ActionExecutionOutcome>,
    ) -> Self {
        let (tx_controller_request, rx_controller_request) = mpsc::channel::<ControllerRequest>(32);
        Self {
            _datastore: datastore.clone(),
            _datastore_runtime: Self::build_datastore_runtime(datastore.clone()),
            _api_runtime: Self::build_api_runtime(datastore.clone(), tx_controller_request.clone()),
            _callback_runtime: Self::build_callback_runtime(
                datastore.clone(),
                rx_execution_status,
                tx_action.clone(),
            ),
            _dispatcher_runtime: Self::build_dispatcher_runtime(
                datastore.clone(),
                tx_action,
                rx_controller_request,
            ),
        }
    }

    pub fn stop(&mut self) -> Result<(), ControllerError> {
        // not implemented
        Ok(())
    }

    fn build_datastore_runtime(datastore: Shared<D>) -> Runtime {
        let runtime = Builder::new_multi_thread()
            .thread_name("datastore")
            .enable_all()
            .build()
            .expect("[datastore] failed to create runtime");
        {
            runtime.handle().spawn(async move {
                datastore.clone().write().await.load_tasks().await;
            });
        }
        runtime
    }

    fn build_api_runtime(datastore: Shared<D>, tx_request: Sender<ControllerRequest>) -> Runtime {
        let runtime = Builder::new_multi_thread()
            .thread_name("grpc-api")
            .enable_all()
            .build()
            .expect("[grpc-api] failed to create runtime");

        let api = Server::new(datastore, tx_request);
        runtime.handle().spawn(async { api.start().await });
        runtime
    }

    fn build_callback_runtime(
        datastore: Shared<D>,
        rx_execution_status: Receiver<executor::ActionExecutionOutcome>,
        tx_action: Sender<Action>,
    ) -> Runtime {
        let runtime = Builder::new_multi_thread()
            .thread_name("callback")
            .enable_all()
            .build()
            .expect("[callback] failed to create runtime");

        let handle_callback = Self::handle_callback(datastore, rx_execution_status, tx_action);

        runtime.handle().spawn(handle_callback);
        runtime
    }

    fn build_dispatcher_runtime(
        datastore: Shared<D>,
        tx_action: Sender<Action>,
        rx_controller_request: Receiver<ControllerRequest>,
    ) -> Runtime {
        let runtime = Builder::new_multi_thread()
            .thread_name("dispatcher")
            .enable_all()
            .build()
            .expect("[dispatcher] failed to create runtime");

        runtime
            .handle()
            .spawn(Self::periodic_state_dump(datastore.clone()));

        runtime
            .handle()
            .spawn(Self::auto_retry_tasks(datastore.clone(), tx_action.clone()));

        let handle_dispatch = Self::handle_dispatch(datastore, rx_controller_request, tx_action);
        runtime.handle().spawn(handle_dispatch);

        runtime
    }

    async fn handle_dispatch(
        datastore: Shared<D>,
        mut rx_controller_request: Receiver<ControllerRequest>,
        tx_action: Sender<Action>,
    ) {
        info!("Starting dispatch loop.");

        while let Some(msg) = rx_controller_request.recv().await {
            match msg {
                ControllerRequest::SubmitTask(task, tx_callback) => {
                    let ds_clone = datastore.clone();
                    let tx_action_clone = tx_action.clone();

                    tokio::spawn(async move {
                        if match Self::send_to_executor(ds_clone, tx_action_clone, task, None).await
                        {
                            Ok(uuid) => {
                                info!(uuid = %uuid.to_hyphenated().to_string(), "callback");
                                tx_callback.send(Ok(uuid.to_hyphenated().to_string()))
                            }
                            Err(err) => tx_callback.send(Err(err)),
                        }
                        .is_err()
                        {
                            error!("Task send to executor, but response to api failed")
                        }
                    });
                }
                ControllerRequest::CancelTask(_, _) => todo!(),
            }
        }
        info!("Finishing dispatch loop.");
    }

    async fn periodic_state_dump(datastore: Shared<D>) {
        let mut interval_15mins = time::interval(Duration::from_secs(60 * 15));

        info!("periodic_state_dump worker started");

        loop {
            let _ = interval_15mins.tick().await;
            let mut stats: [usize; 6] = [0, 0, 0, 0, 0, 0];
            let mut filter_tasks = crate::datastore::Filter::new_tasks();
            stats[0] = datastore.read().await.items(&filter_tasks).await.len();

            filter_tasks.state = Some(crate::model::TaskState::Processing);
            stats[1] = datastore.read().await.items(&filter_tasks).await.len();

            filter_tasks.state = Some(TaskState::Inprogress);
            stats[2] = datastore.read().await.items(&filter_tasks).await.len();

            filter_tasks.state = Some(TaskState::Failed);
            stats[3] = datastore.read().await.items(&filter_tasks).await.len();

            filter_tasks.state = Some(TaskState::Success);
            stats[4] = datastore.read().await.items(&filter_tasks).await.len();

            filter_tasks.state = Some(TaskState::Died);
            stats[5] = datastore.read().await.items(&filter_tasks).await.len();

            info!("Datastore items stats: {:?}", stats);
        }
    }

    async fn auto_retry_tasks(datastore: Shared<D>, tx_action: Sender<Action>) {
        let filter_failed_tasks = crate::datastore::Filter::failed_tasks();
        let filter_inprogress_tasks = crate::datastore::Filter::inprogress_tasks();
        info!("poll_retry_tasks worker started");

        let mut interval = time::interval(Duration::from_micros(1000));
        loop {
            interval.tick().await;
            let mut tasks: Vec<Task>;
            tasks = datastore.read().await.items(&filter_failed_tasks).await;
            let mut tasks_inprogress = datastore.read().await.items(&filter_inprogress_tasks).await;
            tasks.append(&mut tasks_inprogress);

            // TODO try futures::future::join_all(
            while let Some(task) = tasks.pop() {
                if let Some(_) = task.can_retry() {
                    let retries = task.retries;
                    if let Err(err) = Self::send_to_executor(
                        datastore.clone(),
                        tx_action.clone(),
                        task,
                        Some(retries),
                    )
                    .await
                    {
                        error!(reason=%err, "Retry failed.")
                    }
                }
            }
        }
    }

    async fn handle_callback(
        ds: Shared<D>,
        mut rx_execution_status: Receiver<executor::ActionExecutionOutcome>,
        tx_action: Sender<Action>,
    ) {
        info!("Starting callback loop...");
        while let Some(msg) = rx_execution_status.recv().await {
            info!(
                uuid = msg.action.uuid.to_string().as_str(),
                "outcome received"
            );
            let ds_clone = ds.clone();
            let tx_action_clone = tx_action.clone();
            tokio::spawn(
                async move { Self::callback_receiver(ds_clone, msg, tx_action_clone).await },
            );
        }
        info!("Finishing callback loop...");
    }

    // Send a message to Executor that requests a task execution
    async fn send_to_executor(
        ds: Shared<D>,
        tx_action: Sender<Action>,
        task: Task,
        retry: Option<i32>,
    ) -> anyhow::Result<uuid::Uuid> {
        let action = Action::new(&task)?;
        let task_id: Uuid = task.uuid.clone();
        if let None = retry {
            ds.write().await.add(task.clone()).await?;
        }
        Self::task_update_locking(ds.clone(), &task, TaskState::Inprogress).await;
        tx_action.send(action).await?;
        info!(
            uuid = &*task_id.to_hyphenated().to_string(),
            latency = %format!("{}", chrono::Utc::now().naive_utc().signed_duration_since(task.updated_at.unwrap_or(task.timestamp))),
            "Task sent to Executor.",
        );
        Ok(task_id)
    }

    async fn task_update_locking(ds_ref: Shared<D>, task: &Task, new_state: TaskState) {
        let mut new_retries = task.retries;
        if new_state == TaskState::Inprogress {
            new_retries = task.retries + 1;
            debug!(
                uuid = &*task.uuid.to_string(),
                retries = new_retries,
                "retry increase",
            );
        }

        match ds_ref
            .write()
            .await
            .update_state(&task.uuid, new_state, new_retries)
            .await
        {
            Ok(_) => {}
            Err(err) => {
                error!(reason = %err, uuid=%task.uuid.to_hyphenated(), "Failed to update task state in datastore.");
            }
        }
    }

    async fn callback_receiver(
        ds_ref: Shared<D>,
        msg: ActionExecutionOutcome,
        tx_action: Sender<Action>,
    ) {
        let res = ds_ref.read().await.get(&msg.action.uuid).await;
        match res {
            Ok(task) => match msg.outcome {
                Ok(mut tasks) => {
                    info!(
                        uuid = msg.action.uuid.to_string().as_str(),
                        tasks_number = tasks.len(),
                        status = "OK",
                        latency =
                            %format!("{}", chrono::Utc::now().naive_utc().signed_duration_since(task.updated_at.unwrap_or(task.timestamp))),
                        "callback received",
                    );
                    while let Some(new_task) = tasks.pop() {
                        let new_task_model =
                            Task::new(new_task.name, new_task.correlation_id, new_task.parameters);

                        if let Err(err) = Self::send_to_executor(
                            ds_ref.clone(),
                            tx_action.clone(),
                            new_task_model,
                            None,
                        )
                        .await
                        {
                            error!(reason=%err, uuid=msg.action.uuid.to_string().as_str(), "Error sending to Executor.")
                        }
                    }
                    Self::task_update_locking(ds_ref.clone(), &task, TaskState::Success).await;
                }
                Err(err) => {
                    info!(
                        uuid = msg.action.uuid.to_string().as_str(),
                        err = %err,
                        status = "FAIL",
                        "callback received",
                    );
                    Self::task_update_locking(ds_ref.clone(), &task, TaskState::Failed).await;
                }
            },
            Err(err) => {
                error!(reason = %err, uuid=%msg.action.uuid, "Unable to process callback");
                // TODO schedule to hit the db again
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::model::CorrelationId;
    use tokio::sync::mpsc::channel;

    use uuid::Uuid;

    use crate::{
        datastore::HashMapStorage,
        executor::error::ActionExecutionError,
        model::{action::ProgramParameters, ExecutionParameter},
    };

    use super::*;

    use tokio::time::sleep;

    use std::{collections::HashMap, convert::TryFrom, time::Duration};

    #[tokio::test]
    async fn test_poll_invalid_tasks() {
        // GIVEN
        let storage = crate::datastore::MemoryTaskStorage::new();
        let ds_cont = Arc::new(RwLock::new(HashMapStorage::new(storage)));
        let (tx_action, mut rx_action) = channel::<Action>(32);
        let (req_sender, callback) = tokio::sync::oneshot::channel();

        let (tx_out, rx_out) = channel::<ControllerRequest>(2);

        tx_out
            .send(ControllerRequest::SubmitTask(
                Task::new(
                    "invalid".to_string(),
                    CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                        .unwrap(),
                    HashMap::new(),
                ),
                req_sender,
            ))
            .await
            .unwrap();

        // WHEN
        {
            let ds = ds_cont.clone();
            tokio::spawn(async move {
                TaskController::handle_dispatch(ds, rx_out, tx_action.clone()).await;
            });

            // Receiving end to keep tx_action sender alive
            tokio::spawn(async move {
                loop {
                    let _ = rx_action.recv().await;
                }
            });
        }

        // THEN
        // invalid tasks are not stored in the DB
        assert!(callback.await.unwrap().is_err());
        assert_eq!(
            ds_cont
                .read()
                .await
                .items(&crate::datastore::Filter::default())
                .await
                .iter()
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn test_poll_new_tasks() {
        // GIVEN
        let storage = crate::datastore::MemoryTaskStorage::new();
        let ds_cont = Arc::new(RwLock::new(HashMapStorage::new(storage)));
        let (tx_action, mut rx_action) = channel::<Action>(32);
        let (req_sender1, callback1) = tokio::sync::oneshot::channel();
        let (req_sender2, callback2) = tokio::sync::oneshot::channel();

        let (tx_out, rx_out) = channel::<ControllerRequest>(2);

        {
            let ds = ds_cont.clone();
            tokio::spawn(async move {
                TaskController::handle_dispatch(ds, rx_out, tx_action.clone()).await;
            });

            // Receiving end to keep tx_action sender alive
            tokio::spawn(async move {
                loop {
                    let _ = rx_action.recv().await;
                }
            });
        }

        // WHEN
        let mut params = HashMap::new();
        params.insert("program".to_string(), "ssh".to_string());
        tx_out
            .send(ControllerRequest::SubmitTask(
                Task::new(
                    "program".to_string(),
                    CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                        .unwrap(),
                    params.clone(),
                ),
                req_sender1,
            ))
            .await
            .unwrap();
        tx_out
            .send(ControllerRequest::SubmitTask(
                Task::new(
                    "program".to_string(),
                    CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                        .unwrap(),
                    params.clone(),
                ),
                req_sender2,
            ))
            .await
            .unwrap();

        // THEN
        // invalid tasks are not stored in the DB
        println!("{:?}", callback1.await);
        // assert!(callback1.await.unwrap().is_ok());
        assert!(callback2.await.unwrap().is_ok());
        assert_eq!(
            ds_cont
                .read()
                .await
                .items(&crate::datastore::Filter::default())
                .await
                .iter()
                .len(),
            2
        );
        assert!(ds_cont
            .read()
            .await
            .items(&crate::datastore::Filter::default())
            .await
            .iter()
            .all(|task| {
                println!("{:?}", task.state);
                task.state == TaskState::Inprogress
            }));
    }

    #[tokio::test]
    async fn test_handle_callback() {
        // GIVEN
        let storage = crate::datastore::MemoryTaskStorage::new();
        let ds_cont = Arc::new(RwLock::new(HashMapStorage::new(storage)));
        let (tx_action, mut rx_action) = channel::<Action>(32);
        let (tx_out, rx_out) = channel::<ActionExecutionOutcome>(32);

        {
            let ds = ds_cont.clone();
            tokio::spawn(async move {
                TaskController::handle_callback(ds, rx_out, tx_action).await;
            });

            // Receiving end to keep tx_action sender alive
            tokio::spawn(async move {
                loop {
                    let _ = rx_action.recv().await;
                }
            });
        }

        let mut uuids: [Uuid; 2] = [Uuid::default(), Uuid::default()];
        {
            let mut ds = ds_cont.write().await;

            // let mut params: HashMap<String, String> = HashMap::new();
            // params.insert("program".to_string(), "ssh".to_string());

            let task1 = Task::new(
                "program".to_string(),
                CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                    .unwrap(),
                HashMap::new(),
            );
            let task2 = Task::new(
                "program".to_string(),
                CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                    .unwrap(),
                HashMap::new(),
            );
            uuids[0] = task1.uuid.clone();
            uuids[1] = task2.uuid.clone();
            ds.add(task1).await.unwrap();
            ds.add(task2).await.unwrap();
        }

        // WHEN

        tx_out
            .send(ActionExecutionOutcome::new(
                Action {
                    uuid: uuids[0],
                    name: "program".to_string(),
                    correlation_id: CorrelationId::try_from(
                        &"00000000-0000-0000-0000-000000000000".to_string(),
                    )
                    .unwrap(),
                    parameters: ExecutionParameter::Program(ProgramParameters {
                        program: "echo".to_string(),
                        arguments: vec![],
                    }),
                },
                Ok(vec![]),
            ))
            .await
            .unwrap();
        tx_out
            .send(ActionExecutionOutcome::new(
                Action {
                    uuid: uuids[1],
                    name: "program".to_string(),
                    correlation_id: CorrelationId::try_from(
                        &"00000000-0000-0000-0000-000000000000".to_string(),
                    )
                    .unwrap(),
                    parameters: ExecutionParameter::Program(ProgramParameters {
                        program: "echo".to_string(),
                        arguments: vec![],
                    }),
                },
                Err(ActionExecutionError::HttpError("not ok".to_string())),
            ))
            .await
            .unwrap();

        // THEN
        // allow the callbacks to happen
        sleep(Duration::from_millis(20)).await;

        let tasks_state = {
            let mut res: HashMap<String, TaskState> = HashMap::new();
            let ds = ds_cont.read().await;
            let tasks = ds.items(&crate::datastore::Filter::default()).await;
            for task in tasks.iter() {
                res.insert(task.uuid.to_string(), task.state);
            }
            res
        };
        assert_eq!(
            *tasks_state.get(&uuids[0].to_string()).unwrap(),
            TaskState::Success
        );
        assert_eq!(
            *tasks_state.get(&uuids[1].to_string()).unwrap(),
            TaskState::Failed
        );
    }

    // TODO test application of deadline on tasks in progress
    #[tokio::test]
    async fn test_autoretry() {
        // GIVEN
        let storage = crate::datastore::MemoryTaskStorage::new();
        let ds_cont = Arc::new(RwLock::new(HashMapStorage::new(storage)));
        let (tx_action, _) = channel::<Action>(32);
        let mut uuids: [Uuid; 2] = [Uuid::default(), Uuid::default()];
        {
            let mut ds = ds_cont.write().await;

            // let mut params: HashMap<String, String> = HashMap::new();
            // params.insert("program".to_string(), "ssh".to_string());

            let mut hm0: HashMap<String, String> = HashMap::new();
            hm0.insert("url".to_string(), "localhost".to_string());
            hm0.insert("protocol".to_string(), "http".to_string());

            let task1 = Task::new(
                "service".to_string(),
                CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                    .unwrap(),
                hm0,
            );
            let mut hm1: HashMap<String, String> = HashMap::new();
            hm1.insert("url".to_string(), "localhost".to_string());
            hm1.insert("protocol".to_string(), "http".to_string());
            let task2 = Task::new(
                "service".to_string(),
                CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                    .unwrap(),
                hm1,
            );
            uuids[0] = task1.uuid.clone();
            uuids[1] = task2.uuid.clone();
            ds.add(task1).await.unwrap();
            ds.add(task2).await.unwrap();
            ds.update_state(&uuids[1], TaskState::Failed, 1)
                .await
                .unwrap();
        }

        // WHEN
        {
            let ds = ds_cont.clone();
            let tx_action_clone = tx_action.clone();
            tokio::spawn(
                async move { TaskController::auto_retry_tasks(ds, tx_action_clone).await },
            );
            sleep(Duration::from_millis(100)).await;
        }
        sleep(Duration::from_millis(100)).await;

        // THEN
        // all tasks sent as actions
        // let action1 = rx_action.recv().await.unwrap();

        // assert_eq!(action1.uuid, uuids[1]);

        // all tasks state changed to in_progress
        let ds = ds_cont.read().await;
        let items = ds.items(&crate::datastore::Filter::default()).await;
        let mut tasks_state = items
            .iter()
            .filter(|task| task.state == TaskState::Inprogress);
        assert_eq!(uuids[1], tasks_state.next().unwrap().uuid);
        assert_eq!(0, tasks_state.count());
        assert!(true);
    }
}
