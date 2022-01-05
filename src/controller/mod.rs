mod error;

use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::{Builder, Runtime};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        RwLock,
    },
    time,
};

pub use error::ControllerError;
use tracing::{debug, error, info};

use crate::api::Server;
use crate::datastore::TaskDataStore;
use crate::executor::{self, ActionExecutionOutcome};
use crate::model::{Action, Task, TaskState};

type Shared<D> = Arc<RwLock<D>>;

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
        Self {
            _datastore: datastore.clone(),
            _datastore_runtime: Self::build_datastore_runtime(datastore.clone()),
            _api_runtime: Self::build_api_runtime(datastore.clone()),
            _callback_runtime: Self::build_callback_runtime(datastore.clone(), rx_execution_status),
            _dispatcher_runtime: Self::build_dispatcher_runtime(datastore.clone(), tx_action),
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

    fn build_api_runtime(datastore: Shared<D>) -> Runtime {
        let runtime = Builder::new_multi_thread()
            .thread_name("grpc-api")
            .enable_all()
            .build()
            .expect("[grpc-api] failed to create runtime");

        let api = Server::new(datastore);
        runtime.handle().spawn(async { api.start().await });
        runtime
    }

    fn build_callback_runtime(
        datastore: Shared<D>,
        rx_execution_status: Receiver<executor::ActionExecutionOutcome>,
    ) -> Runtime {
        let runtime = Builder::new_multi_thread()
            .thread_name("callback")
            .enable_all()
            .build()
            .expect("[callback] failed to create runtime");

        let handle_callback = Self::handle_callback(datastore, rx_execution_status);

        runtime.handle().spawn(handle_callback);
        runtime
    }

    fn build_dispatcher_runtime(datastore: Shared<D>, tx_action: Sender<Action>) -> Runtime {
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

        let handle_dispatch = async move {
            info!("Starting polling for new tasks...");

            let mut interval = time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                Self::poll_new_tasks(datastore.clone(), tx_action.clone()).await;
            }
        };
        runtime.handle().spawn(handle_dispatch);

        runtime
    }

    async fn poll_new_tasks(ds: Shared<D>, tx_action: Sender<Action>) {
        let filter_new_tasks = crate::datastore::Filter::new_tasks();
        let mut tasks: Vec<Task> = vec![];
        {
            tasks = ds.read().await.items(&filter_new_tasks).await;
        }
        // Synchronously execute a batch of tasks,
        // so the poll_new_tasks will not be executed again in parallel by tokio::select!.
        // But this means other async handles are waiting, e.g. handle_callback.
        for task in tasks {
            Self::send_to_executor(ds.clone(), tx_action.clone(), task.clone()).await;
        }
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
            let mut tasks: Vec<Task> = vec![];
            tasks = datastore.read().await.items(&filter_failed_tasks).await;
            let mut tasks_inprogress = datastore.read().await.items(&filter_inprogress_tasks).await;
            tasks.append(&mut tasks_inprogress);

            // TODO try futures::future::join_all(
            while let Some(task) = tasks.pop() {
                if let Some(_) = task.can_retry() {
                    Self::send_to_executor(datastore.clone(), tx_action.clone(), task.clone())
                        .await;
                }
            }
        }
    }

    async fn handle_callback(
        ds: Shared<D>,
        mut rx_execution_status: Receiver<executor::ActionExecutionOutcome>,
    ) {
        info!("Starting callback loop...");
        while let Some(msg) = rx_execution_status.recv().await {
            info!(
                uuid = msg.action.uuid.to_string().as_str(),
                "outcome received"
            );
            let ds_clone = ds.clone();
            tokio::spawn(async move { Self::callback_receiver(ds_clone, msg).await });
        }
        info!("Finishing callback loop...");
    }

    // Send a message to Executor that requests a task execution
    async fn send_to_executor(ds: Shared<D>, tx_action: Sender<Action>, task: Task) {
        debug!(
            uuid = &*task.uuid.to_string(),
            state=%format!("{:?}", task.state),
            "execute task"
        );

        // Self::task_update_locking(ds.clone(), &task, TaskState::Processing).await;
        // info!(
        //     uuid = &*task.uuid.to_string(),
        //     state=%format!("{:?}", task.state),
        //     "execute task2"
        // );
        match Action::new(&task) {
            Ok(action) => {
                match tx_action.send(action).await {
                    Ok(_) => {
                        info!(
                            uuid = &*task.uuid.to_string(),
                            state=%format!("{:?}", task.state),
                            latency =
                            %format!("{}", chrono::Utc::now().naive_utc().signed_duration_since(task.updated_at.unwrap_or(task.timestamp))),
                            "task state change: {:?} -> {:?}",
                            task.state,
                            TaskState::Inprogress
                        );
                        Self::task_update_locking(ds.clone(), &task, TaskState::Inprogress).await;
                    }
                    Err(err) => {
                        error!(
                        uuid = &*task.uuid.to_string(),
                        state=%format!("{:?}", task.state),
                        reason=%err,
                        "execute task failed");
                        Self::task_update_locking(ds.clone(), &task, TaskState::New).await;
                    }
                };
            }
            Err(err) => {
                error!(
                    uuid = &*task.uuid.to_string(),
                    state=%format!("{:?}", task.state),
                    reason=%err,
                    "execute task failed");
                Self::task_update_locking(ds.clone(), &task, TaskState::Died).await;
            }
        }
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
            Ok(_) => {
                // let _ = ds.get(task.uuid).and_then(|task_updated| {
                //     info!(
                //         uuid = &*task.uuid.to_string(),
                //         state = %format!("{:?}", task_updated.state),
                //         retries = task_updated.retries,
                //         "task update",
                //     );
                //     Ok(())
                // });
            }
            Err(err) => {
                error!(reason = %err, "Failed to update task state in datastore.");
                // TODO schedule to hit the db again
            }
        }
    }

    async fn callback_receiver(ds_ref: Shared<D>, msg: ActionExecutionOutcome) {
        let t1 = chrono::Utc::now().naive_utc();
        let mut t2 = chrono::Utc::now().naive_utc();
        let res = {
            let _ds = ds_ref.read().await;
            t2 = chrono::Utc::now().naive_utc();
            _ds.get(msg.action.uuid).await
        };
        info!(
            latency =
            %format!("{}", chrono::Utc::now().naive_utc().signed_duration_since(t1)),
            latency1 =
            %format!("{}", t2.signed_duration_since(t1)),
            latency2 =
            %format!("{}", chrono::Utc::now().naive_utc().signed_duration_since(t2)),
        "callback: fetch task",
        );
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
                    while let Some(task) = tasks.pop() {
                        if let Err(err) = ds_ref
                            .write()
                            .await
                            .add(task.name, task.correlation_id, task.parameters)
                            .await
                        {
                            error!(reason = %err, uuid=%msg.action.uuid, "Unable to save pending task");
                        };
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
        let (tx_action, _) = channel::<Action>(32);
        let mut uuids: [Uuid; 2] = [Uuid::default(), Uuid::default()];
        {
            let mut params = HashMap::new();
            params.insert("program".to_string(), "ssh".to_string());
            uuids[0] = ds_cont
                .write()
                .await
                .add(
                    "program".to_string(),
                    CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                        .unwrap(),
                    HashMap::new(),
                )
                .await
                .unwrap();
            uuids[1] = ds_cont
                .write()
                .await
                .add(
                    "invalid".to_string(),
                    CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                        .unwrap(),
                    params,
                )
                .await
                .unwrap();
        }

        // WHEN
        TaskController::poll_new_tasks(ds_cont.clone(), tx_action.clone()).await;

        // THEN
        // all tasks state changed to in_progress
        let tasks_state = {
            let ds = ds_cont.read().await;
            let tasks = ds.items(&crate::datastore::Filter::default()).await;
            tasks.iter().all(|task| task.state == TaskState::Died)
        };
        assert!(tasks_state);
    }

    #[tokio::test]
    async fn test_poll_new_tasks() {
        // GIVEN
        let storage = crate::datastore::MemoryTaskStorage::new();
        let ds_cont = Arc::new(RwLock::new(HashMapStorage::new(storage)));
        let (tx_action, mut rx_action) = channel::<Action>(32);
        let mut uuids: [Uuid; 2] = [Uuid::default(), Uuid::default()];
        {
            let mut params = HashMap::new();
            params.insert("program".to_string(), "ssh".to_string());
            uuids[0] = ds_cont
                .write()
                .await
                .add(
                    "program".to_string(),
                    CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                        .unwrap(),
                    params.clone(),
                )
                .await
                .unwrap();
            uuids[1] = ds_cont
                .write()
                .await
                .add(
                    "program".to_string(),
                    CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                        .unwrap(),
                    params,
                )
                .await
                .unwrap();
        }

        // WHEN
        TaskController::poll_new_tasks(ds_cont.clone(), tx_action.clone()).await;

        // THEN
        // all tasks sent as actions
        let action1 = rx_action.recv().await.unwrap();
        let action2 = rx_action.recv().await.unwrap();

        assert!(uuids.contains(&action1.uuid));
        assert!(uuids.contains(&action2.uuid));
        assert!(&action1.uuid != &action2.uuid);

        // all tasks state changed to in_progress
        let tasks_state = {
            let ds = ds_cont.read().await;
            let tasks = ds.items(&crate::datastore::Filter::default()).await;
            tasks.iter().all(|task| task.state == TaskState::Inprogress)
        };
        assert!(tasks_state);
    }

    #[tokio::test]
    async fn test_handle_callback() {
        // GIVEN
        let storage = crate::datastore::MemoryTaskStorage::new();
        let ds_cont = Arc::new(RwLock::new(HashMapStorage::new(storage)));
        let (tx_out, rx_out) = channel::<ActionExecutionOutcome>(32);
        let mut uuids: [Uuid; 2] = [Uuid::default(), Uuid::default()];
        {
            let mut ds = ds_cont.write().await;

            // let mut params: HashMap<String, String> = HashMap::new();
            // params.insert("program".to_string(), "ssh".to_string());

            uuids[0] = ds
                .add(
                    "program".to_string(),
                    CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                        .unwrap(),
                    HashMap::new(),
                )
                .await
                .unwrap();
            uuids[1] = ds
                .add(
                    "task2".to_string(),
                    CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                        .unwrap(),
                    HashMap::new(),
                )
                .await
                .unwrap();
        }

        tx_out
            .send(ActionExecutionOutcome::new(
                Action {
                    uuid: uuids[0],
                    name: "task1".to_string(),
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
                    name: "task2".to_string(),
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

        // WHEN
        {
            let ds = ds_cont.clone();
            tokio::spawn(async move { TaskController::handle_callback(ds, rx_out).await });
            sleep(Duration::from_millis(20)).await;
        }

        // THEN
        // all tasks state changed to success
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
        let (tx_action, mut rx_action) = channel::<Action>(32);
        let mut uuids: [Uuid; 2] = [Uuid::default(), Uuid::default()];
        {
            let mut ds = ds_cont.write().await;

            // let mut params: HashMap<String, String> = HashMap::new();
            // params.insert("program".to_string(), "ssh".to_string());

            let mut hm0: HashMap<String, String> = HashMap::new();
            hm0.insert("url".to_string(), "localhost".to_string());
            hm0.insert("protocol".to_string(), "http".to_string());
            uuids[0] = ds
                .add(
                    "service".to_string(),
                    CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                        .unwrap(),
                    hm0,
                )
                .await
                .unwrap();

            let mut hm1: HashMap<String, String> = HashMap::new();
            hm1.insert("url".to_string(), "localhost".to_string());
            hm1.insert("protocol".to_string(), "http".to_string());
            uuids[1] = ds
                .add(
                    "service".to_string(),
                    CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                        .unwrap(),
                    hm1,
                )
                .await
                .unwrap();
            ds.update_state(&uuids[1], TaskState::Failed, 1)
                .await
                .unwrap();
        }

        // WHEN
        {
            let ds = ds_cont.clone();
            tokio::spawn(async move { TaskController::auto_retry_tasks(ds, tx_action).await });
            sleep(Duration::from_millis(100)).await;
        }
        sleep(Duration::from_millis(100)).await;

        // THEN
        // all tasks sent as actions
        let action1 = rx_action.recv().await.unwrap();

        assert_eq!(action1.uuid, uuids[1]);

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
