use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, info};

use crate::controller::storage::StorageServiceRequest;
use crate::controller::{storage, ControllerError};
use crate::model::Action;
use crate::model::{Task, TaskState};

use super::storage::RequestResponse;
use super::ControllerRequest;

pub struct DispatcherService {
    rx_request: Receiver<ControllerRequest>,
    tx_action: Sender<Action>,
    tx_storage: Sender<RequestResponse>,
}

impl DispatcherService {
    pub fn new(
        rx_request: Receiver<ControllerRequest>,
        tx_action: Sender<Action>,
        tx_storage: Sender<RequestResponse>,
    ) -> Self {
        Self {
            rx_request,
            tx_action,
            tx_storage,
        }
    }

    pub fn build_runtime(self) -> Runtime {
        let runtime = Builder::new_multi_thread()
            .thread_name("dispatcher")
            .enable_all()
            .build()
            .expect("[dispatcher] failed to create runtime");

        runtime.handle().spawn(self.start());

        runtime
    }

    async fn start(mut self) {
        info!("Starting dispatch loop.");
        while let Some(request) = self.rx_request.recv().await {
            let tx_action = self.tx_action.clone();
            let tx_storage = self.tx_storage.clone();
            tokio::spawn(async move {
                if let Err(err) = handle_controller_request(request, tx_action, tx_storage).await {
                    error!(reason=%err, "Unable to dispatch an action.");
                }
            });
        }
        info!("Finishing dispatch loop.");
    }
}

async fn handle_controller_request(
    request: ControllerRequest,
    tx_action: Sender<Action>,
    tx_storage: Sender<RequestResponse>,
) -> anyhow::Result<(), ControllerError> {
    match request {
        ControllerRequest::SubmitTask(task, tx_callback) => {
            // callback will only be sent after the task is saved in the db.
            // the api could be significantly faster if that op was async.
            let _ = match dispatch_task(task, tx_action, tx_storage).await {
                Ok(uuid) => {
                    info!(uuid = %uuid.hyphenated().to_string(), "callback");
                    tx_callback.send(Ok(uuid.hyphenated().to_string()))
                }
                Err(err) => tx_callback.send(Err(err)),
            };

            Ok(())
        }
        ControllerRequest::CancelTask(task_id, tx_callback) => {
            let _ = match cancel_task(&task_id, tx_storage).await {
                Ok(_) => tx_callback.send(Ok(task_id)),
                Err(err) => tx_callback.send(Err(err)),
            };
            Ok(())
        }
        ControllerRequest::RetryTask(task_id, tx_callback) => {
            let _ = match retry_task(&task_id, tx_action, tx_storage).await {
                Ok(_) => tx_callback.send(Ok(task_id)),
                Err(err) => tx_callback.send(Err(err)),
            };
            Ok(())
        }
        ControllerRequest::CloneTask(task_id, tx_callback) => {
            let _ = match clone_task(&task_id, tx_action, tx_storage).await {
                Ok(_) => tx_callback.send(Ok(task_id)),
                Err(err) => tx_callback.send(Err(err)),
            };
            Ok(())
        }
    }
}

pub async fn dispatch_task(
    mut task: Task,
    tx_action: Sender<Action>,
    tx_storage: Sender<RequestResponse>,
) -> Result<uuid::Uuid, ControllerError> {
    let task_id = task.uuid.clone();
    let task_id_string = task.uuid.hyphenated().to_string();

    info!(uuid = &*task_id_string, "Sending to Executor.",);

    let action =
        Action::new(&task).map_err(|err| ControllerError::GenericError(err.to_string()))?;

    if task.state == TaskState::New {
        storage::send(
            tx_storage.clone(),
            StorageServiceRequest::Create(task.clone()),
        )
        .await?;
    }

    info!(uuid = &*task_id_string, "Saved to storage",);

    task.update_state_retries(TaskState::Inprogress, task.retries.clone())
        .map_err(|err| ControllerError::GenericError(err.to_string()))?;

    storage::send(
        tx_storage.clone(),
        StorageServiceRequest::UpdateState(task.as_new_state()),
    )
    .await?;

    info!(uuid = &*task_id_string, "Updated state in storage",);

    tx_action
        .send(action)
        .await
        .map_err(|err| ControllerError::GenericError(err.to_string()))?;
    info!(
        uuid = &*task_id_string,
        latency = %format!("{}", chrono::Utc::now().naive_utc().signed_duration_since(task.updated_at.unwrap_or(task.timestamp))),
        "Task sent to Executor.",
    );
    Ok(task_id)
}

async fn cancel_task(
    task_id: &String,
    tx_storage: Sender<RequestResponse>,
) -> Result<(), ControllerError> {
    let task_uuid = uuid::Uuid::parse_str(task_id.as_str())
        .map_err(|err| ControllerError::GenericError(err.to_string()))?;
    let mut task: Task = storage::send(
        tx_storage.clone(),
        StorageServiceRequest::Fetch(task_uuid.clone()),
    )
    .await?;

    task.update_state_retries(TaskState::Died, task.retries.clone())
        .map_err(|err| ControllerError::GenericError(err.to_string()))?;

    storage::send(
        tx_storage.clone(),
        StorageServiceRequest::UpdateState(task.as_new_state()),
    )
    .await?;

    Ok(())
}

async fn retry_task(
    task_id: &String,
    tx_action: Sender<Action>,
    tx_storage: Sender<RequestResponse>,
) -> Result<(), ControllerError> {
    let task_uuid = uuid::Uuid::parse_str(task_id.as_str())
        .map_err(|err| ControllerError::GenericError(err.to_string()))?;
    let task: Task = storage::send(
        tx_storage.clone(),
        StorageServiceRequest::Fetch(task_uuid.clone()),
    )
    .await?;

    dispatch_task(task, tx_action, tx_storage).await?;

    Ok(())
}

async fn clone_task(
    task_id: &String,
    tx_action: Sender<Action>,
    tx_storage: Sender<RequestResponse>,
) -> Result<(), ControllerError> {
    let task_uuid = uuid::Uuid::parse_str(task_id.as_str())
        .map_err(|err| ControllerError::GenericError(err.to_string()))?;
    let task: Task = storage::send(
        tx_storage.clone(),
        StorageServiceRequest::Fetch(task_uuid.clone()),
    )
    .await?;

    let cloned_task = crate::model::Task::new(
        task.name,
        uuid::Uuid::new_v4().into(),
        task.parameters,
        task.stream,
        task.priority,
    );

    dispatch_task(cloned_task, tx_action, tx_storage).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{controller::api::proto::task::Priority, model::CorrelationId};
    use std::{collections::HashMap, convert::TryFrom, sync::Arc};
    use tokio::sync::{mpsc::channel, RwLock};

    use crate::{controller::storage::StorageService, datastore::HashMapStorage};

    use super::*;

    #[tokio::test]
    async fn test_dispatch() {
        let (tx_storage, rx_storage) = channel::<RequestResponse>(32);
        let (tx_action, mut rx_action) = channel::<Action>(32);

        let storage = crate::datastore::MemoryTaskStorage::new();
        let ds_cont = Arc::new(RwLock::new(HashMapStorage::new(storage)));
        let storage_service = StorageService::new(ds_cont.clone(), rx_storage);

        tokio::spawn(async move {
            storage_service.start().await;
        });

        let mut params = HashMap::new();
        params.insert("program".to_string(), "ssh".to_string());
        let task = Task::new(
            "program".to_string(),
            CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string()).unwrap(),
            params.clone(),
            None,
            Priority::Medium,
        );

        // WHEN
        super::dispatch_task(task.clone(), tx_action.clone(), tx_storage.clone())
            .await
            .unwrap();

        // THEN
        let task2: Task = super::super::storage::send(
            tx_storage.clone(),
            StorageServiceRequest::Fetch(task.uuid.clone()),
        )
        .await
        .unwrap();

        assert_eq!(task2.uuid, task.uuid);
        assert_eq!(task2.state, TaskState::Inprogress);
        assert_eq!(task2.retries, 0i32);

        // just to keep it alive
        let _ = rx_action.recv().await;
    }

    #[tokio::test]
    async fn test_cancel_task() {
        let (tx_storage, rx_storage) = channel::<RequestResponse>(32);
        let storage = crate::datastore::MemoryTaskStorage::new();
        let ds_cont = Arc::new(RwLock::new(HashMapStorage::new(storage)));
        let storage_service = StorageService::new(ds_cont.clone(), rx_storage);

        tokio::spawn(async move {
            storage_service.start().await;
        });

        let mut params = HashMap::new();
        params.insert("program".to_string(), "ssh".to_string());
        let task = Task::new(
            "program".to_string(),
            CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string()).unwrap(),
            params.clone(),
            None,
            Priority::Medium,
        );

        // WHEN new
        let _: () = super::super::storage::send(
            tx_storage.clone(),
            StorageServiceRequest::Create(task.clone()),
        )
        .await
        .unwrap();

        super::cancel_task(&task.uuid.hyphenated().to_string(), tx_storage.clone())
            .await
            .unwrap();

        let task2: Task = super::super::storage::send(
            tx_storage.clone(),
            StorageServiceRequest::Fetch(task.uuid.clone()),
        )
        .await
        .unwrap();

        assert_eq!(task2.uuid, task.uuid);
        assert_eq!(task2.state, TaskState::Died);
        assert_eq!(task2.retries, 0i32);
    }
}
