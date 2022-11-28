use anyhow::anyhow;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time;
use tracing::{debug, error, info};

use crate::config::RetryConfig;
use crate::executor::ActionExecutionOutcome;
use crate::model::{Action, Task, TaskState};

use super::storage::{RequestResponse, StorageServiceRequest};

pub fn build_runtime(
    tx_storage: Sender<RequestResponse>,
    rx_execution_status: Receiver<ActionExecutionOutcome>,
    tx_action: Sender<Action>,
) -> Runtime {
    let runtime = Builder::new_multi_thread()
        .thread_name("callback")
        .enable_all()
        .build()
        .expect("[callback] failed to create runtime");

    let handle_callback = handle_callback(tx_storage, rx_execution_status, tx_action);

    runtime.handle().spawn(handle_callback);
    runtime
}

async fn handle_callback(
    tx_storage: Sender<RequestResponse>,
    mut rx_execution_status: Receiver<ActionExecutionOutcome>,
    tx_action: Sender<Action>,
) {
    info!("Starting callback loop...");
    while let Some(msg) = rx_execution_status.recv().await {
        info!(
            uuid = msg.action.uuid.to_string().as_str(),
            "outcome received"
        );
        let tx_storage_clone = tx_storage.clone();
        let tx_action_clone = tx_action.clone();
        tokio::spawn(async move {
            let action_uuid = msg.action.uuid.clone();
            match callback_receiver(msg, tx_storage_clone, tx_action_clone).await {
                Ok(_) => {
                    debug!(uuid=%action_uuid, "Callback processed.")
                }
                Err(err) => {
                    error!(reason = %err, uuid=%action_uuid, "Unable to process callback.");
                }
            }
        });
    }
    info!("Finishing callback loop...");
}

async fn callback_receiver(
    msg: ActionExecutionOutcome,
    tx_storage: Sender<RequestResponse>,
    tx_action: Sender<Action>,
) -> anyhow::Result<()> {
    let mut task: Task = super::storage::send(
        tx_storage.clone(),
        StorageServiceRequest::Fetch(msg.action.uuid),
    )
    .await?;

    match msg.outcome {
        Ok(mut tasks) => {
            info!(
                name = msg.action.name.as_str(),
                uuid = msg.action.uuid.to_string().as_str(),
                tasks_number = tasks.len(),
                status = "OK",
                latency =
                    %format!("{}", chrono::Utc::now().naive_utc().signed_duration_since(task.updated_at.unwrap_or(task.timestamp))),
                "callback received",
            );

            task.update_state_retries(TaskState::Success, task.retries.clone())?;

            while let Some(new_task) = tasks.pop() {
                let new_task_model = Task::from(new_task);

                let _ = super::dispatcher::dispatch_task(
                    new_task_model,
                    tx_action.clone(),
                    tx_storage.clone(),
                )
                .await?;
                ()
            }
            super::storage::send(
                tx_storage.clone(),
                StorageServiceRequest::UpdateState(task.clone()),
            )
            .await?;
        }
        Err(err) => {
            info!(
                name = msg.action.name.as_str(),
                uuid = msg.action.uuid.to_string().as_str(),
                err = %err,
                status = "FAIL",
                "callback received",
            );
            task.update_state_retries(TaskState::Failed, task.retries.clone())?;
            super::storage::send(
                tx_storage.clone(),
                StorageServiceRequest::UpdateState(task.clone()),
            )
            .await?;
            task_schedule_retry(task, tx_storage.clone(), tx_action.clone()).await?
        }
    }
    Ok(())
}

pub async fn task_schedule_retry(
    mut task: Task,
    tx_storage: Sender<RequestResponse>,
    tx_action: Sender<Action>,
) -> anyhow::Result<()> {
    let retry_config = RetryConfig::for_task(&task.name[..]);
    if task.state == TaskState::Died {
        return Ok(());
    }

    if let Some(schedule_after) = task
        .retry_strategy(&retry_config)
        .nth(task.retries as usize)
    {
        tokio::task::spawn(async move {
            info!(
                duration = %format!("{:?}", schedule_after),
                retries = task.retries,
                "Retry has been scheduled"
            );
            time::sleep(schedule_after).await;
            let task_uuid_hyphenated = task.uuid.hyphenated().to_string();

            if let Err(err) = dispatch_retry(task, tx_storage, tx_action).await {
                error!(reason=%err, uuid=task_uuid_hyphenated.as_str(), "Error retrying task.")
            }
        });
        return Ok(());
    }

    task.update_state_retries(TaskState::Died, task.retries.clone())?;
    let _ =
        super::storage::send(tx_storage, StorageServiceRequest::UpdateState(task.clone())).await?;
    Ok(())
}

async fn dispatch_retry(
    mut task: Task,
    tx_storage: Sender<RequestResponse>,
    tx_action: Sender<Action>,
) -> anyhow::Result<()> {
    task.update_state_retries(TaskState::Inprogress, task.retries + 1)?;

    super::storage::send(
        tx_storage.clone(),
        StorageServiceRequest::UpdateState(task.clone()),
    )
    .await?;

    tx_action
        .send(task.try_into()?)
        .await
        .map_err(|err| anyhow!(err.to_string()))
}

#[cfg(test)]
mod tests {
    use crate::{controller::api::proto::task::Priority, model::CorrelationId};
    use std::{collections::HashMap, convert::TryFrom, sync::Arc, time::Duration};
    use tokio::sync::{mpsc::channel, RwLock};

    use crate::{controller::storage::StorageService, datastore::HashMapStorage};

    use super::*;

    #[tokio::test]
    async fn test_dispatch_retry() {
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

        let _: () = super::super::storage::send(
            tx_storage.clone(),
            StorageServiceRequest::Create(task.clone()),
        )
        .await
        .unwrap();

        super::dispatch_retry(task.clone(), tx_storage.clone(), tx_action.clone())
            .await
            .unwrap();

        let task2: Task = super::super::storage::send(
            tx_storage.clone(),
            StorageServiceRequest::Fetch(task.uuid.clone()),
        )
        .await
        .unwrap();

        assert_eq!(task2.uuid, task.uuid);
        assert_eq!(task2.state, TaskState::Inprogress);
        assert_eq!(task2.retries, 1i32);

        let _ = rx_action.recv().await;
    }

    #[tokio::test]
    async fn test_schedule_retry() {
        let (tx_storage, rx_storage) = channel::<RequestResponse>(32);
        let (tx_action, _) = channel::<Action>(32);

        let storage = crate::datastore::MemoryTaskStorage::new();
        let ds_cont = Arc::new(RwLock::new(HashMapStorage::new(storage)));
        let storage_service = StorageService::new(ds_cont.clone(), rx_storage);

        tokio::spawn(async move {
            storage_service.start().await;
        });

        let mut params = HashMap::new();
        params.insert("program".to_string(), "ssh".to_string());
        let mut task = Task::new(
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

        super::task_schedule_retry(task.clone(), tx_storage.clone(), tx_action.clone())
            .await
            .unwrap();

        time::sleep(Duration::from_millis(50)).await;

        let task2: Task = super::super::storage::send(
            tx_storage.clone(),
            StorageServiceRequest::Fetch(task.uuid.clone()),
        )
        .await
        .unwrap();

        assert_eq!(task2.uuid, task.uuid);
        assert_eq!(task2.state, TaskState::Inprogress);
        assert_eq!(task2.retries, 1i32);

        // WHEN exceeded number of retries
        task.retries = 10;
        let _: () = super::super::storage::send(
            tx_storage.clone(),
            StorageServiceRequest::UpdateState(task.clone()),
        )
        .await
        .unwrap();

        super::task_schedule_retry(task.clone(), tx_storage.clone(), tx_action.clone())
            .await
            .unwrap();

        time::sleep(Duration::from_millis(50)).await;

        let task2: Task = super::super::storage::send(
            tx_storage.clone(),
            StorageServiceRequest::Fetch(task.uuid.clone()),
        )
        .await
        .unwrap();

        assert_eq!(task2.uuid, task.uuid);
        assert_eq!(task2.state, TaskState::Died);
        assert_eq!(task2.retries, 10i32);
    }
}
