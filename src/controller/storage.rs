use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tracing::error;
use uuid::Uuid;

use crate::controller::ControllerError;
use crate::model::task::NewState;
use crate::{
    datastore::{Filter, TaskDataStore},
    model::{Task, TaskState},
};

use super::Shared;

pub enum StorageServiceRequest {
    Create(Task),
    Fetch(Uuid),
    List(Filter),
    ListActive,
    UpdateState(NewState),
}

pub enum StorageServiceResponse {
    Create,
    Fetch(Option<Task>),
    List(Vec<Task>),
    UpdateState,
    Error(String),
}

impl TryFrom<StorageServiceResponse> for Task {
    type Error = crate::controller::ControllerError;

    fn try_from(value: StorageServiceResponse) -> Result<Self, Self::Error> {
        match value {
            StorageServiceResponse::Fetch(Some(task)) => Ok(task),
            StorageServiceResponse::Fetch(None) => {
                Err(ControllerError::GenericError("Not found".into()))
            }
            StorageServiceResponse::Error(err) => {
                Err(ControllerError::StorageServiceError(err.to_string()))
            }
            _ => Err(ControllerError::StorageServiceError(
                "Wrong response".into(),
            )),
        }
    }
}

impl TryFrom<StorageServiceResponse> for Vec<Task> {
    type Error = crate::controller::ControllerError;

    fn try_from(value: StorageServiceResponse) -> Result<Self, Self::Error> {
        match value {
            StorageServiceResponse::List(tasks) => Ok(tasks),
            StorageServiceResponse::Error(err) => {
                Err(ControllerError::StorageServiceError(err.to_string()))
            }
            _ => Err(ControllerError::StorageServiceError(
                "Wrong response".into(),
            )),
        }
    }
}

impl TryFrom<StorageServiceResponse> for () {
    type Error = crate::controller::ControllerError;

    fn try_from(value: StorageServiceResponse) -> Result<Self, Self::Error> {
        match value {
            StorageServiceResponse::Create => Ok(()),
            StorageServiceResponse::UpdateState => Ok(()),
            StorageServiceResponse::Error(err) => {
                Err(ControllerError::StorageServiceError(err.to_string()))
            }
            _ => Err(ControllerError::StorageServiceError(
                "Wrong response".into(),
            )),
        }
    }
}

pub type RequestResponse = (
    StorageServiceRequest,
    oneshot::Sender<StorageServiceResponse>,
);

pub struct StorageService<D: TaskDataStore> {
    storage: Shared<D>,
    rx: Receiver<RequestResponse>,
}

impl<D: TaskDataStore + Send + Sync + 'static> StorageService<D> {
    pub fn new(storage: Shared<D>, rx: Receiver<RequestResponse>) -> Self {
        Self { storage, rx }
    }

    pub fn build_runtime(self) -> Runtime {
        let runtime = Builder::new_multi_thread()
            .thread_name("storage")
            .enable_all()
            .build()
            .expect("[storage] failed to create runtime");

        runtime.handle().spawn(self.start());
        runtime
    }

    pub async fn start(mut self) {
        while let Some((request, response_sender)) = self.rx.recv().await {
            let storage = self.storage.clone();
            tokio::spawn(async move {
                match handle_request(storage, request).await {
                    Ok(res) => response_sender.send(res),
                    Err(err) => {
                        response_sender.send(StorageServiceResponse::Error(err.to_string()))
                    }
                }
            });
        }
    }
}

async fn handle_request<D: TaskDataStore>(
    storage: Shared<D>,
    request: StorageServiceRequest,
) -> Result<StorageServiceResponse, ControllerError> {
    let response = match request {
        StorageServiceRequest::Create(task) => storage
            .write()
            .await
            .add(task)
            .await
            .map(|_| StorageServiceResponse::Create),
        StorageServiceRequest::Fetch(uuid) => storage
            .read()
            .await
            .get(&uuid)
            .await
            .map(|x| StorageServiceResponse::Fetch(x)),
        StorageServiceRequest::List(filter) => Ok(StorageServiceResponse::List(
            storage.read().await.items(&filter).await,
        )),
        StorageServiceRequest::UpdateState(new_state) => storage
            .write()
            .await
            .update_state(new_state)
            .await
            .map(|_| StorageServiceResponse::UpdateState),
        StorageServiceRequest::ListActive => Ok(StorageServiceResponse::List(
            storage
                .read()
                .await
                .items_filtered_state(vec![TaskState::Success as i32, TaskState::Died as i32])
                .await,
        )),
    };

    match response {
        Err(error) => {
            error!(err = %error, "Unable to execute storage service request.");
            Err(ControllerError::StorageServiceError(error.to_string()))
        }
        Ok(response) => Ok(response),
    }
}

pub async fn send<T>(
    tx_storage: Sender<RequestResponse>,
    request: StorageServiceRequest,
) -> Result<T, ControllerError>
where
    T: TryFrom<StorageServiceResponse, Error = ControllerError>,
{
    let (req_sender, callback) = tokio::sync::oneshot::channel();
    let _ = tx_storage
        .send((request, req_sender))
        .await
        .map_err(|err| ControllerError::StorageServiceError(err.to_string()))?;

    let res = callback
        .await
        .map_err(|err| ControllerError::StorageServiceError(err.to_string()))?;
    Ok(T::try_from(res)?)
}

#[cfg(test)]
mod tests {
    use crate::{controller::api::proto::task::Priority, model::CorrelationId};
    use tokio::sync::{mpsc::channel, RwLock};

    use uuid::Uuid;

    use crate::{datastore::HashMapStorage, model::TaskState};

    use super::*;

    use std::{collections::HashMap, convert::TryFrom, sync::Arc};

    #[tokio::test]
    async fn test_handle_storage_service_request() {
        // GIVEN
        let storage = crate::datastore::MemoryTaskStorage::new();
        let ds_cont = Arc::new(RwLock::new(HashMapStorage::new(storage)));
        let (tx_action, rx_action) = channel::<RequestResponse>(32);
        let storage_service = StorageService::new(ds_cont.clone(), rx_action);

        tokio::spawn(async move {
            storage_service.start().await;
        });

        let mut uuids: [Uuid; 2] = [Uuid::default(), Uuid::default()];
        let task1 = Task::new(
            "program".to_string(),
            CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string()).unwrap(),
            HashMap::new(),
            None,
            Priority::Medium,
        );
        let mut task2 = Task::new(
            "program".to_string(),
            CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string()).unwrap(),
            HashMap::new(),
            None,
            Priority::Medium,
        );
        uuids[0] = task1.uuid.clone();
        uuids[1] = task2.uuid.clone();

        // WHEN
        let (req_sender, _) = tokio::sync::oneshot::channel();
        let _ = tx_action
            .send((StorageServiceRequest::Create(task1.clone()), req_sender))
            .await;
        let (req_sender, callback) = tokio::sync::oneshot::channel();
        let _ = tx_action
            .send((StorageServiceRequest::Create(task2.clone()), req_sender))
            .await;

        // THEN
        // allow the callbacks to happen
        let _ = callback.await.unwrap();

        let task: Task = super::send(tx_action.clone(), StorageServiceRequest::Fetch(task1.uuid))
            .await
            .unwrap();
        assert_eq!(task.uuid, task1.uuid);
        assert_eq!(task.state, task1.state);

        let (req_sender, callback) = tokio::sync::oneshot::channel();
        let _ = tx_action
            .send((StorageServiceRequest::List(Filter::default()), req_sender))
            .await;
        if let StorageServiceResponse::List(tasks) = callback.await.unwrap() {
            assert_eq!(2, tasks.len());
        } else {
            assert!(false)
        }

        task2.update_state_retries(TaskState::Failed, 2i32).unwrap();

        let (req_sender, callback) = tokio::sync::oneshot::channel();
        let _ = tx_action
            .send((
                StorageServiceRequest::UpdateState(task2.as_new_state()),
                req_sender,
            ))
            .await;
        let _ = callback.await.unwrap();

        let (req_sender, callback) = tokio::sync::oneshot::channel();
        let _ = tx_action
            .send((StorageServiceRequest::Fetch(task2.uuid), req_sender))
            .await;
        if let StorageServiceResponse::Fetch(Some(task)) = callback.await.unwrap() {
            assert_eq!(task.uuid, task2.uuid);
            assert_eq!(task.state, TaskState::Failed);
            assert_eq!(task.retries, 2i32);
        } else {
            assert!(false)
        }
    }
}
