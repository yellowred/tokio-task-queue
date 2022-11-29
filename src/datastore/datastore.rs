use chrono::{DateTime, Utc};
use std::sync::Arc;

use uuid::Uuid;

use super::{error::DataStoreError, storage::TaskStorage};
use crate::model::{task::NewState, Task, TaskState};

#[tonic::async_trait]
pub trait TaskDataStore {
    async fn add(&mut self, item: Task) -> Result<(), DataStoreError>;
    async fn update_state(&mut self, new_state: NewState) -> Result<(), DataStoreError>;
    async fn items(&self, filter: &Filter) -> Vec<Task>;
    async fn get(&self, uuid: &Uuid) -> Result<Option<Task>, DataStoreError>;

    async fn items_filtered_state(&self, states: Vec<i32>) -> Vec<Task>;
}

#[derive(Debug)]
pub struct Filter {
    pub state: Option<TaskState>,
    pub timestamp: Option<DateTime<Utc>>,
    pub name: Option<String>,
}

impl Default for Filter {
    fn default() -> Self {
        Self {
            state: None,
            timestamp: None,
            name: None,
        }
    }
}

impl Filter {
    #[allow(dead_code)]
    pub fn new_tasks() -> Self {
        Self {
            state: Some(TaskState::New),
            timestamp: None,
            name: None,
        }
    }

    #[allow(dead_code)]
    pub fn success_tasks() -> Self {
        Self {
            state: Some(TaskState::Success),
            timestamp: None,
            name: None,
        }
    }

    #[allow(dead_code)]
    pub fn failed_tasks() -> Self {
        Self {
            state: Some(TaskState::Failed),
            timestamp: None,
            name: None,
        }
    }

    #[allow(dead_code)]
    pub fn inprogress_tasks() -> Self {
        Self {
            state: Some(TaskState::Inprogress),
            timestamp: None,
            name: None,
        }
    }
}

pub struct HashMapStorage<S: TaskStorage> {
    storage: Arc<S>,
}

impl<S> HashMapStorage<S>
where
    S: TaskStorage,
{
    pub fn new(storage: S) -> Self {
        Self {
            storage: Arc::new(storage),
        }
    }
}

#[tonic::async_trait]
impl<S> TaskDataStore for HashMapStorage<S>
where
    S: TaskStorage,
{
    async fn add(&mut self, item: Task) -> Result<(), DataStoreError> {
        self.storage
            .store(item)
            .await
            .map_err(|err| DataStoreError::Storage(err.to_string()))?;
        Ok(())
    }

    async fn update_state(&mut self, new_state: NewState) -> Result<(), DataStoreError> {
        self.storage
            .update(new_state)
            .await
            .map_err(|err| DataStoreError::Storage(err.to_string()))?;
        Ok(())
    }

    async fn items(&self, filter: &Filter) -> Vec<Task> {
        let mut list = self.storage.items().await;
        match filter.state {
            Some(state) => list.retain(|task| task.state == state),
            None => (),
        };
        match &filter.name {
            Some(name) => list.retain(|task| task.name == *name),
            None => (),
        };
        match filter.timestamp {
            Some(_timestamp) => {
                todo!()
            }
            None => (),
        };
        list
    }

    async fn get(&self, uuid: &Uuid) -> Result<Option<Task>, DataStoreError> {
        self.storage
            .fetch(uuid)
            .await
            .map_err(|err| DataStoreError::Storage(err.to_string()))
    }

    async fn items_filtered_state(&self, states: Vec<i32>) -> Vec<Task> {
        self.storage.items_filtered_state(states).await
    }
}
