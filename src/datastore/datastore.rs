use crate::model::CorrelationId;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{error, info, warn};
use uuid::Uuid;

use super::{error::DataStoreError, storage::TaskStorage};
use crate::model::{error::ModelError, Task, TaskState};

#[tonic::async_trait]
pub trait TaskDataStore {
    async fn add(
        &mut self,
        name: String,
        correlation_id: CorrelationId,
        params: HashMap<String, String>,
    ) -> Result<Uuid, DataStoreError>;
    async fn update_state(
        &mut self,
        uuid: &Uuid,
        new_state: TaskState,
        new_retries: i32,
    ) -> Result<Uuid, DataStoreError>;
    fn items(&self, filter: &Filter) -> Vec<Task>;
    fn get(&self, uuid: Uuid) -> Result<Task, DataStoreError>;
    async fn load_tasks(&mut self);
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

    pub fn failed_tasks() -> Self {
        Self {
            state: Some(TaskState::Failed),
            timestamp: None,
            name: None,
        }
    }

    pub fn inprogress_tasks() -> Self {
        Self {
            state: Some(TaskState::Inprogress),
            timestamp: None,
            name: None,
        }
    }
}

pub struct HashMapStorage<S: TaskStorage> {
    tasks: Mutex<HashMap<Uuid, Task>>,
    storage: Arc<tokio::sync::Mutex<S>>,
}

impl<S> HashMapStorage<S>
where
    S: TaskStorage,
{
    pub fn new(storage: S) -> Self {
        let hm = HashMap::new();
        Self {
            tasks: Mutex::new(hm),
            storage: Arc::new(tokio::sync::Mutex::new(storage)),
        }
    }
}

#[tonic::async_trait]
impl<S> TaskDataStore for HashMapStorage<S>
where
    S: TaskStorage,
{
    async fn add(
        &mut self,
        name: String,
        correlation_id: CorrelationId,
        params: HashMap<String, String>,
    ) -> Result<Uuid, DataStoreError> {
        let item = Task::new(name, correlation_id, params);
        let ts: Task;
        let uuid_val: Uuid;
        {
            let mut tasks = self.tasks.lock().unwrap();
            if let Some(task) = tasks.get(&item.uuid) {
                return Err(DataStoreError::Conflict(task.uuid));
            }
            uuid_val = item.uuid.clone();
            tasks.insert(item.uuid, item);
            ts = tasks.get(&uuid_val).unwrap().clone();
        }

        // store in persistence;
        if let Err(err) = self.storage.lock().await.store(ts).await {
            error!("Failed to peristently store the task: {:?}.", err);
        }

        Ok(uuid_val)
    }

    async fn update_state(
        &mut self,
        uuid: &Uuid,
        new_state: TaskState,
        new_retries: i32,
    ) -> Result<Uuid, DataStoreError> {
        let task_obj: Task;
        {
            let mut tasks = self.tasks.lock().unwrap();
            let hashmap_res = tasks.get_mut(uuid);
            if let None = hashmap_res {
                return Err(DataStoreError::NotFound(uuid.clone()));
            }
            let hashmap_task = hashmap_res.unwrap();
            if let Err(ModelError::UnableTransitionState(from_state, to_state)) =
                hashmap_task.update_state_retries(new_state, new_retries)
            {
                warn!(
                    uuid = &*uuid.to_string(),
                    "unable to transition state: {:?} -> {:?}", from_state, to_state
                );
                return Ok(uuid.clone());
            }
            task_obj = hashmap_task.clone();
        }

        if let Err(err) = self.storage.lock().await.update(task_obj).await {
            error!("Failed to peristently store the task: {:?}.", err);
        }
        Ok(uuid.clone())
    }

    fn items(&self, filter: &Filter) -> Vec<Task> {
        let tasks = self.tasks.lock().unwrap();
        let mut list = tasks.values().cloned().collect::<Vec<Task>>();
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

    fn get(&self, uuid: Uuid) -> Result<Task, DataStoreError> {
        let tasks = self.tasks.lock().unwrap();
        match tasks.get(&uuid) {
            Some(task) => Ok(task.clone()),
            None => Err(DataStoreError::NotFound(uuid)),
        }
    }

    async fn load_tasks(&mut self) {
        info!("Loading tasks from storage...");
        let storage_items = self.storage.lock().await.items().await;
        for t in storage_items.iter() {
            self.tasks.lock().unwrap().insert(t.uuid, t.clone());
        }
        info!("Loaded tasks:  {}", self.tasks.lock().unwrap().len());
    }
}

#[cfg(test)]
mod tests {
    use crate::model::CorrelationId;
    use std::convert::TryFrom;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_all() {
        let mut storage = crate::datastore::MemoryTaskStorage::new();
        let mut task_p_1 = Task::new(
            "task_p_1".to_string(),
            CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string()).unwrap(),
            HashMap::new(),
        );
        task_p_1.update_state(TaskState::Success).unwrap();
        let mut task_p_2 = Task::new(
            "task_p_2".to_string(),
            CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string()).unwrap(),
            HashMap::new(),
        );
        task_p_2.update_state(TaskState::Success).unwrap();

        storage.store(task_p_1).await.unwrap();
        storage.store(task_p_2).await.unwrap();
        let mut ds = HashMapStorage::new(storage);

        // no tasks in the beginning
        let mut items = ds.items(&Filter::success_tasks());
        assert_eq!(items.len(), 0);

        // tasks are being loaded from the persistence
        ds.load_tasks().await;
        items = ds.items(&Filter::success_tasks());
        assert_eq!(items.len(), 2);

        // CRU(no delete) tasks
        let uuid = ds
            .add(
                "program".to_string(),
                CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                    .unwrap(),
                HashMap::new(),
            )
            .await
            .unwrap();
        let mut task = ds.get(uuid).unwrap();

        assert_eq!(task.uuid, uuid);
        assert_eq!(task.state, TaskState::New);

        let uuid2 = ds
            .add(
                "dummy2".to_string(),
                CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string())
                    .unwrap(),
                HashMap::new(),
            )
            .await
            .unwrap();
        let task2 = ds.get(uuid2).unwrap();

        assert_eq!(task2.uuid, uuid2);
        assert_eq!(task2.state, TaskState::New);

        ds.update_state(&task.uuid, TaskState::Inprogress, task.retries + 1)
            .await
            .unwrap();

        // task changes state
        task = ds.get(uuid).unwrap();
        assert_eq!(task.state, TaskState::Inprogress);

        // other task does not change it's state
        task = ds.get(uuid2).unwrap();
        assert_eq!(task.state, TaskState::New);

        // list shows 1 new task
        items = ds.items(&Filter::new_tasks());
        assert_eq!(items.len(), 1);
        assert_eq!(items.last().unwrap().uuid, uuid2);

        // success tasks are still present
        items = ds.items(&Filter::success_tasks());
        assert_eq!(items.len(), 2);
    }
}
