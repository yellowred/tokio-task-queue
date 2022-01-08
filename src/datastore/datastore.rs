use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use tracing::{error, info, warn};
use uuid::Uuid;

use super::{error::DataStoreError, storage::TaskStorage};
use crate::model::{error::ModelError, Task, TaskState};

#[tonic::async_trait]
pub trait TaskDataStore {
    async fn add(&mut self, item: Task) -> Result<(), DataStoreError>;
    async fn update_state(
        &mut self,
        uuid: &Uuid,
        new_state: TaskState,
        new_retries: i32,
    ) -> Result<Uuid, DataStoreError>;
    async fn items(&self, filter: &Filter) -> Vec<Task>;
    async fn get(&self, uuid: &Uuid) -> Result<Task, DataStoreError>;
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

type TasksStore = RwLock<HashMap<Uuid, Task>>;

pub struct HashMapStorage<S: TaskStorage> {
    tasks: TasksStore,
    storage: Arc<tokio::sync::Mutex<S>>,
}

impl<S> HashMapStorage<S>
where
    S: TaskStorage,
{
    pub fn new(storage: S) -> Self {
        let hm = HashMap::new();
        Self {
            tasks: RwLock::new(hm),
            storage: Arc::new(tokio::sync::Mutex::new(storage)),
        }
    }
}

#[tonic::async_trait]
impl<S> TaskDataStore for HashMapStorage<S>
where
    S: TaskStorage,
{
    async fn add(&mut self, item: Task) -> Result<(), DataStoreError> {
        if let Some(_) = self.tasks.read().await.get(&item.uuid) {
            return Err(DataStoreError::Conflict(item.uuid));
        }

        self.tasks
            .write()
            .await
            .insert(item.uuid.clone(), item.clone());

        // store in persistence;
        if let Err(err) = self.storage.lock().await.store(item).await {
            error!("Failed to peristently store the task: {:?}.", err);
        }

        Ok(())
    }

    async fn update_state(
        &mut self,
        uuid: &Uuid,
        new_state: TaskState,
        new_retries: i32,
    ) -> Result<Uuid, DataStoreError> {
        info!(
            "Update state: {}: {:?}.",
            uuid.to_hyphenated().to_string(),
            new_state
        );
        let task_obj: Task;
        {
            let mut tasks = self.tasks.write().await;
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

    async fn items(&self, filter: &Filter) -> Vec<Task> {
        let tasks = self.tasks.read().await;
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

    async fn get(&self, uuid: &Uuid) -> Result<Task, DataStoreError> {
        let tasks = self.tasks.read().await;
        match tasks.get(uuid) {
            Some(task) => Ok(task.clone()),
            None => Err(DataStoreError::NotFound(uuid.clone())),
        }
    }

    async fn load_tasks(&mut self) {
        info!("Loading tasks from storage...");
        let storage_items = self.storage.lock().await.items().await;
        let mut counter = 0u32;
        for t in storage_items.iter() {
            self.tasks.write().await.insert(t.uuid, t.clone());
            counter += 1;
        }
        info!("Loaded tasks:  {}", counter);
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
        let mut items = ds.items(&Filter::success_tasks()).await;
        assert_eq!(items.len(), 0);

        // tasks are being loaded from the persistence
        ds.load_tasks().await;
        items = ds.items(&Filter::success_tasks()).await;
        assert_eq!(items.len(), 2);

        // CRU(no delete) tasks

        let task1 = Task::new(
            "program".to_string(),
            CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string()).unwrap(),
            HashMap::new(),
        );
        ds.add(task1.clone()).await.unwrap();
        let mut task2 = ds.get(&task1.uuid).await.unwrap();

        assert_eq!(task2.uuid, task1.uuid);
        assert_eq!(task2.state, task1.state);

        let task3 = Task::new(
            "dummy2".to_string(),
            CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string()).unwrap(),
            HashMap::new(),
        );
        ds.add(task3.clone()).await.unwrap();
        let mut task4 = ds.get(&task3.uuid).await.unwrap();

        assert_eq!(task4.uuid, task3.uuid);
        assert_eq!(task4.state, task3.state);
        assert_eq!(task4.state, TaskState::New);

        ds.update_state(&task1.uuid, TaskState::Inprogress, task1.retries + 1)
            .await
            .unwrap();

        // task changes state
        task2 = ds.get(&task1.uuid).await.unwrap();
        assert_eq!(task2.state, TaskState::Inprogress);

        // other task does not change it's state
        task4 = ds.get(&task3.uuid).await.unwrap();
        assert_eq!(task4.state, TaskState::New);

        // list shows 1 new task
        items = ds.items(&Filter::new_tasks()).await;
        assert_eq!(items.len(), 1);
        assert_eq!(items.last().unwrap().uuid, task3.uuid);

        // success tasks are still present
        items = ds.items(&Filter::success_tasks()).await;
        assert_eq!(items.len(), 2);
    }
}
