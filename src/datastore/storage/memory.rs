use std::collections::HashMap;
use uuid::Uuid;

use super::super::TaskStorage;
use super::error::StorageError;
use crate::model::Task;

pub struct MemoryTaskStorage {
    tasks: std::sync::Mutex<HashMap<Uuid, Task>>,
}

impl MemoryTaskStorage {
    #[allow(dead_code)]
    pub fn new() -> Self {
        let hm = HashMap::new();
        Self {
            tasks: std::sync::Mutex::new(hm),
        }
    }
}

#[tonic::async_trait]
impl TaskStorage for MemoryTaskStorage {
    async fn fetch(&self, uuid: &Uuid) -> Result<Option<Task>, StorageError> {
        let tasks = self.tasks.lock().unwrap();
        match tasks.get(&uuid) {
            Some(task) => Ok(Some(task.clone())),
            None => Ok(None),
        }
    }

    async fn store(&self, mut item: Task) -> Result<Uuid, StorageError> {
        let mut tasks = self.tasks.lock().unwrap();
        match tasks.get(&item.uuid) {
            Some(task) => return Err(StorageError::Conflict(task.uuid)),
            None => {
                tasks.insert(item.uuid, item.clone());
                Ok(item.uuid)
            }
        }
    }

    async fn update(&self, item: Task) -> Result<Uuid, StorageError> {
        let mut tasks = self.tasks.lock().unwrap();
        match tasks.get_mut(&item.uuid) {
            Some(task) => {
                task.state = item.state;
                task.retries = item.retries;
                return Ok(item.uuid);
            }
            None => return Err(StorageError::NotFound(item.uuid)),
        }
    }

    async fn items(&self) -> Vec<Task> {
        let tasks = self.tasks.lock().unwrap();
        tasks.values().cloned().collect::<Vec<Task>>()
    }

    async fn items_filtered_state(&self, states: Vec<i32>) -> Vec<Task> {
        let tasks = self.tasks.lock().unwrap();
        tasks
            .values()
            .cloned()
            .filter(|x| !states.contains(&(x.state as i32)))
            .collect::<Vec<Task>>()
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use super::*;
    use crate::model::CorrelationId;

    #[tokio::test]
    async fn test_store() {
        // GIVEN
        let storage = MemoryTaskStorage::new();
        let task = Task::new(
            "task".to_string(),
            CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string()).unwrap(),
            HashMap::new(),
        );

        // WHEN
        let res = storage.store(task).await;

        // THEN
        assert!(res.is_ok(), "Task storage failed.");
    }

    #[tokio::test]
    async fn test_items() {
        // GIVEN
        let storage = MemoryTaskStorage::new();
        let task_a = Task::new(
            "task_a".to_string(),
            CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string()).unwrap(),
            HashMap::new(),
        );
        let instance_uuid = uuid::Uuid::new_v4();

        // WHEN
        storage.store(task_a.clone()).await.unwrap();

        // THEN
        let res = storage.store(task_a.clone()).await.unwrap_err();
        assert!(matches!(res, StorageError::Conflict(_)));

        // WHEN
        let items = storage.items().await;

        // THEN
        assert_eq!(1, items.len());
        assert_eq!(
            task_a.uuid, items[0].uuid,
            "Items do not match stored ones."
        );
    }
}
