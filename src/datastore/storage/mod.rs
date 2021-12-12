mod error;
mod memory;

use uuid::Uuid;

use crate::model::Task;
pub use error::StorageError;
pub use memory::MemoryTaskStorage;

// Storage
#[tonic::async_trait]
pub trait TaskStorage: Sync + Send + 'static {
    async fn store(&mut self, item: Task) -> Result<Uuid, StorageError>;
    async fn update(&self, item: Task) -> Result<Uuid, StorageError>;
    async fn items(&self) -> Vec<Task>;
}
