use thiserror::*;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("the item exists {0}")]
    Conflict(Uuid),

    #[error("the item not found {0}")]
    NotFound(Uuid),
}
