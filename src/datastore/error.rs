use thiserror::*;
use uuid::Uuid;

#[derive(Debug, PartialEq, Error)]
pub enum DataStoreError {
    #[error("the item exists {0}")]
    Conflict(Uuid),

    #[error("the item not found {0}")]
    NotFound(Uuid),

    #[error("model does not allow update")]
    #[allow(dead_code)]
    Model,
}
