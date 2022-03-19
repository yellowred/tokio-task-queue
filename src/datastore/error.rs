use thiserror::*;

#[derive(Debug, PartialEq, Error)]
pub enum DataStoreError {
    #[error("storage returned error: {0}")]
    Storage(String),
}
