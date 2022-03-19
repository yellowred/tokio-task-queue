use thiserror::Error;

#[derive(Debug, Error)]
pub enum ControllerError {
    #[error("generic controller error")]
    GenericError(String),
    #[error("Internal service error: {0}")]
    StorageServiceError(String),
}
