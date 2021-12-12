use thiserror::Error;

// Internal Executor errors
#[derive(Debug, Error)]
pub enum ControllerError {
    #[error("generic controller error")]
    #[allow(dead_code)]
    GenericError,
}
