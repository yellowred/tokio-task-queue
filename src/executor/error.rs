use thiserror::Error;

// Internal Executor errors
#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum ExecutorError {
    #[error("Stop have no effect cause the executor has not been started")]
    NotRunning,
    #[error("Thread could not be joined: {0:?}")]
    Unreachable(Box<dyn std::any::Any + Send>),
}

// Error wrapper for Action's execution
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ActionExecutionError {
    #[error("Error while starting child process")]
    IOError,
    #[error("http call resulted in error: {0}")]
    HttpError(String),
}
