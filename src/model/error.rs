use crate::controller::api::proto::task::State;
use thiserror::Error;

// Internal Executor errors
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ModelError {
    #[error("Unable to transition state: from {0:?} to {1:?}.")]
    UnableTransitionState(State, State),
    #[error("Incorrect task parameters: {0:?}.")]
    BadTaskParameters(String),
}
