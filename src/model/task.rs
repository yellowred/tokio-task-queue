use crate::{config::RetryConfig, model::CorrelationId};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::iter::Iterator;
use std::time::Duration;
use tokio_retry::strategy::{jitter, ExponentialBackoff, FixedInterval};
use uuid::Uuid;

use super::error::ModelError;

pub use crate::controller::api::proto::{task::Priority, task::State};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    pub uuid: Uuid,
    pub timestamp: NaiveDateTime,
    pub name: String,
    pub state: State,
    pub correlation_id: CorrelationId,
    pub stream: Option<String>,
    pub priority: Priority,
    pub retries: i32,
    pub updated_at: Option<NaiveDateTime>,
    pub parameters: HashMap<String, String>,
}

/// Task is a unit of work.
impl Task {
    pub fn new(
        name: String,
        correlation_id: CorrelationId,
        params: HashMap<String, String>,
    ) -> Task {
        let uuid = Uuid::new_v4();

        Self {
            state: State::New,
            timestamp: chrono::Utc::now().naive_utc(),
            uuid: uuid,
            stream: None,
            priority: Priority::Medium,
            retries: 0,
            updated_at: None,
            parameters: params,
            name,
            correlation_id,
        }
    }

    pub fn is_final_state(&self) -> bool {
        self.state == State::Died || self.state == State::Success
    }

    pub fn update_state(&mut self, new_state: State) -> Result<(), ModelError> {
        if self.is_final_state() {
            return Err(ModelError::UnableTransitionState(self.state, new_state));
        }
        self.state = new_state;
        self.updated_at = Some(chrono::Utc::now().naive_utc());
        Ok(())
    }

    pub fn update_state_retries(
        &mut self,
        new_state: State,
        new_retries: i32,
    ) -> Result<(), ModelError> {
        self.update_state(new_state).and_then(|()| {
            self.retries = new_retries;
            Ok(())
        })
    }

    pub fn can_retry(&self, config: &RetryConfig) -> Option<i64> {
        if self.state != State::Inprogress && self.state != State::Failed {
            return None;
        }

        self.retry_strategy(&config)
            .nth(self.retries as usize)
            .and_then(|next_retry| {
                self.updated_at
                    .unwrap_or(self.timestamp)
                    .timestamp_millis()
                    .checked_add(next_retry.as_millis() as i64)
                    .and_then(|next_retry_timestamp| {
                        if self.state == State::Inprogress {
                            return Some(next_retry_timestamp + config.deadline_ms as i64);
                        }
                        Some(next_retry_timestamp)
                    })
                    .and_then(|next_retry_timestamp| {
                        if next_retry_timestamp <= chrono::Utc::now().timestamp_millis() {
                            return Some(next_retry_timestamp);
                        }
                        None
                    })
            })
    }

    pub fn retry_strategy(&self, config: &RetryConfig) -> Box<dyn Iterator<Item = Duration>> {
        match &self.name[..] {
            "htm_confirm_transaction" => Box::new(
                FixedInterval::from_millis(config.interval_ms).take(config.max_retries as usize),
            ),
            _ => Box::new(
                ExponentialBackoff::from_millis(config.interval_ms)
                    .max_delay(Duration::from_millis(config.max_interval_ms))
                    .map(jitter)
                    .take(config.max_retries as usize),
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct NewTask {
    pub name: String,
    pub correlation_id: CorrelationId,
    pub stream: Option<String>,
    pub priority: Priority,
    pub parameters: HashMap<String, String>,
}

impl NewTask {
    pub fn new_service_call(
        stream: Option<String>,
        correlation_id: CorrelationId,
        url: String,
        method: String,
    ) -> Self {
        let mut task_params = HashMap::new();
        task_params.insert("url".to_string(), url);
        task_params.insert("method".into(), method);

        Self {
            name: "service".into(),
            correlation_id,
            stream,
            priority: Priority::Medium,
            parameters: task_params,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, convert::TryFrom, time::Duration};

    use super::{State, Task};
    use crate::{config::RetryConfig, model::CorrelationId};
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_state_update() {
        let mut task = Task::new(
            "example".to_string(),
            CorrelationId::try_from(&"b7b054ca-0d37-418b-ab16-ebe8aa409285".to_string()).unwrap(),
            HashMap::new(),
        );
        assert_eq!(task.state, State::New, "new task has the correct state");
        assert!(
            task.update_state(State::Success).is_ok(),
            "new task can be transitioned to success state"
        );
        assert!(
            task.update_state(State::Inprogress).is_err(),
            "success task can not be transitioned to in_progress state"
        );
    }

    #[tokio::test]
    async fn test_retriablity() {
        // Con not retry new task
        let mut task1 = Task::new(
            "uuid1".into(),
            CorrelationId::try_from(&"b7b054ca-0d37-418b-ab16-ebe8aa409285".to_string()).unwrap(),
            HashMap::new(),
        );
        let config = RetryConfig::new(10, 20, 86400000, 300000);
        assert!(task1.can_retry(&config).is_none());
        sleep(Duration::from_millis(21)).await;
        assert!(task1.can_retry(&config).is_none());

        // Can retry after some time
        task1.state = State::Failed;
        assert!(task1.can_retry(&config).is_some());
        task1.retries = task1.retries + 2;
        assert!(task1.can_retry(&config).is_none());

        // Maintains a limit on the number of retry attempts
        let mut task2 = Task::new(
            "uuid2".into(),
            CorrelationId::try_from(&"b7b054ca-0d37-418b-ab16-ebe8aa409285".to_string()).unwrap(),
            HashMap::new(),
        );
        task2.state = State::Failed;
        assert_eq!(0, task2.retries);
        sleep(Duration::from_millis(21)).await;
        assert!(task2.can_retry(&config).is_some());
        // task updated 2.5d ago, so the min interval condition is satisfied
        task2.updated_at = Some(chrono::NaiveDateTime::from_timestamp(
            chrono::Utc::now().timestamp() - 186400,
            0,
        ));
        assert!(task2.can_retry(&config).is_some());
        // maximum number of retries reached
        task2.retries = 11;
        assert!(task2.can_retry(&config).is_none());

        // Applies execution deadline for tasks in progress
        let mut task3 = Task::new(
            "uuid2".into(),
            CorrelationId::try_from(&"b7b054ca-0d37-418b-ab16-ebe8aa409285".to_string()).unwrap(),
            HashMap::new(),
        );
        task3.state = State::Inprogress;
        assert_eq!(0, task3.retries);
        assert!(task3.can_retry(&config).is_none());
        sleep(Duration::from_millis(21)).await;
        assert!(task3.can_retry(&config).is_none());
        task3.updated_at = Some(chrono::NaiveDateTime::from_timestamp(
            chrono::Utc::now().timestamp() - 301,
            0,
        ));
        assert!(task3.can_retry(&config).is_some());
    }
}
