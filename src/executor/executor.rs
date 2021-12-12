use super::error::{ActionExecutionError, ExecutorError};
use super::runners::program_runner::ProgramRunner;
use super::runners::service_runner::ServiceRunner;

use crate::model::task::NewTask;
use crate::model::Action;
use crate::model::ExecutionParameter;

use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tracing::{error, info};

// Responssible of processing `In` from a mpsc channel receiver,
//  and returning `Out` on a returned receiver.
pub trait Executor<In, Out> {
    fn start(
        &mut self,
        rx: Receiver<In>,
    ) -> Result<(Receiver<ActionExecutionOutcome>, Runtime), ExecutorError>;
    fn stop(&mut self) -> Result<(), ExecutorError>;
}

#[derive(Debug, PartialEq, Eq)]
pub struct ActionExecutionOutcome {
    pub action: Action,
    pub outcome: Result<Vec<NewTask>, ActionExecutionError>,
}

impl ActionExecutionOutcome {
    pub fn new(action: Action, outcome: Result<Vec<NewTask>, ActionExecutionError>) -> Self {
        Self {
            action: action,
            outcome: outcome,
        }
    }
}

// An Executor concrete implementation for processing Actions
pub struct ActionExecutor {}

impl ActionExecutor {
    pub fn new() -> Self {
        ActionExecutor {}
    }
}

impl ActionExecutor {
    async fn execute(action: Action) -> ActionExecutionOutcome {
        let outcome = match &action.parameters {
            ExecutionParameter::Program(conf) => ProgramRunner::run(conf),
            ExecutionParameter::Service(conf) => ServiceRunner::run(conf).await,
        };
        ActionExecutionOutcome::new(action, outcome)
    }

    async fn dispatch_action(action: Action, callback: Arc<Mutex<Sender<ActionExecutionOutcome>>>) {
        // execute
        let output = Self::execute(action).await;
        {
            let tx_out = callback.lock().await;
            // write output on the pipe
            match tx_out.send(output).await {
                Ok(()) => info!("Successfully returned the outcome of an action"),
                Err(e) => error!(
                    "An action finished but the output could not be sent through the out pipe: {}",
                    e
                ),
            }
        }
    }
}

impl Executor<Action, ActionExecutionOutcome> for ActionExecutor {
    fn start(
        &mut self,
        mut rx: Receiver<Action>,
    ) -> Result<(Receiver<ActionExecutionOutcome>, Runtime), ExecutorError> {
        // init output pipe
        let (tx_out, rx_out) = channel::<ActionExecutionOutcome>(32);
        let thread_safe_tx_out = Arc::new(Mutex::new(tx_out));
        // init long lived listener thread kill switch

        // starts parrallel, long lived, listener thread
        // Create the green threads tokio nested runtime
        let runtime = Builder::new_multi_thread()
            .thread_name("concurent-action-execution")
            .enable_all()
            .build()
            .expect("[concurent-action-execution] failed to create runtime");

        // starts parrallel, long lived, listener thread
        runtime.handle().spawn(async move {
            info!("Starting executor loop...");
            while let Some(action) = rx.recv().await {
                Self::dispatch_action(action, thread_safe_tx_out.clone()).await;
            }
        });

        // returns output receiver to the caller
        Ok((rx_out, runtime))
    }

    fn stop(&mut self) -> Result<(), ExecutorError> {
        // not implemented
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, convert::TryFrom};

    use crate::model::CorrelationId;

    use crate::model::Task;

    use super::*;

    #[test]
    fn test_executor_start() {
        // GIVEN
        let mut executor = ActionExecutor::new();
        let (_, in_rx) = channel(32);

        // THEN
        let _ = executor.start(in_rx).expect("failed to start");
    }

    #[tokio::test]
    async fn test_execute_action_successfully_with_no_child_task() {
        // GIVEN
        let action = dummy_action();
        // init output pipe
        let (tx_out, mut rx_out) = channel::<ActionExecutionOutcome>(32);

        // WHEN
        ActionExecutor::dispatch_action(action.clone(), Arc::new(Mutex::new(tx_out))).await;

        // THEN
        let result = rx_out.recv().await.unwrap();
        let expected = ActionExecutionOutcome::new(action, Ok(vec![]));
        assert_eq!(
            result, expected,
            "The execution result should be succesfull and empty"
        );
    }

    #[tokio::test]
    async fn test_execute_actions() {
        // GIVEN
        let action_a = dummy_action(); // dummy_sleep())).unwrap();
        let action_b = dummy_action(); // dummy_echo())).unwrap();
        let action_c = dummy_action(); // dummy_sleep())).unwrap();

        // init output pipe
        let (tx_out, mut rx_out) = channel::<ActionExecutionOutcome>(32);

        // WHEN
        ActionExecutor::dispatch_action(action_a.clone(), Arc::new(Mutex::new(tx_out.clone())))
            .await;
        ActionExecutor::dispatch_action(action_b.clone(), Arc::new(Mutex::new(tx_out.clone())))
            .await;
        ActionExecutor::dispatch_action(action_c.clone(), Arc::new(Mutex::new(tx_out.clone())))
            .await;

        // THEN
        let mut results = Vec::<ActionExecutionOutcome>::new();
        results.push(rx_out.recv().await.unwrap());
        results.push(rx_out.recv().await.unwrap());
        results.push(rx_out.recv().await.unwrap());

        let expected_a = ActionExecutionOutcome::new(action_a, Ok(vec![]));
        let expected_b = ActionExecutionOutcome::new(action_b, Ok(vec![]));
        let expected_c = ActionExecutionOutcome::new(action_c, Ok(vec![]));

        assert!(
            results.contains(&expected_a),
            "The execution result should be succesfull and empty"
        );
        assert!(
            results.contains(&expected_b),
            "The execution result should be succesfull and empty"
        );
        assert!(
            results.contains(&expected_c),
            "The execution result should be succesfull and empty"
        );
    }

    #[test]
    fn test_executor_stop() {
        // GIVEN
        let mut executor = ActionExecutor::new();
        let (_, in_rx) = channel(32);

        // WHEN
        let _ = executor.start(in_rx).expect("failed to start");

        // THEN
        executor.stop().unwrap();
    }

    fn dummy_action() -> Action {
        let mut params: HashMap<String, String> = HashMap::new();
        params.insert("program".to_string(), "ssh".to_string());
        let task = Task::new(
            "program".to_string(),
            CorrelationId::try_from(&"b7b054ca-0d37-418b-ab16-ebe8aa409285".to_string()).unwrap(),
            params,
        );
        Action::new(&task).unwrap()
    }
}
