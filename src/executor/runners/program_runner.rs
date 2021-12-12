use crate::model::action::ProgramParameters;
use crate::model::task::NewTask;
use std::{
    io,
    process::{Command, Output},
};
use tracing::info;

use super::super::error::ActionExecutionError;

// Long term this should be using another program directly from here.
// Instead of having a split Heda-Controller / Heda-Executor, we can
// keep all this in heda and have a LocalExecutor as a separate program
// that does the forking but that only interact with the Executor to
// abstrat this away from Controller.

pub struct ProgramRunner {}

impl ProgramRunner {
    pub fn run(conf: &ProgramParameters) -> Result<Vec<NewTask>, ActionExecutionError> {
        let program = &conf.program;
        let arguments = &conf.arguments;

        let output = Self::execute(program, arguments);

        match output {
            Ok(output) => info!(
                "Program {} executed successfully. Output: {:?}",
                program, output
            ),
            Err(e) => {
                info!("IOError while running program {} : {}", program, e);
                return Err(ActionExecutionError::IOError);
            }
        }
        // Todo implement child tasks mechanism for programs

        Ok(vec![])
    }

    fn execute(program: &String, arguments: &Vec<String>) -> io::Result<Output> {
        Command::new(program).args(arguments).output()
    }
}

#[cfg(test)]
mod tests {

    use crate::model::task::NewTask;

    use super::*;

    #[test]
    fn test_execute() {
        // GIVEN
        let program = "echo".to_string();
        let arguments = vec!["hello".to_string(), "world".to_string()];

        // WHEN
        // let expected_status = std::process::ExitCode::SUCCESS;
        let expected_output = b"hello world\n";
        let output = ProgramRunner::execute(&program, &arguments).unwrap();

        // THEN
        assert!(
            output.status.success(),
            "The program did not execute successfully"
        );
        assert_eq!(
            output.stdout, expected_output,
            "The ouput did not match expectation"
        );
    }

    #[test]
    fn test_run_program_successful_no_child_tasks() {
        // GIVEN
        let program = "echo".to_string();
        let arguments = vec!["hello".to_string(), "world".to_string()];
        let program_parameters = ProgramParameters::new(program, arguments);

        // WHEN
        let expected_result: Vec<NewTask> = vec![];

        let result = ProgramRunner::run(&program_parameters).unwrap();

        // THEN
        assert_eq!(result, expected_result, "Command did not run as expected");
    }
}
