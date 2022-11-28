use std::collections::HashMap;

use crate::model::CorrelationId;
use serde_derive::Deserialize;
use uuid::Uuid;

use super::{error::ModelError, Task};

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
#[serde(tag = "execution-type", content = "execution-parameters")]
pub enum ExecutionParameter {
    Program(ProgramParameters),
    Service(ServiceParameters),
}

// Representation of the work associated to a Task.
#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct Action {
    #[serde(skip_deserializing)]
    pub uuid: Uuid,
    pub name: String,
    pub correlation_id: CorrelationId,
    #[serde(flatten)]
    pub parameters: ExecutionParameter,
}

impl Action {
    pub fn new(task: &super::Task) -> Result<Self, ModelError> {
        let params = match &task.name[..] {
            "program" => Some(ExecutionParameter::Program(ProgramParameters::from_task(
                task,
            )?)),
            "service" => Some(ExecutionParameter::Service(ServiceParameters::from_task(
                task,
            )?)),
            _ => None,
        };

        Ok(Self {
            uuid: task.uuid.clone(),
            name: task.name.clone(),
            correlation_id: task.correlation_id.clone(),
            parameters: params.ok_or(ModelError::BadTaskParameters(format!(
                "task name is not recognized: {}",
                task.name
            )))?,
        })
    }
}

impl TryFrom<Task> for Action {
    type Error = ModelError;

    fn try_from(value: Task) -> Result<Self, Self::Error> {
        Action::new(&value)
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct ProgramParameters {
    pub program: String,
    pub arguments: Vec<String>,
}

impl ProgramParameters {
    #[allow(dead_code)]
    pub fn new(program: String, arguments: Vec<String>) -> Self {
        Self {
            program: program,
            arguments: arguments,
        }
    }

    pub fn from_task(task: &super::Task) -> Result<Self, ModelError> {
        let program = task
            .parameters
            .get("program")
            .ok_or(ModelError::BadTaskParameters(
                "program is not specified".to_string(),
            ))?;
        Ok(Self {
            program: program.clone(),
            arguments: task
                .parameters
                .get("arguments")
                .and_then(|x| Some(x.split_whitespace().map(|x| x.to_string()).collect()))
                .unwrap_or(vec![]),
        })
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct ServiceParameters {
    pub protocol: ServiceParametersProtocol,
    pub parameters: HashMap<String, String>,
    pub correlation_id: CorrelationId,
}

impl ServiceParameters {
    pub fn from_task(task: &super::Task) -> Result<Self, ModelError> {
        Ok(Self {
            protocol: task
                .parameters
                .get("protocol")
                .and_then(|x| {
                    if *x == "grpc".to_owned() {
                        return Some(ServiceParametersProtocol::Grpc);
                    }
                    None
                })
                .unwrap_or(ServiceParametersProtocol::Http),
            parameters: task.parameters.clone(),
            correlation_id: task.correlation_id,
        })
    }
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum ServiceParametersProtocol {
    Http,
    Grpc,
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, convert::TryFrom};

    use crate::{controller::api::proto::task::Priority, model::Task};

    use super::*;

    #[test]
    fn test_program() {
        let task1 = Task::new(
            "program".to_string(),
            CorrelationId::try_from(&"b7b054ca-0d37-418b-ab16-ebe8aa409285".to_string()).unwrap(),
            HashMap::new(),
            None,
            Priority::Medium,
        );
        let a = Action::new(&task1);
        assert!(a.is_err());
        assert_eq!(
            a.unwrap_err(),
            ModelError::BadTaskParameters("program is not specified".to_string())
        );

        let mut params2: HashMap<String, String> = HashMap::new();
        params2.insert("program".to_string(), "ssh".to_string());
        let task2 = Task::new(
            "program".to_string(),
            CorrelationId::try_from(&"b7b054ca-0d37-418b-ab16-ebe8aa409285".to_string()).unwrap(),
            params2,
            None,
            Priority::Medium,
        );
        let a = Action::new(&task2).unwrap();
        assert_eq!(a.name, "program".to_string());
        let p = match a.parameters {
            ExecutionParameter::Program(ap) => ap.program,
            _ => "error".to_string(),
        };
        assert_eq!(p, "ssh".to_string());
    }

    #[test]
    fn test_service() {
        let invalid_task = Task::new(
            "invalid".to_string(),
            CorrelationId::try_from(&"b7b054ca-0d37-418b-ab16-ebe8aa409285".to_string()).unwrap(),
            HashMap::new(),
            None,
            Priority::Medium,
        );
        let a = Action::new(&invalid_task);
        assert!(a.is_err());
        assert_eq!(
            a.unwrap_err(),
            ModelError::BadTaskParameters("task name is not recognized: invalid".to_string())
        );

        let mut params2: HashMap<String, String> = HashMap::new();
        params2.insert("url".to_string(), "localhost".to_string());
        let valid_task_def_protocol = Task::new(
            "service".to_string(),
            CorrelationId::try_from(&"b7b054ca-0d37-418b-ab16-ebe8aa409285".to_string()).unwrap(),
            params2,
            None,
            Priority::Medium,
        );
        let a = Action::new(&valid_task_def_protocol).unwrap();
        assert_eq!(a.name, "service".to_string());
        let p = match a.parameters {
            ExecutionParameter::Service(sp) => {
                (sp.parameters.get("url").unwrap().clone(), sp.protocol)
            }
            _ => ("error".to_string(), ServiceParametersProtocol::Grpc),
        };
        assert_eq!(p.0, "localhost".to_string());
        assert_eq!(p.1, ServiceParametersProtocol::Http);

        let mut params3: HashMap<String, String> = HashMap::new();
        params3.insert("url".to_string(), "localhost".to_string());
        params3.insert("protocol".to_string(), "grpc".to_string());
        let valid_task_specific_protocol = Task::new(
            "service".to_string(),
            CorrelationId::try_from(&"b7b054ca-0d37-418b-ab16-ebe8aa409285".to_string()).unwrap(),
            params3,
            None,
            Priority::Medium,
        );
        let a = Action::new(&valid_task_specific_protocol).unwrap();
        let p = match a.parameters {
            ExecutionParameter::Service(sp) => {
                (sp.parameters.get("url").unwrap().clone(), sp.protocol)
            }
            _ => ("error".to_string(), ServiceParametersProtocol::Grpc),
        };
        assert_eq!(p.0, "localhost".to_string());
        assert_eq!(p.1, ServiceParametersProtocol::Grpc);
    }
}
