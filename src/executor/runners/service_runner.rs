use std::collections::HashMap;
use std::str::FromStr;

use crate::model::action::{ServiceParameters, ServiceParametersProtocol};
use crate::model::error::ModelError;
use crate::model::task::NewTask;
use crate::model::CorrelationId;
use anyhow::anyhow;
use hyper::header::HeaderName;
use reqwest::{header::HeaderMap, header::HeaderValue, Method};
use serde::{Deserialize, Serialize};

use super::super::error::ActionExecutionError;

pub struct ServiceRunner {}

impl ServiceRunner {
    pub async fn run(conf: ServiceParameters) -> Result<Vec<NewTask>, ActionExecutionError> {
        match conf.protocol {
            ServiceParametersProtocol::Http => {
                Self::execute_http_task(&conf.correlation_id, conf.parameters)
                    .await
                    .map_err(|err| ActionExecutionError::HttpError(err.to_string()))
            }
            ServiceParametersProtocol::Grpc => todo!("not implemented"),
        }
    }

    async fn execute_http_task(
        cid: &CorrelationId,
        mut parameters: HashMap<String, String>,
    ) -> anyhow::Result<Vec<NewTask>> {
        let mut rheaders = HeaderMap::new();
        cid.insert_into_header_map(&mut rheaders)?;

        if let Some(headers_string) = parameters.remove("headers") {
            let provided_headers: HashMap<String, String> =
                serde_json::from_value(serde_json::Value::String(headers_string))?;
            for h in provided_headers.into_iter() {
                let val = HeaderValue::from_str(&h.1)?;
                rheaders.insert(HeaderName::from_str(h.0.as_str())?, val);
            }
        }

        let url = parameters
            .get("url")
            .ok_or(ModelError::BadTaskParameters("url is missing".to_string()))?;

        let method = parameters
            .get("method")
            .ok_or(ModelError::BadTaskParameters(
                "method is missing".to_string(),
            ))
            .map(|x| Method::from_str(x))??;
        let client = reqwest::Client::new();
        let request_builder = match method {
            Method::GET => client.get(url),
            Method::POST => client
                .post(url)
                .body(parameters.get("body").unwrap_or(&"".to_string()).clone()),
            Method::PATCH => client.patch(url),
            Method::PUT => client.put(url),
            Method::DELETE => client.delete(url),
            _ => client.get(url),
        };

        let response = request_builder.headers(rheaders).send().await?;
        if response.status().is_success() {
            let res: ServiceResponse = response.json().await?;
            Ok(res.result)
        } else {
            Err(anyhow!(response.text().await?))
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct ServiceResponse {
    result: Vec<NewTask>,
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    // you could import any of the HTTP methods you are using
    use httpmock::Method::{GET, POST};
    use httpmock::MockServer;
    use serde_json::json;

    use crate::model::task::NewTask;

    use super::*;

    #[tokio::test]
    async fn test_success_tasks() {
        // GIVEN
        let url = "/hello".to_string();
        let task_1 = NewTask::new_service_call(
            None,
            CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string()).unwrap(),
            "url".to_string(),
            "method".to_string(),
            None,
        );
        let task_2 = NewTask::new_service_call(
            None,
            CorrelationId::try_from(&"00000000-0000-0000-0000-000000000000".to_string()).unwrap(),
            "url".to_string(),
            "method".to_string(),
            None,
        );
        let task_vec = vec![task_1, task_2];
        let body = json!(ServiceResponse {
            result: task_vec.clone()
        });
        // Create a mock on the server.
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/hello");
            then.status(200)
                .header("content-type", "application/json")
                .body(body.to_string());
        });
        server.mock(|when, then| {
            when.method(POST).path("/hello_post");
            then.status(200)
                .header("content-type", "application/json")
                .body(body.to_string());
        });

        // WHEN
        let mut params = HashMap::new();
        params.insert("url".to_string(), server.url(url.clone()));
        params.insert("method".to_string(), "GET".to_string());
        let output =
            ServiceRunner::execute_http_task(&CorrelationId::from(uuid::Uuid::new_v4()), params)
                .await
                .unwrap();

        // THEN
        assert_eq!(output, task_vec, "The output did not match expectation");

        // WHEN
        let mut params2 = HashMap::new();
        params2.insert("url".to_string(), server.url(url.clone()));
        params2.insert("method".to_string(), "GET".to_string());
        let output2 =
            ServiceRunner::execute_http_task(&CorrelationId::from(uuid::Uuid::new_v4()), params2)
                .await
                .unwrap();

        // THEN
        assert_eq!(output2, task_vec, "The output did not match expectation");
    }

    #[tokio::test]
    async fn test_fail_404() {
        // GIVEN
        let task_vec: Vec<NewTask> = vec![];
        let body = json!(ServiceResponse {
            result: task_vec.clone()
        });
        // Create a mock on the server.
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/hello");
            then.status(200)
                .header("content-type", "application/json")
                .body(body.to_string());
        });
        // WHEN
        let mut params = HashMap::new();
        params.insert("url".to_string(), server.url("/e404".to_string()));
        params.insert("method".to_string(), "GET".to_string());
        let output =
            ServiceRunner::execute_http_task(&CorrelationId::from(uuid::Uuid::new_v4()), params)
                .await
                .unwrap_err();

        // THEN
        assert_eq!(
            output.to_string(),
            "{\"message\":\"Request did not match any route or mock\"}".to_string(),
            "The ouput did not match expectation"
        );
    }
}
