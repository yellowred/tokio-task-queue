use crate::controller::{ControllerRequest, ControllerRequestPublisher};
pub use crate::datastore::{Filter, HashMapStorage, MemoryTaskStorage};

pub mod proto {
    tonic::include_proto!("taskqueue.v1");
    tonic::include_proto!("grpc.health.v1");
}
use crate::model::CorrelationId;
use anyhow::{anyhow, Result};
use proto::{
    health_check_response::ServingStatus,
    health_server::{Health, HealthServer},
    HealthCheckRequest, HealthCheckResponse,
};
use proto::{
    task_queue_server::{TaskQueue, TaskQueueServer},
    FilterParams, Task, TaskId, TaskList, TaskQueueResult,
};

use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::Sender;

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, instrument};

use super::storage::{self, RequestResponse, StorageServiceRequest};

pub struct ServerConfig {
    pub port: u16,
    pub concurrent: Option<usize>,
    pub timeout: Option<u64>,
}

pub struct Server {
    config: ServerConfig,
    controller_sender: ControllerRequestPublisher,
    storage_sender: Sender<RequestResponse>,
}
struct HealthCheckServer {}

impl HealthCheckServer {
    fn new() -> Self {
        Self {}
    }
}

impl Server {
    pub fn new(
        config: ServerConfig,
        controller_sender: ControllerRequestPublisher,
        storage_sender: Sender<RequestResponse>,
    ) -> Server {
        return Server {
            config,
            controller_sender,
            storage_sender,
        };
    }

    pub async fn start(self) -> Result<()> {
        info!("Starting api...");
        let hostaddr = std::net::Ipv4Addr::UNSPECIFIED;
        let saddr = SocketAddr::V4(std::net::SocketAddrV4::new(hostaddr, self.config.port));

        let timeout = std::time::Duration::new(self.config.timeout.unwrap_or(30), 0);
        let concurrency = self.config.concurrent.unwrap_or(30);
        let task_queue_service = TaskQueueServer::new(self);
        let health_service = HealthServer::new(HealthCheckServer::new());

        match tonic::transport::Server::builder()
            .concurrency_limit_per_connection(concurrency)
            .timeout(timeout)
            .add_service(task_queue_service)
            .add_service(health_service)
            .serve(saddr)
            .await
        {
            Ok(_) => (),
            Err(err) => {
                error!(reason=%err.to_string(), "Unable to start API.");
                anyhow::bail!("Unable to start API.")
            }
        }
        Ok(())
    }

    pub async fn controller_publish(&self, task: crate::model::Task) -> Result<()> {
        let (req_sender, callback) = tokio::sync::oneshot::channel();

        self.controller_sender
            .send(ControllerRequest::SubmitTask(task, req_sender))
            .await?;

        // do not await callback for faster api reposonse
        tokio::spawn(async move {
            match callback.await {
                Ok(v) => {
                    if let Err(err) = v {
                        error!(reason = %err, "task publish callback error")
                    }
                }
                Err(err) => {
                    error!(reason = %err, "task publish callback receiver error")
                }
            }
        });

        Ok(())
    }

    async fn controller_archive(&self, task_id: String) -> anyhow::Result<String> {
        let (req_sender, callback) = tokio::sync::oneshot::channel();

        self.controller_sender
            .send(ControllerRequest::CancelTask(task_id, req_sender))
            .await?;

        match callback.await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(err)) => Err(anyhow!(err)),
            Err(err) => Err(anyhow!(err)),
        }
    }

    async fn controller_retry(&self, task_id: String) -> anyhow::Result<String> {
        let (req_sender, callback) = tokio::sync::oneshot::channel();

        self.controller_sender
            .send(ControllerRequest::RetryTask(task_id, req_sender))
            .await?;

        match callback.await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(err)) => Err(anyhow!(err)),
            Err(err) => Err(anyhow!(err)),
        }
    }

    async fn controller_clone_task(&self, task_id: String) -> anyhow::Result<String> {
        let (req_sender, callback) = tokio::sync::oneshot::channel();

        self.controller_sender
            .send(ControllerRequest::CloneTask(task_id, req_sender))
            .await?;

        match callback.await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(err)) => Err(anyhow!(err)),
            Err(err) => Err(anyhow!(err)),
        }
    }
}

impl fmt::Debug for Server {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "iris_grpc_server")
    }
}

#[tonic::async_trait]
impl TaskQueue for Server {
    #[instrument]
    async fn publish(&self, request: Request<Task>) -> Result<Response<TaskQueueResult>, Status> {
        let correlation_id = CorrelationId::from_tonic_metadata(request.metadata())?;

        // let mut ds = self.datastore.write().await;
        let task: Task = request.into_inner();
        let task_priority = task.priority();
        let model_task = crate::model::Task::new(
            task.name,
            correlation_id,
            task.parameters,
            Some(task.stream),
            task_priority,
        );

        self.controller_publish(model_task.clone())
            .await
            .map_err(|err_code| Status::unknown(format!("task push failed: {:?}", err_code)))
            .and_then(|_| {
                let reply = TaskQueueResult {
                    result_id: model_task.uuid.hyphenated().to_string(),
                    status: proto::task_queue_result::Status::Ok as i32,
                    error: "".to_string(),
                };
                Ok(Response::new(reply))
            })
    }

    #[instrument]
    async fn archive(&self, request: Request<TaskId>) -> Result<Response<TaskQueueResult>, Status> {
        let task_id = request.into_inner();

        let task_uuid = uuid::Uuid::from_slice(&task_id.id);
        if let Err(err) = task_uuid {
            let r = TaskQueueResult {
                result_id: "".into(),
                status: proto::task_queue_result::Status::Fail as i32,
                error: err.to_string(),
            };
            return Ok(Response::new(r));
        }
        let reply = match self
            .controller_archive(task_uuid.unwrap().hyphenated().to_string())
            .await
        {
            Ok(id) => TaskQueueResult {
                result_id: id,
                status: proto::task_queue_result::Status::Ok as i32,
                error: "".to_string(),
            },
            Err(err) => TaskQueueResult {
                result_id: "".into(),
                status: proto::task_queue_result::Status::Fail as i32,
                error: err.to_string(),
            },
        };
        Ok(Response::new(reply))
    }

    async fn retry(&self, request: Request<TaskId>) -> Result<Response<TaskQueueResult>, Status> {
        let task_id = request.into_inner();

        let task_uuid = uuid::Uuid::from_slice(&task_id.id);
        if let Err(err) = task_uuid {
            error!(error=%err, "Retry task error");

            let r = TaskQueueResult {
                result_id: "".into(),
                status: proto::task_queue_result::Status::Fail as i32,
                error: err.to_string(),
            };
            return Ok(Response::new(r));
        }
        let reply = match self
            .controller_retry(task_uuid.unwrap().hyphenated().to_string())
            .await
        {
            Ok(id) => TaskQueueResult {
                result_id: id,
                status: proto::task_queue_result::Status::Ok as i32,
                error: "".to_string(),
            },
            Err(err) => {
                error!(error=%err, "Retry task error");
                TaskQueueResult {
                    result_id: "".into(),
                    status: proto::task_queue_result::Status::Fail as i32,
                    error: err.to_string(),
                }
            }
        };
        Ok(Response::new(reply))
    }

    async fn clone_task(
        &self,
        request: Request<TaskId>,
    ) -> Result<Response<TaskQueueResult>, Status> {
        let task_id = request.into_inner();

        let task_uuid = uuid::Uuid::from_slice(&task_id.id);
        if let Err(err) = task_uuid {
            error!(error=%err, "Retry task error");

            let r = TaskQueueResult {
                result_id: "".into(),
                status: proto::task_queue_result::Status::Fail as i32,
                error: err.to_string(),
            };
            return Ok(Response::new(r));
        }
        let reply = match self
            .controller_clone_task(task_uuid.unwrap().hyphenated().to_string())
            .await
        {
            Ok(id) => TaskQueueResult {
                result_id: id,
                status: proto::task_queue_result::Status::Ok as i32,
                error: "".to_string(),
            },
            Err(err) => {
                error!(error=%err, "Retry task error");
                TaskQueueResult {
                    result_id: "".into(),
                    status: proto::task_queue_result::Status::Fail as i32,
                    error: err.to_string(),
                }
            }
        };
        Ok(Response::new(reply))
    }

    #[instrument]
    async fn list(&self, request: Request<FilterParams>) -> Result<Response<TaskList>, Status> {
        let filter_requested = request.into_inner();
        let mut api_tasks: Vec<Task> = Vec::new();

        let mut filter = Filter::default();
        filter.state = Some(filter_requested.status());

        let tasks: Vec<crate::model::Task> = storage::send(
            self.storage_sender.clone(),
            StorageServiceRequest::List(filter),
        )
        .await
        .unwrap();

        for task in tasks.into_iter() {
            api_tasks.push(Task {
                correlation_id: task.correlation_id.as_hyphenated().to_string(),
                stream: task.stream.unwrap_or_default(),
                parameters: HashMap::new(),
                name: task.name,
                uuid: task.uuid.to_string(),
                state: task.state as i32,
                priority: task.priority as i32,
                timestamp: task.timestamp.timestamp_millis(),
                updated_at: task.updated_at.unwrap_or(task.timestamp).timestamp_millis(),
            })
        }
        let reply = TaskList { tasks: api_tasks };

        Ok(Response::new(reply))
    }
}

#[tonic::async_trait]
impl Health for HealthCheckServer {
    async fn check(
        &self,
        _: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            status: ServingStatus::Serving as i32,
        }))
    }

    async fn watch(
        &self,
        _: Request<HealthCheckRequest>,
    ) -> Result<Response<Streaming<HealthCheckResponse>>, Status> {
        todo!()
    }

    type WatchStream = Streaming<HealthCheckResponse>;
}

pub fn build_runtime(
    cfg: ServerConfig,
    tx_request: Sender<ControllerRequest>,
    tx_storage: Sender<RequestResponse>,
) -> Runtime {
    let runtime = Builder::new_multi_thread()
        .thread_name("grpc-api")
        .enable_all()
        .build()
        .expect("[grpc-api] failed to create runtime");

    let api = Server::new(cfg, tx_request, tx_storage);
    runtime.handle().spawn(async { api.start().await });
    runtime
}
