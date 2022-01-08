use crate::controller::{ControllerRequest, ControllerRequestPublisher};
use crate::datastore::TaskDataStore;
pub use crate::datastore::{Filter, HashMapStorage, MemoryTaskStorage};

pub mod proto {
    tonic::include_proto!("taskqueue.v1");
}

use crate::model::CorrelationId;
use anyhow::Result;
use proto::{task_queue_server::TaskQueue, FilterParams, Task, TaskList, TaskQueueResult};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{error, info};

pub struct Server<D> {
    datastore: Arc<RwLock<D>>,
    controller_sender: ControllerRequestPublisher,
}

impl<D> Server<D>
where
    D: std::marker::Sync,
{
    pub fn new(
        datastore: Arc<RwLock<D>>,
        controller_sender: ControllerRequestPublisher,
    ) -> Server<D> {
        return Server {
            datastore,
            controller_sender,
        };
    }

    pub async fn start(self) -> Result<()>
    where
        D: TaskDataStore + Send + 'static,
    {
        info!("Starting api...");
        let hostaddr = std::net::Ipv4Addr::UNSPECIFIED;
        let saddr = SocketAddr::V4(std::net::SocketAddrV4::new(hostaddr, 50051));

        let svc = proto::task_queue_server::TaskQueueServer::new(self);
        match tonic::transport::Server::builder()
            .add_service(svc)
            .serve(saddr)
            .await
        {
            Ok(_) => (),
            Err(err) => {
                error!(reason=%err.to_string(), "Unable to start API.");
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
}

#[tonic::async_trait]
impl<D> TaskQueue for Server<D>
where
    D: TaskDataStore + Send + 'static + std::marker::Sync,
{
    async fn publish(&self, request: Request<Task>) -> Result<Response<TaskQueueResult>, Status> {
        let correlation_id = CorrelationId::from_tonic_metadata(request.metadata())?;

        // let mut ds = self.datastore.write().await;
        let task: Task = request.into_inner();
        let model_task = crate::model::Task::new(task.name, correlation_id, task.parameters);

        self.controller_publish(model_task.clone())
            .await
            .map_err(|err_code| Status::unknown(format!("task push failed: {:?}", err_code)))
            .and_then(|_| {
                let reply = TaskQueueResult {
                    result_id: model_task.uuid.to_hyphenated().to_string(),
                    status: proto::task_queue_result::Status::Ok as i32,
                };
                Ok(Response::new(reply))
            })
    }

    async fn list(&self, request: Request<FilterParams>) -> Result<Response<TaskList>, Status> {
        let mut api_tasks: Vec<Task> = Vec::new();

        let mut filter = Filter::default();
        let filter_requested: FilterParams = request.into_inner();
        filter.state = Some(filter_requested.status());
        let tasks = self.datastore.read().await.items(&filter).await;
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
