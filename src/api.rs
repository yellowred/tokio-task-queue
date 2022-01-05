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
use tokio::sync::{Mutex, RwLock};
use tonic::{Request, Response, Status};
use tracing::{error, info, span, Level};

pub struct Server<D> {
    datastore: Arc<RwLock<D>>,
}

impl<D> Server<D>
where
    D: std::marker::Sync,
{
    pub fn new(datastore: Arc<RwLock<D>>) -> Server<D> {
        return Server {
            datastore: datastore,
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
}

#[tonic::async_trait]
impl<D> TaskQueue for Server<D>
where
    D: TaskDataStore + Send + 'static + std::marker::Sync,
{
    async fn publish(&self, request: Request<Task>) -> Result<Response<TaskQueueResult>, Status> {
        let correlation_id = CorrelationId::from_tonic_metadata(request.metadata())?;

        let mut ds = self.datastore.write().await;
        let task: Task = request.into_inner();
        match ds.add(task.name, correlation_id, task.parameters).await {
            Ok(uuid) => {
                let reply = TaskQueueResult {
                    result_id: uuid.to_string(),
                    status: proto::task_queue_result::Status::Ok as i32,
                };

                Ok(Response::new(reply))
            }
            Err(err_code) => Err(Status::unknown(format!("task push failed: {:?}", err_code))),
        }
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
