#[cfg(test)]
mod tests {

    use std::collections::HashSet;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::time::sleep;

    use std::time::Duration;
    use futures::lock::Mutex;

    use tonic::{metadata::MetadataValue};

    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    use crate::api::proto::{task_queue_client::TaskQueueClient, task::Priority, task::State, FilterParams, Task};

    #[tokio::test]
    async fn test_e2e_task_publish() {
        let mut names = vec![];
        let uuids = vec![];

        for _x in 0..3 {
            let rand_string: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .map(char::from)
                .collect();
            names.push(rand_string);
        }

        let mut handles = vec![];
        let shared_uuids = Arc::new(Mutex::new(uuids));
        for _name in names.iter() {
            handles.push(tokio::spawn(publish_task(
                "program".to_string(),
                shared_uuids.clone(),
            )));
        }
        futures::future::join_all(handles).await;

        // println!("uuids={:?}", shared_uuids.lock().await);

        // wait for task queue to execute all the submitted tasks, increase the timeout if needed
        sleep(Duration::from_millis(200)).await;

        let list = task_list().await;

        // Uncomment to test that no extra tasks created
        //assert_eq!(list.len(), names.len(), "number of tasks in queue equals to the number published");
        
        let uuids_set:HashSet<String> = shared_uuids.lock().await.iter().cloned().collect();
        assert_eq!(
            list.iter()
                .filter(|task| uuids_set.contains(&task.uuid))
                .count(),
            names.len()
        );
    }

    async fn publish_task(task_name: String, uuids: Arc<Mutex<Vec<String>>>) {
        let mut client = TaskQueueClient::connect("http://localhost:50051").await.unwrap();

        let mut params = HashMap::new();
        params.insert("program".to_string(), "echo".to_string());
        let mut request = tonic::Request::new(Task {
            name: task_name.to_string(),
            correlation_id: "b7b054ca-0d37-418b-ab16-ebe8aa409285".to_string(),
            stream: "stream".to_string(),
            uuid: "".to_string(),
            state: State::New as i32,
            priority: Priority::Medium as i32,
            timestamp: 0,
            updated_at: 0,
            parameters: params,
        });
        request.metadata_mut().insert(
            "correlation-id",
            MetadataValue::from_str("b7b054ca-0d37-418b-ab16-ebe8aa409285").unwrap(),
        );
        request.metadata_mut().insert(
            "client-id",
            MetadataValue::from_str("b7b054ca-0d37-418b-ab16-ebe8aa409285").unwrap(),
        );

        let response = client.publish(request).await.unwrap();
        uuids
            .lock()
            .await
            .push(response.get_ref().result_id.clone());
    }

    async fn task_list() -> Vec<Task> {
        let mut client = TaskQueueClient::connect("http://localhost:50051").await.unwrap();
        let mut request = tonic::Request::new(FilterParams {
            status: State::Success as i32,
            published_from: 0,
        });
        request.metadata_mut().insert(
            "correlation-id",
            MetadataValue::from_str("b7b054ca-0d37-418b-ab16-ebe8aa409285").unwrap(),
        );
        request.metadata_mut().insert(
            "client-id",
            MetadataValue::from_str("b7b054ca-0d37-418b-ab16-ebe8aa409285").unwrap(),
        );

        let response = client.list(request).await.unwrap();
        response.get_ref().tasks.clone()
    }
}
