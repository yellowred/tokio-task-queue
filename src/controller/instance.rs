use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

pub struct InstanceController<S>
where
    S: InstanceStorage,
{
    uuid: uuid::Uuid,
    term: Arc<AtomicBool>,
    alive_timeout_sec: u32,
    storage: S,
}

#[tonic::async_trait]
pub trait InstanceStorage {
    fn ping(&mut self, instance_uuid: &uuid::Uuid) -> anyhow::Result<()>;
    fn get_active_replicas(&mut self, alive_timeout_sec: &u32) -> anyhow::Result<Vec<uuid::Uuid>>;
}

impl<S> InstanceController<S>
where
    S: InstanceStorage,
{
    pub fn new(term: Arc<AtomicBool>, alive_timeout_sec: u32, storage: S) -> anyhow::Result<Self> {
        Ok(Self {
            uuid: uuid::Uuid::new_v4(),
            term,
            alive_timeout_sec,
            storage,
        })
    }

    pub fn ping(&mut self) -> anyhow::Result<()> {
        self.storage.ping(&self.uuid)?;
        Ok(())
    }

    pub fn terminate(&self) {
        self.term.store(true, Ordering::Release);
    }

    pub fn get_id(&self) -> &uuid::Uuid {
        &self.uuid
    }

    pub fn get_active_replicas(&mut self) -> anyhow::Result<Vec<uuid::Uuid>> {
        self.storage.get_active_replicas(&self.alive_timeout_sec)
    }
}

impl<S> core::fmt::Debug for InstanceController<S>
where
    S: InstanceStorage,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.uuid.hyphenated().to_string().as_str())
    }
}

#[cfg(all(test, feature = "test_db"))]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use std::env;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_items() {
        dotenv().ok();

        // GIVEN
        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL");
        let instance_storage =
            crate::datastore::InstanceStoragePg::connect(&database_url).expect("Instance storage");

        let term = Arc::new(AtomicBool::new(false));
        let mut replica_instance =
            crate::controller::InstanceController::new(term.clone(), 1, instance_storage)
                .expect("Replica liveliness connected.");

        replica_instance.ping().unwrap();
        let active_replicas = replica_instance.get_active_replicas().unwrap();
        assert_eq!(1, active_replicas.len());
        assert_eq!(
            replica_instance.get_id().as_u128(),
            active_replicas[0].as_u128()
        );
    }
}
