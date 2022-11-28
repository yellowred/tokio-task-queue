pub(crate) const CHANNEL_SIZE: usize = 1000;

pub struct RetryConfig {
    pub max_retries: u32,
    pub interval_ms: u64,
    pub max_interval_ms: u64,
    pub deadline_ms: u64,
}

impl RetryConfig {
    pub fn new(max_retries: u32, interval_ms: u64, max_interval_ms: u64, deadline_ms: u64) -> Self {
        Self {
            max_retries,
            interval_ms,
            max_interval_ms,
            deadline_ms,
        }
    }

    pub fn for_task(task_name: &str) -> RetryConfig {
        match task_name {
            _ => RetryConfig::new(10, 20, 86400000, 300000),
        }
    }
}
