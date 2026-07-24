use std::time::Duration;

#[derive(Debug, Clone)]

pub struct RetryPolicy {
    pub max_retries: u32,

    pub initial_delay: Duration,

    pub multiplier: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,

            initial_delay: Duration::from_secs(1),

            multiplier: 2,
        }
    }
}
