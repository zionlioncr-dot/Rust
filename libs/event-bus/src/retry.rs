use anyhow::Result;

use tokio::time::sleep;

use tracing::{error, info};

use crate::{EventBusError, RetryPolicy};

pub struct RetryExecutor {
    policy: RetryPolicy,
}

impl RetryExecutor {
    pub fn new(policy: RetryPolicy) -> Self {
        Self { policy }
    }

    pub async fn execute<F, Fut>(&self, mut operation: F) -> Result<()>
    where
        F: FnMut() -> Fut,

        Fut: std::future::Future<Output = Result<()>>,
    {
        let mut delay = self.policy.initial_delay;

        let mut attempt = 0;

        loop {
            attempt += 1;

            match operation().await {
                Ok(_) => {
                    info!(attempt, "Operation succeeded");

                    return Ok(());
                }

                Err(err) => {
                    error!(

                        attempt,

                        error = %err,

                        "Operation failed"

                    );

                    if attempt >= self.policy.max_retries {
                        return Err(EventBusError::RetryLimitExceeded.into());
                    }

                    sleep(delay).await;

                    delay *= self.policy.multiplier;
                }
            }
        }
    }
}
