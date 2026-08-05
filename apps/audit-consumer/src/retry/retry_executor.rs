use std::future::Future;

use anyhow::Result;

use tokio::time::sleep;

use tracing::{error, warn};

use metrics::consumer_metrics;

use super::retry_policy::RetryPolicy;

pub struct RetryExecutor {
    policy: RetryPolicy,
}

impl RetryExecutor {
    pub fn new(policy: RetryPolicy) -> Self {
        Self { policy }
    }

    pub async fn execute<F, Fut>(&self, operation: F) -> Result<()>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let mut attempt = 1;

        loop {
            match operation().await {
                Ok(_) => return Ok(()),

                Err(error) => {
                    if !self.policy.should_retry(attempt) {
                        error!(
                            attempt = attempt,
                            error = %error,
                            "Retry limit reached"
                        );

                        return Err(error);
                    }

                    consumer_metrics::retry();

                    let delay = self.policy.delay_for(attempt);

                    warn!(
                        attempt = attempt,
                        delay_ms = delay.as_millis(),
                        error = %error,
                        "Retrying operation"
                    );

                    sleep(delay).await;

                    attempt += 1;
                }
            }
        }
    }

    pub fn policy(&self) -> &RetryPolicy {
        &self.policy
    }
}

impl Default for RetryExecutor {
    fn default() -> Self {
        Self {
            policy: RetryPolicy::default(),
        }
    }
}