use thiserror::Error;

#[derive(Debug, Error)]

pub enum EventBusError {
    #[error("Maximum retries exceeded")]
    RetryLimitExceeded,

    #[error("Event processing failed: {0}")]
    Processing(String),
}
