use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaEvent {
    pub key: Option<String>,

    pub payload: Vec<u8>,

    pub traceparent: Option<String>,
}
