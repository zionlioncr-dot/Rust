use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaEvent {
    pub key: Option<String>,

    pub payload: String,
}
