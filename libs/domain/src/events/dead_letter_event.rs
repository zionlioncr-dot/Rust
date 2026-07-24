use chrono::{DateTime, Utc};

use serde::{Deserialize, Serialize};

use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterEvent {
    pub id: Uuid,

    pub event_id: Uuid,

    pub event_type: String,

    pub payload: serde_json::Value,

    pub error: String,

    pub attempts: i32,

    pub created_at: DateTime<Utc>,
}
