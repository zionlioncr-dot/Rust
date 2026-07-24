use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedEvent {
    pub event_id: Uuid,
    pub consumer: String,
    pub handler: String,
    pub processed_at: DateTime<Utc>,
}
