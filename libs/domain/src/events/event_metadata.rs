use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    pub event_id: Uuid,

    pub correlation_id: Uuid,

    pub trace_id: String,

    pub source: String,

    pub timestamp: DateTime<Utc>,
}
