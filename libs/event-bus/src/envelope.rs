use chrono::{DateTime, Utc};

use serde::{Deserialize, Serialize};

use serde_json::Value;

use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub id: Uuid,

    pub version: u16,

    pub event_type: String,

    pub occurred_at: DateTime<Utc>,

    pub correlation_id: String,

    pub payload: Value,
}

impl EventEnvelope {
    pub fn new(
        event_type: impl Into<String>,

        payload: Value,

        correlation_id: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),

            version: 1,

            event_type: event_type.into(),

            occurred_at: Utc::now(),

            correlation_id: correlation_id.into(),

            payload,
        }
    }
}
