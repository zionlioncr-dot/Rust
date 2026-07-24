use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct OutboxEvent {
    pub id: Uuid,

    pub aggregate_type: String,

    pub aggregate_id: Uuid,

    pub event_type: String,

    pub payload: Value,

    pub created_at: DateTime<Utc>,

    pub published: bool,
}
