use chrono::{DateTime, Utc};

use serde::{Deserialize, Serialize};

use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditCreatedEvent {
    pub id: Uuid,

    pub user: String,

    pub action: String,

    #[serde(default)]
    pub client_ip: Option<String>,

    pub created_at: DateTime<Utc>,
}
