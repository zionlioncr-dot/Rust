use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmlAlert {
    pub id: Uuid,
    pub tenant_id: String,
    pub event_id: String,
    pub transaction_id: Uuid,
    pub rule_code: String,
    pub severity: String,
    pub description: String,
    pub details: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
pub struct AccountActivity {
    pub transaction_count: i64,
    pub total_amount: rust_decimal::Decimal,
}
