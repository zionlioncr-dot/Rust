use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[async_trait]
pub trait FraudStateStore: Send + Sync {
    async fn record_failed_login(
        &self,
        tenant_id: &str,
        username: &str,
        event_id: Uuid,
        occurred_at: DateTime<Utc>,
    ) -> Result<i32>;
}
