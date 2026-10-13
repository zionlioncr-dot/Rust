use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::fraud_alert::FraudAlert;

#[async_trait]
pub trait FraudAlertRepository: Send + Sync {
    async fn create(&self, alert: &FraudAlert) -> Result<()>;

    async fn find_by_id(&self, tenant_id: &str, id: Uuid) -> Result<Option<FraudAlert>>;

    async fn find_by_event_id(&self, tenant_id: &str, event_id: Uuid)
        -> Result<Option<FraudAlert>>;

    async fn find_all(&self, tenant_id: &str) -> Result<Vec<FraudAlert>>;
}
