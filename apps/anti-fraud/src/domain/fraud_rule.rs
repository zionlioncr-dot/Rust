use anyhow::Result;
use async_trait::async_trait;

use super::{fraud_decision::FraudDecision, fraud_event::FraudEvent};

#[async_trait]
pub trait FraudRule: Send + Sync {
    fn name(&self) -> &'static str;

    async fn evaluate(&self, event: &FraudEvent) -> Result<FraudDecision>;
}
