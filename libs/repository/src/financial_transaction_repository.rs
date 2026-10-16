use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

use domain::{financial_transaction::FinancialTransaction, outbox_event::OutboxEvent};

#[async_trait]
pub trait FinancialTransactionRepository: Send + Sync {
    /// Persist the transaction and its event atomically.
    async fn create_with_outbox(
        &self,
        transaction: FinancialTransaction,
        outbox_event: OutboxEvent,
    ) -> Result<FinancialTransaction>;

    /// Look up a transaction within the specified tenant.
    async fn find_by_id(&self, tenant_id: &str, id: Uuid) -> Result<Option<FinancialTransaction>>;
}
