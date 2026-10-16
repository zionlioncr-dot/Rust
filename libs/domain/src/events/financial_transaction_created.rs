use serde::{Deserialize, Serialize};

use super::super::financial_transaction::FinancialTransaction;

/// Payload for a newly accepted financial transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinancialTransactionCreatedEvent {
    pub transaction: FinancialTransaction,
}
