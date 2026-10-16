use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Financial transaction received from a bank or financial institution.
/// Monetary values use Decimal to avoid binary floating-point errors.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct FinancialTransaction {
    pub id: Uuid,
    pub tenant_id: String,

    /// Identifier assigned by the originating institution, when available.
    pub external_transaction_id: Option<String>,

    pub amount: Decimal,
    pub currency: String,

    pub source_account_id: String,
    pub destination_account_id: String,

    pub counterparty: Option<String>,
    pub country_code: String,
    pub channel: String,

    /// IP reported by the originating bank or institution.
    pub client_ip: Option<String>,

    /// Time at which the financial transaction occurred.
    pub occurred_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod serialization_tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn decimal_serializes_as_string_without_precision_loss() {
        let amount =
            Decimal::from_str("12345678901234567890.12345678").expect("valid decimal amount");

        let serialized = serde_json::to_value(&amount).expect("Decimal should serialize to JSON");

        assert!(
            serialized.is_string(),
            "Expected Decimal as JSON string for the AML Avro schema, got: {serialized}"
        );

        assert_eq!(
            serialized.as_str(),
            Some("12345678901234567890.12345678"),
            "Decimal serialization must preserve the exact amount"
        );
    }
}
