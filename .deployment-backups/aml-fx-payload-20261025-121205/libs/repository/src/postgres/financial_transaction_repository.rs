use anyhow::{ensure, Result};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;
use sqlx::query;
use uuid::Uuid;

use domain::{
    events::{
        event_envelope::EventEnvelope, event_metadata::EventMetadata,
        event_types::FINANCIAL_TRANSACTION_CREATED, event_version::EventVersion,
    },
    financial_transaction::FinancialTransaction,
    outbox_event::OutboxEvent,
};

use crate::{
    financial_transaction_repository::FinancialTransactionRepository,
    postgres::repository::PostgresRepository,
};

#[async_trait]
impl FinancialTransactionRepository for PostgresRepository {
    async fn create_with_outbox(
        &self,
        transaction: FinancialTransaction,
        outbox_event: OutboxEvent,
    ) -> Result<FinancialTransaction> {
        ensure!(
            !transaction.tenant_id.trim().is_empty(),
            "tenant_id is required"
        );
        ensure!(
            transaction.tenant_id == outbox_event.tenant_id,
            "Transaction and outbox tenant_id must match"
        );
        ensure!(
            transaction.id == outbox_event.aggregate_id,
            "Outbox aggregate_id must match transaction id"
        );
        ensure!(
            transaction.amount > rust_decimal::Decimal::ZERO,
            "Transaction amount must be positive"
        );
        ensure!(
            transaction.currency.len() == 3
                && transaction
                    .currency
                    .bytes()
                    .all(|b| b.is_ascii_alphabetic()),
            "currency must be a three-letter code"
        );
        ensure!(
            !transaction.source_account_id.trim().is_empty(),
            "source_account_id is required"
        );
        ensure!(
            !transaction.destination_account_id.trim().is_empty(),
            "destination_account_id is required"
        );
        ensure!(
            !transaction.country_code.trim().is_empty(),
            "country_code is required"
        );
        ensure!(
            !transaction.channel.trim().is_empty(),
            "channel is required"
        );

        let event_payload = EventEnvelope {
            metadata: EventMetadata {
                event_id: outbox_event.id,
                correlation_id: transaction.id,
                trace_id: String::new(),
                traceparent: None,
                source: "financial-transaction-service".to_string(),
                timestamp: Utc::now(),
                tenant_id: transaction.tenant_id.clone(),
            },
            version: EventVersion::default(),
            event_type: FINANCIAL_TRANSACTION_CREATED.to_string(),
            payload: json!({
                "transaction": transaction,
            }),
        };

        let serialized_payload = serde_json::to_value(&event_payload)?;

        let mut tx = self.pool.begin().await?;

        query(
            r#"
            INSERT INTO financial_transactions (
                id,
                tenant_id,
                external_transaction_id,
                amount,
                currency,
                source_account_id,
                destination_account_id,
                counterparty,
                country_code,
                channel,
                client_ip,
                occurred_at,
                created_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12, $13
            )
            "#,
        )
        .bind(transaction.id)
        .bind(&transaction.tenant_id)
        .bind(&transaction.external_transaction_id)
        .bind(transaction.amount)
        .bind(&transaction.currency)
        .bind(&transaction.source_account_id)
        .bind(&transaction.destination_account_id)
        .bind(&transaction.counterparty)
        .bind(&transaction.country_code)
        .bind(&transaction.channel)
        .bind(&transaction.client_ip)
        .bind(transaction.occurred_at)
        .bind(transaction.created_at)
        .execute(&mut *tx)
        .await?;

        query(
            r#"
            INSERT INTO outbox_events (
                id,
                tenant_id,
                aggregate_type,
                aggregate_id,
                event_type,
                payload,
                created_at,
                published
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE)
            "#,
        )
        .bind(outbox_event.id)
        .bind(&transaction.tenant_id)
        .bind("financial_transaction")
        .bind(transaction.id)
        .bind(FINANCIAL_TRANSACTION_CREATED)
        .bind(serialized_payload)
        .bind(Utc::now())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(transaction)
    }

    async fn find_by_id(&self, tenant_id: &str, id: Uuid) -> Result<Option<FinancialTransaction>> {
        let result = sqlx::query_as::<_, FinancialTransaction>(
            r#"
            SELECT
                id,
                tenant_id,
                external_transaction_id,
                amount,
                currency,
                source_account_id,
                destination_account_id,
                counterparty,
                country_code,
                channel,
                client_ip,
                occurred_at,
                created_at
            FROM financial_transactions
            WHERE tenant_id = $1
              AND id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(result)
    }
}
