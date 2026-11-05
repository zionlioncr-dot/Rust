use crate::domain::{AccountActivity, AmlAlert};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;

#[async_trait]
pub trait AmlRepository: Send + Sync {
    /// Persists the alerts and marks the event as processed atomically.
    ///
    /// Returns true if this call claimed and processed the event.
    /// Returns false if another worker already processed it.
    async fn process_event(
        &self,
        tenant_id: &str,
        event_id: &str,
        alerts: &[AmlAlert],
    ) -> Result<bool>;

    async fn account_activity(
        &self,
        tenant_id: &str,
        account_id: &str,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<AccountActivity>;
}

#[derive(Clone)]
pub struct PostgresAmlRepository {
    pool: PgPool,
}

impl PostgresAmlRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AmlRepository for PostgresAmlRepository {
    async fn process_event(
        &self,
        tenant_id: &str,
        event_id: &str,
        alerts: &[AmlAlert],
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;

        // The primary key (tenant_id, event_id) arbitrates concurrent workers.
        // Only the worker that inserts this row may persist the alerts.
        let claimed = sqlx::query_scalar::<_, String>(
            r#"
            INSERT INTO aml_processed_events (tenant_id, event_id)
            VALUES ($1, $2)
            ON CONFLICT (tenant_id, event_id) DO NOTHING
            RETURNING event_id
            "#,
        )
        .bind(tenant_id)
        .bind(event_id)
        .fetch_optional(&mut *tx)
        .await?;

        if claimed.is_none() {
            tx.rollback().await?;
            return Ok(false);
        }

        for alert in alerts {
            // Do not allow an alert for a different tenant or event to enter
            // this event's transaction.
            if alert.tenant_id != tenant_id || alert.event_id != event_id {
                anyhow::bail!("AML alert tenant_id/event_id does not match the claimed event");
            }

            sqlx::query(
                r#"
                INSERT INTO aml_alerts (
                    id, tenant_id, event_id, transaction_id, rule_code,
                    severity, description, details, status, created_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'open', $9)
                ON CONFLICT (tenant_id, event_id, rule_code) DO NOTHING
                "#,
            )
            .bind(alert.id)
            .bind(&alert.tenant_id)
            .bind(&alert.event_id)
            .bind(alert.transaction_id)
            .bind(&alert.rule_code)
            .bind(&alert.severity)
            .bind(&alert.description)
            .bind(&alert.details)
            .bind(alert.created_at)
            .execute(&mut *tx)
            .await?;
        }

        // Alert persistence and the idempotency record commit together.
        tx.commit().await?;

        Ok(true)
    }

    async fn account_activity(
        &self,
        tenant_id: &str,
        account_id: &str,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<AccountActivity> {
        let (transaction_count, total_amount): (i64, Decimal) = sqlx::query_as(
            r#"
            SELECT COUNT(*)::BIGINT, COALESCE(SUM(amount), 0)::NUMERIC
            FROM financial_transactions
            WHERE tenant_id = $1
              AND source_account_id = $2
              AND occurred_at >= $3
              AND occurred_at <= $4
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(since)
        .bind(until)
        .fetch_one(&self.pool)
        .await?;

        Ok(AccountActivity {
            transaction_count,
            total_amount,
        })
    }
}
