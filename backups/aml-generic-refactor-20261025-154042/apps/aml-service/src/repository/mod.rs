use crate::domain::{AccountActivity, AmlAlert};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ActiveAmlRegulatoryRule {
    pub jurisdiction: String,
    pub base_currency: String,
    pub rule_code: String,
    pub threshold_amount: Decimal,
    pub threshold_currency: String,
    pub minimum_transactions: Option<i32>,
    pub regulation_code: String,
    pub article: String,
    pub document_version: String,
    pub source_url: String,
    pub source_excerpt: String,
}

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

    /// Loads approved regulatory rules valid on the transaction date
    /// for the specified tenant.
    async fn active_regulatory_rules(
        &self,
        tenant_id: &str,
        as_of: NaiveDate,
    ) -> Result<Vec<ActiveAmlRegulatoryRule>>;

    async fn account_activity(
        &self,
        tenant_id: &str,
        account_id: &str,
        base_currency: &str,
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
            // Prevent cross-tenant or cross-event alert insertion.
            if alert.tenant_id != tenant_id || alert.event_id != event_id {
                anyhow::bail!("AML alert tenant_id/event_id does not match the claimed event");
            }

            sqlx::query(
                r#"
                INSERT INTO aml_alerts (
                    id,
                    tenant_id,
                    event_id,
                    transaction_id,
                    rule_code,
                    severity,
                    description,
                    details,
                    status,
                    created_at
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

    async fn active_regulatory_rules(
        &self,
        tenant_id: &str,
        as_of: NaiveDate,
    ) -> Result<Vec<ActiveAmlRegulatoryRule>> {
        let rules = sqlx::query_as::<_, ActiveAmlRegulatoryRule>(
            r#"
            SELECT
                p.jurisdiction,
                p.base_currency,
                r.rule_code,
                r.threshold_amount,
                r.threshold_currency,
                r.minimum_transactions,
                r.regulation_code,
                r.article,
                r.document_version,
                r.source_url,
                r.source_excerpt
            FROM aml_regulatory_rules AS r
            INNER JOIN aml_tenant_regulatory_profiles AS p
                ON p.id = r.profile_id
               AND p.tenant_id = r.tenant_id
            WHERE r.tenant_id = $1
              AND p.status = 'active'
              AND r.status = 'approved'
              AND p.effective_from <= $2
              AND (p.effective_to IS NULL OR p.effective_to >= $2)
              AND r.effective_from <= $2
              AND (r.effective_to IS NULL OR r.effective_to >= $2)
            ORDER BY r.rule_code
            "#,
        )
        .bind(tenant_id)
        .bind(as_of)
        .fetch_all(&self.pool)
        .await?;

        Ok(rules)
    }

    async fn account_activity(
        &self,
        tenant_id: &str,
        account_id: &str,
        base_currency: &str,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<AccountActivity> {
        let (transaction_count, total_amount): (i64, Decimal) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)::BIGINT,
                COALESCE(SUM(amount * exchange_rate), 0)::NUMERIC
            FROM financial_transactions
            WHERE tenant_id = $1
              AND source_account_id = $2
              AND base_currency = $3
              AND exchange_rate IS NOT NULL
              AND exchange_rate > 0
              AND occurred_at >= $4
              AND occurred_at <= $5
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(base_currency)
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
