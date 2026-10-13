use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};

use super::fraud_state_store::FraudStateStore;

#[derive(Debug, FromRow)]
struct FailedLoginStateRow {
    attempt_count: i32,
}

#[derive(Clone)]
pub struct PostgresFraudStateStore {
    pool: PgPool,
}

impl PostgresFraudStateStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl FraudStateStore for PostgresFraudStateStore {
    async fn record_failed_login(
        &self,
        tenant_id: &str,
        username: &str,
        now: DateTime<Utc>,
    ) -> Result<i32> {
        let row = sqlx::query_as::<_, FailedLoginStateRow>(
            r#"
            INSERT INTO fraud_failed_login_state
            (
                tenant_id,
                username,
                window_start,
                attempt_count,
                updated_at
            )
            VALUES ($1, $2, $3, 1, $3)
            ON CONFLICT (tenant_id, username)
            DO UPDATE SET
                window_start = CASE
                    WHEN EXCLUDED.updated_at
                         - fraud_failed_login_state.window_start
                         <= INTERVAL '5 minutes'
                    THEN fraud_failed_login_state.window_start
                    ELSE EXCLUDED.updated_at
                END,
                attempt_count = CASE
                    WHEN EXCLUDED.updated_at
                         - fraud_failed_login_state.window_start
                         <= INTERVAL '5 minutes'
                    THEN fraud_failed_login_state.attempt_count + 1
                    ELSE 1
                END,
                updated_at = EXCLUDED.updated_at
            RETURNING attempt_count
            "#,
        )
        .bind(tenant_id)
        .bind(username)
        .bind(now)
        .fetch_one(&self.pool)
        .await
        .context("failed to record failed login state")?;

        Ok(row.attempt_count)
    }
}
