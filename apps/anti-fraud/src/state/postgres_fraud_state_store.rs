use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::fraud_state_store::FraudStateStore;

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
        event_id: Uuid,
        occurred_at: DateTime<Utc>,
    ) -> Result<i32> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin failed-login transaction")?;

        // Serialize concurrent updates for this tenant and username.
        // Hash collisions only serialize unrelated users; they do not merge their data.
        let lock_key = format!("{tenant_id}\u{1f}{username}");

        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(&lock_key)
            .execute(&mut *tx)
            .await
            .context("failed to lock failed-login state")?;

        // A replay of the same event must not increase the count.
        sqlx::query(
            r#"
            INSERT INTO fraud_failed_login_events
                (tenant_id, username, event_id, occurred_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (tenant_id, event_id) DO NOTHING
            "#,
        )
        .bind(tenant_id)
        .bind(username)
        .bind(event_id)
        .bind(occurred_at)
        .execute(&mut *tx)
        .await
        .context("failed to insert failed-login event")?;

        // Count events in the rolling five-minute window ending at this event time.
        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM fraud_failed_login_events
            WHERE tenant_id = $1
              AND username = $2
              AND occurred_at > $3 - INTERVAL '5 minutes'
              AND occurred_at <= $3
            "#,
        )
        .bind(tenant_id)
        .bind(username)
        .bind(occurred_at)
        .fetch_one(&mut *tx)
        .await
        .context("failed to count failed-login events")?;

        tx.commit()
            .await
            .context("failed to commit failed-login transaction")?;

        i32::try_from(count).context("failed-login count exceeds i32 range")
    }
}

#[cfg(test)]
mod postgres_idempotency_tests {
    use super::*;
    use chrono::Duration;
    use sqlx::postgres::PgPoolOptions;

    #[tokio::test]
    #[ignore = "requiere PostgreSQL explícito mediante TEST_DATABASE_URL"]
    async fn replaying_same_event_id_does_not_increase_failed_login_count() -> anyhow::Result<()> {
        let database_url = std::env::var("TEST_DATABASE_URL")
            .expect("define TEST_DATABASE_URL para ejecutar esta prueba");

        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await?;

        let store = PostgresFraudStateStore::new(pool.clone());
        let tenant_id = format!("__anti_fraud_idempotency_test_{}", Uuid::new_v4());
        let username = "__idempotency_test_user__";
        let base = Utc::now() - Duration::seconds(30);

        let first_event_id = Uuid::new_v4();
        let second_event_id = Uuid::new_v4();
        let third_event_id = Uuid::new_v4();

        let result: Result<(i32, i32, i32, i32)> = async {
            let first = store
                .record_failed_login(&tenant_id, username, first_event_id, base)
                .await?;

            let second_time = base + Duration::seconds(1);
            let second = store
                .record_failed_login(&tenant_id, username, second_event_id, second_time)
                .await?;

            let replay = store
                .record_failed_login(&tenant_id, username, second_event_id, second_time)
                .await?;

            let third = store
                .record_failed_login(
                    &tenant_id,
                    username,
                    third_event_id,
                    base + Duration::seconds(2),
                )
                .await?;

            Ok((first, second, replay, third))
        }
        .await;

        let cleanup = sqlx::query("DELETE FROM fraud_failed_login_events WHERE tenant_id = $1")
            .bind(&tenant_id)
            .execute(&pool)
            .await;

        pool.close().await;
        cleanup?;

        let (first, second, replay, third) = result?;

        assert_eq!(first, 1, "el primer evento debe contar como 1");
        assert_eq!(second, 2, "el segundo evento debe elevar el contador a 2");
        assert_eq!(
            replay, 2,
            "reprocesar el mismo evento no debe incrementar el contador"
        );
        assert_eq!(
            third, 3,
            "un tercer evento distinto debe elevar el contador a 3"
        );

        Ok(())
    }
}
