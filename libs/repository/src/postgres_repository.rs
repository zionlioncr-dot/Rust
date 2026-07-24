use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use domain::audit_event::AuditEvent;
use domain::outbox_event::OutboxEvent;

use crate::{audit_repository::AuditRepository, outbox_repository::OutboxRepository};

pub struct PostgresRepository {
    pool: PgPool,
}

impl PostgresRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AuditRepository for PostgresRepository {
    async fn create(&self, event: AuditEvent) -> Result<AuditEvent> {
        sqlx::query(
            r#"
            INSERT INTO audit_events
            (
                id,
                username,
                action,
                created_at
            )
            VALUES
            (
                $1,
                $2,
                $3,
                $4
            )
            "#,
        )
        .bind(event.id)
        .bind(&event.user)
        .bind(&event.action)
        .bind(event.created_at)
        .execute(&self.pool)
        .await?;

        Ok(event)
    }

    async fn find_by_id(&self, id: Uuid) -> Result<Option<AuditEvent>> {
        let event = sqlx::query_as::<_, AuditEvent>(
            r#"
            SELECT
                id,
                username AS user,
                action,
                created_at
            FROM audit_events
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(event)
    }

    async fn find_all(&self) -> Result<Vec<AuditEvent>> {
        let events = sqlx::query_as::<_, AuditEvent>(
            r#"
            SELECT
                id,
                username AS user,
                action,
                created_at
            FROM audit_events
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(events)
    }
}

#[async_trait]
impl OutboxRepository for PostgresRepository {
    async fn save(&self, event: OutboxEvent) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO outbox_events
            (
                id,
                aggregate_type,
                aggregate_id,
                event_type,
                payload,
                created_at,
                published
            )
            VALUES
            ($1,$2,$3,$4,$5,$6,$7)
            "#,
        )
        .bind(event.id)
        .bind(event.aggregate_type)
        .bind(event.aggregate_id)
        .bind(event.event_type)
        .bind(event.payload)
        .bind(event.created_at)
        .bind(event.published)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn find_unpublished(&self, limit: i64) -> Result<Vec<OutboxEvent>> {
        let events = sqlx::query_as::<_, OutboxEvent>(
            r#"
        SELECT
            id,
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
            created_at,
            published
        FROM outbox_events
        WHERE published = false
        ORDER BY created_at
        LIMIT $1
        "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(events)
    }

    async fn mark_as_published(&self, id: Uuid) -> Result<()> {
        sqlx::query(
            r#"
        UPDATE outbox_events
        SET published = true
        WHERE id = $1
        "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
