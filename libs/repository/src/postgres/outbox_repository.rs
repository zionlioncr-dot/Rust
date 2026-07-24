use anyhow::Result;

use async_trait::async_trait;

use sqlx::{query, query_as};

use uuid::Uuid;

use domain::outbox_event::OutboxEvent;

use crate::{outbox_repository::OutboxRepository, postgres::repository::PostgresRepository};

#[async_trait]
impl OutboxRepository for PostgresRepository {
    async fn save(&self, event: OutboxEvent) -> Result<()> {
        query(
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

            (

                $1,

                $2,

                $3,

                $4,

                $5,

                $6,

                $7

            )

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
        let events = query_as::<_, OutboxEvent>(
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
        query(
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
