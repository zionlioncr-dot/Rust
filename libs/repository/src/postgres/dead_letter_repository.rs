use anyhow::Result;

use async_trait::async_trait;

use sqlx::{query, query_as};

use uuid::Uuid;

use domain::events::dead_letter_event::DeadLetterEvent;

use crate::{
    dead_letter_repository::DeadLetterRepository, postgres::repository::PostgresRepository,
};

#[async_trait]
impl DeadLetterRepository for PostgresRepository {
    async fn save(&self, event: DeadLetterEvent) -> Result<()> {
        query(
            r#"
            INSERT INTO dead_letter_events
            (
                id,
                event_id,
                event_type,
                payload,
                error,
                attempts,
                created_at
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
        .bind(event.event_id)
        .bind(event.event_type)
        .bind(event.payload)
        .bind(event.error)
        .bind(event.attempts)
        .bind(event.created_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn find_by_id(&self, id: Uuid) -> Result<Option<DeadLetterEvent>> {
        let event = query_as::<_, DeadLetterEvent>(
            r#"
            SELECT
                id,
                event_id,
                event_type,
                payload,
                error,
                attempts,
                created_at
            FROM dead_letter_events
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(event)
    }

    async fn find_all(&self) -> Result<Vec<DeadLetterEvent>> {
        let events = query_as::<_, DeadLetterEvent>(
            r#"
            SELECT
                id,
                event_id,
                event_type,
                payload,
                error,
                attempts,
                created_at
            FROM dead_letter_events
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(events)
    }

    async fn delete(&self, id: Uuid) -> Result<()> {
        query(
            r#"
            DELETE
            FROM dead_letter_events
            WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn count(&self) -> Result<i64> {
        let total = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM dead_letter_events
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(total)
    }

    async fn clear(&self) -> Result<()> {
        query(
            r#"
            DELETE FROM dead_letter_events
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
