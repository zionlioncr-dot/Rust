use anyhow::Result;

use async_trait::async_trait;

use sqlx::{query, query_as};

use uuid::Uuid;

use domain::events::processed_event::ProcessedEvent;

use crate::{
    postgres::repository::PostgresRepository, processed_event_repository::ProcessedEventRepository,
};

#[async_trait]
impl ProcessedEventRepository for PostgresRepository {
    async fn exists(&self, event_id: Uuid) -> Result<bool> {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM processed_events
                WHERE event_id = $1
            )
            "#,
        )
        .bind(event_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(exists)
    }

    async fn save(&self, event: ProcessedEvent) -> Result<()> {
        query(
            r#"
            INSERT INTO processed_events
            (
                event_id,
                consumer,
                handler,
                processed_at
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
        .bind(event.event_id)
        .bind(&event.consumer)
        .bind(&event.handler)
        .bind(event.processed_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, event_id: Uuid) -> Result<()> {
        query(
            r#"
            DELETE
            FROM processed_events
            WHERE event_id = $1
            "#,
        )
        .bind(event_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn find_by_id(&self, event_id: Uuid) -> Result<Option<ProcessedEvent>> {
        let event = query_as::<_, ProcessedEvent>(
            r#"
            SELECT
                event_id,
                consumer,
                handler,
                processed_at
            FROM processed_events
            WHERE event_id = $1
            "#,
        )
        .bind(event_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(event)
    }

    async fn count(&self) -> Result<i64> {
        let total = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM processed_events
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(total)
    }

    async fn clear(&self) -> Result<()> {
        query(
            r#"
            DELETE FROM processed_events
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
