use anyhow::Result;

use async_trait::async_trait;

use sqlx::query;
use sqlx::query_as;

use uuid::Uuid;

use domain::audit_event::AuditEvent;

use crate::{audit_repository::AuditRepository, postgres::repository::PostgresRepository};

#[async_trait]
impl AuditRepository for PostgresRepository {
    async fn create(&self, event: AuditEvent) -> Result<AuditEvent> {
        query(
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
        let event = query_as::<_, AuditEvent>(
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
        let events = query_as::<_, AuditEvent>(
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
