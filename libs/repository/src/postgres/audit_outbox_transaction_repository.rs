use anyhow::Result;
use async_trait::async_trait;
use sqlx::query;

use domain::{audit_event::AuditEvent, outbox_event::OutboxEvent};

use crate::{
    audit_outbox_transaction_repository::AuditOutboxTransactionRepository,
    postgres::repository::PostgresRepository,
};

#[async_trait]
impl AuditOutboxTransactionRepository for PostgresRepository {
    async fn create_with_outbox(
        &self,
        event: AuditEvent,
        outbox_event: OutboxEvent,
    ) -> Result<AuditEvent> {
        let mut transaction = self.pool.begin().await?;

        query(
            r#"
            INSERT INTO audit_events
                (id, tenant_id, username, action, client_ip, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(event.id)
        .bind(&event.tenant_id)
        .bind(&event.user)
        .bind(&event.action)
        .bind(&event.client_ip)
        .bind(event.created_at)
        .execute(&mut *transaction)
        .await?;

        query(
            r#"
            INSERT INTO outbox_events
                (id, tenant_id, aggregate_type, aggregate_id, event_type,
                 payload, created_at, published)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(outbox_event.id)
        .bind(&outbox_event.tenant_id)
        .bind(&outbox_event.aggregate_type)
        .bind(outbox_event.aggregate_id)
        .bind(&outbox_event.event_type)
        .bind(&outbox_event.payload)
        .bind(outbox_event.created_at)
        .bind(outbox_event.published)
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;

        Ok(event)
    }
}

#[cfg(test)]
mod rollback_integration_test {
    use chrono::Utc;
    use sqlx::{postgres::PgPoolOptions, query, query_scalar, Error as SqlxError};
    use uuid::Uuid;

    use domain::{audit_event::AuditEvent, outbox_event::OutboxEvent};

    use crate::{
        audit_outbox_transaction_repository::AuditOutboxTransactionRepository,
        postgres::repository::PostgresRepository,
    };

    // Ejecutar explícitamente contra una base de datos de pruebas.
    #[tokio::test]
    #[ignore = "requiere TEST_DATABASE_URL apuntando a una base de datos de pruebas"]
    async fn outbox_failure_rolls_back_audit_insert() -> anyhow::Result<()> {
        let database_url = std::env::var("TEST_DATABASE_URL")
            .expect("Define TEST_DATABASE_URL con una base de datos de pruebas");

        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await?;

        let repository = PostgresRepository::new(pool.clone());

        let audit_id = Uuid::new_v4();
        let outbox_id = Uuid::new_v4();
        let tenant_id = format!("rollback-test-{}", Uuid::new_v4());
        let now = Utc::now();

        // Crear una fila auxiliar para provocar un conflicto de clave primaria.
        query(
            r#"
            INSERT INTO outbox_events
                (id, tenant_id, aggregate_type, aggregate_id, event_type,
                 payload, created_at, published)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(outbox_id)
        .bind(&tenant_id)
        .bind("rollback_test")
        .bind(audit_id)
        .bind("rollback_test_seed")
        .bind(serde_json::json!({}))
        .bind(now)
        .bind(false)
        .execute(&pool)
        .await?;

        let audit_event = AuditEvent {
            id: audit_id,
            tenant_id: tenant_id.clone(),
            user: "rollback-test-user".to_string(),
            action: "rollback-test-action".to_string(),
            client_ip: None,
            created_at: now,
        };

        let outbox_event = OutboxEvent {
            id: outbox_id, // Deliberadamente duplicado.
            tenant_id: tenant_id.clone(),
            aggregate_type: "audit_event".to_string(),
            aggregate_id: audit_id,
            event_type: "AUDIT_CREATED".to_string(),
            payload: serde_json::json!({}),
            created_at: now,
            published: false,
        };

        let test_result: anyhow::Result<()> = async {
            let result = repository
                .create_with_outbox(audit_event, outbox_event)
                .await;

            let is_unique_violation = result.as_ref().err().is_some_and(|error| {
                error.downcast_ref::<SqlxError>().is_some_and(|sqlx_error| {
                    matches!(
                        sqlx_error,
                        SqlxError::Database(db_error)
                            if db_error.code().as_deref() == Some("23505")
                    )
                })
            });

            anyhow::ensure!(
                is_unique_violation,
                "La inserción de Outbox debía fallar por clave primaria duplicada; resultado: {:?}",
                result
            );

            let audit_count: i64 = query_scalar(
                "SELECT COUNT(*) FROM audit_events WHERE id = $1 AND tenant_id = $2"
            )
            .bind(audit_id)
            .bind(&tenant_id)
            .fetch_one(&pool)
            .await?;

            let seed_count: i64 = query_scalar(
                "SELECT COUNT(*) FROM outbox_events WHERE id = $1 AND tenant_id = $2"
            )
            .bind(outbox_id)
            .bind(&tenant_id)
            .fetch_one(&pool)
            .await?;

            anyhow::ensure!(
                audit_count == 0,
                "La auditoría quedó persistida a pesar del fallo de Outbox: {audit_count}"
            );
            anyhow::ensure!(
                seed_count == 1,
                "La fila auxiliar de Outbox debía permanecer intacta antes de la limpieza: {seed_count}"
            );

            Ok(())
        }
        .await;

        // Limpiar exclusivamente las filas creadas por esta prueba,
        // incluso si una consulta o comprobación anterior falla.
        let cleanup_result: anyhow::Result<()> = async {
            query("DELETE FROM audit_events WHERE id = $1 AND tenant_id = $2")
                .bind(audit_id)
                .bind(&tenant_id)
                .execute(&pool)
                .await?;

            query("DELETE FROM outbox_events WHERE id = $1 AND tenant_id = $2")
                .bind(outbox_id)
                .bind(&tenant_id)
                .execute(&pool)
                .await?;

            Ok(())
        }
        .await;

        pool.close().await;

        test_result?;
        cleanup_result?;
        Ok(())
    }
}
