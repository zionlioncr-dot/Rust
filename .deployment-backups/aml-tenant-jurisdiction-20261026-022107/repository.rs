use crate::domain::AmlAlert;
use anyhow::Result;
use sqlx::PgPool;

#[derive(Clone)]
pub struct PostgresAmlRepository {
    pool: PgPool,
}

impl PostgresAmlRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Registra el evento y sus alertas en una única transacción.
    /// Devuelve false si el evento ya había sido procesado.
    pub async fn process_event(
        &self,
        tenant_id: &str,
        event_id: &str,
        alerts: &[AmlAlert],
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;

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
            if alert.tenant_id != tenant_id || alert.event_id != event_id {
                anyhow::bail!("La alerta AML no coincide con el tenant_id/event_id del evento");
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

        tx.commit().await?;
        Ok(true)
    }
}
