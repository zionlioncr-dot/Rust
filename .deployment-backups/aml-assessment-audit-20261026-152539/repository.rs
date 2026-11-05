use crate::domain::AmlAlert;
use anyhow::{bail, Context, Result};
use sqlx::PgPool;

#[derive(Clone)]
pub struct PostgresAmlRepository {
    pool: PgPool,
}

impl PostgresAmlRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Resuelve la jurisdicción del tenant desde su perfil activo y vigente.
    /// La fecha corresponde a la fecha de la transacción evaluada.
    pub async fn jurisdiction_for_tenant(
        &self,
        tenant_id: &str,
        as_of: chrono::NaiveDate,
    ) -> Result<String> {
        let jurisdiction = sqlx::query_scalar::<_, String>(
            r#"
            SELECT jurisdiction
            FROM aml_tenant_regulatory_profiles
            WHERE tenant_id = $1
              AND status = 'active'
              AND effective_from <= $2
              AND (effective_to IS NULL OR effective_to >= $2)
            ORDER BY effective_from DESC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(as_of)
        .fetch_optional(&self.pool)
        .await
        .context("No se pudo consultar el perfil regulatorio del tenant AML")?;

        match jurisdiction {
            Some(value) if !value.trim().is_empty() => {
                Ok(value.trim().to_ascii_uppercase())
            }
            Some(_) => bail!(
                "El perfil regulatorio contiene una jurisdicción vacía: tenant_id={tenant_id}"
            ),
            None => bail!(
                "No existe un perfil regulatorio activo y vigente: tenant_id={tenant_id}, fecha={as_of}"
            ),
        }
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
