use anyhow::Result;
use async_trait::async_trait;
use sqlx::{query, query_as, query_scalar, PgPool};
use uuid::Uuid;

use crate::domain::{
    fraud_alert::{FraudAlert, FraudAlertStatus},
    fraud_decision::FraudEvidence,
    risk_level::RiskLevel,
};

use super::fraud_alert_repository::FraudAlertRepository;

#[derive(Clone)]
pub struct PostgresFraudAlertRepository {
    pool: PgPool,
}

impl PostgresFraudAlertRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct FraudAlertRow {
    id: Uuid,
    tenant_id: String,
    event_id: Uuid,
    audit_id: Uuid,
    username: String,
    risk_level: String,
    triggered_rules: serde_json::Value,
    evidence: serde_json::Value,
    status: String,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl TryFrom<FraudAlertRow> for FraudAlert {
    type Error = anyhow::Error;

    fn try_from(row: FraudAlertRow) -> Result<Self> {
        let risk_level = match row.risk_level.as_str() {
            "Low" => RiskLevel::Low,
            "Medium" => RiskLevel::Medium,
            "High" => RiskLevel::High,
            "Critical" => RiskLevel::Critical,
            value => anyhow::bail!("invalid fraud alert risk level: {value}"),
        };

        let status = match row.status.as_str() {
            "OPEN" => FraudAlertStatus::Open,
            "REVIEWED" => FraudAlertStatus::Reviewed,
            "CONFIRMED" => FraudAlertStatus::Confirmed,
            "DISMISSED" => FraudAlertStatus::Dismissed,
            value => anyhow::bail!("invalid fraud alert status: {value}"),
        };

        let triggered_rules = serde_json::from_value::<Vec<String>>(row.triggered_rules)
            .map_err(|error| anyhow::anyhow!("invalid triggered_rules JSON: {error}"))?;

        let evidence = serde_json::from_value::<Vec<FraudEvidence>>(row.evidence)
            .map_err(|error| anyhow::anyhow!("invalid evidence JSON: {error}"))?;

        Ok(FraudAlert {
            id: row.id,
            tenant_id: row.tenant_id,
            event_id: row.event_id,
            audit_id: row.audit_id,
            username: row.username,
            risk_level,
            triggered_rules,
            evidence,
            status,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[async_trait]
impl FraudAlertRepository for PostgresFraudAlertRepository {
    async fn create(&self, alert: &FraudAlert) -> Result<bool> {
        let triggered_rules = serde_json::to_value(&alert.triggered_rules)?;
        let evidence = serde_json::to_value(&alert.evidence)?;

        let result = query(
            r#"
            INSERT INTO fraud_alerts
            (
                id,
                tenant_id,
                event_id,
                audit_id,
                username,
                risk_level,
                triggered_rules,
                evidence,
                status,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (tenant_id, event_id) DO NOTHING
            "#,
        )
        .bind(alert.id)
        .bind(&alert.tenant_id)
        .bind(alert.event_id)
        .bind(alert.audit_id)
        .bind(&alert.username)
        .bind(format!("{:?}", alert.risk_level))
        .bind(triggered_rules)
        .bind(evidence)
        .bind(alert.status.as_str())
        .bind(alert.created_at)
        .bind(alert.updated_at)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    async fn consolidate_failed_login_burst(
        &self,
        alert: &FraudAlert,
        occurred_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<Uuid>> {
        let mut tx = self.pool.begin().await?;

        // Serializa las actualizaciones de la misma combinación tenant/usuario.
        // Usa la misma clave de bloqueo que el estado de intentos fallidos.
        let lock_key = format!("{}\u{1f}{}", alert.tenant_id, alert.username);

        query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(&lock_key)
            .execute(&mut *tx)
            .await?;

        // Idempotencia: un evento ya asociado no puede volver a modificar la alerta.
        let already_associated = query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM fraud_alert_events
                WHERE tenant_id = $1 AND event_id = $2
            )
            "#,
        )
        .bind(&alert.tenant_id)
        .bind(alert.event_id)
        .fetch_one(&mut *tx)
        .await?;

        if already_associated {
            tx.rollback().await?;
            return Ok(None);
        }

        // Protege también contra eventos que ya tengan una alerta creada antes
        // de que existiera la tabla de asociaciones.
        let alert_already_exists = query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM fraud_alerts
                WHERE tenant_id = $1 AND event_id = $2
            )
            "#,
        )
        .bind(&alert.tenant_id)
        .bind(alert.event_id)
        .fetch_one(&mut *tx)
        .await?;

        if alert_already_exists {
            tx.rollback().await?;
            return Ok(None);
        }

        // Busca una alerta abierta del mismo tenant/usuario y regla,
        // con al menos un evento asociado a no más de cinco minutos del evento actual.
        let existing_alert_id = query_scalar::<_, Uuid>(
            r#"
            SELECT fa.id
            FROM fraud_alerts AS fa
            WHERE fa.tenant_id = $1
              AND fa.username = $2
              AND fa.status = 'OPEN'
              AND fa.triggered_rules @> '["failed_login_burst"]'::jsonb
              AND (
                  SELECT MAX(e.occurred_at)
                  FROM fraud_alert_events AS e
                  WHERE e.tenant_id = fa.tenant_id
                    AND e.alert_id = fa.id
              ) >= $3 - INTERVAL '5 minutes'
              AND (
                  SELECT MAX(e.occurred_at)
                  FROM fraud_alert_events AS e
                  WHERE e.tenant_id = fa.tenant_id
                    AND e.alert_id = fa.id
              ) <= $3 + INTERVAL '5 minutes'
            ORDER BY (
                SELECT MAX(e.occurred_at)
                FROM fraud_alert_events AS e
                WHERE e.tenant_id = fa.tenant_id
                  AND e.alert_id = fa.id
            ) DESC
            LIMIT 1
            FOR UPDATE
            "#,
        )
        .bind(&alert.tenant_id)
        .bind(&alert.username)
        .bind(occurred_at)
        .fetch_optional(&mut *tx)
        .await?;

        let evidence = serde_json::to_value(&alert.evidence)?;
        let risk_name = format!("{:?}", alert.risk_level);
        let risk_rank = match alert.risk_level {
            RiskLevel::Low => 1_i32,
            RiskLevel::Medium => 2_i32,
            RiskLevel::High => 3_i32,
            RiskLevel::Critical => 4_i32,
        };

        let alert_id = if let Some(existing_id) = existing_alert_id {
            // Conserva el nivel de riesgo más alto y añade evidencia del nuevo evento.
            query(
                r#"
                UPDATE fraud_alerts
                SET risk_level = CASE
                    WHEN (
                        CASE risk_level
                            WHEN 'Low' THEN 1
                            WHEN 'Medium' THEN 2
                            WHEN 'High' THEN 3
                            WHEN 'Critical' THEN 4
                            ELSE 0
                        END
                    ) < $2 THEN $3
                    ELSE risk_level
                END,
                evidence = COALESCE(evidence, '[]'::jsonb) || $4::jsonb,
                updated_at = NOW()
                WHERE id = $1 AND tenant_id = $5
                "#,
            )
            .bind(existing_id)
            .bind(risk_rank)
            .bind(&risk_name)
            .bind(&evidence)
            .bind(&alert.tenant_id)
            .execute(&mut *tx)
            .await?;

            existing_id
        } else {
            // No hay una ráfaga abierta dentro de la ventana: crea una alerta nueva.
            let inserted_id = query_scalar::<_, Uuid>(
                r#"
                INSERT INTO fraud_alerts (
                    id, tenant_id, event_id, audit_id, username,
                    risk_level, triggered_rules, evidence, status,
                    created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (tenant_id, event_id) DO NOTHING
                RETURNING id
                "#,
            )
            .bind(alert.id)
            .bind(&alert.tenant_id)
            .bind(alert.event_id)
            .bind(alert.audit_id)
            .bind(&alert.username)
            .bind(&risk_name)
            .bind(serde_json::to_value(&alert.triggered_rules)?)
            .bind(&evidence)
            .bind(alert.status.as_str())
            .bind(alert.created_at)
            .bind(alert.updated_at)
            .fetch_optional(&mut *tx)
            .await?;

            let Some(inserted_id) = inserted_id else {
                tx.rollback().await?;
                return Ok(None);
            };

            inserted_id
        };

        // Asocia el evento a la alerta. La clave tenant/event impide duplicados.
        query(
            r#"
            INSERT INTO fraud_alert_events (
                tenant_id, event_id, alert_id, audit_id, username, occurred_at
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(&alert.tenant_id)
        .bind(alert.event_id)
        .bind(alert_id)
        .bind(alert.audit_id)
        .bind(&alert.username)
        .bind(occurred_at)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(Some(alert_id))
    }

    async fn find_by_id(&self, tenant_id: &str, id: Uuid) -> Result<Option<FraudAlert>> {
        let row = query_as::<_, FraudAlertRow>(
            r#"
            SELECT
                id,
                tenant_id,
                event_id,
                audit_id,
                username,
                risk_level,
                triggered_rules,
                evidence,
                status,
                created_at,
                updated_at
            FROM fraud_alerts
            WHERE id = $1
              AND tenant_id = $2
            "#,
        )
        .bind(id)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(FraudAlert::try_from).transpose()
    }

    async fn find_by_event_id(
        &self,
        tenant_id: &str,
        event_id: Uuid,
    ) -> Result<Option<FraudAlert>> {
        let row = query_as::<_, FraudAlertRow>(
            r#"
            SELECT
                id,
                tenant_id,
                event_id,
                audit_id,
                username,
                risk_level,
                triggered_rules,
                evidence,
                status,
                created_at,
                updated_at
            FROM fraud_alerts
            WHERE event_id = $1
              AND tenant_id = $2
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(event_id)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(FraudAlert::try_from).transpose()
    }

    async fn find_all(&self, tenant_id: &str) -> Result<Vec<FraudAlert>> {
        let rows = query_as::<_, FraudAlertRow>(
            r#"
            SELECT
                id,
                tenant_id,
                event_id,
                audit_id,
                username,
                risk_level,
                triggered_rules,
                evidence,
                status,
                created_at,
                updated_at
            FROM fraud_alerts
            WHERE tenant_id = $1
            ORDER BY created_at DESC
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(FraudAlert::try_from).collect()
    }
}
