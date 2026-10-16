use anyhow::Result;
use async_trait::async_trait;
use sqlx::{query, query_as, PgPool};
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
    async fn create(&self, alert: &FraudAlert) -> Result<()> {
        let triggered_rules = serde_json::to_value(&alert.triggered_rules)?;
        let evidence = serde_json::to_value(&alert.evidence)?;

        query(
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
            VALUES
            (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9,
                $10,
                $11
            )
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

        Ok(())
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
