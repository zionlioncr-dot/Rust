use crate::domain::AmlAssessment;
use anyhow::{Context, Result, bail};
use domain::financial_transaction::FinancialTransaction;
use redis::{AsyncCommands, aio::ConnectionManager};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use std::time::Duration;
use tokio::time::timeout;

const TENANT_CONFIG_CACHE_TTL_SECONDS: u64 = 300;
const REDIS_OPERATION_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedTenantConfig {
    tenant_id: String,
    time_zone: String,
    config_version: i64,
}

#[derive(Clone)]
pub struct PostgresAmlRepository {
    pool: PgPool,
    redis: Option<ConnectionManager>,
}

impl PostgresAmlRepository {
    /// Consulta la idempotencia antes de ejecutar el análisis RAG.
    pub async fn is_processed(&self, tenant_id: &str, event_id: &str) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1 FROM aml_processed_events
                WHERE tenant_id = $1 AND event_id = $2
            )",
        )
        .bind(tenant_id)
        .bind(event_id)
        .fetch_one(&self.pool)
        .await
        .context("No se pudo consultar la idempotencia del evento AML")
    }

    pub fn with_cache(pool: PgPool, redis: Option<ConnectionManager>) -> Self {
        Self { pool, redis }
    }

    fn tenant_config_cache_key(tenant_id: &str) -> String {
        format!("tenant-config:v1:{}:{tenant_id}", tenant_id.len())
    }

    async fn invalidate_tenant_config_cache(&self, tenant_id: &str) {
        let Some(redis) = &self.redis else {
            return;
        };

        let key = Self::tenant_config_cache_key(tenant_id);
        let mut connection = redis.clone();

        match timeout(REDIS_OPERATION_TIMEOUT, connection.del::<_, usize>(&key)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                tracing::warn!(
                    %error,
                    tenant_id = %tenant_id,
                    "No se pudo invalidar la caché de configuración del tenant"
                );
            }
            Err(_) => {
                tracing::warn!(
                    tenant_id = %tenant_id,
                    "Timeout invalidando la caché de configuración del tenant"
                );
            }
        }
    }

    /// Crea la configuración operacional inicial de un tenant.
    /// PostgreSQL es la fuente de verdad; Redis se invalida después del INSERT.
    pub async fn create_tenant_config(&self, tenant_id: &str, time_zone: &str) -> Result<i64> {
        if tenant_id.trim().is_empty() {
            bail!("tenant_id no puede estar vacío");
        }

        let time_zone = time_zone.trim();
        if time_zone.is_empty() {
            bail!("time_zone no puede estar vacío");
        }

        self.validate_time_zone(time_zone, tenant_id).await?;

        let config_version = sqlx::query_scalar::<_, i64>(
            "INSERT INTO tenant_configurations
                (tenant_id, time_zone, config_version)
             VALUES ($1, $2, 1)
             RETURNING config_version",
        )
        .bind(tenant_id)
        .bind(time_zone)
        .fetch_one(&self.pool)
        .await
        .context("No se pudo crear la configuración operacional del tenant")?;

        self.invalidate_tenant_config_cache(tenant_id).await;

        Ok(config_version)
    }

    /// Actualiza la zona horaria e incrementa la versión solo si cambia.
    pub async fn update_tenant_time_zone(&self, tenant_id: &str, time_zone: &str) -> Result<i64> {
        if tenant_id.trim().is_empty() {
            bail!("tenant_id no puede estar vacío");
        }

        let time_zone = time_zone.trim();
        if time_zone.is_empty() {
            bail!("time_zone no puede estar vacío");
        }

        self.validate_time_zone(time_zone, tenant_id).await?;

        let updated_version = sqlx::query_scalar::<_, i64>(
            "UPDATE tenant_configurations
             SET time_zone = $2,
                 config_version = config_version + 1,
                 updated_at = NOW()
             WHERE tenant_id = $1
               AND time_zone IS DISTINCT FROM $2
             RETURNING config_version",
        )
        .bind(tenant_id)
        .bind(time_zone)
        .fetch_optional(&self.pool)
        .await
        .context("No se pudo actualizar la configuración operacional del tenant")?;

        let config_version = match updated_version {
            Some(version) => version,
            None => sqlx::query_scalar::<_, i64>(
                "SELECT config_version
                     FROM tenant_configurations
                     WHERE tenant_id = $1",
            )
            .bind(tenant_id)
            .fetch_optional(&self.pool)
            .await
            .context("No se pudo consultar la versión de configuración del tenant")?
            .ok_or_else(|| {
                anyhow::anyhow!("No existe configuración operacional para tenant_id={tenant_id}")
            })?,
        };

        self.invalidate_tenant_config_cache(tenant_id).await;

        Ok(config_version)
    }

    async fn validate_time_zone(&self, time_zone: &str, tenant_id: &str) -> Result<()> {
        let valid = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_timezone_names WHERE name = $1)",
        )
        .bind(time_zone)
        .fetch_one(&self.pool)
        .await
        .context("No se pudo validar la zona horaria del tenant")?;

        if !valid {
            bail!("Zona horaria IANA inválida para tenant_id={tenant_id}: {time_zone}");
        }

        Ok(())
    }

    /// PostgreSQL es autoritativo. Redis es una caché opcional de 300 segundos.
    pub async fn time_zone_for_tenant(&self, tenant_id: &str) -> Result<String> {
        if tenant_id.trim().is_empty() {
            bail!("tenant_id no puede estar vacío");
        }

        let cache_key = Self::tenant_config_cache_key(tenant_id);

        if let Some(redis) = &self.redis {
            let mut connection = redis.clone();

            match timeout(
                REDIS_OPERATION_TIMEOUT,
                connection.get::<_, Option<String>>(&cache_key),
            )
            .await
            {
                Ok(Ok(Some(payload))) => {
                    match serde_json::from_str::<CachedTenantConfig>(&payload) {
                        Ok(cached)
                            if cached.tenant_id == tenant_id
                                && !cached.time_zone.trim().is_empty()
                                && cached.config_version > 0 =>
                        {
                            let current_version = sqlx::query_scalar::<_, i64>(
                                "SELECT config_version
                                 FROM tenant_configurations
                                 WHERE tenant_id = $1",
                            )
                            .bind(tenant_id)
                            .fetch_optional(&self.pool)
                            .await
                            .context(
                                "No se pudo verificar la versión actual de configuración del tenant",
                            )?;

                            if current_version == Some(cached.config_version)
                                && self
                                    .validate_time_zone(&cached.time_zone, tenant_id)
                                    .await
                                    .is_ok()
                            {
                                return Ok(cached.time_zone);
                            }

                            tracing::warn!(
                                tenant_id = %tenant_id,
                                cached_version = cached.config_version,
                                current_version = ?current_version,
                                "Caché obsoleta o inválida; se consultará PostgreSQL"
                            );
                        }
                        _ => {
                            tracing::warn!(
                                tenant_id = %tenant_id,
                                "Entrada Redis inválida para configuración del tenant; se consultará PostgreSQL"
                            );
                        }
                    }
                }
                Ok(Ok(None)) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, "Falló lectura de caché Redis; se consultará PostgreSQL");
                }
                Err(_) => {
                    tracing::warn!("Timeout leyendo caché Redis; se consultará PostgreSQL");
                }
            }
        }

        let config = sqlx::query_as::<_, (String, i64)>(
            "SELECT time_zone, config_version
             FROM tenant_configurations
             WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await
        .context("No se pudo consultar la configuración operacional del tenant")?
        .ok_or_else(|| {
            anyhow::anyhow!("No existe configuración operacional para tenant_id={tenant_id}")
        })?;

        let (time_zone, config_version) = config;

        if time_zone.trim().is_empty() || config_version <= 0 {
            bail!("Configuración operacional inválida para tenant_id={tenant_id}");
        }

        self.validate_time_zone(&time_zone, tenant_id).await?;

        if let Some(redis) = &self.redis {
            let cached = CachedTenantConfig {
                tenant_id: tenant_id.to_string(),
                time_zone: time_zone.clone(),
                config_version,
            };

            match serde_json::to_string(&cached) {
                Ok(payload) => {
                    let mut connection = redis.clone();

                    match timeout(
                        REDIS_OPERATION_TIMEOUT,
                        connection.set_ex::<_, _, ()>(
                            &cache_key,
                            payload,
                            TENANT_CONFIG_CACHE_TTL_SECONDS,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            tracing::warn!(%error, "No se pudo escribir configuración en caché Redis");
                        }
                        Err(_) => {
                            tracing::warn!("Timeout escribiendo configuración en caché Redis");
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "No se pudo serializar configuración para caché Redis");
                }
            }
        }

        Ok(time_zone)
    }

    /// Recupera operaciones históricas relacionadas por cuentas.
    /// El filtro de tenant se aplica en SQL y la operación actual se excluye.
    /// Este contexto informa al modelo; no determina por sí mismo una sospecha.
    pub async fn related_transactions_for_context(
        &self,
        tenant_id: &str,
        transaction: &FinancialTransaction,
        time_zone: &str,
    ) -> Result<Vec<Value>> {
        let rows = sqlx::query_scalar::<_, Value>(
            r#"
            WITH tenant_month AS (
                SELECT (
                    date_trunc(
                        'month',
                        $3::timestamptz AT TIME ZONE $6
                    ) AT TIME ZONE $6
                ) AS month_start
            ),
            accounts AS (
                SELECT 'source_account'::text AS account_role, $4 AS account_id
                UNION ALL
                SELECT 'destination_account'::text, $5
                WHERE $5 IS DISTINCT FROM $4
            )
            SELECT jsonb_build_object(
                'account_role', a.account_role,
                'account_id', a.account_id,
                'currency', ft.currency,
                'period_start', m.month_start,
                'period_end', $3::timestamptz,
                'movement_count', COUNT(ft.id),
                'incoming_count',
                    COUNT(ft.id) FILTER (
                        WHERE ft.destination_account_id = a.account_id
                    ),
                'outgoing_count',
                    COUNT(ft.id) FILTER (
                        WHERE ft.source_account_id = a.account_id
                    ),
                'incoming_amount',
                    COALESCE(
                        SUM(ft.amount) FILTER (
                            WHERE ft.destination_account_id = a.account_id
                        ), 0
                    ),
                'outgoing_amount',
                    COALESCE(
                        SUM(ft.amount) FILTER (
                            WHERE ft.source_account_id = a.account_id
                        ), 0
                    )
            )
            FROM accounts AS a
            CROSS JOIN tenant_month AS m
            LEFT JOIN financial_transactions AS ft
              ON ft.tenant_id = $1
             AND ft.id <> $2
             AND ft.occurred_at >= m.month_start
             AND ft.occurred_at <= $3
             AND (
                    ft.source_account_id = a.account_id
                    OR ft.destination_account_id = a.account_id
             )
            GROUP BY
                a.account_role,
                a.account_id,
                ft.currency,
                m.month_start
            ORDER BY a.account_role, ft.currency
            "#,
        )
        .bind(tenant_id)
        .bind(transaction.id)
        .bind(transaction.occurred_at)
        .bind(&transaction.source_account_id)
        .bind(&transaction.destination_account_id)
        .bind(time_zone)
        .fetch_all(&self.pool)
        .await
        .context("No se pudieron calcular los agregados mensuales AML")?;

        Ok(rows)
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
            Some(value) if !value.trim().is_empty() => Ok(value.trim().to_ascii_uppercase()),
            Some(_) => bail!(
                "El perfil regulatorio contiene una jurisdicción vacía: tenant_id={tenant_id}"
            ),
            None => bail!(
                "No existe un perfil regulatorio activo y vigente: tenant_id={tenant_id}, fecha={as_of}"
            ),
        }
    }

    /// Guarda el evento procesado, la evaluación completa y sus alertas
    /// en una única transacción. Devuelve false si el evento ya fue procesado.
    pub async fn process_event(
        &self,
        tenant_id: &str,
        event_id: &str,
        assessment: &AmlAssessment,
    ) -> Result<bool> {
        validate_assessment(tenant_id, event_id, assessment)?;

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

        sqlx::query(
            r#"
            INSERT INTO aml_assessments (
                assessment_id,
                tenant_id,
                event_id,
                transaction_id,
                model,
                jurisdiction,
                suspicious,
                severity,
                category,
                description,
                rationale,
                confidence,
                evidence_refs,
                regulatory_evidence,
                limitations,
                transaction_data,
                created_at,
                assessment_status
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15, $16, $17, $18
            )
            "#,
        )
        .bind(assessment.id)
        .bind(&assessment.tenant_id)
        .bind(&assessment.event_id)
        .bind(assessment.transaction_id)
        .bind(&assessment.model)
        .bind(&assessment.jurisdiction)
        .bind(assessment.suspicious)
        .bind(&assessment.severity)
        .bind(&assessment.category)
        .bind(&assessment.description)
        .bind(&assessment.rationale)
        .bind(assessment.confidence)
        .bind(&assessment.evidence_refs)
        .bind(&assessment.regulatory_evidence)
        .bind(&assessment.limitations)
        .bind(&assessment.transaction_data)
        .bind(assessment.created_at)
        .bind(&assessment.assessment_status)
        .execute(&mut *tx)
        .await
        .context("No se pudo persistir la evaluación AML")?;

        if let Some(alert) = assessment.alert.as_ref() {
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
            .await
            .context("No se pudo persistir la alerta AML")?;
        }

        tx.commit().await?;
        Ok(true)
    }
}

fn validate_assessment(tenant_id: &str, event_id: &str, assessment: &AmlAssessment) -> Result<()> {
    if assessment.tenant_id != tenant_id || assessment.event_id != event_id {
        bail!("La evaluación AML no coincide con el tenant_id/event_id del evento");
    }

    if assessment.assessment_status != "evaluated"
        && assessment.assessment_status != "manual_review"
    {
        bail!("Estado de evaluación AML no válido");
    }

    if assessment.assessment_status == "manual_review" {
        if assessment.suspicious
            || assessment.alert.is_some()
            || assessment.confidence.is_some()
            || assessment.evidence_refs != serde_json::json!([])
            || assessment.regulatory_evidence != serde_json::json!([])
        {
            bail!(
                "Una evaluación manual debe carecer de conclusión automática, alerta, confianza y evidencia regulatoria"
            );
        }
        return Ok(());
    }

    if assessment.suspicious != assessment.alert.is_some() {
        bail!(
            "Una evaluación sospechosa debe tener exactamente una alerta; una no sospechosa no debe tenerla"
        );
    }

    if let Some(alert) = assessment.alert.as_ref() {
        if alert.tenant_id != tenant_id
            || alert.event_id != event_id
            || alert.transaction_id != assessment.transaction_id
        {
            bail!("La alerta AML no coincide con la identidad de la evaluación");
        }

        if alert.severity != assessment.severity || alert.description != assessment.description {
            bail!("La severidad o descripción de la alerta no coincide con la evaluación");
        }

        if alert.rule_code.trim().is_empty() {
            bail!("La alerta AML debe incluir rule_code");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{CachedTenantConfig, PostgresAmlRepository, validate_assessment};
    use serde_json::json;

    fn valid_cached_config() -> CachedTenantConfig {
        CachedTenantConfig {
            tenant_id: "tenant-test".to_string(),
            time_zone: "America/Costa_Rica".to_string(),
            config_version: 1,
        }
    }

    #[test]
    fn tenant_cache_key_is_namespaced_and_versioned() {
        let key = PostgresAmlRepository::tenant_config_cache_key("tenant-test");

        assert_eq!(key, "tenant-config:v1:11:tenant-test");
    }

    #[test]
    fn tenant_cache_keys_are_isolated() {
        let first = PostgresAmlRepository::tenant_config_cache_key("tenant-a");
        let second = PostgresAmlRepository::tenant_config_cache_key("tenant-b");

        assert_ne!(first, second);
    }

    #[test]
    fn tenant_cache_key_length_prefix_prevents_ambiguous_boundaries() {
        let first = PostgresAmlRepository::tenant_config_cache_key("ab:c");
        let second = PostgresAmlRepository::tenant_config_cache_key("a:bc");

        assert_ne!(first, second);
    }

    #[test]
    fn cached_tenant_config_serializes_and_deserializes() {
        let original = valid_cached_config();

        let payload =
            serde_json::to_string(&original).expect("La configuración válida debe serializarse");

        let restored: CachedTenantConfig =
            serde_json::from_str(&payload).expect("La configuración serializada debe recuperarse");

        assert_eq!(restored.tenant_id, original.tenant_id);
        assert_eq!(restored.time_zone, original.time_zone);
        assert_eq!(restored.config_version, original.config_version);
    }

    #[test]
    fn cached_tenant_config_rejects_corrupt_json() {
        let result = serde_json::from_str::<CachedTenantConfig>("{ payload corrupto");

        assert!(result.is_err());
    }

    #[test]
    fn cached_tenant_config_rejects_missing_fields() {
        let result = serde_json::from_value::<CachedTenantConfig>(json!({
            "tenant_id": "tenant-test",
            "time_zone": "America/Costa_Rica"
        }));

        assert!(result.is_err());
    }

    #[test]
    fn cached_tenant_config_rejects_wrong_field_types() {
        let result = serde_json::from_value::<CachedTenantConfig>(json!({
            "tenant_id": "tenant-test",
            "time_zone": "America/Costa_Rica",
            "config_version": "1"
        }));

        assert!(result.is_err());
    }

    use crate::domain::{AmlAlert, AmlAssessment};
    use chrono::Utc;
    use uuid::Uuid;

    fn assessment(suspicious: bool) -> AmlAssessment {
        let tenant_id = "tenant-test".to_string();
        let event_id = Uuid::new_v4().to_string();
        let transaction_id = Uuid::new_v4();
        let created_at = Utc::now();

        let alert = suspicious.then(|| AmlAlert {
            id: Uuid::new_v4(),
            tenant_id: tenant_id.clone(),
            event_id: event_id.clone(),
            transaction_id,
            rule_code: "llm_regulatory_assessment".to_string(),
            severity: "high".to_string(),
            description: "Revisión contextual".to_string(),
            details: json!({}),
            created_at,
        });

        AmlAssessment {
            id: Uuid::new_v4(),
            tenant_id,
            event_id,
            transaction_id,
            model: "test-model".to_string(),
            jurisdiction: "CR".to_string(),
            suspicious,
            severity: "high".to_string(),
            category: "contextual_review".to_string(),
            description: "Revisión contextual".to_string(),
            rationale: "Justificación de prueba".to_string(),
            confidence: Some(0.8),
            evidence_refs: json!([]),
            regulatory_evidence: json!([]),
            limitations: json!([]),
            transaction_data: json!({}),
            created_at,
            assessment_status: "evaluated".to_string(),
            alert,
        }
    }

    #[test]
    fn accepts_manual_review_without_evidence_or_alert() {
        let mut item = assessment(false);
        item.assessment_status = "manual_review".to_string();
        item.model = "not_invoked_no_evidence".to_string();
        item.confidence = None;
        item.evidence_refs = json!([]);
        item.regulatory_evidence = json!([]);
        item.alert = None;

        assert!(validate_assessment(&item.tenant_id, &item.event_id, &item).is_ok());
    }

    #[test]
    fn rejects_manual_review_with_confidence() {
        let mut item = assessment(false);
        item.assessment_status = "manual_review".to_string();
        item.confidence = Some(0.5);

        assert!(validate_assessment(&item.tenant_id, &item.event_id, &item).is_err());
    }

    #[test]
    fn rejects_manual_review_with_evidence() {
        let mut item = assessment(false);
        item.assessment_status = "manual_review".to_string();
        item.confidence = None;
        item.evidence_refs = json!(["unexpected-document"]);

        assert!(validate_assessment(&item.tenant_id, &item.event_id, &item).is_err());
    }

    #[test]
    fn rejects_unknown_assessment_status() {
        let mut item = assessment(false);
        item.assessment_status = "unknown".to_string();

        assert!(validate_assessment(&item.tenant_id, &item.event_id, &item).is_err());
    }

    #[test]
    fn accepts_non_suspicious_assessment_without_alert() {
        let item = assessment(false);
        assert!(validate_assessment(&item.tenant_id, &item.event_id, &item).is_ok());
    }

    #[test]
    fn accepts_suspicious_assessment_with_matching_alert() {
        let item = assessment(true);
        assert!(validate_assessment(&item.tenant_id, &item.event_id, &item).is_ok());
    }

    #[test]
    fn rejects_suspicious_assessment_without_alert() {
        let mut item = assessment(true);
        item.alert = None;
        assert!(validate_assessment(&item.tenant_id, &item.event_id, &item).is_err());
    }

    #[test]
    fn rejects_non_suspicious_assessment_with_alert() {
        let mut item = assessment(false);
        item.alert = Some(AmlAlert {
            id: Uuid::new_v4(),
            tenant_id: item.tenant_id.clone(),
            event_id: item.event_id.clone(),
            transaction_id: item.transaction_id,
            rule_code: "llm_regulatory_assessment".to_string(),
            severity: item.severity.clone(),
            description: item.description.clone(),
            details: json!({}),
            created_at: Utc::now(),
        });
        assert!(validate_assessment(&item.tenant_id, &item.event_id, &item).is_err());
    }

    #[test]
    fn rejects_alert_with_different_transaction_id() {
        let mut item = assessment(true);
        item.alert.as_mut().unwrap().transaction_id = Uuid::new_v4();
        assert!(validate_assessment(&item.tenant_id, &item.event_id, &item).is_err());
    }

    #[test]
    fn rejects_alert_with_different_severity() {
        let mut item = assessment(true);
        item.alert.as_mut().unwrap().severity = "critical".to_string();
        assert!(validate_assessment(&item.tenant_id, &item.event_id, &item).is_err());
    }

    #[tokio::test]
    async fn tenant_config_postgres_redis_integration() -> anyhow::Result<()> {
        if std::env::var("AML_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
            return Ok(());
        }

        dotenvy::dotenv().ok();

        use redis::AsyncCommands;
        use serde_json::from_str;
        use sqlx::postgres::PgPoolOptions;
        use uuid::Uuid;

        let database_url = std::env::var("DATABASE_URL")?;
        let options = database_url
            .parse::<sqlx::postgres::PgConnectOptions>()?
            .port(15432);

        let pool = PgPoolOptions::new()
            .max_connections(3)
            .connect_with(options)
            .await?;

        let redis_client = redis::Client::open("redis://127.0.0.1:16379/")?;
        let redis_manager = redis::aio::ConnectionManager::new(redis_client).await?;

        let tenant_a = format!("aml-it-{}", Uuid::new_v4());
        let tenant_b = format!("aml-it-{}", Uuid::new_v4());
        let missing_tenant = format!("aml-it-{}", Uuid::new_v4());

        let key_a = PostgresAmlRepository::tenant_config_cache_key(&tenant_a);
        let key_b = PostgresAmlRepository::tenant_config_cache_key(&tenant_b);
        let key_missing = PostgresAmlRepository::tenant_config_cache_key(&missing_tenant);

        let repository =
            PostgresAmlRepository::with_cache(pool.clone(), Some(redis_manager.clone()));

        let test_result: anyhow::Result<()> = async {
            let created_a = repository
                .create_tenant_config(&tenant_a, "America/Costa_Rica")
                .await?;
            let created_b = repository
                .create_tenant_config(&tenant_b, "Europe/Madrid")
                .await?;

            anyhow::ensure!(created_a == 1, "La versión inicial del tenant A debe ser 1");
            anyhow::ensure!(created_b == 1, "La versión inicial del tenant B debe ser 1");

            // Primera consulta: PostgreSQL y creación de caché.
            let timezone_a = repository.time_zone_for_tenant(&tenant_a).await?;
            anyhow::ensure!(
                timezone_a == "America/Costa_Rica",
                "Zona horaria incorrecta para tenant A: {timezone_a}"
            );

            let mut redis_connection = redis_manager.clone();

            let payload: Option<String> = redis_connection.get(&key_a).await?;
            let payload = payload
                .ok_or_else(|| anyhow::anyhow!("No se creó la entrada Redis del tenant A"))?;

            let cached: CachedTenantConfig = from_str(&payload)?;
            anyhow::ensure!(cached.tenant_id == tenant_a, "Tenant incorrecto en caché");
            anyhow::ensure!(
                cached.time_zone == "America/Costa_Rica",
                "Zona horaria incorrecta en caché"
            );
            anyhow::ensure!(cached.config_version == 1, "Versión incorrecta en caché");

            let ttl: i64 = redis_connection.ttl(&key_a).await?;
            anyhow::ensure!(
                (0..=300).contains(&ttl),
                "TTL fuera del intervalo esperado: {ttl}"
            );

            // Entrada corrupta: debe recuperarse desde PostgreSQL y repararse.
            redis_connection
                .set::<_, _, ()>(&key_a, "{not-json")
                .await?;

            let recovered_timezone = repository.time_zone_for_tenant(&tenant_a).await?;
            anyhow::ensure!(
                recovered_timezone == "America/Costa_Rica",
                "No se recuperó la configuración desde PostgreSQL"
            );

            let repaired_payload: Option<String> = redis_connection.get(&key_a).await?;
            let repaired_payload = repaired_payload
                .ok_or_else(|| anyhow::anyhow!("La entrada corrupta no fue reparada"))?;

            let repaired: CachedTenantConfig = from_str(&repaired_payload)?;
            anyhow::ensure!(
                repaired.tenant_id == tenant_a
                    && repaired.time_zone == "America/Costa_Rica"
                    && repaired.config_version == 1,
                "La entrada Redis reparada no coincide con PostgreSQL"
            );

            // Aislamiento: el segundo tenant conserva su propia configuración.
            let timezone_b = repository.time_zone_for_tenant(&tenant_b).await?;
            anyhow::ensure!(
                timezone_b == "Europe/Madrid",
                "Configuración cruzada entre tenants: {timezone_b}"
            );

            let payload_b: Option<String> = redis_connection.get(&key_b).await?;
            let payload_b = payload_b
                .ok_or_else(|| anyhow::anyhow!("No se creó la entrada Redis del tenant B"))?;

            let cached_b: CachedTenantConfig = from_str(&payload_b)?;
            anyhow::ensure!(
                cached_b.tenant_id == tenant_b && cached_b.time_zone == "Europe/Madrid",
                "La caché del tenant B contiene datos incorrectos"
            );

            // Actualización: incrementa versión e invalida la caché anterior.
            let updated_version = repository
                .update_tenant_time_zone(&tenant_a, "Europe/London")
                .await?;

            anyhow::ensure!(
                updated_version == 2,
                "La actualización debía incrementar config_version a 2"
            );

            let updated_timezone = repository.time_zone_for_tenant(&tenant_a).await?;
            anyhow::ensure!(
                updated_timezone == "Europe/London",
                "No se recuperó la zona horaria actualizada"
            );

            let updated_payload: Option<String> = redis_connection.get(&key_a).await?;
            let updated_payload = updated_payload
                .ok_or_else(|| anyhow::anyhow!("No se reconstruyó la caché actualizada"))?;

            let updated_cache: CachedTenantConfig = from_str(&updated_payload)?;
            anyhow::ensure!(
                updated_cache.time_zone == "Europe/London" && updated_cache.config_version == 2,
                "Redis contiene una configuración o versión incorrecta después de actualizar"
            );

            // Repetir el mismo valor no debe incrementar la versión.
            let unchanged_version = repository
                .update_tenant_time_zone(&tenant_a, "Europe/London")
                .await?;

            anyhow::ensure!(
                unchanged_version == 2,
                "La versión cambió aunque la zona horaria permaneció igual"
            );

            // La actualización de A no debe modificar la configuración de B.
            let timezone_b_after_update = repository.time_zone_for_tenant(&tenant_b).await?;

            anyhow::ensure!(
                timezone_b_after_update == "Europe/Madrid",
                "La actualización del tenant A afectó al tenant B"
            );

            // Tenant inexistente: debe producir un error.
            anyhow::ensure!(
                repository
                    .time_zone_for_tenant(&missing_tenant)
                    .await
                    .is_err(),
                "Se esperaba un error para un tenant inexistente"
            );

            Ok(())
        }
        .await;

        // Limpieza acotada: solo las tres claves y los dos tenants de esta prueba.
        let redis_cleanup_result: redis::RedisResult<()> = async {
            let mut connection = redis_manager.clone();
            let _: usize = connection.del(&key_a).await?;
            let _: usize = connection.del(&key_b).await?;
            let _: usize = connection.del(&key_missing).await?;
            Ok(())
        }
        .await;

        let db_cleanup_result =
            sqlx::query("DELETE FROM tenant_configurations WHERE tenant_id = $1 OR tenant_id = $2")
                .bind(&tenant_a)
                .bind(&tenant_b)
                .execute(&pool)
                .await;

        let cleanup_result = match (redis_cleanup_result, db_cleanup_result) {
            (Ok(()), Ok(_)) => Ok(()),
            (redis_result, db_result) => {
                let redis_error = redis_result.err().map(|e| e.to_string());
                let db_error = db_result.err().map(|e| e.to_string());
                Err(anyhow::anyhow!(
                    "Falló la limpieza. Redis: {:?}; PostgreSQL: {:?}",
                    redis_error,
                    db_error
                ))
            }
        };

        match (test_result, cleanup_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(test_error), Ok(())) => Err(test_error),
            (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
            (Err(test_error), Err(cleanup_error)) => {
                Err(test_error.context(format!("Además, falló la limpieza: {cleanup_error}")))
            }
        }
    }
}
