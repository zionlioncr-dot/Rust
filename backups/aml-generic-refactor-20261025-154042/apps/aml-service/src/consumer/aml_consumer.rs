use crate::{
    config::AmlConfig,
    domain::{AccountActivity, AmlAlert},
    rag::RegulatoryRag,
    repository::AmlRepository,
};
use anyhow::{Context, Result};
use chrono::{TimeDelta, Utc};
use domain::events::{
    event_envelope::EventEnvelope, event_types::FINANCIAL_TRANSACTION_CREATED,
    financial_transaction_created::FinancialTransactionCreatedEvent,
};
use kafka::consumer::KafkaConsumer;
use rust_decimal::Decimal;
use serde_json::json;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

pub async fn run_consumer(
    config: AmlConfig,
    repository: Arc<dyn AmlRepository>,
    rag: Arc<RegulatoryRag>,
) -> Result<()> {
    let consumer = Arc::new(KafkaConsumer::new(
        &config.app.kafka_brokers,
        &config.consumer_group,
        &config.app.schema_registry_url,
    )?);

    consumer.subscribe(&[config.kafka_topic.as_str()]).await?;

    info!(
        topic = %config.kafka_topic,
        group = %config.consumer_group,
        "AML consumer iniciado"
    );

    let rule_config = config.clone();
    let consumer_for_handler = Arc::clone(&consumer);

    consumer
        .listen(move |event| {
            let consumer = Arc::clone(&consumer_for_handler);
            let repository = Arc::clone(&repository);
            let config = rule_config.clone();
            let rag = Arc::clone(&rag);

            async move {
                let decoded = consumer.decode_avro(&event.payload).await?;
                let envelope: EventEnvelope =
                    serde_json::from_value(decoded).context("EventEnvelope inválido")?;

                if envelope.event_type != FINANCIAL_TRANSACTION_CREATED {
                    return Ok(());
                }

                let tenant_id = envelope.metadata.tenant_id.trim().to_string();
                let event_id = envelope.metadata.event_id.to_string();

                if tenant_id.is_empty() {
                    warn!(%event_id, "Evento rechazado: tenant_id vacío");
                    return Ok(());
                }

                let payload: FinancialTransactionCreatedEvent =
                    serde_json::from_value(envelope.payload.clone())
                        .context("FinancialTransactionCreated inválido")?;

                let transaction = payload.transaction;

                if transaction.tenant_id.trim() != tenant_id {
                    warn!(
                        %tenant_id,
                        %event_id,
                        "Evento rechazado por discrepancia de tenant"
                    );
                    return Ok(());
                }

                if transaction.amount <= Decimal::ZERO {
                    warn!(
                        %tenant_id,
                        %event_id,
                        "Importe no positivo; evento rechazado"
                    );
                    return Ok(());
                }

                let mut alerts = Vec::new();

                // Los países de alto riesgo siguen siendo una configuración
                // independiente de los umbrales monetarios regulatorios.
                let country = transaction.country_code.trim().to_ascii_uppercase();

                if is_high_risk_country(&country, &config.high_risk_countries) {
                    alerts.push(build_alert(
                        &tenant_id,
                        &event_id,
                        transaction.id,
                        "high_risk_country",
                        "high",
                        "Transacción asociada a un país configurado de alto riesgo",
                        json!({
                            "country_code": country,
                            "amount": transaction.amount.to_string(),
                            "currency": transaction.currency
                        }),
                    ));
                }

                // Consultar exclusivamente reglas aprobadas y vigentes para
                // este tenant en la fecha de ocurrencia de la transacción.
                let rules = repository
                    .active_regulatory_rules(
                        &tenant_id,
                        transaction.occurred_at.date_naive(),
                    )
                    .await?;

                if rules.is_empty() {
                    warn!(
                        %tenant_id,
                        %event_id,
                        "No hay reglas AML aprobadas y vigentes; se omiten los umbrales monetarios"
                    );
                }

                let transaction_base_currency =
                    normalize_base_currency(transaction.base_currency.as_deref());

                let converted_amount =
                    convert_amount_to_base(transaction.amount, transaction.exchange_rate);

                for rule in &rules {
                    let profile_currency =
                        normalize_base_currency(Some(rule.base_currency.as_str()));

                    let threshold_currency =
                        normalize_base_currency(Some(rule.threshold_currency.as_str()));

                    // No comparar importes expresados en monedas diferentes.
                    if profile_currency.is_none()
                        || transaction_base_currency != profile_currency
                        || threshold_currency != profile_currency
                    {
                        warn!(
                            %tenant_id,
                            %event_id,
                            rule_code = %rule.rule_code,
                            profile_currency = %rule.base_currency,
                            transaction_currency = ?transaction_base_currency,
                            threshold_currency = %rule.threshold_currency,
                            "Regla AML omitida: moneda incompatible con el perfil normativo"
                        );
                        continue;
                    }

                    let base_currency = profile_currency.unwrap_or_default();

                    let amount_in_base = match converted_amount {
                        Some(amount) => amount,
                        None => {
                            warn!(
                                %tenant_id,
                                %event_id,
                                rule_code = %rule.rule_code,
                                "Regla AML omitida: conversión FX ausente o inválida"
                            );
                            continue;
                        }
                    };

                    let legal_basis = json!({
                        "jurisdiction": rule.jurisdiction,
                        "regulation_code": rule.regulation_code,
                        "article": rule.article,
                        "document_version": rule.document_version,
                        "source_url": rule.source_url,
                        "source_excerpt": rule.source_excerpt,
                        "effective_date": transaction
                            .occurred_at
                            .date_naive()
                            .to_string()
                    });

                    match rule.rule_code.as_str() {
                        "high_value_transaction" => {
                            if should_alert_high_value(
                                amount_in_base,
                                rule.threshold_amount,
                            ) {
                                alerts.push(build_alert(
                                    &tenant_id,
                                    &event_id,
                                    transaction.id,
                                    "high_value_transaction",
                                    "high",
                                    "Transacción por encima del umbral normativo de alto valor",
                                    json!({
                                        "amount": transaction.amount.to_string(),
                                        "currency": transaction.currency,
                                        "exchange_rate": transaction
                                            .exchange_rate
                                            .map(|rate| rate.to_string()),
                                        "amount_in_base_currency": amount_in_base.to_string(),
                                        "base_currency": base_currency,
                                        "threshold": rule.threshold_amount.to_string(),
                                        "threshold_currency": rule.threshold_currency,
                                        "legal_basis": legal_basis
                                    }),
                                ));
                            }
                        }

                        "structuring_activity" => {
                            let minimum_transactions = match rule.minimum_transactions {
                                Some(value) if value >= 2 => i64::from(value),
                                _ => {
                                    warn!(
                                        %tenant_id,
                                        %event_id,
                                        "Regla de estructuración omitida: minimum_transactions inválido"
                                    );
                                    continue;
                                }
                            };

                            // La ventana se calcula respecto a la fecha del
                            // evento, no respecto al momento de procesamiento.
                            let window_end = transaction.occurred_at;
                            let window_start = window_end - TimeDelta::hours(24);

                            let activity = repository
                                .account_activity(
                                    &tenant_id,
                                    &transaction.source_account_id,
                                    &base_currency,
                                    window_start,
                                    window_end,
                                )
                                .await?;

                            let transaction_count = activity.transaction_count;
                            let total_amount = activity.total_amount;

                            let matches_rule = should_alert_structuring(
                                activity,
                                minimum_transactions,
                                rule.threshold_amount,
                            );

                            if matches_rule {
                                alerts.push(build_alert(
                                    &tenant_id,
                                    &event_id,
                                    transaction.id,
                                    "structuring_activity",
                                    "medium",
                                    "Actividad potencial de estructuración según regla normativa",
                                    json!({
                                        "source_account_id": transaction.source_account_id,
                                        "window_hours": 24,
                                        "window_start": window_start,
                                        "window_end": window_end,
                                        "transaction_count": transaction_count,
                                        "total_amount": total_amount.to_string(),
                                        "base_currency": base_currency,
                                        "minimum_transactions": minimum_transactions,
                                        "minimum_total": rule.threshold_amount.to_string(),
                                        "threshold_currency": rule.threshold_currency,
                                        "legal_basis": legal_basis
                                    }),
                                ));
                            }
                        }

                        _ => {
                            warn!(
                                %tenant_id,
                                %event_id,
                                rule_code = %rule.rule_code,
                                "Regla AML desconocida; se omite"
                            );
                        }
                    }
                }

                // RAG aporta evidencia regulatoria a las alertas disparadas.
                // Un fallo de recuperación no elimina la alerta determinista.
                for alert in &mut alerts {
                    if let Err(error) =
                        rag.enrich_alert(alert, transaction.occurred_at).await
                    {
                        warn!(
                            %tenant_id,
                            %event_id,
                            rule_code = %alert.rule_code,
                            %error,
                            "No se pudo recuperar evidencia regulatoria; se conserva la alerta AML"
                        );

                        rag.mark_unavailable(alert);
                    }
                }

                // La persistencia de alertas y la marca de idempotencia
                // se confirman en una única transacción PostgreSQL.
                let processed = repository
                    .process_event(&tenant_id, &event_id, &alerts)
                    .await?;

                if !processed {
                    info!(
                        %tenant_id,
                        %event_id,
                        "Evento AML duplicado; se omite"
                    );
                    return Ok(());
                }

                info!(
                    %tenant_id,
                    %event_id,
                    transaction_id = %transaction.id,
                    alerts = alerts.len(),
                    "Evento AML procesado"
                );

                Ok(())
            }
        })
        .await?;

    Ok(())
}

fn build_alert(
    tenant_id: &str,
    event_id: &str,
    transaction_id: Uuid,
    rule_code: &str,
    severity: &str,
    description: &str,
    details: serde_json::Value,
) -> AmlAlert {
    AmlAlert {
        id: Uuid::new_v4(),
        tenant_id: tenant_id.to_string(),
        event_id: event_id.to_string(),
        transaction_id,
        rule_code: rule_code.to_string(),
        severity: severity.to_string(),
        description: description.to_string(),
        details,
        created_at: Utc::now(),
    }
}

fn normalize_base_currency(value: Option<&str>) -> Option<String> {
    let currency = value?.trim().to_ascii_uppercase();

    if currency.len() == 3 && currency.bytes().all(|byte| byte.is_ascii_uppercase()) {
        Some(currency)
    } else {
        None
    }
}

fn convert_amount_to_base(amount: Decimal, exchange_rate: Option<Decimal>) -> Option<Decimal> {
    let rate = exchange_rate.filter(|rate| *rate > Decimal::ZERO)?;
    amount.checked_mul(rate)
}

fn should_alert_high_value(amount: Decimal, threshold: Decimal) -> bool {
    amount >= threshold
}

fn is_high_risk_country(country_code: &str, high_risk_countries: &[String]) -> bool {
    let country = country_code.trim();

    !country.is_empty()
        && high_risk_countries
            .iter()
            .any(|configured| configured.trim().eq_ignore_ascii_case(country))
}

fn should_alert_structuring(
    activity: AccountActivity,
    minimum_transactions: i64,
    minimum_total: Decimal,
) -> bool {
    activity.transaction_count >= minimum_transactions && activity.total_amount >= minimum_total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fx_conversion_uses_rate_supplied_by_bank() {
        let amount = Decimal::new(800, 0);
        let rate = Decimal::new(125, 2);

        assert_eq!(
            convert_amount_to_base(amount, Some(rate)),
            Some(Decimal::new(1000, 0))
        );
    }

    #[test]
    fn fx_conversion_rejects_missing_nonpositive_and_overflowing_rates() {
        let amount = Decimal::new(800, 0);

        assert_eq!(convert_amount_to_base(amount, None), None);
        assert_eq!(convert_amount_to_base(amount, Some(Decimal::ZERO)), None);
        assert_eq!(
            convert_amount_to_base(amount, Some(Decimal::new(-1, 0))),
            None
        );
        assert_eq!(
            convert_amount_to_base(Decimal::MAX, Some(Decimal::new(2, 0))),
            None
        );
    }

    #[test]
    fn base_currency_is_trimmed_uppercased_and_validated() {
        assert_eq!(
            normalize_base_currency(Some(" crc ")),
            Some("CRC".to_string())
        );
        assert_eq!(normalize_base_currency(Some("US")), None);
        assert_eq!(normalize_base_currency(Some("U$D")), None);
        assert_eq!(normalize_base_currency(Some("   ")), None);
        assert_eq!(normalize_base_currency(None), None);
    }

    #[test]
    fn high_value_alert_triggers_at_threshold() {
        let threshold = Decimal::new(10_000, 0);

        assert!(should_alert_high_value(threshold, threshold));
        assert!(should_alert_high_value(Decimal::new(10_001, 0), threshold));
        assert!(!should_alert_high_value(Decimal::new(9_999, 0), threshold));
    }

    #[test]
    fn high_risk_country_is_case_insensitive_and_trims_whitespace() {
        let countries = vec!["CR".to_string(), "XX".to_string()];

        assert!(is_high_risk_country("CR", &countries));
        assert!(is_high_risk_country(" xx ", &countries));
        assert!(!is_high_risk_country("US", &countries));
        assert!(!is_high_risk_country("  ", &countries));
    }

    #[test]
    fn structuring_requires_both_transaction_count_and_total() {
        let minimum_total = Decimal::new(10_000, 0);

        assert!(should_alert_structuring(
            AccountActivity {
                transaction_count: 5,
                total_amount: minimum_total,
            },
            5,
            minimum_total,
        ));

        assert!(!should_alert_structuring(
            AccountActivity {
                transaction_count: 4,
                total_amount: Decimal::new(12_000, 0),
            },
            5,
            minimum_total,
        ));

        assert!(!should_alert_structuring(
            AccountActivity {
                transaction_count: 5,
                total_amount: Decimal::new(9_999, 0),
            },
            5,
            minimum_total,
        ));
    }
}
