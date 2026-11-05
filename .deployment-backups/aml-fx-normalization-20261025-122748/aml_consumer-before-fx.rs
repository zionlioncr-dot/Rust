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
                    warn!(%tenant_id, %event_id, "Importe no positivo; evento rechazado");
                    return Ok(());
                }

                let mut alerts = Vec::new();

                if should_alert_high_value(transaction.amount, config.high_value_threshold) {
                    alerts.push(build_alert(
                        &tenant_id,
                        &event_id,
                        transaction.id,
                        "high_value_transaction",
                        "high",
                        "Transacción por encima del umbral de alto valor",
                        json!({
                            "amount": transaction.amount.to_string(),
                            "currency": transaction.currency,
                            "threshold": config.high_value_threshold.to_string()
                        }),
                    ));
                }

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

                // Evaluate the 24-hour window relative to the transaction's
                // occurrence time, not the consumer's processing time.
                let window_end = transaction.occurred_at;
                let window_start = window_end - TimeDelta::hours(24);

                let activity = repository
                    .account_activity(
                        &tenant_id,
                        &transaction.source_account_id,
                        window_start,
                        window_end,
                    )
                    .await?;

                if should_alert_structuring(
                    activity,
                    config.structuring_min_transactions,
                    config.structuring_min_total,
                ) {
                    alerts.push(build_alert(
                        &tenant_id,
                        &event_id,
                        transaction.id,
                        "structuring_activity",
                        "medium",
                        "Actividad que cumple los umbrales configurados de posible estructuración",
                        json!({
                            "source_account_id": transaction.source_account_id,
                            "window_hours": 24,
                            "window_start": window_start,
                            "window_end": window_end,
                            "transaction_count": activity.transaction_count,
                            "total_amount": activity.total_amount.to_string(),
                            "minimum_transactions": config.structuring_min_transactions,
                            "minimum_total": config.structuring_min_total.to_string()
                        }),
                    ));
                }

                // RAG enriches triggered alerts with verifiable regulatory
                // evidence. Retrieval errors must not suppress deterministic AML
                // alerts or break event idempotency.
                for alert in &mut alerts {
                    if let Err(error) = rag.enrich_alert(alert, transaction.occurred_at).await {
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

                // A single PostgreSQL transaction claims the event, writes
                // all its alerts, and commits the processed-event marker.
                let processed = repository
                    .process_event(&tenant_id, &event_id, &alerts)
                    .await?;

                if !processed {
                    info!(%tenant_id, %event_id, "Evento AML duplicado; se omite");
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
