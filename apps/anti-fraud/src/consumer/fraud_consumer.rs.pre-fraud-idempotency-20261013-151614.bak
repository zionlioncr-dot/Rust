use std::sync::Arc;

use anyhow::{Context, Result};

use domain::events::{
    audit_created::AuditCreatedEvent, event_envelope::EventEnvelope, event_types::AUDIT_CREATED,
};

use kafka::KafkaConsumer;

use crate::{
    config::FraudConfig,
    domain::{
        failed_login_burst_rule::FailedLoginBurstRule, fraud_engine::FraudEngine,
        fraud_event::FraudEvent, sensitive_action_rule::SensitiveActionRule,
    },
    repository::fraud_alert_repository::FraudAlertRepository,
    state::fraud_state_store::FraudStateStore,
};

pub struct FraudConsumer {
    consumer: KafkaConsumer,
    config: FraudConfig,
    engine: FraudEngine,
    alert_repository: Arc<dyn FraudAlertRepository>,
}

impl FraudConsumer {
    pub async fn new(
        config: FraudConfig,
        alert_repository: Arc<dyn FraudAlertRepository>,
        state_store: Arc<dyn FraudStateStore>,
    ) -> Result<Self> {
        tracing::info!("ANTI_FRAUD_INIT: loading configuration");

        tracing::info!(
            kafka_brokers = %config.app.kafka_brokers,
            kafka_topic = %config.app.kafka_topic,
            consumer_group = %config.consumer_group,
            schema_registry_url = %config.app.schema_registry_url,
            "ANTI_FRAUD_INIT: configuration loaded"
        );

        let consumer = KafkaConsumer::new(
            &config.app.kafka_brokers,
            &config.consumer_group,
            &config.app.schema_registry_url,
        )?;

        let engine = FraudEngine::new(vec![
            Box::new(SensitiveActionRule),
            Box::new(FailedLoginBurstRule::new(state_store)),
        ]);

        tracing::info!(
            consumer_group = %config.consumer_group,
            "ANTI_FRAUD_INIT: Kafka consumer created"
        );

        tracing::info!(rules = 2, "ANTI_FRAUD_INIT: fraud engine initialized");

        Ok(Self {
            consumer,
            config,
            engine,
            alert_repository,
        })
    }

    pub async fn run(&self) -> Result<()> {
        self.consumer
            .subscribe(&[self.config.app.kafka_topic.as_str()])
            .await?;

        tracing::info!(
            kafka_topic = %self.config.app.kafka_topic,
            consumer_group = %self.config.consumer_group,
            "ANTI_FRAUD_RUN: Kafka subscription established"
        );

        let consumer = &self.consumer;
        let engine = &self.engine;
        let alert_repository = self.alert_repository.clone();

        consumer
            .listen(move |event| {
                let alert_repository = alert_repository.clone();

                async move {
                    tracing::info!(
                        traceparent = ?event.traceparent,
                        "ANTI_FRAUD_EVENT: Kafka event received"
                    );

                    let json_value = consumer
                        .decode_avro(&event.payload)
                        .await
                        .context("failed to decode Avro Kafka payload")?;

                    tracing::debug!(
                        payload = %json_value,
                        "ANTI_FRAUD_EVENT: Avro payload decoded"
                    );

                    let envelope = serde_json::from_value::<EventEnvelope>(json_value)
                        .context("failed to deserialize EventEnvelope")?;

                    tracing::info!(
                        event_type = %envelope.event_type,
                        event_id = %envelope.metadata.event_id,
                        tenant_id = %envelope.metadata.tenant_id,
                        trace_id = %envelope.metadata.trace_id,
                        "ANTI_FRAUD_EVENT: EventEnvelope deserialized"
                    );

                    if envelope.event_type != AUDIT_CREATED {
                        tracing::debug!(
                            event_type = %envelope.event_type,
                            "ANTI_FRAUD_EVENT: event ignored"
                        );

                        return Ok(());
                    }

                    let audit_event = serde_json::from_value::<AuditCreatedEvent>(envelope.payload)
                        .context("failed to deserialize AuditCreatedEvent")?;

                    let client_ip = audit_event.client_ip.clone();

                    let fraud_event = FraudEvent::new(
                        envelope.metadata.tenant_id.clone(),
                        envelope.metadata.event_id,
                        audit_event.id,
                        audit_event.user,
                        audit_event.action,
                        audit_event.created_at,
                    )
                    .with_client_ip(client_ip);

                    let decision = engine
                        .evaluate(&fraud_event)
                        .await
                        .context("fraud engine evaluation failed")?;

                    tracing::info!(
                        tenant_id = %fraud_event.tenant_id,
                        event_id = %fraud_event.event_id,
                        audit_id = %fraud_event.audit_id,
                        user = %fraud_event.user,
                        action = %fraud_event.action,
                        client_ip_present = fraud_event.client_ip.is_some(),
                        risk_level = ?decision.risk_level,
                        triggered_rules = ?decision.triggered_rules,
                        evidence = ?decision.evidence,
                        "ANTI_FRAUD_DECISION: fraud evaluation completed"
                    );

                    if !decision.is_suspicious() {
                        tracing::debug!(
                            tenant_id = %fraud_event.tenant_id,
                            event_id = %fraud_event.event_id,
                            risk_level = ?decision.risk_level,
                            "ANTI_FRAUD_ALERT: decision below alert threshold"
                        );

                        return Ok(());
                    }

                    if let Some(existing_alert) = alert_repository
                        .find_by_event_id(&fraud_event.tenant_id, fraud_event.event_id)
                        .await
                        .context("failed to check existing fraud alert")?
                    {
                        tracing::info!(
                            tenant_id = %fraud_event.tenant_id,
                            event_id = %fraud_event.event_id,
                            alert_id = %existing_alert.id,
                            "ANTI_FRAUD_ALERT: duplicate event already has an alert"
                        );

                        return Ok(());
                    }

                    let alert = crate::domain::fraud_alert::FraudAlert::new(
                        &fraud_event,
                        decision.risk_level,
                        decision.triggered_rules.clone(),
                        decision.evidence.clone(),
                    );

                    let alert_id = alert.id;

                    alert_repository
                        .create(&alert)
                        .await
                        .context("failed to persist fraud alert")?;

                    tracing::info!(
                        tenant_id = %alert.tenant_id,
                        event_id = %alert.event_id,
                        audit_id = %alert.audit_id,
                        alert_id = %alert_id,
                        risk_level = ?alert.risk_level,
                        status = %alert.status.as_str(),
                        "ANTI_FRAUD_ALERT: fraud alert persisted"
                    );

                    Ok(())
                }
            })
            .await?;

        Ok(())
    }
}
