use crate::{config::AmlConfig, rag::RegulatoryRag, repository::PostgresAmlRepository};
use anyhow::{Context, Result};
use domain::events::{
    event_envelope::EventEnvelope, event_types::FINANCIAL_TRANSACTION_CREATED,
    financial_transaction_created::FinancialTransactionCreatedEvent,
};
use kafka::consumer::KafkaConsumer;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{info, warn};

pub async fn run_consumer(config: AmlConfig, pool: PgPool) -> Result<()> {
    let kafka = Arc::new(KafkaConsumer::new(
        &config.app.kafka_brokers,
        &config.consumer_group,
        &config.app.schema_registry_url,
    )?);

    kafka
        .subscribe(&[config.kafka_topic.as_str()])
        .await
        .context("No se pudo suscribir el consumidor AML al tópico")?;

    let repository = Arc::new(PostgresAmlRepository::new(pool.clone()));

    let rag_repository = repository_lib::RagRepository::new(pool);
    let rag = Arc::new(RegulatoryRag::new(&config, rag_repository)?);

    info!(
        topic = %config.kafka_topic,
        group = %config.consumer_group,
        jurisdiction = %config.jurisdiction,
        model = %config.ollama_chat_model,
        "Consumidor AML iniciado"
    );

    let kafka_for_handler = Arc::clone(&kafka);
    let repository_for_handler = Arc::clone(&repository);
    let rag_for_handler = Arc::clone(&rag);

    kafka
        .listen(move |event| {
            let kafka = Arc::clone(&kafka_for_handler);
            let repository = Arc::clone(&repository_for_handler);
            let rag = Arc::clone(&rag_for_handler);

            async move {
                let decoded = kafka.decode_avro(&event.payload).await?;
                let envelope: EventEnvelope =
                    serde_json::from_value(decoded).context("EventEnvelope inválido")?;

                if envelope.event_type != FINANCIAL_TRANSACTION_CREATED {
                    return Ok(());
                }

                let tenant_id = envelope.metadata.tenant_id.trim().to_string();
                let event_id = envelope.metadata.event_id.to_string();

                if tenant_id.is_empty() {
                    warn!(%event_id, "Evento AML descartado: tenant_id vacío");
                    return Ok(());
                }

                let payload: FinancialTransactionCreatedEvent =
                    serde_json::from_value(envelope.payload)
                        .context("Payload FinancialTransactionCreated inválido")?;

                let transaction = payload.transaction;

                if transaction.tenant_id.trim() != tenant_id {
                    anyhow::bail!(
                        "tenant_id del evento no coincide con el de la transacción: event_id={event_id}"
                    );
                }

                if transaction.amount <= Decimal::ZERO {
                    anyhow::bail!(
                        "Importe de transacción no positivo: event_id={event_id}"
                    );
                }

                if transaction.currency.trim().len() != 3 {
                    anyhow::bail!(
                        "Código de moneda inválido: event_id={event_id}"
                    );
                }

                // El modelo decide contextualmente si genera una alerta.
                // Si Ollama o el RAG falla, se propaga el error para no marcar
                // intencionalmente el evento como procesado sin análisis.
                let alert = rag
                    .assess(&tenant_id, &event_id, &transaction)
                    .await?;

                let alerts = alert.into_iter().collect::<Vec<_>>();

                let processed = repository
                    .process_event(&tenant_id, &event_id, &alerts)
                    .await?;

                if processed {
                    info!(
                        %tenant_id,
                        %event_id,
                        transaction_id = %transaction.id,
                        alert_count = alerts.len(),
                        "Evento AML procesado"
                    );
                } else {
                    info!(%tenant_id, %event_id, "Evento AML duplicado; omitido");
                }

                Ok(())
            }
        })
        .await
}
