use std::{env, sync::OnceLock};

use anyhow::{Context, Result};

use opentelemetry::{global, trace::TracerProvider};

use opentelemetry_otlp::{SpanExporter, WithExportConfig};

use opentelemetry_sdk::{trace::SdkTracerProvider, Resource};

use tracing_opentelemetry::OpenTelemetryLayer;

use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

pub fn init_tracing() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let service_name = env::var("OTEL_SERVICE_NAME")
        .unwrap_or_else(|_| "financial-intelligence-platform".to_string());

    let otlp_endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .build()
        .context("failed to build OpenTelemetry OTLP exporter")?;

    let resource = Resource::builder().with_service_name(service_name).build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("financial-intelligence-platform");

    TRACER_PROVIDER
        .set(provider.clone())
        .map_err(|_| anyhow::anyhow!("tracer provider already initialized"))?;

    global::set_tracer_provider(provider);

    let otel_layer = OpenTelemetryLayer::new(tracer);

    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_thread_names(true),
        )
        .with(otel_layer)
        .init();

    Ok(())
}

pub fn shutdown_tracing() {
    if let Some(provider) = TRACER_PROVIDER.get() {
        if let Err(error) = provider.force_flush() {
            tracing::error!(%error, "failed to flush OpenTelemetry spans");
        }

        let _ = provider.shutdown();
    }
}
