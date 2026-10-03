use std::{collections::HashMap, env, future::Future, sync::OnceLock};

use anyhow::{Context as AnyhowContext, Result};

use http::{
    header::{HeaderName, HeaderValue},
    HeaderMap,
};

use opentelemetry::{
    global,
    propagation::{Extractor, Injector},
    trace::{FutureExt, TraceContextExt, TracerProvider},
    Context,
};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SdkTracerProvider, Resource};
use tracing_opentelemetry::{OpenTelemetryLayer, OpenTelemetrySpanExt};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

/// Inicializa OpenTelemetry + tracing y configura W3C Trace Context.
pub fn init_tracing() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let service_name = env::var("OTEL_SERVICE_NAME")
        .unwrap_or_else(|_| "financial-intelligence-platform".to_string());

    let otlp_endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    // W3C Trace Context:
    // traceparent / tracestate
    global::set_text_map_propagator(TraceContextPropagator::new());

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

/// Finaliza y fuerza el envío de spans pendientes.
pub fn shutdown_tracing() {
    if let Some(provider) = TRACER_PROVIDER.get() {
        if let Err(error) = provider.force_flush() {
            tracing::error!(%error, "failed to flush OpenTelemetry spans");
        }

        let _ = provider.shutdown();
    }
}

pub fn extract_http_context(headers: &HeaderMap) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&HeaderExtractor(headers)))
}

#[derive(Clone)]
pub struct IncomingTraceContext(pub Context);

/// Inyecta el contexto actual de tracing/OpenTelemetry
/// en headers HTTP.
///
/// Principalmente genera:
/// - traceparent
/// - tracestate (si existe)
pub fn inject_current_context(headers: &mut HeaderMap) {
    let context = tracing::Span::current().context();

    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut HeaderInjector(headers));
    });
}

/// Obtiene el contexto actual como:
///
/// `(trace_id, traceparent)`
pub fn current_trace_context() -> (Option<String>, Option<String>) {
    let context = tracing::Span::current().context();

    // No tomar una referencia directamente de:
    // context.span().span_context()
    //
    // porque `context.span()` devuelve un temporal.
    let span = context.span();
    let span_context = span.span_context();

    if !span_context.is_valid() {
        return (None, None);
    }

    let trace_id = span_context.trace_id().to_string();

    let mut carrier = HashMap::new();

    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut carrier);
    });

    let traceparent = carrier.get("traceparent").cloned();

    (Some(trace_id), traceparent)
}

/// Construye un OpenTelemetry Context desde un `traceparent`
/// recibido por Kafka u otro transporte no-HTTP.
///
/// Si el valor no existe o no es válido, el propagador
/// devolverá un contexto vacío.
pub fn extract_traceparent_context(traceparent: Option<&str>) -> Context {
    let mut carrier = HashMap::new();

    if let Some(value) = traceparent {
        carrier.insert("traceparent".to_string(), value.to_string());
    }

    global::get_text_map_propagator(|propagator| propagator.extract(&carrier))
}

/// Ejecuta un future bajo un OpenTelemetry Context.
///
/// Se utiliza para propagación entre procesos, especialmente
/// después de extraer contexto desde Kafka.
///
/// Importante: usamos `with_context()` en lugar de `Context::attach()`
/// para no mantener un guard a través de `.await`.
pub async fn run_with_context<F, T>(context: Context, future: F) -> T
where
    F: Future<Output = T>,
{
    future.with_context(context).await
}

/// Injector para headers HTTP de Axum.
struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let name = match HeaderName::from_bytes(key.as_bytes()) {
            Ok(name) => name,
            Err(_) => return,
        };

        let value = match HeaderValue::from_str(&value) {
            Ok(value) => value,
            Err(_) => return,
        };

        self.0.insert(name, value);
    }
}

/// Extractor para headers HTTP de Axum.
struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect()
    }
}
