use std::time::Instant;

use axum::{extract::Request, middleware::Next, response::Response};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

pub async fn logging(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();

    let request_id = request.extensions().get::<Uuid>().cloned();

    let parent_context = telemetry::tracing::extract_http_context(request.headers());

    let span = tracing::info_span!(
        "api.request",
        method = %method,
        uri = %uri,
        request_id = tracing::field::Empty,
        status = tracing::field::Empty,
        duration_ms = tracing::field::Empty,
    );

    if let Err(error) = span.set_parent(parent_context) {
        tracing::error!(
            %error,
            "Failed to set incoming OpenTelemetry parent context"
        );
    }

    if let Some(request_id) = request_id {
        span.record("request_id", tracing::field::display(request_id));
    }

    let _guard = span.enter();

    let (trace_id, traceparent) = telemetry::tracing::current_trace_context();

    tracing::info!(
        trace_id = ?trace_id,
        traceparent = ?traceparent,
        "API Gateway OpenTelemetry context"
    );

    let started_at = Instant::now();

    let response = next.run(request).await;

    let duration_ms = started_at.elapsed().as_millis();

    span.record("status", tracing::field::display(response.status()));
    span.record("duration_ms", duration_ms);

    tracing::info!("API Gateway request completed");

    response
}
