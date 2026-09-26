use std::time::Instant;

use axum::{extract::Request, middleware::Next, response::Response};
use uuid::Uuid;

pub async fn logging(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();

    let request_id = request.extensions().get::<Uuid>().cloned();

    let span = tracing::info_span!(
        "api.request",
        method = %method,
        uri = %uri,
        request_id = tracing::field::Empty,
        status = tracing::field::Empty,
        duration_ms = tracing::field::Empty,
    );

    if let Some(request_id) = request_id {
        span.record("request_id", tracing::field::display(request_id));
    }

    let _guard = span.enter();

    let started_at = Instant::now();

    let response = next.run(request).await;

    let duration_ms = started_at.elapsed().as_millis();

    span.record("status", tracing::field::display(response.status()));
    span.record("duration_ms", duration_ms);

    tracing::info!("API Gateway request completed");

    response
}
