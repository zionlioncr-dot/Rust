use axum::{extract::Request, middleware::Next, response::Response};

pub async fn propagate_incoming_context(request: Request, next: Next) -> Response {
    let parent_context = telemetry::tracing::extract_http_context(request.headers());

    telemetry::tracing::run_with_context(parent_context, next.run(request)).await
}
