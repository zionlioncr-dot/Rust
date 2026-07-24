use std::time::Instant;

use axum::{
    extract::Request,
    middleware::Next,
    response::Response,
};

pub async fn metrics_middleware(
    request: Request,
    next: Next,
) -> Response {

    let method = request.method().to_string();

    let path = request.uri().path().to_string();

    let start = Instant::now();

    let response = next.run(request).await;

    let duration = start.elapsed().as_secs_f64();

    metrics::metrics::record_http(
        &method,
        &path,
    );

    metrics::metrics::record_duration(
        &method,
        &path,
        duration,
    );

    response
}