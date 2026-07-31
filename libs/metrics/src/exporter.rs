use axum::{
    http::{header, StatusCode},
    response::IntoResponse,
};

use prometheus::{Encoder, TextEncoder};

use crate::metrics::REGISTRY;

/// Endpoint compatible con Prometheus.
///
/// GET /metrics
pub async fn metrics() -> impl IntoResponse {
    let encoder = TextEncoder::new();

    let metric_families = REGISTRY.gather();

    let mut buffer = Vec::new();

    encoder.encode(&metric_families, &mut buffer).unwrap();

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, encoder.format_type().to_string())],
        buffer,
    )
}
