use axum::response::IntoResponse;

pub async fn metrics() -> impl IntoResponse {
    metrics::exporter::metrics().await
}
