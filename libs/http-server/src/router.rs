use axum::{
    routing::get,
    Router,
};

use crate::handlers::{
    health_handler,
    metrics_handler,
    version_handler,
};

pub fn router() -> Router {
    Router::new()
        .route("/health", get(health_handler::health))
        .route("/metrics", get(metrics_handler::metrics))
        .route("/version", get(version_handler::version))
}