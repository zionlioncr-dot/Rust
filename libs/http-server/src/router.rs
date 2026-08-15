use std::sync::Arc;

use axum::{
    routing::get,
    Router,
};

use health::manager::HealthManager;

use crate::handlers::{
    health_handler,
    liveness_handler,
    metrics_handler,
    version_handler,
};

pub fn router(_health: Arc<HealthManager>) -> Router {
    Router::new()
        .route("/health", get(health_handler::health))
        .route("/live", get(liveness_handler::live))
        .route("/metrics", get(metrics_handler::metrics))
        .route("/version", get(version_handler::version))
}