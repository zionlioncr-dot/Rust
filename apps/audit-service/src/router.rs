use axum::{
    Router, middleware,
    routing::{get, post},
};

use telemetry::middleware::metrics_middleware;

use crate::{
    handlers::{
        audit_handler::create_audit, health_handler::health, metrics_handler,
        version_handler::version,
    },
    state::AppState,
};

pub fn create_router(state: AppState) -> Router {
    Router::new()

    .route("/health", get(health))

    .route("/version", get(version))

    .route("/audit", post(create_audit))

    .route("/metrics", get(metrics_handler::metrics))

    .layer(
        middleware::from_fn(metrics_middleware)
    )

    .with_state(state)
}
