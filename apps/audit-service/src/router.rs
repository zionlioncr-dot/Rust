use axum::{
    Router, middleware,
    routing::{get, post},
};

use crate::handlers::{
    audit_handler, health_handler, live_handler, metrics_handler, ready_handler, version_handler,
};

use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/audit", post(audit_handler::create_audit))
        .route("/health", get(health_handler::health))
        .route("/live", get(live_handler::live))
        .route("/ready", get(ready_handler::ready))
        .route("/metrics", get(metrics_handler::metrics))
        .route("/version", get(version_handler::version))
        .layer(middleware::from_fn(telemetry::metrics_middleware))
        .with_state(state)
}
