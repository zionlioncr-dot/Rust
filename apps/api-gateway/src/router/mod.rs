use axum::{
    middleware,
    routing::{any, get, post},
    Router,
};
use redis::aio::ConnectionManager;
use sqlx::PgPool;

use crate::{
    config::Config,
    middleware::{
        auth::authenticate,
        bank_ip_allowlist::bank_ip_allowlist,
        logging::logging,
        request_id::request_id,
        scope::{authorize_fraud_scope, authorize_scope},
        trace_context::propagate_incoming_context,
    },
    proxy::{
        audit::{proxy_audit, ProxyState},
        fraud::proxy_fraud_alerts,
    },
    routes,
};

pub fn build_router(config: Config, pool: PgPool, redis: Option<ConnectionManager>) -> Router {
    let proxy_state = ProxyState::new(config, pool, redis);

    let public_routes = Router::new()
        .route("/health", get(routes::health))
        .route("/ready", get(routes::ready))
        .route("/oauth/token", post(routes::oauth::issue_token));

    let protected_audit_routes = Router::new()
        .route("/audit", any(proxy_audit))
        .route("/audit/{path}", any(proxy_audit))
        .route_layer(middleware::from_fn(authorize_scope))
        .route_layer(middleware::from_fn_with_state(
            proxy_state.clone(),
            authenticate,
        ));

    let protected_fraud_routes = Router::new()
        .route("/fraud/alerts", get(proxy_fraud_alerts))
        .route("/fraud/alerts/{path}", get(proxy_fraud_alerts))
        /*
         * Execution order:
         *
         * authenticate
         *     ↓
         * bank_ip_allowlist
         *     ↓
         * authorize_fraud_scope
         *     ↓
         * proxy_fraud_alerts
         */
        .route_layer(middleware::from_fn(authorize_fraud_scope))
        .route_layer(middleware::from_fn_with_state(
            proxy_state.clone(),
            bank_ip_allowlist,
        ))
        .route_layer(middleware::from_fn_with_state(
            proxy_state.clone(),
            authenticate,
        ));

    public_routes
        .merge(protected_audit_routes)
        .merge(protected_fraud_routes)
        .with_state(proxy_state)
        .layer(middleware::from_fn(logging))
        .layer(middleware::from_fn(propagate_incoming_context))
        .layer(middleware::from_fn(request_id))
}
