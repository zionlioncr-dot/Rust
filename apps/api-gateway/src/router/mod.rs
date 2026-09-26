use axum::{
    middleware,
    routing::{any, get},
    Router,
};

use crate::{
    config::Config,
    middleware::{
        auth::authenticate, logging::logging, request_id::request_id, scope::authorize_scope,
    },
    proxy::audit::{proxy_audit, ProxyState},
    routes,
};

pub fn build_router(config: Config) -> Router {
    let proxy_state = ProxyState::new(config);

    let public_routes = Router::new()
        .route("/health", get(routes::health))
        .route("/ready", get(routes::ready));

    let protected_routes = Router::new()
        .route("/audit", any(proxy_audit))
        .route("/audit/{path}", any(proxy_audit))
        .route_layer(middleware::from_fn(authorize_scope))
        .route_layer(middleware::from_fn(authenticate));

    public_routes
        .merge(protected_routes)
        .with_state(proxy_state)
        /*
         * Incoming client traceparent must be
         * extracted before api.request is created.
         */
        .layer(middleware::from_fn(
            crate::middleware::trace_context::propagate_incoming_context,
        ))
        .layer(middleware::from_fn(logging))
        .layer(middleware::from_fn(request_id))
}
