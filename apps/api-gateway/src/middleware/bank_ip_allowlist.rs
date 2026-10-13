use std::net::SocketAddr;

use axum::{
    extract::{ConnectInfo, Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};

use repository::BankIpAllowlistRepository;

use crate::{
    middleware::auth::{forbidden, unauthorized, Claims},
    proxy::audit::ProxyState,
};

pub async fn bank_ip_allowlist(
    State(state): State<ProxyState>,
    ConnectInfo(socket_addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    let claims = match request.extensions().get::<Claims>() {
        Some(claims) => claims,

        None => {
            tracing::warn!(
                source_ip = %socket_addr.ip(),
                "Bank IP allowlist executed without authenticated claims"
            );

            return unauthorized("Authenticated claims are missing");
        }
    };

    let tenant_id = claims.tenant_id.trim();

    if tenant_id.is_empty() {
        tracing::warn!(
            source_ip = %socket_addr.ip(),
            "Bank IP allowlist received empty tenant_id"
        );

        return unauthorized("JWT tenant_id is required");
    }

    let source_ip = socket_addr.ip();

    tracing::Span::current().record("tenant_id", tracing::field::display(tenant_id));

    tracing::info!(
        tenant_id = %tenant_id,
        source_ip = %source_ip,
        "Checking bank source IP allowlist"
    );

    let allowed = match state
        .bank_ip_allowlist_repository
        .is_source_ip_allowed(tenant_id, source_ip)
        .await
    {
        Ok(allowed) => allowed,

        Err(error) => {
            /*
             * Fail closed.
             *
             * If the authorization database cannot be consulted,
             * the bank request must not be allowed through.
             */
            tracing::error!(
                error = %error,
                tenant_id = %tenant_id,
                source_ip = %source_ip,
                "Failed to query bank IP allowlist"
            );

            return (
                StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(serde_json::json!({
                    "error": "service_unavailable",
                    "message": "Bank source IP authorization is temporarily unavailable"
                })),
            )
                .into_response();
        }
    };

    if !allowed {
        tracing::warn!(
            tenant_id = %tenant_id,
            source_ip = %source_ip,
            "Bank source IP rejected by tenant allowlist"
        );

        return forbidden("Bank source IP is not authorized");
    }

    tracing::info!(
        tenant_id = %tenant_id,
        source_ip = %source_ip,
        "Bank source IP accepted by tenant allowlist"
    );

    next.run(request).await
}
