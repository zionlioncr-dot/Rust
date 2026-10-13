use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{ConnectInfo, Form, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use repository::BankIpAllowlistRepository;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::FromRow;
use subtle::ConstantTimeEq;

use crate::{middleware::auth::Claims, proxy::audit::ProxyState};

const ACCESS_TOKEN_TTL_SECS: u64 = 3600;

#[derive(Debug, Deserialize)]
pub struct TokenRequest {
    grant_type: String,
    client_id: String,
    client_secret: String,
}

#[derive(Debug, Serialize)]
struct TokenResponse {
    access_token: String,
    token_type: &'static str,
    expires_in: u64,
    scope: String,
}

#[derive(Debug, Serialize)]
struct OAuthError {
    error: &'static str,
    error_description: &'static str,
}

#[derive(Debug, FromRow)]
struct M2mClient {
    tenant_id: String,
    client_secret_hash: Vec<u8>,
    scopes: Vec<String>,
    enabled: bool,
}

pub async fn issue_token(
    State(state): State<ProxyState>,
    ConnectInfo(socket_addr): ConnectInfo<SocketAddr>,
    Form(input): Form<TokenRequest>,
) -> Response {
    if input.grant_type != "client_credentials" {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "unsupported_grant_type",
            "Only client_credentials is supported",
        );
    }

    if input.client_id.trim().is_empty()
        || input.client_secret.is_empty()
        || input.client_id.len() > 200
        || input.client_secret.len() > 512
    {
        return invalid_client();
    }

    let client = match sqlx::query_as::<_, M2mClient>(
        r#"
        SELECT tenant_id, client_secret_hash, scopes, enabled
        FROM bank_m2m_clients
        WHERE client_id = $1
        "#,
    )
    .bind(input.client_id.trim())
    .fetch_optional(&state.pool)
    .await
    {
        Ok(Some(client)) => client,
        Ok(None) => return invalid_client(),
        Err(error) => {
            tracing::error!(%error, "M2M client lookup failed");
            return oauth_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "temporarily_unavailable",
                "Authentication service is temporarily unavailable",
            );
        }
    };

    let supplied_hash = Sha256::digest(input.client_secret.as_bytes());
    let secret_matches = client
        .client_secret_hash
        .as_slice()
        .ct_eq(supplied_hash.as_slice())
        .unwrap_u8()
        == 1;

    if !client.enabled || !secret_matches {
        tracing::warn!(
            client_id = %input.client_id,
            "M2M client authentication failed"
        );
        return invalid_client();
    }

    let tenant_id = client.tenant_id.trim();

    if tenant_id.is_empty() || client.scopes.is_empty() {
        tracing::error!(
            client_id = %input.client_id,
            "M2M client configuration is invalid"
        );
        return oauth_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "Client authorization is not correctly configured",
        );
    }

    // Reuse the existing PostgreSQL/Redis-backed bank IP allowlist.
    // The source IP is the TCP peer, not a caller-supplied HTTP header.
    match state
        .bank_ip_allowlist_repository
        .is_source_ip_allowed(tenant_id, socket_addr.ip())
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            tracing::warn!(
                client_id = %input.client_id,
                tenant_id = %tenant_id,
                source_ip = %socket_addr.ip(),
                "M2M token request rejected by bank IP allowlist"
            );
            return oauth_error(
                StatusCode::FORBIDDEN,
                "access_denied",
                "Source IP is not authorized",
            );
        }
        Err(error) => {
            tracing::error!(
                %error,
                tenant_id = %tenant_id,
                "Bank IP authorization lookup failed"
            );
            return oauth_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "temporarily_unavailable",
                "Bank IP authorization is temporarily unavailable",
            );
        }
    }

    let iat = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as usize,
        Err(error) => {
            tracing::error!(%error, "System clock is before UNIX epoch");
            return oauth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "Unable to issue access token",
            );
        }
    };

    let claims = Claims {
        sub: input.client_id.trim().to_string(),
        iss: state.config.jwt_issuer.clone(),
        aud: state.config.jwt_audience.clone(),
        iat,
        exp: iat + ACCESS_TOKEN_TTL_SECS as usize,
        scopes: client.scopes.clone(),
        tenant_id: tenant_id.to_string(),
    };

    let access_token = match encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(state.config.jwt_secret.as_bytes()),
    ) {
        Ok(token) => token,
        Err(error) => {
            tracing::error!(%error, "M2M access token signing failed");
            return oauth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "Unable to issue access token",
            );
        }
    };

    tracing::info!(
        client_id = %input.client_id,
        tenant_id = %tenant_id,
        "M2M access token issued"
    );

    (
        StatusCode::OK,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::PRAGMA, "no-cache"),
        ],
        Json(TokenResponse {
            access_token,
            token_type: "Bearer",
            expires_in: ACCESS_TOKEN_TTL_SECS,
            scope: client.scopes.join(" "),
        }),
    )
        .into_response()
}

fn invalid_client() -> Response {
    oauth_error(
        StatusCode::UNAUTHORIZED,
        "invalid_client",
        "Client authentication failed",
    )
}

fn oauth_error(status: StatusCode, error: &'static str, description: &'static str) -> Response {
    (
        status,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::PRAGMA, "no-cache"),
        ],
        Json(OAuthError {
            error,
            error_description: description,
        }),
    )
        .into_response()
}
