use axum::{
    extract::{Request, State},
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

use crate::{config::Config, proxy::audit::ProxyState};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,

    pub iss: String,

    pub aud: String,

    pub exp: usize,

    pub iat: usize,

    #[serde(default)]
    pub scopes: Vec<String>,

    pub tenant_id: String,
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub jwt_secret: String,

    pub jwt_issuer: String,

    pub jwt_audience: String,
}

impl AuthConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            jwt_secret: config.jwt_secret.clone(),

            jwt_issuer: config.jwt_issuer.clone(),

            jwt_audience: config.jwt_audience.clone(),
        }
    }
}

pub async fn authenticate(
    State(state): State<ProxyState>,
    mut request: Request,
    next: Next,
) -> Response {
    let authorization = match request.headers().get(AUTHORIZATION) {
        Some(value) => value,

        None => {
            return unauthorized("Missing Authorization header");
        }
    };

    let authorization = match authorization.to_str() {
        Ok(value) => value,

        Err(_) => {
            return unauthorized("Invalid Authorization header");
        }
    };

    let token = match authorization.strip_prefix("Bearer ") {
        Some(token) if !token.is_empty() => token,

        _ => {
            return unauthorized("Authorization header must use Bearer token");
        }
    };

    let config = Config::load();

    let mut validation = Validation::new(Algorithm::HS256);

    validation.set_issuer(&[config.jwt_issuer.as_str()]);

    validation.set_audience(&[config.jwt_audience.as_str()]);

    let token_data = match decode::<Claims>(
        token,
        &DecodingKey::from_secret(config.jwt_secret.as_bytes()),
        &validation,
    ) {
        Ok(token_data) => token_data,

        Err(error) => {
            tracing::warn!(
                error = %error,
                "JWT authentication failed"
            );

            return unauthorized("Invalid or expired token");
        }
    };

    let claims = token_data.claims;

    if claims.tenant_id.trim().is_empty() {
        tracing::warn!(
            subject = %claims.sub,
            "JWT is missing tenant_id"
        );

        return unauthorized("JWT tenant_id is required");
    }

    // PostgreSQL is the source of truth for M2M client revocation.
    // Do not cache positive authorization decisions: disabling a client
    // must invalidate its existing JWTs on the next protected request.
    let client_status = sqlx::query_as::<_, (bool, String)>(
        r#"
        SELECT enabled, tenant_id
        FROM bank_m2m_clients
        WHERE client_id = $1
        "#,
    )
    .bind(claims.sub.trim())
    .fetch_optional(&state.pool)
    .await;

    match client_status {
        Ok(Some((true, client_tenant_id)))
            if client_tenant_id.trim() == claims.tenant_id.trim() => {}
        Ok(_) => {
            tracing::warn!(
                subject = %claims.sub,
                tenant_id = %claims.tenant_id,
                "JWT rejected because M2M client is disabled, missing, or tenant mismatched"
            );
            return unauthorized("M2M client is disabled or no longer valid");
        }
        Err(error) => {
            tracing::error!(
                error = %error,
                subject = %claims.sub,
                "Unable to verify M2M client status"
            );

            return (
                StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(serde_json::json!({
                    "error": "temporarily_unavailable",
                    "message": "Client authorization status is temporarily unavailable"
                })),
            )
                .into_response();
        }
    }

    request.extensions_mut().insert(claims);

    next.run(request).await
}

pub fn required_scope(claims: &Claims, scope: &str) -> bool {
    claims.scopes.iter().any(|item| item == scope)
}

pub fn has_scope(claims: &Claims, scope: &str) -> bool {
    required_scope(claims, scope)
}

pub fn unauthorized(message: &'static str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        axum::Json(serde_json::json!({
            "error": "unauthorized",
            "message": message
        })),
    )
        .into_response()
}

pub fn forbidden(message: &'static str) -> Response {
    (
        StatusCode::FORBIDDEN,
        axum::Json(serde_json::json!({
            "error": "forbidden",
            "message": message
        })),
    )
        .into_response()
}

pub fn internal_error(message: &'static str) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        axum::Json(serde_json::json!({
            "error": "internal_error",
            "message": message
        })),
    )
        .into_response()
}
