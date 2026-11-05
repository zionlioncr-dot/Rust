use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use std::time::Duration;

use crate::{middleware::auth::Claims, proxy::audit::ProxyState};

const INTERNAL_TOKEN_HEADER: &str = "x-internal-agent-token";
const TENANT_HEADER: &str = "x-tenant-id";

pub async fn proxy_ai_agent_chat(State(state): State<ProxyState>, request: Request) -> Response {
    let claims = match request.extensions().get::<Claims>() {
        Some(claims) => claims.clone(),
        None => {
            return (StatusCode::UNAUTHORIZED, "Missing authenticated claims").into_response();
        }
    };

    let tenant_id = claims.tenant_id.trim();
    if tenant_id.is_empty() {
        return (StatusCode::UNAUTHORIZED, "Missing tenant_id").into_response();
    }

    let token = state.config.ai_agent_internal_token.trim();
    if token.is_empty() {
        tracing::error!("AI_AGENT_INTERNAL_TOKEN no está configurado");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "AI agent integration is not configured",
        )
            .into_response();
    }

    let uri = format!(
        "{}/internal/chat",
        state.config.ai_agent_url.trim_end_matches('/')
    );

    let (parts, body) = request.into_parts();
    let body = match axum::body::to_bytes(body, 2 * 1024 * 1024).await {
        Ok(body) => body,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "Invalid request body").into_response();
        }
    };

    let mut headers = HeaderMap::new();
    for (name, value) in &parts.headers {
        if name == header::AUTHORIZATION
            || name == header::HOST
            || name == header::CONTENT_LENGTH
            || name == header::CONNECTION
            || name == TENANT_HEADER
            || name == INTERNAL_TOKEN_HEADER
            || name == "x-authenticated-user"
        {
            continue;
        }

        headers.append(name.clone(), value.clone());
    }

    let tenant_value = match HeaderValue::from_str(tenant_id) {
        Ok(value) => value,
        Err(_) => {
            return (StatusCode::UNAUTHORIZED, "Invalid tenant_id").into_response();
        }
    };

    let token_value = match HeaderValue::from_str(token) {
        Ok(value) => value,
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Invalid internal agent token configuration",
            )
                .into_response();
        }
    };

    headers.insert(HeaderName::from_static(TENANT_HEADER), tenant_value);
    headers.insert(HeaderName::from_static(INTERNAL_TOKEN_HEADER), token_value);

    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(%error, "No se pudo crear cliente HTTP para AI Agent");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    match client.post(uri).headers(headers).body(body).send().await {
        Ok(upstream) => {
            let status = upstream.status();
            let upstream_headers = upstream.headers().clone();
            let bytes = match upstream.bytes().await {
                Ok(bytes) => bytes,
                Err(error) => {
                    tracing::error!(%error, "Error leyendo respuesta de AI Agent");
                    return StatusCode::BAD_GATEWAY.into_response();
                }
            };

            let mut response = Response::new(Body::from(bytes));
            *response.status_mut() = status;

            for (name, value) in &upstream_headers {
                if name == header::CONTENT_LENGTH
                    || name == header::CONNECTION
                    || name == header::TRANSFER_ENCODING
                {
                    continue;
                }
                response.headers_mut().append(name.clone(), value.clone());
            }

            response
        }
        Err(error) => {
            tracing::error!(%error, "Error comunicando con AI Agent");
            StatusCode::BAD_GATEWAY.into_response()
        }
    }
}
