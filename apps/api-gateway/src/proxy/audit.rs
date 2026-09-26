use axum::{
    body::Body,
    extract::{Path, Request, State},
    http::header::HeaderName,
    response::{IntoResponse, Response},
};
use reqwest::Client;

use crate::config::Config;

#[derive(Clone)]
pub struct ProxyState {
    pub client: Client,
    pub config: Config,
}

impl ProxyState {
    pub fn new(config: Config) -> Self {
        Self {
            client: Client::new(),
            config,
        }
    }
}

#[tracing::instrument(
    name = "audit.proxy",
    skip(state, request),
    fields(
        upstream = tracing::field::Empty,
        status = tracing::field::Empty
    )
)]
pub async fn proxy_audit(
    State(state): State<ProxyState>,
    path: Option<Path<String>>,
    request: Request,
) -> Response {
    let upstream_path = match path {
        Some(Path(path)) => {
            if path.is_empty() {
                "/audit".to_string()
            } else {
                format!("/audit/{}", path)
            }
        }

        None => "/audit".to_string(),
    };

    proxy(state, request, &upstream_path).await
}

#[tracing::instrument(
    name = "audit.upstream",
    skip(state, request),
    fields(
        http.method = %request.method(),
        http.url = tracing::field::Empty,
        http.status_code = tracing::field::Empty
    )
)]
async fn proxy(state: ProxyState, request: Request, upstream_path: &str) -> Response {
    let method = request.method().clone();

    let query = request
        .uri()
        .query()
        .map(|value| format!("?{}", value))
        .unwrap_or_default();

    let url = format!(
        "{}{}{}",
        state.config.audit_service_url.trim_end_matches('/'),
        upstream_path,
        query
    );

    tracing::Span::current().record("http.url", tracing::field::display(&url));

    let mut headers = request.headers().clone();

    /*
     * IMPORTANT:
     *
     * Do not simply forward an incoming traceparent.
     *
     * The current Gateway span is the immediate
     * parent of the outgoing HTTP operation.
     *
     * OpenTelemetry therefore injects the current
     * context and overwrites traceparent/tracestate.
     */
    telemetry::tracing::inject_current_context(&mut headers);

    let body = request.into_body();

    let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(bytes) => bytes,

        Err(error) => {
            tracing::error!(
                error = %error,
                "Failed to read request body"
            );

            return gateway_error(400, "bad_request", "Unable to read request body");
        }
    };

    let mut upstream_request = state.client.request(method.clone(), &url);

    for (name, value) in &headers {
        if should_forward_header(name) {
            upstream_request = upstream_request.header(name, value);
        }
    }

    tracing::info!(
        method = %method,
        url = %url,
        "Forwarding request to audit-service"
    );

    let upstream_response = match upstream_request.body(body_bytes).send().await {
        Ok(response) => response,

        Err(error) => {
            tracing::error!(
                error = %error,
                url = %url,
                "Audit service unavailable"
            );

            return gateway_error(502, "bad_gateway", "Audit service unavailable");
        }
    };

    let status = upstream_response.status();

    tracing::Span::current().record("http.status_code", tracing::field::display(status));

    let response_headers = upstream_response.headers().clone();

    let response_body = match upstream_response.bytes().await {
        Ok(body) => body,

        Err(error) => {
            tracing::error!(
                error = %error,
                "Failed to read audit-service response"
            );

            return gateway_error(502, "bad_gateway", "Invalid upstream response");
        }
    };

    let mut builder = Response::builder().status(status);

    for (name, value) in &response_headers {
        if should_forward_response_header(name) {
            builder = builder.header(name, value);
        }
    }

    match builder.body(Body::from(response_body)) {
        Ok(response) => response,

        Err(error) => {
            tracing::error!(
                error = %error,
                "Failed to build gateway response"
            );

            gateway_error(500, "internal_error", "Failed to build gateway response")
        }
    }
}

fn should_forward_header(name: &HeaderName) -> bool {
    !matches!(
        name.as_str(),
        "host" | "authorization" | "content-length" | "connection" | "x-authenticated-user"
    )
}

fn should_forward_response_header(name: &HeaderName) -> bool {
    !matches!(
        name.as_str(),
        "content-length" | "connection" | "transfer-encoding"
    )
}

fn gateway_error(status: u16, error: &'static str, message: &'static str) -> Response {
    let status = axum::http::StatusCode::from_u16(status)
        .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);

    (
        status,
        axum::Json(serde_json::json!({
            "error": error,
            "message": message
        })),
    )
        .into_response()
}
