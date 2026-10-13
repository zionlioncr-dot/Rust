use axum::{extract::State, http::StatusCode};

use crate::proxy::audit::ProxyState;

pub async fn ready(State(state): State<ProxyState>) -> (StatusCode, &'static str) {
    match state.check_readiness().await {
        Ok(()) => (StatusCode::OK, "READY"),
        Err(error) => {
            tracing::error!(
            error = %error,
            "API Gateway readiness check failed"
            );

            (StatusCode::SERVICE_UNAVAILABLE, "NOT READY")
        }
    }
}
