use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};

use common::tenant::TenantContext;
use domain::audit_event::AuditEvent;
use telemetry::record_audit_created;

use crate::state::AppState;

#[derive(serde::Deserialize, Debug)]
pub struct CreateAuditRequest {
    pub user: String,
    pub action: String,
    #[serde(default)]
    pub client_ip: Option<String>,
}

#[tracing::instrument(
    name = "audit.create",
    skip(state, request, headers),
    fields(
        audit.user = %request.user,
        audit.action = %request.action,
        tenant_id = tracing::field::Empty
    )
)]
pub async fn create_audit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateAuditRequest>,
) -> Result<Json<AuditEvent>, StatusCode> {
    let tenant_id = headers
        .get("x-tenant-id")
        .ok_or(StatusCode::UNAUTHORIZED)?
        .to_str()
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    let tenant_context = TenantContext::new(tenant_id).map_err(|_| StatusCode::UNAUTHORIZED)?;

    tracing::Span::current().record(
        "tenant_id",
        tracing::field::display(tenant_context.tenant_id()),
    );

    let client_ip = validate_client_ip(request.client_ip).map_err(|_| StatusCode::BAD_REQUEST)?;

    let event = state
        .audit_service
        .create(tenant_context, request.user, request.action, client_ip)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    record_audit_created();

    Ok(Json(event))
}

pub async fn list_audit(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<AuditEvent>>, StatusCode> {
    let tenant_id = headers
        .get("x-tenant-id")
        .ok_or(StatusCode::UNAUTHORIZED)?
        .to_str()
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    let tenant_context = TenantContext::new(tenant_id).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let events = state
        .audit_service
        .list(tenant_context)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(events))
}

fn validate_client_ip(client_ip: Option<String>) -> Result<Option<String>, ()> {
    match client_ip {
        Some(value) => {
            let value = value.trim();

            value.parse::<std::net::IpAddr>().map_err(|_| ())?;

            Ok(Some(value.to_string()))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod client_ip_tests {
    use super::validate_client_ip;

    #[test]
    fn accepts_ipv4() {
        assert_eq!(
            validate_client_ip(Some("203.0.113.25".to_string())),
            Ok(Some("203.0.113.25".to_string()))
        );
    }

    #[test]
    fn accepts_ipv6() {
        assert_eq!(
            validate_client_ip(Some("2001:db8::1".to_string())),
            Ok(Some("2001:db8::1".to_string()))
        );
    }

    #[test]
    fn trims_whitespace_around_valid_ip() {
        assert_eq!(
            validate_client_ip(Some("  203.0.113.25  ".to_string())),
            Ok(Some("203.0.113.25".to_string()))
        );
    }

    #[test]
    fn accepts_missing_ip() {
        assert_eq!(validate_client_ip(None), Ok(None));
    }

    #[test]
    fn rejects_empty_ip() {
        assert!(validate_client_ip(Some(String::new())).is_err());
    }

    #[test]
    fn rejects_whitespace_only_ip() {
        assert!(validate_client_ip(Some("   ".to_string())).is_err());
    }

    #[test]
    fn rejects_invalid_ip() {
        assert!(validate_client_ip(Some("999.999.999.999".to_string())).is_err());
    }

    #[test]
    fn rejects_hostname_instead_of_ip() {
        assert!(validate_client_ip(Some("example.com".to_string())).is_err());
    }
}
