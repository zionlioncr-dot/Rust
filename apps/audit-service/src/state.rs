use std::sync::Arc;

use crate::service::audit_service::AuditService;

#[derive(Clone)]
pub struct AppState {
    pub audit_service: Arc<AuditService>,
}
