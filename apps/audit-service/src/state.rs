use std::sync::Arc;

use sqlx::PgPool;

use crate::service::audit_service::AuditService;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,

    pub audit_service: Arc<AuditService>,
}
