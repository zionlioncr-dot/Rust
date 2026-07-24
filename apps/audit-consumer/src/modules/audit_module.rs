use std::sync::Arc;

use crate::{dispatcher::handler_registry::HandlerRegistry, handler::audit_handler::AuditHandler};

pub fn register(registry: &mut HandlerRegistry) {
    registry.register(Arc::new(AuditHandler::new()));
}
