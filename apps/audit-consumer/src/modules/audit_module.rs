use std::sync::Arc;

use crate::{
    dispatcher::handler_registry::HandlerRegistry, handler::audit_handler::AuditHandler,
    service::audit_processing_service::AuditProcessingService,
};

pub fn register(registry: &mut HandlerRegistry, service: Arc<AuditProcessingService>) {
    registry.register(Arc::new(AuditHandler::new(service)));
}
