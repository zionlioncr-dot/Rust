use std::{collections::HashMap, sync::Arc};

use crate::handler::event_handler::EventHandler;

pub struct HandlerRegistry {
    handlers: HashMap<String, Arc<dyn EventHandler>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub fn register(&mut self, handler: Arc<dyn EventHandler>) {
        self.handlers
            .insert(handler.event_type().to_string(), handler);
    }

    pub fn get(&self, event_type: &str) -> Option<Arc<dyn EventHandler>> {
        self.handlers.get(event_type).cloned()
    }
}
