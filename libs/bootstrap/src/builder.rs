use std::sync::Arc;

use anyhow::Result;

use common::config::AppConfig;

use health::manager::HealthManager;

use crate::application::Application;

pub struct BootstrapBuilder {
    config: AppConfig,

    health: Option<Arc<HealthManager>>,
}

impl BootstrapBuilder {
    pub fn new() -> Self {
        let config = AppConfig::load();

        Self {
            config,
            health: None,
        }
    }

    pub fn health(mut self, health: Arc<HealthManager>) -> Self {
        self.health = Some(health);

        self
    }

    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    pub fn build(self) -> Result<Application> {
        let health = self
            .health
            .unwrap_or_else(|| Arc::new(HealthManager::new()));

        Ok(Application::new(self.config, health))
    }
}

impl Default for BootstrapBuilder {
    fn default() -> Self {
        Self::new()
    }
}
