use std::sync::Arc;

use anyhow::Result;

use common::config::AppConfig;
use health::manager::HealthManager;

pub struct Application {
    config: AppConfig,
    health: Arc<HealthManager>,
}

impl Application {
    pub fn new(config: AppConfig, health: Arc<HealthManager>) -> Self {
        Self { config, health }
    }

    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    pub fn health(&self) -> Arc<HealthManager> {
        self.health.clone()
    }

    pub async fn start_http(&self, port: u16) -> Result<()> {
        let health = self.health();

        http_server::start(port, health).await
    }
}
