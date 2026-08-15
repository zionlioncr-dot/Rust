use std::sync::Arc;

use anyhow::Result;

use health::manager::HealthManager;

pub async fn start() -> Result<()> {
    let health = Arc::new(HealthManager::new());

    http_server::start(3002, health).await
}
