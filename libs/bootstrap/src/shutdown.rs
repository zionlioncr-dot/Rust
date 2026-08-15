use anyhow::Result;

use tracing::info;

pub async fn wait_for_shutdown() -> Result<()> {
    tokio::signal::ctrl_c().await?;

    info!("Shutdown signal received");

    Ok(())
}
