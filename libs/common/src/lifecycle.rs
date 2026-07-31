use anyhow::Result;

use tokio::signal;

use tracing::info;

/// Espera la señal de terminación del proceso.
///
/// Actualmente soporta:
///
/// - CTRL+C
///
/// En el futuro podrá extenderse para:
///
/// - SIGTERM
/// - Kubernetes
/// - Docker
/// - ECS
pub async fn wait_for_shutdown() -> Result<()> {
    signal::ctrl_c().await?;

    info!("Shutdown signal received.");

    Ok(())
}
