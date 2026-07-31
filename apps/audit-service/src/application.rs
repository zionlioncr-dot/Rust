use std::net::SocketAddr;

use anyhow::Result;
use tracing::info;

use common::{config::AppConfig, lifecycle::wait_for_shutdown};

use crate::{router, state::AppState};

pub async fn run(state: AppState) -> Result<()> {
    let config = AppConfig::load();

    let app = router::create_router(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], config.server_port));

    let listener = tokio::net::TcpListener::bind(addr).await?;

    info!("Audit Service listening on {}", addr);

    tokio::select! {
        result = axum::serve(listener, app) => {
            result?;
        }

        result = wait_for_shutdown() => {
            result?;
            info!("Stopping Audit Service...");
        }
    }

    info!("Audit Service stopped.");

    Ok(())
}
