mod config;
mod http;

use anyhow::Result;
use config::AmlConfig;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    telemetry::tracing::init_tracing()?;

    let config = AmlConfig::load()?;
    let pool = common::database::create_pool(config.app.max_db_connections).await?;

    let address = SocketAddr::from(([0, 0, 0, 0], config.port));
    let listener = TcpListener::bind(address).await?;

    info!(%address, "AML HTTP server iniciado; monitoreo automático deshabilitado");

    if let Err(error) = axum::serve(listener, http::router(pool)).await {
        error!(%error, "AML HTTP server terminó con error");
        return Err(error.into());
    }

    Ok(())
}
