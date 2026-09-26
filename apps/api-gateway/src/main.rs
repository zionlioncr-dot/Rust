mod config;
mod middleware;
mod proxy;
mod router;
mod routes;

use std::net::SocketAddr;

use anyhow::Result;

use crate::config::Config;
use crate::router::build_router;

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::init_tracing()?;

    let config = Config::load();

    let app = build_router(config.clone());

    let addr: SocketAddr = config.server_addr.parse()?;

    tracing::info!(
        service = "api-gateway",
        address = %addr,
        "API Gateway starting"
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;

    tracing::info!(
        address = %addr,
        "API Gateway listening"
    );

    axum::serve(listener, app).await?;

    telemetry::shutdown_tracing();

    Ok(())
}
