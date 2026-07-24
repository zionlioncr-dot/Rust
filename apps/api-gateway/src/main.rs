mod config;
mod middleware;
mod router;
mod routes;

use anyhow::Result;
use router::build_router;
use telemetry::tracing::init_tracing;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    init_tracing()?;

    let config = config::Config::load();

    let app = build_router();

    let listener = tokio::net::TcpListener::bind(&config.server_addr).await?;

    tracing::info!("Gateway running on {}", config.server_addr);

    axum::serve(listener, app).await?;

    Ok(())
}
