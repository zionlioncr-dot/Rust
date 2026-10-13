mod config;
mod middleware;
mod proxy;
mod router;
mod routes;

use redis::aio::ConnectionManager;
use std::{net::SocketAddr, time::Duration};

const REDIS_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

use anyhow::Result;

use common::database::create_pool;

use crate::config::Config;
use crate::router::build_router;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    telemetry::init_tracing()?;

    let config = Config::load();

    tracing::info!(service = "api-gateway", "API Gateway starting");

    tracing::info!(
        max_db_connections = config.max_db_connections,
        "Creating PostgreSQL pool"
    );

    let pool = create_pool(config.max_db_connections).await?;

    tracing::info!("PostgreSQL pool created");

    let redis_manager: Option<ConnectionManager> =
        match redis::Client::open(config.redis_url.as_str()) {
            Ok(client) => {
                match tokio::time::timeout(REDIS_CONNECT_TIMEOUT, client.get_connection_manager())
                    .await
                {
                    Ok(Ok(connection)) => {
                        tracing::info!("Redis cache connection established");
                        Some(connection)
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(
                            error = %error,
                            "Redis unavailable; using PostgreSQL without cache"
                        );
                        None
                    }
                    Err(_) => {
                        tracing::warn!(
                            timeout_ms = REDIS_CONNECT_TIMEOUT.as_millis(),
                            "Redis connection timed out; using PostgreSQL without cache"
                        );
                        None
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "Invalid Redis URL; using PostgreSQL without cache"
                );
                None
            }
        };

    let app = build_router(config.clone(), pool, redis_manager);

    let addr: SocketAddr = config.server_addr.parse()?;

    tracing::info!(
        address = %addr,
        "API Gateway listening"
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    telemetry::shutdown_tracing();

    Ok(())
}
