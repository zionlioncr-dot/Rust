mod config;
mod consumer;
mod domain;
mod http;
mod rag;
mod repository;

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

    info!(%address, "AML HTTP server iniciado");
    info!(
        topic = %config.kafka_topic,
        group = %config.consumer_group,
        "Iniciando HTTP y consumidor Kafka AML"
    );

    let consumer_config = config.clone();
    let consumer_pool = pool.clone();

    tokio::select! {
        result = axum::serve(listener, http::router(pool)) => {
            if let Err(error) = result {
                error!(%error, "AML HTTP server terminó con error");
                return Err(error.into());
            }
        }

        result = consumer::run_consumer(consumer_config, consumer_pool) => {
            if let Err(error) = result {
                error!(%error, "Consumidor AML terminó con error");
                return Err(error);
            }

            anyhow::bail!("El consumidor AML terminó inesperadamente");
        }
    }

    Ok(())
}
