mod config;
mod consumer;
mod domain;
mod http;
mod rag;
mod repository;

use anyhow::Result;
use config::AmlConfig;
use repository::{AmlRepository, PostgresAmlRepository};
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    telemetry::tracing::init_tracing()?;

    let config = AmlConfig::load()?;
    let pool = common::database::create_pool(config.app.max_db_connections).await?;
    let repository: Arc<dyn AmlRepository> = Arc::new(PostgresAmlRepository::new(pool.clone()));
    let rag = Arc::new(rag::RegulatoryRag::new(
        repository_lib::RagRepository::new(pool.clone()),
        config.app.ollama_base_url.clone(),
        config.app.ollama_embedding_model.clone(),
    )?);

    let address = SocketAddr::from(([0, 0, 0, 0], config.port));
    let listener = TcpListener::bind(address).await?;
    info!(%address, "AML HTTP server iniciado");

    let http_task = async move {
        axum::serve(listener, http::router(pool))
            .await
            .map_err(anyhow::Error::from)
    };

    let kafka_task = consumer::aml_consumer::run_consumer(config, repository, rag);

    tokio::select! {
        result = http_task => {
            if let Err(error) = result {
                error!(%error, "AML HTTP server terminó con error");
                return Err(error);
            }
        }
        result = kafka_task => {
            if let Err(error) = result {
                error!(%error, "AML consumer terminó con error");
                return Err(error);
            }
        }
        result = tokio::signal::ctrl_c() => {
            result?;
            info!("Señal de apagado recibida");
        }
    }

    Ok(())
}
