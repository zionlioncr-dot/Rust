use std::sync::Arc;

use anyhow::Result;
use axum::{serve, Router};
use health::manager::HealthManager;
use tokio::net::TcpListener;

pub mod handlers;
pub mod router;

/// Inicia el servidor HTTP genérico de observabilidad.
pub async fn start(port: u16, health: Arc<HealthManager>) -> Result<()> {
    let app = router::router(health);

    let address = format!("0.0.0.0:{port}");

    let listener = TcpListener::bind(&address).await?;

    serve(listener, app).await?;

    Ok(())
}

/// Inicia el servidor HTTP utilizando un Router
/// proporcionado por la aplicación.
pub async fn start_with_router(port: u16, app: Router) -> Result<()> {
    let address = format!("0.0.0.0:{port}");

    let listener = TcpListener::bind(&address).await?;

    serve(listener, app).await?;

    Ok(())
}
