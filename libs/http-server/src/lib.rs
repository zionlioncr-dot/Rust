use anyhow::Result;
use axum::serve;
use tokio::net::TcpListener;

pub mod handlers;
pub mod router;

/// Inicia el servidor HTTP de observabilidad.
///
/// Expone:
/// - GET /health
/// - GET /version
/// - GET /metrics
pub async fn start(port: u16) -> Result<()> {
    let app = router::router();

    let address = format!("0.0.0.0:{port}");

    let listener = TcpListener::bind(&address).await?;

    serve(listener, app).await?;

    Ok(())
}