use anyhow::Result;

use sqlx::{PgPool, postgres::PgPoolOptions};

/// Crea un pool de conexiones PostgreSQL.
///
/// Todas las aplicaciones de la plataforma
/// deben utilizar este helper para mantener
/// una configuración uniforme.
pub async fn create_pool(max_connections: u32) -> Result<PgPool> {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database_url)
        .await?;

    Ok(pool)
}

pub async fn health_check(pool: &PgPool) -> Result<()> {
    sqlx::query("SELECT 1").execute(pool).await?;

    Ok(())
}
