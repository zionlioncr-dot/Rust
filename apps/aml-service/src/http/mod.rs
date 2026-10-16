use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use sqlx::PgPool;

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

async fn ready(State(pool): State<PgPool>) -> impl IntoResponse {
    match sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&pool)
        .await
    {
        Ok(_) => (StatusCode::OK, "READY"),
        Err(error) => {
            tracing::error!(%error, "AML readiness check falló");
            (StatusCode::SERVICE_UNAVAILABLE, "NOT READY")
        }
    }
}

pub fn router(pool: PgPool) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .with_state(pool)
}
