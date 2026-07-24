use axum::extract::Request;

use axum::middleware::Next;

use axum::response::Response;

pub async fn logging(request: Request, next: Next) -> Response {
    tracing::info!("{} {}", request.method(), request.uri());

    next.run(request).await
}
