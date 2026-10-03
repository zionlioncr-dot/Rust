use axum::{extract::Request, middleware::Next, response::Response};

pub async fn propagate_incoming_context(request: Request, next: Next) -> Response {
    next.run(request).await
}
