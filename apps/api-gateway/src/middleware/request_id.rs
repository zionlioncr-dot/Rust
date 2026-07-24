use axum::extract::Request;

use axum::middleware::Next;

use axum::response::Response;

use uuid::Uuid;

pub async fn request_id(mut request: Request, next: Next) -> Response {
    request.extensions_mut().insert(Uuid::new_v4());

    next.run(request).await
}
