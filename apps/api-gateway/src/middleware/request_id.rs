use axum::{extract::Request, http::HeaderValue, middleware::Next, response::Response};

use uuid::Uuid;

pub async fn request_id(mut request: Request, next: Next) -> Response {
    let request_id = Uuid::new_v4();

    request.extensions_mut().insert(request_id);

    let request_id_header = match HeaderValue::from_str(&request_id.to_string()) {
        Ok(value) => value,

        Err(error) => {
            tracing::error!(
                error = %error,
                "Failed to create request ID header"
            );

            return next.run(request).await;
        }
    };

    request
        .headers_mut()
        .insert("x-request-id", request_id_header.clone());

    let mut response = next.run(request).await;

    response
        .headers_mut()
        .insert("x-request-id", request_id_header);

    response
}
