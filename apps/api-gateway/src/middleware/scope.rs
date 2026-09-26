use axum::{extract::Request, http::Method, middleware::Next, response::Response};

use super::auth::{forbidden, Claims};

pub async fn authorize_scope(request: Request, next: Next) -> Response {
    let required_scope = match *request.method() {
        Method::GET => "audit:read",

        Method::POST | Method::PUT | Method::PATCH | Method::DELETE => "audit:write",

        _ => {
            return forbidden("HTTP method is not allowed for audit API");
        }
    };

    let claims = match request.extensions().get::<Claims>() {
        Some(claims) => claims,

        None => {
            return forbidden("Authenticated claims are missing");
        }
    };

    if !claims.scopes.iter().any(|scope| scope == required_scope) {
        tracing::warn!(
            subject = %claims.sub,
            required_scope = required_scope,
            "Insufficient JWT scope"
        );

        return forbidden("Insufficient scope");
    }

    next.run(request).await
}
