use std::time::Instant;

use axum::{
    extract::Request,
    middleware::Next,
    response::Response,
};

use crate::metrics;

pub async fn metrics_middleware(

    request: Request,

    next: Next,

) -> Response {

    let method =

        request.method()

        .to_string();

    let path =

        request

            .uri()

            .path()

            .to_string();

    let start =

        Instant::now();

    let response =

        next

            .run(request)

            .await;

    let elapsed =

        start

            .elapsed()

            .as_secs_f64();

    metrics::record_http(

        &method,

        &path,

        response

            .status()

            .as_str(),

    );

    metrics::record_duration(

        &method,

        &path,

        elapsed,

    );

    response

}