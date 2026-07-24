use once_cell::sync::Lazy;

use prometheus::{
    register_counter_vec,
    register_histogram_vec,
    CounterVec,
    HistogramVec,
};

pub static HTTP_REQUESTS: Lazy<CounterVec> =
    Lazy::new(|| {

        register_counter_vec!(
            "http_requests_total",
            "Total HTTP Requests",
            &[
                "method",
                "path",
                "status",
            ],
        )
        .unwrap()

    });

pub static HTTP_DURATION: Lazy<HistogramVec> =
    Lazy::new(|| {

        register_histogram_vec!(
            "http_request_duration_seconds",
            "HTTP Request Duration",
            &[
                "method",
                "path",
            ],
        )
        .unwrap()

    });

pub static AUDIT_CREATED: Lazy<CounterVec> =
    Lazy::new(|| {

        register_counter_vec!(
            "audit_events_created_total",
            "Audit Events",
            &[
                "action",
            ],
        )
        .unwrap()

    });

pub fn record_http(

    method: &str,

    path: &str,

    status: &str,

) {

    HTTP_REQUESTS

        .with_label_values(

            &[
                method,
                path,
                status,
            ],

        )

        .inc();

}

pub fn record_duration(

    method: &str,

    path: &str,

    seconds: f64,

) {

    HTTP_DURATION

        .with_label_values(

            &[

                method,

                path,

            ],

        )

        .observe(seconds);

}

pub fn record_audit(

    action: &str,

) {

    AUDIT_CREATED

        .with_label_values(

            &[action],

        )

        .inc();

}