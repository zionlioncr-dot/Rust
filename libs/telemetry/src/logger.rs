use tracing::{error, info, warn};

pub fn startup(service: &str) {
    info!(service = service, "Service started");
}

pub fn shutdown(service: &str) {
    warn!(service = service, "Service stopped");
}

pub fn failure<E: std::fmt::Display>(error_message: E) {
    error!(
        error = %error_message,
        "Unhandled error"
    );
}
