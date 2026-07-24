use std::env;

pub struct Config {
    pub server_addr: String,
}

impl Config {
    pub fn load() -> Self {
        Self {
            server_addr: env::var("SERVER_ADDR").unwrap_or_else(|_| "0.0.0.0:8088".to_string()),
        }
    }
}
