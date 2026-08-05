use common::config::AppConfig;

#[derive(Clone)]
pub struct ConsumerConfig {
    pub app: AppConfig,

    pub workers: usize,

    pub channel_size: usize,
}

impl ConsumerConfig {
    pub fn load() -> Self {
        Self {
            app: AppConfig::load(),

            workers: std::env::var("CONSUMER_WORKERS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(4),

            channel_size: std::env::var("CHANNEL_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
        }
    }
}
