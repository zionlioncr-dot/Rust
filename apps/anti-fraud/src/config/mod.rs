use common::config::AppConfig;

#[derive(Debug, Clone)]
pub struct FraudConfig {
    pub app: AppConfig,
    pub consumer_group: String,
    pub port: u16,
}

impl FraudConfig {
    pub fn load() -> Self {
        let app = AppConfig::load();

        let consumer_group =
            std::env::var("FRAUD_CONSUMER_GROUP").unwrap_or_else(|_| "fraud-group".to_string());

        let port = std::env::var("FRAUD_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(3002);

        Self {
            app,
            consumer_group,
            port,
        }
    }
}
