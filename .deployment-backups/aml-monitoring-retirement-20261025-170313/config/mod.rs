use anyhow::{Context, Result};
use common::config::AppConfig;
use std::env;

#[derive(Debug, Clone)]
pub struct AmlConfig {
    pub app: AppConfig,
    pub consumer_group: String,
    pub kafka_topic: String,
    pub port: u16,
    pub high_risk_countries: Vec<String>,
}

impl AmlConfig {
    pub fn load() -> Result<Self> {
        let app = AppConfig::load();

        let high_risk_countries = env::var("AML_HIGH_RISK_COUNTRIES")
            .unwrap_or_default()
            .split(',')
            .map(|country| country.trim().to_ascii_uppercase())
            .filter(|country| !country.is_empty())
            .collect();

        let port = env::var("AML_PORT")
            .unwrap_or_else(|_| "3003".to_string())
            .parse::<u16>()
            .context("AML_PORT debe ser un puerto válido")?;

        Ok(Self {
            app,
            consumer_group: env::var("AML_CONSUMER_GROUP")
                .unwrap_or_else(|_| "aml-group".to_string()),
            kafka_topic: env::var("AML_KAFKA_TOPIC")
                .unwrap_or_else(|_| "financial-transactions".to_string()),
            port,
            high_risk_countries,
        })
    }
}
