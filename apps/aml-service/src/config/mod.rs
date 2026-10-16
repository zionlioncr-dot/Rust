use anyhow::{Context, Result};
use common::config::AppConfig;
use rust_decimal::Decimal;
use std::{env, str::FromStr};

#[derive(Debug, Clone)]
pub struct AmlConfig {
    pub app: AppConfig,
    pub consumer_group: String,
    pub kafka_topic: String,
    pub port: u16,
    pub high_value_threshold: Decimal,
    pub high_risk_countries: Vec<String>,
    pub structuring_min_transactions: i64,
    pub structuring_min_total: Decimal,
}

impl AmlConfig {
    pub fn load() -> Result<Self> {
        let app = AppConfig::load();

        let high_value_threshold = parse_decimal_env("AML_HIGH_VALUE_THRESHOLD", "10000")?;

        let structuring_min_total = parse_decimal_env("AML_STRUCTURING_MIN_TOTAL", "10000")?;

        anyhow::ensure!(
            high_value_threshold > Decimal::ZERO,
            "AML_HIGH_VALUE_THRESHOLD debe ser positivo"
        );
        anyhow::ensure!(
            structuring_min_total > Decimal::ZERO,
            "AML_STRUCTURING_MIN_TOTAL debe ser positivo"
        );

        let high_risk_countries = env::var("AML_HIGH_RISK_COUNTRIES")
            .unwrap_or_default()
            .split(',')
            .map(|country| country.trim().to_ascii_uppercase())
            .filter(|country| !country.is_empty())
            .collect();

        let structuring_min_transactions = env::var("AML_STRUCTURING_MIN_TRANSACTIONS")
            .unwrap_or_else(|_| "5".to_string())
            .parse::<i64>()
            .context("AML_STRUCTURING_MIN_TRANSACTIONS debe ser un entero")?;

        anyhow::ensure!(
            structuring_min_transactions >= 2,
            "AML_STRUCTURING_MIN_TRANSACTIONS debe ser al menos 2"
        );

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
            high_value_threshold,
            high_risk_countries,
            structuring_min_transactions,
            structuring_min_total,
        })
    }
}

fn parse_decimal_env(name: &str, default: &str) -> Result<Decimal> {
    let value = env::var(name).unwrap_or_else(|_| default.to_string());
    Decimal::from_str(&value).with_context(|| format!("{name} debe ser un decimal válido"))
}
