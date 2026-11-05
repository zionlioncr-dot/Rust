use anyhow::{Context, Result};
use common::config::AppConfig;
use std::env;

#[derive(Debug, Clone)]
pub struct AmlConfig {
    pub app: AppConfig,
    pub port: u16,
}

impl AmlConfig {
    pub fn load() -> Result<Self> {
        let app = AppConfig::load();

        let port = env::var("AML_PORT")
            .unwrap_or_else(|_| "3003".to_string())
            .parse::<u16>()
            .context("AML_PORT debe ser un puerto válido")?;

        Ok(Self { app, port })
    }
}
