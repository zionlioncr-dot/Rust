use anyhow::{Context, Result};
use common::config::AppConfig;
use std::env;

#[derive(Debug, Clone)]
pub struct AmlConfig {
    pub app: AppConfig,
    pub port: u16,
    pub kafka_topic: String,
    pub consumer_group: String,
    pub jurisdiction: String,
    pub ollama_chat_url: String,
    pub ollama_chat_model: String,
}

impl AmlConfig {
    pub fn load() -> Result<Self> {
        let app = AppConfig::load();

        let port = env::var("AML_PORT")
            .unwrap_or_else(|_| "3003".to_string())
            .parse::<u16>()
            .context("AML_PORT debe ser un puerto válido")?;

        let kafka_topic =
            env::var("AML_KAFKA_TOPIC").unwrap_or_else(|_| "financial-transactions".to_string());

        let consumer_group =
            env::var("AML_CONSUMER_GROUP").unwrap_or_else(|_| "aml-group".to_string());

        let jurisdiction = env::var("AML_JURISDICTION").unwrap_or_else(|_| "CR".to_string());

        let ollama_chat_url = env::var("OLLAMA_CHAT_URL").unwrap_or_else(|_| {
            format!(
                "{}/api/chat",
                env::var("OLLAMA_BASE_URL")
                    .unwrap_or_else(|_| "http://127.0.0.1:11434".to_string())
                    .trim_end_matches('/')
            )
        });

        let ollama_chat_model =
            env::var("OLLAMA_MODEL").unwrap_or_else(|_| "qwen3:0.6b".to_string());

        Ok(Self {
            app,
            port,
            kafka_topic,
            consumer_group,
            jurisdiction,
            ollama_chat_url,
            ollama_chat_model,
        })
    }
}
