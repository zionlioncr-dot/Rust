use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use reqwest::Client;
use rmcp::{
    model::{
        CallToolRequestParams, ClientCapabilities, ClientInfo, Implementation,
        InitializeRequestParams,
    },
    service::ServiceExt,
    transport::StreamableHttpClientTransport,
    RoleClient,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::timeout;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

const OLLAMA_CHAT_URL: &str = "http://127.0.0.1:11434/api/chat";
const OLLAMA_MODEL: &str = "qwen3:0.6b";
const MCP_URL: &str = "http://127.0.0.1:8000/mcp";

const HTTP_HOST: &str = "0.0.0.0";
const HTTP_PORT: u16 = 8001;

const OLLAMA_TIMEOUT_SECONDS: u64 = 60;
const MAX_AGENT_ITERATIONS: usize = 8;

const SYSTEM_PROMPT: &str = r#"
Eres el agente de IA de la Financial Intelligence Platform.

Tu función es responder preguntas sobre la plataforma utilizando las herramientas MCP
disponibles.

REGLAS IMPORTANTES:

1. Usa las herramientas MCP cuando la pregunta requiera datos reales de la plataforma.
2. Nunca inventes eventos, estados, schemas, datos de Kafka o información de PostgreSQL.
3. Para verificar la salud de la plataforma utiliza system.health.
4. Para consultar eventos de auditoría utiliza audit.list_events o audit.get_event.
5. Para consultar eventos pendientes del transactional outbox utiliza audit.get_pending_outbox.
6. Para consultar schemas Avro utiliza schema.get_latest.
7. Para información relacionada con Kafka/Redpanda utiliza kafka.list_topics.
8. Si una herramienta devuelve datos, basa tu respuesta exclusivamente en esos datos.
9. Responde en español.
10. Si una herramienta falla, informa claramente del error.
11. Nunca afirmes que la plataforma está saludable, que un evento existe, que un schema
    existe o que Kafka funciona sin haber obtenido esa información mediante una herramienta.
12. Los resultados de las herramientas MCP son la fuente de verdad para el estado actual
    de la plataforma.

Cuando una herramienta pueda proporcionar la información solicitada, debes utilizarla.
"#;

#[derive(Debug, Serialize)]
struct OllamaRequest {
    model: String,
    messages: Vec<OllamaMessage>,
    tools: Vec<OllamaTool>,
    stream: bool,
    think: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OllamaMessage {
    role: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<OllamaToolCall>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    tool_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OllamaToolCall {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    call_type: Option<String>,

    function: OllamaFunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OllamaFunctionCall {
    name: String,
    arguments: Value,
}

#[derive(Debug, Clone, Serialize)]
struct OllamaTool {
    #[serde(rename = "type")]
    tool_type: String,

    function: OllamaFunction,
}

#[derive(Debug, Clone, Serialize)]
struct OllamaFunction {
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Debug, Deserialize)]
struct OllamaResponse {
    message: OllamaMessage,
}

#[derive(Debug, Clone)]
struct McpToolDefinition {
    name: String,
    description: String,
    input_schema: Value,
}

struct AiAgent {
    ollama: Client,
    mcp: rmcp::service::RunningService<RoleClient, InitializeRequestParams>,
    tools: Vec<McpToolDefinition>,
}

#[derive(Debug, Clone)]
struct AgentResult {
    answer: String,
    tool: Option<String>,
    execution_time_ms: u128,
}

#[derive(Debug, Deserialize)]
struct ChatRequest {
    message: String,
}

#[derive(Debug, Serialize)]
struct ChatResponse {
    answer: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    tool: Option<String>,

    execution_time_ms: u128,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Clone)]
struct AppState {
    agent: Arc<AiAgent>,
}

impl AiAgent {
    async fn connect() -> Result<Self> {
        info!(mcp_url = MCP_URL, "Connecting to MCP server");

        let transport = StreamableHttpClientTransport::from_uri(MCP_URL);

        let client_info = ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("financial-intelligence-ai-agent", "0.1.0"),
        );

        let mcp = client_info
            .serve(transport)
            .await
            .context("Failed to connect to MCP server")?;

        info!(
            server_info = ?mcp.peer_info(),
            "Connected to MCP server"
        );

        let tool_result = mcp
            .list_tools(Default::default())
            .await
            .context("Failed to list MCP tools")?;

        let tools = tool_result
            .tools
            .into_iter()
            .map(|tool| {
                let input_schema = serde_json::to_value(&tool.input_schema)
                    .unwrap_or_else(|_| json!({ "type": "object" }));

                McpToolDefinition {
                    name: tool.name.to_string(),
                    description: tool.description.unwrap_or_default().to_string(),
                    input_schema,
                }
            })
            .collect::<Vec<_>>();

        info!(
            tool_count = tools.len(),
            tools = ?tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            "MCP tools discovered"
        );

        let ollama = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(OLLAMA_TIMEOUT_SECONDS))
            .build()
            .context("Failed to create Ollama HTTP client")?;

        Ok(Self {
            ollama,
            mcp,
            tools,
        })
    }

    fn ollama_tools(&self) -> Vec<OllamaTool> {
        self.tools
            .iter()
            .map(|tool| OllamaTool {
                tool_type: "function".to_string(),
                function: OllamaFunction {
                    name: tool.name.clone(),
                    description: tool.description.clone(),
                    parameters: tool.input_schema.clone(),
                },
            })
            .collect()
    }

    fn has_tool(&self, name: &str) -> bool {
        self.tools.iter().any(|tool| tool.name == name)
    }

    async fn call_mcp_tool(&self, name: &str, arguments: Value) -> Result<String> {
        info!(
            tool = name,
            arguments = %arguments,
            "Calling MCP tool"
        );

        if !self.has_tool(name) {
            anyhow::bail!("MCP tool is not available: {name}");
        }

        let arguments = match arguments {
            Value::Object(map) => map,
            other => {
                warn!(
                    tool = name,
                    arguments = %other,
                    "Tool arguments were not a JSON object; using empty arguments"
                );

                serde_json::Map::new()
            }
        };

        let result = self
            .mcp
            .call_tool(
                CallToolRequestParams::new(name.to_string()).with_arguments(arguments),
            )
            .await
            .with_context(|| format!("MCP tool call failed: {name}"))?;

        let serialized =
            serde_json::to_string_pretty(&result).context("Failed to serialize MCP result")?;

        info!(
            tool = name,
            result_bytes = serialized.len(),
            "MCP tool call completed"
        );

        Ok(serialized)
    }

    async fn chat(
        &self,
        messages: Vec<OllamaMessage>,
        include_tools: bool,
    ) -> Result<OllamaMessage> {
        let tools = if include_tools {
            self.ollama_tools()
        } else {
            Vec::new()
        };

        let request = OllamaRequest {
            model: OLLAMA_MODEL.to_string(),
            messages,
            tools,
            stream: false,
            think: false,
        };

        let payload =
            serde_json::to_vec(&request).context("Failed to serialize Ollama request")?;

        info!(
            url = OLLAMA_CHAT_URL,
            model = OLLAMA_MODEL,
            payload_bytes = payload.len(),
            message_count = request.messages.len(),
            tool_count = request.tools.len(),
            include_tools,
            "Sending request to Ollama"
        );

        let response = timeout(
            Duration::from_secs(OLLAMA_TIMEOUT_SECONDS),
            self.ollama
                .post(OLLAMA_CHAT_URL)
                .header("Content-Type", "application/json")
                .body(payload)
                .send(),
        )
        .await
        .context("Timed out waiting for Ollama HTTP request")?
        .context("Failed to send request to Ollama")?;

        let status = response.status();

        info!(
            status = %status,
            "Ollama HTTP response received"
        );

        let response_text = timeout(
            Duration::from_secs(OLLAMA_TIMEOUT_SECONDS),
            response.text(),
        )
        .await
        .context("Timed out reading Ollama response")?
        .context("Failed to read Ollama response body")?;

        info!(
            response_bytes = response_text.len(),
            "Ollama response body received"
        );

        if !status.is_success() {
            error!(
                status = %status,
                response = %response_text,
                "Ollama returned an error"
            );

            anyhow::bail!("Ollama returned HTTP {}: {}", status, response_text);
        }

        let parsed: OllamaResponse =
            serde_json::from_str(&response_text).with_context(|| {
                format!(
                    "Failed to deserialize Ollama response: {}",
                    response_text
                )
            })?;

        info!(
            has_tool_calls = parsed.message.tool_calls.is_some(),
            tool_call_count = parsed
                .message
                .tool_calls
                .as_ref()
                .map(|calls| calls.len())
                .unwrap_or(0),
            content_bytes = parsed
                .message
                .content
                .as_ref()
                .map(|content| content.len())
                .unwrap_or(0),
            "Ollama response parsed"
        );

        Ok(parsed.message)
    }

    fn forced_tool_for_question(
        &self,
        question: &str,
    ) -> Option<(&'static str, Value)> {
        let q = question.to_lowercase();

        // ------------------------------------------------------------
        // 1. PLATFORM HEALTH
        // ------------------------------------------------------------
        if q.contains("salud")
            || q.contains("saludable")
            || q.contains("health")
            || q.contains("healthy")
            || q.contains("estado de la plataforma")
            || q.contains("estado del sistema")
            || q.contains("está funcionando")
            || q.contains("esta funcionando")
            || q.contains("funciona la plataforma")
        {
            return Some(("system.health", json!({})));
        }

        // ------------------------------------------------------------
        // 2. TRANSACTIONAL OUTBOX
        // ------------------------------------------------------------
        if q.contains("outbox")
            || q.contains("pendiente")
            || q.contains("pendientes")
        {
            return Some((
                "audit.get_pending_outbox",
                json!({
                    "limit": 10
                }),
            ));
        }

        // ------------------------------------------------------------
        // 3. AVRO / SCHEMA REGISTRY
        // ------------------------------------------------------------
        if q.contains("schema")
            || q.contains("schemas")
            || q.contains("avro")
            || q.contains("schema registry")
        {
            let subject = extract_schema_subject(question)
                .unwrap_or_else(|| "AuditCreated-value".to_string());

            return Some((
                "schema.get_latest",
                json!({
                    "subject": subject
                }),
            ));
        }

        // ------------------------------------------------------------
        // 4. KAFKA / REDPANDA
        // ------------------------------------------------------------
        if q.contains("kafka")
            || q.contains("redpanda")
            || q.contains("topic")
            || q.contains("topics")
        {
            return Some(("kafka.list_topics", json!({})));
        }

        // ------------------------------------------------------------
        // 5. SPECIFIC AUDIT EVENT BY UUID
        // ------------------------------------------------------------
        if let Some(uuid) = extract_uuid(question) {
            if q.contains("evento")
                || q.contains("auditor")
                || q.contains("audit")
                || q.contains("id")
            {
                return Some((
                    "audit.get_event",
                    json!({
                        "id": uuid
                    }),
                ));
            }
        }

        // ------------------------------------------------------------
        // 6. AUDIT EVENT LIST
        // ------------------------------------------------------------
        if q.contains("evento")
            || q.contains("eventos")
            || q.contains("auditoría")
            || q.contains("auditoria")
            || q.contains("audit")
            || q.contains("login")
        {
            let limit = extract_requested_limit(question).unwrap_or(10);

            return Some((
                "audit.list_events",
                json!({
                    "limit": limit
                }),
            ));
        }

        None
    }

    async fn execute_forced_tool(
        &self,
        tool_name: &str,
        arguments: Value,
    ) -> Result<OllamaMessage> {
        info!(
            tool = tool_name,
            arguments = %arguments,
            "Executing deterministic MCP tool policy"
        );

        let tool_result = self.call_mcp_tool(tool_name, arguments.clone()).await?;

        Ok(OllamaMessage {
            role: "tool".to_string(),
            content: Some(tool_result),
            tool_calls: None,
            tool_name: Some(tool_name.to_string()),
        })
    }

    async fn run(&self, question: String) -> Result<AgentResult> {
        let started = Instant::now();

        info!(question = %question, "Processing user request");

        let mut messages = vec![
            OllamaMessage {
                role: "system".to_string(),
                content: Some(SYSTEM_PROMPT.to_string()),
                tool_calls: None,
                tool_name: None,
            },
            OllamaMessage {
                role: "user".to_string(),
                content: Some(question.clone()),
                tool_calls: None,
                tool_name: None,
            },
        ];

        // ============================================================
        // DETERMINISTIC TOOL POLICY
        // ============================================================
        if let Some((tool_name, arguments)) =
            self.forced_tool_for_question(&question)
        {
            info!(
                tool = tool_name,
                arguments = %arguments,
                "Deterministic tool selected"
            );

            let tool_result = self
                .execute_forced_tool(tool_name, arguments.clone())
                .await
                .with_context(|| {
                    format!(
                        "Required MCP tool '{}' failed for the requested operation",
                        tool_name
                    )
                })?;

            let assistant_tool_call = OllamaMessage {
                role: "assistant".to_string(),
                content: None,
                tool_calls: Some(vec![OllamaToolCall {
                    call_type: Some("function".to_string()),
                    function: OllamaFunctionCall {
                        name: tool_name.to_string(),
                        arguments,
                    },
                }]),
                tool_name: None,
            };

            messages.push(assistant_tool_call);
            messages.push(tool_result);

            info!(
                tool = tool_name,
                "Deterministic MCP tool completed; requesting final answer from Ollama"
            );

            let final_message = self.chat(messages, false).await?;

            info!(
                tool = tool_name,
                execution_time_ms = started.elapsed().as_millis(),
                "Final response generated from deterministic MCP result"
            );

            return Ok(AgentResult {
                answer: final_message.content.unwrap_or_default(),
                tool: Some(tool_name.to_string()),
                execution_time_ms: started.elapsed().as_millis(),
            });
        }

        // ============================================================
        // NORMAL AGENTIC FLOW
        // ============================================================
        let mut include_tools = true;
        let mut last_tool: Option<String> = None;

        for iteration in 1..=MAX_AGENT_ITERATIONS {
            info!(
                iteration,
                message_count = messages.len(),
                include_tools,
                "Running agent iteration"
            );

            let assistant_message =
                self.chat(messages.clone(), include_tools).await?;

            let tool_calls =
                assistant_message.tool_calls.clone().unwrap_or_default();

            if tool_calls.is_empty() {
                info!(
                    iteration,
                    execution_time_ms = started.elapsed().as_millis(),
                    "Agent produced final response"
                );

                return Ok(AgentResult {
                    answer: assistant_message.content.unwrap_or_default(),
                    tool: last_tool,
                    execution_time_ms: started.elapsed().as_millis(),
                });
            }

            info!(
                iteration,
                tool_count = tool_calls.len(),
                "Model requested MCP tools"
            );

            messages.push(assistant_message);

            for tool_call in tool_calls {
                let tool_name = tool_call.function.name;
                let arguments = tool_call.function.arguments;

                last_tool = Some(tool_name.clone());

                info!(
                    tool = %tool_name,
                    arguments = %arguments,
                    "Executing requested MCP tool"
                );

                let tool_result =
                    match self.call_mcp_tool(&tool_name, arguments).await {
                        Ok(result) => result,
                        Err(error) => {
                            warn!(
                                tool = %tool_name,
                                error = %error,
                                "MCP tool execution failed"
                            );

                            json!({
                                "error": error.to_string()
                            })
                            .to_string()
                        }
                    };

                messages.push(OllamaMessage {
                    role: "tool".to_string(),
                    content: Some(tool_result),
                    tool_calls: None,
                    tool_name: Some(tool_name),
                });
            }

            // qwen3:0.6b:
            // después de recibir los datos MCP, no enviamos nuevamente
            // todas las definiciones de herramientas.
            include_tools = false;
        }

        anyhow::bail!(
            "Agent exceeded the maximum number of iterations ({})",
            MAX_AGENT_ITERATIONS
        )
    }
}

// ================================================================
// HTTP HANDLERS
// ================================================================

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "ai-agent",
    })
}

async fn chat(
    State(state): State<AppState>,
    Json(request): Json<ChatRequest>,
) -> Result<Json<ChatResponse>, (StatusCode, Json<ErrorResponse>)> {
    let message = request.message.trim();

    if message.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "The 'message' field cannot be empty".to_string(),
            }),
        ));
    }

    info!(
        message = %message,
        "Received HTTP chat request"
    );

    match state.agent.run(message.to_string()).await {
        Ok(result) => {
            info!(
                tool = ?result.tool,
                execution_time_ms = result.execution_time_ms,
                "HTTP chat request completed"
            );

            Ok(Json(ChatResponse {
                answer: result.answer,
                tool: result.tool,
                execution_time_ms: result.execution_time_ms,
            }))
        }

        Err(error) => {
            error!(
                error = %error,
                "HTTP chat request failed"
            );

            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: error.to_string(),
                }),
            ))
        }
    }
}

// ================================================================
// HELPERS
// ================================================================

fn extract_requested_limit(question: &str) -> Option<u64> {
    let lower = question.to_lowercase();

    let markers = [
        "últimos ",
        "ultimos ",
        "primeros ",
        "primeras ",
        "últimas ",
        "ultimas ",
        "top ",
    ];

    for marker in markers {
        if let Some(position) = lower.find(marker) {
            let remainder = &lower[position + marker.len()..];

            let digits: String = remainder
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();

            if !digits.is_empty() {
                if let Ok(value) = digits.parse::<u64>() {
                    if value > 0 && value <= 100 {
                        return Some(value);
                    }
                }
            }
        }
    }

    None
}

fn extract_uuid(text: &str) -> Option<String> {
    for token in text.split_whitespace() {
        let cleaned = token
            .trim_matches(|c: char| {
                matches!(
                    c,
                    '`' | '"' | '\'' | ',' | '.' | ':' | ';' | '(' | ')' | '[' | ']'
                )
            })
            .to_string();

        if cleaned.len() == 36 {
            if uuid::Uuid::parse_str(&cleaned).is_ok() {
                return Some(cleaned);
            }
        }
    }

    None
}

fn extract_schema_subject(question: &str) -> Option<String> {
    let lower = question.to_lowercase();

    if lower.contains("auditcreated-value") {
        return Some("AuditCreated-value".to_string());
    }

    if lower.contains("auditcreated_value") {
        return Some("AuditCreated-value".to_string());
    }

    for quote in ['"', '`', '\''] {
        let mut parts = question.split(quote);

        while let Some(part) = parts.next() {
            let candidate = part.trim();

            if candidate.ends_with("-value")
                && candidate.len() > "-value".len()
            {
                return Some(candidate.to_string());
            }
        }
    }

    None
}

// ================================================================
// MAIN
// ================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    info!("Starting Financial Intelligence Platform AI Agent");

    let agent = Arc::new(AiAgent::connect().await?);

    let state = AppState {
        agent: Arc::clone(&agent),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/chat", post(chat))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let address = format!("{HTTP_HOST}:{HTTP_PORT}");

    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .with_context(|| format!("Failed to bind HTTP server on {address}"))?;

    info!(
        address = %address,
        "AI Agent HTTP server started"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("AI Agent HTTP server failed")?;

    info!("AI Agent HTTP server stopped");

    Ok(())
}

async fn shutdown_signal() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            info!("Shutdown signal received");
        }

        Err(error) => {
            error!(
                error = %error,
                "Failed to listen for shutdown signal"
            );
        }
    }

    info!("AI Agent shutdown completed");
}