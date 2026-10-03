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

const OLLAMA_TIMEOUT_SECONDS: u64 = 180;
const MAX_AGENT_ITERATIONS: usize = 8;

const MAX_RAG_CONTEXT_CHARS: usize = 8000;
const MAX_RAG_DOCUMENT_CHARS: usize = 2200;

const SYSTEM_PROMPT: &str = r#"
Eres el agente de IA de la Financial Intelligence Platform.

Tu función es responder preguntas sobre la plataforma utilizando las herramientas MCP
disponibles y, cuando corresponda, el contexto documental recuperado mediante RAG.

REGLAS IMPORTANTES:

- Preserva exactamente los nombres de los símbolos encontrados en el código.
- No confundas AuditCreated con AuditCreatedEvent.
- No confundas AuditCreatedEvent con AuditEvent.
- No confundas un nombre de evento con un struct, tabla, topic, herramienta MCP o servicio.
- Si el usuario pregunta por un símbolo que no aparece literalmente como definición,
  dilo explícitamente y explica qué símbolo relacionado sí aparece.
- No digas que algo "es un struct" a menos que el código mostrado defina explícitamente
  un struct con ese nombre.
- Cuando exista evidencia directa en un archivo, priorízala sobre inferencias.

REGLAS PARA SÍMBOLOS DEL CÓDIGO:

1. Identifica primero la definición exacta del símbolo solicitado.
2. No sustituyas un símbolo por otro símbolo parecido.
3. Distingue exactamente entre:
   - AuditCreated
   - AuditCreatedEvent
   - AuditEvent
   - AuditPayload
4. Si el usuario pregunta por un nombre que no aparece literalmente como símbolo o
   definición en los fragmentos recuperados, dilo.
5. Usa los nombres de structs, funciones, archivos y módulos literalmente.
6. No conviertas una relación indirecta en una afirmación directa.
7. No inventes nombres de servicios, tablas, topics, repositorios o responsabilidades.
8. Si un nombre aparece como argumento de una función, nombre de operación, tipo de
   evento, topic, schema o campo, describe exactamente ese uso; no lo conviertas
   automáticamente en un struct o en otro tipo de componente.
9. Si existen varios símbolos relacionados, explica primero cuál aparece literalmente
   y después cómo se relaciona con los demás.
10. Si el nombre preguntado no aparece como definición explícita, no afirmes que tiene
    una definición propia.

REGLAS DE RAG:

11. Para preguntas sobre arquitectura, implementación, flujo de ejecución, código,
    componentes, responsabilidades, diseño interno o cómo funciona una parte de
    la plataforma, utiliza rag.search.

12. rag.search devuelve documentación y fragmentos del código fuente de la
    plataforma. Utiliza exclusivamente ese contexto para explicar la implementación.

13. No uses rag.search para afirmar el estado actual de la plataforma cuando exista
    una herramienta determinística que pueda proporcionar ese estado.

14. Para preguntas que combinen conocimiento documental con estado actual, puedes
    utilizar rag.search y después la herramienta determinística correspondiente.

15. Distingue siempre entre estos conceptos:
    - evento de dominio
    - servicio
    - herramienta MCP
    - repositorio
    - tabla de PostgreSQL
    - tópico Kafka
    - schema Avro

    No los presentes como si fueran el mismo tipo de componente.

16. Cuando expliques código recuperado mediante RAG, menciona únicamente relaciones,
    responsabilidades y comportamientos que estén respaldados por los fragmentos
    recuperados.

17. No conviertas nombres de clases, structs, funciones o archivos en afirmaciones
    sobre comportamiento si el contexto recuperado no demuestra ese comportamiento.

18. Si el contexto recuperado no contiene suficiente información para responder una
    parte de la pregunta, dilo explícitamente. No completes la información mediante
    suposiciones.

19. Si una herramienta devuelve datos, basa la respuesta exclusivamente en esos datos.

20. Responde siempre en español.

21. Si una herramienta falla, informa claramente del error.

22. Nunca afirmes que la plataforma está saludable, que un evento existe, que un
    schema existe o que Kafka funciona sin haber obtenido esa información mediante
    una herramienta determinística.

23. Los resultados de las herramientas MCP son la fuente de verdad para el estado
    actual de la plataforma.

24. Para respuestas basadas en RAG, prioriza precisión sobre amplitud. Una respuesta
    breve y estrictamente respaldada es preferible a una explicación extensa con
    inferencias.

25. Cuando respondas una pregunta de arquitectura o implementación, estructura la
    respuesta de forma clara, por ejemplo:
    - qué componente es
    - qué responsabilidad tiene
    - cómo se relaciona con los demás componentes
    - qué está explícitamente demostrado por el código recuperado

26. No digas que una herramienta MCP fue utilizada como si fuera parte de la
    arquitectura de negocio. MCP es la interfaz de herramientas del agente.

27. El contexto RAG contiene evidencia del código. No lo describas como documentación
    generada si los fragmentos corresponden directamente a archivos fuente.
"#;

#[derive(Debug, Serialize)]
struct OllamaRequest {
    model: String,
    messages: Vec<OllamaMessage>,
    tools: Vec<OllamaTool>,
    stream: bool,
    think: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    keep_alive: Option<String>,
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

#[derive(Debug, Clone)]
struct RagDocument {
    source: String,
    metadata: String,
    content: String,
}

impl AiAgent {
    async fn connect() -> Result<Self> {
        info!(mcp_url = MCP_URL, "Connecting AI Agent to MCP server");

        info!("STEP 1: creating MCP transport");

        let transport = StreamableHttpClientTransport::from_uri(MCP_URL);

        info!("STEP 2: MCP transport created");

        let client_info = ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("financial-intelligence-ai-agent", env!("CARGO_PKG_VERSION")),
        );

        info!("STEP 3: MCP client info created");

        info!("STEP 4: starting MCP client session");

        let mcp = client_info
            .serve(transport)
            .await
            .context("Failed to connect to MCP server")?;

        info!("STEP 5: Connected to MCP server");

        info!("STEP 6: requesting MCP tool list");

        let listed_tools = mcp
            .list_tools(Default::default())
            .await
            .context("Failed to list MCP tools")?;

        info!("STEP 7: MCP tools listed");

        let tools = listed_tools
            .tools
            .into_iter()
            .map(|tool| McpToolDefinition {
                name: tool.name.to_string(),
                description: tool.description.unwrap_or_default().to_string(),
                input_schema: serde_json::to_value(tool.input_schema).unwrap_or_else(|_| json!({})),
            })
            .collect::<Vec<_>>();

        info!(
            tool_count = tools.len(),
            tools = ?tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            "MCP tools loaded"
        );

        info!("STEP 8: creating Ollama HTTP client");

        let ollama = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(OLLAMA_TIMEOUT_SECONDS))
            .build()
            .context("Failed to create Ollama HTTP client")?;

        info!(
            url = OLLAMA_CHAT_URL,
            model = OLLAMA_MODEL,
            timeout_seconds = OLLAMA_TIMEOUT_SECONDS,
            "Ollama client configured"
        );

        info!("STEP 9: AI Agent connection initialization completed");

        Ok(Self { ollama, mcp, tools })
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
            anyhow::bail!("MCP tool '{}' is not registered", name);
        }

        let arguments = if arguments.is_object() {
            arguments
        } else {
            json!({})
        };

        let mut request = CallToolRequestParams::new(name.to_string());

        request.arguments = arguments
            .as_object()
            .cloned()
            .map(rmcp::model::JsonObject::from);

        let result = self
            .mcp
            .call_tool(request)
            .await
            .with_context(|| format!("MCP tool '{}' failed", name))?;

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
            keep_alive: Some("10m".to_string()),
        };

        let payload = serde_json::to_vec(&request).context("Failed to serialize Ollama request")?;

        info!(
            url = OLLAMA_CHAT_URL,
            model = OLLAMA_MODEL,
            payload_bytes = payload.len(),
            message_count = request.messages.len(),
            tool_count = request.tools.len(),
            include_tools,
            timeout_seconds = OLLAMA_TIMEOUT_SECONDS,
            "Sending request to Ollama"
        );

        let started = Instant::now();

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
        .map_err(|error| {
            anyhow::anyhow!(
                "Failed to send request to Ollama: {} (elapsed={}ms)",
                error,
                started.elapsed().as_millis()
            )
        })?;

        let status = response.status();

        info!(
            status = %status,
            elapsed_ms = started.elapsed().as_millis(),
            "Ollama HTTP response received"
        );

        let response_text = timeout(Duration::from_secs(OLLAMA_TIMEOUT_SECONDS), response.text())
            .await
            .context("Timed out reading Ollama response")?
            .context("Failed to read Ollama response body")?;

        info!(
            response_bytes = response_text.len(),
            elapsed_ms = started.elapsed().as_millis(),
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

        let parsed: OllamaResponse = serde_json::from_str(&response_text)
            .with_context(|| format!("Failed to deserialize Ollama response: {}", response_text))?;

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
            elapsed_ms = started.elapsed().as_millis(),
            "Ollama response parsed"
        );

        Ok(parsed.message)
    }

    fn forced_tool_for_question(&self, question: &str) -> Option<(&'static str, Value)> {
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
        if q.contains("outbox") || q.contains("pendiente") || q.contains("pendientes") {
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
        // 6. SEMANTIC RAG / PLATFORM KNOWLEDGE
        // ------------------------------------------------------------
        if is_rag_question(question) {
            return Some((
                "rag.search",
                json!({
                    "query": question,
                    "limit": 5
                }),
            ));
        }

        // ------------------------------------------------------------
        // 7. AUDIT EVENT LIST
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
        question: &str,
    ) -> Result<OllamaMessage> {
        info!(
            tool = tool_name,
            arguments = %arguments,
            "Executing deterministic MCP tool policy"
        );

        let raw_result = self.call_mcp_tool(tool_name, arguments.clone()).await?;

        let content = if tool_name == "rag.search" {
            let compacted = compact_rag_context(&raw_result, question);

            info!(
                original_bytes = raw_result.len(),
                compacted_bytes = compacted.len(),
                "Compacted RAG context for Ollama"
            );

            compacted
        } else {
            raw_result
        };

        Ok(OllamaMessage {
            role: "tool".to_string(),
            content: Some(content),
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
        if let Some((tool_name, arguments)) = self.forced_tool_for_question(&question) {
            info!(
                tool = tool_name,
                arguments = %arguments,
                "Deterministic tool selected"
            );

            // --------------------------------------------------------
            // RAG: execute first so we can inspect the actual source
            // before deciding whether Ollama is necessary.
            // --------------------------------------------------------
            if tool_name == "rag.search" {
                let raw_result = self
                    .call_mcp_tool(tool_name, arguments.clone())
                    .await
                    .with_context(|| {
                        format!(
                            "Required MCP tool '{}' failed for the requested operation",
                            tool_name
                        )
                    })?;

                info!(
                    tool = tool_name,
                    result_bytes = raw_result.len(),
                    "RAG result received"
                );

                // ----------------------------------------------------
                // Deterministic symbol-definition answer.
                //
                // This deliberately bypasses the small local model
                // for exact code-symbol questions so the model cannot
                // hallucinate a type, file, or relationship.
                // ----------------------------------------------------
                if let Some(answer) = answer_symbol_question(&question, &raw_result) {
                    info!(
                        tool = tool_name,
                        execution_time_ms = started.elapsed().as_millis(),
                        "RAG symbol question answered deterministically"
                    );

                    return Ok(AgentResult {
                        answer,
                        tool: Some(tool_name.to_string()),
                        execution_time_ms: started.elapsed().as_millis(),
                    });
                }

                let compacted = compact_rag_context(&raw_result, &question);

                info!(
                    original_bytes = raw_result.len(),
                    compacted_bytes = compacted.len(),
                    "Compacted RAG context for Ollama"
                );

                messages.push(OllamaMessage {
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
                });

                messages.push(OllamaMessage {
                    role: "tool".to_string(),
                    content: Some(compacted),
                    tool_calls: None,
                    tool_name: Some(tool_name.to_string()),
                });

                info!(tool = tool_name, "RAG result requires Ollama final answer");

                let final_message = self.chat(messages, false).await?;

                return Ok(AgentResult {
                    answer: final_message.content.unwrap_or_default(),
                    tool: Some(tool_name.to_string()),
                    execution_time_ms: started.elapsed().as_millis(),
                });
            }

            // --------------------------------------------------------
            // NON-RAG DETERMINISTIC TOOLS
            // --------------------------------------------------------
            let tool_result = self
                .execute_forced_tool(tool_name, arguments.clone(), &question)
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

            let assistant_message = self.chat(messages.clone(), include_tools).await?;

            let tool_calls = assistant_message.tool_calls.clone().unwrap_or_default();

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

                let tool_result = match self.call_mcp_tool(&tool_name, arguments).await {
                    Ok(result) => {
                        if tool_name == "rag.search" {
                            compact_rag_context(&result, &question)
                        } else {
                            result
                        }
                    }

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

            include_tools = false;
        }

        anyhow::bail!(
            "Agent exceeded the maximum number of iterations ({})",
            MAX_AGENT_ITERATIONS
        )
    }
}

// ================================================================
// DETERMINISTIC RAG SYMBOL ANSWERS
// ================================================================

fn answer_symbol_question(question: &str, raw_result: &str) -> Option<String> {
    if !is_symbol_definition_question(question) {
        return None;
    }

    let target = extract_symbol_from_question(question)?;

    tracing::info!(
        target = %target,
        raw_result_preview = %raw_result.chars().take(2000).collect::<String>(),
        "Deterministic symbol analysis input"
    );

    tracing::info!(
        target = %target,
        "Analyzing RAG result for deterministic symbol answer"
    );

    // ---------------------------------------------------------------------
    // Special case: AuditCreated
    //
    // AuditCreated is NOT the same symbol as AuditCreatedEvent.
    //
    // In the platform code, AuditCreated appears as the operation:
    //
    //     OutboxBuilder::audit_created(&saved_event)
    //
    // while AuditCreatedEvent is an explicitly defined struct.
    // ---------------------------------------------------------------------
    if target == "AuditCreated" {
        let normalized = raw_result.to_ascii_lowercase();

        let has_audit_created_operation = normalized.contains("outboxbuilder::audit_created")
            || normalized.contains("outboxbuilder :: audit_created")
            || normalized.contains("audit_created(&saved_event)")
            || normalized.contains("audit_created ( &saved_event )");

        let has_audit_created_event =
            normalized.contains("auditcreatedevent") || normalized.contains("audit_created_event");

        if has_audit_created_operation || has_audit_created_event {
            tracing::info!(
                has_audit_created_operation,
                has_audit_created_event,
                "RAG symbol question answered deterministically"
            );

            let mut answer = String::new();

            answer.push_str(
                "Según el código recuperado, `AuditCreated` no aparece definido \
                 como un `struct` o `enum` independiente.\n\n",
            );

            if has_audit_created_operation {
                answer.push_str(
                    "En `apps/audit-service/src/service/audit_service.rs`, \
                     aparece como la operación `OutboxBuilder::audit_created(&saved_event)`, \
                     utilizada después de crear y guardar el evento de auditoría. \
                     Esta operación construye el registro que se coloca en el outbox \
                     para representar la creación del evento.\n\n",
                );
            }

            if has_audit_created_event {
                answer.push_str(
                    "Esto debe distinguirse de `AuditCreatedEvent`, que sí es un \
                     `struct` definido explícitamente en \
                     `libs/domain/src/events/audit_created.rs`.",
                );
            }

            return Some(answer);
        }

        tracing::info!(
            target = %target,
            "AuditCreated detected but expected RAG evidence was not found"
        );
    }

    // ---------------------------------------------------------------------
    // Generic exact struct detection.
    // ---------------------------------------------------------------------

    let documents = parse_rag_documents(raw_result);

    tracing::info!(
        target = %target,
        document_count = documents.len(),
        "Parsed RAG documents for deterministic symbol analysis"
    );

    for document in &documents {
        let source = &document.source;

        let struct_pattern = format!("struct {}", target);

        if source.contains(&struct_pattern) {
            let fields = extract_struct_fields(source, &target);

            let mut answer = format!(
                "Según el código recuperado, `{}` está definido como un `struct` \
                 en `{}`.",
                target, document.source
            );

            if !fields.is_empty() {
                answer.push_str("\n\nSus campos son:\n");

                for field in fields {
                    answer.push_str(&format!("- `{}`\n", field));
                }
            }

            return Some(answer);
        }

        let enum_pattern = format!("enum {}", target);

        if source.contains(&enum_pattern) {
            return Some(format!(
                "Según el código recuperado, `{}` está definido como un `enum` \
                 en `{}`.",
                target, document.source
            ));
        }
    }

    // ---------------------------------------------------------------------
    // Generic operation/function detection.
    // ---------------------------------------------------------------------

    let snake_target = to_snake_case_symbol(&target);

    for document in &documents {
        let source = &document.source;

        let patterns = [
            format!("fn {}", snake_target),
            format!("async fn {}", snake_target),
            format!(".{}(", snake_target),
            format!("::{}(", snake_target),
        ];

        if patterns.iter().any(|pattern| source.contains(pattern)) {
            return Some(format!(
                "Según el código recuperado, `{}` aparece como una \
                 función u operación en `{}`. Los fragmentos recuperados \
                 no muestran una definición de `{}` como `struct` o `enum`.",
                target, document.source, target
            ));
        }
    }

    None
}

fn is_symbol_definition_question(question: &str) -> bool {
    let q = question.to_lowercase();

    let definition_markers = [
        "qué es ",
        "que es ",
        "qué significa ",
        "que significa ",
        "define ",
        "definición de ",
        "definicion de ",
        "qué representa ",
        "que representa ",
        "qué tipo es ",
        "que tipo es ",
    ];

    definition_markers.iter().any(|marker| q.contains(marker))
}

fn extract_symbol_from_question(question: &str) -> Option<String> {
    let lower = question.to_lowercase();

    let markers = [
        "qué es ",
        "que es ",
        "qué significa ",
        "que significa ",
        "define ",
        "definición de ",
        "definicion de ",
        "qué representa ",
        "que representa ",
        "qué tipo es ",
        "que tipo es ",
    ];

    // Common Spanish words that may appear between the definition
    // marker and the actual code symbol.
    let ignored_words = [
        "el",
        "la",
        "los",
        "las",
        "un",
        "una",
        "un",
        "tipo",
        "símbolo",
        "simbolo",
        "nombre",
        "componente",
    ];

    for marker in markers {
        if let Some(position) = lower.find(marker) {
            let remainder = &question[position + marker.len()..];

            for raw_candidate in remainder.split_whitespace() {
                let candidate = raw_candidate
                    .trim_matches(|c: char| {
                        matches!(
                            c,
                            '`' | '"'
                                | '\''
                                | ','
                                | '.'
                                | ':'
                                | ';'
                                | '('
                                | ')'
                                | '['
                                | ']'
                                | '?'
                                | '¿'
                        )
                    })
                    .trim();

                if candidate.is_empty() {
                    continue;
                }

                if ignored_words
                    .iter()
                    .any(|word| candidate.eq_ignore_ascii_case(word))
                {
                    continue;
                }

                if is_code_symbol(candidate) {
                    return Some(candidate.to_string());
                }
            }
        }
    }

    None
}

fn is_code_symbol(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }

    let mut chars = value.chars();

    let first = match chars.next() {
        Some(ch) => ch,
        None => return false,
    };

    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }

    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn to_snake_case_symbol(symbol: &str) -> String {
    if symbol.contains('_') {
        return symbol.to_string();
    }

    let mut result = String::new();

    for (index, ch) in symbol.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index > 0 {
                result.push('_');
            }

            result.push(ch.to_ascii_lowercase());
        } else {
            result.push(ch);
        }
    }

    result
}

fn parse_rag_documents(raw: &str) -> Vec<RagDocument> {
    let parsed: Value = match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(_) => return Vec::new(),
    };

    let mut documents = Vec::new();

    extract_rag_document_records(&parsed, &mut documents);

    documents
}

fn extract_rag_document_records(value: &Value, output: &mut Vec<RagDocument>) {
    match value {
        Value::Array(items) => {
            for item in items {
                extract_rag_document_records(item, output);
            }
        }

        Value::Object(map) => {
            let source = map
                .get("source")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();

            let content = map
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();

            if !content.is_empty() {
                let metadata = map
                    .get("metadata")
                    .and_then(Value::as_object)
                    .map(format_rag_metadata)
                    .unwrap_or_default();

                output.push(RagDocument {
                    source,
                    metadata,
                    content,
                });

                return;
            }

            for child in map.values() {
                extract_rag_document_records(child, output);
            }
        }

        // Plain strings are not RAG documents.
        Value::String(_) => {}

        _ => {}
    }
}

fn extract_struct_fields(content: &str, struct_name: &str) -> Vec<String> {
    let markers = [
        format!("pub struct {}", struct_name),
        format!("struct {}", struct_name),
    ];

    let mut start = None;

    for marker in &markers {
        if let Some(position) = content.find(marker) {
            start = Some(position + marker.len());
            break;
        }
    }

    let Some(start) = start else {
        return Vec::new();
    };

    let remainder = &content[start..];

    let Some(open_brace) = remainder.find('{') else {
        return Vec::new();
    };

    let body = &remainder[open_brace + 1..];

    let Some(close_brace) = body.find('}') else {
        return Vec::new();
    };

    let body = &body[..close_brace];

    let mut fields = Vec::new();

    for line in body.lines() {
        let line = line.trim();

        if let Some(field) = line.strip_prefix("pub ") {
            if let Some((name, type_name)) = field.split_once(':') {
                let name = name.trim();
                let type_name = type_name.trim().trim_end_matches(',').trim();

                if is_code_symbol(name) && !type_name.is_empty() {
                    fields.push(format!("{}: {}", name, type_name));
                }
            }
        }
    }

    fields
}

// ================================================================
// RAG CONTEXT COMPACTION
// ================================================================

fn compact_rag_context(raw: &str, question: &str) -> String {
    let parsed: Value = match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(_) => {
            return truncate_chars(
                &format!(
                    "RAG CONTEXT\n\
                     QUESTION:\n{}\n\n\
                     INSTRUCCIONES:\n\
                     - Usa únicamente la información proporcionada.\n\
                     - No inventes relaciones o comportamientos.\n\
                     - Conserva literalmente los nombres de los símbolos.\n\n\
                     {}",
                    question, raw
                ),
                MAX_RAG_CONTEXT_CHARS,
            );
        }
    };

    let mut documents = Vec::<String>::new();

    extract_rag_documents(&parsed, &mut documents);

    if documents.is_empty() {
        return truncate_chars(
            &format!(
                "RAG CONTEXT\n\
                 QUESTION:\n{}\n\n\
                 INSTRUCCIONES:\n\
                 - Usa únicamente la información proporcionada.\n\
                 - No inventes relaciones o comportamientos.\n\
                 - Conserva literalmente los nombres de los símbolos.\n\n\
                 {}",
                question, raw
            ),
            MAX_RAG_CONTEXT_CHARS,
        );
    }

    let mut output = format!(
        "RAG CONTEXT\n\
         QUESTION:\n{}\n\n\
         INSTRUCCIONES:\n\
         - Usa únicamente la información de los documentos siguientes.\n\
         - El contenido es código fuente de la plataforma, no documentación generada.\n\
         - Conserva literalmente los nombres de structs, enums, funciones, módulos, archivos, topics y tablas.\n\
         - No trates dos nombres diferentes como sinónimos.\n\
         - Si se pregunta por un símbolo concreto, distingue ese símbolo de otros símbolos relacionados.\n\
         - No inventes componentes, relaciones, responsabilidades o comportamientos.\n\
         - No conviertas una relación indirecta en una afirmación directa.\n\
         - Si la evidencia no permite determinar algo, indícalo explícitamente.\n\n",
        question
    );

    for (index, document) in documents.iter().enumerate() {
        if output.chars().count() >= MAX_RAG_CONTEXT_CHARS {
            break;
        }

        let document = truncate_chars(document, MAX_RAG_DOCUMENT_CHARS);

        let section = format!(
            "==================================================\n\
             DOCUMENT {}\n\
             ==================================================\n\
             {}\n\n",
            index + 1,
            document
        );

        let remaining = MAX_RAG_CONTEXT_CHARS.saturating_sub(output.chars().count());

        if remaining == 0 {
            break;
        }

        if section.chars().count() <= remaining {
            output.push_str(&section);
        } else {
            output.push_str(&truncate_chars(&section, remaining));
            break;
        }
    }

    output
}

fn extract_rag_documents(value: &Value, output: &mut Vec<String>) {
    match value {
        Value::Array(items) => {
            for item in items {
                extract_rag_documents(item, output);
            }
        }

        Value::Object(map) => {
            let source = map
                .get("source")
                .and_then(Value::as_str)
                .unwrap_or("unknown");

            let content = map.get("content").and_then(Value::as_str).unwrap_or("");

            if !content.trim().is_empty() {
                let metadata = map
                    .get("metadata")
                    .and_then(Value::as_object)
                    .map(format_rag_metadata)
                    .unwrap_or_default();

                let document = if metadata.is_empty() {
                    format!(
                        "SOURCE: {}\n\
                         CONTENT:\n{}",
                        source,
                        content.trim()
                    )
                } else {
                    format!(
                        "SOURCE: {}\n\
                         METADATA: {}\n\
                         CONTENT:\n{}",
                        source,
                        metadata,
                        content.trim()
                    )
                };

                output.push(document);
                return;
            }

            for child in map.values() {
                extract_rag_documents(child, output);
            }
        }

        // Plain strings are not RAG documents.
        Value::String(_) => {}

        _ => {}
    }
}

fn format_rag_metadata(metadata: &serde_json::Map<String, Value>) -> String {
    let mut fields = Vec::new();

    for key in ["component", "layer", "topic", "language", "path"] {
        if let Some(value) = metadata.get(key).and_then(Value::as_str) {
            if !value.is_empty() {
                fields.push(format!("{}={}", key, value));
            }
        }
    }

    if fields.is_empty() {
        String::new()
    } else {
        fields.join(" ")
    }
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }

    let truncated: String = text.chars().take(max_chars).collect();

    format!("{}\n[context truncated]", truncated)
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

fn is_rag_question(question: &str) -> bool {
    let q = question.to_lowercase();

    let knowledge_terms = [
        // Arquitectura
        "arquitectura",
        "architecture",
        "diseño",
        "design",
        "estructura",
        "componentes",
        "componente",
        "component",
        // Funcionamiento
        "cómo funciona",
        "como funciona",
        "cómo se",
        "como se",
        "cómo procesa",
        "como procesa",
        "cómo recibe",
        "como recibe",
        "cómo publica",
        "como publica",
        "cómo consume",
        "como consume",
        "cómo fluye",
        "como fluye",
        "flujo",
        "flow",
        // Implementación
        "implementación",
        "implementacion",
        "implementado",
        "implementation",
        "código",
        "codigo",
        "code",
        "fuente",
        "source",
        "archivo",
        "files",
        "función",
        "funcion",
        "method",
        "método",
        "metodo",
        // Responsabilidades
        "responsabilidad",
        "responsabilidades",
        "qué hace",
        "que hace",
        "para qué sirve",
        "para que sirve",
        // Internals
        "interno",
        "internals",
        "detalle",
        "detalles",
        "proceso",
        "procesamiento",
        "pipeline",
        "workflow",
        // Relaciones entre componentes
        "relación entre",
        "relacion entre",
        "conecta",
        "conexión",
        "conexion",
        "interactúa",
        "interactua",
        "comunicación",
        "comunicacion",
    ];

    knowledge_terms.iter().any(|term| q.contains(term))
}

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

        if cleaned.len() == 36 && uuid::Uuid::parse_str(&cleaned).is_ok() {
            return Some(cleaned);
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

            if candidate.ends_with("-value") && candidate.len() > "-value".len() {
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
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
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
