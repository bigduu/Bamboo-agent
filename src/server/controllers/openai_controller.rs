use crate::agent::llm::api::models::{
    ChatCompletionRequest, ChatCompletionResponse, ChatCompletionStreamChunk,
};
use crate::agent::llm::protocol::FromProvider;
use crate::agent::server::state::AppState as AgentAppState;
use crate::server::{
    error::AppError, model_config_helper::get_default_model_from_config, app_state::AppState,
};
use actix_web::{get, post, web, HttpResponse};
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Serialize)]
struct ListModelsResponse {
    object: String,
    data: Vec<Model>,
}

#[derive(Serialize)]
struct Model {
    id: String,
    object: String,
    created: u64,
    owned_by: String,
}

#[derive(Deserialize)]
struct CopilotTokenConfig {
    #[allow(dead_code)]
    token: String,
    expires_at: u64,
    #[allow(dead_code)]
    annotations_enabled: bool,
    #[allow(dead_code)]
    chat_enabled: bool,
}

/// Check if we have valid authentication before triggering device flow
/// Returns true if auth is available (via env var or valid token files)
fn has_valid_auth(app_data_dir: &Path) -> bool {
    // Check for COPILOT_API_KEY environment variable first
    if std::env::var("COPILOT_API_KEY")
        .ok()
        .map(|k| !k.trim().is_empty())
        .unwrap_or(false)
    {
        log::info!("COPILOT_API_KEY is set, auth available");
        return true;
    }

    let token_path = app_data_dir.join(".token");
    let copilot_token_path = app_data_dir.join(".copilot_token.json");

    // Check .copilot_token.json first (cached config with expiry)
    if copilot_token_path.exists() {
        match std::fs::read_to_string(&copilot_token_path) {
            Ok(content) => {
                // Try to parse and validate the token
                if let Ok(config) = serde_json::from_str::<CopilotTokenConfig>(&content) {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);

                    // Add 60 second buffer to match token validation logic
                    if config.expires_at.saturating_sub(60) > now {
                        log::info!("Valid cached copilot token found, auth available");
                        return true;
                    } else {
                        log::info!("Cached copilot token expired, will trigger auth if needed");
                        // Remove expired token file
                        let _ = std::fs::remove_file(&copilot_token_path);
                    }
                } else {
                    log::warn!("Failed to parse .copilot_token.json, will re-authenticate");
                    // Remove invalid token file
                    let _ = std::fs::remove_file(&copilot_token_path);
                }
            }
            Err(e) => {
                log::error!("Failed to read .copilot_token.json: {}", e);
                // Continue to check .token file
            }
        }
    }

    // Check .token file (access token for exchange)
    if token_path.exists() {
        match std::fs::read_to_string(&token_path) {
            Ok(content) => {
                let trimmed = content.trim();
                if !trimmed.is_empty() {
                    log::info!("Valid .token file found, auth available");
                    true
                } else {
                    log::info!(".token file is empty, auth not available");
                    false
                }
            }
            Err(e) => {
                log::error!("Failed to read .token file: {}", e);
                false
            }
        }
    } else {
        log::info!("No token files found, auth not available");
        false
    }
}

#[get("/models")]
pub async fn get_models(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    // Check if we have valid authentication before triggering any auth flow
    if !has_valid_auth(&app_state.app_data_dir) {
        log::info!("No valid authentication found (no env var or valid token files), returning empty model list");
        return Ok(HttpResponse::Ok().json(ListModelsResponse {
            object: "list".to_string(),
            data: vec![],
        }));
    }

    // Get provider and fetch models
    let provider = app_state.get_provider().await;
    let model_ids = match provider.list_models().await {
        Ok(model_ids) => model_ids,
        Err(e) => {
            // Check if error is related to proxy auth
            let err_msg = e.to_string();
            if err_msg.contains("proxy") || err_msg.contains("407") {
                return Err(AppError::ProxyAuthRequired);
            }
            return Err(AppError::InternalError(anyhow::anyhow!(
                "Failed to fetch models: {}",
                e
            )));
        }
    };

    // Convert model IDs to OpenAI-compatible format
    let models: Vec<Model> = model_ids
        .into_iter()
        .map(|id| Model {
            id,
            object: "model".to_string(),
            created: 1677610602, // Use a fixed timestamp for compatibility
            owned_by: "github-copilot".to_string(),
        })
        .collect();

    let response = ListModelsResponse {
        object: "list".to_string(),
        data: models,
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Convert OpenAI chat messages to internal messages
fn convert_messages(
    messages: Vec<crate::agent::llm::api::models::ChatMessage>,
) -> Result<Vec<crate::agent::core::Message>, AppError> {
    messages
        .into_iter()
        .map(|msg| {
            crate::agent::core::Message::from_provider(msg).map_err(|e| {
                AppError::InternalError(anyhow::anyhow!("Failed to convert message: {}", e))
            })
        })
        .collect()
}

/// Convert OpenAI tools to internal tool schemas
fn convert_tools(
    tools: Option<Vec<crate::agent::llm::api::models::Tool>>,
) -> Result<Vec<crate::agent::core::tools::ToolSchema>, AppError> {
    match tools {
        Some(tools) => tools
            .into_iter()
            .map(|tool| {
                crate::agent::core::tools::ToolSchema::from_provider(tool).map_err(|e| {
                    AppError::InternalError(anyhow::anyhow!("Failed to convert tool: {}", e))
                })
            })
            .collect(),
        None => Ok(vec![]),
    }
}

/// Convert LLMChunk stream to OpenAI stream format
fn convert_chunk_to_openai(
    chunk: crate::agent::llm::types::LLMChunk,
    model: &str,
) -> Option<ChatCompletionStreamChunk> {
    use crate::agent::llm::api::models::*;

    match chunk {
        crate::agent::llm::types::LLMChunk::Token(text) => Some(ChatCompletionStreamChunk {
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
            object: Some("chat.completion.chunk".to_string()),
            created: chrono::Utc::now().timestamp() as u64,
            model: Some(model.to_string()),
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    role: None,
                    content: Some(text),
                    tool_calls: None,
                },
                finish_reason: None,
            }],
            usage: None,
        }),
        crate::agent::llm::types::LLMChunk::ToolCalls(tool_calls) => {
            let stream_tool_calls: Vec<StreamToolCall> = tool_calls
                .into_iter()
                .enumerate()
                .map(|(idx, tc)| StreamToolCall {
                    index: idx as u32,
                    id: Some(tc.id),
                    tool_type: Some(tc.tool_type),
                    function: Some(StreamFunctionCall {
                        name: Some(tc.function.name),
                        arguments: Some(tc.function.arguments),
                    }),
                })
                .collect();

            Some(ChatCompletionStreamChunk {
                id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
                object: Some("chat.completion.chunk".to_string()),
                created: chrono::Utc::now().timestamp() as u64,
                model: Some(model.to_string()),
                choices: vec![StreamChoice {
                    index: 0,
                    delta: StreamDelta {
                        role: None,
                        content: None,
                        tool_calls: Some(stream_tool_calls),
                    },
                    finish_reason: None,
                }],
                usage: None,
            })
        }
        crate::agent::llm::types::LLMChunk::Done => Some(ChatCompletionStreamChunk {
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
            object: Some("chat.completion.chunk".to_string()),
            created: chrono::Utc::now().timestamp() as u64,
            model: Some(model.to_string()),
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    role: None,
                    content: None,
                    tool_calls: None,
                },
                finish_reason: Some("stop".to_string()),
            }],
            usage: None,
        }),
    }
}

/// Build a complete response from accumulated chunks
fn build_completion_response(
    content: String,
    tool_calls: Option<Vec<crate::agent::llm::api::models::ToolCall>>,
    model: &str,
) -> ChatCompletionResponse {
    use crate::agent::llm::api::models::*;

    ChatCompletionResponse {
        id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
        object: Some("chat.completion".to_string()),
        created: Some(chrono::Utc::now().timestamp() as u64),
        model: Some(model.to_string()),
        choices: vec![ResponseChoice {
            index: 0,
            message: ChatMessage {
                role: Role::Assistant,
                content: Content::Text(content),
                tool_calls,
                tool_call_id: None,
            },
            finish_reason: Some("stop".to_string()),
        }],
        usage: Some(Usage {
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
        }),
        system_fingerprint: None,
    }
}

#[post("/chat/completions")]
pub async fn chat_completions(
    app_state: web::Data<AppState>,
    _agent_state: web::Data<AgentAppState>,
    req: web::Json<ChatCompletionRequest>,
) -> Result<HttpResponse, AppError> {
    let stream = req.stream.unwrap_or(false);
    let request = req.into_inner();
    let requested_model = request.model.clone();
    let resolved_model = if requested_model.trim() == "default" {
        let config = app_state.config.read().await.clone();
        get_default_model_from_config(&config).map_err(|e| {
            AppError::InternalError(anyhow::anyhow!(
                "No default model configured: {}. Please set a model in config.json.",
                e
            ))
        })?
    } else {
        requested_model
    };

    // Convert messages to internal format
    let internal_messages = convert_messages(request.messages)?;
    let internal_tools = convert_tools(request.tools)?;
    let max_tokens = request
        .parameters
        .get("max_tokens")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32);

    if stream {
        let provider = app_state.get_provider().await;

        // Start streaming
        let mut stream_result = provider
            .chat_stream(
                &internal_messages,
                &internal_tools,
                max_tokens,
                resolved_model.as_str(),
            )
            .await
            .map_err(|e| {
                let err_msg = e.to_string();
                if err_msg.contains("proxy") || err_msg.contains("407") {
                    AppError::ProxyAuthRequired
                } else {
                    AppError::InternalError(anyhow::anyhow!("LLM error: {}", e))
                }
            })?;

        let (tx, rx) = mpsc::channel(10);
        let model_clone = resolved_model.clone();

        // Spawn a task to handle the streaming response
        tokio::spawn(async move {
            use futures::StreamExt;
            while let Some(chunk_result) = stream_result.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        if let Some(openai_chunk) = convert_chunk_to_openai(chunk, &model_clone) {
                            let chunk_str =
                                serde_json::to_string(&openai_chunk).unwrap_or_default();
                            if tx.send(Ok(Bytes::from(chunk_str))).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Stream error: {}", e);
                        break;
                    }
                }
            }
        });

        let stream = ReceiverStream::new(rx).map(|res| {
            res.map(|bytes| {
                let data = format!("data: {}\n\n", String::from_utf8_lossy(&bytes));
                Bytes::from(data)
            })
            .map_err(AppError::InternalError)
        });

        Ok(HttpResponse::Ok()
            .content_type("text/event-stream")
            .streaming(stream))
    } else {
        let provider = app_state.get_provider().await;

        // For non-streaming, we need to collect the stream
        let mut stream = provider
            .chat_stream(
                &internal_messages,
                &internal_tools,
                max_tokens,
                resolved_model.as_str(),
            )
            .await
            .map_err(|e| {
                let err_msg = e.to_string();
                if err_msg.contains("proxy") || err_msg.contains("407") {
                    AppError::ProxyAuthRequired
                } else {
                    AppError::InternalError(anyhow::anyhow!("LLM error: {}", e))
                }
            })?;

        // Collect all chunks
        use futures::StreamExt;
        let mut content = String::new();
        let mut tool_calls: Option<Vec<crate::agent::llm::api::models::ToolCall>> = None;

        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(crate::agent::llm::types::LLMChunk::Token(text)) => {
                    content.push_str(&text);
                }
                Ok(crate::agent::llm::types::LLMChunk::ToolCalls(calls)) => {
                    let converted_calls: Vec<crate::agent::llm::api::models::ToolCall> = calls
                        .into_iter()
                        .map(|tc| crate::agent::llm::api::models::ToolCall {
                            id: tc.id,
                            tool_type: tc.tool_type,
                            function: crate::agent::llm::api::models::FunctionCall {
                                name: tc.function.name,
                                arguments: tc.function.arguments,
                            },
                        })
                        .collect();
                    tool_calls = Some(converted_calls);
                }
                Ok(crate::agent::llm::types::LLMChunk::Done) => break,
                Err(e) => {
                    return Err(AppError::InternalError(anyhow::anyhow!(
                        "Stream error: {}",
                        e
                    )));
                }
            }
        }

        let response = build_completion_response(content, tool_calls, &resolved_model);
        Ok(HttpResponse::Ok().json(response))
    }
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_models).service(chat_completions);
}
