use crate::agent::llm::api::models::{
    ChatCompletionRequest, ChatCompletionResponse, ChatCompletionStreamChunk,
};
use crate::agent::llm::protocol::FromProvider;
use crate::agent::metrics::types::ForwardStatus;
use crate::server::{app_state::AppState, error::AppError};
use actix_web::{web, HttpResponse};
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::SystemTime};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Serialize)]
struct ListModelsResponse {
    success: bool,
    object: String,
    data: Vec<Model>,
}

#[derive(Serialize)]
struct Model {
    id: String,
    object: String,
    created: u64,
    owned_by: String,
    supported_endpoint_types: Vec<String>,
}

// ============================================================================
// OpenAI Responses API (v1/responses) - Minimal compatibility layer
// ============================================================================

/// OpenAI Responses API request (subset).
///
/// We intentionally accept a subset of the spec to support common SDK usage:
/// - `model`
/// - `input` (string or array of message-like objects)
/// - `instructions` (mapped to a system message)
/// - `tools` (OpenAI tool schema; reuses existing OpenAI-compatible tool model)
/// - `stream`
/// - `max_output_tokens` (mapped to provider max tokens)
#[derive(Debug, Deserialize, Clone, Default)]
pub struct ResponsesCreateRequest {
    model: String,
    #[serde(default)]
    input: serde_json::Value,
    #[serde(default)]
    instructions: Option<String>,
    #[serde(default)]
    tools: Option<Vec<crate::agent::llm::api::models::Tool>>,
    #[serde(default)]
    stream: Option<bool>,
    #[serde(default)]
    max_output_tokens: Option<u32>,
    #[serde(flatten)]
    parameters: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Clone)]
struct ResponsesUsage {
    #[serde(skip_serializing_if = "Option::is_none")]
    input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_tokens: Option<u64>,
}

#[derive(Debug, Serialize, Clone)]
struct ResponsesTextContent {
    #[serde(rename = "type")]
    content_type: String, // "output_text"
    text: String,
}

#[derive(Debug, Serialize, Clone)]
struct ResponsesMessageOutputItem {
    id: String,
    #[serde(rename = "type")]
    item_type: String, // "message"
    role: String, // "assistant"
    content: Vec<ResponsesTextContent>,
}

#[derive(Debug, Serialize, Clone)]
struct ResponsesFunctionCallOutputItem {
    id: String,
    #[serde(rename = "type")]
    item_type: String, // "function_call"
    call_id: String,
    name: String,
    arguments: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(untagged)]
enum ResponsesOutputItem {
    Message(ResponsesMessageOutputItem),
    FunctionCall(ResponsesFunctionCallOutputItem),
}

#[derive(Debug, Serialize, Clone)]
struct ResponsesCreateResponse {
    id: String,
    object: String, // "response"
    created_at: u64,
    model: String,
    status: String, // "completed" | "in_progress"
    output: Vec<ResponsesOutputItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<ResponsesUsage>,
}

#[derive(Debug, Serialize)]
struct ResponsesStreamEvent<T> {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    response: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    item_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta: Option<String>,
}

pub async fn get_models(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
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
            // Keep this endpoint best-effort so UIs/SDKs can call it even when the provider
            // is not configured yet. Provider config pages should surface real errors via
            // the dedicated Bamboo settings endpoints.
            log::warn!("Failed to fetch models (returning empty list): {}", e);
            return Ok(HttpResponse::Ok().json(ListModelsResponse {
                success: true,
                object: "list".to_string(),
                data: vec![],
            }));
        }
    };

    // Convert model IDs to OpenAI-compatible format
    let models: Vec<Model> = model_ids
        .into_iter()
        .map(|id| Model {
            id,
            object: "model".to_string(),
            created: 1626777600, // Stable timestamp for UI compatibility
            owned_by: "custom".to_string(),
            supported_endpoint_types: vec!["openai".to_string(), "anthropic".to_string()],
        })
        .collect();

    let response = ListModelsResponse {
        success: true,
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

fn now_unix_ts() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn sse_data(json: &str) -> Bytes {
    // Note: OpenAI-compatible streaming uses "data: <json>\n\n".
    Bytes::from(format!("data: {}\n\n", json))
}

fn role_str_to_openai_role(role: &str) -> crate::agent::llm::api::models::Role {
    use crate::agent::llm::api::models::Role as OpenAIRole;
    match role {
        "system" => OpenAIRole::System,
        "assistant" => OpenAIRole::Assistant,
        "tool" => OpenAIRole::Tool,
        _ => OpenAIRole::User,
    }
}

/// Convert a Responses API `input` payload into OpenAI-compatible chat messages.
///
/// Supported forms:
/// - string: treated as a single user message
/// - array: each element may be:
///   - { role, content: "..." }
///   - { type: "message", role, content: "..." | [{ type, text }...] }
fn responses_input_to_chat_messages(
    input: serde_json::Value,
) -> Result<Vec<crate::agent::llm::api::models::ChatMessage>, AppError> {
    use crate::agent::llm::api::models::{ChatMessage, Content, ContentPart, ImageUrl, Role};

    if input.is_null() {
        return Ok(vec![]);
    }

    if let Some(s) = input.as_str() {
        return Ok(vec![ChatMessage {
            role: Role::User,
            content: Content::Text(s.to_string()),
            tool_calls: None,
            tool_call_id: None,
        }]);
    }

    let Some(arr) = input.as_array() else {
        return Err(AppError::BadRequest(
            "Invalid `input`: expected string or array".to_string(),
        ));
    };

    let mut out = Vec::new();

    for item in arr {
        // Allow a raw string inside the array (treat as a user message).
        if let Some(s) = item.as_str() {
            out.push(ChatMessage {
                role: Role::User,
                content: Content::Text(s.to_string()),
                tool_calls: None,
                tool_call_id: None,
            });
            continue;
        }

        let Some(obj) = item.as_object() else {
            continue;
        };

        let role = obj
            .get("role")
            .and_then(|v| v.as_str())
            .map(role_str_to_openai_role)
            .unwrap_or(Role::User);

        // `content` can be a string or an array of parts.
        let content_value = obj
            .get("content")
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        let content = if let Some(s) = content_value.as_str() {
            Content::Text(s.to_string())
        } else if let Some(parts) = content_value.as_array() {
            let mut out_parts: Vec<ContentPart> = Vec::new();
            for p in parts {
                let Some(pobj) = p.as_object() else { continue };
                let ptype = pobj.get("type").and_then(|v| v.as_str()).unwrap_or("");
                match ptype {
                    // Responses API commonly uses `input_text`.
                    "input_text" | "text" => {
                        if let Some(text) = pobj.get("text").and_then(|v| v.as_str()) {
                            out_parts.push(ContentPart::Text {
                                text: text.to_string(),
                            });
                        }
                    }
                    // Best-effort mapping for images.
                    "input_image" | "image_url" => {
                        if let Some(url) = pobj
                            .get("image_url")
                            .and_then(|v| v.get("url").or(Some(v)))
                            .and_then(|v| v.as_str())
                        {
                            out_parts.push(ContentPart::ImageUrl {
                                image_url: ImageUrl {
                                    url: url.to_string(),
                                    detail: None,
                                },
                            });
                        }
                    }
                    _ => {}
                }
            }

            if out_parts.is_empty() {
                // If we can't interpret parts, fall back to empty text.
                Content::Text(String::new())
            } else {
                Content::Parts(out_parts)
            }
        } else {
            Content::Text(String::new())
        };

        out.push(ChatMessage {
            role,
            content,
            tool_calls: None,
            tool_call_id: None,
        });
    }

    Ok(out)
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

pub async fn chat_completions(
    app_state: web::Data<AppState>,
    req: web::Json<ChatCompletionRequest>,
) -> Result<HttpResponse, AppError> {
    let stream = req.stream.unwrap_or(false);
    let request = req.into_inner();
    let forward_id = uuid::Uuid::new_v4().to_string();
    let requested_model = request.model.trim().to_string();
    if requested_model.is_empty() || requested_model == "default" {
        return Err(AppError::BadRequest(
            "model is required (do not use 'default')".to_string(),
        ));
    }
    let resolved_model = requested_model;

    // Convert messages to internal format (preserving multimodal parts).
    let mut internal_messages = convert_messages(request.messages)?;
    // Apply preflight hooks (OCR / placeholder / error) before forwarding upstream.
    let config_snapshot = app_state.config.read().await.clone();
    crate::server::message_hooks::apply_message_preflight_hooks(
        Some(app_state.as_ref()),
        &config_snapshot,
        resolved_model.as_str(),
        &mut internal_messages,
    )
    .await
    .map_err(|e| match e {
        crate::server::message_hooks::HookError::Unsupported(msg) => AppError::BadRequest(msg),
        crate::server::message_hooks::HookError::InvalidConfig(msg) => {
            AppError::InternalError(anyhow::anyhow!(msg))
        }
    })?;

    let internal_tools = convert_tools(request.tools)?;
    let max_tokens = request
        .parameters
        .get("max_tokens")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32);

    if stream {
        app_state.metrics_service.collector().forward_started(
            forward_id.clone(),
            "openai.chat_completions",
            resolved_model.clone(),
            true,
            chrono::Utc::now(),
        );
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
        let metrics = app_state.metrics_service.collector();
        let forward_id_clone = forward_id.clone();

        // Spawn a task to handle the streaming response
        tokio::spawn(async move {
            use futures::StreamExt;
            let mut had_error = false;
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
                        had_error = true;
                        metrics.forward_completed(
                            forward_id_clone.clone(),
                            chrono::Utc::now(),
                            None,
                            ForwardStatus::Error,
                            None,
                            Some(e.to_string()),
                        );
                        break;
                    }
                }
            }

            // If we exit cleanly, mark success (best-effort; usage not available from stream).
            if !had_error {
                metrics.forward_completed(
                    forward_id_clone,
                    chrono::Utc::now(),
                    Some(200),
                    ForwardStatus::Success,
                    None,
                    None,
                );
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
        app_state.metrics_service.collector().forward_started(
            forward_id.clone(),
            "openai.chat_completions",
            resolved_model.clone(),
            false,
            chrono::Utc::now(),
        );
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
                    app_state.metrics_service.collector().forward_completed(
                        forward_id,
                        chrono::Utc::now(),
                        None,
                        ForwardStatus::Error,
                        None,
                        Some(e.to_string()),
                    );
                    return Err(AppError::InternalError(anyhow::anyhow!(
                        "Stream error: {}",
                        e
                    )));
                }
            }
        }

        let response = build_completion_response(content, tool_calls, &resolved_model);
        app_state.metrics_service.collector().forward_completed(
            forward_id,
            chrono::Utc::now(),
            Some(200),
            ForwardStatus::Success,
            None,
            None,
        );
        Ok(HttpResponse::Ok().json(response))
    }
}

pub async fn responses_create(
    app_state: web::Data<AppState>,
    req: web::Json<ResponsesCreateRequest>,
) -> Result<HttpResponse, AppError> {
    let request = req.into_inner();
    let stream = request.stream.unwrap_or(false);
    let forward_id = uuid::Uuid::new_v4().to_string();

    let requested_model = request.model.trim().to_string();
    if requested_model.is_empty() || requested_model == "default" {
        return Err(AppError::BadRequest(
            "model is required (do not use 'default')".to_string(),
        ));
    }
    let resolved_model = requested_model;

    let mut openai_messages = Vec::new();
    if let Some(instructions) = request
        .instructions
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        openai_messages.push(crate::agent::llm::api::models::ChatMessage {
            role: crate::agent::llm::api::models::Role::System,
            content: crate::agent::llm::api::models::Content::Text(instructions.to_string()),
            tool_calls: None,
            tool_call_id: None,
        });
    }

    let mut input_messages = responses_input_to_chat_messages(request.input)?;
    openai_messages.append(&mut input_messages);

    if openai_messages.is_empty() {
        return Err(AppError::BadRequest(
            "Missing `input`: at least one message is required".to_string(),
        ));
    }

    // Convert to internal messages (preserving multimodal parts), then apply preflight hooks.
    let mut internal_messages = convert_messages(openai_messages)?;
    let config_snapshot = app_state.config.read().await.clone();
    crate::server::message_hooks::apply_message_preflight_hooks(
        Some(app_state.as_ref()),
        &config_snapshot,
        resolved_model.as_str(),
        &mut internal_messages,
    )
    .await
    .map_err(|e| match e {
        crate::server::message_hooks::HookError::Unsupported(msg) => AppError::BadRequest(msg),
        crate::server::message_hooks::HookError::InvalidConfig(msg) => {
            AppError::InternalError(anyhow::anyhow!(msg))
        }
    })?;
    let internal_tools = convert_tools(request.tools)?;

    let max_tokens = request.max_output_tokens.or_else(|| {
        request
            .parameters
            .get("max_output_tokens")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
    });

    if stream {
        app_state.metrics_service.collector().forward_started(
            forward_id.clone(),
            "openai.responses",
            resolved_model.clone(),
            true,
            chrono::Utc::now(),
        );

        let provider = app_state.get_provider().await;
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

        let response_id = format!("resp_{}", uuid::Uuid::new_v4());
        let message_id = format!("msg_{}", uuid::Uuid::new_v4());
        let created_at = now_unix_ts();

        // Send an initial response.created event (in_progress)
        let created_event = ResponsesStreamEvent::<ResponsesCreateResponse> {
            event_type: "response.created".to_string(),
            response: Some(ResponsesCreateResponse {
                id: response_id.clone(),
                object: "response".to_string(),
                created_at,
                model: resolved_model.clone(),
                status: "in_progress".to_string(),
                output: vec![],
                usage: None,
            }),
            response_id: None,
            item_id: None,
            output_index: None,
            content_index: None,
            delta: None,
        };
        let _ = tx
            .send(Ok(sse_data(
                &serde_json::to_string(&created_event).unwrap_or_default(),
            )))
            .await;

        let metrics = app_state.metrics_service.collector();
        let forward_id_clone = forward_id.clone();
        let resolved_model_clone = resolved_model.clone();
        let response_id_clone = response_id.clone();
        let message_id_clone = message_id.clone();

        tokio::spawn(async move {
            use futures::StreamExt;

            let mut content = String::new();
            let mut tool_calls: Vec<crate::agent::core::tools::ToolCall> = Vec::new();

            while let Some(chunk_result) = stream_result.next().await {
                match chunk_result {
                    Ok(crate::agent::llm::types::LLMChunk::Token(text)) => {
                        if !text.is_empty() {
                            content.push_str(&text);
                            let ev = ResponsesStreamEvent::<ResponsesCreateResponse> {
                                event_type: "response.output_text.delta".to_string(),
                                response: None,
                                response_id: Some(response_id_clone.clone()),
                                item_id: Some(message_id_clone.clone()),
                                output_index: Some(0),
                                content_index: Some(0),
                                delta: Some(text),
                            };
                            let _ = tx
                                .send(Ok(sse_data(
                                    &serde_json::to_string(&ev).unwrap_or_default(),
                                )))
                                .await;
                        }
                    }
                    Ok(crate::agent::llm::types::LLMChunk::ToolCalls(calls)) => {
                        tool_calls.extend(calls);
                    }
                    Ok(crate::agent::llm::types::LLMChunk::Done) => break,
                    Err(e) => {
                        log::error!("Responses stream error: {}", e);
                        metrics.forward_completed(
                            forward_id_clone,
                            chrono::Utc::now(),
                            None,
                            ForwardStatus::Error,
                            None,
                            Some(e.to_string()),
                        );
                        return;
                    }
                }
            }

            // Final response.completed event with the assembled response object.
            let mut output: Vec<ResponsesOutputItem> = Vec::new();
            output.push(ResponsesOutputItem::Message(ResponsesMessageOutputItem {
                id: message_id_clone.clone(),
                item_type: "message".to_string(),
                role: "assistant".to_string(),
                content: vec![ResponsesTextContent {
                    content_type: "output_text".to_string(),
                    text: content,
                }],
            }));

            for (idx, tc) in tool_calls.into_iter().enumerate() {
                output.push(ResponsesOutputItem::FunctionCall(
                    ResponsesFunctionCallOutputItem {
                        id: format!("fc_{}_{}", message_id_clone, idx),
                        item_type: "function_call".to_string(),
                        call_id: tc.id,
                        name: tc.function.name,
                        arguments: tc.function.arguments,
                    },
                ));
            }

            let completed_response = ResponsesCreateResponse {
                id: response_id_clone.clone(),
                object: "response".to_string(),
                created_at,
                model: resolved_model_clone,
                status: "completed".to_string(),
                output,
                usage: None,
            };

            let completed_event = ResponsesStreamEvent::<ResponsesCreateResponse> {
                event_type: "response.completed".to_string(),
                response: Some(completed_response),
                response_id: None,
                item_id: None,
                output_index: None,
                content_index: None,
                delta: None,
            };

            let _ = tx
                .send(Ok(sse_data(
                    &serde_json::to_string(&completed_event).unwrap_or_default(),
                )))
                .await;

            metrics.forward_completed(
                forward_id_clone,
                chrono::Utc::now(),
                Some(200),
                ForwardStatus::Success,
                None,
                None,
            );
        });

        let stream = ReceiverStream::new(rx).map(|res| res.map_err(AppError::InternalError));

        Ok(HttpResponse::Ok()
            .content_type("text/event-stream")
            .streaming(stream))
    } else {
        app_state.metrics_service.collector().forward_started(
            forward_id.clone(),
            "openai.responses",
            resolved_model.clone(),
            false,
            chrono::Utc::now(),
        );

        let provider = app_state.get_provider().await;
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

        use futures::StreamExt;
        let mut content = String::new();
        let mut tool_calls: Vec<crate::agent::core::tools::ToolCall> = Vec::new();

        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(crate::agent::llm::types::LLMChunk::Token(text)) => content.push_str(&text),
                Ok(crate::agent::llm::types::LLMChunk::ToolCalls(calls)) => {
                    tool_calls.extend(calls)
                }
                Ok(crate::agent::llm::types::LLMChunk::Done) => break,
                Err(e) => {
                    app_state.metrics_service.collector().forward_completed(
                        forward_id,
                        chrono::Utc::now(),
                        None,
                        ForwardStatus::Error,
                        None,
                        Some(e.to_string()),
                    );
                    return Err(AppError::InternalError(anyhow::anyhow!(
                        "Stream error: {}",
                        e
                    )));
                }
            }
        }

        let response_id = format!("resp_{}", uuid::Uuid::new_v4());
        let message_id = format!("msg_{}", uuid::Uuid::new_v4());
        let created_at = now_unix_ts();

        let mut output: Vec<ResponsesOutputItem> = Vec::new();
        output.push(ResponsesOutputItem::Message(ResponsesMessageOutputItem {
            id: message_id.clone(),
            item_type: "message".to_string(),
            role: "assistant".to_string(),
            content: vec![ResponsesTextContent {
                content_type: "output_text".to_string(),
                text: content,
            }],
        }));

        for (idx, tc) in tool_calls.into_iter().enumerate() {
            output.push(ResponsesOutputItem::FunctionCall(
                ResponsesFunctionCallOutputItem {
                    id: format!("fc_{}_{}", message_id, idx),
                    item_type: "function_call".to_string(),
                    call_id: tc.id,
                    name: tc.function.name,
                    arguments: tc.function.arguments,
                },
            ));
        }

        let resp = ResponsesCreateResponse {
            id: response_id,
            object: "response".to_string(),
            created_at,
            model: resolved_model,
            status: "completed".to_string(),
            output,
            usage: None,
        };

        app_state.metrics_service.collector().forward_completed(
            forward_id,
            chrono::Utc::now(),
            Some(200),
            ForwardStatus::Success,
            None,
            None,
        );

        Ok(HttpResponse::Ok().json(resp))
    }
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("/models", web::get().to(get_models))
        .route("/chat/completions", web::post().to(chat_completions))
        .route("/responses", web::post().to(responses_create));
}

#[cfg(test)]
mod responses_tests {
    use super::*;

    #[test]
    fn responses_input_string_becomes_single_user_message() {
        let msgs = responses_input_to_chat_messages(serde_json::json!("hi")).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].role, crate::agent::llm::api::models::Role::User);
        match &msgs[0].content {
            crate::agent::llm::api::models::Content::Text(t) => assert_eq!(t, "hi"),
            _ => panic!("expected text content"),
        }
    }

    #[test]
    fn responses_input_array_parses_role_and_content_string() {
        let msgs = responses_input_to_chat_messages(serde_json::json!([
            { "role": "system", "content": "s" },
            { "role": "user", "content": "u" },
            { "role": "assistant", "content": "a" }
        ]))
        .unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0].role, crate::agent::llm::api::models::Role::System);
        assert_eq!(msgs[1].role, crate::agent::llm::api::models::Role::User);
        assert_eq!(
            msgs[2].role,
            crate::agent::llm::api::models::Role::Assistant
        );
    }

    #[test]
    fn responses_input_parts_support_input_text() {
        let msgs = responses_input_to_chat_messages(serde_json::json!([
            {
              "role": "user",
              "content": [{ "type": "input_text", "text": "hello" }]
            }
        ]))
        .unwrap();
        assert_eq!(msgs.len(), 1);
        match &msgs[0].content {
            crate::agent::llm::api::models::Content::Parts(parts) => {
                assert_eq!(parts.len(), 1);
                match &parts[0] {
                    crate::agent::llm::api::models::ContentPart::Text { text } => {
                        assert_eq!(text, "hello")
                    }
                    _ => panic!("expected text part"),
                }
            }
            _ => panic!("expected parts content"),
        }
    }
}
