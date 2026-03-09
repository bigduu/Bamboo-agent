use crate::agent::core::budget::{HeuristicTokenCounter, TokenCounter};
use crate::agent::core::tools::ToolSchema;
use crate::agent::core::Message;
use crate::agent::llm::protocol::gemini::{
    GeminiCandidate, GeminiContent, GeminiFunctionCall, GeminiPart, GeminiRequest, GeminiResponse,
};
use crate::agent::llm::protocol::FromProvider;
use crate::agent::llm::LLMChunk;
use crate::agent::metrics::types::{ForwardStatus, TokenUsage as MetricsTokenUsage};
use crate::server::services::gemini_model_mapping_service::resolve_model;
use crate::server::{app_state::AppState, error::AppError};
use actix_web::{web, HttpResponse};
use anyhow::anyhow;
use bytes::Bytes;
use futures::StreamExt;
use serde_json::json;

/// Configure Gemini API routes
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("/models", web::get().to(list_models))
        .route(
            "/models/{model}:generateContent",
            web::post().to(generate_content),
        )
        .route(
            "/models/{model}:streamGenerateContent",
            web::post().to(stream_generate_content),
        );
}

fn estimate_prompt_tokens(messages: &[Message]) -> u64 {
    let counter = HeuristicTokenCounter::with_defaults();
    u64::from(counter.count_messages(messages))
}

fn estimate_completion_tokens(output_text: &str) -> u64 {
    let counter = HeuristicTokenCounter::with_defaults();
    u64::from(counter.count_text(output_text))
}

fn build_estimated_usage(prompt_tokens: u64, completion_tokens: u64) -> MetricsTokenUsage {
    MetricsTokenUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens: prompt_tokens.saturating_add(completion_tokens),
    }
}

/// Generate content (non-streaming)
pub async fn generate_content(
    path: web::Path<String>,
    request: web::Json<GeminiRequest>,
    state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let forward_id = uuid::Uuid::new_v4().to_string();
    let gemini_model = path.into_inner();

    // Resolve model mapping
    let resolution = {
        let config = state.config.read().await;
        resolve_model(&config.gemini_model_mapping, &gemini_model)
    };

    log::info!(
        "Gemini generateContent: requested='{}', mapped='{}'",
        gemini_model,
        if resolution.mapped_model.is_empty() {
            "(default)"
        } else {
            &resolution.mapped_model
        }
    );

    // 1. Convert Gemini format → Message
    let mut internal_messages = convert_gemini_to_messages(&request.contents)?;

    // 2. Convert tools if present
    let internal_tools = convert_gemini_tools(&request.tools)?;

    // 3. Call provider with mapped model
    let model_to_use = resolution.mapped_model.trim().to_string();
    if model_to_use.is_empty() {
        return Err(AppError::BadRequest(
            "No Gemini model mapping configured for requested model. Please configure gemini_model_mapping.".to_string(),
        ));
    }

    // Apply preflight hooks before forwarding upstream.
    let config_snapshot = state.config.read().await.clone();
    crate::server::message_hooks::apply_message_preflight_hooks(
        Some(state.as_ref()),
        &config_snapshot,
        model_to_use.as_str(),
        &mut internal_messages,
    )
    .await
    .map_err(|e| AppError::BadRequest(e.to_string()))?;
    let estimated_prompt_tokens = estimate_prompt_tokens(&internal_messages);

    // 4. Get provider
    state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "gemini.generate_content",
        model_to_use.clone(),
        false,
        chrono::Utc::now(),
    );
    let provider = state.get_provider().await;

    let mut stream = match provider
        .chat_stream(
            &internal_messages,
            &internal_tools,
            None,
            model_to_use.as_str(),
        )
        .await
    {
        Ok(stream) => stream,
        Err(e) => {
            state.metrics_service.collector().forward_completed(
                forward_id,
                chrono::Utc::now(),
                None,
                ForwardStatus::Error,
                None,
                Some(format!("Provider error: {}", e)),
            );
            return Err(AppError::InternalError(anyhow!("Provider error: {}", e)));
        }
    };

    // 5. Collect response
    let mut full_content = String::new();
    let mut tool_calls: Option<Vec<crate::agent::core::tools::ToolCall>> = None;

    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(LLMChunk::Token(token)) => full_content.push_str(&token),
            Ok(LLMChunk::Done) => break,
            Ok(LLMChunk::ToolCalls(calls)) => {
                tool_calls = Some(calls);
            }
            Err(e) => {
                state.metrics_service.collector().forward_completed(
                    forward_id.clone(),
                    chrono::Utc::now(),
                    None,
                    ForwardStatus::Error,
                    None,
                    Some(format!("Stream error: {}", e)),
                );
                return Err(AppError::InternalError(anyhow!("Stream error: {}", e)));
            }
        }
    }
    let completion_tokens = estimate_completion_tokens(&full_content);

    // 6. Convert back to Gemini format
    let mut parts = vec![GeminiPart {
        text: if full_content.is_empty() && tool_calls.is_none() {
            Some(String::new())
        } else if full_content.is_empty() {
            None
        } else {
            Some(full_content)
        },
        function_call: None,
        function_response: None,
    }];

    // Add tool calls as function_call parts
    if let Some(calls) = tool_calls {
        // Remove empty text part if we have tool calls and no text
        if parts[0].text.as_ref().is_none_or(|t| t.is_empty()) {
            parts.clear();
        }

        for tc in calls {
            let args: serde_json::Value = serde_json::from_str(&tc.function.arguments)
                .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

            parts.push(GeminiPart {
                text: None,
                function_call: Some(GeminiFunctionCall {
                    name: tc.function.name,
                    args,
                }),
                function_response: None,
            });
        }
    }

    let gemini_response = GeminiResponse {
        candidates: vec![GeminiCandidate {
            content: GeminiContent {
                role: "model".to_string(),
                parts,
            },
            finish_reason: Some("STOP".to_string()),
        }],
    };
    state.metrics_service.collector().forward_completed(
        forward_id,
        chrono::Utc::now(),
        Some(200),
        ForwardStatus::Success,
        Some(build_estimated_usage(
            estimated_prompt_tokens,
            completion_tokens,
        )),
        None,
    );

    Ok(HttpResponse::Ok().json(gemini_response))
}

/// Stream generate content
pub async fn stream_generate_content(
    path: web::Path<String>,
    request: web::Json<GeminiRequest>,
    state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let forward_id = uuid::Uuid::new_v4().to_string();
    let gemini_model = path.into_inner();

    // Resolve model mapping
    let resolution = {
        let config = state.config.read().await;
        resolve_model(&config.gemini_model_mapping, &gemini_model)
    };

    log::info!(
        "Gemini streamGenerateContent: requested='{}', mapped='{}'",
        gemini_model,
        if resolution.mapped_model.is_empty() {
            "(default)"
        } else {
            &resolution.mapped_model
        }
    );

    // 1. Convert Gemini format → Message
    let mut internal_messages = convert_gemini_to_messages(&request.contents)?;

    // 2. Convert tools if present
    let internal_tools = convert_gemini_tools(&request.tools)?;

    // 3. Get provider and create stream
    let model_to_use = resolution.mapped_model.trim().to_string();
    if model_to_use.is_empty() {
        return Err(AppError::BadRequest(
            "No Gemini model mapping configured for requested model. Please configure gemini_model_mapping.".to_string(),
        ));
    }

    // Apply preflight hooks before forwarding upstream.
    let config_snapshot = state.config.read().await.clone();
    crate::server::message_hooks::apply_message_preflight_hooks(
        Some(state.as_ref()),
        &config_snapshot,
        model_to_use.as_str(),
        &mut internal_messages,
    )
    .await
    .map_err(|e| AppError::BadRequest(e.to_string()))?;
    let estimated_prompt_tokens = estimate_prompt_tokens(&internal_messages);

    state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "gemini.stream_generate_content",
        model_to_use.clone(),
        true,
        chrono::Utc::now(),
    );

    let mut stream = match state
        .get_provider()
        .await
        .chat_stream(
            &internal_messages,
            &internal_tools,
            None,
            model_to_use.as_str(),
        )
        .await
    {
        Ok(stream) => stream,
        Err(e) => {
            state.metrics_service.collector().forward_completed(
                forward_id,
                chrono::Utc::now(),
                None,
                ForwardStatus::Error,
                None,
                Some(format!("Provider error: {}", e)),
            );
            return Err(AppError::InternalError(anyhow!("Provider error: {}", e)));
        }
    };

    let metrics = state.metrics_service.collector();
    let forward_id_clone = forward_id.clone();
    let estimated_prompt_tokens_clone = estimated_prompt_tokens;

    // 4. Create SSE stream
    let gemini_stream = async_stream::stream! {
        let mut had_error = false;
        let mut streamed_text = String::new();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(LLMChunk::Token(token)) => {
                    streamed_text.push_str(&token);
                    let gemini_chunk = GeminiResponse {
                        candidates: vec![GeminiCandidate {
                            content: GeminiContent {
                                role: "model".to_string(),
                                parts: vec![GeminiPart {
                                    text: Some(token),
                                    function_call: None,
                                    function_response: None,
                                }],
                            },
                            finish_reason: None,
                        }],
                    };

                    let json = match serde_json::to_string(&gemini_chunk) {
                        Ok(s) => s,
                        Err(e) => {
                            yield Err(actix_web::Error::from(std::io::Error::other(
                                format!("JSON error: {}", e),
                            )));
                            continue;
                        }
                    };

                    yield Ok::<_, actix_web::Error>(Bytes::from(format!("data: {}\n\n", json)));
                }
                Ok(LLMChunk::ToolCalls(tool_calls)) => {
                    // Convert tool calls to Gemini function_call parts
                    for tc in tool_calls {
                        let args: serde_json::Value = serde_json::from_str(&tc.function.arguments)
                            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

                        let gemini_chunk = GeminiResponse {
                            candidates: vec![GeminiCandidate {
                                content: GeminiContent {
                                    role: "model".to_string(),
                                    parts: vec![GeminiPart {
                                        text: None,
                                        function_call: Some(GeminiFunctionCall {
                                            name: tc.function.name,
                                            args,
                                        }),
                                        function_response: None,
                                    }],
                                },
                                finish_reason: None,
                            }],
                        };

                        let json = match serde_json::to_string(&gemini_chunk) {
                            Ok(s) => s,
                            Err(e) => {
                                yield Err(actix_web::Error::from(std::io::Error::other(
                                    format!("JSON error: {}", e),
                                )));
                                continue;
                            }
                        };

                        yield Ok::<_, actix_web::Error>(Bytes::from(format!("data: {}\n\n", json)));
                    }
                }
                Ok(LLMChunk::Done) => {
                    // Send final chunk
                    let final_chunk = GeminiResponse {
                        candidates: vec![GeminiCandidate {
                            content: GeminiContent {
                                role: "model".to_string(),
                                parts: vec![],
                            },
                            finish_reason: Some("STOP".to_string()),
                        }],
                    };
                    let json = serde_json::to_string(&final_chunk).unwrap_or_default();
                    yield Ok::<_, actix_web::Error>(Bytes::from(format!("data: {}\n\n", json)));
                    break;
                }
                Err(e) => {
                    had_error = true;
                    metrics.forward_completed(
                        forward_id_clone.clone(),
                        chrono::Utc::now(),
                        None,
                        ForwardStatus::Error,
                        None,
                        Some(format!("Stream error: {}", e)),
                    );
                    yield Err(actix_web::Error::from(std::io::Error::other(
                        format!("Stream error: {}", e),
                    )));
                    break;
                }
            }
        }

        if !had_error {
            let completion_tokens = estimate_completion_tokens(&streamed_text);
            metrics.forward_completed(
                forward_id_clone,
                chrono::Utc::now(),
                Some(200),
                ForwardStatus::Success,
                Some(build_estimated_usage(
                    estimated_prompt_tokens_clone,
                    completion_tokens,
                )),
                None,
            );
        }
    };

    Ok(HttpResponse::Ok()
        .content_type("text/event-stream")
        .streaming(gemini_stream))
}

/// List available models
pub async fn list_models(state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let forward_id = uuid::Uuid::new_v4().to_string();
    state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "gemini.models",
        "models",
        false,
        chrono::Utc::now(),
    );

    let provider = state.get_provider().await;

    let models = match provider.list_models().await {
        Ok(models) => {
            state.metrics_service.collector().forward_completed(
                forward_id,
                chrono::Utc::now(),
                Some(200),
                ForwardStatus::Success,
                None,
                None,
            );
            models
        }
        Err(e) => {
            state.metrics_service.collector().forward_completed(
                forward_id,
                chrono::Utc::now(),
                None,
                ForwardStatus::Error,
                None,
                Some(format!("Failed to list models: {}", e)),
            );
            return Err(AppError::InternalError(anyhow!(
                "Failed to list models: {}",
                e
            )));
        }
    };

    // Convert to Gemini format
    let gemini_models: Vec<_> = models
        .into_iter()
        .map(|name| {
            json!({
                "name": format!("models/{}", name),
                "displayName": name,
                "supportedGenerationMethods": [
                    "generateContent",
                    "streamGenerateContent"
                ],
            })
        })
        .collect();

    Ok(HttpResponse::Ok().json(json!({
        "models": gemini_models
    })))
}

/// Helper: Convert Gemini contents to internal Messages
fn convert_gemini_to_messages(contents: &[GeminiContent]) -> Result<Vec<Message>, AppError> {
    contents
        .iter()
        .map(|content| {
            Message::from_provider(content.clone()).map_err(|e| {
                AppError::ToolExecutionError(format!("Failed to convert message: {}", e))
            })
        })
        .collect()
}

/// Helper: Convert Gemini tools to internal ToolSchemas
fn convert_gemini_tools(
    tools: &Option<Vec<crate::agent::llm::protocol::gemini::GeminiTool>>,
) -> Result<Vec<ToolSchema>, AppError> {
    match tools {
        Some(tools) => {
            // Gemini groups all functions in one tool, we need to extract them
            let mut all_schemas = Vec::new();

            for tool in tools {
                for func_decl in &tool.function_declarations {
                    let schema = ToolSchema {
                        schema_type: "function".to_string(),
                        function: crate::agent::core::tools::FunctionSchema {
                            name: func_decl.name.clone(),
                            description: func_decl.description.clone().unwrap_or_default(),
                            parameters: func_decl.parameters.clone(),
                        },
                    };
                    all_schemas.push(schema);
                }
            }

            Ok(all_schemas)
        }
        None => Ok(vec![]),
    }
}
