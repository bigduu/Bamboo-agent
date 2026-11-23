use actix_web::{web, HttpResponse};
use async_stream::stream;
use bytes::Bytes;
use serde_json::{json, Value};

use crate::{app_state::AppState, error::AppError};
use bamboo_llm::api::models::ChatCompletionRequest;
use bamboo_llm::types::{LLMChunk, ProviderUsageReplacement};
use bamboo_llm::LLMRequestOptions;
use bamboo_metrics::types::{ForwardStatus, ForwardTokenDetails, TokenUsage};

use super::super::conversion::convert_llm_chunk_to_openai;
use super::super::errors::{anthropic_error_response, AnthropicError};
use super::super::stream::{format_sse_event, AnthropicStreamState};
use super::shared::{map_prepare_error, prepare_internal_execution};
use crate::handlers::llm_compat::usage::estimate_completion_tokens;

pub(super) async fn handle_streaming_messages(
    app_state: web::Data<AppState>,
    openai_request: ChatCompletionRequest,
    response_model: String,
    forward_id: String,
) -> Result<HttpResponse, AppError> {
    let provider = app_state.get_provider_for_endpoint("anthropic").await?;
    let model = response_model;

    let prepared = match prepare_internal_execution(&app_state, &openai_request).await {
        Ok(prepared) => prepared,
        Err(err) => return map_prepare_error(err),
    };

    app_state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "anthropic.messages",
        openai_request.model.clone(),
        true,
        chrono::Utc::now(),
    );

    // Start streaming.
    let stream_result = provider
        .chat_stream_with_options(
            &prepared.internal_messages,
            &prepared.internal_tools,
            prepared.max_tokens,
            openai_request.model.as_str(),
            Some(&LLMRequestOptions {
                session_id: None,
                reasoning_effort: prepared.reasoning_effort,
                parallel_tool_calls: None,
                required_tool: None,
                responses: None,
                request_purpose: Some("anthropic_compat".to_string()),
                cache: None,
            }),
        )
        .await;

    let mut stream = match stream_result {
        Ok(stream) => stream,
        Err(error) => {
            app_state.metrics_service.collector().forward_completed(
                forward_id.clone(),
                chrono::Utc::now(),
                None,
                ForwardStatus::Error,
                None,
                Some(format!("Upstream API error: {error}")),
            );
            return Ok(anthropic_error_response(AnthropicError::new(
                actix_web::http::StatusCode::BAD_GATEWAY,
                "api_error",
                format!("Upstream API error: {error}"),
            )));
        }
    };

    let metrics = app_state.metrics_service.collector();
    let forward_id_clone = forward_id.clone();
    let estimated_prompt_tokens = prepared.estimated_prompt_tokens;

    let stream = stream! {
        let mut state = AnthropicStreamState::new(model.clone());
        let mut provider_usage = ProviderUsageReplacement::default();
        let mut terminal_chunk = None;
        let mut completion_text = String::new();
        use futures::StreamExt;

        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(LLMChunk::Done) => {
                    terminal_chunk = convert_llm_chunk_to_openai(LLMChunk::Done, &model);
                    break;
                }
                Ok(chunk) => {
                    if let LLMChunk::Token(text) = &chunk {
                        completion_text.push_str(text);
                    }
                    capture_provider_usage(&mut provider_usage, &chunk);
                    // Convert LLMChunk to ChatCompletionStreamChunk.
                    if let Some(openai_chunk) = convert_llm_chunk_to_openai(chunk, &model) {
                        let payload = state.handle_chunk(&openai_chunk);
                        if !payload.is_empty() {
                            yield Ok::<Bytes, AppError>(Bytes::from(payload));
                        }
                    }
                }
                Err(error) => {
                    metrics.forward_completed(
                        forward_id_clone.clone(),
                        chrono::Utc::now(),
                        None,
                        ForwardStatus::Error,
                        None,
                        Some(format!("Stream error: {error}")),
                    );
                    let payload = format_sse_event(
                        "error",
                        json!({
                            "type": "error",
                            "error": {
                                "type": "api_error",
                                "message": format!("Stream error: {}", error)
                            }
                        }),
                    );
                    yield Ok::<Bytes, AppError>(Bytes::from(payload));
                    yield Ok::<Bytes, AppError>(Bytes::from("data: [DONE]\n\n"));
                    return;
                }
            }
        }

        let prompt_tokens = provider_usage.input_tokens.unwrap_or(estimated_prompt_tokens);
        let completion_tokens = provider_usage.output_tokens
            .unwrap_or_else(|| estimate_completion_tokens(&completion_text));
        let token_details = ForwardTokenDetails {
            cache_creation_input_tokens: provider_usage.cache_creation_input_tokens,
            cache_read_input_tokens: provider_usage.cache_read_input_tokens,
            cache_write_input_tokens: provider_usage.cache_write_input_tokens,
            reasoning_output_tokens: provider_usage.reasoning_tokens,
        };
        // Record before emitting the terminal event so closing the body after
        // message_stop does not leave the request pending in the metrics DB.
        metrics.forward_completed_with_details(
            forward_id_clone,
            chrono::Utc::now(),
            Some(200),
            ForwardStatus::Success,
            Some(TokenUsage {
                prompt_tokens,
                completion_tokens,
                total_tokens: provider_usage.total_tokens
                    .unwrap_or_else(|| prompt_tokens.saturating_add(completion_tokens)),
            }),
            (!token_details.is_empty()).then_some(token_details),
            None,
        );

        state.final_usage = Some(message_delta_usage(provider_usage));
        let payload = match terminal_chunk {
            Some(chunk) => state.handle_chunk(&chunk),
            None => state.finish(None),
        };
        yield Ok::<Bytes, AppError>(Bytes::from(payload));
        yield Ok::<Bytes, AppError>(Bytes::from("data: [DONE]\n\n"));
    };

    Ok(HttpResponse::Ok()
        .content_type("text/event-stream")
        .streaming(stream))
}

fn capture_provider_usage(usage: &mut ProviderUsageReplacement, chunk: &LLMChunk) {
    match *chunk {
        LLMChunk::ProviderUsage {
            input_tokens,
            output_tokens,
            total_tokens,
            reasoning_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            cache_write_input_tokens,
        } => {
            *usage = ProviderUsageReplacement {
                input_tokens,
                output_tokens,
                total_tokens,
                reasoning_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                cache_write_input_tokens,
                ..Default::default()
            };
        }
        LLMChunk::ProviderUsageReplace(replacement) => *usage = replacement,
        _ => {}
    }
}

fn message_delta_usage(usage: ProviderUsageReplacement) -> Value {
    let fresh_input = usage.input_tokens.and_then(|total| {
        total
            .checked_sub(usage.cache_read_input_tokens.unwrap_or(0))?
            .checked_sub(usage.cache_creation_input_tokens.unwrap_or(0))
    });
    Value::Object(
        [
            ("input_tokens", fresh_input),
            ("output_tokens", Some(usage.output_tokens.unwrap_or(0))),
            (
                "cache_creation_input_tokens",
                usage.cache_creation_input_tokens,
            ),
            ("cache_read_input_tokens", usage.cache_read_input_tokens),
        ]
        .into_iter()
        .filter_map(|(name, count)| count.map(|count| (name.to_owned(), Value::from(count))))
        .collect(),
    )
}
