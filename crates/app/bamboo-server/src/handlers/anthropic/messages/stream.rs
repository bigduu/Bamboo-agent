use actix_web::{web, HttpResponse};
use async_stream::stream;
use bytes::Bytes;
use serde_json::json;

use crate::{app_state::AppState, error::AppError};
use bamboo_llm::api::models::ChatCompletionRequest;
use bamboo_llm::LLMRequestOptions;
use bamboo_metrics::types::ForwardStatus;

use super::super::conversion::convert_llm_chunk_to_openai;
use super::super::errors::{anthropic_error_response, AnthropicError};
use super::super::stream::{format_sse_event, AnthropicStreamState};
use super::shared::{map_prepare_error, prepare_internal_execution};
use crate::handlers::llm_compat::usage::{build_estimated_usage, estimate_completion_tokens};

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
        let mut had_error = false;
        let mut completion_text = String::new();
        use futures::StreamExt;

        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => {
                    if let bamboo_llm::types::LLMChunk::Token(text) = &chunk {
                        completion_text.push_str(text);
                    }
                    // Convert LLMChunk to ChatCompletionStreamChunk.
                    if let Some(openai_chunk) = convert_llm_chunk_to_openai(chunk, &model) {
                        let payload = state.handle_chunk(&openai_chunk);
                        if !payload.is_empty() {
                            yield Ok::<Bytes, AppError>(Bytes::from(payload));
                        }
                    }
                }
                Err(error) => {
                    had_error = true;
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
                    break;
                }
            }
        }

        // Finish the stream.
        if !state.sent_message_stop {
            let payload = state.finish(None);
            yield Ok::<Bytes, AppError>(Bytes::from(payload));
        }
        yield Ok::<Bytes, AppError>(Bytes::from("data: [DONE]\n\n"));

        if !had_error {
            let completion_tokens = estimate_completion_tokens(&completion_text);
            metrics.forward_completed(
                forward_id_clone,
                chrono::Utc::now(),
                Some(200),
                ForwardStatus::Success,
                Some(build_estimated_usage(
                    estimated_prompt_tokens,
                    completion_tokens,
                )),
                None,
            );
        }
    };

    Ok(HttpResponse::Ok()
        .content_type("text/event-stream")
        .streaming(stream))
}
