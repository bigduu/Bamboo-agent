use actix_web::{http::StatusCode, web, HttpResponse};
use async_stream::stream;
use bytes::Bytes;
use serde_json::json;

use crate::{app_state::AppState, error::AppError};
use bamboo_llm::LLMRequestOptions;
use bamboo_metrics::types::ForwardStatus;

use super::PreparedCompleteRequest;
use crate::handlers::anthropic::{
    conversion::convert_llm_chunk_to_openai,
    errors::{anthropic_error_response, AnthropicError},
    stream::{format_sse_data, map_completion_stream_chunk},
};
use crate::handlers::llm_compat::usage::{build_estimated_usage, estimate_completion_tokens};

pub(super) async fn handle_streaming_complete(
    app_state: web::Data<AppState>,
    prepared: PreparedCompleteRequest,
    forward_id: String,
) -> Result<HttpResponse, AppError> {
    let PreparedCompleteRequest {
        mapped_model,
        response_model,
        internal_messages,
        internal_tools,
        max_tokens,
        reasoning_effort,
        estimated_prompt_tokens,
    } = prepared;

    app_state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "anthropic.complete",
        mapped_model.clone(),
        true,
        chrono::Utc::now(),
    );

    let provider = app_state.get_provider().await;
    let stream_result = provider
        .chat_stream_with_options(
            &internal_messages,
            &internal_tools,
            max_tokens,
            mapped_model.as_str(),
            Some(&LLMRequestOptions {
                session_id: None,
                reasoning_effort,
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
                StatusCode::BAD_GATEWAY,
                "api_error",
                format!("Upstream API error: {error}"),
            )));
        }
    };

    let metrics = app_state.metrics_service.collector();
    let forward_id_clone = forward_id.clone();

    let stream = stream! {
        use futures::StreamExt;
        let mut had_error = false;
        let mut completion_text = String::new();

        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => {
                    if let bamboo_llm::types::LLMChunk::Token(text) = &chunk {
                        completion_text.push_str(text);
                    }
                    if let Some(openai_chunk) = convert_llm_chunk_to_openai(chunk, &response_model) {
                        let payload = map_completion_stream_chunk(&openai_chunk, &response_model);
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
                    let payload = format_sse_data(json!({
                        "type": "error",
                        "error": {
                            "type": "api_error",
                            "message": format!("Stream error: {}", error)
                        }
                    }));
                    yield Ok::<Bytes, AppError>(Bytes::from(payload));
                    yield Ok::<Bytes, AppError>(Bytes::from("data: [DONE]\n\n"));
                    break;
                }
            }
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
