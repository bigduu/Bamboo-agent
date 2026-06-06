use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};
use bamboo_engine::metrics::types::ForwardStatus;
use bamboo_infrastructure::api::models::{ChatCompletionRequest, ChatCompletionResponse};
use bamboo_infrastructure::LLMRequestOptions;

use super::super::conversion::convert_messages_response;
use super::super::errors::anthropic_error_response;
use crate::handlers::llm_compat::usage::{build_estimated_usage, estimate_completion_tokens};
use super::shared::{map_prepare_error, map_tool_calls, prepare_internal_execution};

pub(super) async fn handle_non_streaming_messages(
    app_state: web::Data<AppState>,
    openai_request: ChatCompletionRequest,
    response_model: String,
    forward_id: String,
) -> Result<HttpResponse, AppError> {
    let provider = app_state.get_provider_for_endpoint("anthropic").await?;

    let prepared = match prepare_internal_execution(&app_state, &openai_request).await {
        Ok(prepared) => prepared,
        Err(err) => return map_prepare_error(err),
    };

    app_state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "anthropic.messages",
        openai_request.model.clone(),
        false,
        chrono::Utc::now(),
    );

    // Get completion by collecting the stream.
    let mut stream = match provider
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
        .await
    {
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
            return Err(AppError::InternalError(anyhow::anyhow!(
                "Upstream API error: {}",
                error
            )));
        }
    };

    // Collect stream into a response.
    use futures::StreamExt;
    let mut content = String::new();
    let mut tool_calls: Option<Vec<bamboo_infrastructure::api::models::ToolCall>> = None;

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(bamboo_infrastructure::types::LLMChunk::ResponseId(_)) => {}
            Ok(bamboo_infrastructure::types::LLMChunk::Token(text)) => {
                content.push_str(&text);
            }
            Ok(bamboo_infrastructure::types::LLMChunk::ReasoningToken(_)) => {}
            Ok(bamboo_infrastructure::types::LLMChunk::ToolCalls(calls)) => {
                tool_calls = Some(map_tool_calls(calls));
            }
            Ok(bamboo_infrastructure::types::LLMChunk::Done) => break,
            Ok(bamboo_infrastructure::types::LLMChunk::CacheUsage { .. })
            | Ok(bamboo_infrastructure::types::LLMChunk::UsageSummary { .. }) => {}
            Err(error) => {
                app_state.metrics_service.collector().forward_completed(
                    forward_id.clone(),
                    chrono::Utc::now(),
                    None,
                    ForwardStatus::Error,
                    None,
                    Some(format!("Stream error: {error}")),
                );
                return Err(AppError::InternalError(anyhow::anyhow!(
                    "Stream error: {}",
                    error
                )));
            }
        }
    }
    let completion_tokens = estimate_completion_tokens(&content);

    let completion = ChatCompletionResponse {
        id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
        object: Some("chat.completion".to_string()),
        created: Some(chrono::Utc::now().timestamp() as u64),
        model: Some(openai_request.model.clone()),
        choices: vec![bamboo_infrastructure::api::models::ResponseChoice {
            index: 0,
            message: bamboo_infrastructure::api::models::ChatMessage {
                role: bamboo_infrastructure::api::models::Role::Assistant,
                content: bamboo_infrastructure::api::models::Content::Text(content),
                phase: None,
                tool_calls,
                tool_call_id: None,
            },
            finish_reason: Some("stop".to_string()),
        }],
        usage: Some(bamboo_infrastructure::api::models::Usage {
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
        }),
        system_fingerprint: None,
    };

    let response = match convert_messages_response(completion, &response_model) {
        Ok(value) => value,
        Err(error) => {
            app_state.metrics_service.collector().forward_completed(
                forward_id,
                chrono::Utc::now(),
                None,
                ForwardStatus::Error,
                None,
                Some(error.message.clone()),
            );
            return Ok(anthropic_error_response(error));
        }
    };

    app_state.metrics_service.collector().forward_completed(
        forward_id,
        chrono::Utc::now(),
        Some(200),
        ForwardStatus::Success,
        Some(build_estimated_usage(
            prepared.estimated_prompt_tokens,
            completion_tokens,
        )),
        None,
    );

    Ok(HttpResponse::Ok().json(response))
}
