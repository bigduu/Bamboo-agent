use crate::{app_state::AppState, error::AppError};
use actix_web::{web, HttpResponse};
use bamboo_llm::api::models::{
    ChatCompletionResponse, ChatMessage, Content, FunctionCall, ResponseChoice, Role, ToolCall,
    Usage,
};
use bamboo_llm::LLMRequestOptions;
use bamboo_metrics::types::ForwardStatus;

use super::PreparedCompleteRequest;
use crate::handlers::anthropic::conversion::convert_complete_response;
use crate::handlers::llm_compat::usage;

pub(super) async fn handle_non_streaming_complete(
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
        false,
        chrono::Utc::now(),
    );

    let provider = app_state.get_provider().await;
    let mut stream = match provider
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

    use futures::StreamExt;
    let mut content = String::new();
    let mut tool_calls: Option<Vec<ToolCall>> = None;

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(bamboo_llm::types::LLMChunk::ResponseId(_)) => {}
            Ok(bamboo_llm::types::LLMChunk::Token(text)) => {
                content.push_str(&text);
            }
            Ok(bamboo_llm::types::LLMChunk::ReasoningToken(_)) => {}
            Ok(bamboo_llm::types::LLMChunk::ToolCalls(calls)) => {
                tool_calls = Some(convert_tool_calls(calls));
            }
            // Indexed variant: drop indices, same behavior. #236.
            Ok(bamboo_llm::types::LLMChunk::ToolCallsIndexed(calls)) => {
                tool_calls = Some(convert_tool_calls(
                    calls.into_iter().map(|(_, call)| call).collect(),
                ));
            }
            Ok(bamboo_llm::types::LLMChunk::Done) => break,
            Ok(bamboo_llm::types::LLMChunk::TransportActivity)
            | Ok(bamboo_llm::types::LLMChunk::ResponsesEvent { .. })
            | Ok(bamboo_llm::types::LLMChunk::ProviderTranscriptItem(_))
            | Ok(bamboo_llm::types::LLMChunk::CacheUsage { .. })
            | Ok(bamboo_llm::types::LLMChunk::ProviderUsage { .. })
            | Ok(bamboo_llm::types::LLMChunk::UsageSummary { .. })
            | Ok(bamboo_llm::types::LLMChunk::ReasoningSignature(_)) => {}
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

    let completion_tokens = usage::estimate_completion_tokens(&content);

    let completion = ChatCompletionResponse {
        id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
        object: Some("chat.completion".to_string()),
        created: Some(chrono::Utc::now().timestamp() as u64),
        model: Some(mapped_model),
        choices: vec![ResponseChoice {
            index: 0,
            message: ChatMessage {
                role: Role::Assistant,
                content: Content::Text(content),
                phase: None,
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
    };

    let response = match convert_complete_response(completion, &response_model) {
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
            return Ok(crate::handlers::anthropic::errors::anthropic_error_response(error));
        }
    };

    app_state.metrics_service.collector().forward_completed(
        forward_id,
        chrono::Utc::now(),
        Some(200),
        ForwardStatus::Success,
        Some(usage::build_estimated_usage(
            estimated_prompt_tokens,
            completion_tokens,
        )),
        None,
    );

    Ok(HttpResponse::Ok().json(response))
}

fn convert_tool_calls(
    calls: Vec<bamboo_agent_core::tools::ToolCall>,
) -> Vec<bamboo_llm::api::models::ToolCall> {
    calls
        .into_iter()
        .map(|tool_call| bamboo_llm::api::models::ToolCall {
            id: tool_call.id,
            tool_type: tool_call.tool_type,
            function: FunctionCall {
                name: tool_call.function.name,
                arguments: tool_call.function.arguments,
            },
        })
        .collect()
}
