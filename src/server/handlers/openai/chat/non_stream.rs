use actix_web::{web, HttpResponse};

use crate::agent::llm::api::models::{FunctionCall, ToolCall};
use crate::agent::metrics::types::ForwardStatus;
use crate::server::{app_state::AppState, error::AppError};

use super::{map_provider_error, PreparedChatRequest};
use crate::server::handlers::openai::{
    helpers::build_completion_response,
    usage::{build_estimated_usage, estimate_completion_tokens},
};

pub(super) async fn handle_non_streaming_chat(
    app_state: web::Data<AppState>,
    prepared: PreparedChatRequest,
    forward_id: String,
) -> Result<HttpResponse, AppError> {
    let PreparedChatRequest {
        resolved_model,
        internal_messages,
        internal_tools,
        max_tokens,
        estimated_prompt_tokens,
        ..
    } = prepared;

    app_state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "openai.chat_completions",
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
        .map_err(map_provider_error)?;

    use futures::StreamExt;
    let mut content = String::new();
    let mut tool_calls: Option<Vec<ToolCall>> = None;

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(crate::agent::llm::types::LLMChunk::Token(text)) => {
                content.push_str(&text);
            }
            Ok(crate::agent::llm::types::LLMChunk::ToolCalls(calls)) => {
                tool_calls = Some(convert_tool_calls(calls));
            }
            Ok(crate::agent::llm::types::LLMChunk::Done) => break,
            Err(error) => {
                app_state.metrics_service.collector().forward_completed(
                    forward_id,
                    chrono::Utc::now(),
                    None,
                    ForwardStatus::Error,
                    None,
                    Some(error.to_string()),
                );
                return Err(AppError::InternalError(anyhow::anyhow!(
                    "Stream error: {}",
                    error
                )));
            }
        }
    }

    let completion_tokens = estimate_completion_tokens(&content);
    let response = build_completion_response(content, tool_calls, &resolved_model);
    let usage = build_estimated_usage(estimated_prompt_tokens, completion_tokens);
    app_state.metrics_service.collector().forward_completed(
        forward_id,
        chrono::Utc::now(),
        Some(200),
        ForwardStatus::Success,
        Some(usage),
        None,
    );
    Ok(HttpResponse::Ok().json(response))
}

fn convert_tool_calls(calls: Vec<crate::agent::core::tools::ToolCall>) -> Vec<ToolCall> {
    calls
        .into_iter()
        .map(|tool_call| ToolCall {
            id: tool_call.id,
            tool_type: tool_call.tool_type,
            function: FunctionCall {
                name: tool_call.function.name,
                arguments: tool_call.function.arguments,
            },
        })
        .collect()
}
