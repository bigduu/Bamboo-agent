use actix_web::{web, HttpResponse};
use futures::StreamExt;

use crate::agent::llm::LLMRequestOptions;
use crate::agent::metrics::types::ForwardStatus;
use crate::server::{app_state::AppState, error::AppError};

use super::super::helpers::now_unix_ts;
use super::super::usage::{build_estimated_usage, estimate_completion_tokens};
use super::output::{build_completed_response, build_output_items};
use super::PreparedResponsesRequest;

pub(super) async fn handle_non_streaming_response(
    app_state: web::Data<AppState>,
    prepared: PreparedResponsesRequest,
    forward_id: String,
) -> Result<HttpResponse, AppError> {
    app_state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "openai.responses",
        prepared.resolved_model.clone(),
        false,
        chrono::Utc::now(),
    );

    let provider = app_state.get_provider().await;
    let request_options = LLMRequestOptions {
        reasoning_effort: prepared.reasoning_effort,
        parallel_tool_calls: prepared.parallel_tool_calls,
        responses: Some(prepared.responses_options.clone()),
    };
    let mut stream = provider
        .chat_stream_with_options(
            &prepared.internal_messages,
            &prepared.internal_tools,
            prepared.max_tokens,
            prepared.resolved_model.as_str(),
            Some(&request_options),
        )
        .await
        .map_err(map_provider_error)?;

    let mut content = String::new();
    let mut tool_calls: Vec<crate::agent::core::tools::ToolCall> = Vec::new();
    let mut upstream_response_id: Option<String> = None;

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(crate::agent::llm::types::LLMChunk::ResponseId(response_id)) => {
                upstream_response_id = Some(response_id);
            }
            Ok(crate::agent::llm::types::LLMChunk::Token(text)) => content.push_str(&text),
            Ok(crate::agent::llm::types::LLMChunk::ReasoningToken(_)) => {}
            Ok(crate::agent::llm::types::LLMChunk::ToolCalls(calls)) => tool_calls.extend(calls),
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
    let response_id =
        upstream_response_id.unwrap_or_else(|| format!("resp_{}", uuid::Uuid::new_v4()));
    let message_id = format!("msg_{}", uuid::Uuid::new_v4());
    let created_at = now_unix_ts();

    let output = build_output_items(&message_id, content, tool_calls);
    let resp = build_completed_response(response_id, created_at, prepared.resolved_model, output);

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

    Ok(HttpResponse::Ok().json(resp))
}

fn map_provider_error(error: impl std::fmt::Display) -> AppError {
    let err_msg = error.to_string();
    if err_msg.contains("proxy") || err_msg.contains("407") {
        AppError::ProxyAuthRequired
    } else {
        AppError::InternalError(anyhow::anyhow!("LLM error: {}", error))
    }
}
