use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};
use bamboo_engine::metrics::types::ForwardStatus;
use bamboo_infrastructure::api::models::{FunctionCall, ToolCall};
use bamboo_infrastructure::LLMRequestOptions;

use super::{map_provider_error, PreparedChatRequest};
use crate::handlers::openai::{
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
        provider_name,
        internal_messages,
        internal_tools,
        max_tokens,
        reasoning_effort,
        parallel_tool_calls,
        estimated_prompt_tokens,
        ..
    } = prepared;

    let display_model = provider_name
        .as_ref()
        .map(|p| format!("{}/{}", p, resolved_model))
        .unwrap_or_else(|| resolved_model.clone());

    app_state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "openai.chat_completions",
        display_model.clone(),
        false,
        chrono::Utc::now(),
    );

    let provider = match &provider_name {
        Some(name) => {
            let model_ref = bamboo_domain::ProviderModelRef::new(name, &resolved_model);
            app_state.get_provider_for_model_ref(&model_ref)?
        }
        None => app_state.get_provider_for_endpoint("openai").await?,
    };

    let mut stream = provider
        .chat_stream_with_options(
            &internal_messages,
            &internal_tools,
            max_tokens,
            resolved_model.as_str(),
            Some(&LLMRequestOptions {
                session_id: None,
                reasoning_effort,
                parallel_tool_calls,
                responses: None,
                request_purpose: Some("openai_compat".to_string()),
                cache: None,
            }),
        )
        .await
        .map_err(map_provider_error)?;

    use futures::StreamExt;
    let mut content = String::new();
    let mut tool_calls: Option<Vec<ToolCall>> = None;

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(bamboo_infrastructure::types::LLMChunk::ResponseId(_)) => {}
            Ok(bamboo_infrastructure::types::LLMChunk::Token(text)) => {
                content.push_str(&text);
            }
            Ok(bamboo_infrastructure::types::LLMChunk::ReasoningToken(_)) => {}
            Ok(bamboo_infrastructure::types::LLMChunk::ToolCalls(calls)) => {
                tool_calls = Some(convert_tool_calls(calls));
            }
            Ok(bamboo_infrastructure::types::LLMChunk::CacheUsage { .. }) => {}
            Ok(bamboo_infrastructure::types::LLMChunk::UsageSummary { .. }) => {}
            Ok(bamboo_infrastructure::types::LLMChunk::Done) => break,
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
    let response = build_completion_response(content, tool_calls, &display_model);
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

fn convert_tool_calls(calls: Vec<bamboo_agent_core::tools::ToolCall>) -> Vec<ToolCall> {
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
