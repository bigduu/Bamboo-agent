use actix_web::{web, HttpResponse};
use futures::StreamExt;

use crate::{app_state::AppState, error::AppError};
use bamboo_llm::LLMRequestOptions;
use bamboo_metrics::types::ForwardStatus;

use super::super::helpers::now_unix_ts;
use super::output::{build_completed_response, build_output_items};
use super::usage::ResponsesUsageAccumulator;
use super::PreparedResponsesRequest;
use crate::handlers::llm_compat::usage::estimate_completion_tokens;

pub(super) async fn handle_non_streaming_response(
    app_state: web::Data<AppState>,
    prepared: PreparedResponsesRequest,
    forward_id: String,
) -> Result<HttpResponse, AppError> {
    let display_model = prepared
        .provider_name
        .as_ref()
        .map(|p| format!("{}/{}", p, prepared.resolved_model))
        .unwrap_or_else(|| prepared.resolved_model.clone());

    app_state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "openai.responses",
        display_model.clone(),
        false,
        chrono::Utc::now(),
    );

    let provider = match &prepared.provider_name {
        Some(name) => {
            let model_ref = bamboo_domain::ProviderModelRef::new(name, &prepared.resolved_model);
            app_state.get_provider_for_model_ref(&model_ref)?
        }
        None => app_state.get_provider().await,
    };
    let request_options = LLMRequestOptions {
        session_id: prepared.request_session_id.clone(),
        reasoning_effort: prepared.reasoning_effort,
        parallel_tool_calls: prepared.parallel_tool_calls,
        required_tool: None,
        responses: Some(prepared.responses_options.clone()),
        request_purpose: Some("openai_compat".to_string()),
        cache: None,
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
    let mut reasoning_content = String::new();
    // Merge partial tool-call fragments by index/id — a raw Vec shatters one
    // call into N broken output items (#525). Same as the streaming worker.
    let mut tool_calls = bamboo_agent_core::tools::ToolCallAccumulator::new();
    let mut upstream_response_id: Option<String> = None;
    let mut provider_usage = ResponsesUsageAccumulator::default();
    let mut saw_done = false;
    let mut raw_completed_response: Option<serde_json::Value> = None;

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(bamboo_llm::types::LLMChunk::ResponsesEvent { event_type, data }) => {
                if event_type == "response.completed" {
                    raw_completed_response = data.get("response").cloned();
                }
            }
            Ok(bamboo_llm::types::LLMChunk::ResponseId(response_id)) => {
                upstream_response_id = Some(response_id);
            }
            Ok(bamboo_llm::types::LLMChunk::Token(text)) => content.push_str(&text),
            // Native Responses events retain reasoning items structurally.
            // Never relabel provider-neutral reasoning as assistant output text.
            Ok(bamboo_llm::types::LLMChunk::ReasoningToken(text)) => {
                reasoning_content.push_str(&text)
            }
            Ok(bamboo_llm::types::LLMChunk::ToolCalls(calls)) => tool_calls.extend(calls),
            // Indexed variant: route fragments by provider index (#236/#525).
            Ok(bamboo_llm::types::LLMChunk::ToolCallsIndexed(calls)) => {
                tool_calls.extend_indexed(calls)
            }
            Ok(bamboo_llm::types::LLMChunk::Done) => {
                saw_done = true;
                break;
            }
            Ok(bamboo_llm::types::LLMChunk::ProviderUsage {
                input_tokens,
                output_tokens,
                total_tokens,
                reasoning_tokens,
                cache_read_input_tokens,
                cache_write_input_tokens,
                ..
            }) => provider_usage.record(
                input_tokens,
                output_tokens,
                total_tokens,
                reasoning_tokens,
                cache_read_input_tokens,
                cache_write_input_tokens,
            ),
            Ok(bamboo_llm::types::LLMChunk::TransportActivity)
            | Ok(bamboo_llm::types::LLMChunk::CacheUsage { .. })
            | Ok(bamboo_llm::types::LLMChunk::UsageSummary { .. })
            | Ok(bamboo_llm::types::LLMChunk::ReasoningSignature(_)) => {}
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

    if !saw_done {
        let message = "Stream ended before a protocol completion event";
        app_state.metrics_service.collector().forward_completed(
            forward_id,
            chrono::Utc::now(),
            None,
            ForwardStatus::Error,
            None,
            Some(message.to_string()),
        );
        return Err(AppError::InternalError(anyhow::anyhow!(message)));
    }

    let completion_tokens = estimate_completion_tokens(&content)
        .saturating_add(estimate_completion_tokens(&reasoning_content));
    let response_id =
        upstream_response_id.unwrap_or_else(|| format!("resp_{}", uuid::Uuid::new_v4()));
    let message_id = format!("msg_{}", uuid::Uuid::new_v4());
    let created_at = now_unix_ts();

    // Merged fragments whose name never arrived are dropped by finalize();
    // make that visible instead of silently losing a call attempt (#525).
    let fragment_groups = tool_calls.parts().len();
    let finalized_calls = tool_calls.finalize();
    if finalized_calls.len() < fragment_groups {
        tracing::warn!(
            dropped = fragment_groups - finalized_calls.len(),
            "Dropping incomplete streamed tool call(s) whose name never arrived"
        );
    }
    let output = build_output_items(&message_id, content, finalized_calls);
    let response_usage = provider_usage.response_usage();
    let (metrics_usage, metrics_details) =
        provider_usage.metrics_usage(prepared.estimated_prompt_tokens, completion_tokens);
    app_state
        .metrics_service
        .collector()
        .forward_completed_with_details(
            forward_id,
            chrono::Utc::now(),
            Some(200),
            ForwardStatus::Success,
            Some(metrics_usage),
            metrics_details,
            None,
        );

    if let Some(mut raw_response) = raw_completed_response {
        if raw_response
            .get("usage")
            .is_none_or(serde_json::Value::is_null)
        {
            if let Some(usage) = response_usage.as_ref() {
                raw_response["usage"] = serde_json::to_value(usage).unwrap_or_default();
            }
        }
        return Ok(HttpResponse::Ok().json(raw_response));
    }

    let resp = build_completed_response(
        response_id,
        created_at,
        display_model,
        output,
        response_usage,
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
