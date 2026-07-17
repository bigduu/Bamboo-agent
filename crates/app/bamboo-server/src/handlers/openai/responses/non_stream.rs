use actix_web::{web, HttpResponse};
use futures::StreamExt;

use crate::{app_state::AppState, error::AppError};
use bamboo_llm::LLMRequestOptions;
use bamboo_metrics::types::ForwardStatus;

use super::super::helpers::now_unix_ts;
use super::output::{build_completed_response, build_output_items};
use super::PreparedResponsesRequest;
use crate::handlers::llm_compat::usage::{build_estimated_usage, estimate_completion_tokens};

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
        session_id: None,
        reasoning_effort: prepared.reasoning_effort,
        parallel_tool_calls: prepared.parallel_tool_calls,
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
    // Merge partial tool-call fragments by index/id — a raw Vec shatters one
    // call into N broken output items (#525). Same as the streaming worker.
    let mut tool_calls = bamboo_agent_core::tools::ToolCallAccumulator::new();
    let mut upstream_response_id: Option<String> = None;

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(bamboo_llm::types::LLMChunk::ResponseId(response_id)) => {
                upstream_response_id = Some(response_id);
            }
            Ok(bamboo_llm::types::LLMChunk::Token(text)) => content.push_str(&text),
            // Keep parity with streaming behavior: expose reasoning narration as text.
            Ok(bamboo_llm::types::LLMChunk::ReasoningToken(text)) => content.push_str(&text),
            Ok(bamboo_llm::types::LLMChunk::ToolCalls(calls)) => tool_calls.extend(calls),
            // Indexed variant: route fragments by provider index (#236/#525).
            Ok(bamboo_llm::types::LLMChunk::ToolCallsIndexed(calls)) => {
                tool_calls.extend_indexed(calls)
            }
            Ok(bamboo_llm::types::LLMChunk::Done) => break,
            Ok(bamboo_llm::types::LLMChunk::CacheUsage { .. })
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

    let completion_tokens = estimate_completion_tokens(&content);
    let response_id =
        upstream_response_id.unwrap_or_else(|| format!("resp_{}", uuid::Uuid::new_v4()));
    let message_id = format!("msg_{}", uuid::Uuid::new_v4());
    let created_at = now_unix_ts();

    let output = build_output_items(&message_id, content, tool_calls.finalize());
    let resp = build_completed_response(response_id, created_at, display_model, output);

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
