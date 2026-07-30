use actix_web::web;

use crate::{app_state::AppState, error::AppError};

use super::super::helpers::{
    convert_messages, convert_responses_tools, has_responses_prompt_cache_breakpoint,
    parse_parallel_tool_calls, parse_reasoning_effort, parse_responses_request_options,
    responses_input_to_chat_messages,
};
use super::super::types::ResponsesCreateRequest;
use super::PreparedResponsesRequest;
use crate::handlers::llm_compat::usage::{estimate_prompt_tokens, estimate_text_tokens};

pub(super) async fn prepare_request(
    app_state: &web::Data<AppState>,
    request: ResponsesCreateRequest,
) -> Result<PreparedResponsesRequest, AppError> {
    let requested_model = request.model.trim().to_string();
    if requested_model.is_empty() || requested_model == "default" {
        return Err(AppError::BadRequest(
            "model is required (do not use 'default')".to_string(),
        ));
    }

    // Parse "provider/model" format for cross-provider routing.
    let (provider_name, resolved_model) = match requested_model.split_once('/') {
        Some((p, m)) if !p.is_empty() && !m.is_empty() => (Some(p.to_string()), m.to_string()),
        _ => (None, requested_model),
    };

    let instructions = request
        .instructions
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let raw_input_with_cache_breakpoints =
        has_responses_prompt_cache_breakpoint(&request.input).then(|| request.input.clone());
    let input_messages = responses_input_to_chat_messages(request.input)?;

    if input_messages.is_empty() && instructions.is_none() {
        return Err(AppError::BadRequest(
            "Missing `input`: at least one message is required".to_string(),
        ));
    }

    // Convert to internal messages (preserving multimodal parts), then apply preflight hooks.
    let mut internal_messages = convert_messages(input_messages)?;
    let config_snapshot = app_state.config.read().await.clone();
    bamboo_engine::message_hooks::apply_message_preflight_hooks(
        Some(app_state.session_store.as_ref() as &dyn bamboo_agent_core::storage::AttachmentReader),
        &config_snapshot,
        resolved_model.as_str(),
        &mut internal_messages,
    )
    .await
    .map_err(|error| match error {
        bamboo_engine::message_hooks::HookError::Unsupported(msg) => AppError::BadRequest(msg),
        bamboo_engine::message_hooks::HookError::InvalidConfig(msg) => {
            AppError::InternalError(anyhow::anyhow!(msg))
        }
    })?;

    let internal_tools = convert_responses_tools(request.tools)?;

    let max_tokens = request.max_output_tokens.or_else(|| {
        request
            .parameters
            .get("max_output_tokens")
            .and_then(|value| value.as_u64())
            .map(|value| value as u32)
    });
    let reasoning_effort = parse_reasoning_effort(&request.parameters);
    let parallel_tool_calls = parse_parallel_tool_calls(&request.parameters);
    let mut responses_options = parse_responses_request_options(&request.parameters);
    responses_options.instructions = instructions.clone();
    responses_options.raw_input_with_cache_breakpoints = raw_input_with_cache_breakpoints;

    let estimated_prompt_tokens = estimate_prompt_tokens(&internal_messages).saturating_add(
        instructions
            .as_deref()
            .map(estimate_text_tokens)
            .unwrap_or(0),
    );

    Ok(PreparedResponsesRequest {
        resolved_model,
        provider_name,
        internal_messages,
        internal_tools,
        max_tokens,
        reasoning_effort,
        parallel_tool_calls,
        responses_options,
        estimated_prompt_tokens,
        request_session_id: None,
    })
}
