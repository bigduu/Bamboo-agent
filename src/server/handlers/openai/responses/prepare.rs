use actix_web::web;

use crate::server::{app_state::AppState, error::AppError};

use super::super::helpers::{
    convert_messages, convert_tools, parse_parallel_tool_calls, parse_reasoning_effort,
    parse_responses_request_options, responses_input_to_chat_messages,
};
use super::super::types::ResponsesCreateRequest;
use super::super::usage::estimate_prompt_tokens;
use super::PreparedResponsesRequest;

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
    let resolved_model = requested_model;

    let mut openai_messages = Vec::new();
    if let Some(instructions) = request
        .instructions
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        openai_messages.push(crate::agent::llm::api::models::ChatMessage {
            role: crate::agent::llm::api::models::Role::System,
            content: crate::agent::llm::api::models::Content::Text(instructions.to_string()),
            phase: None,
            tool_calls: None,
            tool_call_id: None,
        });
    }

    let mut input_messages = responses_input_to_chat_messages(request.input)?;
    openai_messages.append(&mut input_messages);

    if openai_messages.is_empty() {
        return Err(AppError::BadRequest(
            "Missing `input`: at least one message is required".to_string(),
        ));
    }

    // Convert to internal messages (preserving multimodal parts), then apply preflight hooks.
    let mut internal_messages = convert_messages(openai_messages)?;
    let config_snapshot = app_state.config.read().await.clone();
    crate::server::message_hooks::apply_message_preflight_hooks(
        Some(app_state.as_ref()),
        &config_snapshot,
        resolved_model.as_str(),
        &mut internal_messages,
    )
    .await
    .map_err(|error| match error {
        crate::server::message_hooks::HookError::Unsupported(msg) => AppError::BadRequest(msg),
        crate::server::message_hooks::HookError::InvalidConfig(msg) => {
            AppError::InternalError(anyhow::anyhow!(msg))
        }
    })?;

    let internal_tools = convert_tools(request.tools)?;

    let max_tokens = request.max_output_tokens.or_else(|| {
        request
            .parameters
            .get("max_output_tokens")
            .and_then(|value| value.as_u64())
            .map(|value| value as u32)
    });
    let reasoning_effort = parse_reasoning_effort(&request.parameters);
    let parallel_tool_calls = parse_parallel_tool_calls(&request.parameters);
    let responses_options = parse_responses_request_options(&request.parameters);

    let estimated_prompt_tokens = estimate_prompt_tokens(&internal_messages);

    Ok(PreparedResponsesRequest {
        resolved_model,
        internal_messages,
        internal_tools,
        max_tokens,
        reasoning_effort,
        parallel_tool_calls,
        responses_options,
        estimated_prompt_tokens,
    })
}
