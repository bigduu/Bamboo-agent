use actix_web::web;

use crate::{app_state::AppState, error::AppError};
use bamboo_llm::api::models::ChatCompletionRequest;

use super::PreparedChatRequest;
use crate::handlers::openai::{
    helpers::{convert_messages, convert_tools, parse_parallel_tool_calls, parse_reasoning_effort},
};
use crate::handlers::llm_compat::usage::estimate_prompt_tokens;

pub(super) async fn prepare_chat_request(
    app_state: &web::Data<AppState>,
    request: ChatCompletionRequest,
) -> Result<PreparedChatRequest, AppError> {
    let stream = request.stream.unwrap_or(false);
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

    let mut internal_messages = convert_messages(request.messages)?;
    let config_snapshot = app_state.config.read().await.clone();
    bamboo_engine::message_hooks::apply_message_preflight_hooks(
        Some(app_state.session_store.as_ref() as &dyn bamboo_agent_core::storage::AttachmentReader),
        &config_snapshot,
        resolved_model.as_str(),
        &mut internal_messages,
    )
    .await
    .map_err(|error| match error {
        bamboo_engine::message_hooks::HookError::Unsupported(message) => {
            AppError::BadRequest(message)
        }
        bamboo_engine::message_hooks::HookError::InvalidConfig(message) => {
            AppError::InternalError(anyhow::anyhow!(message))
        }
    })?;

    let internal_tools = convert_tools(request.tools)?;
    let max_tokens = request
        .parameters
        .get("max_tokens")
        .and_then(|value| value.as_u64())
        .map(|value| value as u32);
    let reasoning_effort = parse_reasoning_effort(&request.parameters);
    let parallel_tool_calls = parse_parallel_tool_calls(&request.parameters);
    let estimated_prompt_tokens = estimate_prompt_tokens(&internal_messages);

    Ok(PreparedChatRequest {
        stream,
        resolved_model,
        provider_name,
        internal_messages,
        internal_tools,
        max_tokens,
        reasoning_effort,
        parallel_tool_calls,
        estimated_prompt_tokens,
    })
}
