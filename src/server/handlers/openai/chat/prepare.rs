use actix_web::web;

use crate::agent::llm::api::models::ChatCompletionRequest;
use crate::server::{app_state::AppState, error::AppError};

use super::PreparedChatRequest;
use crate::server::handlers::openai::{
    helpers::{convert_messages, convert_tools},
    usage::estimate_prompt_tokens,
};

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
    let resolved_model = requested_model;

    let mut internal_messages = convert_messages(request.messages)?;
    let config_snapshot = app_state.config.read().await.clone();
    crate::server::message_hooks::apply_message_preflight_hooks(
        Some(app_state.as_ref()),
        &config_snapshot,
        resolved_model.as_str(),
        &mut internal_messages,
    )
    .await
    .map_err(|error| match error {
        crate::server::message_hooks::HookError::Unsupported(message) => {
            AppError::BadRequest(message)
        }
        crate::server::message_hooks::HookError::InvalidConfig(message) => {
            AppError::InternalError(anyhow::anyhow!(message))
        }
    })?;

    let internal_tools = convert_tools(request.tools)?;
    let max_tokens = request
        .parameters
        .get("max_tokens")
        .and_then(|value| value.as_u64())
        .map(|value| value as u32);
    let estimated_prompt_tokens = estimate_prompt_tokens(&internal_messages);

    Ok(PreparedChatRequest {
        stream,
        resolved_model,
        internal_messages,
        internal_tools,
        max_tokens,
        estimated_prompt_tokens,
    })
}
