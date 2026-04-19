use actix_web::{http::StatusCode, web, HttpResponse};

use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::Message;
use bamboo_infrastructure::api::models::ChatCompletionRequest;
use crate::{app_state::AppState, error::AppError};

use super::super::conversion::{convert_messages, convert_tools};
use super::super::errors::{anthropic_error_response, AnthropicError};
use super::super::usage::estimate_prompt_tokens;

pub(super) struct PreparedInternalExecution {
    pub(super) internal_messages: Vec<Message>,
    pub(super) internal_tools: Vec<ToolSchema>,
    pub(super) max_tokens: Option<u32>,
    pub(super) reasoning_effort: Option<bamboo_domain::reasoning::ReasoningEffort>,
    pub(super) estimated_prompt_tokens: u64,
}

pub(super) enum PrepareInternalError {
    App(AppError),
    Hook(crate::message_hooks::HookError),
}

impl From<AppError> for PrepareInternalError {
    fn from(value: AppError) -> Self {
        Self::App(value)
    }
}

pub(super) async fn prepare_internal_execution(
    app_state: &web::Data<AppState>,
    openai_request: &ChatCompletionRequest,
) -> Result<PreparedInternalExecution, PrepareInternalError> {
    // Convert messages to internal format (preserving multimodal parts), then apply preflight hooks.
    let mut internal_messages = convert_messages(openai_request.messages.clone())?;
    let config_snapshot = app_state.config.read().await.clone();
    crate::message_hooks::apply_message_preflight_hooks(
        Some(app_state.as_ref()),
        &config_snapshot,
        openai_request.model.as_str(),
        &mut internal_messages,
    )
    .await
    .map_err(PrepareInternalError::Hook)?;

    let internal_tools = convert_tools(openai_request.tools.clone())?;
    let max_tokens = openai_request
        .parameters
        .get("max_tokens")
        .and_then(|value| value.as_u64())
        .map(|value| value as u32);
    let reasoning_effort = crate::handlers::openai::helpers::parse_reasoning_effort(
        &openai_request.parameters,
    );
    let estimated_prompt_tokens = estimate_prompt_tokens(&internal_messages);

    Ok(PreparedInternalExecution {
        internal_messages,
        internal_tools,
        max_tokens,
        reasoning_effort,
        estimated_prompt_tokens,
    })
}

pub(super) fn map_prepare_error(err: PrepareInternalError) -> Result<HttpResponse, AppError> {
    match err {
        PrepareInternalError::App(err) => Err(err),
        PrepareInternalError::Hook(error) => Ok(anthropic_error_response(AnthropicError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            error.to_string(),
        ))),
    }
}

pub(super) fn map_tool_calls(
    calls: Vec<bamboo_agent_core::tools::ToolCall>,
) -> Vec<bamboo_infrastructure::api::models::ToolCall> {
    calls
        .into_iter()
        .map(|tool_call| bamboo_infrastructure::api::models::ToolCall {
            id: tool_call.id,
            tool_type: tool_call.tool_type,
            function: bamboo_infrastructure::api::models::FunctionCall {
                name: tool_call.function.name,
                arguments: tool_call.function.arguments,
            },
        })
        .collect()
}
