mod non_stream;
mod prepare;
mod stream;

use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};
use bamboo_agent_core::{tools::ToolSchema, Message};
use bamboo_infrastructure::api::models::ChatCompletionRequest;

pub async fn chat_completions(
    app_state: web::Data<AppState>,
    req: web::Json<ChatCompletionRequest>,
) -> Result<HttpResponse, AppError> {
    let forward_id = uuid::Uuid::new_v4().to_string();
    let prepared = prepare::prepare_chat_request(&app_state, req.into_inner()).await?;

    if prepared.stream {
        stream::handle_streaming_chat(app_state, prepared, forward_id).await
    } else {
        non_stream::handle_non_streaming_chat(app_state, prepared, forward_id).await
    }
}

pub(super) fn map_provider_error(error: impl std::fmt::Display) -> AppError {
    let err_msg = error.to_string();
    if err_msg.contains("proxy") || err_msg.contains("407") {
        AppError::ProxyAuthRequired
    } else {
        AppError::InternalError(anyhow::anyhow!("LLM error: {}", err_msg))
    }
}

pub(super) struct PreparedChatRequest {
    pub(super) stream: bool,
    /// Raw model string — may be "provider/model" or plain "model".
    pub(super) resolved_model: String,
    /// Parsed provider name when model is in "provider/model" format.
    pub(super) provider_name: Option<String>,
    pub(super) internal_messages: Vec<Message>,
    pub(super) internal_tools: Vec<ToolSchema>,
    pub(super) max_tokens: Option<u32>,
    pub(super) reasoning_effort: Option<bamboo_domain::reasoning::ReasoningEffort>,
    pub(super) parallel_tool_calls: Option<bool>,
    pub(super) estimated_prompt_tokens: u64,
}
