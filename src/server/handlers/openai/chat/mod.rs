mod non_stream;
mod prepare;
mod stream;

use actix_web::{web, HttpResponse};

use crate::agent::core::{tools::ToolSchema, Message};
use crate::agent::llm::api::models::ChatCompletionRequest;
use crate::server::{app_state::AppState, error::AppError};

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
    pub(super) resolved_model: String,
    pub(super) internal_messages: Vec<Message>,
    pub(super) internal_tools: Vec<ToolSchema>,
    pub(super) max_tokens: Option<u32>,
    pub(super) estimated_prompt_tokens: u64,
}
