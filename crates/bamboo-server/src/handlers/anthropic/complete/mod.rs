mod non_stream;
mod stream;

use actix_web::{http::StatusCode, web, HttpResponse};

use bamboo_application_agent::{tools::ToolSchema, Message};
use bamboo_infrastructure_llm::{api::models::StreamOptions, providers::anthropic::api_types::AnthropicCompleteRequest};
use crate::{app_state::AppState, error::AppError};

use super::conversion::{convert_complete_request, convert_messages, convert_tools};
use super::errors::{anthropic_error_response, AnthropicError};
use super::resolution::resolve_model;
use super::usage::estimate_prompt_tokens;

pub async fn complete(
    app_state: web::Data<AppState>,
    req: web::Json<AnthropicCompleteRequest>,
) -> Result<HttpResponse, AppError> {
    let stream = req.stream.unwrap_or(false);
    let request = req.into_inner();
    let response_model = request.model.clone();
    let forward_id = uuid::Uuid::new_v4().to_string();

    let resolution = {
        let config = app_state.config.read().await;
        resolve_model(&config.anthropic_model_mapping, &response_model)
    };

    let mut openai_request = match convert_complete_request(request) {
        Ok(request) => request,
        Err(error) => return Ok(anthropic_error_response(error)),
    };
    openai_request.model = resolution.mapped_model.clone();

    if stream {
        openai_request.stream_options = Some(StreamOptions {
            include_usage: true,
        });
    }

    let mut internal_messages = convert_messages(openai_request.messages.clone())?;
    let config_snapshot = app_state.config.read().await.clone();
    if let Err(error) = crate::message_hooks::apply_message_preflight_hooks(
        Some(app_state.as_ref()),
        &config_snapshot,
        openai_request.model.as_str(),
        &mut internal_messages,
    )
    .await
    {
        return Ok(anthropic_error_response(AnthropicError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            error.to_string(),
        )));
    }

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

    let prepared = PreparedCompleteRequest {
        mapped_model: resolution.mapped_model,
        response_model: resolution.response_model,
        internal_messages,
        internal_tools,
        max_tokens,
        reasoning_effort,
        estimated_prompt_tokens,
    };

    if stream {
        stream::handle_streaming_complete(app_state, prepared, forward_id).await
    } else {
        non_stream::handle_non_streaming_complete(app_state, prepared, forward_id).await
    }
}

struct PreparedCompleteRequest {
    mapped_model: String,
    response_model: String,
    internal_messages: Vec<Message>,
    internal_tools: Vec<ToolSchema>,
    max_tokens: Option<u32>,
    reasoning_effort: Option<bamboo_shared_types::reasoning::ReasoningEffort>,
    estimated_prompt_tokens: u64,
}
