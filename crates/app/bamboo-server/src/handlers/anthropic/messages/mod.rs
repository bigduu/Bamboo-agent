mod non_stream;
mod shared;
mod stream;

use actix_web::{http::StatusCode, web, HttpRequest, HttpResponse};

use crate::{app_state::AppState, error::AppError};
use bamboo_llm::api::models::StreamOptions;
use bamboo_llm::providers::anthropic::api_types::AnthropicMessagesRequest;

use super::conversion::convert_messages_request;
use super::errors::{anthropic_error_response, AnthropicError};
use super::resolution::resolve_model;

pub async fn messages(
    app_state: web::Data<AppState>,
    req: web::Json<AnthropicMessagesRequest>,
    http: HttpRequest,
) -> Result<HttpResponse, AppError> {
    if crate::handlers::llm_compat::activation::requested(&http) {
        // Validate this protocol's own DTO before ordinary conversion. The
        // neutral Native port never receives a vendor transcript or options.
        if req.model != "bamboo-agent"
            || !req.messages.is_empty()
            || req.system.is_some()
            || req.max_tokens.is_some()
            || req.temperature.is_some()
            || req.top_p.is_some()
            || req.top_k.is_some()
            || req.stop_sequences.is_some()
            || req.stream.unwrap_or(false)
            || req.tools.as_ref().is_some_and(|tools| !tools.is_empty())
            || req.tool_choice.is_some()
            || !req.extra.is_empty()
        {
            return Ok(crate::handlers::llm_compat::activation::error(
                StatusCode::BAD_REQUEST, "unsupported_bamboo_activation_body",
                "Receipt activation requires model=bamboo-agent, empty messages/tools and no system, max_tokens, generation or streaming options"));
        }
        return Ok(crate::handlers::llm_compat::activation::activate(app_state, http).await);
    }
    let stream = req.stream.unwrap_or(false);
    let request = req.into_inner();
    let response_model = request.model.clone();
    let forward_id = uuid::Uuid::new_v4().to_string();

    let resolution = {
        let config = app_state.config.read().await;
        resolve_model(&config.anthropic_model_mapping, &response_model)
    };

    let mut openai_request = match convert_messages_request(request) {
        Ok(request) => request,
        Err(err) => return Ok(anthropic_error_response(err)),
    };
    openai_request.model = resolution.mapped_model.clone();
    if openai_request.model.trim().is_empty() {
        return Ok(anthropic_error_response(AnthropicError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "model is required (no default model fallback)".to_string(),
        )));
    }

    // Enable usage tracking for streaming requests.
    if stream {
        openai_request.stream_options = Some(StreamOptions {
            include_usage: true,
        });
    }

    if stream {
        stream::handle_streaming_messages(
            app_state,
            openai_request,
            resolution.response_model,
            forward_id,
        )
        .await
    } else {
        non_stream::handle_non_streaming_messages(
            app_state,
            openai_request,
            resolution.response_model,
            forward_id,
        )
        .await
    }
}
