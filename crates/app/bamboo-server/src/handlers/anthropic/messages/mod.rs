mod non_stream;
mod shared;
mod stream;

use actix_web::{http::StatusCode, web, FromRequest, HttpRequest, HttpResponse};

use crate::{app_state::AppState, error::AppError};
use bamboo_llm::api::models::StreamOptions;
use bamboo_llm::providers::anthropic::api_types::AnthropicMessagesRequest;

use super::conversion::convert_messages_request;
use super::errors::{anthropic_error_response, AnthropicError};
use super::resolution::resolve_model;

/// Select the original ordinary JSON extractor or the explicit extension's
/// strict four-field schema. Shared activation never receives a vendor DTO.
pub enum MessagesHttpRequest {
    Activation(ReceiptActivationBody),
    Forward(AnthropicMessagesRequest),
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptActivationBody {
    model: String,
    messages: Vec<bamboo_llm::providers::anthropic::api_types::AnthropicMessage>,
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    tools: Vec<bamboo_llm::providers::anthropic::api_types::AnthropicTool>,
}

impl FromRequest for MessagesHttpRequest {
    type Error = actix_web::Error;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self, Self::Error>>>>;
    fn from_request(http: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        if crate::handlers::llm_compat::activation::requested(http) {
            let parsed = web::Json::<ReceiptActivationBody>::from_request(http, payload);
            Box::pin(async move {
                let body = parsed.await.map_err(|error| {
                    if error.as_response_error().status_code() != StatusCode::BAD_REQUEST {
                        return error;
                    }
                    actix_web::error::InternalError::from_response(error,
                        crate::handlers::llm_compat::activation::error(StatusCode::BAD_REQUEST,
                            "unsupported_bamboo_activation_body",
                            "Receipt activation allows only model=bamboo-agent, empty messages/tools and optional stream:false; null and other fields are rejected"))
                        .into()
                })?;
                Ok(Self::Activation(body.into_inner()))
            })
        } else {
            let parsed = web::Json::<AnthropicMessagesRequest>::from_request(http, payload);
            Box::pin(async move { Ok(Self::Forward(parsed.await?.into_inner())) })
        }
    }
}

pub async fn messages(
    app_state: web::Data<AppState>,
    req: MessagesHttpRequest,
    http: HttpRequest,
) -> Result<HttpResponse, AppError> {
    let request = match req {
        MessagesHttpRequest::Activation(body) => {
            if body.model != "bamboo-agent"
                || !body.messages.is_empty()
                || body.stream
                || !body.tools.is_empty()
            {
                return Ok(crate::handlers::llm_compat::activation::error(StatusCode::BAD_REQUEST,
                    "unsupported_bamboo_activation_body",
                    "Receipt activation requires model=bamboo-agent, empty messages/tools and optional stream:false"));
            }
            return Ok(crate::handlers::llm_compat::activation::activate(app_state, http).await);
        }
        MessagesHttpRequest::Forward(request) => request,
    };
    let stream = request.stream.unwrap_or(false);
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
