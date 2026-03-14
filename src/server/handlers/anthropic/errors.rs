use crate::agent::llm::providers::anthropic::{
    api_types::{AnthropicErrorDetail, AnthropicErrorEnvelope},
    conversion::AnthropicConversionError,
};
use actix_web::{http::StatusCode, HttpResponse};

#[derive(Clone, Debug)]
pub(super) struct AnthropicError {
    pub(super) status: StatusCode,
    pub(super) error_type: String,
    pub(super) message: String,
}

impl AnthropicError {
    pub(super) fn new(status: StatusCode, error_type: &str, message: String) -> Self {
        Self {
            status,
            error_type: error_type.to_string(),
            message,
        }
    }
}

pub(super) fn anthropic_error_response(error: AnthropicError) -> HttpResponse {
    HttpResponse::build(error.status).json(AnthropicErrorEnvelope {
        error_type: "error".to_string(),
        error: AnthropicErrorDetail {
            error_type: error.error_type,
            message: error.message,
        },
    })
}

pub(super) fn map_conversion_error(error: AnthropicConversionError) -> AnthropicError {
    let status = StatusCode::from_u16(error.status).unwrap_or(StatusCode::BAD_GATEWAY);
    AnthropicError::new(status, &error.error_type, error.message)
}
