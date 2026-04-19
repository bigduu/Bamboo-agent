use bamboo_infrastructure::providers::anthropic::{
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

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::StatusCode;

    #[test]
    fn test_anthropic_error_new() {
        let error = AnthropicError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Test error message".to_string(),
        );
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.error_type, "invalid_request");
        assert_eq!(error.message, "Test error message");
    }

    #[test]
    fn test_anthropic_error_clone() {
        let error1 = AnthropicError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "Resource not found".to_string(),
        );
        let error2 = error1.clone();
        assert_eq!(error1.status, error2.status);
        assert_eq!(error1.error_type, error2.error_type);
        assert_eq!(error1.message, error2.message);
    }

    #[test]
    fn test_anthropic_error_debug() {
        let error = AnthropicError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Something went wrong".to_string(),
        );
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("AnthropicError"));
        assert!(debug_str.contains("internal_error"));
    }

    #[test]
    fn test_anthropic_error_with_various_status_codes() {
        let statuses = vec![
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
        ];

        for status in statuses {
            let error = AnthropicError::new(status, "test_error", format!("Error {}", status));
            assert_eq!(error.status, status);
        }
    }

    #[test]
    fn test_anthropic_error_empty_message() {
        let error = AnthropicError::new(StatusCode::OK, "test", String::new());
        assert_eq!(error.message, "");
    }

    #[test]
    fn test_anthropic_error_special_characters() {
        let error = AnthropicError::new(
            StatusCode::BAD_REQUEST,
            "error_with_special_chars",
            "Error: \n\t\"特殊字符\" emoji😀".to_string(),
        );
        assert!(error.message.contains("特殊字符"));
        assert!(error.message.contains("emoji😀"));
    }

    #[test]
    fn test_anthropic_error_long_message() {
        let long_message = "x".repeat(10000);
        let error =
            AnthropicError::new(StatusCode::BAD_REQUEST, "long_error", long_message.clone());
        assert_eq!(error.message.len(), 10000);
    }
}
