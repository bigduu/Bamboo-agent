use crate::error::AppError;

pub(super) fn map_provider_error(error: impl std::fmt::Display) -> AppError {
    let err_msg = error.to_string();
    if err_msg.contains("proxy") || err_msg.contains("407") {
        AppError::ProxyAuthRequired
    } else {
        AppError::InternalError(anyhow::anyhow!("LLM error: {}", err_msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::StatusCode;
    use actix_web::ResponseError;

    #[test]
    fn test_map_provider_error_proxy_keyword() {
        let error_msg = "proxy connection failed";
        let result = map_provider_error(error_msg);
        assert!(matches!(result, AppError::ProxyAuthRequired));
    }

    #[test]
    fn test_map_provider_error_407_status() {
        let error_msg = "HTTP 407 Proxy Authentication Required";
        let result = map_provider_error(error_msg);
        assert!(matches!(result, AppError::ProxyAuthRequired));
    }

    #[test]
    fn test_map_provider_error_generic_llm_error() {
        let error_msg = "Connection timeout";
        let result = map_provider_error(error_msg);
        assert!(matches!(result, AppError::InternalError(_)));
    }

    #[test]
    fn test_map_provider_error_empty_string() {
        let error_msg = "";
        let result = map_provider_error(error_msg);
        assert!(matches!(result, AppError::InternalError(_)));
    }

    #[test]
    fn test_map_provider_error_407_only() {
        let error_msg = "Error: 407";
        let result = map_provider_error(error_msg);
        assert!(matches!(result, AppError::ProxyAuthRequired));
    }

    #[test]
    fn test_map_provider_error_407_in_sentence() {
        let error_msg = "The server returned 407 status code";
        let result = map_provider_error(error_msg);
        assert!(matches!(result, AppError::ProxyAuthRequired));
    }

    #[test]
    fn test_map_provider_error_proxy_and_407() {
        let error_msg = "proxy error 407";
        let result = map_provider_error(error_msg);
        assert!(matches!(result, AppError::ProxyAuthRequired));
    }

    #[test]
    fn test_map_provider_error_other_status_codes() {
        let status_codes = vec!["400", "401", "403", "404", "500", "502", "503"];
        for code in status_codes {
            let error_msg = format!("HTTP {}", code);
            let result = map_provider_error(error_msg);
            assert!(matches!(result, AppError::InternalError(_)));
        }
    }

    #[test]
    fn test_map_provider_error_internal_error_status() {
        let error_msg = "Generic LLM error";
        let result = map_provider_error(error_msg);
        assert_eq!(result.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_map_provider_error_proxy_status() {
        let error_msg = "proxy required";
        let result = map_provider_error(error_msg);
        assert_eq!(result.status_code(), StatusCode::PRECONDITION_REQUIRED);
    }

    #[test]
    fn test_map_provider_error_with_struct() {
        struct TestError {
            message: String,
        }
        impl std::fmt::Display for TestError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.message)
            }
        }

        let error = TestError {
            message: "proxy authentication needed".to_string(),
        };
        let result = map_provider_error(error);
        assert!(matches!(result, AppError::ProxyAuthRequired));
    }

    #[test]
    fn test_map_provider_error_with_struct_non_proxy() {
        struct TestError {
            message: String,
        }
        impl std::fmt::Display for TestError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.message)
            }
        }

        let error = TestError {
            message: "timeout error".to_string(),
        };
        let result = map_provider_error(error);
        assert!(matches!(result, AppError::InternalError(_)));
    }

    #[test]
    fn test_map_provider_error_unicode() {
        let error_msg = "proxy 错误 🚫";
        let result = map_provider_error(error_msg);
        assert!(matches!(result, AppError::ProxyAuthRequired));
    }

    #[test]
    fn test_map_provider_error_multiline() {
        let error_msg = "Multiple errors:\nproxy failed\nconnection lost";
        let result = map_provider_error(error_msg);
        assert!(matches!(result, AppError::ProxyAuthRequired));
    }

    #[test]
    fn test_map_provider_error_long_message() {
        let error_msg = format!("proxy {}", "x".repeat(10000));
        let result = map_provider_error(&error_msg);
        assert!(matches!(result, AppError::ProxyAuthRequired));
    }
}
