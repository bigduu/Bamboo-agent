//! Shared helpers for provider implementations.

pub mod model_fetcher;
pub mod openai_compat;
pub mod openai_responses;
pub mod request_overrides;
pub mod responses_debug;
pub mod sse;
pub mod stream_tool_accumulator;
pub mod tool_schema;

use sha2::{Digest, Sha256};

use crate::provider::LLMError;

/// Bound caller/provider-controlled identifiers in operational logs while
/// retaining stable correlation. Request bodies and returned errors stay intact.
pub(crate) fn log_identity(value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"bamboo-provider-operational-log-v1\0");
    digest.update(value.as_bytes());
    hex::encode(&digest.finalize()[..8])
}

pub(crate) fn http_error_kind(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_request() {
        "request"
    } else if error.is_body() {
        "body"
    } else if error.is_decode() {
        "decode"
    } else if error.is_status() {
        "status"
    } else {
        "transport"
    }
}

pub(crate) fn llm_error_kind(error: &LLMError) -> &'static str {
    match error {
        LLMError::Http(error) => http_error_kind(error),
        LLMError::Json(_) => "json",
        LLMError::Stream(_) => "stream",
        LLMError::Api(_) => "api",
        LLMError::Auth(_) => "auth",
        LLMError::RequestFailed(_) => "request_failed",
        LLMError::Protocol(_) => "protocol",
    }
}

/// Whether an error response means the referenced `previous_response_id` does
/// not exist upstream (OpenAI's `previous_response_not_found`), so a single
/// retry WITHOUT the stateful continuation is warranted. Bamboo always sends
/// the full input array alongside the id, so dropping it loses no context.
///
/// This fires for ids that reference a `store=false` (never persisted) turn,
/// ids past the upstream retention window, ids minted under a different
/// key/org, and fabricated ids a compat-proxy client chained back. It must NOT
/// fire on other "not found" errors (e.g. an unknown model), so it requires an
/// explicit mention of the previous-response parameter.
pub(crate) fn looks_like_previous_response_not_found_error(
    status: reqwest::StatusCode,
    body: &str,
) -> bool {
    if !(status == 400 || status == 404) {
        return false;
    }
    let b = body.to_ascii_lowercase();
    if b.contains("previous_response_not_found") {
        return true;
    }
    let mentions_previous_response =
        b.contains("previous_response_id") || b.contains("previous response");
    let indicates_missing =
        b.contains("not found") || b.contains("not_found") || b.contains("does not exist");
    mentions_previous_response && indicates_missing
}

#[cfg(test)]
mod previous_response_not_found_tests {
    use super::looks_like_previous_response_not_found_error as f;
    use reqwest::StatusCode;

    #[test]
    fn fires_on_openai_previous_response_not_found_error() {
        let bad = StatusCode::BAD_REQUEST;
        // The exact OpenAI error shape (code = previous_response_not_found).
        assert!(f(
            bad,
            r#"{"error":{"message":"Previous response with id 'resp_123' not found.","type":"invalid_request_error","param":"previous_response_id","code":"previous_response_not_found"}}"#
        ));
        // Message-only variants, with and without the structured code.
        assert!(f(bad, "Previous response with id 'resp_x' not found."));
        assert!(f(bad, "previous_response_id 'resp_x' does not exist"));
        assert!(f(StatusCode::NOT_FOUND, "previous_response_not_found"));
    }

    #[test]
    fn does_not_fire_on_unrelated_errors() {
        let bad = StatusCode::BAD_REQUEST;
        // Other "not found" errors must not trigger a continuation-stripping retry.
        assert!(!f(bad, "The model `gpt-x` does not exist or is not found"));
        assert!(!f(bad, "Invalid value for 'previous_response_id'")); // bad VALUE, not missing
        assert!(!f(
            bad,
            "previous_response_id is not supported for this model"
        )); // Copilot-style unsupported, handled separately
            // Non-4xx statuses never qualify.
        assert!(!f(
            StatusCode::INTERNAL_SERVER_ERROR,
            "previous_response_not_found"
        ));
        assert!(!f(StatusCode::UNAUTHORIZED, "previous response not found"));
    }
}

#[cfg(test)]
pub(crate) mod log_privacy_tests;
