//! Shared helpers for provider implementations.

pub mod model_fetcher;
pub mod openai_compat;
pub mod openai_responses;
pub mod request_overrides;
pub mod responses_debug;
pub mod sse;
pub mod stream_tool_accumulator;
pub mod tool_schema;

/// Whether an error response means the model/endpoint doesn't support the
/// reasoning/thinking parameter, so a retry with reasoning stripped is warranted.
///
/// Tightened over the original co-occurrence heuristic (#237, finding 5): it must
/// mention an actual reasoning token (`reasoning` / `reasoning_effort` /
/// `thinking`) AND a parameter-REJECTION phrasing. The old version counted a bare
/// `"unknown parameter"` as a reasoning mention (it fires for ANY rejected param,
/// e.g. `temperature`) and a bare `"invalid"` / `"unknown"` as the unsupported
/// signal — so an unrelated validation error was misclassified and the request
/// silently retried with reasoning stripped, yielding a lower-quality answer.
/// Bare `"invalid"` (usually a bad-VALUE error, not "unsupported") is excluded.
pub(crate) fn looks_like_reasoning_unsupported_error(
    status: reqwest::StatusCode,
    body: &str,
) -> bool {
    if !(status == 400 || status == 404 || status == 405 || status == 409 || status == 422) {
        return false;
    }
    let b = body.to_ascii_lowercase();
    // Union of every provider's reasoning/thinking parameter name. `thinking`
    // substring-covers Gemini's `thinkingBudget`/`thinkingConfig`; `budget_tokens`
    // is Anthropic's. A provider's error body won't contain another's token.
    // `reasoning` already substring-covers `reasoning_effort`; `thinking` covers
    // Gemini's `thinkingBudget`/`thinkingConfig`; `budget_tokens` is Anthropic's.
    let mentions_reasoning =
        b.contains("reasoning") || b.contains("thinking") || b.contains("budget_tokens");
    let indicates_unsupported = b.contains("unsupported")
        || b.contains("not supported")
        || b.contains("does not support")
        || b.contains("unknown parameter")
        || b.contains("unexpected parameter")
        || b.contains("unrecognized")
        || b.contains("invalid parameter");
    mentions_reasoning && indicates_unsupported
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
mod reasoning_heuristic_tests {
    use super::looks_like_reasoning_unsupported_error as f;
    use reqwest::StatusCode;

    #[test]
    fn fires_only_on_reasoning_parameter_rejection() {
        let bad = StatusCode::BAD_REQUEST;
        // Genuine "reasoning is unsupported" → strip-and-retry is right.
        assert!(f(bad, "reasoning_effort is not supported for this model"));
        assert!(f(bad, "Unknown parameter: reasoning_effort"));
        assert!(f(bad, "This model does not support reasoning"));
        assert!(f(bad, "Unrecognized request argument: thinking"));

        // Unrelated validation errors must NOT be misread as reasoning-unsupported
        // (the #237-5 false positives that silently degraded answers).
        assert!(!f(bad, "Unknown parameter: temperature")); // different param
        assert!(!f(
            bad,
            "Invalid value for 'reasoning_effort': must be one of low, medium, high"
        )); // bad VALUE, not unsupported
        assert!(!f(
            bad,
            "Invalid request: missing required field 'messages'"
        ));
        assert!(!f(bad, "The model gpt-x is unknown or unsupported")); // model, not reasoning

        // Only the listed 4xx statuses qualify.
        assert!(!f(
            StatusCode::INTERNAL_SERVER_ERROR,
            "reasoning not supported"
        ));
    }
}
