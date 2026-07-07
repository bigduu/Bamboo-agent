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
    let mentions_reasoning = b.contains("reasoning")
        || b.contains("reasoning_effort")
        || b.contains("thinking")
        || b.contains("budget_tokens");
    let indicates_unsupported = b.contains("unsupported")
        || b.contains("not supported")
        || b.contains("does not support")
        || b.contains("unknown parameter")
        || b.contains("unexpected parameter")
        || b.contains("unrecognized")
        || b.contains("invalid parameter");
    mentions_reasoning && indicates_unsupported
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
        assert!(!f(bad, "Invalid request: missing required field 'messages'"));
        assert!(!f(bad, "The model gpt-x is unknown or unsupported")); // model, not reasoning

        // Only the listed 4xx statuses qualify.
        assert!(!f(
            StatusCode::INTERNAL_SERVER_ERROR,
            "reasoning not supported"
        ));
    }
}
