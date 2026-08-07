//! Shared helpers for provider implementations.

pub mod model_fetcher;
pub mod openai_compat;
pub mod openai_responses;
pub mod request_overrides;
pub mod responses_debug;
pub mod sse;
pub mod stream_tool_accumulator;
pub mod tool_schema;

use bamboo_domain::ReasoningEffort;

use crate::provider::{LLMError, Result};

const MIN_NUMERIC_THINKING_TOKENS: u32 = 1_024;
const MIN_VISIBLE_OUTPUT_TOKENS: u32 = 1_024;

/// Map Bamboo effort levels to provider numeric thinking budgets while
/// preserving space for a visible answer inside the provider's total output
/// limit.
///
/// At roomy/unspecified limits the canonical targets remain 1K/4K/8K/16K.
/// Under a tighter limit, Xhigh may consume at most half of the total and Max
/// at most three quarters, with at least 1K left for visible output. If a Max
/// request is too small to remain strictly above Xhigh under those constraints,
/// no numeric budget is returned instead of silently collapsing the two levels.
pub(crate) fn bounded_thinking_budget(
    effort: ReasoningEffort,
    max_output_tokens: Option<u32>,
) -> Option<u32> {
    let target = match effort {
        ReasoningEffort::Low => return None,
        ReasoningEffort::Medium => 1_024,
        ReasoningEffort::High => 4_096,
        ReasoningEffort::Xhigh => 8_192,
        ReasoningEffort::Max => 16_384,
    };
    let Some(total) = max_output_tokens else {
        return Some(target);
    };

    let ratio_cap = match effort {
        ReasoningEffort::Low => unreachable!("low returned before budget calculation"),
        ReasoningEffort::Medium | ReasoningEffort::High | ReasoningEffort::Xhigh => total / 2,
        ReasoningEffort::Max => ((u64::from(total) * 3) / 4) as u32,
    };
    let available_after_output = total.saturating_sub(MIN_VISIBLE_OUTPUT_TOKENS);
    let budget = target.min(ratio_cap).min(available_after_output);
    if budget < MIN_NUMERIC_THINKING_TOKENS {
        return None;
    }

    if matches!(effort, ReasoningEffort::Max) {
        let xhigh_budget = 8_192.min(total / 2).min(available_after_output);
        if budget <= xhigh_budget {
            return None;
        }
    }

    Some(budget)
}

pub(crate) fn validate_max_thinking_budget(
    effort: Option<ReasoningEffort>,
    max_output_tokens: Option<u32>,
) -> Result<()> {
    let (Some(ReasoningEffort::Max), Some(total)) = (effort, max_output_tokens) else {
        return Ok(());
    };
    if bounded_thinking_budget(ReasoningEffort::Max, Some(total)).is_some() {
        return Ok(());
    }

    Err(LLMError::Api(format!(
        "max reasoning effort requires max_output_tokens of at least 2049 so its thinking budget can remain above xhigh while reserving 1024 visible output tokens; got {total}"
    )))
}

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

#[cfg(test)]
mod bounded_thinking_budget_tests {
    use super::{bounded_thinking_budget as budget, validate_max_thinking_budget};
    use bamboo_domain::ReasoningEffort;

    #[test]
    fn roomy_or_unspecified_limits_keep_the_canonical_targets() {
        assert_eq!(budget(ReasoningEffort::Xhigh, None), Some(8_192));
        assert_eq!(budget(ReasoningEffort::Max, None), Some(16_384));
        assert_eq!(budget(ReasoningEffort::Max, Some(32_000)), Some(16_384));
    }

    #[test]
    fn common_and_tight_limits_keep_max_distinct_and_reserve_output() {
        assert_eq!(budget(ReasoningEffort::Xhigh, Some(16_384)), Some(8_192));
        assert_eq!(budget(ReasoningEffort::Max, Some(16_384)), Some(12_288));
        assert_eq!(budget(ReasoningEffort::Xhigh, Some(8_320)), Some(4_160));
        assert_eq!(budget(ReasoningEffort::Max, Some(8_320)), Some(6_240));
        assert_eq!(budget(ReasoningEffort::Xhigh, Some(2_049)), Some(1_024));
        assert_eq!(budget(ReasoningEffort::Max, Some(2_049)), Some(1_025));
    }

    #[test]
    fn impossible_limits_disable_max_instead_of_collapsing_to_xhigh() {
        assert_eq!(budget(ReasoningEffort::Xhigh, Some(2_048)), Some(1_024));
        assert_eq!(budget(ReasoningEffort::Max, Some(2_048)), None);
        assert_eq!(budget(ReasoningEffort::Max, Some(1_024)), None);
        assert!(validate_max_thinking_budget(Some(ReasoningEffort::Max), Some(2_048)).is_err());
        assert!(validate_max_thinking_budget(Some(ReasoningEffort::Max), Some(2_049)).is_ok());
    }
}
