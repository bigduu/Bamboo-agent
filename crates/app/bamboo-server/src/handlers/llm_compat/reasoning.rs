//! Reasoning-effort parsing over the OpenAI-canonical request `parameters`.
//!
//! All three compat surfaces translate through the OpenAI `ChatCompletionRequest`
//! as a canonical intermediate, so this parser is shared. It previously lived in
//! `openai/helpers/reasoning.rs`, which the anthropic handler reached into —
//! moving it here removes that cross-protocol coupling.

use std::collections::HashMap;

use serde_json::Value;

use bamboo_domain::reasoning::ReasoningEffort;

pub(crate) fn parse_reasoning_effort(
    parameters: &HashMap<String, Value>,
) -> Option<ReasoningEffort> {
    // OpenAI-style flat field: { "reasoning_effort": "medium" }
    if let Some(value) = parameters.get("reasoning_effort").and_then(|v| v.as_str()) {
        if let Some(effort) = ReasoningEffort::parse(value) {
            return Some(effort);
        }
    }

    // Responses-style object: { "reasoning": { "effort": "medium" } }
    if let Some(value) = parameters
        .get("reasoning")
        .and_then(|v| v.get("effort"))
        .and_then(|v| v.as_str())
    {
        if let Some(effort) = ReasoningEffort::parse(value) {
            return Some(effort);
        }
    }

    // Legacy/alternate: { "reasoning": "high" }
    if let Some(value) = parameters.get("reasoning").and_then(|v| v.as_str()) {
        if let Some(effort) = ReasoningEffort::parse(value) {
            return Some(effort);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::parse_reasoning_effort;
    use bamboo_domain::reasoning::ReasoningEffort;

    fn params(value: serde_json::Value) -> HashMap<String, serde_json::Value> {
        serde_json::from_value(value).expect("valid parameters")
    }

    #[test]
    fn parses_flat_reasoning_effort_field() {
        let parameters = params(json!({
            "reasoning_effort": " HIGH "
        }));

        assert_eq!(
            parse_reasoning_effort(&parameters),
            Some(ReasoningEffort::High)
        );
    }

    #[test]
    fn parses_nested_responses_reasoning_effort() {
        let parameters = params(json!({
            "reasoning": {
                "effort": "medium"
            }
        }));

        assert_eq!(
            parse_reasoning_effort(&parameters),
            Some(ReasoningEffort::Medium)
        );
    }

    #[test]
    fn parses_legacy_reasoning_string() {
        let parameters = params(json!({
            "reasoning": "xhigh"
        }));

        assert_eq!(
            parse_reasoning_effort(&parameters),
            Some(ReasoningEffort::Xhigh)
        );
    }

    #[test]
    fn prefers_flat_reasoning_effort_over_other_formats() {
        let parameters = params(json!({
            "reasoning_effort": "low",
            "reasoning": {
                "effort": "high"
            }
        }));

        assert_eq!(
            parse_reasoning_effort(&parameters),
            Some(ReasoningEffort::Low)
        );
    }

    #[test]
    fn returns_none_for_invalid_reasoning_effort() {
        let invalid = params(json!({
            "reasoning_effort": "extreme"
        }));
        let missing = params(json!({
            "temperature": 0.2
        }));

        assert_eq!(parse_reasoning_effort(&invalid), None);
        assert_eq!(parse_reasoning_effort(&missing), None);
    }
}
