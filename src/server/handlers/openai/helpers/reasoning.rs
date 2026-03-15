use std::collections::HashMap;

use serde_json::Value;

use crate::core::ReasoningEffort;

pub(super) fn parse_reasoning_effort(
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
