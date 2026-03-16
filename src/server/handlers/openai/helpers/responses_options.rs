use std::collections::HashMap;

use serde_json::Value;

use crate::agent::llm::provider::ResponsesRequestOptions;

fn parse_reasoning_summary(parameters: &HashMap<String, Value>) -> Option<String> {
    parameters
        .get("reasoning")
        .and_then(|value| value.get("summary"))
        .and_then(|value| value.as_str())
        .or_else(|| {
            parameters
                .get("reasoning_summary")
                .and_then(|value| value.as_str())
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn parse_include(parameters: &HashMap<String, Value>) -> Option<Vec<String>> {
    let value = parameters.get("include")?;
    let mut values = Vec::new();
    if let Some(single) = value
        .as_str()
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        values.push(single.to_string());
    } else if let Some(array) = value.as_array() {
        for entry in array {
            let Some(text) = entry
                .as_str()
                .map(str::trim)
                .filter(|item| !item.is_empty())
            else {
                continue;
            };
            if !values.iter().any(|existing| existing == text) {
                values.push(text.to_string());
            }
        }
    }

    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

fn parse_truncation(parameters: &HashMap<String, Value>) -> Option<String> {
    let raw = parameters
        .get("truncation")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let normalized = raw.to_ascii_lowercase();
    match normalized.as_str() {
        "auto" | "disabled" => Some(normalized),
        _ => None,
    }
}

pub(super) fn parse_responses_request_options(
    parameters: &HashMap<String, Value>,
) -> ResponsesRequestOptions {
    ResponsesRequestOptions {
        reasoning_summary: parse_reasoning_summary(parameters),
        include: parse_include(parameters),
        store: parameters.get("store").and_then(|value| value.as_bool()),
        truncation: parse_truncation(parameters),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_responses_request_options;

    #[test]
    fn parses_responses_options_from_parameters() {
        let params = serde_json::from_value(serde_json::json!({
            "reasoning": { "summary": "detailed" },
            "include": ["reasoning.encrypted_content"],
            "store": true,
            "truncation": "auto"
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.reasoning_summary.as_deref(), Some("detailed"));
        assert_eq!(
            parsed.include,
            Some(vec!["reasoning.encrypted_content".to_string()])
        );
        assert_eq!(parsed.store, Some(true));
        assert_eq!(parsed.truncation.as_deref(), Some("auto"));
    }

    #[test]
    fn ignores_invalid_truncation_value() {
        let params = serde_json::from_value(serde_json::json!({
            "truncation": "drop_oldest"
        }))
        .expect("valid params");
        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.truncation, None);
    }
}
