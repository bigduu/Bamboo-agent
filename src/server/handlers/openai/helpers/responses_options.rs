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

fn parse_text_verbosity(parameters: &HashMap<String, Value>) -> Option<String> {
    let raw = parameters
        .get("text")
        .and_then(|value| value.get("verbosity"))
        .and_then(|value| value.as_str())
        .or_else(|| {
            parameters
                .get("text_verbosity")
                .and_then(|value| value.as_str())
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())?;

    let normalized = raw.to_ascii_lowercase();
    match normalized.as_str() {
        "low" | "medium" | "high" => Some(normalized),
        _ => None,
    }
}

pub(super) fn parse_responses_request_options(
    parameters: &HashMap<String, Value>,
) -> ResponsesRequestOptions {
    let include = if parameters.contains_key("include") {
        parse_include(parameters)
    } else {
        Some(vec!["reasoning.encrypted_content".to_string()])
    };
    let text_verbosity =
        if parameters.contains_key("text") || parameters.contains_key("text_verbosity") {
            parse_text_verbosity(parameters)
        } else {
            Some("low".to_string())
        };

    ResponsesRequestOptions {
        reasoning_summary: parse_reasoning_summary(parameters),
        include,
        store: parameters.get("store").and_then(|value| value.as_bool()),
        previous_response_id: parameters
            .get("previous_response_id")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string),
        truncation: parse_truncation(parameters),
        text_verbosity,
    }
}

#[cfg(test)]
mod tests {
    use super::parse_responses_request_options;
    use serde_json::Value;
    use std::collections::HashMap;

    #[test]
    fn parses_responses_options_from_parameters() {
        let params = serde_json::from_value(serde_json::json!({
            "reasoning": { "summary": "detailed" },
            "include": ["reasoning.encrypted_content"],
            "store": true,
            "previous_response_id": "resp_123",
            "truncation": "auto",
            "text": { "verbosity": "high" }
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.reasoning_summary.as_deref(), Some("detailed"));
        assert_eq!(
            parsed.include,
            Some(vec!["reasoning.encrypted_content".to_string()])
        );
        assert_eq!(parsed.store, Some(true));
        assert_eq!(parsed.previous_response_id.as_deref(), Some("resp_123"));
        assert_eq!(parsed.truncation.as_deref(), Some("auto"));
        assert_eq!(parsed.text_verbosity.as_deref(), Some("high"));
    }

    #[test]
    fn ignores_invalid_truncation_value() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "truncation": "drop_oldest"
        }))
        .expect("valid params");
        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.truncation, None);
    }

    #[test]
    fn parses_reasoning_summary_from_legacy_field() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "reasoning_summary": "concise"
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.reasoning_summary.as_deref(), Some("concise"));
    }

    #[test]
    fn prefers_nested_reasoning_over_legacy() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "reasoning": { "summary": "detailed" },
            "reasoning_summary": "concise"
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.reasoning_summary.as_deref(), Some("detailed"));
    }

    #[test]
    fn trims_reasoning_summary() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "reasoning_summary": "  concise  "
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.reasoning_summary.as_deref(), Some("concise"));
    }

    #[test]
    fn rejects_empty_reasoning_summary() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "reasoning_summary": "   "
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.reasoning_summary, None);
    }

    #[test]
    fn trims_previous_response_id() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "previous_response_id": "  resp_456  "
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.previous_response_id.as_deref(), Some("resp_456"));
    }

    #[test]
    fn parses_single_include_string() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "include": "reasoning.encrypted_content"
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(
            parsed.include,
            Some(vec!["reasoning.encrypted_content".to_string()])
        );
    }

    #[test]
    fn parses_include_array_with_duplicates() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "include": ["file_search_call", "web_search_call", "file_search_call"]
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(
            parsed.include,
            Some(vec![
                "file_search_call".to_string(),
                "web_search_call".to_string()
            ])
        );
    }

    #[test]
    fn trims_include_entries() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "include": ["  file_search_call  "]
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.include, Some(vec!["file_search_call".to_string()]));
    }

    #[test]
    fn ignores_empty_include_entries() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "include": ["file_search_call", "  ", ""]
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.include, Some(vec!["file_search_call".to_string()]));
    }

    #[test]
    fn rejects_empty_include_array() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "include": []
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.include, None);
    }

    #[test]
    fn rejects_empty_include_string() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "include": "   "
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.include, None);
    }

    #[test]
    fn parses_truncation_disabled() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "truncation": "disabled"
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.truncation.as_deref(), Some("disabled"));
    }

    #[test]
    fn normalizes_truncation_case() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "truncation": "AUTO"
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.truncation.as_deref(), Some("auto"));
    }

    #[test]
    fn trims_truncation_value() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "truncation": "  auto  "
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.truncation.as_deref(), Some("auto"));
    }

    #[test]
    fn parses_store_false() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "store": false
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.store, Some(false));
    }

    #[test]
    fn parses_text_verbosity_from_legacy_field() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "text_verbosity": "MEDIUM"
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.text_verbosity.as_deref(), Some("medium"));
    }

    #[test]
    fn ignores_invalid_text_verbosity() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "text": { "verbosity": "verbose" }
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.text_verbosity, None);
    }

    #[test]
    fn handles_missing_all_fields() {
        let params: HashMap<String, Value> = HashMap::new();
        let parsed = parse_responses_request_options(&params);

        assert_eq!(parsed.reasoning_summary, None);
        assert_eq!(
            parsed.include,
            Some(vec!["reasoning.encrypted_content".to_string()])
        );
        assert_eq!(parsed.store, None);
        assert_eq!(parsed.truncation, None);
        assert_eq!(parsed.text_verbosity.as_deref(), Some("low"));
    }

    #[test]
    fn handles_mixed_valid_and_invalid_fields() {
        let params: HashMap<String, Value> = serde_json::from_value(serde_json::json!({
            "reasoning_summary": "",
            "include": ["file_search_call", ""],
            "store": true,
            "truncation": "invalid",
            "text": { "verbosity": "invalid" }
        }))
        .expect("valid params");

        let parsed = parse_responses_request_options(&params);
        assert_eq!(parsed.reasoning_summary, None);
        assert_eq!(parsed.include, Some(vec!["file_search_call".to_string()]));
        assert_eq!(parsed.store, Some(true));
        assert_eq!(parsed.truncation, None);
        assert_eq!(parsed.text_verbosity, None);
    }
}
