use std::collections::HashMap;

use serde_json::Value;

pub(super) fn parse_parallel_tool_calls(parameters: &HashMap<String, Value>) -> Option<bool> {
    parameters
        .get("parallel_tool_calls")
        .and_then(|value| value.as_bool())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::parse_parallel_tool_calls;

    fn params(value: serde_json::Value) -> HashMap<String, serde_json::Value> {
        serde_json::from_value(value).expect("valid parameters")
    }

    #[test]
    fn parses_parallel_tool_calls_true() {
        let parameters = params(json!({
            "parallel_tool_calls": true
        }));
        assert_eq!(parse_parallel_tool_calls(&parameters), Some(true));
    }

    #[test]
    fn parses_parallel_tool_calls_false() {
        let parameters = params(json!({
            "parallel_tool_calls": false
        }));
        assert_eq!(parse_parallel_tool_calls(&parameters), Some(false));
    }

    #[test]
    fn ignores_non_boolean_parallel_tool_calls() {
        let parameters = params(json!({
            "parallel_tool_calls": "true"
        }));
        assert_eq!(parse_parallel_tool_calls(&parameters), None);
    }
}
