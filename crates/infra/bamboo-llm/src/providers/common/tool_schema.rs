use serde_json::{json, Map, Value};

const OPENAI_TOP_LEVEL_FORBIDDEN_KEYS: [&str; 5] = ["oneOf", "anyOf", "allOf", "not", "enum"];

/// Normalize tool parameter schema to satisfy OpenAI function parameter constraints.
///
/// OpenAI rejects top-level combinators (oneOf/anyOf/allOf/not/enum) for
/// function parameters. We keep runtime validation in tool execution paths and
/// make the schema transport-safe here.
pub fn sanitize_openai_function_parameters_schema(parameters: &Value) -> Value {
    let mut object = parameters
        .as_object()
        .cloned()
        .unwrap_or_else(Map::<String, Value>::new);

    object.insert("type".to_string(), json!("object"));

    for forbidden in OPENAI_TOP_LEVEL_FORBIDDEN_KEYS {
        object.remove(forbidden);
    }

    if !object.get("properties").is_some_and(Value::is_object) {
        object.insert("properties".to_string(), json!({}));
    }

    canonicalize_json_value(&Value::Object(object))
}

/// Canonical object ordering for provider-visible schemas; array order is semantic.
pub fn canonicalize_json_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            Value::Object(
                keys.into_iter()
                    .map(|key| (key.clone(), canonicalize_json_value(&object[key])))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json_value).collect()),
        _ => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::{canonicalize_json_value, sanitize_openai_function_parameters_schema};
    use serde_json::json;

    #[test]
    fn schema_bytes_are_independent_of_object_insertion_order() {
        let a: serde_json::Value = serde_json::from_str(r#"{"properties":{"z":{"type":"string","description":"z"},"a":{"type":"number"}},"required":["z","a"]}"#).unwrap();
        let b: serde_json::Value = serde_json::from_str(r#"{"required":["z","a"],"properties":{"a":{"type":"number"},"z":{"description":"z","type":"string"}}}"#).unwrap();
        assert_eq!(
            canonicalize_json_value(&a).to_string(),
            canonicalize_json_value(&b).to_string()
        );
        assert_eq!(canonicalize_json_value(&a)["required"], json!(["z", "a"]));
        assert_eq!(
            sanitize_openai_function_parameters_schema(&a).to_string(),
            sanitize_openai_function_parameters_schema(&b).to_string()
        );
    }

    #[test]
    fn sanitize_removes_forbidden_top_level_keywords() {
        let input = json!({
            "type": "object",
            "properties": {
                "file_path": { "type": "string" },
                "patch": { "type": "string" }
            },
            "required": ["file_path"],
            "oneOf": [
                { "required": ["patch"] },
                { "required": ["old_string", "new_string"] }
            ],
            "not": { "required": ["x"] }
        });

        let out = sanitize_openai_function_parameters_schema(&input);
        assert_eq!(out["type"], "object");
        assert!(out["oneOf"].is_null());
        assert!(out["not"].is_null());
        assert_eq!(out["required"], json!(["file_path"]));
        assert!(out["properties"].is_object());
    }

    #[test]
    fn sanitize_coerces_non_object_schema() {
        let input = json!({
            "type": "string"
        });

        let out = sanitize_openai_function_parameters_schema(&input);
        assert_eq!(out["type"], "object");
        assert_eq!(out["properties"], json!({}));
    }
}
