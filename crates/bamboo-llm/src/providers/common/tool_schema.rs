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

    Value::Object(object)
}

#[cfg(test)]
mod tests {
    use super::sanitize_openai_function_parameters_schema;
    use serde_json::json;

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
