use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value};

const OPENAI_TOP_LEVEL_FORBIDDEN_KEYS: [&str; 5] = ["oneOf", "anyOf", "allOf", "not", "enum"];
const ANTHROPIC_ROOT_COMBINATORS: [&str; 3] = ["allOf", "oneOf", "anyOf"];

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

/// Anthropic rejects composition keywords at the root of a custom tool schema.
/// Keep the declared fields visible to the model, leaving cross-field validation
/// to the tool's execution path and its original input schema.
pub(crate) fn sanitize_anthropic_tool_input_schema(parameters: &Value) -> Value {
    let Some(root) = parameters.as_object().filter(|root| {
        ANTHROPIC_ROOT_COMBINATORS
            .iter()
            .any(|keyword| root.contains_key(*keyword))
    }) else {
        return canonicalize_json_value(parameters);
    };

    let (properties, required) = projected_object_fields(parameters);
    let mut schema = root.clone();
    for keyword in ANTHROPIC_ROOT_COMBINATORS {
        schema.remove(keyword);
    }
    schema.insert("type".into(), json!("object"));
    schema.insert("properties".into(), Value::Object(properties));

    // Keep existing required fields in order, then add those implied by composition.
    if !required.is_empty() {
        let mut fields = schema
            .get("required")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for name in required {
            let field = Value::String(name);
            if !fields.contains(&field) {
                fields.push(field);
            }
        }
        schema.insert("required".into(), Value::Array(fields));
    }
    canonicalize_json_value(&Value::Object(schema))
}

fn projected_object_fields(schema: &Value) -> (Map<String, Value>, BTreeSet<String>) {
    let mut properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut required = schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();

    for keyword in ANTHROPIC_ROOT_COMBINATORS {
        let Some(branches) = schema.get(keyword).and_then(Value::as_array) else {
            continue;
        };
        let mut branch_properties = BTreeMap::<String, Vec<Value>>::new();
        let mut branch_required: Option<BTreeSet<String>> = None;
        for branch in branches {
            let (fields, mandatory) = projected_object_fields(branch);
            for (name, field) in fields {
                // An explicit parent definition takes precedence. A branch that
                // forbids a field in a choice contributes no value for that field.
                if properties.contains_key(&name) || (keyword != "allOf" && field == false) {
                    continue;
                }
                let variants = branch_properties.entry(name).or_default();
                if !variants.contains(&field) {
                    variants.push(field);
                }
            }
            match &mut branch_required {
                Some(fields) if keyword == "allOf" => fields.extend(mandatory),
                Some(fields) => fields.retain(|name| mandatory.contains(name)),
                None => branch_required = Some(mandatory),
            }
        }
        required.extend(branch_required.into_iter().flatten());
        for (name, mut variants) in branch_properties {
            let field = if variants.len() == 1 {
                variants.pop().expect("one field definition")
            } else {
                let composition = if keyword == "allOf" { "allOf" } else { "anyOf" };
                json!({ (composition): variants })
            };
            properties.insert(name, field);
        }
    }

    (properties, required)
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
    use super::{
        canonicalize_json_value, sanitize_anthropic_tool_input_schema,
        sanitize_openai_function_parameters_schema,
    };
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
    fn anthropic_keeps_existing_object_and_nested_schema() {
        let input = json!({
            "type": "object",
            "properties": {
                "value": {"oneOf": [{"type": "string"}, {"type": "integer"}]}
            },
            "required": ["value"],
            "additionalProperties": false
        });
        assert_eq!(sanitize_anthropic_tool_input_schema(&input), input);
    }

    #[test]
    fn anthropic_preserves_fields_and_unconditional_requirements_of_inline_branches() {
        let input = json!({
            "type": "object",
            "properties": {
                "mode": {"type": "string", "enum": ["list", "inspect"], "description": "Operation to run"}
            },
            "required": ["mode"],
            "oneOf": [
                {
                    "properties": {"mode": {"const": "list"}, "cursor": {"type": "string"}, "id": false},
                    "required": ["scope"]
                },
                {
                    "properties": {"mode": {"const": "inspect"}, "id": {"type": "string"}, "cursor": false},
                    "required": ["scope", "id"]
                }
            ],
            "allOf": [
                {"properties": {"scope": {"type": "string"}}},
                {
                    "anyOf": [
                        {"properties": {"details": {"type": "string"}}, "required": ["details"]},
                        {"properties": {"details": {"type": "boolean"}}, "required": ["details"]}
                    ]
                },
                {"properties": {"limit": {"type": "integer"}}, "required": ["limit"]},
                {"properties": {"limit": {"minimum": 1}}}
            ]
        });
        assert_eq!(
            sanitize_anthropic_tool_input_schema(&input),
            json!({
                "type": "object",
                "properties": {
                    "mode": input["properties"]["mode"],
                    "cursor": {"type": "string"},
                    "id": {"type": "string"},
                    "scope": {"type": "string"},
                    "details": {"anyOf": [{"type": "string"}, {"type": "boolean"}]},
                    "limit": {"allOf": [{"type": "integer"}, {"minimum": 1}]}
                },
                "required": ["mode", "details", "limit", "scope"]
            })
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
