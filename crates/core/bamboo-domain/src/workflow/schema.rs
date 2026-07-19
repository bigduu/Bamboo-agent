use serde_json::Value;

pub fn validate_schema(schema: &Value, value: &Value) -> Result<(), String> {
    validate_schema_shape(schema, "$")?;
    validate_at(schema, value, "$")
}

pub fn validate_schema_shape(schema: &Value, path: &str) -> Result<(), String> {
    let object = schema
        .as_object()
        .ok_or_else(|| format!("{path}: schema must be an object"))?;
    const SUPPORTED: &[&str] = &[
        "type",
        "enum",
        "required",
        "properties",
        "additionalProperties",
        "items",
        "minimum",
        "maximum",
        "x-bamboo-secret",
    ];
    for key in object.keys() {
        if !SUPPORTED.contains(&key.as_str()) {
            return Err(format!("{path}: unsupported schema keyword '{key}'"));
        }
    }
    if object
        .get("x-bamboo-secret")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(format!("{path}: x-bamboo-secret must be boolean"));
    }
    if let Some(kind) = object.get("type") {
        let valid_kind = |kind: &str| {
            matches!(
                kind,
                "null" | "boolean" | "object" | "array" | "number" | "integer" | "string"
            )
        };
        match kind {
            Value::String(kind) if valid_kind(kind) => {}
            Value::Array(kinds)
                if !kinds.is_empty()
                    && kinds
                        .iter()
                        .all(|kind| kind.as_str().is_some_and(valid_kind)) => {}
            _ => return Err(format!("{path}: invalid or unsupported type")),
        }
    }
    if object.get("enum").is_some_and(|value| !value.is_array()) {
        return Err(format!("{path}: enum must be an array"));
    }
    if let Some(required) = object.get("required") {
        if !required
            .as_array()
            .is_some_and(|values| values.iter().all(Value::is_string))
        {
            return Err(format!("{path}: required must be an array of strings"));
        }
    }
    if let Some(properties) = object.get("properties") {
        let properties = properties
            .as_object()
            .ok_or_else(|| format!("{path}: properties must be an object"))?;
        for (key, child) in properties {
            validate_schema_shape(child, &format!("{path}/properties/{key}"))?;
        }
    }
    if object
        .get("additionalProperties")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(format!("{path}: additionalProperties must be boolean"));
    }
    if let Some(items) = object.get("items") {
        validate_schema_shape(items, &format!("{path}/items"))?;
    }
    for keyword in ["minimum", "maximum"] {
        if object.get(keyword).is_some_and(|value| !value.is_number()) {
            return Err(format!("{path}: {keyword} must be numeric"));
        }
    }
    Ok(())
}

fn validate_at(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    let object = schema
        .as_object()
        .ok_or_else(|| format!("{path}: schema must be an object"))?;
    if object.get("x-bamboo-secret") == Some(&Value::Bool(true)) {
        let handle = value
            .as_object()
            .filter(|handle| handle.len() == 1)
            .and_then(|handle| handle.get("$secret"))
            .and_then(Value::as_str)
            .filter(|handle| !handle.trim().is_empty());
        return handle
            .map(|_| ())
            .ok_or_else(|| format!("{path}: secret input must be a typed capability handle"));
    }
    if let Some(types) = object.get("type") {
        let valid = match types {
            Value::String(kind) => matches_type(kind, value),
            Value::Array(kinds) => kinds
                .iter()
                .filter_map(Value::as_str)
                .any(|kind| matches_type(kind, value)),
            _ => false,
        };
        if !valid {
            return Err(format!("{path}: value does not match schema type"));
        }
    }
    if let Some(values) = object.get("enum").and_then(Value::as_array) {
        if !values.contains(value) {
            return Err(format!("{path}: value is not in enum"));
        }
    }
    if let Some(required) = object.get("required").and_then(Value::as_array) {
        let value_object = value
            .as_object()
            .ok_or_else(|| format!("{path}: required applies to an object"))?;
        for key in required.iter().filter_map(Value::as_str) {
            if !value_object.contains_key(key) {
                return Err(format!("{path}: required property '{key}' is missing"));
            }
        }
    }
    if let Some(value_object) = value.as_object() {
        let properties = object.get("properties").and_then(Value::as_object);
        for (key, child) in value_object {
            if let Some(child_schema) = properties.and_then(|schemas| schemas.get(key)) {
                validate_at(child_schema, child, &format!("{path}/{key}"))?;
            } else if object.get("additionalProperties") == Some(&Value::Bool(false)) {
                return Err(format!(
                    "{path}: additional property '{key}' is not allowed"
                ));
            }
        }
    }
    if let (Some(items), Some(values)) = (object.get("items"), value.as_array()) {
        for (index, child) in values.iter().enumerate() {
            validate_at(items, child, &format!("{path}/{index}"))?;
        }
    }
    if let Some(minimum) = object.get("minimum").and_then(Value::as_f64) {
        if value.as_f64().is_none_or(|number| number < minimum) {
            return Err(format!("{path}: value is below minimum"));
        }
    }
    if let Some(maximum) = object.get("maximum").and_then(Value::as_f64) {
        if value.as_f64().is_none_or(|number| number > maximum) {
            return Err(format!("{path}: value is above maximum"));
        }
    }
    Ok(())
}

fn matches_type(kind: &str, value: &Value) -> bool {
    match kind {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "string" => value.is_string(),
        _ => false,
    }
}
