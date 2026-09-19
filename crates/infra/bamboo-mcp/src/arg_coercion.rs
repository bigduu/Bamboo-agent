//! Correct scalar spelling only when the schema has one unambiguous type.

use serde_json::Value;

pub(crate) fn coerce_args_to_schema(value: &mut Value, schema: &Value) {
    if ["$ref", "anyOf", "oneOf", "allOf", "enum", "const"]
        .iter()
        .any(|key| schema.get(key).is_some())
    {
        return;
    }
    let target = match schema.get("type") {
        Some(Value::String(t)) => Some(t.as_str()),
        Some(Value::Array(types)) => {
            let mut types = types
                .iter()
                .filter_map(Value::as_str)
                .filter(|t| *t != "null");
            let first = types.next();
            if types.next().is_some() {
                None
            } else {
                first
            }
        }
        _ => None,
    };
    match value {
        Value::Object(fields) => {
            if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
                for (key, value) in fields {
                    if let Some(property) = properties.get(key) {
                        coerce_args_to_schema(value, property);
                    }
                }
            }
        }
        Value::Array(items) => {
            if let Some(item_schema) = schema.get("items").filter(|s| s.is_object()) {
                for item in items {
                    coerce_args_to_schema(item, item_schema);
                }
            }
        }
        Value::String(text) => {
            let trimmed = text.trim();
            let replacement = match target {
                Some("integer") => exact_integer(trimmed),
                Some("number") => serde_json::from_str::<Value>(trimmed)
                    .ok()
                    .filter(Value::is_number),
                Some("boolean") if trimmed.eq_ignore_ascii_case("true") => Some(Value::Bool(true)),
                Some("boolean") if trimmed.eq_ignore_ascii_case("false") => {
                    Some(Value::Bool(false))
                }
                _ => None,
            };
            if let Some(replacement) = replacement {
                *value = replacement;
            }
        }
        Value::Number(number) if target == Some("integer") && number.is_f64() => {
            // Outside this range the parsed float may already have lost digits.
            if let Some(n) = number
                .as_f64()
                .filter(|n| n.fract() == 0.0 && n.abs() <= 9_007_199_254_740_991.0)
            {
                *value = Value::from(n as i64);
            }
        }
        _ => {}
    }
}

fn exact_integer(text: &str) -> Option<Value> {
    if let Ok(n) = text.parse::<i64>() {
        return Some(Value::from(n));
    }
    if let Ok(n) = text.parse::<u64>() {
        return Some(Value::from(n));
    }
    if text.len() > 128 {
        return None;
    }
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i32>().ok()?),
        None => (unsigned, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let mut digits = format!("{whole}{fraction}");
    let scale = exponent.checked_sub(i32::try_from(fraction.len()).ok()?)?;
    if scale < 0 {
        let removed = usize::try_from(scale.checked_neg()?).ok()?;
        if removed > digits.len() || !digits[digits.len() - removed..].bytes().all(|b| b == b'0') {
            return None;
        }
        digits.truncate(digits.len() - removed);
    }
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some(Value::from(0));
    }
    let zeros = usize::try_from(scale.max(0)).ok()?;
    if digits.len().checked_add(zeros)? > 20 {
        return None;
    }
    let normalized = format!(
        "{}{digits}{}",
        if negative { "-" } else { "" },
        "0".repeat(zeros)
    );
    normalized
        .parse::<i64>()
        .map(Value::from)
        .ok()
        .or_else(|| normalized.parse::<u64>().map(Value::from).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn large_integers_are_exact_or_left_untouched() {
        for (input, expected) in [
            ("9007199254740993.0", json!(9_007_199_254_740_993u64)),
            ("18446744073709551615", json!(u64::MAX)),
            ("18446744073709551616", json!("18446744073709551616")),
            ("-9223372036854775809", json!("-9223372036854775809")),
            ("1.20e2", json!(120)),
            ("1.2", json!("1.2")),
            ("1e999999999", json!("1e999999999")),
        ] {
            let mut value = json!(input);
            coerce_args_to_schema(&mut value, &json!({"type":"integer"}));
            assert_eq!(value, expected, "{input}");
        }
    }

    #[test]
    fn nested_fields_follow_schema_without_rewriting_strings_or_unions() {
        let mut value = json!({"count":"200", "on":"TRUE", "name":"200", "union":"200", "items":["3.0"], "unknown":"1"});
        coerce_args_to_schema(
            &mut value,
            &json!({"properties":{
                "count":{"type":["integer","null"]}, "on":{"type":"boolean"},
                "name":{"type":"string"}, "union":{"type":["integer","string"]},
                "items":{"type":"array","items":{"type":"integer"}}
            }}),
        );
        assert_eq!(
            value,
            json!({"count":200,"on":true,"name":"200","union":"200","items":[3],"unknown":"1"})
        );
    }

    #[test]
    fn constraints_and_imprecise_numeric_values_are_not_guessed() {
        for schema in [
            json!({"type":"integer","enum":[1]}),
            json!({"oneOf":[{"type":"integer"}]}),
            json!({"$ref":"#/$defs/n"}),
        ] {
            let mut value = json!("1");
            coerce_args_to_schema(&mut value, &schema);
            assert_eq!(value, json!("1"));
        }
        let mut value = json!(9_007_199_254_740_992.0);
        coerce_args_to_schema(&mut value, &json!({"type":"integer"}));
        assert!(value.as_number().unwrap().is_f64());
    }
}
