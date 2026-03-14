use serde_json::Value;

pub(super) const MASKED_SECRET: &str = "****...****";

pub(super) fn masked_secret_value() -> Value {
    Value::String(MASKED_SECRET.to_string())
}
