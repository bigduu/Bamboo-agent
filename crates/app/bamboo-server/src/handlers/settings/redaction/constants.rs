use serde_json::Value;

pub(super) const MASKED_SECRET: &str = "****...****";

pub(super) fn masked_secret_value() -> Value {
    Value::String(MASKED_SECRET.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_masked_secret_constant() {
        assert_eq!(MASKED_SECRET, "****...****");
    }

    #[test]
    fn test_masked_secret_value_returns_string() {
        let value = masked_secret_value();
        assert!(value.is_string());
    }

    #[test]
    fn test_masked_secret_value_content() {
        let value = masked_secret_value();
        assert_eq!(value.as_str().unwrap(), MASKED_SECRET);
    }

    #[test]
    fn test_masked_secret_value_multiple_calls() {
        let value1 = masked_secret_value();
        let value2 = masked_secret_value();
        assert_eq!(value1, value2);
    }
}
