use crate::handlers::settings::keyword_masking::types::ValidationError;
use serde_json::{json, Value};

pub(super) fn validation_success_payload() -> Value {
    json!({ "valid": true })
}

pub(super) fn validation_error_payload(errors: Vec<ValidationError>) -> Value {
    json!({
        "valid": false,
        "errors": errors
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_success_payload() {
        let payload = validation_success_payload();
        assert_eq!(payload["valid"], true);
        assert_eq!(payload.as_object().unwrap().len(), 1);
    }

    #[test]
    fn test_validation_error_payload_with_empty_errors() {
        let errors = vec![];
        let payload = validation_error_payload(errors);
        assert_eq!(payload["valid"], false);
        assert!(payload["errors"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_validation_error_payload_with_errors() {
        let errors = vec![ValidationError {
            index: 0,
            message: "Invalid regex".to_string(),
        }];
        let payload = validation_error_payload(errors);
        assert_eq!(payload["valid"], false);
        let errors_array = payload["errors"].as_array().unwrap();
        assert_eq!(errors_array.len(), 1);
        assert_eq!(errors_array[0]["index"], 0);
        assert_eq!(errors_array[0]["message"], "Invalid regex");
    }

    #[test]
    fn test_validation_error_payload_with_multiple_errors() {
        let errors = vec![
            ValidationError {
                index: 0,
                message: "Invalid regex".to_string(),
            },
            ValidationError {
                index: 2,
                message: "Pattern too long".to_string(),
            },
        ];
        let payload = validation_error_payload(errors);
        assert_eq!(payload["valid"], false);
        let errors_array = payload["errors"].as_array().unwrap();
        assert_eq!(errors_array.len(), 2);
    }
}
