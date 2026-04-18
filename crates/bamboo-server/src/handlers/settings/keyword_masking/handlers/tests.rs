use super::payload::{validation_error_payload, validation_success_payload};
use crate::handlers::settings::keyword_masking::types::ValidationError;

#[test]
fn validation_success_payload_marks_valid_true() {
    let payload = validation_success_payload();
    assert_eq!(payload["valid"], true);
}

#[test]
fn validation_error_payload_includes_structured_errors() {
    let payload = validation_error_payload(vec![ValidationError {
        index: 1,
        message: "invalid pattern".to_string(),
    }]);
    assert_eq!(payload["valid"], false);
    assert_eq!(payload["errors"][0]["index"], 1);
    assert_eq!(payload["errors"][0]["message"], "invalid pattern");
}
