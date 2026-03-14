use crate::server::handlers::settings::keyword_masking::types::ValidationError;
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
