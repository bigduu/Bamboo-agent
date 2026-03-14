use super::response::execute_response_payload;
use super::validation::validate_and_normalize_model;

#[test]
fn validate_and_normalize_model_rejects_empty_value() {
    assert!(validate_and_normalize_model("   ").is_err());
}

#[test]
fn validate_and_normalize_model_trims_whitespace() {
    let model = validate_and_normalize_model(" gpt-4o-mini ").expect("model should be valid");
    assert_eq!(model, "gpt-4o-mini");
}

#[test]
fn execute_response_payload_formats_status_and_events_url() {
    let payload = execute_response_payload("session-123", "started");
    assert_eq!(payload.session_id, "session-123");
    assert_eq!(payload.status, "started");
    assert_eq!(payload.events_url, "/api/v1/events/session-123");
}
