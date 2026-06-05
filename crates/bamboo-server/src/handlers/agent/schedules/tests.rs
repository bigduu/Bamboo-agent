use super::validation::{validate_schedule_name, validate_schedule_trigger};

#[test]
fn validate_schedule_name_trims_and_accepts_non_empty_values() {
    let name = validate_schedule_name("  every-hour  ").expect("name should be valid");
    assert_eq!(name, "every-hour");
}

#[test]
fn validate_schedule_name_rejects_empty_values() {
    let response = validate_schedule_name("   ").expect_err("name should be invalid");
    assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
}

#[test]
fn validate_interval_trigger_rejects_zero() {
    let response = validate_schedule_trigger(&crate::schedule_app::ScheduleTrigger::Interval {
        every_seconds: 0,
        anchor_at: None,
    })
    .expect_err("interval should reject zero value");
    assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
}

#[test]
fn validate_interval_trigger_accepts_positive_values() {
    validate_schedule_trigger(&crate::schedule_app::ScheduleTrigger::Interval {
        every_seconds: 1,
        anchor_at: None,
    })
    .expect("interval should accept positive value");
}
