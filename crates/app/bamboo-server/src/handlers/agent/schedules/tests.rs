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

#[test]
fn workflow_schedule_target_requires_explicit_once_and_no_task() {
    use crate::schedule_app::{ScheduleRunConfig, ScheduleTrigger};
    let config: ScheduleRunConfig = serde_json::from_value(serde_json::json!({
        "auto_execute":true,"workflow_target":{"workflow_id":"read-once","revision":7,"args":{}}
    }))
    .unwrap();
    let once = ScheduleTrigger::Once {
        at: chrono::Utc::now() + chrono::Duration::hours(1),
    };
    super::validation::validate_workflow_target(&once, &config).unwrap();
    for (trigger, config) in [
        (
            ScheduleTrigger::Interval {
                every_seconds: 60,
                anchor_at: None,
            },
            config.clone(),
        ),
        (
            once.clone(),
            ScheduleRunConfig {
                auto_execute: false,
                ..config.clone()
            },
        ),
        (
            once,
            ScheduleRunConfig {
                task_message: Some(String::new()),
                ..config
            },
        ),
    ] {
        assert_eq!(
            super::validation::validate_workflow_target(&trigger, &config)
                .unwrap_err()
                .status(),
            actix_web::http::StatusCode::BAD_REQUEST
        );
    }
}
