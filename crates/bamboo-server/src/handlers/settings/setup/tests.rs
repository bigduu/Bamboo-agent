use bamboo_infrastructure::Config;
use crate::error::AppError;

use super::{
    status::build_setup_status,
    updates::{set_setup_complete, set_setup_incomplete},
};

#[test]
fn build_setup_status_marks_complete_when_setup_completed_flag_is_true() {
    let mut config = Config::default();
    config.extra.insert(
        "setup".to_string(),
        serde_json::json!({
            "completed": true
        }),
    );

    let status = build_setup_status(&config, &[]);
    assert!(status.is_complete);
    assert_eq!(
        status.message,
        "Setup has already been completed in config.json."
    );
}

#[test]
fn build_setup_status_reports_proxy_config_without_completion_flag() {
    let mut config = Config::default();
    config.extra.clear();
    config.http_proxy = "http://proxy.internal:8080".to_string();

    let status = build_setup_status(&config, &[]);
    assert!(!status.has_proxy_env);
    assert!(status.has_proxy_config);
    assert!(!status.is_complete);
    assert_eq!(
        status.message,
        "Proxy configuration already exists in config.json. Setup is not required."
    );
}

#[test]
fn build_setup_status_reports_proxy_environment_variables() {
    let mut config = Config::default();
    config.extra.clear();
    let status = build_setup_status(&config, &["HTTP_PROXY", "HTTPS_PROXY"]);

    assert!(status.has_proxy_env);
    assert!(status.message.contains("HTTP_PROXY, HTTPS_PROXY"));
}

#[test]
fn set_setup_complete_requires_setup_object_shape() {
    let mut config = Config::default();
    config.extra.insert(
        "setup".to_string(),
        serde_json::Value::String("invalid".to_string()),
    );

    let error = set_setup_complete(&mut config, "2026-01-01T00:00:00Z".to_string())
        .expect_err("expected setup object-shape error");

    match error {
        AppError::BadRequest(message) => {
            assert_eq!(message, "config.setup must be a JSON object");
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn set_setup_complete_and_incomplete_update_extra_fields() {
    let mut config = Config::default();

    set_setup_complete(&mut config, "2026-01-01T00:00:00Z".to_string())
        .expect("complete should succeed");
    let setup = config
        .extra
        .get("setup")
        .and_then(|value| value.as_object())
        .expect("setup should exist as object");
    assert_eq!(setup.get("completed"), Some(&serde_json::Value::Bool(true)));
    assert_eq!(
        setup.get("completed_at"),
        Some(&serde_json::Value::String(
            "2026-01-01T00:00:00Z".to_string()
        ))
    );
    assert_eq!(
        setup.get("version"),
        Some(&serde_json::Value::Number(1.into()))
    );

    set_setup_incomplete(&mut config, "2026-01-02T00:00:00Z".to_string());
    let setup = config
        .extra
        .get("setup")
        .and_then(|value| value.as_object())
        .expect("setup should still exist as object");
    assert_eq!(
        setup.get("completed"),
        Some(&serde_json::Value::Bool(false))
    );
    assert_eq!(
        setup.get("reset_at"),
        Some(&serde_json::Value::String(
            "2026-01-02T00:00:00Z".to_string()
        ))
    );
}
