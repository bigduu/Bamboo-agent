use crate::{core::config::CopilotConfig, Config};
use actix_web::http::StatusCode;

use super::{client::resolve_headless_auth, status_logout::auth_status_from_token_content};

#[test]
fn resolve_headless_auth_prefers_provider_specific_value() {
    let mut config = Config::default();
    config.headless_auth = false;
    config.providers.copilot = Some(CopilotConfig {
        enabled: true,
        headless_auth: true,
        model: None,
        fast_model: None,
        vision_model: None,
        reasoning_effort: None,
        responses_only_models: Vec::new(),
        request_overrides: None,
        extra: Default::default(),
    });

    assert!(resolve_headless_auth(&config));
}

#[test]
fn resolve_headless_auth_falls_back_to_legacy_root_field() {
    let mut config = Config::default();
    config.providers.copilot = None;
    config.headless_auth = true;

    assert!(resolve_headless_auth(&config));
}

#[test]
fn auth_status_from_token_content_reports_authenticated_when_token_valid() {
    let status = auth_status_from_token_content(r#"{"expires_at": 700}"#, 100)
        .expect("status should be parsed");

    assert!(status.authenticated);
    assert_eq!(
        status.message.as_deref(),
        Some("Token expires in 10 minutes")
    );
}

#[test]
fn auth_status_from_token_content_reports_expired_when_token_is_expired() {
    let status = auth_status_from_token_content(r#"{"expires_at": 150}"#, 100)
        .expect("status should be parsed");

    assert!(!status.authenticated);
    assert_eq!(status.message.as_deref(), Some("Token expired"));
}

#[test]
fn auth_status_from_token_content_returns_none_when_payload_missing_expiry() {
    let status = auth_status_from_token_content(r#"{"access_token":"x"}"#, 100);
    assert!(status.is_none());
}

#[actix_web::test]
async fn copilot_auth_config_registers_expected_routes() {
    let app = actix_web::test::init_service(actix_web::App::new().configure(super::config)).await;

    for uri in [
        "/bamboo/copilot/auth/start",
        "/bamboo/copilot/auth/complete",
        "/bamboo/copilot/authenticate",
        "/bamboo/copilot/auth/status",
        "/bamboo/copilot/logout",
    ] {
        let req = actix_web::test::TestRequest::post().uri(uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        assert_ne!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "expected copilot auth route to be registered: {uri}"
        );
    }
}
