use crate::handlers::settings::keyword_masking::handlers::{
    get::get_keyword_masking_config,
    update::update_keyword_masking_config,
    validate::validate_keyword_masking_patterns,
};
use crate::handlers::settings::keyword_masking::types::KeywordMaskingResponse;
use crate::app_state::AppState;
use bamboo_llm::Config;
use actix_web::{test, web, App};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;

fn create_test_app_state() -> web::Data<AppState> {
    let config = Arc::new(RwLock::new(Config::default()));
    web::Data::new(AppState::new_test(config))
}

#[actix_web::test]
async fn get_keyword_masking_config_returns_empty_by_default() {
    let app_state = create_test_app_state();

    let app = test::init_service(
        App::new()
            .app_data(app_state)
            .route("/api/settings/keyword-masking", web::get().to(get_keyword_masking_config)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/api/settings/keyword-masking")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body: KeywordMaskingResponse = test::read_body_json(resp).await;
    assert!(body.entries.is_empty());
}

#[actix_web::test]
async fn get_keyword_masking_config_returns_configured_entries() {
    let app_state = create_test_app_state();

    // Add some entries to config
    {
        let mut config = app_state.config.write().await;
        config.keyword_masking.entries = vec![
            crate::config::KeywordMaskingEntry {
                pattern: "secret".to_string(),
                replacement: "***".to_string(),
                case_sensitive: false,
            },
        ];
    }

    let app = test::init_service(
        App::new()
            .app_data(app_state)
            .route("/api/settings/keyword-masking", web::get().to(get_keyword_masking_config)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/api/settings/keyword-masking")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body: KeywordMaskingResponse = test::read_body_json(resp).await;
    assert_eq!(body.entries.len(), 1);
    assert_eq!(body.entries[0].pattern, "secret");
    assert_eq!(body.entries[0].replacement, "***");
    assert!(!body.entries[0].case_sensitive);
}

#[actix_web::test]
async fn update_keyword_masking_config_validates_patterns() {
    let app_state = create_test_app_state();

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/api/settings/keyword-masking", web::post().to(update_keyword_masking_config)),
    )
    .await;

    // Send invalid regex pattern
    let invalid_patterns = json!({
        "entries": [
            {
                "pattern": "[invalid(",
                "replacement": "***",
                "case_sensitive": false
            }
        ]
    });

    let req = test::TestRequest::post()
        .uri("/api/settings/keyword-masking")
        .set_json(&invalid_patterns)
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_client_error());
}

#[actix_web::test]
async fn update_keyword_masking_config_accepts_valid_patterns() {
    let app_state = create_test_app_state();

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/api/settings/keyword-masking", web::post().to(update_keyword_masking_config)),
    )
    .await;

    let valid_patterns = json!({
        "entries": [
            {
                "pattern": "secret",
                "replacement": "***",
                "case_sensitive": false
            }
        ]
    });

    let req = test::TestRequest::post()
        .uri("/api/settings/keyword-masking")
        .set_json(&valid_patterns)
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // Verify config was updated
    let config = app_state.config.read().await;
    assert_eq!(config.keyword_masking.entries.len(), 1);
    assert_eq!(config.keyword_masking.entries[0].pattern, "secret");
}

#[actix_web::test]
async fn update_keyword_masking_config_clears_entries_when_empty() {
    let app_state = create_test_app_state();

    // Add initial entries
    {
        let mut config = app_state.config.write().await;
        config.keyword_masking.entries = vec![
            crate::config::KeywordMaskingEntry {
                pattern: "secret".to_string(),
                replacement: "***".to_string(),
                case_sensitive: false,
            },
        ];
    }

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/api/settings/keyword-masking", web::post().to(update_keyword_masking_config)),
    )
    .await;

    let empty_patterns = json!({
        "entries": []
    });

    let req = test::TestRequest::post()
        .uri("/api/settings/keyword-masking")
        .set_json(&empty_patterns)
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // Verify entries were cleared
    let config = app_state.config.read().await;
    assert!(config.keyword_masking.entries.is_empty());
}

#[actix_web::test]
async fn validate_keyword_masking_patterns_returns_valid_for_correct_patterns() {
    let app_state = create_test_app_state();

    let app = test::init_service(
        App::new()
            .app_data(app_state)
            .route("/api/settings/keyword-masking/validate", web::post().to(validate_keyword_masking_patterns)),
    )
    .await;

    let patterns = json!({
        "entries": [
            {
                "pattern": "\\d{4}-\\d{4}-\\d{4}-\\d{4}",
                "replacement": "****-****-****-****",
                "case_sensitive": false
            }
        ]
    });

    let req = test::TestRequest::post()
        .uri("/api/settings/keyword-masking/validate")
        .set_json(&patterns)
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["valid"], true);
}

#[actix_web::test]
async fn validate_keyword_masking_patterns_returns_errors_for_invalid_regex() {
    let app_state = create_test_app_state();

    let app = test::init_service(
        App::new()
            .app_data(app_state)
            .route("/api/settings/keyword-masking/validate", web::post().to(validate_keyword_masking_patterns)),
    )
    .await;

    let patterns = json!({
        "entries": [
            {
                "pattern": "[invalid(regex",
                "replacement": "***",
                "case_sensitive": false
            }
        ]
    });

    let req = test::TestRequest::post()
        .uri("/api/settings/keyword-masking/validate")
        .set_json(&patterns)
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["valid"], false);
    assert!(body["errors"].as_array().unwrap().len() > 0);
}

#[actix_web::test]
async fn validate_keyword_masking_patterns_validates_multiple_entries() {
    let app_state = create_test_app_state();

    let app = test::init_service(
        App::new()
            .app_data(app_state)
            .route("/api/settings/keyword-masking/validate", web::post().to(validate_keyword_masking_patterns)),
    )
    .await;

    let patterns = json!({
        "entries": [
            {
                "pattern": "valid_pattern",
                "replacement": "***",
                "case_sensitive": false
            },
            {
                "pattern": "[invalid",
                "replacement": "***",
                "case_sensitive": false
            },
            {
                "pattern": "another_valid",
                "replacement": "***",
                "case_sensitive": true
            }
        ]
    });

    let req = test::TestRequest::post()
        .uri("/api/settings/keyword-masking/validate")
        .set_json(&patterns)
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["valid"], false);
    let errors = body["errors"].as_array().unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["index"], 1);
}

#[actix_web::test]
async fn keyword_masking_handles_case_sensitive_patterns() {
    let app_state = create_test_app_state();

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/api/settings/keyword-masking", web::post().to(update_keyword_masking_config)),
    )
    .await;

    let patterns = json!({
        "entries": [
            {
                "pattern": "Secret",
                "replacement": "***",
                "case_sensitive": true
            }
        ]
    });

    let req = test::TestRequest::post()
        .uri("/api/settings/keyword-masking")
        .set_json(&patterns)
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let config = app_state.config.read().await;
    assert!(config.keyword_masking.entries[0].case_sensitive);
}

#[actix_web::test]
async fn keyword_masking_handles_empty_replacement() {
    let app_state = create_test_app_state();

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/api/settings/keyword-masking", web::post().to(update_keyword_masking_config)),
    )
    .await;

    let patterns = json!({
        "entries": [
            {
                "pattern": "secret",
                "replacement": "",
                "case_sensitive": false
            }
        ]
    });

    let req = test::TestRequest::post()
        .uri("/api/settings/keyword-masking")
        .set_json(&patterns)
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let config = app_state.config.read().await;
    assert_eq!(config.keyword_masking.entries[0].replacement, "");
}

#[actix_web::test]
async fn keyword_masking_handles_unicode_patterns() {
    let app_state = create_test_app_state();

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/api/settings/keyword-masking", web::post().to(update_keyword_masking_config)),
    )
    .await;

    let patterns = json!({
        "entries": [
            {
                "pattern": "秘密",
                "replacement": "***",
                "case_sensitive": false
            }
        ]
    });

    let req = test::TestRequest::post()
        .uri("/api/settings/keyword-masking")
        .set_json(&patterns)
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let config = app_state.config.read().await;
    assert_eq!(config.keyword_masking.entries[0].pattern, "秘密");
}

#[actix_web::test]
async fn keyword_masking_handles_large_number_of_entries() {
    let app_state = create_test_app_state();

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/api/settings/keyword-masking", web::post().to(update_keyword_masking_config)),
    )
    .await;

    let entries: Vec<_> = (0..100)
        .map(|i| {
            json!({
                "pattern": format!("pattern{}", i),
                "replacement": "***",
                "case_sensitive": false
            })
        })
        .collect();

    let patterns = json!({
        "entries": entries
    });

    let req = test::TestRequest::post()
        .uri("/api/settings/keyword-masking")
        .set_json(&patterns)
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let config = app_state.config.read().await;
    assert_eq!(config.keyword_masking.entries.len(), 100);
}
