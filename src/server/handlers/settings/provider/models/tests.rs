use crate::server::error::AppError;

use super::upstream::{build_provider_models_request, extract_model_ids};

#[test]
fn build_provider_models_request_constructs_openai_defaults() {
    let request =
        build_provider_models_request("openai", "sk-test", None).expect("openai request builds");
    assert_eq!(request.url, "https://api.openai.com/v1/models");
    assert_eq!(
        request.auth_header,
        Some(("Authorization", "Bearer sk-test".to_string()))
    );
}

#[test]
fn build_provider_models_request_constructs_gemini_base_url_query_auth() {
    let request = build_provider_models_request(
        "gemini",
        "g-key",
        Some("https://custom.googleapis.com/v1beta/models/"),
    )
    .expect("gemini request builds");
    assert_eq!(
        request.url,
        "https://custom.googleapis.com/v1beta/models?key=g-key"
    );
    assert!(request.auth_header.is_none());
}

#[test]
fn extract_model_ids_supports_openai_data_format() {
    let models = extract_model_ids(serde_json::json!({
        "data": [
            {"id": "gpt-4o"},
            {"id": "gpt-4.1-mini"}
        ]
    }))
    .expect("model extraction should succeed");

    assert_eq!(models, vec!["gpt-4o", "gpt-4.1-mini"]);
}

#[test]
fn extract_model_ids_supports_gemini_models_format() {
    let models = extract_model_ids(serde_json::json!({
        "models": [
            {"name": "models/gemini-2.0-flash"},
            {"id": "gemini-2.5-pro"},
            "gemini-custom"
        ]
    }))
    .expect("model extraction should succeed");

    assert_eq!(
        models,
        vec!["models/gemini-2.0-flash", "gemini-2.5-pro", "gemini-custom"]
    );
}

#[test]
fn extract_model_ids_errors_for_unexpected_shape() {
    let error =
        extract_model_ids(serde_json::json!({"unexpected": true})).expect_err("expected error");

    match error {
        AppError::InternalError(error) => {
            assert!(error.to_string().contains("Unexpected response format"))
        }
        other => panic!("unexpected error: {other}"),
    }
}
