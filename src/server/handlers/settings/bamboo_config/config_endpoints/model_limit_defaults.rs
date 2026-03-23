use actix_web::HttpResponse;
use serde::{Deserialize, Serialize};

use crate::agent::core::budget::limits::{DEFAULT_SAFETY_MARGIN, KNOWN_MODEL_LIMITS};
use crate::server::error::AppError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ModelLimitDefault {
    vendor: String,
    model_pattern: String,
    max_context_tokens: u32,
    max_output_tokens: u32,
    safety_margin: u32,
    note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ModelLimitDefaultsResponse {
    model_limits: Vec<ModelLimitDefault>,
}

fn infer_vendor(pattern: &str) -> &'static str {
    if pattern == "default" {
        return "System";
    }
    if pattern.starts_with("gpt-") {
        return "OpenAI";
    }
    if pattern.starts_with("claude-") {
        return "Anthropic";
    }
    if pattern.starts_with("gemini-") {
        return "Google";
    }
    if pattern.starts_with("grok-") {
        return "xAI";
    }
    if pattern.starts_with("oswe-") || pattern.starts_with("copilot-") {
        return "GitHub";
    }
    if pattern.starts_with("kimi-") {
        return "Moonshot";
    }
    if pattern.starts_with("glm-") {
        return "Zhipu";
    }
    ""
}

/// Returns built-in model limit defaults from backend source-of-truth.
pub async fn get_model_limit_defaults() -> Result<HttpResponse, AppError> {
    let model_limits = KNOWN_MODEL_LIMITS
        .iter()
        .map(|(model_pattern, max_context_tokens, max_output_tokens)| ModelLimitDefault {
            vendor: infer_vendor(model_pattern).to_string(),
            model_pattern: (*model_pattern).to_string(),
            max_context_tokens: *max_context_tokens,
            max_output_tokens: *max_output_tokens,
            safety_margin: DEFAULT_SAFETY_MARGIN,
            note: String::new(),
        })
        .collect::<Vec<_>>();

    Ok(HttpResponse::Ok().json(ModelLimitDefaultsResponse { model_limits }))
}

#[cfg(test)]
mod tests {
    use actix_web::{test, web, App};

    use crate::agent::core::budget::limits::KNOWN_MODEL_LIMITS;

    use super::{get_model_limit_defaults, ModelLimitDefaultsResponse};

    #[actix_web::test]
    async fn get_model_limit_defaults_returns_builtin_profiles() {
        let app = test::init_service(
            App::new().route(
                "/bamboo/model-limits/defaults",
                web::get().to(get_model_limit_defaults),
            ),
        )
        .await;

        let request = test::TestRequest::get()
            .uri("/bamboo/model-limits/defaults")
            .to_request();
        let response = test::call_service(&app, request).await;

        assert!(response.status().is_success());

        let payload: ModelLimitDefaultsResponse = test::read_body_json(response).await;
        assert_eq!(payload.model_limits.len(), KNOWN_MODEL_LIMITS.len());

        let gpt54 = payload
            .model_limits
            .iter()
            .find(|row| row.model_pattern == "gpt-5.4")
            .expect("gpt-5.4 entry should exist");
        assert_eq!(gpt54.max_context_tokens, 1_050_000);
        assert_eq!(gpt54.max_output_tokens, 32_768);
        assert_eq!(gpt54.vendor, "OpenAI");

        let codex = payload
            .model_limits
            .iter()
            .find(|row| row.model_pattern == "gpt-5.3-codex")
            .expect("gpt-5.3-codex entry should exist");
        assert_eq!(codex.max_context_tokens, 400_000);
        assert_eq!(codex.max_output_tokens, 128_000);
        assert_eq!(codex.vendor, "OpenAI");
    }
}
