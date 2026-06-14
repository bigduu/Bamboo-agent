use actix_web::HttpResponse;
use serde::{Deserialize, Serialize};

use crate::error::AppError;
use bamboo_compression::limits::default_model_limit;

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
    /// The single global default applied to any model without provider runtime
    /// metadata or a user override. There is no per-model built-in table; this
    /// list always contains exactly one entry (`model_limits[0]`).
    model_limits: Vec<ModelLimitDefault>,
}

/// Returns the single global model-limit default from the backend
/// source-of-truth (`1M` context / `64K` output).
pub async fn get_model_limit_defaults() -> Result<HttpResponse, AppError> {
    let limit = default_model_limit();
    let default = ModelLimitDefault {
        vendor: String::new(),
        model_pattern: limit.model_pattern.clone(),
        max_context_tokens: limit.max_context_tokens,
        max_output_tokens: limit.get_max_output_tokens(),
        safety_margin: limit.get_safety_margin(),
        note: String::new(),
    };

    Ok(HttpResponse::Ok().json(ModelLimitDefaultsResponse {
        model_limits: vec![default],
    }))
}

#[cfg(test)]
mod tests {
    use actix_web::{test, web, App};

    use super::{get_model_limit_defaults, ModelLimitDefaultsResponse};

    #[actix_web::test]
    async fn get_model_limit_defaults_returns_single_global_default() {
        let app = test::init_service(App::new().route(
            "/bamboo/model-limits/defaults",
            web::get().to(get_model_limit_defaults),
        ))
        .await;

        let request = test::TestRequest::get()
            .uri("/bamboo/model-limits/defaults")
            .to_request();
        let response = test::call_service(&app, request).await;

        assert!(response.status().is_success());

        let payload: ModelLimitDefaultsResponse = test::read_body_json(response).await;
        assert_eq!(payload.model_limits.len(), 1);

        let default = &payload.model_limits[0];
        assert_eq!(default.model_pattern, "default");
        assert_eq!(default.max_context_tokens, 1_000_000);
        assert_eq!(default.max_output_tokens, 64_000);
        // Safety margin scales with context window: max(1_000_000 / 100, 1_000).
        assert_eq!(default.safety_margin, 10_000);
    }
}
