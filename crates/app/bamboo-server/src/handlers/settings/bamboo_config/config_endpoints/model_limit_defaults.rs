use actix_web::{web, HttpResponse};
use bamboo_config::Config;
use bamboo_domain::ProviderModelRef;
use bamboo_engine::model_config_helper::{try_resolve_primary_model, ResolvedPrimaryModel};
use bamboo_llm::ProviderRegistry;
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::app_state::AppState;
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
    supports_unlimited_output: bool,
    /// The single global default applied to any model without provider runtime
    /// metadata or a user override. There is no per-model built-in table; this
    /// list always contains exactly one entry (`model_limits[0]`).
    model_limits: Vec<ModelLimitDefault>,
}

/// Returns the same global fallback values used by the runtime.
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
        supports_unlimited_output: true,
        model_limits: vec![default],
    }))
}

#[derive(Deserialize)]
pub struct ModelLimitWindowsRequest {
    models: Vec<ModelLimitTarget>,
}

#[derive(Deserialize)]
struct ModelLimitTarget {
    model: String,
    provider: Option<String>,
}

#[derive(Serialize)]
struct EffectiveModelWindow {
    provider: String,
    max_context_tokens: u32,
    uses_context_fallback: bool,
}

#[derive(Serialize)]
struct ModelLimitWindowsResponse {
    /// One result per requested model, in order. Null means no matching route
    /// could be identified conclusively from current configuration.
    model_windows: Vec<Option<EffectiveModelWindow>>,
}

pub async fn resolve_model_limit_windows(
    app_state: web::Data<AppState>,
    body: web::Json<ModelLimitWindowsRequest>,
) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await.clone();
    let state = app_state.get_ref();
    let model_windows = stream::iter(body.into_inner().models)
        .map(|target| {
            let config = &config;
            async move {
                let route = select_model_limit_route(config, &state.provider_registry, &target)?;
                let (max_context_tokens, uses_context_fallback) = state
                    .agent
                    .effective_chat_context_window(config, &state.app_data_dir, &route)
                    .await;
                Some(EffectiveModelWindow {
                    provider: route.target.provider,
                    max_context_tokens,
                    uses_context_fallback,
                })
            }
        })
        .buffered(16)
        .collect()
        .await;
    Ok(HttpResponse::Ok().json(ModelLimitWindowsResponse { model_windows }))
}

fn select_model_limit_route(
    config: &Config,
    registry: &Arc<ProviderRegistry>,
    target: &ModelLimitTarget,
) -> Option<ResolvedPrimaryModel> {
    let resolve = |provider: &str| {
        try_resolve_primary_model(
            config,
            registry,
            &ProviderModelRef::new(provider, &target.model),
        )
        .ok()
    };
    if let Some(provider) = &target.provider {
        return resolve(provider);
    }
    if !config.features.provider_model_ref {
        return resolve(config.effective_default_provider());
    }

    // Older discovery records only stored the model ID. Resolve one current
    // eligible route, or leave the window unknown when the ID is ambiguous.
    let mut routes = registry
        .provider_names()
        .into_iter()
        .filter_map(|provider| resolve(&provider));
    let route = routes.next()?;
    routes.next().is_none().then_some(route)
}

#[cfg(test)]
mod tests {
    use actix_web::{test, web, App};
    use serde_json::{json, Value};
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    use super::{get_model_limit_defaults, ModelLimitDefaultsResponse};
    use crate::{app_state::AppState, routes::configure_routes};

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
        assert!(payload.supports_unlimited_output);
        assert_eq!(payload.model_limits.len(), 1);

        let default = &payload.model_limits[0];
        assert_eq!(default.model_pattern, "default");
        assert_eq!(default.max_context_tokens, 475_000);
        assert_eq!(default.max_output_tokens, 4_096);
        assert_eq!(default.safety_margin, 4_750);
    }

    #[actix_web::test]
    async fn effective_model_windows_follow_routes_and_report_only_actual_fallbacks() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x68; 32]);
        let home = tempfile::tempdir().unwrap();
        let upstream = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/primary/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": [
                {"id": "bounded-model", "max_context_tokens": 128_000},
                {"id": "reported-475k", "max_context_tokens": 475_000},
                {"id": "separate-limits", "max_input_tokens": 100_000, "max_output_tokens": 8_000},
                {"id": "output-only", "max_output_tokens": 8_000},
                {"id": "unknown"},
                {"id": "local-32k"},
                {"id": "local-475k"}
            ]})))
            .expect(1)
            .mount(&upstream)
            .await;
        Mock::given(method("GET"))
            .and(path("/secondary/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": [
                {"id": "bounded-model", "max_context_tokens": 200_000}
            ]})))
            .expect(1)
            .mount(&upstream)
            .await;
        let config = json!({
            "features": { "provider_model_ref": true },
            "default_provider_instance": "primary",
            "provider_instances": {
                "primary": {
                    "provider_type": "openai", "enabled": true,
                    "api_key": "fixture", "base_url": format!("{}/primary", upstream.uri()),
                    "model": "bounded-model",
                    "runtime_models": [
                        "bounded-model", "reported-475k", "separate-limits", "output-only",
                        "unknown", "local-32k", "local-475k"
                    ]
                },
                "secondary": {
                    "provider_type": "openai", "enabled": true,
                    "api_key": "fixture", "base_url": format!("{}/secondary", upstream.uri()),
                    "model": "bounded-model", "runtime_models": ["bounded-model"]
                }
            },
            "model_limits": [
                { "model_pattern": "bounded-", "max_context_tokens": 150_000 },
                { "model_pattern": "local-32k", "max_context_tokens": 32_768 },
                { "model_pattern": "local-475k", "max_context_tokens": 475_000 }
            ]
        });
        std::fs::write(home.path().join("config.json"), config.to_string()).unwrap();
        let state = web::Data::new(
            Box::pin(AppState::new(home.path().to_path_buf()))
                .await
                .unwrap(),
        );
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let request = test::TestRequest::post()
            .uri("/api/v1/bamboo/model-limits/effective")
            .insert_header(("host", "localhost:9562"))
            .set_json(json!({"models": [
                {"model": "bounded-model", "provider": "primary"},
                {"model": "bounded-model", "provider": "secondary"},
                {"model": "bounded-model"},
                {"model": "reported-475k"},
                {"model": "separate-limits"},
                {"model": "output-only"},
                {"model": "unknown"},
                {"model": "local-32k"},
                {"model": "local-475k"},
                {"model": "unknown", "provider": "secondary"},
                {"model": "missing"}
            ]}))
            .to_request();
        let response: Value = test::call_and_read_body_json(&app, request).await;
        let window = |provider: &str, size, uses_context_fallback| {
            json!({
                "provider": provider,
                "max_context_tokens": size,
                "uses_context_fallback": uses_context_fallback
            })
        };
        assert_eq!(
            response["model_windows"],
            json!([
                window("primary", 128_000, false),
                window("secondary", 150_000, false),
                null,
                window("primary", 475_000, false),
                window("primary", 108_000, false),
                window("primary", 475_000, true),
                window("primary", 475_000, true),
                window("primary", 32_768, false),
                window("primary", 475_000, false),
                null,
                null
            ])
        );
        state.shutdown().await;
    }
}
