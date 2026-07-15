//! `GET /api/v1/execute/defaults` — the public twin of the connect bridge's
//! per-message run-config resolution (issue #480).
//!
//! An external connector (or any REST/WS client that isn't the in-proc
//! connect bridge) needs to know "what model/provider/prompt/workspace/gold
//! config would the server currently resolve for a run with no per-request
//! overrides" — e.g. to display it, or to omit `model` on `POST /chat` and
//! trust the server's resolution. Before this endpoint, the only way to get
//! that was `GET /bamboo/config`, which returns the raw (redacted) `Config`
//! and would force every external caller to reimplement the
//! `model_areas`/`model_config_helper`/`prompt_defaults`/
//! `context::assemble_system_prompt` resolution cascade by hand — and drift
//! from it over time.
//!
//! This handler calls the SAME shared resolver
//! ([`bamboo_engine::resolved_defaults::resolve_default_run_config`]) as the
//! connect bridge (`connect::bridge::resolve_connect_run_config`) — there is
//! exactly one implementation of this cascade.
//!
//! No secrets are surfaced: the response carries resolved model/provider
//! *names*, the assembled system prompt, the workspace path, and
//! `gold_config` (flags/goal text/prompt tuning only — see
//! `bamboo_engine::config::GoldConfig`, which has no credential fields).

use actix_web::{web, HttpResponse, Responder};
use serde::Serialize;

use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_engine::config::GoldConfig;

use crate::app_state::AppState;

/// Response body for `GET /api/v1/execute/defaults`.
#[derive(Debug, Serialize)]
pub struct ExecuteDefaultsResponse {
    /// Primary (main chat) model the server would currently use. `None`/empty
    /// when no model is configured at all.
    pub model: Option<String>,
    /// Provider routing key for the primary model.
    pub provider: Option<String>,
    /// Underlying provider type (e.g. `openai`, `anthropic`, `copilot`).
    pub provider_type: Option<String>,
    /// Resolved reasoning effort, if any.
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Assembled system prompt (base template + workspace note) — what a new
    /// session's first system message would contain.
    pub system_prompt: String,
    /// The base template alone, before workspace assembly.
    pub base_system_prompt: String,
    /// Default workspace path, if configured.
    pub workspace_path: Option<String>,
    /// Effective Gold config (global default; no session override applies
    /// here since this endpoint has no session context).
    pub gold_config: Option<GoldConfig>,
    /// Fast/cheap auxiliary model.
    pub fast_model: Option<String>,
    /// Memory/background auxiliary model.
    pub background_model: Option<String>,
    /// Task-summary/compression auxiliary model.
    pub summarization_model: Option<String>,
}

/// `GET /api/v1/execute/defaults`
///
/// Returns the server-side resolved run configuration with no per-request
/// overrides applied — the same resolution a fresh `POST /chat` (with no
/// explicit `model`) or a connect-bridged chat message would get.
pub async fn handler(state: web::Data<AppState>) -> impl Responder {
    let config_snapshot = state.config.read().await.clone();
    let resolved = bamboo_engine::resolved_defaults::resolve_default_run_config(
        &config_snapshot,
        &state.provider_registry,
    );

    HttpResponse::Ok().json(ExecuteDefaultsResponse {
        model: resolved.model_roster.model.clone(),
        provider: resolved.model_roster.provider_name.clone(),
        provider_type: resolved.model_roster.provider_type.clone(),
        reasoning_effort: resolved.reasoning_effort,
        system_prompt: resolved.system_prompt,
        base_system_prompt: resolved.base_system_prompt,
        workspace_path: resolved.workspace_path,
        gold_config: resolved.gold_config,
        fast_model: resolved.model_roster.fast_model(),
        background_model: resolved.model_roster.background_model(),
        summarization_model: resolved.model_roster.summarization_model(),
    })
}

#[cfg(test)]
mod tests {
    use actix_web::{test, web, App};
    use serde_json::Value;
    use tempfile::tempdir;

    use crate::routes::configure_routes;
    use crate::AppState;

    async fn new_state() -> web::Data<AppState> {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        )
    }

    #[actix_web::test]
    async fn defaults_endpoint_returns_resolved_area_models_not_raw_config_echo() {
        let state = new_state().await;

        // Configure distinct area models so a passing test proves RESOLUTION
        // happened (fast/background/summarization differ from the chat model),
        // not that the handler just echoed a single config field three times.
        {
            let mut config = state.config.write().await;
            config.features.provider_model_ref = true;
            config.provider = "openai".to_string();
            // `resolve_fast_model`/`resolve_background_model`/`resolve_task_summary_model`
            // route each `ProviderModelRef` through the live provider registry
            // (`ProviderModelRouter::route`), which only succeeds for a provider
            // that's actually registered — an API key is what registers it.
            config.providers.openai = Some(bamboo_config::OpenAIConfig {
                api_key: "test-key".to_string(),
                ..Default::default()
            });
            config.defaults = Some(bamboo_config::DefaultsConfig {
                chat: bamboo_domain::ProviderModelRef::new("openai", "gpt-chat"),
                fast: Some(bamboo_domain::ProviderModelRef::new("openai", "gpt-fast")),
                task_summary: Some(bamboo_domain::ProviderModelRef::new(
                    "openai",
                    "gpt-summary",
                )),
                vision: None,
                memory_background: Some(bamboo_domain::ProviderModelRef::new(
                    "openai",
                    "gpt-memory",
                )),
                planning: None,
                search: None,
                code_review: None,
                sub_agent: None,
                subagent_models: Default::default(),
            });
            state
                .provider_registry
                .reload_from_config(&config, state.app_data_dir.clone())
                .await
                .expect("provider registry should reload with the openai provider registered");
        }

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/execute/defaults")
                .to_request(),
        )
        .await;

        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["model"], "gpt-chat");
        assert_eq!(body["fast_model"], "gpt-fast");
        assert_eq!(body["background_model"], "gpt-memory");
        assert_eq!(body["summarization_model"], "gpt-summary");
    }

    #[actix_web::test]
    async fn defaults_endpoint_never_leaks_api_keys() {
        let state = new_state().await;
        {
            let mut config = state.config.write().await;
            config.provider = "openai".to_string();
            config.providers.openai = Some(bamboo_config::OpenAIConfig {
                api_key: "sk-super-secret-value".to_string(),
                model: Some("gpt-4o".to_string()),
                ..Default::default()
            });
        }

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/execute/defaults")
                .to_request(),
        )
        .await;

        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);
        assert!(!body_str.contains("sk-super-secret-value"));
    }
}
