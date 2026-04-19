use bamboo_infrastructure::Config;
use crate::{app_state::AppState, error::AppError};
use serde_json::Value;

use super::upstream::fetch_models_from_api;

pub(super) fn provider_type_from_payload<'a>(payload: &'a Value, fallback: &'a str) -> &'a str {
    payload
        .get("provider")
        .and_then(|value| value.as_str())
        .unwrap_or(fallback)
}

pub(super) fn build_proxy_aware_http_client(config: &Config) -> Result<reqwest::Client, AppError> {
    bamboo_infrastructure::http_client::build_http_client(config).map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("Failed to build HTTP client: {error}"))
    })
}

pub(super) async fn fetch_models_for_provider(
    app_state: &AppState,
    config: &Config,
    provider_type: &str,
    client: &reqwest::Client,
) -> Result<Vec<String>, AppError> {
    match provider_type {
        "copilot" => fetch_copilot_models(app_state).await,
        "openai" => {
            let openai =
                config.providers.openai.as_ref().ok_or_else(|| {
                    AppError::BadRequest("OpenAI configuration required".to_string())
                })?;
            ensure_api_key(openai.api_key.as_str())?;
            fetch_models_from_api(
                client,
                "openai",
                &openai.api_key,
                openai.base_url.as_deref(),
                openai.request_overrides.as_ref(),
            )
            .await
        }
        "anthropic" => {
            let anthropic = config.providers.anthropic.as_ref().ok_or_else(|| {
                AppError::BadRequest("Anthropic configuration required".to_string())
            })?;
            ensure_api_key(anthropic.api_key.as_str())?;
            fetch_models_from_api(
                client,
                "anthropic",
                &anthropic.api_key,
                anthropic.base_url.as_deref(),
                anthropic.request_overrides.as_ref(),
            )
            .await
        }
        "gemini" => {
            let gemini =
                config.providers.gemini.as_ref().ok_or_else(|| {
                    AppError::BadRequest("Gemini configuration required".to_string())
                })?;
            ensure_api_key(gemini.api_key.as_str())?;
            fetch_models_from_api(
                client,
                "gemini",
                &gemini.api_key,
                gemini.base_url.as_deref(),
                gemini.request_overrides.as_ref(),
            )
            .await
        }
        other => Err(AppError::BadRequest(format!(
            "Unsupported provider: {other}"
        ))),
    }
}

async fn fetch_copilot_models(app_state: &AppState) -> Result<Vec<String>, AppError> {
    let provider = app_state.get_provider().await;
    provider.list_models().await.map_err(|error| {
        let message = error.to_string();
        if message.contains("proxy") || message.contains("407") {
            AppError::ProxyAuthRequired
        } else {
            AppError::InternalError(anyhow::anyhow!("Failed to fetch models: {error}"))
        }
    })
}

fn ensure_api_key(api_key: &str) -> Result<(), AppError> {
    if api_key.trim().is_empty() {
        return Err(AppError::BadRequest("API key not configured".to_string()));
    }
    Ok(())
}
