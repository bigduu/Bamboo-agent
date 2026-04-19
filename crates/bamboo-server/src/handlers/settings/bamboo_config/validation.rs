use std::collections::BTreeMap;

use actix_web::{web, HttpResponse};
use serde_json::Value;

use bamboo_infrastructure::Config;
use crate::config_manager;
use crate::{app_state::AppState, error::AppError};

use super::types::{ValidateConfigResponse, ValidationIssue};

/// Validates a config patch without persisting it.
///
/// # HTTP Route
/// `POST /bamboo/config/validate`
///
/// This endpoint is designed for UX flows that want to surface issues early without
/// forcing strict validation on the permissive `/bamboo/config` patch endpoint.
pub async fn validate_bamboo_config_patch(
    app_state: web::Data<AppState>,
    payload: web::Json<Value>,
) -> Result<HttpResponse, AppError> {
    let patch = payload.into_inner();
    let mut patch_obj = config_manager::assert_json_object(patch)?;
    config_manager::sanitize_root_patch(&mut patch_obj);

    let current = app_state.config.read().await.clone();
    let merged = config_manager::build_merged_config(&current, patch_obj.clone())?;
    let domains = config_manager::domains_for_root_patch(&patch_obj);

    let mut errors: BTreeMap<String, Vec<ValidationIssue>> = BTreeMap::new();

    let mut push_error = |domain: &str, path: &str, message: String| {
        errors
            .entry(domain.to_string())
            .or_default()
            .push(ValidationIssue {
                path: path.to_string(),
                message,
            });
    };

    if domains.proxy {
        if let Err(err) = bamboo_infrastructure::http_client::build_proxy(&merged) {
            push_error("proxy", "http_proxy/https_proxy", err.to_string());
        }
    }

    if domains.provider {
        if let Err(err) = bamboo_infrastructure::validate_provider_config(&merged) {
            let (path, message) = provider_validation_issue(&merged, err.to_string());
            push_error("provider", path, message);
        }
    }

    if domains.setup {
        if let Some(setup) = merged.extra.get("setup") {
            if !setup.is_object() {
                push_error(
                    "setup",
                    "setup",
                    "config.setup must be a JSON object".to_string(),
                );
            }
        }
    }

    let valid = errors.values().all(|items| items.is_empty());
    Ok(HttpResponse::Ok().json(ValidateConfigResponse { valid, errors }))
}

pub(super) fn provider_validation_issue(
    config: &Config,
    fallback_error: String,
) -> (&'static str, String) {
    match config.provider.as_str() {
        "openai" => provider_issue(
            config
                .providers
                .openai
                .as_ref()
                .map(|provider| {
                    !provider.api_key.trim().is_empty()
                        || provider
                            .api_key_encrypted
                            .as_deref()
                            .map(|value| !value.trim().is_empty())
                            .unwrap_or(false)
                })
                .unwrap_or(false),
            "providers.openai.api_key",
            "OpenAI API key is required",
            fallback_error,
        ),
        "anthropic" => provider_issue(
            config
                .providers
                .anthropic
                .as_ref()
                .map(|provider| {
                    !provider.api_key.trim().is_empty()
                        || provider
                            .api_key_encrypted
                            .as_deref()
                            .map(|value| !value.trim().is_empty())
                            .unwrap_or(false)
                })
                .unwrap_or(false),
            "providers.anthropic.api_key",
            "Anthropic API key is required",
            fallback_error,
        ),
        "gemini" => provider_issue(
            config
                .providers
                .gemini
                .as_ref()
                .map(|provider| {
                    !provider.api_key.trim().is_empty()
                        || provider
                            .api_key_encrypted
                            .as_deref()
                            .map(|value| !value.trim().is_empty())
                            .unwrap_or(false)
                })
                .unwrap_or(false),
            "providers.gemini.api_key",
            "Gemini API key is required",
            fallback_error,
        ),
        _ => ("provider", fallback_error),
    }
}

fn provider_issue(
    is_configured: bool,
    missing_path: &'static str,
    missing_message: &'static str,
    fallback_error: String,
) -> (&'static str, String) {
    if is_configured {
        ("provider", fallback_error)
    } else {
        (missing_path, missing_message.to_string())
    }
}
