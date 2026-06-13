//! Configuration patching helpers.
//!
//! The server has multiple endpoints that update different "sections" of the unified `config.json`
//! (provider, proxy, setup, mcp, etc). These helpers keep patch application consistent and safe:
//! - sanitize incoming patches (never accept encrypted secret material from clients)
//! - preserve masked API keys (UI sends placeholders)
//! - compute which runtime side-effects should run (reload provider / reconcile MCP)
//!
//! Pure domain logic (domain types, sanitization, merge) lives in
//! `bamboo_infrastructure::patch`. This module keeps the infrastructure-coupled
//! orchestration functions.
//!
//! Important design note:
//! - `/v1/bamboo/config` is a *permissive* config management endpoint used by setup/UX flows.
//!   It should allow persisting partial config even when the currently-selected provider is
//!   not fully configured yet.
//! - Strict provider validation belongs in provider-specific endpoints like
//!   `/v1/bamboo/settings/provider` (and explicit reload/apply actions).

use serde_json::{Map, Value};

use crate::error::AppError;
use bamboo_config::patch::ProviderApiKeyIntents;
use bamboo_llm::Config;

// Re-export pure domain logic from the config crate so server consumers
// can import through `config_manager`.
pub use bamboo_config::patch::{
    deep_merge_json, domains_for_root_patch, effects_for_root_patch, is_masked_api_key,
    preserve_masked_provider_api_keys, provider_api_key_intents, sanitize_root_patch,
    DomainChanges, PatchEffects, ReloadMode,
};

pub fn sync_provider_api_keys_encrypted_for_patch(
    config: &mut Config,
    intents: &ProviderApiKeyIntents,
) -> Result<(), AppError> {
    for name in intents.providers.iter() {
        match name.as_str() {
            "openai" => {
                if let Some(openai) = config.providers.openai.as_mut() {
                    let api_key = openai.api_key.trim();
                    openai.api_key_encrypted = if api_key.is_empty() {
                        None
                    } else {
                        Some(bamboo_config::encryption::encrypt(api_key).map_err(|e| {
                            AppError::InternalError(anyhow::anyhow!(
                                "Failed to encrypt OpenAI api_key: {e}"
                            ))
                        })?)
                    };
                }
            }
            "anthropic" => {
                if let Some(anthropic) = config.providers.anthropic.as_mut() {
                    let api_key = anthropic.api_key.trim();
                    anthropic.api_key_encrypted = if api_key.is_empty() {
                        None
                    } else {
                        Some(bamboo_config::encryption::encrypt(api_key).map_err(|e| {
                            AppError::InternalError(anyhow::anyhow!(
                                "Failed to encrypt Anthropic api_key: {e}"
                            ))
                        })?)
                    };
                }
            }
            "gemini" => {
                if let Some(gemini) = config.providers.gemini.as_mut() {
                    let api_key = gemini.api_key.trim();
                    gemini.api_key_encrypted = if api_key.is_empty() {
                        None
                    } else {
                        Some(bamboo_config::encryption::encrypt(api_key).map_err(|e| {
                            AppError::InternalError(anyhow::anyhow!(
                                "Failed to encrypt Gemini api_key: {e}"
                            ))
                        })?)
                    };
                }
            }
            "bodhi" => {
                if let Some(bodhi) = config.providers.bodhi.as_mut() {
                    let api_key = bodhi.api_key.trim();
                    bodhi.api_key_encrypted = if api_key.is_empty() {
                        None
                    } else {
                        Some(bamboo_config::encryption::encrypt(api_key).map_err(|e| {
                            AppError::InternalError(anyhow::anyhow!(
                                "Failed to encrypt Bodhi api_key: {e}"
                            ))
                        })?)
                    };
                }
            }
            _ => {}
        }
    }

    for instance_id in intents.provider_instances.iter() {
        if let Some(instance) = config.provider_instances.get_mut(instance_id) {
            let api_key = instance.api_key.trim();
            instance.api_key_encrypted = if api_key.is_empty() {
                None
            } else {
                Some(bamboo_config::encryption::encrypt(api_key).map_err(|e| {
                    AppError::InternalError(anyhow::anyhow!(
                        "Failed to encrypt provider instance api_key for '{instance_id}': {e}"
                    ))
                })?)
            };
        }
    }

    Ok(())
}

pub fn assert_json_object(value: Value) -> Result<Map<String, Value>, AppError> {
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(AppError::BadRequest(
            "config.json must be a JSON object".to_string(),
        )),
    }
}

pub fn build_merged_config(
    current: &Config,
    patch_obj: Map<String, Value>,
) -> Result<Config, AppError> {
    let mut merged = serde_json::to_value(current)
        .map_err(|e| AppError::InternalError(anyhow::anyhow!("Failed to serialize config: {e}")))?;

    deep_merge_json(&mut merged, Value::Object(patch_obj));

    let mut new_config: Config = serde_json::from_value(merged)
        .map_err(|e| AppError::BadRequest(format!("Invalid configuration JSON: {e}")))?;
    new_config.hydrate_proxy_auth_from_encrypted();
    new_config.hydrate_provider_api_keys_from_encrypted();
    new_config.hydrate_provider_instance_api_keys_from_encrypted();
    new_config.hydrate_mcp_secrets_from_encrypted();
    new_config.hydrate_env_vars_from_encrypted();
    new_config.normalize_tool_settings();
    new_config.normalize_skill_settings();

    Ok(new_config)
}
