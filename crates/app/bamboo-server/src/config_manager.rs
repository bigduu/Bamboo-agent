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
    preserve_masked_connect_secrets, preserve_masked_notification_secrets,
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
                    // Never encrypt/persist an env-sourced key — mirrors
                    // refresh_provider_api_keys_encrypted's guard (#253). Without
                    // it, an explicit `api_key: ""` clear of an env-sourced provider
                    // (which preserve_env_sourced_provider_keys refills with the env
                    // secret + api_key_from_env=true) would bake that plaintext
                    // secret into config.json here. An explicit NEW key resets
                    // api_key_from_env=false, so a real override still persists. #373.
                    if !openai.api_key_from_env {
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
            }
            "anthropic" => {
                if let Some(anthropic) = config.providers.anthropic.as_mut() {
                    // Skip env-sourced keys (see openai above). #373.
                    if !anthropic.api_key_from_env {
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
            }
            "gemini" => {
                if let Some(gemini) = config.providers.gemini.as_mut() {
                    // Skip env-sourced keys (see openai above). #373.
                    if !gemini.api_key_from_env {
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
    new_config.hydrate_notifications_from_encrypted();
    new_config.hydrate_connect_platform_tokens_from_encrypted();
    // The serde round-trip above drops every provider's `#[serde(skip_serializing)]`
    // `api_key`; hydration only restores ciphertext-backed keys, so an env-sourced
    // key (no ciphertext, #253) would be silently blanked by any settings PATCH.
    // Copy env-sourced keys back from the live `current` config. #373.
    new_config.preserve_env_sourced_provider_keys(current);
    // Defense-in-depth: restore a provider instance's ciphertext (and
    // re-hydrate its plaintext) if it came out of the merge+hydrate above
    // with neither field, while `current` still has the ciphertext. #515.
    new_config.preserve_provider_instance_encrypted_keys(current);
    new_config.normalize_tool_settings();
    new_config.normalize_skill_settings();
    new_config.normalize_plugin_trust_settings();

    Ok(new_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_config::OpenAIConfig;

    fn env_sourced_openai_config() -> Config {
        let mut config = Config::default();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "sk-env-secret".to_string(),
            api_key_from_env: true,
            ..Default::default()
        });
        config
    }

    // #373: an explicit `api_key: ""` clear of an env-sourced provider must NOT
    // bake the env secret into config.json. build_merged_config restores the live
    // env key (api_key_from_env=true), and the from_env guard in
    // sync_provider_api_keys_encrypted_for_patch must then skip encrypting it.
    #[test]
    fn clearing_env_sourced_key_does_not_persist_the_secret() {
        let current = env_sourced_openai_config();
        let patch: Map<String, Value> =
            serde_json::from_str(r#"{"providers":{"openai":{"api_key":""}}}"#).unwrap();
        let intents = provider_api_key_intents(&patch);
        assert!(
            intents.providers.contains("openai"),
            "empty string is a clear intent"
        );

        let mut merged = build_merged_config(&current, patch).expect("merge");
        sync_provider_api_keys_encrypted_for_patch(&mut merged, &intents).expect("sync");

        let openai = merged.providers.openai.as_ref().unwrap();
        assert!(
            openai.api_key_encrypted.is_none(),
            "env secret must NOT be encrypted to disk on a clear"
        );
        assert!(openai.api_key_from_env, "still flagged env-sourced");
        assert_eq!(openai.api_key, "sk-env-secret", "live env key preserved");
    }

    // A genuine NEW key for an env-sourced provider resets api_key_from_env=false
    // and IS persisted (the explicit override wins).
    #[test]
    fn explicit_new_key_overrides_env_and_persists() {
        let current = env_sourced_openai_config();
        let patch: Map<String, Value> =
            serde_json::from_str(r#"{"providers":{"openai":{"api_key":"sk-brand-new"}}}"#).unwrap();
        let intents = provider_api_key_intents(&patch);

        let mut merged = build_merged_config(&current, patch).expect("merge");
        sync_provider_api_keys_encrypted_for_patch(&mut merged, &intents).expect("sync");

        let openai = merged.providers.openai.as_ref().unwrap();
        assert_eq!(openai.api_key, "sk-brand-new", "explicit override wins");
        assert!(!openai.api_key_from_env, "override clears the env flag");
        assert!(
            openai.api_key_encrypted.is_some(),
            "a real override is encrypted/persisted"
        );
    }

    // A PATCH that doesn't touch api_key must not drop the env-sourced key.
    #[test]
    fn unrelated_patch_preserves_env_key() {
        let current = env_sourced_openai_config();
        let patch: Map<String, Value> =
            serde_json::from_str(r#"{"providers":{"openai":{"model":"gpt-x"}}}"#).unwrap();
        let intents = provider_api_key_intents(&patch);
        assert!(
            !intents.providers.contains("openai"),
            "no api_key in patch → no intent"
        );

        let mut merged = build_merged_config(&current, patch).expect("merge");
        sync_provider_api_keys_encrypted_for_patch(&mut merged, &intents).expect("sync");

        let openai = merged.providers.openai.as_ref().unwrap();
        assert_eq!(
            openai.api_key, "sk-env-secret",
            "env key preserved across unrelated patch"
        );
        assert!(openai.api_key_from_env);
        assert!(openai.api_key_encrypted.is_none(), "still not persisted");
    }

    fn instance_config(api_key: &str) -> Config {
        let mut instance: bamboo_config::ProviderInstanceConfig =
            serde_json::from_value(serde_json::json!({ "provider_type": "openai" })).unwrap();
        instance.api_key = api_key.to_string();
        let mut config = Config::default();
        config
            .provider_instances
            .insert("inst-1".to_string(), instance);
        config
            .refresh_provider_instance_api_keys_encrypted()
            .expect("encrypt");
        config
    }

    // #515: an unrelated settings-save PATCH (one that never mentions
    // `provider_instances` at all) must not wipe an instance's ciphertext —
    // full pipeline (build_merged_config + sync_provider_api_keys_encrypted_for_patch),
    // as exercised by set_bamboo_config.
    #[test]
    fn unrelated_patch_preserves_provider_instance_ciphertext() {
        let current = instance_config("sk-instance-secret");
        let prev_ciphertext = current.provider_instances["inst-1"]
            .api_key_encrypted
            .clone()
            .expect("current should have ciphertext");

        let patch: Map<String, Value> =
            serde_json::from_str(r#"{"http_proxy":"http://example.invalid:8080"}"#).unwrap();
        let intents = provider_api_key_intents(&patch);
        assert!(intents.provider_instances.is_empty());

        let mut merged = build_merged_config(&current, patch).expect("merge");
        sync_provider_api_keys_encrypted_for_patch(&mut merged, &intents).expect("sync");

        let instance = &merged.provider_instances["inst-1"];
        assert_eq!(
            instance.api_key, "sk-instance-secret",
            "plaintext must survive an unrelated save"
        );
        assert_eq!(
            instance.api_key_encrypted.as_deref(),
            Some(prev_ciphertext.as_str()),
            "ciphertext must survive an unrelated save"
        );
    }

    // NOTE: an end-to-end "explicit `api_key: ''` clear actually clears a
    // provider-instance key that already has stored ciphertext" test is
    // deliberately NOT included here. Investigating #515's clear-semantics
    // caveat surfaced a SEPARATE, pre-existing bug (present before this patch,
    // and equally reproducible for legacy `providers.*`, not just
    // `provider_instances`): `build_merged_config` unconditionally calls
    // `hydrate_*_from_encrypted` after the merge, which resurrects the
    // plaintext from ciphertext for ANY empty `api_key` — including one that's
    // empty because the patch explicitly cleared it. By the time
    // `sync_provider_api_keys_encrypted_for_patch` runs, the resurrected
    // (non-empty) plaintext looks like "still has a key", so it re-encrypts
    // the OLD value instead of clearing it. Fixing that needs
    // `ProviderApiKeyIntents` (or an equivalent) to carry the original patch
    // value through to `build_merged_config` so hydration can skip entries
    // the patch explicitly touched — a distinct change from #515's fix, out
    // of scope here. Filed as a follow-up issue; see PR description.
}
