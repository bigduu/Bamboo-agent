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
    clear_connect_ciphertext_for_explicit_clears,
    clear_notification_ciphertext_for_explicit_clears,
    clear_provider_ciphertext_for_explicit_clears, connect_secret_intents, deep_merge_json,
    domains_for_root_patch, effects_for_root_patch, is_masked_api_key, notification_secret_intents,
    preserve_masked_connect_secrets, preserve_masked_notification_secrets,
    preserve_masked_provider_api_keys, preserve_unpatched_notification_secrets,
    preserve_unpatched_provider_secrets, provider_api_key_intents, sanitize_root_patch,
    ConnectSecretIntents, DomainChanges, NotificationSecretIntents, PatchEffects, ReloadMode,
};

pub fn sync_provider_api_keys_encrypted_for_patch(
    config: &mut Config,
    intents: &ProviderApiKeyIntents,
) -> Result<(), AppError> {
    for name in intents.providers.iter() {
        match name.as_str() {
            "openai" => {
                if let Some(openai) = config.providers_mut().openai.as_mut() {
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
                if let Some(anthropic) = config.providers_mut().anthropic.as_mut() {
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
                if let Some(gemini) = config.providers_mut().gemini.as_mut() {
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
                if let Some(bodhi) = config.providers_mut().bodhi.as_mut() {
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

/// Legacy full-config clients may echo the secret-free Core proxy projection,
/// but they may not mutate it without the owned Core section revision.
///
/// Dropping only an exact lock-time echo preserves bounded compatibility while
/// ensuring proxy URLs and the server-managed credential reference cannot
/// bypass the typed Core/proxy-auth APIs.
pub fn remove_unchanged_core_proxy_echo(
    current: &Config,
    patch_obj: &mut Map<String, Value>,
) -> Result<(), AppError> {
    if ["proxy_auth", "proxy_auth_encrypted"]
        .iter()
        .any(|field| patch_obj.contains_key(*field))
    {
        return Err(core_proxy_patch_error());
    }
    let current_value = current.to_compatibility_value()?;
    for field in ["http_proxy", "https_proxy", "proxy_auth_credential_ref"] {
        let Some(incoming) = patch_obj.get(field) else {
            continue;
        };
        if current_value.get(field) != Some(incoming) {
            return Err(core_proxy_patch_error());
        }
        patch_obj.remove(field);
    }
    Ok(())
}

fn core_proxy_patch_error() -> AppError {
    AppError::BadRequest(
        "proxy configuration must be changed through the dedicated revisioned Core and proxy-auth APIs"
            .to_string(),
    )
}

pub fn build_merged_config(
    current: &Config,
    patch_obj: Map<String, Value>,
) -> Result<Config, AppError> {
    // Captured before the merge consumes the patch: which providers/instances
    // this patch explicitly sets or clears — those must NOT get their dropped
    // key carried forward below (#516).
    let api_key_intents = provider_api_key_intents(&patch_obj);
    // Same capture for the other secret domains that flow through this merge
    // (#521 — ntfy `token` / Bark `device_key` / connect `token` and Feishu
    // `app_secret`): must be read before the patch is consumed below.
    let notification_intents = notification_secret_intents(&patch_obj);
    let connect_intents = connect_secret_intents(&patch_obj);

    let mut merged = current
        .to_compatibility_value()
        .map_err(|e| AppError::InternalError(anyhow::anyhow!("Failed to serialize config: {e}")))?;

    deep_merge_json(&mut merged, Value::Object(patch_obj));

    let mut new_config: Config = serde_json::from_value(merged)
        .map_err(|e| AppError::BadRequest(format!("Invalid configuration JSON: {e}")))?;
    // An explicit `api_key: ""` clear must drop the round-tripped ciphertext
    // BEFORE hydration — otherwise hydration refills the plaintext from it and
    // the subsequent sync/save re-encrypts, silently undoing the clear (#516).
    clear_provider_ciphertext_for_explicit_clears(&mut new_config, &api_key_intents);
    // Same treatment for notification/connect secrets (#521) — same rationale,
    // same ordering requirement (before hydration below).
    clear_notification_ciphertext_for_explicit_clears(&mut new_config, &notification_intents);
    clear_connect_ciphertext_for_explicit_clears(&mut new_config, &connect_intents);
    new_config.hydrate_proxy_auth_from_encrypted();
    // Proxy auth is credential-store backed and intentionally omitted from the
    // compatibility JSON round-trip. Root PATCH sanitization forbids changing
    // both the secret and its reference, so preserve the already-hydrated live
    // value just like the CLI dot-path setter does.
    if new_config.proxy_auth_credential_ref == current.proxy_auth_credential_ref {
        new_config.proxy_auth = current.proxy_auth.clone();
    }
    new_config.hydrate_provider_api_keys_from_encrypted();
    new_config.hydrate_provider_instance_api_keys_from_encrypted();
    new_config.hydrate_mcp_secrets_from_encrypted();
    new_config.hydrate_env_vars_from_encrypted();
    new_config.hydrate_notifications_from_encrypted();
    preserve_unpatched_notification_secrets(&mut new_config, current, &notification_intents);
    new_config.hydrate_connect_platform_tokens_from_encrypted();
    // The serde round-trip above drops every provider's `#[serde(skip_serializing)]`
    // `api_key`; hydration only restores ciphertext-backed keys, so an env-sourced
    // key (no ciphertext, #253) would be silently blanked by any settings PATCH.
    // Copy env-sourced keys back from the live `current` config. #373.
    new_config.preserve_env_sourced_provider_keys(current);
    // Same round-trip hazard for any OTHER plaintext-only key (ciphertext still
    // `None` in the live config — e.g. a provider instance freshly created via
    // the instance CRUD endpoints): hydration has nothing to decrypt and the
    // key would vanish from config.json on the next persist. Carry unpatched
    // keys forward from the live config. #515/#516.
    preserve_unpatched_provider_secrets(&mut new_config, current, &api_key_intents);
    // Explicit instance preserve path for #633: carry untouched instance
    // plaintext keys (including instances created before first persist) from
    // current to merged when the merge consumed the merge-time plaintext.
    new_config.preserve_provider_instance_plaintext_keys(current, &api_key_intents);
    new_config.normalize_tool_settings();
    new_config.normalize_skill_settings();
    new_config.normalize_plugin_trust_settings();

    Ok(new_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_config::{credential_ref, OpenAIConfig, ProxyAuth};

    #[test]
    fn unrelated_root_patch_preserves_store_hydrated_proxy_auth() {
        let mut current = Config::default();
        current.proxy_auth_credential_ref =
            Some(credential_ref("proxy", "default", "auth").unwrap());
        current.proxy_auth = Some(ProxyAuth {
            username: "proxy-user".to_string(),
            password: "proxy-password".to_string(),
        });
        let patch: Map<String, Value> =
            serde_json::from_str(r#"{"http_proxy":"http://proxy.example:8080"}"#).unwrap();

        let merged = build_merged_config(&current, patch).expect("merge");

        assert_eq!(
            merged.proxy_auth_credential_ref,
            current.proxy_auth_credential_ref
        );
        let auth = merged
            .proxy_auth
            .as_ref()
            .expect("live proxy auth must survive");
        assert_eq!(auth.username, "proxy-user");
        assert_eq!(auth.password, "proxy-password");
    }

    fn env_sourced_openai_config() -> Config {
        let mut config = Config::default();
        config.providers_mut().openai = Some(OpenAIConfig {
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

        let openai = merged.providers().openai.as_ref().unwrap();
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

        let openai = merged.providers().openai.as_ref().unwrap();
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

        let openai = merged.providers().openai.as_ref().unwrap();
        assert_eq!(
            openai.api_key, "sk-env-secret",
            "env key preserved across unrelated patch"
        );
        assert!(openai.api_key_from_env);
        assert!(openai.api_key_encrypted.is_none(), "still not persisted");
    }

    /// The live in-memory state right after `POST /provider-instances`:
    /// plaintext key, ciphertext still `None` (ciphertext is only ever computed
    /// on `save_to_dir`'s save-time clone).
    fn config_with_plaintext_only_instance(api_key: &str) -> Config {
        let mut config = Config::default();
        let instance: bamboo_config::ProviderInstanceConfig =
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai",
                "api_key": api_key,
            }))
            .expect("valid instance");
        config
            .provider_instances
            .insert("uuid-1".to_string(), instance);
        config
    }

    // #516 regression: the lotus instance-mode 保存配置 sends a defaults/features
    // patch that never mentions the instance. The merge round-trip drops the
    // `skip_serializing` plaintext, hydration has no ciphertext to restore, and
    // the freshly-created instance's key was silently wiped from config.json
    // (config.json.bak kept the previous good copy).
    #[test]
    fn unrelated_patch_preserves_plaintext_only_instance_key() {
        let current = config_with_plaintext_only_instance("sk-instance-live");

        let patch: Map<String, Value> =
            serde_json::from_str(r#"{"features":{"provider_model_ref":true}}"#).unwrap();
        let intents = provider_api_key_intents(&patch);
        assert!(intents.provider_instances.is_empty());

        let mut merged = build_merged_config(&current, patch).expect("merge");
        sync_provider_api_keys_encrypted_for_patch(&mut merged, &intents).expect("sync");

        let instance = merged.provider_instances.get("uuid-1").expect("instance");
        assert_eq!(
            instance.api_key, "sk-instance-live",
            "an unrelated settings PATCH must not lose the instance key (#516)"
        );
    }

    #[test]
    fn unrelated_patch_preserves_fresh_instance_plaintext_key() {
        let current = config_with_plaintext_only_instance("sk-instance-fresh");

        let patch: Map<String, Value> = serde_json::from_str(
            r#"{"defaults":{"chat":{"provider":"openai","model":"gpt-4o-mini","temperature":1}}}"#,
        )
        .unwrap();
        let intents = provider_api_key_intents(&patch);
        assert!(intents.provider_instances.is_empty());

        let mut merged = build_merged_config(&current, patch).expect("merge");
        sync_provider_api_keys_encrypted_for_patch(&mut merged, &intents).expect("sync");

        let instance = merged.provider_instances.get("uuid-1").expect("instance");
        assert_eq!(instance.api_key, "sk-instance-fresh");
    }

    #[test]
    fn explicit_instance_key_in_patch_wins_over_preserved_key() {
        let current = config_with_plaintext_only_instance("sk-instance-live");
        let patch: Map<String, Value> = serde_json::from_str(
            r#"{"provider_instances":{"uuid-1":{"api_key":"sk-instance-updated"}}}"#,
        )
        .unwrap();
        let intents = provider_api_key_intents(&patch);
        assert!(intents.provider_instances.contains("uuid-1"));

        let mut merged = build_merged_config(&current, patch).expect("merge");
        sync_provider_api_keys_encrypted_for_patch(&mut merged, &intents).expect("sync");

        let instance = merged.provider_instances.get("uuid-1").expect("instance");
        assert_eq!(instance.api_key, "sk-instance-updated");
        assert!(
            instance
                .api_key_encrypted
                .as_deref()
                .is_some_and(|cipher| !cipher.is_empty()),
            "explicit instance key must be encrypted after sync"
        );
    }

    // The carry-forward must not resurrect a key the patch explicitly cleared.
    #[test]
    fn explicit_instance_key_clear_still_clears() {
        let current = config_with_plaintext_only_instance("sk-old");

        let patch: Map<String, Value> =
            serde_json::from_str(r#"{"provider_instances":{"uuid-1":{"api_key":""}}}"#).unwrap();
        let intents = provider_api_key_intents(&patch);
        assert!(
            intents.provider_instances.contains("uuid-1"),
            "empty string is a clear intent"
        );

        let mut merged = build_merged_config(&current, patch).expect("merge");
        sync_provider_api_keys_encrypted_for_patch(&mut merged, &intents).expect("sync");

        let instance = merged.provider_instances.get("uuid-1").expect("instance");
        assert!(instance.api_key.is_empty(), "explicit clear must win");
        assert!(instance.api_key_encrypted.is_none());
    }

    // An explicit clear must also win when the live config holds ciphertext in
    // memory — the normal state now that update_config keeps ciphertext in sync
    // (#516). Without the pre-hydration ciphertext drop, hydration would refill
    // the plaintext from the round-tripped ciphertext and sync would re-encrypt
    // it, silently undoing the clear.
    #[test]
    fn explicit_instance_key_clear_wins_over_in_memory_ciphertext() {
        let mut current = config_with_plaintext_only_instance("sk-old");
        current.refresh_encrypted_secrets().expect("refresh");
        assert!(
            current.provider_instances["uuid-1"]
                .api_key_encrypted
                .is_some(),
            "precondition: live config holds ciphertext"
        );

        let patch: Map<String, Value> =
            serde_json::from_str(r#"{"provider_instances":{"uuid-1":{"api_key":""}}}"#).unwrap();
        let intents = provider_api_key_intents(&patch);

        let mut merged = build_merged_config(&current, patch).expect("merge");
        sync_provider_api_keys_encrypted_for_patch(&mut merged, &intents).expect("sync");

        let instance = merged.provider_instances.get("uuid-1").expect("instance");
        assert!(instance.api_key.is_empty(), "explicit clear must win");
        assert!(
            instance.api_key_encrypted.is_none(),
            "ciphertext must be cleared too"
        );
    }

    // #515: an unrelated settings-save PATCH must also preserve an instance
    // whose live config already holds ciphertext in memory (the normal state
    // now that update_config keeps ciphertext in sync) — both the plaintext
    // AND the exact stored ciphertext must survive the round trip.
    #[test]
    fn unrelated_patch_preserves_provider_instance_ciphertext() {
        let mut current = config_with_plaintext_only_instance("sk-instance-secret");
        current.refresh_encrypted_secrets().expect("refresh");
        let prev_ciphertext = current.provider_instances["uuid-1"]
            .api_key_encrypted
            .clone()
            .expect("current should have ciphertext");

        let patch: Map<String, Value> =
            serde_json::from_str(r#"{"http_proxy":"http://example.invalid:8080"}"#).unwrap();
        let intents = provider_api_key_intents(&patch);
        assert!(intents.provider_instances.is_empty());

        let mut merged = build_merged_config(&current, patch).expect("merge");
        sync_provider_api_keys_encrypted_for_patch(&mut merged, &intents).expect("sync");

        let instance = &merged.provider_instances["uuid-1"];
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

    // ── #521: full-pipeline coverage for notification secrets ──────────
    //
    // Mirrors set_bamboo_config's actual call order: preserve_masked_* first
    // (mutating the patch against `current`), THEN build_merged_config, THEN
    // the post-merge re-encrypt (`refresh_notifications_encrypted`, run in
    // production by `Config::refresh_encrypted_secrets` inside
    // `AppState::update_config`).

    fn config_with_notification_secrets(ntfy_token: &str, bark_key: &str) -> Config {
        let mut config = Config::default();
        config.notifications.ntfy.token = Some(ntfy_token.to_string());
        config.notifications.bark.device_key = Some(bark_key.to_string());
        config.refresh_encrypted_secrets().expect("refresh");
        config
    }

    fn merge_notifications_patch(current: &Config, patch_json: &str) -> Config {
        let mut patch_obj: Map<String, Value> = serde_json::from_str(patch_json).unwrap();
        preserve_masked_notification_secrets(&mut patch_obj, current);
        let mut merged = build_merged_config(current, patch_obj).expect("merge");
        merged.refresh_encrypted_secrets().expect("refresh");
        merged
    }

    #[test]
    fn explicit_notification_secret_clear_wins_over_in_memory_ciphertext() {
        let current = config_with_notification_secrets("ntfy-secret", "bark-secret");
        assert!(current.notifications.ntfy.token_encrypted.is_some());
        assert!(current.notifications.bark.device_key_encrypted.is_some());

        let merged = merge_notifications_patch(
            &current,
            r#"{"notifications":{"ntfy":{"token":""},"bark":{"device_key":""}}}"#,
        );

        assert!(
            merged
                .notifications
                .ntfy
                .token
                .as_deref()
                .unwrap_or("")
                .is_empty(),
            "explicit clear must win"
        );
        assert!(
            merged.notifications.ntfy.token_encrypted.is_none(),
            "ciphertext must be cleared too (#521)"
        );
        assert!(merged
            .notifications
            .bark
            .device_key
            .as_deref()
            .unwrap_or("")
            .is_empty());
        assert!(merged.notifications.bark.device_key_encrypted.is_none());
    }

    #[test]
    fn unrelated_patch_preserves_notification_secrets() {
        // Compatibility merging must carry store-hydrated plaintext forward
        // even though it is intentionally absent from serialized JSON.
        let current = config_with_notification_secrets("ntfy-secret", "bark-secret");

        let merged =
            merge_notifications_patch(&current, r#"{"http_proxy":"http://example.invalid:8080"}"#);

        assert_eq!(
            merged.notifications.ntfy.token.as_deref(),
            Some("ntfy-secret"),
            "an unrelated settings PATCH must not lose the ntfy token"
        );
        assert!(merged.notifications.ntfy.token_encrypted.is_some());
        assert_eq!(
            merged.notifications.bark.device_key.as_deref(),
            Some("bark-secret"),
            "an unrelated settings PATCH must not lose the Bark device key"
        );
        assert!(merged.notifications.bark.device_key_encrypted.is_some());
    }

    #[test]
    fn masked_notification_secret_placeholder_preserves_value() {
        let current = config_with_notification_secrets("ntfy-secret", "bark-secret");

        let merged = merge_notifications_patch(
            &current,
            r#"{"notifications":{"ntfy":{"token":"****...****"},"bark":{"device_key":"****...****"}}}"#,
        );

        assert_eq!(
            merged.notifications.ntfy.token.as_deref(),
            Some("ntfy-secret")
        );
        assert!(merged.notifications.ntfy.token_encrypted.is_some());
        assert_eq!(
            merged.notifications.bark.device_key.as_deref(),
            Some("bark-secret")
        );
        assert!(merged.notifications.bark.device_key_encrypted.is_some());
    }

    #[test]
    fn new_notification_secret_value_replaces_and_encrypts() {
        let current = config_with_notification_secrets("ntfy-old", "bark-old");

        let merged = merge_notifications_patch(
            &current,
            r#"{"notifications":{"ntfy":{"token":"ntfy-new"},"bark":{"device_key":"bark-new"}}}"#,
        );

        assert_eq!(merged.notifications.ntfy.token.as_deref(), Some("ntfy-new"));
        assert!(merged.notifications.ntfy.token_encrypted.is_some());
        assert_eq!(
            merged.notifications.bark.device_key.as_deref(),
            Some("bark-new")
        );
        assert!(merged.notifications.bark.device_key_encrypted.is_some());
    }

    // ── #521: full-pipeline coverage for connect platform secrets ──────

    fn config_with_connect_platform(platform_type: &str, token: &str) -> Config {
        let mut config = Config::default();
        let platform: bamboo_config::ConnectPlatformConfig =
            serde_json::from_value(serde_json::json!({
                "type": platform_type,
                "token": token,
            }))
            .expect("valid platform");
        config.connect.platforms = vec![platform];
        config.refresh_encrypted_secrets().expect("refresh");
        config
    }

    fn merge_connect_patch(current: &Config, patch_json: &str) -> Config {
        let mut patch_obj: Map<String, Value> = serde_json::from_str(patch_json).unwrap();
        preserve_masked_connect_secrets(&mut patch_obj, current);
        let mut merged = build_merged_config(current, patch_obj).expect("merge");
        merged.refresh_encrypted_secrets().expect("refresh");
        merged
    }

    #[test]
    fn explicit_connect_token_clear_wins_over_in_memory_ciphertext() {
        let current = config_with_connect_platform("telegram", "tg-secret-token");
        assert!(current.connect.platforms[0].token_encrypted.is_some());

        let merged = merge_connect_patch(
            &current,
            r#"{"connect":{"platforms":[{"type":"telegram","token":""}]}}"#,
        );

        assert!(
            merged.connect.platforms[0]
                .token
                .as_deref()
                .unwrap_or("")
                .is_empty(),
            "explicit clear must win"
        );
        assert!(
            merged.connect.platforms[0].token_encrypted.is_none(),
            "ciphertext must be cleared too (#521)"
        );
    }

    #[test]
    fn explicit_connect_app_secret_clear_wins_over_in_memory_ciphertext() {
        let mut current = Config::default();
        let platform: bamboo_config::ConnectPlatformConfig =
            serde_json::from_value(serde_json::json!({
                "type": "feishu",
                "app_id": "cli_x",
                "app_secret": "feishu-secret",
            }))
            .expect("valid platform");
        current.connect.platforms = vec![platform];
        current.refresh_encrypted_secrets().expect("refresh");
        assert!(current.connect.platforms[0].app_secret_encrypted.is_some());

        let merged = merge_connect_patch(
            &current,
            r#"{"connect":{"platforms":[{"type":"feishu","app_id":"cli_x","app_secret":""}]}}"#,
        );

        assert!(
            merged.connect.platforms[0]
                .app_secret
                .as_deref()
                .unwrap_or("")
                .is_empty(),
            "explicit clear must win"
        );
        assert!(
            merged.connect.platforms[0].app_secret_encrypted.is_none(),
            "ciphertext must be cleared too (#521)"
        );
    }

    #[test]
    fn unrelated_patch_preserves_connect_token() {
        // Same NOTE as `unrelated_patch_preserves_notification_secrets`: connect
        // ciphertext is unconditionally recomputed by `refresh_encrypted_secrets`
        // on every write, so only plaintext survival + ciphertext presence are
        // asserted.
        let current = config_with_connect_platform("telegram", "tg-secret-token");

        let merged =
            merge_connect_patch(&current, r#"{"http_proxy":"http://example.invalid:8080"}"#);

        assert_eq!(
            merged.connect.platforms[0].token.as_deref(),
            Some("tg-secret-token"),
            "an unrelated settings PATCH must not lose the connect platform token"
        );
        assert!(merged.connect.platforms[0].token_encrypted.is_some());
    }

    #[test]
    fn masked_connect_token_placeholder_preserves_value() {
        let current = config_with_connect_platform("telegram", "tg-secret-token");

        let merged = merge_connect_patch(
            &current,
            r#"{"connect":{"platforms":[{"type":"telegram","token":"****...****"}]}}"#,
        );

        assert_eq!(
            merged.connect.platforms[0].token.as_deref(),
            Some("tg-secret-token")
        );
        assert!(merged.connect.platforms[0].token_encrypted.is_some());
    }

    #[test]
    fn new_connect_token_value_replaces_and_encrypts() {
        let current = config_with_connect_platform("telegram", "tg-old-token");

        let merged = merge_connect_patch(
            &current,
            r#"{"connect":{"platforms":[{"type":"telegram","token":"tg-new-token"}]}}"#,
        );

        assert_eq!(
            merged.connect.platforms[0].token.as_deref(),
            Some("tg-new-token")
        );
        assert!(merged.connect.platforms[0].token_encrypted.is_some());
    }

    // ── #505: RFC7386-style null-delete through the FULL production pipeline ──
    //
    // These mirror the existing `""`-clear tests above (same helpers, same
    // call order: preserve_masked_* → build_merged_config →
    // sync_provider_api_keys_encrypted_for_patch / refresh_encrypted_secrets)
    // but exercise a `null` clear instead, proving the new delete semantics
    // compose correctly with the #516/#521 secret machinery end-to-end, not
    // just at the `bamboo-config`-crate unit level.

    #[test]
    fn null_instance_api_key_clear_wins_over_in_memory_ciphertext() {
        // Same scenario as `explicit_instance_key_clear_wins_over_in_memory_ciphertext`
        // above, but the client sends `null` instead of `""`.
        let mut current = config_with_plaintext_only_instance("sk-old");
        current.refresh_encrypted_secrets().expect("refresh");
        assert!(
            current.provider_instances["uuid-1"]
                .api_key_encrypted
                .is_some(),
            "precondition: live config holds ciphertext"
        );

        let patch: Map<String, Value> =
            serde_json::from_str(r#"{"provider_instances":{"uuid-1":{"api_key":null}}}"#).unwrap();
        let intents = provider_api_key_intents(&patch);
        assert!(
            intents.provider_instances.contains("uuid-1"),
            "null must register as a clear intent, same as \"\""
        );

        let mut merged = build_merged_config(&current, patch).expect("merge");
        sync_provider_api_keys_encrypted_for_patch(&mut merged, &intents).expect("sync");

        let instance = merged.provider_instances.get("uuid-1").expect("instance");
        assert!(instance.api_key.is_empty(), "null clear must win");
        assert!(
            instance.api_key_encrypted.is_none(),
            "ciphertext must be cleared too, not resurrected via hydration"
        );
    }

    #[test]
    fn null_deletes_a_whole_provider_instance_entry() {
        // The other half of #505: deleting an entire map entry (not just
        // clearing one field within it). Two instances exist; the patch
        // null-deletes one by id and must leave the other untouched.
        let mut current = config_with_plaintext_only_instance("sk-keep-me");
        let second: bamboo_config::ProviderInstanceConfig =
            serde_json::from_value(serde_json::json!({
                "provider_type": "anthropic",
                "label": "Delete Me",
            }))
            .expect("valid instance");
        current
            .provider_instances
            .insert("uuid-2".to_string(), second);

        let patch: Map<String, Value> =
            serde_json::from_str(r#"{"provider_instances":{"uuid-2":null}}"#).unwrap();
        let intents = provider_api_key_intents(&patch);

        let mut merged = build_merged_config(&current, patch).expect("merge");
        sync_provider_api_keys_encrypted_for_patch(&mut merged, &intents).expect("sync");

        assert!(
            !merged.provider_instances.contains_key("uuid-2"),
            "the null-targeted instance must be gone"
        );
        assert_eq!(
            merged
                .provider_instances
                .get("uuid-1")
                .map(|i| &i.provider_type),
            Some(&"openai".to_string()),
            "the untouched sibling instance must survive"
        );
    }

    #[test]
    fn null_ntfy_token_clear_wins_over_in_memory_ciphertext() {
        // Same scenario as `explicit_notification_secret_clear_wins_over_in_memory_ciphertext`
        // above, but only ntfy is cleared, and via `null` rather than `""`.
        let current = config_with_notification_secrets("ntfy-secret", "bark-secret");

        let merged =
            merge_notifications_patch(&current, r#"{"notifications":{"ntfy":{"token":null}}}"#);

        assert!(
            merged
                .notifications
                .ntfy
                .token
                .as_deref()
                .unwrap_or("")
                .is_empty(),
            "null clear must win"
        );
        assert!(
            merged.notifications.ntfy.token_encrypted.is_none(),
            "ciphertext must be cleared too, not resurrected via hydration"
        );
        // Sibling secret domain (bark), untouched by the patch, must survive.
        assert_eq!(
            merged.notifications.bark.device_key.as_deref(),
            Some("bark-secret")
        );
        assert!(merged.notifications.bark.device_key_encrypted.is_some());
    }

    #[test]
    fn null_connect_token_clear_wins_over_in_memory_ciphertext() {
        let current = config_with_connect_platform("telegram", "tg-old-token");

        let merged = merge_connect_patch(
            &current,
            r#"{"connect":{"platforms":[{"type":"telegram","token":null}]}}"#,
        );

        assert!(
            merged.connect.platforms[0]
                .token
                .as_deref()
                .unwrap_or("")
                .is_empty(),
            "null clear must win"
        );
        assert!(
            merged.connect.platforms[0].token_encrypted.is_none(),
            "ciphertext must be cleared too, not resurrected via hydration"
        );
    }

    #[test]
    fn null_subagents_claude_code_binary_is_unset_and_does_not_crash_the_patch() {
        // The exact motivating case from issue #505, exercised through the
        // full `build_merged_config` pipeline (not just `deep_merge_json` in
        // isolation): an `Option<String>` field written once must become
        // un-settable via a later PATCH.
        let mut current = Config::default();
        current.subagents_mut().claude_code_binary = Some("/usr/local/bin/claude".to_string());
        current.subagents_mut().executor = Some("claude_code".to_string());

        let patch: Map<String, Value> =
            serde_json::from_str(r#"{"subagents":{"claude_code_binary":null}}"#).unwrap();
        let merged = build_merged_config(&current, patch).expect("merge must not error");

        assert_eq!(merged.subagents().claude_code_binary, None);
        // Sibling field untouched by the patch survives — proves this was a
        // surgical field-level delete, not a whole-subtree reset.
        assert_eq!(merged.subagents().executor, Some("claude_code".to_string()));
    }
}
