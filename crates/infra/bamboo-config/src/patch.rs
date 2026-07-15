//! Config patch domain logic.
//!
//! Pure business rules for interpreting, sanitizing, and merging
//! partial config patches. Used by the server's config management endpoints.

use serde_json::{Map, Value};

use crate::Config;

/// Detect whether a string value looks like a masked/placeholder API key.
///
/// Only a value consisting entirely of `*`/`.` characters counts (the redaction
/// placeholder `****...****`, or truncated/retyped variants of it). Substring
/// matching is deliberately avoided: the UI prefills the placeholder into the
/// editable field, so a paste that doesn't fully clear it yields values like
/// `****...****sk-new…` — treating those as "keep existing key" silently
/// discards the user's new token (#430).
pub fn is_masked_api_key(value: &str) -> bool {
    let v = value.trim();
    // Empty string is treated as an explicit "clear" signal (we control all clients).
    !v.is_empty() && v.chars().all(|c| c == '*' || c == '.')
}

/// Extract API-key update intents from a config patch.
///
/// Masked placeholders are ignored — they signal "keep existing key".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderApiKeyIntents {
    pub providers: std::collections::BTreeSet<String>,
    pub provider_instances: std::collections::BTreeSet<String>,
}

pub fn provider_api_key_intents(patch_obj: &Map<String, Value>) -> ProviderApiKeyIntents {
    let mut intents = ProviderApiKeyIntents::default();

    if let Some(root) = patch_obj.get("providers").and_then(|v| v.as_object()) {
        for (provider_name, provider_patch) in root.iter() {
            let Some(obj) = provider_patch.as_object() else {
                continue;
            };
            let Some(api_key) = obj.get("api_key").and_then(|v| v.as_str()) else {
                continue;
            };
            if is_masked_api_key(api_key) {
                continue;
            }
            intents.providers.insert(provider_name.clone());
        }
    }

    if let Some(root) = patch_obj
        .get("provider_instances")
        .and_then(|v| v.as_object())
    {
        for (instance_id, instance_patch) in root.iter() {
            let Some(obj) = instance_patch.as_object() else {
                continue;
            };
            let Some(api_key) = obj.get("api_key").and_then(|v| v.as_str()) else {
                continue;
            };
            if is_masked_api_key(api_key) {
                continue;
            }
            intents.provider_instances.insert(instance_id.clone());
        }
    }

    intents
}

/// Reload strategy to apply after a config patch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadMode {
    None,
    /// Attempt reload, but do not fail the request if reload fails.
    BestEffort,
    /// Reload must succeed; otherwise the request fails.
    Strict,
}

/// Side-effects determined from a config patch.
#[derive(Debug, Clone, Copy)]
pub struct PatchEffects {
    pub reload_provider: ReloadMode,
    pub reconcile_mcp: bool,
}

/// Which config domains are touched by a patch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DomainChanges {
    pub provider: bool,
    pub proxy: bool,
    pub setup: bool,
    pub mcp: bool,
    pub keyword_masking: bool,
    pub hooks: bool,
    pub model_mapping: bool,
}

/// Classify which config domains are affected by a patch.
pub fn domains_for_root_patch(patch_obj: &Map<String, Value>) -> DomainChanges {
    let mut changes = DomainChanges::default();

    for key in patch_obj.keys() {
        match key.as_str() {
            // Provider domain
            "provider"
            | "providers"
            | "provider_instances"
            | "default_provider_instance"
            | "model"
            | "defaults"
            | "features" => changes.provider = true,

            // Proxy domain
            "http_proxy"
            | "https_proxy"
            | "proxy_auth"
            | "proxy_auth_encrypted"
            | "http_proxy_auth_encrypted"
            | "https_proxy_auth_encrypted" => changes.proxy = true,

            // Setup domain (stored under Config.extra via serde flatten)
            "setup" => changes.setup = true,

            // MCP domain
            "mcp" | "mcpServers" => changes.mcp = true,

            // Other known config domains
            "keyword_masking" => changes.keyword_masking = true,
            "hooks" => changes.hooks = true,
            "anthropic_model_mapping" | "gemini_model_mapping" => changes.model_mapping = true,

            _ => {}
        }
    }

    changes
}

/// Determine what side-effects a config patch should trigger.
pub fn effects_for_root_patch(patch_obj: &Map<String, Value>) -> PatchEffects {
    let domains = domains_for_root_patch(patch_obj);

    let touches_provider = domains.provider || domains.hooks || domains.keyword_masking;
    let touches_proxy = domains.proxy;
    let touches_mcp = domains.mcp;

    PatchEffects {
        reload_provider: if touches_provider || touches_proxy {
            ReloadMode::BestEffort
        } else {
            ReloadMode::None
        },
        // SSE-based MCP servers are HTTP clients and must respect proxy settings.
        // Reconcile so proxy changes take effect without a restart.
        reconcile_mcp: touches_mcp || touches_proxy,
    }
}

/// Remove forbidden fields from a config patch before application.
///
/// Strips encrypted auth material, data_dir, and MCP secret fields
/// that should never be set directly by clients.
pub fn sanitize_root_patch(patch_obj: &mut Map<String, Value>) {
    // Never allow clients to modify proxy auth fields or data_dir via this endpoint.
    patch_obj.remove("proxy_auth");
    patch_obj.remove("proxy_auth_encrypted");
    // Legacy/compat proxy auth keys (written by older Bodhi/Tauri builds).
    patch_obj.remove("http_proxy_auth_encrypted");
    patch_obj.remove("https_proxy_auth_encrypted");
    patch_obj.remove("data_dir");

    // Never allow clients to set encrypted key material directly.
    if let Some(providers) = patch_obj
        .get_mut("providers")
        .and_then(|v| v.as_object_mut())
    {
        for (_provider_name, provider_cfg) in providers.iter_mut() {
            let Some(obj) = provider_cfg.as_object_mut() else {
                continue;
            };
            obj.remove("api_key_encrypted");
        }
    }

    if let Some(provider_instances) = patch_obj
        .get_mut("provider_instances")
        .and_then(|v| v.as_object_mut())
    {
        for (_instance_id, instance_cfg) in provider_instances.iter_mut() {
            let Some(obj) = instance_cfg.as_object_mut() else {
                continue;
            };
            obj.remove("api_key_encrypted");
        }
    }

    // Never allow clients to set encrypted notification-channel secrets directly.
    if let Some(notifications) = patch_obj
        .get_mut("notifications")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(ntfy) = notifications
            .get_mut("ntfy")
            .and_then(|v| v.as_object_mut())
        {
            ntfy.remove("token_encrypted");
        }
        if let Some(bark) = notifications
            .get_mut("bark")
            .and_then(|v| v.as_object_mut())
        {
            bark.remove("device_key_encrypted");
        }
    }

    // Never allow clients to set encrypted bamboo-connect platform secrets
    // (token, Feishu app_secret) directly.
    if let Some(platforms) = patch_obj
        .get_mut("connect")
        .and_then(|c| c.get_mut("platforms"))
        .and_then(|v| v.as_array_mut())
    {
        for platform in platforms.iter_mut() {
            if let Some(obj) = platform.as_object_mut() {
                obj.remove("token_encrypted");
                obj.remove("app_secret_encrypted");
            }
        }
    }

    // Never allow clients to set encrypted secret material directly.
    //
    // Canonical MCP format:
    //   "mcpServers": { "<id>": { env_encrypted, headers[*].value_encrypted, ... } }
    if let Some(mcp_servers) = patch_obj
        .get_mut("mcpServers")
        .and_then(|v| v.as_object_mut())
    {
        for (_id, server) in mcp_servers.iter_mut() {
            let Some(server_obj) = server.as_object_mut() else {
                continue;
            };
            server_obj.remove("env_encrypted");
            if let Some(headers) = server_obj.get_mut("headers").and_then(|v| v.as_array_mut()) {
                for header in headers.iter_mut() {
                    let Some(header_obj) = header.as_object_mut() else {
                        continue;
                    };
                    header_obj.remove("value_encrypted");
                }
            }
        }
    }

    // Legacy MCP shape:
    //   "mcp": { "servers": [ { transport: { env_encrypted / headers[*].value_encrypted } } ] }
    if let Some(servers) = patch_obj
        .get_mut("mcp")
        .and_then(|m| m.get_mut("servers"))
        .and_then(|v| v.as_array_mut())
    {
        for server in servers.iter_mut() {
            let Some(server_obj) = server.as_object_mut() else {
                continue;
            };
            let Some(transport) = server_obj
                .get_mut("transport")
                .and_then(|v| v.as_object_mut())
            else {
                continue;
            };

            match transport.get("type").and_then(|v| v.as_str()) {
                Some("stdio") => {
                    transport.remove("env_encrypted");
                }
                Some("sse") => {
                    if let Some(headers) =
                        transport.get_mut("headers").and_then(|v| v.as_array_mut())
                    {
                        for header in headers.iter_mut() {
                            let Some(header_obj) = header.as_object_mut() else {
                                continue;
                            };
                            header_obj.remove("value_encrypted");
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

/// Replace masked API key placeholders in a patch with the current config's plain keys.
///
/// The UI sends masked values (e.g. `****...****`) to indicate "do not change this key".
/// This function resolves those back to the existing plain-text key from the live config.
pub fn preserve_masked_provider_api_keys(patch_obj: &mut Map<String, Value>, current: &Config) {
    if let Some(patch_providers) = patch_obj
        .get_mut("providers")
        .and_then(|v| v.as_object_mut())
    {
        for (provider_name, provider_patch) in patch_providers.iter_mut() {
            let Some(patch_cfg_obj) = provider_patch.as_object_mut() else {
                continue;
            };

            let Some(api_key) = patch_cfg_obj.get("api_key").and_then(|v| v.as_str()) else {
                continue;
            };
            if !is_masked_api_key(api_key) {
                continue;
            }

            let existing_plain = match provider_name.as_str() {
                "openai" => current.providers.openai.as_ref().map(|c| c.api_key.clone()),
                "anthropic" => current
                    .providers
                    .anthropic
                    .as_ref()
                    .map(|c| c.api_key.clone()),
                "gemini" => current.providers.gemini.as_ref().map(|c| c.api_key.clone()),
                "bodhi" => current.providers.bodhi.as_ref().map(|c| c.api_key.clone()),
                _ => None,
            };

            if let Some(existing_plain) = existing_plain {
                if !existing_plain.trim().is_empty() {
                    patch_cfg_obj.insert("api_key".to_string(), Value::String(existing_plain));
                } else {
                    patch_cfg_obj.remove("api_key");
                }
            } else {
                patch_cfg_obj.remove("api_key");
            }
        }
    }

    if let Some(patch_instances) = patch_obj
        .get_mut("provider_instances")
        .and_then(|v| v.as_object_mut())
    {
        for (instance_id, instance_patch) in patch_instances.iter_mut() {
            let Some(patch_cfg_obj) = instance_patch.as_object_mut() else {
                continue;
            };

            let Some(api_key) = patch_cfg_obj.get("api_key").and_then(|v| v.as_str()) else {
                continue;
            };
            if !is_masked_api_key(api_key) {
                continue;
            }

            let existing_plain = current
                .provider_instances
                .get(instance_id)
                .map(|instance| instance.api_key.clone());

            if let Some(existing_plain) = existing_plain {
                if !existing_plain.trim().is_empty() {
                    patch_cfg_obj.insert("api_key".to_string(), Value::String(existing_plain));
                } else {
                    patch_cfg_obj.remove("api_key");
                }
            } else {
                patch_cfg_obj.remove("api_key");
            }
        }
    }
}

/// Replace masked notification-channel secret placeholders (ntfy `token`, Bark
/// `device_key`) in a patch with the current config's plaintext values.
///
/// Mirrors [`preserve_masked_provider_api_keys`]: the UI sends the masked
/// placeholder to mean "do not change this secret"; this resolves that back to
/// the live plaintext so the merge doesn't wipe it. A masked value with no
/// existing plaintext (nothing configured yet) is dropped from the patch
/// entirely, same as an unset key.
pub fn preserve_masked_notification_secrets(patch_obj: &mut Map<String, Value>, current: &Config) {
    let Some(notifications) = patch_obj
        .get_mut("notifications")
        .and_then(|v| v.as_object_mut())
    else {
        return;
    };

    if let Some(ntfy) = notifications
        .get_mut("ntfy")
        .and_then(|v| v.as_object_mut())
    {
        preserve_masked_secret_field(ntfy, "token", current.notifications.ntfy.token.as_deref());
    }

    if let Some(bark) = notifications
        .get_mut("bark")
        .and_then(|v| v.as_object_mut())
    {
        preserve_masked_secret_field(
            bark,
            "device_key",
            current.notifications.bark.device_key.as_deref(),
        );
    }
}

/// Replace masked bamboo-connect platform `token` placeholders in a patch with
/// the current config's plaintext values.
///
/// Mirrors [`preserve_masked_notification_secrets`]. `connect.platforms` is a
/// list (not a single object like ntfy/bark), so entries are matched
/// POSITIONALLY: patch index `i` is resolved against `current.connect.platforms[i]`.
/// This mirrors how the settings UI round-trips the list (it always sends the
/// full array back in the same order it was fetched in — the same convention
/// `env_vars`' full-array replace relies on) — reordering platforms in the
/// same request as leaving a token masked is not supported and drops that
/// entry's token, same as no plaintext being configured yet.
///
/// Known limitation (issue #454 follow-up), still present: there is no
/// stable per-entry id, so reordering two entries of the SAME
/// `platform_type` (e.g. two `"telegram"` entries, once multi-bot is
/// supported) at the same time as leaving one masked is indistinguishable
/// from "not reordered" and can still attach the wrong token to an entry —
/// fixing that needs a schema change (a stable id field) tracked separately.
/// What IS fixed here: a reorder is detected whenever it changes the
/// `platform_type` at a given index — `type` is checked against `current`'s
/// entry at the same index, and a mismatch (or an index beyond
/// `current.connect.platforms`, e.g. a preceding entry was removed) no
/// longer drops the secret outright. Instead (issue #490) it falls back to a
/// type-based lookup: `current.connect.platforms.iter().find(|p|
/// p.platform_type == patch_type)`. This is safe because `multi_bot_guard`
/// (#462) means only the FIRST entry of a given type is ever started, so
/// resolving to any same-typed entry is strictly better than silently
/// wiping the secret. Only when no entry of that type exists anywhere in
/// `current` does the mask get dropped, same as "nothing configured yet".
pub fn preserve_masked_connect_secrets(patch_obj: &mut Map<String, Value>, current: &Config) {
    let Some(platforms) = patch_obj
        .get_mut("connect")
        .and_then(|c| c.get_mut("platforms"))
        .and_then(|v| v.as_array_mut())
    else {
        return;
    };

    for (index, platform) in platforms.iter_mut().enumerate() {
        let Some(obj) = platform.as_object_mut() else {
            continue;
        };
        let patch_type = obj.get("type").and_then(|v| v.as_str());
        let existing = current.connect.platforms.get(index);
        // A patch entry that names a "type" disagreeing with `current`'s
        // entry at the same index (or an index beyond `current`'s array)
        // means the array was reordered/shrunk since the client fetched it
        // — the position no longer identifies the same logical platform, so
        // don't resolve the mask against it positionally. A patch entry with
        // no "type" at all (shouldn't happen with a well-behaved client,
        // which always echoes the whole object back) can't be checked and
        // falls back to the pre-existing positional-only behavior (no
        // type-based fallback either, since there's no type to search by).
        let guarded = existing
            .filter(|p| match patch_type {
                Some(patch_type) => patch_type == p.platform_type,
                None => true,
            })
            .or_else(|| {
                patch_type.and_then(|patch_type| {
                    current
                        .connect
                        .platforms
                        .iter()
                        .find(|p| p.platform_type == patch_type)
                })
            });
        preserve_masked_secret_field(obj, "token", guarded.and_then(|p| p.token.as_deref()));
        preserve_masked_secret_field(
            obj,
            "app_secret",
            guarded.and_then(|p| p.app_secret.as_deref()),
        );
    }
}

/// Resolve a single masked secret field in place: replace a masked placeholder
/// with `existing_plain`, or drop the field if nothing is configured yet.
/// A non-masked value (a genuine new secret, or an explicit empty-string
/// clear) is left untouched.
fn preserve_masked_secret_field(
    obj: &mut Map<String, Value>,
    field: &str,
    existing_plain: Option<&str>,
) {
    let Some(value) = obj.get(field).and_then(|v| v.as_str()) else {
        return;
    };
    if !is_masked_api_key(value) {
        return;
    }

    match existing_plain {
        Some(plain) if !plain.trim().is_empty() => {
            obj.insert(field.to_string(), Value::String(plain.to_string()));
        }
        _ => {
            obj.remove(field);
        }
    }
}

/// Deep merge `src` into `dst`, recursively combining objects and replacing leaf values.
pub fn deep_merge_json(dst: &mut Value, src: Value) {
    match (dst, src) {
        (Value::Object(dst_map), Value::Object(src_map)) => {
            for (key, value) in src_map {
                match dst_map.get_mut(&key) {
                    Some(existing) => deep_merge_json(existing, value),
                    None => {
                        dst_map.insert(key, value);
                    }
                }
            }
        }
        (dst_slot, src_value) => {
            *dst_slot = src_value;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn domains_for_root_patch_detects_proxy_and_provider() {
        let patch = json!({
            "provider": "openai",
            "http_proxy": "http://proxy:8080",
            "setup": { "completed": false },
            "mcpServers": {}
        });

        let domains = domains_for_root_patch(patch.as_object().unwrap());
        assert!(domains.provider);
        assert!(domains.proxy);
        assert!(domains.setup);
        assert!(domains.mcp);
    }

    #[test]
    fn domains_for_root_patch_detects_provider_instances() {
        let patch = json!({
            "provider_instances": {
                "openai-work": { "provider_type": "openai" }
            },
            "default_provider_instance": "openai-work",
            "defaults": {
                "chat": { "provider": "openai-work", "model": "gpt-4o" }
            },
            "features": {
                "provider_model_ref": true
            }
        });

        let domains = domains_for_root_patch(patch.as_object().unwrap());
        assert!(domains.provider);
    }

    #[test]
    fn provider_api_key_intents_ignores_masked_placeholders() {
        let patch = json!({
            "providers": {
                "openai": { "api_key": "****...****" },
                "gemini": { "api_key": "sk-real" }
            },
            "provider_instances": {
                "work-openai": { "api_key": "****...****" },
                "personal-openai": { "api_key": "sk-live" }
            }
        });
        let intents = provider_api_key_intents(patch.as_object().unwrap());
        assert!(intents.providers.contains("gemini"));
        assert!(!intents.providers.contains("openai"));
        assert!(intents.provider_instances.contains("personal-openai"));
        assert!(!intents.provider_instances.contains("work-openai"));
    }

    #[test]
    fn is_masked_api_key_requires_placeholder_only_values() {
        // The redaction placeholder and all-asterisk/dot variants are masked.
        assert!(is_masked_api_key("****...****"));
        assert!(is_masked_api_key("********"));
        assert!(is_masked_api_key("  ****...****  "));

        // Empty is a "clear" signal, not a mask.
        assert!(!is_masked_api_key(""));
        assert!(!is_masked_api_key("   "));

        // A placeholder with a real key pasted after it must NOT be treated as
        // masked — that silently discards the user's new token (#430).
        assert!(!is_masked_api_key("****...****sk-newkey123"));
        assert!(!is_masked_api_key("sk-newkey123****...****"));

        // Real keys containing dots or asterisks among other characters are keys.
        assert!(!is_masked_api_key("id.secret...suffix"));
        assert!(!is_masked_api_key("sk-live-abc"));
    }

    #[test]
    fn sanitize_root_patch_strips_notification_encrypted_fields() {
        let mut patch = json!({
            "notifications": {
                "ntfy": { "token": "new-token", "token_encrypted": "client-supplied-cipher" },
                "bark": { "device_key": "new-key", "device_key_encrypted": "client-supplied-cipher" }
            }
        });
        let obj = patch.as_object_mut().unwrap();
        sanitize_root_patch(obj);

        assert!(!obj["notifications"]["ntfy"]
            .as_object()
            .unwrap()
            .contains_key("token_encrypted"));
        assert!(!obj["notifications"]["bark"]
            .as_object()
            .unwrap()
            .contains_key("device_key_encrypted"));
        // Plaintext fields the client legitimately sent are untouched.
        assert_eq!(obj["notifications"]["ntfy"]["token"], "new-token");
        assert_eq!(obj["notifications"]["bark"]["device_key"], "new-key");
    }

    #[test]
    fn preserve_masked_notification_secrets_keeps_existing_plaintext() {
        let mut current = Config::default();
        current.notifications.ntfy.token = Some("existing-ntfy-token".to_string());
        current.notifications.bark.device_key = Some("existing-bark-key".to_string());

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"notifications":{"ntfy":{"token":"****...****"},"bark":{"device_key":"****...****"}}}"#,
        )
        .unwrap();

        preserve_masked_notification_secrets(&mut patch, &current);

        assert_eq!(
            patch["notifications"]["ntfy"]["token"],
            "existing-ntfy-token"
        );
        assert_eq!(
            patch["notifications"]["bark"]["device_key"],
            "existing-bark-key"
        );
    }

    #[test]
    fn preserve_masked_notification_secrets_drops_mask_when_nothing_configured() {
        let current = Config::default();
        let mut patch: Map<String, Value> =
            serde_json::from_str(r#"{"notifications":{"ntfy":{"token":"****...****"}}}"#).unwrap();

        preserve_masked_notification_secrets(&mut patch, &current);

        assert!(!patch["notifications"]["ntfy"]
            .as_object()
            .unwrap()
            .contains_key("token"));
    }

    #[test]
    fn preserve_masked_notification_secrets_leaves_real_values_untouched() {
        let current = Config::default();
        let mut patch: Map<String, Value> =
            serde_json::from_str(r#"{"notifications":{"ntfy":{"token":"tk-real-new-value"}}}"#)
                .unwrap();

        preserve_masked_notification_secrets(&mut patch, &current);

        assert_eq!(patch["notifications"]["ntfy"]["token"], "tk-real-new-value");
    }

    fn connect_platform(platform_type: &str, token: &str) -> crate::ConnectPlatformConfig {
        crate::ConnectPlatformConfig {
            platform_type: platform_type.to_string(),
            token: Some(token.to_string()),
            token_encrypted: None,
            app_id: None,
            app_secret: None,
            app_secret_encrypted: None,
            domain: None,
            allow_from: Vec::new(),
            admin_from: Vec::new(),
        }
    }

    #[test]
    fn sanitize_root_patch_strips_connect_platform_encrypted_field() {
        let mut patch = json!({
            "connect": {
                "platforms": [
                    { "type": "telegram", "token": "new-token", "token_encrypted": "client-supplied-cipher" }
                ]
            }
        });
        let obj = patch.as_object_mut().unwrap();
        sanitize_root_patch(obj);

        let platform = &obj["connect"]["platforms"][0];
        assert!(!platform
            .as_object()
            .unwrap()
            .contains_key("token_encrypted"));
        assert_eq!(platform["token"], "new-token");
    }

    #[test]
    fn sanitize_root_patch_strips_connect_platform_app_secret_encrypted_field() {
        let mut patch = json!({
            "connect": {
                "platforms": [
                    {
                        "type": "feishu",
                        "app_id": "cli_x",
                        "app_secret": "new-secret",
                        "app_secret_encrypted": "client-supplied-cipher",
                        "domain": "feishu"
                    }
                ]
            }
        });
        let obj = patch.as_object_mut().unwrap();
        sanitize_root_patch(obj);

        let platform = &obj["connect"]["platforms"][0];
        assert!(!platform
            .as_object()
            .unwrap()
            .contains_key("app_secret_encrypted"));
        assert_eq!(platform["app_secret"], "new-secret");
        assert_eq!(platform["app_id"], "cli_x");
    }

    #[test]
    fn preserve_masked_connect_secrets_keeps_existing_plaintext_by_position() {
        let mut current = Config::default();
        current.connect.platforms = vec![connect_platform("telegram", "existing-bot-token")];

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"telegram","token":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["token"],
            "existing-bot-token"
        );
    }

    #[test]
    fn preserve_masked_connect_secrets_drops_mask_when_nothing_configured() {
        let current = Config::default();
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"telegram","token":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert!(!patch["connect"]["platforms"][0]
            .as_object()
            .unwrap()
            .contains_key("token"));
    }

    #[test]
    fn preserve_masked_connect_secrets_leaves_real_values_untouched() {
        let current = Config::default();
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"telegram","token":"tg-real-new-value"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["token"],
            "tg-real-new-value"
        );
    }

    /// Issue #454 follow-up: if the array was reordered/edited since the
    /// client fetched it — detectable here because the patch entry's "type"
    /// disagrees with `current`'s entry at the same index — a masked token
    /// must NOT be resolved against the wrong (now co-located) platform's
    /// plaintext; it must drop, exactly like "nothing configured yet".
    #[test]
    fn preserve_masked_connect_secrets_drops_mask_when_type_at_index_disagrees() {
        let mut current = Config::default();
        current.connect.platforms = vec![connect_platform("telegram", "telegram-secret-token")];

        // The patch's entry at index 0 claims to be a DIFFERENT platform type
        // than what's actually at `current.connect.platforms[0]` — e.g. a
        // reorder/insert raced the fetch that pre-filled this mask.
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"feishu","token":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert!(
            !patch["connect"]["platforms"][0]
                .as_object()
                .unwrap()
                .contains_key("token"),
            "masked token must not be resolved against a different platform's secret"
        );
    }

    /// #490: when the type at an index mismatches, the guard now falls back
    /// to a type-based lookup across all of `current.connect.platforms`
    /// rather than dropping outright. Here index 1's patch entry claims
    /// "telegram" (matching index 0's type, not index 1's) — the mismatch at
    /// index 1 is detected, but since a "telegram" entry DOES exist
    /// elsewhere in `current` (at index 0), the mask resolves to it. This is
    /// safe because `multi_bot_guard` (#462) only ever starts the first
    /// entry of a given type, so resolving to any same-typed entry is
    /// strictly better than wiping the secret.
    #[test]
    fn preserve_masked_connect_secrets_type_mismatch_falls_back_to_type_lookup() {
        let mut current = Config::default();
        current.connect.platforms = vec![
            connect_platform("telegram", "bot-a-token"),
            connect_platform("feishu", "feishu-token"),
        ];

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[
                {"type":"telegram","token":"tg-real-value"},
                {"type":"telegram","token":"****...****"}
            ]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(patch["connect"]["platforms"][0]["token"], "tg-real-value");
        assert_eq!(
            patch["connect"]["platforms"][1]["token"], "bot-a-token",
            "index 1's mismatched type must fall back to the same-typed entry found elsewhere in current"
        );
    }

    /// A patch entry with a matching "type" at its index is unaffected by
    /// the new guard — this is the common case and must keep working exactly
    /// as before.
    #[test]
    fn preserve_masked_connect_secrets_keeps_plaintext_when_type_at_index_matches() {
        let mut current = Config::default();
        current.connect.platforms = vec![
            connect_platform("telegram", "bot-a-token"),
            connect_platform("telegram", "bot-b-token"),
        ];

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[
                {"type":"telegram","token":"****...****"},
                {"type":"telegram","token":"****...****"}
            ]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(patch["connect"]["platforms"][0]["token"], "bot-a-token");
        assert_eq!(patch["connect"]["platforms"][1]["token"], "bot-b-token");
    }

    /// A patch entry missing "type" entirely (shouldn't happen with a
    /// well-behaved client) falls back to the pre-existing positional-only
    /// behavior rather than being treated as an automatic mismatch.
    #[test]
    fn preserve_masked_connect_secrets_falls_back_to_positional_when_type_is_absent() {
        let mut current = Config::default();
        current.connect.platforms = vec![connect_platform("telegram", "existing-bot-token")];

        let mut patch: Map<String, Value> =
            serde_json::from_str(r#"{"connect":{"platforms":[{"token":"****...****"}]}}"#).unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["token"],
            "existing-bot-token"
        );
    }

    fn feishu_platform(app_secret: &str) -> crate::ConnectPlatformConfig {
        crate::ConnectPlatformConfig {
            platform_type: "feishu".to_string(),
            token: None,
            token_encrypted: None,
            app_id: Some("cli_x".to_string()),
            app_secret: Some(app_secret.to_string()),
            app_secret_encrypted: None,
            domain: Some("lark".to_string()),
            allow_from: Vec::new(),
            admin_from: Vec::new(),
        }
    }

    #[test]
    fn preserve_masked_connect_secrets_keeps_existing_app_secret_by_position() {
        let mut current = Config::default();
        current.connect.platforms = vec![feishu_platform("existing-app-secret")];

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"feishu","app_id":"cli_x","app_secret":"****...****","domain":"lark"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["app_secret"],
            "existing-app-secret"
        );
        // app_id/domain are untouched by the secret-preserve pass.
        assert_eq!(patch["connect"]["platforms"][0]["app_id"], "cli_x");
        assert_eq!(patch["connect"]["platforms"][0]["domain"], "lark");
    }

    #[test]
    fn preserve_masked_connect_secrets_drops_app_secret_mask_when_nothing_configured() {
        let current = Config::default();
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"feishu","app_secret":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert!(!patch["connect"]["platforms"][0]
            .as_object()
            .unwrap()
            .contains_key("app_secret"));
    }

    #[test]
    fn preserve_masked_connect_secrets_leaves_real_app_secret_untouched() {
        let current = Config::default();
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"feishu","app_secret":"feishu-real-new-value"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["app_secret"],
            "feishu-real-new-value"
        );
    }

    /// The #454 type-at-index guard applies to app_secret exactly as it does
    /// to token: a reordered array must not resolve a masked app_secret
    /// against whatever platform now sits at that index.
    #[test]
    fn preserve_masked_connect_secrets_drops_app_secret_mask_when_type_at_index_disagrees() {
        let mut current = Config::default();
        current.connect.platforms = vec![feishu_platform("feishu-secret")];

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"telegram","app_secret":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert!(
            !patch["connect"]["platforms"][0]
                .as_object()
                .unwrap()
                .contains_key("app_secret"),
            "masked app_secret must not be resolved against a different platform's secret"
        );
    }

    /// #490 regression: the exact scenario from the issue. Stored platforms
    /// are `[telegram, feishu]`; the client disables telegram and echoes
    /// back only the (still-masked) feishu entry, which now shifts to index
    /// 0. The positional guard sees telegram≠feishu at index 0 and used to
    /// drop the mask outright, silently wiping the feishu app_secret on
    /// save. It must now fall back to a type-based lookup and resolve to the
    /// stored feishu plaintext.
    #[test]
    fn preserve_masked_connect_secrets_resolves_by_type_when_preceding_entry_removed() {
        let mut current = Config::default();
        current.connect.platforms = vec![
            connect_platform("telegram", "telegram-token"),
            feishu_platform("existing-app-secret"),
        ];

        // telegram was removed client-side; only the feishu entry (still
        // masked) is echoed back, now at index 0.
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[
                {"type":"feishu","app_id":"cli_x","app_secret":"****...****","domain":"lark"}
            ]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["app_secret"], "existing-app-secret",
            "masked app_secret must resolve via type fallback after a preceding entry was removed"
        );
    }

    /// #490: an index beyond `current.connect.platforms` (the patch array
    /// grew, e.g. a new platform was added client-side before saving) must
    /// also fall back to the type-based lookup rather than dropping the mask
    /// when a same-typed entry exists elsewhere in `current`.
    #[test]
    fn preserve_masked_connect_secrets_resolves_by_type_when_index_out_of_range() {
        let mut current = Config::default();
        current.connect.platforms = vec![
            connect_platform("telegram", "telegram-token"),
            feishu_platform("existing-app-secret"),
        ];

        // Patch has 3 entries; index 2 is beyond `current`'s 2-entry array
        // (a new telegram entry was inserted at index 1 client-side), but
        // its masked feishu app_secret should still resolve by type.
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[
                {"type":"telegram","token":"tg-real-value"},
                {"type":"telegram","token":"new-bot-token"},
                {"type":"feishu","app_id":"cli_x","app_secret":"****...****","domain":"lark"}
            ]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][2]["app_secret"], "existing-app-secret",
            "masked app_secret at an out-of-range index must resolve via type fallback"
        );
    }

    /// #490: the type-based fallback only searches by type — if the patched
    /// type doesn't exist anywhere in `current`, the mask still drops, same
    /// as before. Unchanged behavior, exercised here with a multi-entry
    /// `current` (not just the empty-config case already covered above).
    #[test]
    fn preserve_masked_connect_secrets_drops_mask_when_type_absent_from_current_entirely() {
        let mut current = Config::default();
        current.connect.platforms = vec![connect_platform("telegram", "telegram-token")];

        // No feishu entry exists anywhere in `current` — the type-based
        // fallback has nothing to resolve against.
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[
                {"type":"feishu","app_id":"cli_x","app_secret":"****...****","domain":"lark"}
            ]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert!(
            !patch["connect"]["platforms"][0]
                .as_object()
                .unwrap()
                .contains_key("app_secret"),
            "mask must still drop when no entry of that type exists anywhere in current"
        );
    }
}
