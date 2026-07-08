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
}
