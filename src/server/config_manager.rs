//! Configuration patching helpers.
//!
//! The server has multiple endpoints that update different "sections" of the unified `config.json`
//! (provider, proxy, setup, mcp, etc). These helpers keep patch application consistent and safe:
//! - sanitize incoming patches (never accept encrypted secret material from clients)
//! - preserve masked API keys (UI sends placeholders)
//! - compute which runtime side-effects should run (reload provider / reconcile MCP)
//!
//! Important design note:
//! - `/v1/bamboo/config` is a *permissive* config management endpoint used by setup/UX flows.
//!   It should allow persisting partial config even when the currently-selected provider is
//!   not fully configured yet.
//! - Strict provider validation belongs in provider-specific endpoints like
//!   `/v1/bamboo/settings/provider` (and explicit reload/apply actions).

use serde_json::{Map, Value};

use crate::core::Config;
use crate::server::error::AppError;

fn is_masked_api_key(value: &str) -> bool {
    let v = value.trim();
    // Empty string is treated as an explicit "clear" signal (we control all clients).
    v.contains("***") || v.contains("...") || v == "****...****"
}

pub fn provider_api_key_intents(
    patch_obj: &Map<String, Value>,
) -> std::collections::BTreeSet<String> {
    let mut providers = std::collections::BTreeSet::new();
    let Some(root) = patch_obj.get("providers").and_then(|v| v.as_object()) else {
        return providers;
    };

    for (provider_name, provider_patch) in root.iter() {
        let Some(obj) = provider_patch.as_object() else {
            continue;
        };
        let Some(api_key) = obj.get("api_key").and_then(|v| v.as_str()) else {
            continue;
        };
        // Ignore masked placeholders; those are "preserve existing" signals from the UI.
        if is_masked_api_key(api_key) {
            continue;
        }
        providers.insert(provider_name.clone());
    }

    providers
}

pub fn sync_provider_api_keys_encrypted_for_patch(
    config: &mut Config,
    providers: &std::collections::BTreeSet<String>,
) -> Result<(), AppError> {
    for name in providers.iter() {
        match name.as_str() {
            "openai" => {
                if let Some(openai) = config.providers.openai.as_mut() {
                    let api_key = openai.api_key.trim();
                    openai.api_key_encrypted = if api_key.is_empty() {
                        None
                    } else {
                        Some(crate::core::encryption::encrypt(api_key).map_err(|e| {
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
                        Some(crate::core::encryption::encrypt(api_key).map_err(|e| {
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
                        Some(crate::core::encryption::encrypt(api_key).map_err(|e| {
                            AppError::InternalError(anyhow::anyhow!(
                                "Failed to encrypt Gemini api_key: {e}"
                            ))
                        })?)
                    };
                }
            }
            _ => {}
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadMode {
    None,
    /// Attempt reload, but do not fail the request if reload fails.
    BestEffort,
    /// Reload must succeed; otherwise the request fails.
    Strict,
}

#[derive(Debug, Clone, Copy)]
pub struct PatchEffects {
    pub reload_provider: ReloadMode,
    pub reconcile_mcp: bool,
}

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

pub fn domains_for_root_patch(patch_obj: &Map<String, Value>) -> DomainChanges {
    let mut changes = DomainChanges::default();

    for key in patch_obj.keys() {
        match key.as_str() {
            // Provider domain
            "provider" | "providers" | "model" => changes.provider = true,

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

pub fn assert_json_object(value: Value) -> Result<Map<String, Value>, AppError> {
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(AppError::BadRequest(
            "config.json must be a JSON object".to_string(),
        )),
    }
}

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

pub fn preserve_masked_provider_api_keys(patch_obj: &mut Map<String, Value>, current: &Config) {
    let Some(patch_providers) = patch_obj
        .get_mut("providers")
        .and_then(|v| v.as_object_mut())
    else {
        return;
    };

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
    new_config.hydrate_mcp_secrets_from_encrypted();
    new_config.hydrate_env_vars_from_encrypted();
    new_config.normalize_tool_settings();

    Ok(new_config)
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
    fn provider_api_key_intents_ignores_masked_placeholders() {
        let patch = json!({
            "providers": {
                "openai": { "api_key": "****...****" },
                "gemini": { "api_key": "sk-real" }
            }
        });
        let intents = provider_api_key_intents(patch.as_object().unwrap());
        assert!(intents.contains("gemini"));
        assert!(!intents.contains("openai"));
    }
}
