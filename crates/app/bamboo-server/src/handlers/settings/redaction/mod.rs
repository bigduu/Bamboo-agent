use bamboo_llm::Config;
use serde_json::Value;

mod constants;
mod mcp;
mod provider;

#[cfg(test)]
mod tests;

pub fn redact_config_for_api(mut value: Value, config: &Config) -> Value {
    let Some(root) = value.as_object_mut() else {
        return value;
    };

    // Never send decrypted secrets. Also avoid sending encrypted key material.
    root.remove("proxy_auth_encrypted");
    // Back-compat: older Bodhi/Tauri stored proxy auth using these keys.
    root.remove("http_proxy_auth_encrypted");
    root.remove("https_proxy_auth_encrypted");

    if let Some(providers) = root.get_mut("providers").and_then(|v| v.as_object_mut()) {
        for (name, provider_cfg) in providers.iter_mut() {
            provider::redact_provider_entry(name, provider_cfg, config);
        }
    }

    if let Some(provider_instances) = root
        .get_mut("provider_instances")
        .and_then(|v| v.as_object_mut())
    {
        for (instance_id, instance_cfg) in provider_instances.iter_mut() {
            let Some(instance_obj) = instance_cfg.as_object_mut() else {
                continue;
            };

            instance_obj.remove("api_key_encrypted");

            let is_configured = config
                .provider_instances
                .get(instance_id)
                .map(|instance| {
                    !instance.api_key.trim().is_empty() || instance.api_key_encrypted.is_some()
                })
                .unwrap_or(false);

            if is_configured {
                instance_obj.insert(
                    "api_key".to_string(),
                    Value::String("****...****".to_string()),
                );
            } else {
                instance_obj.remove("api_key");
            }
        }
    }

    mcp::redact_mcp_for_api(root, config);

    if let Some(access_control) = root
        .get_mut("access_control")
        .and_then(|v| v.as_object_mut())
    {
        access_control.remove("password_hash");
        access_control.remove("password_salt");
    }

    // Redact secret env var values.
    if let Some(env_vars) = root.get_mut("env_vars").and_then(|v| v.as_array_mut()) {
        for entry in env_vars.iter_mut() {
            if let Some(obj) = entry.as_object_mut() {
                let is_secret = obj.get("secret").and_then(|v| v.as_bool()).unwrap_or(false);
                if is_secret {
                    obj.insert(
                        "value".to_string(),
                        Value::String("****...****".to_string()),
                    );
                }
                // Never expose encrypted material via API.
                obj.remove("value_encrypted");
            }
        }
    }

    // Redact the broker client token (subagents.broker.token).
    if let Some(broker) = root
        .get_mut("subagents")
        .and_then(|s| s.get_mut("broker"))
        .and_then(|b| b.as_object_mut())
    {
        if broker
            .get("token")
            .and_then(|v| v.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false)
            || broker.contains_key("token_encrypted")
        {
            broker.insert(
                "token".to_string(),
                Value::String("****...****".to_string()),
            );
        }
        broker.remove("token_encrypted");
    }

    // Redact notification-channel secrets (ntfy token, Bark device key).
    // `token`/`device_key` are `#[serde(skip_serializing)]` on `Config` so they
    // never appear here already; mirror the provider-instance `api_key`
    // pattern above by inserting a masked placeholder when configured.
    if let Some(notifications) = root
        .get_mut("notifications")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(ntfy) = notifications
            .get_mut("ntfy")
            .and_then(|v| v.as_object_mut())
        {
            let configured = config
                .notifications
                .ntfy
                .token
                .as_deref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false)
                || config.notifications.ntfy.token_encrypted.is_some();
            if configured {
                ntfy.insert(
                    "token".to_string(),
                    Value::String("****...****".to_string()),
                );
            } else {
                ntfy.remove("token");
            }
            ntfy.remove("token_encrypted");
        }

        if let Some(bark) = notifications
            .get_mut("bark")
            .and_then(|v| v.as_object_mut())
        {
            let configured = config
                .notifications
                .bark
                .device_key
                .as_deref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false)
                || config.notifications.bark.device_key_encrypted.is_some();
            if configured {
                bark.insert(
                    "device_key".to_string(),
                    Value::String("****...****".to_string()),
                );
            } else {
                bark.remove("device_key");
            }
            bark.remove("device_key_encrypted");
        }
    }

    // Redact bamboo-connect platform tokens (Telegram bot token, etc.).
    // `token` is `#[serde(skip_serializing)]` on `Config` so it never appears
    // here already; mirror the notification-channel pattern above by
    // inserting a masked placeholder when configured. Platforms are matched
    // POSITIONALLY against `config.connect.platforms` (see
    // `bamboo_config::patch::preserve_masked_connect_secrets`'s doc comment
    // for why array order is the contract here, same as `env_vars`).
    if let Some(platforms) = root
        .get_mut("connect")
        .and_then(|c| c.get_mut("platforms"))
        .and_then(|v| v.as_array_mut())
    {
        for (index, platform) in platforms.iter_mut().enumerate() {
            let Some(obj) = platform.as_object_mut() else {
                continue;
            };
            let configured = config
                .connect
                .platforms
                .get(index)
                .map(|p| {
                    p.token
                        .as_deref()
                        .map(|s| !s.trim().is_empty())
                        .unwrap_or(false)
                        || p.token_encrypted.is_some()
                })
                .unwrap_or(false);
            if configured {
                obj.insert(
                    "token".to_string(),
                    Value::String("****...****".to_string()),
                );
            } else {
                obj.remove("token");
            }
            obj.remove("token_encrypted");
        }
    }

    // Redact cluster-fabric SSH secrets on each node's placement.auth.
    if let Some(nodes) = root
        .get_mut("cluster_fabric")
        .and_then(|f| f.get_mut("nodes"))
        .and_then(|v| v.as_array_mut())
    {
        for node in nodes.iter_mut() {
            if let Some(auth) = node
                .get_mut("placement")
                .and_then(|p| p.get_mut("auth"))
                .and_then(|a| a.as_object_mut())
            {
                for field in ["password", "private_key", "passphrase"] {
                    if auth.get(field).and_then(|v| v.as_str()).is_some() {
                        auth.insert(field.to_string(), Value::String("****...****".to_string()));
                    }
                    auth.remove(&format!("{field}_encrypted"));
                }
            }
        }
    }

    value
}

pub fn redact_providers_for_api(mut value: Value, config: &Config) -> Value {
    let Some(obj) = value.as_object_mut() else {
        return value;
    };

    for (name, provider_cfg) in obj.iter_mut() {
        provider::redact_provider_entry(name, provider_cfg, config);
    }

    value
}
