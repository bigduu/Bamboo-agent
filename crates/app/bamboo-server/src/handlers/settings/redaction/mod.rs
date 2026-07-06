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
