use crate::core::Config;
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
