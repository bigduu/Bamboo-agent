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
        if let Some(devices) = access_control
            .get_mut("devices")
            .and_then(|v| v.as_array_mut())
        {
            for device in devices {
                let Some(device) = device.as_object_mut() else {
                    continue;
                };
                device.remove("token_hash");
                device.remove("token_salt");
            }
        }
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
                obj.remove("credential_ref");
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

    if let Some(placements) = root
        .get_mut("subagents")
        .and_then(|s| s.get_mut("remote_placements"))
        .and_then(Value::as_array_mut)
    {
        for placement in placements {
            if placement
                .get("broker_peer")
                .is_some_and(|peer| !peer.is_null())
            {
                *placement = serde_json::json!({"role": placement.get("role"), "broker_peer_configured": true});
            }
        }
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
                        || p.token_configured
                        || p.token_credential_ref.is_some()
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

            // Feishu `app_secret` — same masking pattern; `app_id`/`domain`
            // are not secrets and are left visible untouched.
            let app_secret_configured = config
                .connect
                .platforms
                .get(index)
                .map(|p| {
                    p.app_secret
                        .as_deref()
                        .map(|s| !s.trim().is_empty())
                        .unwrap_or(false)
                        || p.app_secret_encrypted.is_some()
                        || p.app_secret_configured
                        || p.app_secret_credential_ref.is_some()
                })
                .unwrap_or(false);
            if app_secret_configured {
                obj.insert(
                    "app_secret".to_string(),
                    Value::String("****...****".to_string()),
                );
            } else {
                obj.remove("app_secret");
            }
            obj.remove("app_secret_encrypted");
        }
    }

    // Cluster-fabric credentials are represented by status metadata on the
    // section endpoint. Never expose plaintext, ciphertext, or mask markers
    // through the legacy root configuration response.
    if let Some(fabric) = root
        .get_mut("cluster_fabric")
        .and_then(Value::as_object_mut)
    {
        // Stable references reveal the existence and storage layout of
        // credentials. The dedicated cluster section exposes only status.
        fabric.remove("credential_refs");
        if let Some(nodes) = fabric.get_mut("nodes").and_then(Value::as_array_mut) {
            for node in nodes.iter_mut() {
                if let Some(auth) = node
                    .get_mut("placement")
                    .and_then(|p| p.get_mut("auth"))
                    .and_then(|a| a.as_object_mut())
                {
                    for field in [
                        "password",
                        "password_encrypted",
                        "private_key",
                        "private_key_encrypted",
                        "passphrase",
                        "passphrase_encrypted",
                    ] {
                        auth.remove(field);
                    }
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

/// Restore only an exact lock-time public echo. Strict routes are operator-only;
/// a masked entry never supplies transport identity or credential authority.
pub(crate) fn preserve_remote_broker_echo(
    current: &Config,
    patch: &mut serde_json::Map<String, Value>,
) -> Result<(), &'static str> {
    let denied = "remote broker routes must be configured by the operator";
    let has_strict = current
        .subagents()
        .remote_placements
        .iter()
        .any(|row| row.broker_peer.is_some());
    if has_strict && patch.get("subagents").is_some_and(|s| !s.is_object()) {
        return Err(denied);
    }
    let Some(incoming) = patch
        .get_mut("subagents")
        .and_then(|s| s.get_mut("remote_placements"))
    else {
        return Ok(());
    };
    if incoming.is_null() && !has_strict {
        return Ok(());
    }
    let private = current.to_compatibility_value().map_err(|_| denied)?;
    let expected = redact_config_for_api(private.clone(), current);
    let current_rows = private["subagents"]["remote_placements"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let incoming = incoming.as_array_mut().ok_or(denied)?;
    for (index, row) in current_rows.iter().enumerate() {
        if row.get("broker_peer").is_some_and(|peer| !peer.is_null()) {
            let echoed = incoming.get_mut(index).ok_or(denied)?;
            if echoed != &expected["subagents"]["remote_placements"][index]
                || incoming_role_count(&expected["subagents"]["remote_placements"], row.get("role"))
                    != 1
            {
                return Err(denied);
            }
            *echoed = row.clone();
        }
    }
    for (index, row) in incoming.iter().enumerate() {
        if row.get("broker_peer_configured").is_some()
            || row.get("broker_peer").is_some_and(|peer| !peer.is_null())
                && current_rows.get(index) != Some(row)
        {
            return Err(denied);
        }
        if current_rows.iter().any(|old| {
            old.get("broker_peer").is_some_and(|p| !p.is_null())
                && old.get("role") == row.get("role")
        }) && incoming
            .iter()
            .filter(|other| other.get("role") == row.get("role"))
            .count()
            != 1
        {
            return Err(denied);
        }
    }
    Ok(())
}
fn incoming_role_count(rows: &Value, role: Option<&Value>) -> usize {
    rows.as_array().map_or(0, |rows| {
        rows.iter().filter(|row| row.get("role") == role).count()
    })
}

#[cfg(test)]
mod remote_broker_redaction_tests {
    use super::*;
    #[test]
    fn private_route_echo_preserves_mixed_routes_and_rejects_edits() {
        let original = serde_json::json!({"subagents":{"remote_placements":[
            {"role":"worker", "endpoint":"wss://private.invalid", "token_env":"PRIVATE_TOKEN", "ca_cert_file":"private-ca",
                "broker_peer":{"parent_mailbox":"private-parent", "worker_mailbox":"private-worker", "parent_role":"host", "worker_role":"worker"}},
            {"role":"legacy", "endpoint":"ws://legacy.invalid"}]}});
        let config: Config = serde_json::from_value(original).unwrap();
        let private = config.to_compatibility_value().unwrap();
        let public = redact_config_for_api(private.clone(), &config);
        let route = &public["subagents"]["remote_placements"][0];
        assert_eq!(
            route,
            &serde_json::json!({"role":"worker", "broker_peer_configured":true})
        );
        assert_eq!(
            public["subagents"]["remote_placements"][1],
            private["subagents"]["remote_placements"][1]
        );
        let mut echo = public.as_object().unwrap().clone();
        preserve_remote_broker_echo(&config, &mut echo).unwrap();
        assert_eq!(
            echo["subagents"]["remote_placements"],
            private["subagents"]["remote_placements"]
        );
        for change in [
            serde_json::json!([]),
            serde_json::json!([{"role":"worker", "broker_peer_configured":false}]),
            serde_json::json!([public["subagents"]["remote_placements"][1], route]),
            serde_json::json!([route, {"role":"worker", "endpoint":"ws://replacement"}]),
        ] {
            let mut patch = serde_json::json!({"subagents":{"remote_placements":change}})
                .as_object()
                .unwrap()
                .clone();
            assert!(preserve_remote_broker_echo(&config, &mut patch).is_err());
        }
        let mut unrelated = serde_json::json!({"subagents":{"max_concurrent":2}})
            .as_object()
            .unwrap()
            .clone();
        preserve_remote_broker_echo(&config, &mut unrelated).unwrap();
        assert_eq!(unrelated["subagents"]["max_concurrent"], 2);
        for invalid in [Value::Null, serde_json::json!(7)] {
            let mut patch = serde_json::json!({"subagents":invalid})
                .as_object()
                .unwrap()
                .clone();
            assert!(preserve_remote_broker_echo(&config, &mut patch).is_err());
        }
        for rows in [
            serde_json::json!([]),
            serde_json::json!([{"role":"legacy","endpoint":"ws://legacy.invalid"}]),
        ] {
            let mut patch = serde_json::json!({"subagents":{"remote_placements":rows}})
                .as_object()
                .unwrap()
                .clone();
            preserve_remote_broker_echo(&Config::default(), &mut patch).unwrap();
        }
        let mut legacy = config.clone();
        legacy.subagents_mut().remote_placements.remove(0);
        let mut legacy_clear = serde_json::json!({"subagents":{"remote_placements":null}})
            .as_object()
            .unwrap()
            .clone();
        assert!(preserve_remote_broker_echo(&config, &mut legacy_clear).is_err());
        preserve_remote_broker_echo(&legacy, &mut legacy_clear).unwrap();
        for row in [
            route.clone(),
            private["subagents"]["remote_placements"][0].clone(),
        ] {
            let mut patch = serde_json::json!({"subagents":{"remote_placements":[row]}})
                .as_object()
                .unwrap()
                .clone();
            assert!(preserve_remote_broker_echo(&Config::default(), &mut patch).is_err());
        }
    }
}
