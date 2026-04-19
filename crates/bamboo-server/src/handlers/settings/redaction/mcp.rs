use bamboo_infrastructure::Config;
use serde_json::{Map, Value};

use super::constants::masked_secret_value;

pub(super) fn redact_mcp_for_api(root: &mut Map<String, Value>, config: &Config) {
    // MCP config may contain credentials in env vars / headers. Do not return either plaintext
    // or encrypted blobs to clients; return masked placeholders instead.
    //
    // On disk / API we use the mainstream `mcpServers` key (map form). We still support
    // older installs that may have been serialized as `mcp` (legacy list form).
    if let Some(mcp_servers) = root.get_mut("mcpServers").and_then(|v| v.as_object_mut()) {
        redact_mcp_servers_map(mcp_servers);
    } else if let Some(mcp) = root.get_mut("mcp").and_then(|v| v.as_object_mut()) {
        redact_legacy_mcp(mcp, config);
    }
}

fn redact_mcp_servers_map(mcp_servers: &mut Map<String, Value>) {
    for (_server_id, server_cfg) in mcp_servers.iter_mut() {
        let Some(server_obj) = server_cfg.as_object_mut() else {
            continue;
        };

        if server_obj.get("command").is_some() {
            redact_stdio_server(server_obj);
        }

        if server_obj.get("url").is_some() {
            redact_sse_server(server_obj);
        }
    }
}

fn redact_stdio_server(server_obj: &mut Map<String, Value>) {
    // Drop legacy encrypted blobs if present.
    let mut keys: Vec<String> = server_obj
        .get("env_encrypted")
        .and_then(|v| v.as_object())
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default();
    server_obj.remove("env_encrypted");

    if let Some(env_obj) = server_obj.get_mut("env").and_then(|v| v.as_object_mut()) {
        for (_key, value) in env_obj.iter_mut() {
            *value = masked_secret_value();
        }
    } else if !keys.is_empty() {
        // If the config only had encrypted env vars, still expose the keys so
        // clients can see which variables are configured.
        let env_obj = keys
            .drain(..)
            .map(|key| (key, masked_secret_value()))
            .collect::<Map<String, Value>>();
        server_obj.insert("env".to_string(), Value::Object(env_obj));
    }
}

fn redact_sse_server(server_obj: &mut Map<String, Value>) {
    // Mainstream style: headers is an object map.
    if let Some(headers_obj) = server_obj
        .get_mut("headers")
        .and_then(|v| v.as_object_mut())
    {
        for (_key, value) in headers_obj.iter_mut() {
            *value = masked_secret_value();
        }
    }

    // Legacy style: headers is an array of {name,value,value_encrypted}.
    if let Some(headers) = server_obj.get_mut("headers").and_then(|v| v.as_array_mut()) {
        for header in headers.iter_mut() {
            let Some(header_obj) = header.as_object_mut() else {
                continue;
            };
            header_obj.remove("value_encrypted");
            header_obj.insert("value".to_string(), masked_secret_value());
        }
    }
}

fn redact_legacy_mcp(mcp: &mut Map<String, Value>, config: &Config) {
    // Legacy list-form redaction (best-effort).
    if let Some(servers) = mcp.get_mut("servers").and_then(|v| v.as_array_mut()) {
        for server in servers.iter_mut() {
            let Some(server_obj) = server.as_object_mut() else {
                continue;
            };
            let server_id = server_obj
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            let Some(transport) = server_obj
                .get_mut("transport")
                .and_then(|v| v.as_object_mut())
            else {
                continue;
            };

            let transport_type = transport
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            match transport_type {
                "stdio" => redact_legacy_stdio_transport(transport, &server_id, config),
                "sse" => redact_legacy_sse_transport(transport),
                _ => {}
            }
        }
    }
}

fn redact_legacy_stdio_transport(
    transport: &mut Map<String, Value>,
    server_id: &str,
    config: &Config,
) {
    let mut keys: Vec<String> = transport
        .get("env_encrypted")
        .and_then(|v| v.as_object())
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default();

    if keys.is_empty() {
        if let Some(cfg_server) = config
            .mcp
            .servers
            .iter()
            .find(|server| server.id == server_id)
        {
            if let bamboo_engine::TransportConfig::Stdio(stdio) = &cfg_server.transport {
                keys = stdio.env.keys().cloned().collect();
            }
        }
    }

    transport.remove("env_encrypted");
    let env_obj = keys
        .into_iter()
        .map(|key| (key, masked_secret_value()))
        .collect::<Map<String, Value>>();
    transport.insert("env".to_string(), Value::Object(env_obj));
}

fn redact_legacy_sse_transport(transport: &mut Map<String, Value>) {
    if let Some(headers) = transport.get_mut("headers").and_then(|v| v.as_array_mut()) {
        for header in headers.iter_mut() {
            let Some(header_obj) = header.as_object_mut() else {
                continue;
            };
            header_obj.remove("value_encrypted");
            header_obj.insert("value".to_string(), masked_secret_value());
        }
    }
}
