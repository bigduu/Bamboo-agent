use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EffectiveHeaderConfig {
    name: String,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum EffectiveTransportConfig {
    Stdio {
        command: String,
        args: Vec<String>,
        cwd: Option<String>,
        env: BTreeMap<String, String>,
        startup_timeout_ms: u64,
    },
    Sse {
        url: String,
        headers: Vec<EffectiveHeaderConfig>,
        connect_timeout_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EffectiveServerConfig {
    transport: EffectiveTransportConfig,
    request_timeout_ms: u64,
    healthcheck_interval_ms: u64,
    reconnect: crate::ReconnectConfig,
    allowed_tools: Vec<String>,
    denied_tools: Vec<String>,
}

pub(super) fn effective_server_config(config: &McpServerConfig) -> EffectiveServerConfig {
    let transport = match &config.transport {
        TransportConfig::Stdio(stdio) => EffectiveTransportConfig::Stdio {
            command: stdio.command.clone(),
            args: stdio.args.clone(),
            cwd: stdio.cwd.clone(),
            env: stdio
                .env
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            startup_timeout_ms: stdio.startup_timeout_ms,
        },
        TransportConfig::Sse(sse) => EffectiveTransportConfig::Sse {
            url: sse.url.clone(),
            headers: sse
                .headers
                .iter()
                .map(|h| EffectiveHeaderConfig {
                    name: h.name.clone(),
                    value: h.value.clone(),
                })
                .collect(),
            connect_timeout_ms: sse.connect_timeout_ms,
        },
    };

    EffectiveServerConfig {
        transport,
        request_timeout_ms: config.request_timeout_ms,
        healthcheck_interval_ms: config.healthcheck_interval_ms,
        reconnect: config.reconnect.clone(),
        allowed_tools: config.allowed_tools.clone(),
        denied_tools: config.denied_tools.clone(),
    }
}

pub(super) fn proxy_fingerprint(config: &Config) -> Option<String> {
    let http_proxy = config.http_proxy.trim();
    let https_proxy = config.https_proxy.trim();

    let proxy_url = if !http_proxy.is_empty() {
        http_proxy
    } else if !https_proxy.is_empty() {
        https_proxy
    } else {
        return None;
    };

    let (username, password) = config
        .proxy_auth
        .as_ref()
        .map(|a| (a.username.as_str(), a.password.as_str()))
        .unwrap_or(("", ""));

    // Hash to avoid keeping raw password copies outside the global config.
    let mut hasher = Sha256::new();
    hasher.update(proxy_url.as_bytes());
    hasher.update(b"\0");
    hasher.update(username.as_bytes());
    hasher.update(b"\0");
    hasher.update(password.as_bytes());
    Some(hex::encode(hasher.finalize()))
}

pub(super) async fn manager_proxy_fingerprint(
    cfg_handle: Option<&Arc<tokio::sync::RwLock<Config>>>,
) -> Option<String> {
    let handle = cfg_handle?;
    let cfg = handle.read().await.clone();
    proxy_fingerprint(&cfg)
}

pub(super) async fn desired_proxy_fingerprint(
    cfg_handle: Option<&Arc<tokio::sync::RwLock<Config>>>,
) -> Option<String> {
    manager_proxy_fingerprint(cfg_handle).await
}
