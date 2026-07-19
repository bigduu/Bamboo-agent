use super::fingerprint::{effective_server_config, manager_proxy_fingerprint};
use super::*;
use std::collections::HashSet;

impl McpServerManager {
    /// Reconcile running MCP servers with the desired configuration.
    ///
    /// This is best-effort and will:
    /// - Stop servers that are running but removed/disabled in config.
    /// - Start enabled servers that are not running.
    /// - Restart servers whose effective runtime config changed.
    ///
    /// Secrets are compared by their hydrated plaintext (env/header values), not by the
    /// encrypted-at-rest blobs (which can change on every save due to random nonces).
    pub async fn reconcile_from_config(&self, config: &McpConfig) {
        if let Err(error) = self.reconcile_from_config_transactional(config).await {
            error!("Failed to reconcile MCP configuration transactionally: {error}");
        }
    }

    /// Reconcile configuration without evicting any working runtime until all
    /// new and changed servers have completed transport connection, protocol
    /// initialization, and tool discovery.
    pub async fn reconcile_from_config_transactional(&self, config: &McpConfig) -> Result<()> {
        let _reconcile = self.reconcile_lock.lock().await;
        let mut seen = HashSet::new();
        for server in &config.servers {
            if server.id.trim().is_empty() {
                return Err(McpError::InvalidConfig(
                    "MCP server id cannot be empty".to_string(),
                ));
            }
            if !seen.insert(server.id.clone()) {
                return Err(McpError::InvalidConfig(format!(
                    "duplicate MCP server id '{}'",
                    server.id
                )));
            }
        }

        let desired_proxy_fingerprint = manager_proxy_fingerprint(self.config.as_ref()).await;
        let mut replacements = Vec::new();
        for desired in config.servers.iter().filter(|server| server.enabled) {
            let needs_replacement = self
                .runtimes
                .get(&desired.id)
                .map(|runtime| {
                    effective_server_config(&runtime.config) != effective_server_config(desired)
                        || matches!(
                            runtime.config.transport,
                            TransportConfig::Sse(_) | TransportConfig::StreamableHttp(_)
                        ) && runtime.proxy_fingerprint != desired_proxy_fingerprint
                })
                .unwrap_or(true);
            if !needs_replacement {
                continue;
            }

            match self
                .prepare_server_runtime(desired.clone(), "config reload")
                .await
            {
                Ok(prepared) => replacements.push(prepared),
                Err(error) => {
                    for prepared in replacements {
                        let id = prepared.runtime.config.id.clone();
                        self.shutdown_detached_runtime(&id, prepared.runtime).await;
                    }
                    return Err(error);
                }
            }
        }

        let desired_enabled: HashSet<&str> = config
            .servers
            .iter()
            .filter(|server| server.enabled)
            .map(|server| server.id.as_str())
            .collect();
        let removals: Vec<String> = self
            .list_servers()
            .into_iter()
            .filter(|id| !desired_enabled.contains(id.as_str()))
            .collect();

        let mut replaced = Vec::new();
        for prepared in replacements {
            let id = prepared.runtime.config.id.clone();
            if let Some(old) = self.install_prepared_runtime(prepared).await {
                replaced.push((id, old));
            }
        }
        for id in removals {
            if let Err(error) = self.stop_server_unlocked(&id).await {
                // The reconcile lock makes this unreachable for ordinary
                // manager callers, but a commit must remain best-effort and
                // infallible once replacements are published.
                warn!("Failed to stop removed MCP server '{}': {}", id, error);
            }
        }
        for (id, old) in replaced {
            self.shutdown_detached_runtime(&id, old).await;
        }
        Ok(())
    }

    /// Initialize from configuration.
    pub async fn initialize_from_config(&self, config: &McpConfig) {
        for server_config in &config.servers {
            if !server_config.enabled {
                continue;
            }

            if let Err(e) = self.start_server(server_config.clone()).await {
                error!("Failed to start MCP server '{}': {}", server_config.id, e);
            }
        }
    }
}
