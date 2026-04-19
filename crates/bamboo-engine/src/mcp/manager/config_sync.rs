use super::fingerprint::{effective_server_config, manager_proxy_fingerprint};
use super::*;

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
        let desired_proxy_fingerprint = manager_proxy_fingerprint(self.config.as_ref()).await;

        // Stop or restart existing runtimes.
        for running_id in self.list_servers() {
            let desired = config.servers.iter().find(|s| s.id == running_id);

            let Some(desired) = desired else {
                info!(
                    "Stopping MCP server '{}' (removed from configuration)",
                    running_id
                );
                if let Err(e) = self.stop_server(&running_id).await {
                    warn!("Failed to stop MCP server '{}': {}", running_id, e);
                }
                continue;
            };

            if !desired.enabled {
                info!("Stopping MCP server '{}' (disabled in config)", running_id);
                if let Err(e) = self.stop_server(&running_id).await {
                    warn!("Failed to stop MCP server '{}': {}", running_id, e);
                }
                continue;
            }

            let needs_restart = self
                .runtimes
                .get(&running_id)
                .map(|runtime| {
                    let mut restart = effective_server_config(&runtime.config)
                        != effective_server_config(desired);

                    // SSE transports are HTTP clients; if proxy settings change we must restart
                    // to re-create the underlying reqwest client with the new proxy config.
                    if let TransportConfig::Sse(_) = &runtime.config.transport {
                        if runtime.proxy_fingerprint != desired_proxy_fingerprint {
                            restart = true;
                        }
                    }

                    restart
                })
                .unwrap_or(false);

            if needs_restart {
                info!("Restarting MCP server '{}' (config changed)", running_id);
                let _ = self.stop_server(&running_id).await;
                if let Err(e) = self.start_server(desired.clone()).await {
                    error!("Failed to restart MCP server '{}': {}", running_id, e);
                }
            }
        }

        // Start any enabled servers that are not running.
        for desired in &config.servers {
            if !desired.enabled {
                continue;
            }
            if self.is_server_running(&desired.id) {
                continue;
            }

            info!(
                "Starting MCP server '{}' (enabled in configuration)",
                desired.id
            );
            if let Err(e) = self.start_server(desired.clone()).await {
                error!("Failed to start MCP server '{}': {}", desired.id, e);
            }
        }
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
