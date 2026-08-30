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
        self.reconcile_from_config_transactional_after(config, || async { Ok(()) })
            .await
    }

    /// Stage every new/changed runtime, then run `before_publish`, and only
    /// publish the prepared runtimes when that durable boundary succeeds.
    /// This lets a section store place its CAS commit exactly between runtime
    /// validation and runtime/tool-index publication.
    pub async fn reconcile_from_config_transactional_after<F, Fut>(
        &self,
        config: &McpConfig,
        before_publish: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        self.reconcile_from_config_transactional_after_forcing(
            config,
            &HashSet::new(),
            before_publish,
        )
        .await
    }

    /// Transactional reconcile with explicit runtime replacements even when
    /// their effective configuration is unchanged. Legacy reconnect/update
    /// endpoints use this to preserve their restart contract without doing an
    /// out-of-transaction stop/start after the durable boundary.
    pub async fn reconcile_from_config_transactional_after_forcing<F, Fut>(
        &self,
        config: &McpConfig,
        force_replacements: &HashSet<String>,
        before_publish: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
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
            let needs_replacement = force_replacements.contains(&desired.id)
                || self
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

        // Validate the complete resulting catalog before crossing the durable
        // configuration boundary. This catches cross-server alias conflicts as
        // one unit and leaves every currently published runtime/index entry
        // untouched on failure.
        let replacement_catalogs: Vec<_> = replacements
            .iter()
            .map(|prepared| prepared.catalog.clone())
            .collect();
        let catalog_update = match self
            .index
            .preflight_catalog_update(&replacement_catalogs, &removals)
        {
            Ok(update) => update,
            Err(error) => {
                for prepared in replacements {
                    let id = prepared.runtime.config.id.clone();
                    self.shutdown_detached_runtime(&id, prepared.runtime).await;
                }
                return Err(error.into());
            }
        };

        if let Err(error) = before_publish().await {
            for prepared in replacements {
                let id = prepared.runtime.config.id.clone();
                self.shutdown_detached_runtime(&id, prepared.runtime).await;
            }
            return Err(error);
        }

        // From the durable boundary through the end of these loops there must
        // be no suspension point: cancellation must observe either the old
        // section or every committed runtime/tool-index publication.
        let mut published = Vec::new();
        let mut replaced = Vec::new();
        for prepared in replacements {
            let (id, tool_names, old) = self.publish_prepared_runtime(prepared);
            if let Some(old) = old {
                // publish_prepared_runtime sets shutdown synchronously before
                // returning, but keep the old generation for deferred cleanup.
                replaced.push((id.clone(), old));
            }
            published.push((id, tool_names));
        }
        let mut removed = Vec::new();
        for id in removals {
            match self.detach_runtime_without_index(&id) {
                Ok(runtime) => removed.push((id, runtime)),
                Err(error) => {
                    // The reconcile lock makes this unreachable for ordinary
                    // manager callers, but a commit must remain best-effort and
                    // infallible once replacements are published.
                    warn!("Failed to detach removed MCP server '{}': {}", id, error);
                }
            }
        }
        self.index.commit_catalog_update(catalog_update);

        // Event channel backpressure and transport shutdown are post-commit
        // cleanup. They must never delay section health publication by the
        // caller or make a committed reconcile cancellable halfway through.
        for (id, tool_names) in published {
            let manager = self.clone();
            tokio::spawn(async move {
                manager.emit_runtime_ready_events(id, tool_names).await;
            });
        }
        for (id, runtime) in removed {
            let manager = self.clone();
            tokio::spawn(async move {
                manager.finish_detached_stop(id, runtime, true).await;
            });
        }
        for (id, old) in replaced {
            let manager = self.clone();
            tokio::spawn(async move {
                manager.finish_detached_stop(id, old, false).await;
            });
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
