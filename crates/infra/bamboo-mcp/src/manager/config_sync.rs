use super::fingerprint::{effective_server_config, manager_proxy_fingerprint};
use super::lifecycle::RetiredRuntimeCleanup;
use super::*;
use std::collections::HashSet;

impl McpServerManager {
    /// Reconcile running MCP servers with the desired configuration.
    ///
    /// New and changed runtimes are fully staged before one immutable
    /// generation replaces the complete live catalog/runtime view.
    pub async fn reconcile_from_config(&self, config: &McpConfig) {
        if let Err(error) = self.reconcile_from_config_transactional(config).await {
            error!("Failed to reconcile MCP configuration transactionally: {error}");
        }
    }

    /// Reconcile without evicting any working publication until every new or
    /// changed server has connected, initialized, and published its tool schema.
    pub async fn reconcile_from_config_transactional(&self, config: &McpConfig) -> Result<()> {
        self.reconcile_from_config_transactional_after(config, || async { Ok(()) })
            .await
    }

    /// Stage the next generation, run `before_publish` as the durable boundary,
    /// then complete the already-prevalidated publication in the same poll.
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

    /// Transactional reconcile with explicit replacements even when effective
    /// runtime configuration is unchanged.
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
        let sequence = self.event_sequence_lock.clone().lock_owned().await;
        let reconcile = self.reconcile_lock.lock().await;
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

        let base = self.authority.generation();
        let desired_proxy_fingerprint = manager_proxy_fingerprint(self.config.as_ref()).await;
        let mut prepared = Vec::new();
        for desired in config.servers.iter().filter(|server| server.enabled) {
            let needs_replacement = force_replacements.contains(&desired.id)
                || base
                    .servers
                    .get(&desired.id)
                    .map(|publication| {
                        let runtime = &publication.runtime.runtime;
                        effective_server_config(&runtime.config) != effective_server_config(desired)
                            || matches!(
                                runtime.config.transport,
                                TransportConfig::Sse(_) | TransportConfig::StreamableHttp(_)
                            ) && runtime.proxy_fingerprint != desired_proxy_fingerprint
                    })
                    .unwrap_or(true);
            if needs_replacement {
                prepared.push(
                    self.prepare_server_runtime(desired.clone(), "config reload")
                        .await?,
                );
            }
        }

        let desired_enabled: HashSet<&str> = config
            .servers
            .iter()
            .filter(|server| server.enabled)
            .map(|server| server.id.as_str())
            .collect();
        let removals = base
            .servers
            .keys()
            .filter(|server_id| !desired_enabled.contains(server_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let replacements = prepared
            .iter()
            .map(|prepared| prepared.publication().clone())
            .collect::<Vec<_>>();

        // Collision/history/capacity/schema/revision validation and full next
        // generation allocation all precede the durable callback.
        let next = McpRuntimeGeneration::plan(
            &base,
            &replacements,
            &removals,
            self.authority.ledger_relationship_limit,
            true,
        )?;
        let retirements = base
            .servers
            .values()
            .filter_map(|publication| {
                let replaced = replacements
                    .iter()
                    .any(|replacement| replacement.server_id == publication.server_id);
                let removed = removals.contains(&publication.server_id);
                (replaced || removed).then(|| (publication.clone(), removed))
            })
            .collect::<Vec<_>>();
        if !self.authority.is_current(&base) {
            return Err(McpError::Connection(
                "stale MCP reconcile base generation".to_string(),
            ));
        }

        let mut events = Vec::new();
        for publication in &replacements {
            events.extend(self.runtime_ready_events(publication));
        }
        for (publication, removed) in &retirements {
            if *removed {
                events.push(McpEvent::ServerStatusChanged {
                    server_id: publication.server_id.clone(),
                    status: ServerStatus::Stopped,
                    error: None,
                });
            }
        }
        let retired = retirements
            .iter()
            .map(|(publication, _)| RetiredRuntimeCleanup {
                runtime: publication.runtime.clone(),
            })
            .collect();
        let event_batch = self.prepare_event_batch(sequence, events, retired);
        let mut commits = prepared
            .into_iter()
            .map(PreparedServerRuntime::into_commit)
            .collect::<Vec<_>>();

        before_publish().await?;

        // No await or fallible operation is permitted after the durable
        // boundary. The reconcile lock proves the prevalidated base is current.
        #[cfg(test)]
        self.observe_publish(PublishProbePhase::BeforeFenceAndSwap);
        self.authority.replace_prevalidated_with(&base, next, || {
            for (publication, _) in &retirements {
                publication.retire_with_runtime();
            }
            #[cfg(test)]
            self.observe_publish(PublishProbePhase::AfterFencesBeforeSwap);
        });

        for commit in &mut commits {
            commit.mark_published();
        }
        for commit in &mut commits {
            commit.activate();
        }
        #[cfg(test)]
        self.observe_publish(PublishProbePhase::AfterTransferAndSwapBeforeUnlock);
        drop(reconcile);
        event_batch.activate();
        Ok(())
    }

    /// Initialize the enabled configuration as one generation instead of
    /// exposing a partial server-by-server prefix.
    pub async fn initialize_from_config(&self, config: &McpConfig) {
        if let Err(error) = self.reconcile_from_config_transactional(config).await {
            error!("Failed to initialize MCP configuration: {error}");
        }
    }
}
