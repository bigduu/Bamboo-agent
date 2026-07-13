use super::*;

impl AppState {
    /// Reload the provider based on current configuration
    ///
    /// Re-reads the configuration and creates a new LLM provider
    /// instance, allowing runtime switching of providers or models.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the provider was successfully reloaded.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Configuration cannot be read
    /// - Provider initialization fails (e.g., invalid API key)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use bamboo_server::app_state::AppState;
    /// use std::path::PathBuf;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let state = AppState::new(PathBuf::from("/path/to/.bamboo"))
    ///         .await
    ///         .expect("failed to initialize app state");
    ///
    ///     // User updated config file...
    ///     state.reload_provider().await.expect("Provider reload failed");
    /// }
    /// ```
    pub async fn reload_provider(&self) -> Result<(), bamboo_llm::LLMError> {
        let config = self.config.read().await.clone();

        self.provider_registry
            .reload_from_config(&config, self.app_data_dir.clone())
            .await?;

        let default_provider_name = self.provider_registry.default_provider_name();
        tracing::info!(
            default_provider = %default_provider_name,
            legacy_provider = %config.provider,
            has_provider_instances = config.has_provider_instances(),
            "Reloading provider runtime from current config"
        );

        let new_provider = self.provider_registry.get_default().unwrap_or_else(|| {
            let message = if config.has_provider_instances() {
                format!(
                    "Default provider instance '{}' is not available or failed to initialize",
                    default_provider_name
                )
            } else {
                format!(
                    "Provider '{}' is not available or failed to initialize",
                    config.provider
                )
            };
            Arc::new(UnconfiguredProvider { message }) as Arc<dyn LLMProvider>
        });

        let mut provider = self.provider.write().await;
        *provider = new_provider;

        tracing::info!(
            default_provider = %default_provider_name,
            "Provider reloaded successfully"
        );
        Ok(())
    }

    /// Reload the configuration from file
    ///
    /// Reads the configuration file again and updates the in-memory
    /// config. Note: This does NOT automatically reload the provider;
    /// call `reload_provider()` afterwards if needed.
    ///
    /// # Returns
    ///
    /// The newly loaded configuration.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use bamboo_server::app_state::AppState;
    /// use std::path::PathBuf;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let state = AppState::new(PathBuf::from("/path/to/.bamboo"))
    ///         .await
    ///         .expect("failed to initialize app state");
    ///
    ///     // Reload config from disk
    ///     let new_config = state.reload_config().await;
    ///
    ///     // Optionally reload provider with new config
    ///     state.reload_provider().await.ok();
    /// }
    /// ```
    pub async fn reload_config(&self) -> Config {
        // Read from disk INSIDE the write lock. If the disk read happened before
        // acquiring the lock, a concurrent update_config() could persist new
        // state in that gap and then be clobbered here by the stale disk copy
        // (in-memory-only mutations silently lost). Holding the lock across the
        // read+swap serializes reload with update_config's in-memory mutation.
        // Config::from_data_dir is a sync read (no await), so this doesn't hold
        // the lock across an await point. #41.
        // Hold the config-IO lock across the read+swap so it can't interleave
        // with a config write's mutate+persist (which would let us read the disk
        // BEFORE that write persisted, then clobber its in-memory mutation). #126.
        let _io = self.config_io_lock.lock().await;
        let mut config = self.config.write().await;
        let new_config = Config::from_data_dir(Some(self.app_data_dir.clone()));
        *config = new_config.clone();
        new_config
    }

    async fn persist_config_snapshot(&self, config: Config) -> anyhow::Result<()> {
        let data_dir = self.app_data_dir.clone();
        tokio::task::spawn_blocking(move || config.save_to_dir(data_dir))
            .await
            .map_err(|e| anyhow::anyhow!("Config save task failed: {e}"))??;
        Ok(())
    }

    /// Unified config update entrypoint.
    ///
    /// Invariants:
    /// - Update in-memory first
    /// - Persist to disk
    /// - Apply runtime side-effects last (provider reload, MCP reconcile)
    pub async fn update_config<F>(
        &self,
        update: F,
        effects: ConfigUpdateEffects,
    ) -> Result<Config, AppError>
    where
        F: FnOnce(&mut Config) -> Result<(), AppError>,
    {
        // Hold the config-IO lock across BOTH the in-memory mutation AND the disk
        // persist, so a concurrent reload_config can't read the disk in the gap
        // before we persist and then clobber this mutation with the stale copy
        // (#126). The lock is dropped before apply_config_effects — slow side
        // effects (provider reload) don't need to block reloads/other updates.
        let snapshot = {
            let _io = self.config_io_lock.lock().await;
            let (snapshot, enforcement_newly_off) = {
                let mut cfg = self.config.write().await;
                let was_off = cfg.plugin_trust.enforcement_is_off();
                update(&mut cfg)?;
                cfg.publish_env_vars();
                let newly_off = !was_off && cfg.plugin_trust.enforcement_is_off();
                (cfg.clone(), newly_off)
            };
            // Loud signal at the MOMENT plugin_trust.enforcement is flipped off
            // live (e.g. via `bamboo config set plugin_trust.enforcement off`
            // over HTTP), mirroring the boot-time warn in `AppState::new` — so
            // this security-relevant relaxation is never applied silently. Only
            // on the transition into `Off` (not every unrelated config write
            // while already off).
            if enforcement_newly_off {
                warn_plugin_trust_enforcement_off();
            }
            self.persist_config_snapshot(snapshot.clone())
                .await
                .map_err(|e| {
                    AppError::InternalError(anyhow::anyhow!("Failed to save config: {e}"))
                })?;
            snapshot
        };

        self.apply_config_effects(snapshot.clone(), effects).await?;
        Ok(snapshot)
    }

    /// Replace the full config (used for JSON merge endpoints).
    pub async fn replace_config(
        &self,
        new_config: Config,
        effects: ConfigUpdateEffects,
    ) -> Result<Config, AppError> {
        // Same #126 serialization as update_config: mutate + persist under the
        // config-IO lock so a reload can't interleave; effects run unlocked.
        {
            let _io = self.config_io_lock.lock().await;
            let enforcement_newly_off = {
                let mut cfg = self.config.write().await;
                let was_off = cfg.plugin_trust.enforcement_is_off();
                *cfg = new_config.clone();
                cfg.publish_env_vars();
                !was_off && cfg.plugin_trust.enforcement_is_off()
            };
            // Same live signal as `update_config` — a full-config replace (JSON
            // merge / PATCH endpoints) that transitions plugin_trust.enforcement
            // into `Off` must warn just as loudly as a targeted set.
            if enforcement_newly_off {
                warn_plugin_trust_enforcement_off();
            }
            self.persist_config_snapshot(new_config.clone())
                .await
                .map_err(|e| {
                    AppError::InternalError(anyhow::anyhow!("Failed to save config: {e}"))
                })?;
        }

        self.apply_config_effects(new_config.clone(), effects)
            .await?;
        Ok(new_config)
    }

    async fn apply_config_effects(
        &self,
        new_config: Config,
        effects: ConfigUpdateEffects,
    ) -> Result<(), AppError> {
        if effects.reload_provider {
            self.reload_provider().await.map_err(|e| {
                AppError::InternalError(anyhow::anyhow!(
                    "Failed to reload provider after updating config: {e}"
                ))
            })?;
        }

        if effects.reconcile_mcp {
            self.mcp_manager
                .reconcile_from_config(&new_config.mcp)
                .await;
        }

        Ok(())
    }
}

/// The prominent warning emitted whenever `plugin_trust.enforcement` is (or
/// becomes) `Off` — that setting silently downgrades EVERY subsequent `url`
/// plugin install/update to skip the host allowlist, signature, and
/// checksum-required-by-default layers, with no per-install flag needed (see
/// `crate::plugin_source`'s module docs). Factored into one function so the
/// boot-time signal (`AppState::new`) and the live-apply signal
/// (`update_config`/`replace_config`, covering the HTTP `config set` / PATCH
/// paths) emit the EXACT same message — no drift, and no trigger can flip
/// enforcement off silently.
pub(crate) fn warn_plugin_trust_enforcement_off() {
    tracing::warn!(
        "plugin_trust.enforcement is OFF — plugin installs from ANY URL are accepted \
         without host/signature/checksum verification (config.json plugin_trust.enforcement)"
    );
}
