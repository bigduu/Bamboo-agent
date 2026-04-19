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
    pub async fn reload_provider(&self) -> Result<(), bamboo_infrastructure::LLMError> {
        let config = self.config.read().await.clone();

        let configured_model = match config.provider.as_str() {
            "copilot" => config
                .providers
                .copilot
                .as_ref()
                .and_then(|p| p.model.as_ref()),
            "openai" => config
                .providers
                .openai
                .as_ref()
                .and_then(|p| p.model.as_ref()),
            "anthropic" => config
                .providers
                .anthropic
                .as_ref()
                .and_then(|p| p.model.as_ref()),
            "gemini" => config
                .providers
                .gemini
                .as_ref()
                .and_then(|p| p.model.as_ref()),
            _ => None,
        };

        tracing::info!(
            "Reloading provider: type={}, model={:?}",
            config.provider,
            configured_model
        );

        let new_provider =
            bamboo_infrastructure::create_provider_with_dir(&config, self.app_data_dir.clone())
                .await?;

        let mut provider = self.provider.write().await;
        *provider = new_provider;

        tracing::info!("Provider reloaded successfully to: {}", config.provider);
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
        let new_config = Config::from_data_dir(Some(self.app_data_dir.clone()));
        let mut config = self.config.write().await;
        *config = new_config.clone();
        new_config
    }

    /// Persist the current in-memory config to disk (`{app_data_dir}/config.json`).
    ///
    /// This is the single "exit" for configuration writes in the server runtime.
    pub async fn persist_config(&self) -> anyhow::Result<()> {
        let config = self.config.read().await.clone();
        let data_dir = self.app_data_dir.clone();
        tokio::task::spawn_blocking(move || config.save_to_dir(data_dir))
            .await
            .map_err(|e| anyhow::anyhow!("Config save task failed: {e}"))??;
        Ok(())
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
        let snapshot = {
            let mut cfg = self.config.write().await;
            update(&mut cfg)?;
            cfg.publish_env_vars();
            cfg.clone()
        };

        self.persist_config_snapshot(snapshot.clone())
            .await
            .map_err(|e| AppError::InternalError(anyhow::anyhow!("Failed to save config: {e}")))?;

        self.apply_config_effects(snapshot.clone(), effects).await?;
        Ok(snapshot)
    }

    /// Replace the full config (used for JSON merge endpoints).
    pub async fn replace_config(
        &self,
        new_config: Config,
        effects: ConfigUpdateEffects,
    ) -> Result<Config, AppError> {
        {
            let mut cfg = self.config.write().await;
            *cfg = new_config.clone();
            cfg.publish_env_vars();
        }

        self.persist_config_snapshot(new_config.clone())
            .await
            .map_err(|e| AppError::InternalError(anyhow::anyhow!("Failed to save config: {e}")))?;

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
