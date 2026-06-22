use super::*;

use crate::tools::ToolSurface;

impl AppState {
    /// Get a clone of the current provider
    ///
    /// Returns a thread-safe reference to the current LLM provider.
    /// This is the preferred way to access the provider for making requests.
    ///
    /// # Returns
    ///
    /// An Arc reference to the current provider implementation.
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
    ///     let provider = state.get_provider().await;
    ///
    ///     // Use provider to make LLM requests...
    /// }
    /// ```
    pub async fn get_provider(&self) -> Arc<dyn LLMProvider> {
        // Important: return the reloadable handle, not a snapshot clone of the current provider.
        // This ensures config/provider switches take effect without restarting the server.
        self.provider_handle.clone()
    }

    /// Get a provider for a specific [`ProviderModelRef`].
    ///
    /// Used when `features.provider_model_ref` is enabled to route requests
    /// to the correct provider based on the model reference.
    pub fn get_provider_for_model_ref(
        &self,
        target: &bamboo_domain::ProviderModelRef,
    ) -> Result<Arc<dyn LLMProvider>, AppError> {
        self.provider_router
            .route(target)
            .map_err(|e| AppError::BadRequest(e.to_string()))
    }

    /// Get the appropriate provider for a named provider endpoint (e.g., "openai", "anthropic").
    ///
    /// Uses the registry when the `provider_model_ref` feature flag is enabled,
    /// otherwise falls back to the default provider.
    pub async fn get_provider_for_endpoint(
        &self,
        provider_name: &str,
    ) -> Result<Arc<dyn LLMProvider>, AppError> {
        let use_registry = {
            let config = self.config.read().await;
            config.features.provider_model_ref
        };

        if use_registry {
            self.provider_registry.get(provider_name).ok_or_else(|| {
                AppError::InternalError(anyhow::anyhow!(
                    "Provider '{}' not found in registry",
                    provider_name
                ))
            })
        } else {
            Ok(self.get_provider().await)
        }
    }

    /// Shutdown all MCP servers gracefully
    ///
    /// Sends shutdown signals to all running MCP server processes
    /// and waits for them to terminate cleanly.
    ///
    /// This should be called during application shutdown to ensure
    /// MCP servers are not left running as orphaned processes. Invoked by
    /// [`crate::server::web_service::WebService::stop`]. #119.
    pub async fn shutdown(&self) {
        tracing::info!("Shutting down MCP servers...");
        // Stop the supervised MCP proxy service (issue #47) so its reconnect
        // supervisor exits cleanly instead of looping after an intended stop.
        self.mcp_proxy_shutdown.cancel();
        self.mcp_manager.shutdown_all().await;
        tracing::info!("MCP servers shut down complete");
    }

    /// Get the tool executor for a specific surface variant.
    ///
    /// Use [`ToolSurface::Root`] for primary sessions,
    /// [`ToolSurface::Child`] for child sessions, etc.
    pub fn tools_for(
        &self,
        surface: ToolSurface,
    ) -> Arc<dyn bamboo_agent_core::tools::ToolExecutor> {
        self.tool_factory.get(surface)
    }

    /// Get all tool schemas from the composite tool executor
    ///
    /// Returns schemas for both built-in tools and MCP-provided tools.
    /// These schemas are used to inform the LLM about available tools.
    ///
    /// # Returns
    ///
    /// Vector of tool schemas in Anthropic's tool definition format.
    pub fn get_all_tool_schemas(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        self.tool_factory.get(ToolSurface::Root).list_tools()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create AppState from a temp dir.
    async fn make_state() -> (tempfile::TempDir, AppState) {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state");
        (temp_dir, state)
    }

    // ---- get_provider ----

    #[tokio::test]
    async fn get_provider_returns_a_provider() {
        let (_temp, state) = make_state().await;
        let provider = state.get_provider().await;
        let models = provider.list_models().await;
        // Default config has no API keys, so the unconfigured provider
        // returns an auth error — but it should still return a provider handle.
        assert!(
            models.is_err(),
            "Default provider should be UnconfiguredProvider"
        );
    }

    // ---- get_provider_for_endpoint ----

    #[tokio::test]
    async fn endpoint_flag_off_returns_default_provider() {
        let (_temp, state) = make_state().await;
        // Flag is OFF by default
        {
            let config = state.config.read().await;
            assert!(!config.features.provider_model_ref);
        }

        let result = state.get_provider_for_endpoint("openai").await;
        assert!(
            result.is_ok(),
            "Flag OFF should always return default provider"
        );
    }

    #[tokio::test]
    async fn endpoint_flag_on_unknown_provider_returns_error() {
        let (_temp, state) = make_state().await;
        {
            let mut config = state.config.write().await;
            config.features.provider_model_ref = true;
        }

        let result = state.get_provider_for_endpoint("nonexistent").await;
        assert!(result.is_err(), "Flag ON with unknown provider should fail");
    }

    #[tokio::test]
    async fn endpoint_flag_on_copilot_returns_provider() {
        let (_temp, state) = make_state().await;
        {
            let mut config = state.config.write().await;
            config.features.provider_model_ref = true;
        }

        // Copilot is always available (no API key required)
        let result = state.get_provider_for_endpoint("copilot").await;
        assert!(result.is_ok(), "Flag ON with copilot should succeed");
    }

    // ---- get_provider_for_model_ref ----

    #[tokio::test]
    async fn model_ref_unknown_provider_returns_error() {
        let (_temp, state) = make_state().await;
        let target = bamboo_domain::ProviderModelRef::new("nonexistent", "some-model");
        let result = state.get_provider_for_model_ref(&target);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn model_ref_copilot_returns_provider() {
        let (_temp, state) = make_state().await;
        let target = bamboo_domain::ProviderModelRef::new("copilot", "gpt-4o");
        let result = state.get_provider_for_model_ref(&target);
        assert!(result.is_ok(), "copilot provider should be routable");
    }

    // ---- tools_for ----

    #[tokio::test]
    async fn tools_for_root_returns_tool_executor() {
        let (_temp, state) = make_state().await;
        let executor = state.tools_for(ToolSurface::Root);
        let schemas = executor.list_tools();
        assert!(!schemas.is_empty(), "Root tools should not be empty");
    }

    // ---- get_all_tool_schemas ----

    #[tokio::test]
    async fn get_all_tool_schemas_includes_core_tools() {
        let (_temp, state) = make_state().await;
        let schemas = state.get_all_tool_schemas();
        let names: std::collections::HashSet<&str> =
            schemas.iter().map(|s| s.function.name.as_str()).collect();
        assert!(names.contains("Task"));
        assert!(names.contains("SubAgent"));
    }
}
