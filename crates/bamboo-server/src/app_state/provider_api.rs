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

    /// Shutdown all MCP servers gracefully
    ///
    /// Sends shutdown signals to all running MCP server processes
    /// and waits for them to terminate cleanly.
    ///
    /// This should be called during application shutdown to ensure
    /// MCP servers are not left running as orphaned processes.
    #[allow(dead_code)]
    pub async fn shutdown(&self) {
        tracing::info!("Shutting down MCP servers...");
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
