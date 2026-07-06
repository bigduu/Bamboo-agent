//! Server-only tools and tool executors.
//!
//! These tools are registered only when running the Bamboo HTTP server.
//! They may depend on `AppState` components (storage, schedulers, etc.).
//!
//! Framework-agnostic tools (MemoryTool, OverlayToolExecutor, SubAgentTool,
//! etc.) live in the `bamboo-server-tools` crate. This module re-exports them
//! for convenience and keeps the server-bound glue (`ChildSessionAdapter`,
//! `ScheduleTasksTool`) that wires those tools to `AppState` via ports.

// Re-export framework-agnostic tools from the bamboo-server-tools crate.
pub use bamboo_server_tools::{
    AskAgentTool, ClusterTool, CompactContextTool, DeployAgentTool, DeployedRegistry,
    LoadSkillTool, MemoryTool, OverlayToolExecutor, ReadSkillResourceTool, SessionInspectorTool,
    SubAgentTool, ToolSurface, ToolSurfaceFactory,
};

pub mod child_session_adapter;
pub mod model_catalog;

// Integration tests that wire `SubAgentTool` to a real `ChildSessionAdapter`
// (the tool itself + its pure unit tests live in `bamboo-server-tools`).
#[cfg(test)]
mod sub_agent_tests;

pub type SubagentModelResolver = std::sync::Arc<
    dyn Fn(String) -> futures::future::BoxFuture<'static, Option<bamboo_domain::ProviderModelRef>>
        + Send
        + Sync,
>;
pub type OptionalSubagentModelResolver = Option<SubagentModelResolver>;

// Re-export server-specific tool types for convenience
pub use child_session_adapter::ChildSessionAdapter;
pub use model_catalog::RegistryModelCatalog;
