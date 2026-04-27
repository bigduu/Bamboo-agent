//! Server-only tools and tool executors.
//!
//! These tools are registered only when running the Bamboo HTTP server.
//! They may depend on `AppState` components (storage, schedulers, etc.).
//!
//! Framework-agnostic tools (MemoryTool, OverlayToolExecutor, etc.) live in
//! `bamboo-server-tools` crate. This module re-exports them for convenience.

// Re-export framework-agnostic tools from crate
pub use crate::server_tools::{
    CompactContextTool, LoadSkillTool, MemoryTool, OverlayToolExecutor, ReadSkillResourceTool,
    SessionInspectorTool, ToolSurface, ToolSurfaceFactory,
};

pub mod child_session_adapter;
pub mod policy_aware;
pub mod schedule_tasks;
pub mod sub_session;

pub type SubagentModelResolver = std::sync::Arc<
    dyn Fn(String) -> futures::future::BoxFuture<'static, Option<bamboo_domain::ProviderModelRef>>
        + Send
        + Sync,
>;
pub type OptionalSubagentModelResolver = Option<SubagentModelResolver>;

// Re-export server-specific tool types for convenience
pub use child_session_adapter::ChildSessionAdapter;
pub use policy_aware::PolicyAwareToolExecutor;
pub use schedule_tasks::ScheduleTasksTool;
pub use sub_session::SubSessionTool;
