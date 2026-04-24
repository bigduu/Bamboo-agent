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
pub mod schedule_tasks;
pub mod spawn_session;
pub mod sub_session_manager;

pub type SubagentModelResolver = std::sync::Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;
pub type OptionalSubagentModelResolver = Option<SubagentModelResolver>;

// Re-export server-specific tool types for convenience
pub use child_session_adapter::ChildSessionAdapter;
pub use schedule_tasks::ScheduleTasksTool;
pub use spawn_session::SpawnSessionTool;
pub use sub_session_manager::SubSessionManagerTool;
