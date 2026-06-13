//! Framework-agnostic server-side tool implementations.
//!
//! These tools (memory, session inspector, skill runtime, compact, overlay) and
//! the [`ToolSurfaceFactory`] depend only on lower crates (`bamboo-agent-core`,
//! `bamboo-engine`, `bamboo-infrastructure`, `bamboo-memory`, `bamboo-tools`) —
//! never on `bamboo-server`'s `AppState`. Server-bound tools (sub-agent,
//! schedule) live in `bamboo-server::tools` and reach this crate through ports.

pub mod compact;
pub mod memory;
pub mod overlay_executor;
pub mod session_inspector;
pub mod skill_runtime;
pub mod sub_agent;
pub mod surface;

pub use compact::CompactContextTool;
pub use memory::MemoryTool;
pub use overlay_executor::OverlayToolExecutor;
pub use session_inspector::SessionInspectorTool;
pub use skill_runtime::{LoadSkillTool, ReadSkillResourceTool};
pub use sub_agent::SubAgentTool;
pub use surface::{ToolSurface, ToolSurfaceFactory};
