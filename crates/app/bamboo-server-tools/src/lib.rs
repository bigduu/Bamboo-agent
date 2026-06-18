//! Framework-agnostic server-side tool implementations.
//!
//! These tools (memory, session inspector, skill runtime, compact, overlay) and
//! the [`ToolSurfaceFactory`] depend only on lower crates (`bamboo-agent-core`,
//! `bamboo-engine`, `bamboo-infrastructure`, `bamboo-memory`, `bamboo-tools`) —
//! never on `bamboo-server`'s `AppState`. Server-bound tools (sub-agent,
//! schedule) live in `bamboo-server::tools` and reach this crate through ports.

pub mod ask_agent;
pub mod compact;
pub mod deploy_agent;
pub mod memory;
pub mod overlay_executor;
pub mod session_inspector;
pub mod skill_runtime;
pub mod sub_agent;
pub mod surface;

pub use ask_agent::AskAgentTool;
pub use compact::CompactContextTool;
pub use deploy_agent::{DeployAgentTool, DeployedRegistry};
pub use memory::MemoryTool;
pub use overlay_executor::OverlayToolExecutor;
pub use session_inspector::SessionInspectorTool;
pub use skill_runtime::{LoadSkillTool, ReadSkillResourceTool};
pub use sub_agent::{SubAgentTool, DEFAULT_MAX_SPAWN_DEPTH};
pub use surface::{ToolSurface, ToolSurfaceFactory};
