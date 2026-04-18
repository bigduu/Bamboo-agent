//! Server-only tools that are framework-agnostic (no Actix-web dependencies).
//!
//! These tools provide:
//! - Memory tool for session memory management
//! - Skill runtime tool for loading and reading skill resources
//! - OverlayToolExecutor for composable tool layering
//! - SessionInspector for session inspection capabilities
//! - ToolSurface selection logic

pub mod memory;
pub mod overlay_executor;
pub mod session_inspector;
pub mod skill_runtime;
pub mod surface;

pub use memory::MemoryTool;
pub use overlay_executor::OverlayToolExecutor;
pub use session_inspector::SessionInspectorTool;
pub use skill_runtime::{LoadSkillTool, ReadSkillResourceTool};
pub use surface::{ToolSurface, ToolSurfaceFactory};
