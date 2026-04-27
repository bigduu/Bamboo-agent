//! Tool surface selection semantics.
//!
//! Different session types require different tool sets. This module provides
//! an enum-based way to select the appropriate tool surface at execution time.

use std::sync::Arc;

use bamboo_agent_core::tools::ToolExecutor;

/// Tool surface variants for different session types.
///
/// | Surface  | Base | +SubSession | +scheduler | +recall/session inspector |
/// |----------|------|-------------|------------|---------------------------|
/// | Base     | ✓    |             |            |                           |
/// | Child    | ✓    |             |            |                           |
/// | WithTask | ✓    | ✓           |            |                           |
/// | Root     | ✓    | ✓           | ✓          | ✓                         |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolSurface {
    /// Base tool set: builtin + MCP + memory + skills.
    /// Used by the agent runtime as default_tools.
    Base,
    /// Same as Base — child sessions cannot recursively spawn.
    Child,
    /// Base + SubSession tool — used by schedule runs.
    WithTask,
    /// Full root tool set: WithTask + scheduler + recall/session inspector.
    Root,
}

/// Factory that provides pre-built tool executors for each surface variant.
///
/// Constructed once during `AppState` initialization.  Call sites
/// obtain the appropriate executor by calling [`ToolSurfaceFactory::get`].
#[derive(Clone)]
pub struct ToolSurfaceFactory {
    base: Arc<dyn ToolExecutor>,
    with_task: Arc<dyn ToolExecutor>,
    root: Arc<dyn ToolExecutor>,
}

impl ToolSurfaceFactory {
    /// Create a factory from pre-built tool executors.
    pub fn new(
        base: Arc<dyn ToolExecutor>,
        with_task: Arc<dyn ToolExecutor>,
        root: Arc<dyn ToolExecutor>,
    ) -> Self {
        Self {
            base,
            with_task,
            root,
        }
    }

    /// Return the tool executor for the requested surface.
    ///
    /// Child and Base both return the same underlying executor.
    pub fn get(&self, surface: ToolSurface) -> Arc<dyn ToolExecutor> {
        match surface {
            ToolSurface::Base => self.base.clone(),
            ToolSurface::Child => self.base.clone(),
            ToolSurface::WithTask => self.with_task.clone(),
            ToolSurface::Root => self.root.clone(),
        }
    }
}
