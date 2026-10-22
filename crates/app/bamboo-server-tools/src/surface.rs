//! Tool surface selection semantics.
//!
//! Session capability tools share the Base assembly; role-specific management
//! and restricted Child execution keep their independent boundaries.

use std::sync::Arc;

use bamboo_agent_core::tools::ToolExecutor;

/// Tool surface variants for different session types.
///
/// Every variant starts with Host-routed history, current-history retrieval,
/// fresh independent Root creation and the configured scoped message plane.
/// Registration grants no scope. LocalChildTools additionally applies its own
/// admitted native/framework tool boundary after this factory selects Base.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolSurface {
    /// Base tool set: builtin + MCP + memory + skills and caller-scoped Session
    /// capabilities. Used by defaults and headless/scheduled execution.
    /// Used by the agent runtime as default_tools.
    Base,
    /// Same stored executor as Base, before the local Child's separate ceiling.
    Child,
    /// Base + SubAgent tool — used by schedule runs.
    WithTask,
    /// Root management additions: SubAgent, planning, scheduler and Supervisor
    /// controls; ordinary Session capabilities already belong to Base.
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
