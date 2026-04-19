//! Stable public API for the agent runtime.
//!
//! [`Agent`] wraps an [`AgentRuntime`] with method-based access and serves as
//! the primary entry point for SDK consumers.

use std::sync::Arc;

use bamboo_agent_core::Session;

use crate::runtime::{AgentRuntime, AgentRuntimeBuilder, ExecuteRequest};

// ---------------------------------------------------------------------------
// Agent — stable public object
// ---------------------------------------------------------------------------

/// Stable public entry point for agent execution.
///
/// Wraps an [`AgentRuntime`] and provides:
/// - [`Agent::execute()`] — run the agent loop on a session
/// - [`Agent::storage()`] — access the shared storage backend
///
/// Clone is cheap (inner is `Arc`).
#[derive(Clone)]
pub struct Agent {
    runtime: Arc<AgentRuntime>,
}

impl Agent {
    /// Wrap an existing [`AgentRuntime`] in an `Agent`.
    pub fn from_runtime(runtime: Arc<AgentRuntime>) -> Self {
        Agent { runtime }
    }

    /// Return a new builder.
    pub fn builder() -> AgentBuilder {
        AgentBuilder::new()
    }

    /// Execute the agent loop with the given request.
    pub async fn execute(
        &self,
        session: &mut Session,
        req: ExecuteRequest,
    ) -> crate::runtime::runner::Result<()> {
        self.runtime.execute(session, req).await
    }

    /// Access the shared storage backend.
    pub fn storage(&self) -> &Arc<dyn bamboo_agent_core::storage::Storage> {
        &self.runtime.storage
    }
}

// ---------------------------------------------------------------------------
// AgentBuilder
// ---------------------------------------------------------------------------

/// Builder for [`Agent`].
///
/// Delegates to [`AgentRuntimeBuilder`] internally.
pub struct AgentBuilder {
    inner: AgentRuntimeBuilder,
}

impl AgentBuilder {
    pub fn new() -> Self {
        Self {
            inner: AgentRuntimeBuilder::new(),
        }
    }

    pub fn storage(mut self, v: Arc<dyn bamboo_agent_core::storage::Storage>) -> Self {
        self.inner = self.inner.storage(v);
        self
    }

    pub fn attachment_reader(
        mut self,
        v: Arc<dyn bamboo_agent_core::storage::AttachmentReader>,
    ) -> Self {
        self.inner = self.inner.attachment_reader(v);
        self
    }

    pub fn skill_manager(mut self, v: Arc<crate::skills::SkillManager>) -> Self {
        self.inner = self.inner.skill_manager(v);
        self
    }

    pub fn metrics_collector(mut self, v: crate::metrics::MetricsCollector) -> Self {
        self.inner = self.inner.metrics_collector(v);
        self
    }

    pub fn config(mut self, v: Arc<tokio::sync::RwLock<bamboo_infrastructure::Config>>) -> Self {
        self.inner = self.inner.config(v);
        self
    }

    pub fn provider(mut self, v: Arc<dyn bamboo_infrastructure::LLMProvider>) -> Self {
        self.inner = self.inner.provider(v);
        self
    }

    pub fn default_tools(mut self, v: Arc<dyn bamboo_agent_core::tools::ToolExecutor>) -> Self {
        self.inner = self.inner.default_tools(v);
        self
    }

    pub fn build(self) -> Result<Agent, &'static str> {
        let runtime = self.inner.build()?;
        Ok(Agent {
            runtime: Arc::new(runtime),
        })
    }
}

impl Default for AgentBuilder {
    fn default() -> Self {
        Self::new()
    }
}
