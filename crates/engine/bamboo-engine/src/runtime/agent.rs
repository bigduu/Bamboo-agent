//! Stable public API for the agent runtime.
//!
//! [`Agent`] wraps an [`AgentRuntime`] with method-based access and serves as
//! the primary entry point for SDK consumers.

use std::sync::Arc;

use bamboo_agent_core::Session;

use crate::runtime::{AgentRuntime, AgentRuntimeBuilder, ExecuteRequest};
use bamboo_domain::RuntimeSessionPersistence;

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

/// Opaque ownership lease for one direct logical-session execution.
///
/// SDK facades acquire this before any pre-execution side effect, then transfer
/// it into [`Agent::execute_direct_registered`]. Dropping it early invokes the
/// same abandoned-owner recovery as cancellation during provider execution.
pub struct DirectExecutionLease {
    target_session_id: String,
    router: Option<Arc<crate::session_activation::SessionActivationRouter>>,
    registration: Option<crate::session_activation::SessionRunRegistration>,
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

    /// Execute a caller-owned session under a complete logical-session
    /// activation lifecycle.
    ///
    /// Server/child entry points already own an external runner reservation and
    /// therefore use [`execute`](Self::execute) plus their existing terminal
    /// handshake. Direct SDK callers have no runner registry, so this wrapper
    /// registers the current run before entering the provider loop, marks it
    /// finalizing immediately after return, migrates any terminal-window legacy
    /// ingress, and lets the router reserve at most one successor for work the
    /// completed reasoning turn did not admit.
    pub async fn execute_direct(
        &self,
        session: &mut Session,
        req: ExecuteRequest,
    ) -> crate::runtime::runner::Result<()> {
        let lease = self.begin_direct_execution(&session.id).await?;
        self.execute_direct_registered(session, req, lease).await
    }

    /// Acquire direct logical-session ownership before an SDK facade performs
    /// pre-execution work such as replaying an approved mutating tool.
    pub async fn begin_direct_execution(
        &self,
        target_session_id: &str,
    ) -> crate::runtime::runner::Result<DirectExecutionLease> {
        let Some(router) = self.activation_router().cloned() else {
            return Ok(DirectExecutionLease {
                target_session_id: target_session_id.to_string(),
                router: None,
                registration: None,
            });
        };
        let run_id = format!("sdk-direct-{}", uuid::Uuid::new_v4());
        let registration = router
            .register_run(target_session_id, &run_id)
            .await
            .map_err(|error| bamboo_agent_core::AgentError::LLM(error.to_string()))?;
        Ok(DirectExecutionLease {
            target_session_id: target_session_id.to_string(),
            router: Some(router),
            registration: Some(registration),
        })
    }

    /// Execute and finalize a direct run whose ownership was acquired by
    /// [`begin_direct_execution`](Self::begin_direct_execution).
    pub async fn execute_direct_registered(
        &self,
        session: &mut Session,
        req: ExecuteRequest,
        mut lease: DirectExecutionLease,
    ) -> crate::runtime::runner::Result<()> {
        if lease.target_session_id != session.id {
            return Err(bamboo_agent_core::AgentError::LLM(format!(
                "direct execution lease target {} does not match session {}",
                lease.target_session_id, session.id
            )));
        }
        let Some(router) = lease.router.take() else {
            return self.execute(session, req).await;
        };
        let mut registration = lease.registration.take().ok_or_else(|| {
            bamboo_agent_core::AgentError::LLM(
                "direct execution lease is missing its router registration".to_string(),
            )
        })?;
        let result = self.execute(session, req).await;

        // Freeze what this provider execution actually consumed. Compatibility
        // migration and concurrent deliveries below must remain newer work.
        let executed_admitted_generation = session
            .session_inbox_admission()
            .map_or(0, |state| state.last_admitted_sequence);
        registration.begin_finalization().await;

        let legacy_migration = crate::runtime::runner::state_bridge::migrate_legacy_pending_only(
            session,
            Some(self.storage()),
            Some(self.persistence()),
            self.session_inbox(),
        )
        .await;
        if let Some(generation) = legacy_migration.highest_generation {
            session.session_inbox_admission_mut().observe(generation);
        }
        let pending_generation = session
            .session_inbox_admission()
            .and_then(|state| state.pending_activation_generation());
        if let Some(generation) = pending_generation {
            let activation_ready = if let Some(inbox) = self.session_inbox() {
                match inbox
                    .mark_activation_eligible(
                        &session.id,
                        generation,
                        bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
                    )
                    .await
                {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::error!(
                            session_id = %session.id,
                            %error,
                            "failed to persist direct SDK SessionInbox activation watermark"
                        );
                        false
                    }
                }
            } else {
                false
            };
            if activation_ready {
                if let Err(error) = bamboo_domain::SessionActivationPort::request_activation(
                    router.as_ref(),
                    &session.id,
                    generation,
                )
                .await
                {
                    tracing::error!(
                        session_id = %session.id,
                        %error,
                        "failed to hand direct SDK SessionInbox generation to activation router"
                    );
                }
            }
        }

        // Keep the owner receiver alive until finalizing is visible. Persist
        // the observed-generation marker before a successor can start.
        if let Err(error) = self.persistence().checkpoint_runtime_session(session).await {
            tracing::warn!(
                session_id = %session.id,
                %error,
                "failed to checkpoint direct SDK terminal SessionInbox state"
            );
        }
        if let Err(error) = registration.finish(executed_admitted_generation).await {
            tracing::error!(
                session_id = %session.id,
                %error,
                "direct SDK SessionInbox finalization failed"
            );
        }

        result
    }

    /// Access the shared storage backend.
    pub fn storage(&self) -> &Arc<dyn bamboo_agent_core::storage::Storage> {
        &self.runtime.storage
    }

    /// Access the runtime persistence adapter for non-authoritative saves.
    pub fn persistence(&self) -> &Arc<dyn RuntimeSessionPersistence> {
        &self.runtime.persistence
    }

    pub fn session_inbox(&self) -> Option<&Arc<dyn bamboo_domain::SessionInboxPort>> {
        self.runtime.session_inbox.as_ref()
    }

    /// Execute the same durable SessionInbox boundary used by the agent loop
    /// before its first provider call. Actor workers use this after embedding
    /// initial RunSpec deliveries, so those messages cannot race the first
    /// reasoning context.
    pub async fn admit_session_inbox_at_safe_boundary(
        &self,
        session: &mut bamboo_agent_core::Session,
    ) -> usize {
        crate::runtime::runner::state_bridge::refresh_turn_boundary_with_inbox(
            session,
            Some(self.storage()),
            Some(self.persistence()),
            self.session_inbox(),
        )
        .await
        .merged
    }

    pub fn activation_router(
        &self,
    ) -> Option<&Arc<crate::session_activation::SessionActivationRouter>> {
        self.runtime.activation_router.as_ref()
    }

    pub fn session_messenger(&self) -> Option<&Arc<crate::SessionMessenger>> {
        self.runtime.session_messenger.as_ref()
    }

    /// Access the runtime's default tool executor (the root/full tool surface
    /// assembled at build time).
    ///
    /// Exposed so callers can compose additional one-off dispatches against the
    /// SAME executor the loop itself uses — e.g. re-executing a single
    /// previously-gated tool call after a permission approval — without forking
    /// or reaching into `AgentLoopConfig` (which stays unconstructible outside
    /// the engine). This is a read-only accessor alongside `storage()` /
    /// `persistence()`; it does not touch the sealed loop config.
    pub fn default_tools(&self) -> &Arc<dyn bamboo_agent_core::tools::ToolExecutor> {
        &self.runtime.default_tools
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

    pub fn persistence(mut self, v: Arc<dyn RuntimeSessionPersistence>) -> Self {
        self.inner = self.inner.persistence(v);
        self
    }

    pub fn session_inbox(mut self, v: Arc<dyn bamboo_domain::SessionInboxPort>) -> Self {
        self.inner = self.inner.session_inbox(v);
        self
    }

    pub fn activation_router(
        mut self,
        v: Arc<crate::session_activation::SessionActivationRouter>,
    ) -> Self {
        self.inner = self.inner.activation_router(v);
        self
    }

    pub fn session_messenger(mut self, v: Arc<crate::SessionMessenger>) -> Self {
        self.inner = self.inner.session_messenger(v);
        self
    }

    pub fn attachment_reader(
        mut self,
        v: Arc<dyn bamboo_agent_core::storage::AttachmentReader>,
    ) -> Self {
        self.inner = self.inner.attachment_reader(v);
        self
    }

    pub fn skill_manager(mut self, v: Arc<bamboo_skills::SkillManager>) -> Self {
        self.inner = self.inner.skill_manager(v);
        self
    }

    pub fn project_context_resolver(
        mut self,
        v: Arc<crate::project_context::ProjectContextResolver>,
    ) -> Self {
        self.inner = self.inner.project_context_resolver(v);
        self
    }

    pub fn metrics_collector(mut self, v: bamboo_metrics::MetricsCollector) -> Self {
        self.inner = self.inner.metrics_collector(v);
        self
    }

    pub fn config(mut self, v: Arc<tokio::sync::RwLock<bamboo_llm::Config>>) -> Self {
        self.inner = self.inner.config(v);
        self
    }

    pub fn permission_config(mut self, v: Arc<bamboo_tools::permission::PermissionConfig>) -> Self {
        self.inner = self.inner.permission_config(v);
        self
    }

    pub fn permission_mode(mut self, v: bamboo_domain::PermissionMode) -> Self {
        self.inner = self.inner.permission_mode(v);
        self
    }

    pub fn provider(mut self, v: Arc<dyn bamboo_llm::LLMProvider>) -> Self {
        self.inner = self.inner.provider(v);
        self
    }

    pub fn default_tools(mut self, v: Arc<dyn bamboo_agent_core::tools::ToolExecutor>) -> Self {
        self.inner = self.inner.default_tools(v);
        self
    }

    /// Install an immutable lifecycle-hook registry for this agent runtime.
    pub fn hook_runner(mut self, v: Arc<crate::runtime::HookRunner>) -> Self {
        self.inner = self.inner.hook_runner(v);
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
