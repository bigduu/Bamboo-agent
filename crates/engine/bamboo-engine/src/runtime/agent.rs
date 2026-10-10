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

impl DirectExecutionLease {
    /// Release ownership before returning a handled pre-execution stop to an
    /// SDK caller, so its next resume does not race the asynchronous Drop path.
    pub async fn abandon(mut self) {
        if let Some(registration) = self.registration.take() {
            registration.abandon().await;
        }
    }
}

/// Opaque, single-use SDK submission receipt. The owned User is the genuine
/// portable-hook result; subsequent F preparation may change only its prompt.
pub struct SdkInputSubmission {
    session_id: String,
    execution_id: String,
    user: bamboo_agent_core::Message,
    hook_runner: Arc<crate::runtime::hooks::HookRunner>,
}

impl SdkInputSubmission {
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }
    pub fn user(&self) -> &bamboo_agent_core::Message {
        &self.user
    }
}

impl Agent {
    /// Wrap an existing [`AgentRuntime`] in an `Agent`.
    pub fn from_runtime(runtime: Arc<AgentRuntime>) -> Self {
        Agent { runtime }
    }

    /// One execution's immutable persistence capability. Shared tool/provider
    /// resources remain on the existing runtime; default callers stay unbound.
    #[doc(hidden)]
    pub fn with_execution_persistence(
        &self,
        persistence: Arc<dyn RuntimeSessionPersistence>,
    ) -> Self {
        let mut runtime = (*self.runtime).clone();
        runtime.persistence = persistence;
        runtime.inherited_child_wait_captured = true;
        Self::from_runtime(Arc::new(runtime))
    }

    /// Install the registered SDK adapter on this execution's runtime only.
    #[doc(hidden)]
    pub fn with_sdk_skill_execution_host(
        &self,
        host: Arc<dyn crate::runtime::config::SdkSkillExecutionHost>,
    ) -> Self {
        self.with_skill_execution_host(host)
    }

    /// Attach an execution-local registered host; this creates no caller authority.
    #[doc(hidden)]
    pub fn with_skill_execution_host(
        &self,
        host: Arc<dyn crate::runtime::config::SkillExecutionHost>,
    ) -> Self {
        let mut runtime = (*self.runtime).clone();
        runtime.sdk_skill_execution_host = Some(host);
        Self::from_runtime(Arc::new(runtime))
    }

    /// Run the genuine portable submission hook before F and append. No old
    /// metadata receipt may skip this new submission's hook.
    #[doc(hidden)]
    pub async fn precheck_sdk_user_input(
        &self,
        session: &mut Session,
        user: &bamboo_agent_core::Message,
        app_data_dir: Option<std::path::PathBuf>,
    ) -> crate::runtime::runner::Result<SdkInputSubmission> {
        if user.role != bamboo_agent_core::Role::User
            || crate::runtime::config::UntrustedInputObservation::new(&user.id, None).is_none()
            || session.messages.iter().any(|message| message.id == user.id)
        {
            return Err(bamboo_agent_core::AgentError::Tool(
                "invalid new SDK User".into(),
            ));
        }
        let hook_runner = {
            let config = self.runtime.config.read().await;
            Arc::new(
                self.runtime
                    .hook_runner
                    .with_lifecycle_config(&config.lifecycle_hooks, app_data_dir),
            )
        };
        session.metadata.remove("runtime.plugin_prompt_prechecked");
        let prompt = hook_runner
            .apply_portable_user_prompt(session, &user.content)
            .await?;
        let mut transformed = user.clone();
        transformed.content = prompt.clone();
        if let Some(parts) = transformed.content_parts.as_mut() {
            if let Some(bamboo_domain::MessagePart::Text { text }) =
                parts.iter_mut().find(|part| matches!(part, bamboo_domain::MessagePart::Text { text } if text == &user.content))
            {
                *text = prompt;
            }
        }
        Ok(SdkInputSubmission {
            session_id: session.id.clone(),
            execution_id: crate::runtime::runner::round_prelude::new_execution_id(),
            user: transformed,
            hook_runner,
        })
    }

    /// Actual SDK producer append. The private pending receipt becomes checked
    /// only after startup hooks and a durable same-repository checkpoint.
    #[doc(hidden)]
    pub fn append_sdk_user_input(
        &self,
        session: &mut Session,
        user: bamboo_agent_core::Message,
        receipt: Option<SdkInputSubmission>,
        request: Option<bamboo_domain::SessionSkillRequest>,
    ) -> crate::runtime::runner::Result<crate::runtime::config::UntrustedExecutionInputs> {
        use crate::runtime::config::{
            sdk_message_digest, sdk_message_header_digest, SdkPendingInput,
            UntrustedExecutionInputs, UntrustedInputObservation,
        };
        let invalid = || bamboo_agent_core::AgentError::Tool("invalid SDK append receipt".into());
        let header_digest = sdk_message_header_digest(&user);
        let input_id = user.id.clone();
        if let Some(receipt) = receipt.as_ref() {
            if receipt.session_id != session.id
                || receipt.user.role != bamboo_agent_core::Role::User
                || user.role != bamboo_agent_core::Role::User
                || sdk_message_header_digest(&receipt.user) != header_digest
                || session
                    .messages
                    .iter()
                    .any(|message| message.id == input_id)
            {
                return Err(invalid());
            }
        }
        // Preserve the legacy String append even when optional observation data
        // is malformed. Typed callers fail before append on an invalid receipt.
        let final_digest = receipt.as_ref().map(|_| sdk_message_digest(&user));
        if receipt.is_some() {
            crate::runtime::hooks::HookRunner::mark_user_prompt_prechecked(session, &user.content);
        }
        session.add_message(user);
        let observation =
            UntrustedInputObservation::new(&input_id, request.as_ref()).ok_or_else(invalid)?;
        let carrier = UntrustedExecutionInputs::new(vec![observation]).ok_or_else(invalid)?;
        let (execution_id, hook_runner) = receipt
            .map(|receipt| (receipt.execution_id, Some(receipt.hook_runner)))
            .unwrap_or_else(|| {
                (
                    crate::runtime::runner::round_prelude::new_execution_id(),
                    None,
                )
            });
        Ok(carrier.bind_sdk_append(
            execution_id,
            SdkPendingInput {
                session_id: session.id.clone(),
                input_id,
                header_digest,
                final_digest,
            },
            hook_runner,
        ))
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

    /// Explicit bounded caller-data handoff; no currentness or Skill authority.
    /// Ordinary execute/direct/spawn entrypoints continue to supply None.
    pub async fn execute_with_inputs(
        &self,
        session: &mut Session,
        req: ExecuteRequest,
        inputs: Option<crate::runtime::config::UntrustedExecutionInputs>,
    ) -> crate::runtime::runner::Result<()> {
        self.runtime.execute_with_inputs(session, req, inputs).await
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
        self.prepare_external_session_for_execution(session).await?;
        self.execute_direct_registered(session, req, lease).await
    }

    /// Validate and publish Project/Workspace context for a caller-owned
    /// session before any external pre-execution side effect.
    ///
    /// SDK facades that acquire a direct lease themselves call this before
    /// approved-tool replay. [`execute_direct`](Self::execute_direct) also calls
    /// it, so the lower-level escape hatch cannot bypass legacy migration,
    /// Project validation, or runtime workspace publication.
    pub async fn prepare_external_session_for_execution(
        &self,
        session: &mut Session,
    ) -> crate::runtime::runner::Result<()> {
        crate::session_app::execution_prep::prepare_external_session_for_execution(
            session,
            self.runtime.project_context_resolver.as_deref(),
        )
        .await
    }

    /// Resolve a proposed SDK Project assignment without publishing a runtime
    /// workspace. The caller must persist the validated candidate before the
    /// ordinary execution handoff publishes it or replays an approved tool.
    pub async fn prepare_external_project_assignment_read_only(
        &self,
        session: &mut Session,
    ) -> crate::runtime::runner::Result<()> {
        let resolver = self
            .runtime
            .project_context_resolver
            .as_deref()
            .ok_or_else(|| {
                bamboo_agent_core::AgentError::ProjectContext(
                    "Project assignment requires a ProjectContextResolver".to_string(),
                )
            })?;
        resolver
            .refresh_session_prompt_read_only(session)
            .await
            .map(|_| ())
            .map_err(|error| bamboo_agent_core::AgentError::ProjectContext(error.to_string()))
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
        lease: DirectExecutionLease,
    ) -> crate::runtime::runner::Result<()> {
        self.execute_direct_registered_with_inputs(session, req, lease, None)
            .await
    }

    /// Existing direct lifecycle with a separately owned, untrusted input handoff.
    pub async fn execute_direct_registered_with_inputs(
        &self,
        session: &mut Session,
        req: ExecuteRequest,
        lease: DirectExecutionLease,
        inputs: Option<crate::runtime::config::UntrustedExecutionInputs>,
    ) -> crate::runtime::runner::Result<()> {
        let inherited = self.persistence().inherited_child_wait().or_else(|| {
            (!self.runtime.inherited_child_wait_captured)
                .then(|| bamboo_domain::InheritedChildWait::capture(session))
                .flatten()
        });
        let agent = if let Some(inherited) = inherited {
            inherited
                .validate_session(session)
                .map_err(|error| bamboo_agent_core::AgentError::LLM(error.to_string()))?;
            if self.persistence().inherited_child_wait().is_some() {
                self.clone()
            } else {
                self.with_execution_persistence(
                    self.persistence()
                        .bind_inherited_child_wait(inherited)
                        .map_err(|error| bamboo_agent_core::AgentError::LLM(error.to_string()))?,
                )
            }
        } else {
            self.with_execution_persistence(self.persistence().clone())
        };
        agent
            .execute_direct_registered_bound(session, req, lease, inputs)
            .await
    }

    async fn execute_direct_registered_bound(
        &self,
        session: &mut Session,
        req: ExecuteRequest,
        mut lease: DirectExecutionLease,
        inputs: Option<crate::runtime::config::UntrustedExecutionInputs>,
    ) -> crate::runtime::runner::Result<()> {
        if lease.target_session_id != session.id {
            return Err(bamboo_agent_core::AgentError::LLM(format!(
                "direct execution lease target {} does not match session {}",
                lease.target_session_id, session.id
            )));
        }
        let Some(router) = lease.router.take() else {
            return self.execute_with_inputs(session, req, inputs).await;
        };
        let mut registration = lease.registration.take().ok_or_else(|| {
            bamboo_agent_core::AgentError::LLM(
                "direct execution lease is missing its router registration".to_string(),
            )
        })?;
        let result = self.execute_with_inputs(session, req, inputs).await;

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
    /// before its first provider call. This compatibility method reports only
    /// the merge count; it cannot confirm successful ACK. Execution entry points
    /// must use [`Self::admit_session_inbox_at_safe_boundary_checked`] instead.
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

    /// Confirm the durable admission boundary before entering provider execution.
    /// An ACK error is unresolved even after receipt publication. Preserve the
    /// checkpoint and existing claim recovery, but reject this activation.
    pub async fn admit_session_inbox_at_safe_boundary_checked(
        &self,
        session: &mut bamboo_agent_core::Session,
    ) -> Result<usize, bamboo_agent_core::AgentError> {
        let refresh = crate::runtime::runner::state_bridge::refresh_turn_boundary_with_inbox(
            session,
            Some(self.storage()),
            Some(self.persistence()),
            self.session_inbox(),
        )
        .await;
        if let Some(error) = refresh.admission_error {
            return Err(bamboo_agent_core::AgentError::Tool(error));
        }
        Ok(refresh.merged)
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

    pub fn memory_store(mut self, v: bamboo_memory::memory_store::MemoryStore) -> Self {
        self.inner = self.inner.memory_store(v);
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
