//! Stable public API for the agent runtime.
//!
//! [`Agent`] wraps an [`AgentRuntime`] with method-based access and serves as
//! the primary entry point for SDK consumers.

use std::sync::Arc;

use bamboo_agent_core::Session;

use crate::runtime::{AgentRuntime, AgentRuntimeBuilder, ExecuteRequest};
use bamboo_domain::RuntimeSessionPersistence;

// An inherited policy may decline the supplied delegate, but an opaque new
// wrapper/provider cannot prove it still represents the managed selected route.
// Refuse only this Gold selection instead of dropping the inherited policy.
struct ChildGoldPolicyRefusal {
    dispatched: std::sync::atomic::AtomicBool,
    rejected: Option<Arc<dyn bamboo_llm::LLMProvider>>,
}

impl ChildGoldPolicyRefusal {
    fn unverified_error() -> bamboo_llm::LLMError {
        bamboo_llm::LLMError::Api(
            "Existing Gold policy returned an unverified provider for managed child resume".into(),
        )
    }

    fn error(&self) -> bamboo_llm::LLMError {
        if self
            .rejected
            .as_ref()
            .is_some_and(|provider| provider.local_dispatch_refused())
        {
            return stale_gold_policy_error();
        }
        self.rejected
            .as_ref()
            .and_then(|provider| provider.known_pre_dispatch_error())
            .unwrap_or_else(Self::unverified_error)
    }
}

fn stale_gold_policy_error() -> bamboo_llm::LLMError {
    bamboo_llm::LLMError::Api(
        "Inherited Gold target already refused a dispatch; a fresh target is required".into(),
    )
}

#[async_trait::async_trait]
impl bamboo_llm::LLMProvider for ChildGoldPolicyRefusal {
    fn known_pre_dispatch_error(&self) -> Option<bamboo_llm::LLMError> {
        Some(self.error())
    }
    fn queue_requires_current_route(&self) -> bool {
        // Keep the default unique queue identity. A prior allowed queued Gold
        // request must not compare equal when the current policy now refuses.
        true
    }
    fn local_dispatch_refused(&self) -> bool {
        self.dispatched.load(std::sync::atomic::Ordering::Acquire)
    }
    async fn chat_stream(
        &self,
        _: &[bamboo_agent_core::Message],
        _: &[bamboo_agent_core::ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
        self.dispatched
            .store(true, std::sync::atomic::Ordering::Release);
        Err(self.error())
    }
}

pub(crate) fn compose_child_gold_policy(
    inherited: Option<crate::runtime::config::GoldTargetResolver>,
    managed: crate::runtime::config::GoldTargetResolver,
) -> crate::runtime::config::GoldTargetResolver {
    let Some(inherited) = inherited else {
        return managed;
    };
    Arc::new(move |model, delegate| {
        // `delegate` was already fixed to the selected provider after the
        // optional Work decoration by bind_child_gold_delegate.
        let result = inherited(model, delegate.clone());
        if !gold_policy_reaches_delegate(&result, &delegate) {
            return Arc::new(ChildGoldPolicyRefusal {
                dispatched: std::sync::atomic::AtomicBool::new(false),
                rejected: None,
            }) as Arc<dyn bamboo_llm::LLMProvider>;
        }
        if result.local_dispatch_refused() {
            return Arc::new(ChildGoldPolicyRefusal {
                dispatched: std::sync::atomic::AtomicBool::new(false),
                rejected: Some(result),
            }) as Arc<dyn bamboo_llm::LLMProvider>;
        }
        let allocation_identity = gold_policy_allocation_identity(&result, &delegate);
        let policy: Arc<dyn bamboo_llm::LLMProvider> = Arc::new(ChildGoldPolicyProvider {
            inherited: inherited.clone(),
            delegate,
            allocation_identity,
            model: model.to_owned(),
            dispatched: std::sync::atomic::AtomicBool::new(false),
        });
        managed(model, policy)
    })
}

fn gold_policy_allocation_identity(
    policy: &Arc<dyn bamboo_llm::LLMProvider>,
    delegate: &Arc<dyn bamboo_llm::LLMProvider>,
) -> usize {
    policy
        .resource_allocation_identity()
        .or_else(|| delegate.resource_allocation_identity())
        .unwrap_or(Arc::as_ptr(delegate) as *const () as usize)
}

// Queue/allocation identity is intentionally not proof of dispatch provenance.
// Only explicitly attested immutable delegate edges may reach the exact Work
// decorated Arc supplied to the policy. Bound traversal so a bad adapter cannot
// create an infinite cycle or unbounded inspection.
fn gold_policy_reaches_delegate(
    candidate: &Arc<dyn bamboo_llm::LLMProvider>,
    delegate: &Arc<dyn bamboo_llm::LLMProvider>,
) -> bool {
    const MAX_WRAPPERS: usize = 32;
    let mut seen = Vec::<Arc<dyn bamboo_llm::LLMProvider>>::new();
    let mut current = candidate.clone();
    for _ in 0..=MAX_WRAPPERS {
        if Arc::ptr_eq(&current, delegate) {
            return true;
        }
        if seen.iter().any(|previous| Arc::ptr_eq(previous, &current)) {
            return false;
        }
        seen.push(current.clone());
        let Some(next) = current.fixed_gold_policy_delegate() else {
            return false;
        };
        current = next;
    }
    false
}

// The inherited resolver is a live policy. Re-evaluate it at dispatch, after
// any auxiliary semaphore wait, and keep the returned wrapper rather than
// discarding its policy. A non-attested result never enters the Work delegate.
struct ChildGoldPolicyProvider {
    inherited: crate::runtime::config::GoldTargetResolver,
    delegate: Arc<dyn bamboo_llm::LLMProvider>,
    allocation_identity: usize,
    model: String,
    dispatched: std::sync::atomic::AtomicBool,
}

impl ChildGoldPolicyProvider {
    fn select(
        &self,
        model: &str,
    ) -> Result<Arc<dyn bamboo_llm::LLMProvider>, bamboo_llm::LLMError> {
        if model != self.model {
            return Err(bamboo_llm::LLMError::Api(
                "Gold model changed after policy selection".into(),
            ));
        }
        let current = (self.inherited)(model, self.delegate.clone());
        if !gold_policy_reaches_delegate(&current, &self.delegate) {
            return Err(ChildGoldPolicyRefusal::unverified_error());
        }
        // The refusal marker is per target. Reusing a target that already
        // refused could misclassify a later upstream error as a local refusal.
        // Refuse before the Work delegate rather than invoking that target.
        if current.local_dispatch_refused() {
            return Err(stale_gold_policy_error());
        }
        // Keep the concurrency budget chosen for this evaluation. An adapter
        // which changes its explicit allocation identity cannot safely reuse
        // the slot already acquired for the prior policy.
        if gold_policy_allocation_identity(&current, &self.delegate) != self.allocation_identity {
            return Err(bamboo_llm::LLMError::Api(
                "Gold policy changed its allocation identity before dispatch".into(),
            ));
        }
        Ok(current)
    }

    fn dispatch(
        &self,
        model: &str,
    ) -> Result<Arc<dyn bamboo_llm::LLMProvider>, bamboo_llm::LLMError> {
        let selected = self.select(model).and_then(|provider| {
            if let Some(error) = provider.known_pre_dispatch_error() {
                Err(error)
            } else {
                Ok(provider)
            }
        });
        if selected.is_err() {
            self.dispatched
                .store(true, std::sync::atomic::Ordering::Release);
        }
        selected
    }

    fn note_delegate_refusal<T>(
        &self,
        provider: &Arc<dyn bamboo_llm::LLMProvider>,
        result: &Result<T, bamboo_llm::LLMError>,
    ) {
        if result.is_err() && provider.local_dispatch_refused() {
            self.dispatched
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }
}

#[async_trait::async_trait]
impl bamboo_llm::LLMProvider for ChildGoldPolicyProvider {
    fn fixed_gold_policy_delegate(&self) -> Option<Arc<dyn bamboo_llm::LLMProvider>> {
        // Every successful dispatch is rechecked to reach this immutable
        // delegate; a changed or opaque route is refused before dispatch.
        Some(self.delegate.clone())
    }

    fn known_pre_dispatch_error(&self) -> Option<bamboo_llm::LLMError> {
        match self.select(&self.model) {
            Ok(provider) => provider.known_pre_dispatch_error(),
            Err(error) => Some(error),
        }
    }

    fn queue_requires_current_route(&self) -> bool {
        true
    }

    fn queued_route_identity(&self) -> bamboo_llm::provider::QueuedRouteIdentity {
        bamboo_llm::provider::queued_identity_for(&self.delegate)
    }

    fn local_dispatch_refused(&self) -> bool {
        self.dispatched.load(std::sync::atomic::Ordering::Acquire)
    }

    fn resource_allocation_identity(&self) -> Option<usize> {
        Some(self.allocation_identity)
    }

    async fn supports_vision(&self, model: &str) -> bool {
        match self.select(&self.model) {
            Ok(provider) => provider.supports_vision(model).await,
            Err(_) => false,
        }
    }

    async fn vision_support_override(&self, model: &str) -> Option<bool> {
        match self.select(&self.model) {
            Ok(provider) => provider.vision_support_override(model).await,
            Err(_) => Some(false),
        }
    }

    async fn capability_loading_mode(
        &self,
        model: &str,
        required: Option<&str>,
    ) -> bamboo_domain::CapabilityLoadingMode {
        match self.select(&self.model) {
            Ok(provider) => provider.capability_loading_mode(model, required).await,
            Err(_) => bamboo_domain::CapabilityLoadingMode::LegacyFullCatalog,
        }
    }

    async fn provider_visible_tool_footprint(
        &self,
        ir: &bamboo_llm::PromptIR,
        tools: &[bamboo_agent_core::ToolSchema],
        model: &str,
        required: Option<&str>,
    ) -> Result<bamboo_llm::ProviderVisibleToolFootprint, bamboo_llm::LLMError> {
        self.select(model)?
            .provider_visible_tool_footprint(ir, tools, model, required)
            .await
    }

    async fn chat_stream(
        &self,
        messages: &[bamboo_agent_core::Message],
        tools: &[bamboo_agent_core::ToolSchema],
        max: Option<u32>,
        model: &str,
    ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
        let provider = self.dispatch(model)?;
        let result = provider.chat_stream(messages, tools, max, model).await;
        self.note_delegate_refusal(&provider, &result);
        result
    }

    async fn chat_stream_with_options(
        &self,
        messages: &[bamboo_agent_core::Message],
        tools: &[bamboo_agent_core::ToolSchema],
        max: Option<u32>,
        model: &str,
        options: Option<&bamboo_llm::LLMRequestOptions>,
    ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
        let provider = self.dispatch(model)?;
        let result = provider
            .chat_stream_with_options(messages, tools, max, model, options)
            .await;
        self.note_delegate_refusal(&provider, &result);
        result
    }

    async fn chat_stream_ir(
        &self,
        ir: &bamboo_llm::PromptIR,
        tools: &[bamboo_agent_core::ToolSchema],
        max: Option<u32>,
        model: &str,
        options: Option<&bamboo_llm::LLMRequestOptions>,
    ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
        let provider = self.dispatch(model)?;
        let result = provider
            .chat_stream_ir(ir, tools, max, model, options)
            .await;
        self.note_delegate_refusal(&provider, &result);
        result
    }

    async fn list_models(&self) -> Result<Vec<String>, bamboo_llm::LLMError> {
        self.select(&self.model)?.list_models().await
    }

    async fn list_model_info(
        &self,
    ) -> Result<Vec<bamboo_llm::provider::ProviderModelInfo>, bamboo_llm::LLMError> {
        self.select(&self.model)?.list_model_info().await
    }
}

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

    /// Return only the Host's still-current explicitly injected provider.
    #[doc(hidden)]
    pub fn legacy_injected_provider_snapshot(
        &self,
    ) -> Result<Option<Arc<dyn bamboo_llm::LLMProvider>>, bamboo_llm::LLMError> {
        self.runtime.provider.legacy_injected_provider_snapshot()
    }

    /// Freeze the same verified injection and retain its Host budget identity.
    #[doc(hidden)]
    pub fn fixed_legacy_injected_provider(
        &self,
        expected: &Arc<dyn bamboo_llm::LLMProvider>,
    ) -> Result<Option<Arc<dyn bamboo_llm::LLMProvider>>, bamboo_llm::LLMError> {
        Ok(self
            .runtime
            .provider
            .legacy_injected_provider_snapshot()?
            .and_then(|current| {
                Arc::ptr_eq(&current, expected).then(|| {
                    bamboo_llm::provider::fixed_injected_provider_with_identity(
                        current,
                        self.runtime.provider.clone(),
                    )
                })
            }))
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

    /// Adapt the default provider for this execution while retaining every
    /// other runtime capability. Explicit request-level overrides stay separate.
    #[doc(hidden)]
    pub fn with_default_provider_adapter(
        &self,
        adapter: impl FnOnce(Arc<dyn bamboo_llm::LLMProvider>) -> Arc<dyn bamboo_llm::LLMProvider>,
    ) -> Self {
        let mut runtime = (*self.runtime).clone();
        runtime.provider = adapter(runtime.provider.clone());
        Self::from_runtime(Arc::new(runtime))
    }

    /// Bind a live-bootstrap Host to this execution only. The Host must retain
    /// its exact runner and writer; this method does not create authority.
    #[doc(hidden)]
    pub fn with_live_bootstrap_host(
        &self,
        host: Arc<dyn crate::runtime::config::LiveBootstrapHost>,
    ) -> Self {
        let mut runtime = (*self.runtime).clone();
        runtime.live_bootstrap_host = Some(host);
        Self::from_runtime(Arc::new(runtime))
    }

    /// Bind a live background route to this execution without changing other roles.
    #[doc(hidden)]
    pub fn with_background_model_resolver(
        &self,
        resolver: crate::runtime::config::BackgroundModelResolver,
    ) -> Self {
        let mut runtime = (*self.runtime).clone();
        runtime.background_model_resolver = Some(resolver);
        Self::from_runtime(Arc::new(runtime))
    }

    /// Install a run-keyed auxiliary resolver on this Agent clone only.
    #[doc(hidden)]
    pub fn with_auxiliary_model_resolver_factory(
        &self,
        factory: Arc<
            dyn Fn(&str) -> Arc<dyn Fn() -> crate::AuxiliaryModelConfig + Send + Sync>
                + Send
                + Sync,
        >,
    ) -> Self {
        let mut runtime = (*self.runtime).clone();
        runtime.auxiliary_model_resolver_factory = Some(factory);
        // The factory resolves background and summary with the other roles.
        // Inherited overrides could belong to another run and would otherwise
        // replace those selections with a second config read.
        runtime.background_model_resolver = None;
        runtime.resume_auxiliary_resolver = None;
        Self::from_runtime(Arc::new(runtime))
    }

    /// Bind a resumed Connect run's live summary/background selection.
    #[doc(hidden)]
    pub fn with_resume_auxiliary_resolver(
        &self,
        resolver: crate::runtime::config::ResumeAuxiliaryResolver,
    ) -> Self {
        let mut runtime = (*self.runtime).clone();
        runtime.resume_auxiliary_resolver = Some(resolver);
        Self::from_runtime(Arc::new(runtime))
    }

    /// Opt a Host-owned non-HTTP consumer into per-attempt Gold provenance.
    /// Direct SDK Agents keep their existing provider contract.
    #[doc(hidden)]
    pub fn with_host_gold_origin_resolver(
        &self,
        resolver: crate::runtime::host_gold::HostGoldOriginResolver,
    ) -> Self {
        let mut runtime = (*self.runtime).clone();
        runtime.host_gold_origin_resolver = Some(resolver);
        Self::from_runtime(Arc::new(runtime))
    }

    /// Bind the Host's Gold route to this execution without changing SDK defaults.
    /// For managed child resume the synchronous resolver may be called again
    /// at preflight and dispatch; it must not initiate a request or block.
    #[doc(hidden)]
    pub fn with_gold_target_resolver(
        &self,
        resolver: crate::runtime::config::GoldTargetResolver,
    ) -> Self {
        let mut runtime = (*self.runtime).clone();
        runtime.gold_target_resolver = Some(resolver);
        runtime.child_resume_gold_selected = None;
        Self::from_runtime(Arc::new(runtime))
    }

    /// Add a managed Host policy while retaining any inherited Gold policy.
    #[doc(hidden)]
    pub fn with_composed_gold_target_resolver(
        &self,
        resolver: crate::runtime::config::GoldTargetResolver,
    ) -> Self {
        let mut runtime = (*self.runtime).clone();
        runtime.gold_target_resolver = Some(compose_child_gold_policy(
            runtime.gold_target_resolver.take(),
            resolver,
        ));
        Self::from_runtime(Arc::new(runtime))
    }

    // Only managed child resume opts into an exact selected-main check. SDK
    // and other Host resolvers retain their existing delegate behavior.
    pub(crate) fn with_child_resume_gold_target_resolver(
        &self,
        selected: Arc<dyn bamboo_llm::LLMProvider>,
        resolver: crate::runtime::config::GoldTargetResolver,
    ) -> Self {
        let mut runtime = (*self.runtime).clone();
        runtime.gold_target_resolver = Some(compose_child_gold_policy(
            runtime.gold_target_resolver.take(),
            resolver,
        ));
        runtime.child_resume_gold_selected = Some(selected);
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

    /// Bind Workspace adoption to this execution without opting into Skill mode.
    #[doc(hidden)]
    pub fn with_workspace_execution_host(
        &self,
        host: Arc<dyn crate::runtime::config::WorkspaceExecutionHost>,
    ) -> Self {
        let mut runtime = (*self.runtime).clone();
        runtime.workspace_execution_host = Some(host);
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
        session
            .check_execution_compatibility()
            .map_err(|error| bamboo_agent_core::AgentError::Tool(error.to_string()))?;
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
        session
            .check_execution_compatibility()
            .map_err(|error| bamboo_agent_core::AgentError::Tool(error.to_string()))?;
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

    /// Execute with an explicit initial upstream request policy.
    pub async fn execute_with_upstream_retry(
        &self,
        session: &mut Session,
        req: ExecuteRequest,
        upstream_retry: bamboo_llm::retry::UpstreamRetry,
    ) -> crate::runtime::runner::Result<()> {
        self.runtime
            .execute_with_upstream_retry(session, req, upstream_retry)
            .await
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

    pub(crate) async fn execute_with_inputs_and_upstream_retry(
        &self,
        session: &mut Session,
        req: ExecuteRequest,
        inputs: Option<crate::runtime::config::UntrustedExecutionInputs>,
        upstream_retry: bamboo_llm::retry::UpstreamRetry,
    ) -> crate::runtime::runner::Result<()> {
        self.runtime
            .execute_with_inputs_and_upstream_retry(session, req, inputs, upstream_retry)
            .await
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
        self.execute_direct_with_upstream_retry(
            session,
            req,
            bamboo_llm::retry::UpstreamRetry::Bounded,
        )
        .await
    }

    /// Execute through the direct owner lifecycle with an explicit retry policy.
    pub async fn execute_direct_with_upstream_retry(
        &self,
        session: &mut Session,
        req: ExecuteRequest,
        upstream_retry: bamboo_llm::retry::UpstreamRetry,
    ) -> crate::runtime::runner::Result<()> {
        session
            .check_execution_compatibility()
            .map_err(|error| bamboo_agent_core::AgentError::Tool(error.to_string()))?;
        let lease = self.begin_direct_execution(&session.id).await?;
        self.check_direct_execution_admission(session, &lease)
            .await?;
        self.prepare_external_session_for_execution(session).await?;
        self.execute_direct_registered_with_inputs_and_upstream_retry(
            session,
            req,
            lease,
            None,
            upstream_retry,
        )
        .await
    }

    /// Check counted-state admission under an already acquired opaque lease.
    /// SDK facades call this before Project preparation or approved-tool replay.
    /// This can refuse/fence an interrupted generation; it never replays it.
    pub fn check_direct_execution_admission<'a>(
        &'a self,
        session: &'a mut Session,
        lease: &'a DirectExecutionLease,
    ) -> impl std::future::Future<Output = crate::runtime::runner::Result<()>> + Send + 'a {
        let matches_target = lease.target_session_id == session.id;
        // Keep the lease borrowed for this future's lifetime without carrying
        // its non-Sync FnOnce cleanup across await. Only real private proof
        // enters the Send future; it is revalidated against the current owner.
        let ownership = lease.registration.as_ref().map(|run| run.ownership());
        async move {
            if !matches_target {
                return Err(bamboo_agent_core::AgentError::LLM(
                    "direct execution lease target does not match session".into(),
                ));
            }
            session
                .check_execution_compatibility()
                .map_err(|error| bamboo_agent_core::AgentError::Tool(error.to_string()))?;
            super::counted_loop::admit_registered(
                session,
                ownership.as_ref(),
                self.activation_router(),
                self.persistence().as_ref(),
            )
            .await
        }
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
        self.execute_direct_registered_with_inputs_and_upstream_retry(
            session,
            req,
            lease,
            inputs,
            bamboo_llm::retry::UpstreamRetry::Bounded,
        )
        .await
    }

    async fn execute_direct_registered_with_inputs_and_upstream_retry(
        &self,
        session: &mut Session,
        req: ExecuteRequest,
        lease: DirectExecutionLease,
        inputs: Option<crate::runtime::config::UntrustedExecutionInputs>,
        upstream_retry: bamboo_llm::retry::UpstreamRetry,
    ) -> crate::runtime::runner::Result<()> {
        self.check_direct_execution_admission(session, &lease)
            .await?;
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
            .execute_direct_registered_bound(session, req, lease, inputs, upstream_retry)
            .await
    }

    async fn execute_direct_registered_bound(
        &self,
        session: &mut Session,
        mut req: ExecuteRequest,
        mut lease: DirectExecutionLease,
        inputs: Option<crate::runtime::config::UntrustedExecutionInputs>,
        upstream_retry: bamboo_llm::retry::UpstreamRetry,
    ) -> crate::runtime::runner::Result<()> {
        if lease.target_session_id != session.id {
            return Err(bamboo_agent_core::AgentError::LLM(format!(
                "direct execution lease target {} does not match session {}",
                lease.target_session_id, session.id
            )));
        }
        let Some(router) = lease.router.take() else {
            return self
                .execute_with_inputs_and_upstream_retry(session, req, inputs, upstream_retry)
                .await;
        };
        let mut registration = lease.registration.take().ok_or_else(|| {
            bamboo_agent_core::AgentError::LLM(
                "direct execution lease is missing its router registration".to_string(),
            )
        })?;
        if !router
            .bind_run_lifetime(&registration.ownership(), session)
            .await
        {
            return Err(bamboo_agent_core::AgentError::Tool(
                "registered run Session lifetime differs".into(),
            ));
        }
        if let Some(counted) = req.counted_loop.as_mut() {
            counted.bind_owner(registration.ownership(), session);
        }
        if let Some(control) = req.loop_control.as_mut() {
            control.bind_owner(registration.ownership(), session);
            control.bind_transport_cleanup(
                &mut registration,
                self.storage().clone(),
                self.session_inbox().cloned(),
                self.persistence().root_actor_writer(),
            );
        }
        let result = if req.loop_control.is_some() {
            bamboo_agent_core::tools::observe_tool_suspension(false, async {
                let token = bamboo_agent_core::tools::capture_suspension_restriction().ok_or_else(
                    || {
                        bamboo_agent_core::AgentError::Tool(
                            "direct loop_control execution is missing its host observation scope"
                                .into(),
                        )
                    },
                )?;
                // Bind before the runtime is first polled, so cancellation of
                // this observer still leaves Drop the same sticky host proof.
                registration.bind_tool_suspension_observation(token);
                self.execute_with_inputs_and_upstream_retry(session, req, inputs, upstream_retry)
                    .await
            })
            .await
            .0
        } else {
            self.execute_with_inputs_and_upstream_retry(session, req, inputs, upstream_retry)
                .await
        };
        if registration.observed_forbidden_running() {
            // No stop receipt exists for this detached outcome, including when
            // direct Root execution has no LocalTaskResources domain.
            // Retain the exact owner before migration or ordinary finish can
            // release it or dispatch a successor. Preserve the runtime error.
            registration.quarantine().await;
            return match result {
                Err(error) => Err(error),
                Ok(()) => Err(bamboo_agent_core::AgentError::Tool(
                    "suspension_disabled: detached Running outcome; stop unconfirmed".into(),
                )),
            };
        }

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

    /// Resolve model limits from a host-owned last-known-good section on each
    /// round instead of reopening the instance's configuration file.
    pub fn model_limits_section(
        mut self,
        section: Arc<bamboo_config::LiveSection<bamboo_config::ModelLimitsSection>>,
    ) -> Self {
        self.inner = self.inner.model_limits_section(section);
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

#[cfg(test)]
mod auxiliary_factory_isolation_tests {
    use super::*;
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::{Message, ToolSchema};
    use bamboo_llm::{LLMError, LLMProvider, LLMStream};

    struct UnusedProvider;

    #[async_trait::async_trait]
    impl LLMProvider for UnusedProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Err(LLMError::Api("provider must not be called".into()))
        }
    }

    #[tokio::test]
    async fn factory_clone_discards_only_its_inherited_route_overrides() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(dir.path().join("sessions"))
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store.clone();
        let persistence: Arc<dyn RuntimeSessionPersistence> =
            Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
        let metrics = bamboo_metrics::MetricsCollector::spawn(
            Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                dir.path().join("metrics.db"),
            )),
            7,
        );
        let ordinary = Agent::builder()
            .storage(storage)
            .persistence(persistence)
            .attachment_reader(store)
            .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
            .metrics_collector(metrics)
            .config(Arc::new(tokio::sync::RwLock::new(
                bamboo_llm::Config::default(),
            )))
            .provider(Arc::new(UnusedProvider))
            .default_tools(Arc::new(bamboo_tools::BuiltinToolExecutor::new()))
            .build()
            .unwrap();
        let background: crate::runtime::config::BackgroundModelResolver = Arc::new(|_| None);
        let resume: crate::runtime::config::ResumeAuxiliaryResolver = Arc::new(|| (Ok(None), None));
        let bound = ordinary
            .with_background_model_resolver(background.clone())
            .with_resume_auxiliary_resolver(resume.clone());
        let sibling = bound.clone();
        let schedule = bound.with_auxiliary_model_resolver_factory(Arc::new(|key| {
            let key = key.to_owned();
            Arc::new(move || crate::AuxiliaryModelConfig {
                fast_model_name: Some(key.clone()),
                ..Default::default()
            })
        }));

        assert!(ordinary.runtime.auxiliary_model_resolver_factory.is_none());
        assert!(ordinary.runtime.background_model_resolver.is_none());
        assert!(ordinary.runtime.resume_auxiliary_resolver.is_none());
        for agent in [&bound, &sibling] {
            assert!(agent.runtime.auxiliary_model_resolver_factory.is_none());
            assert!(Arc::ptr_eq(
                agent.runtime.background_model_resolver.as_ref().unwrap(),
                &background,
            ));
            assert!(Arc::ptr_eq(
                agent.runtime.resume_auxiliary_resolver.as_ref().unwrap(),
                &resume,
            ));
        }
        assert!(schedule.runtime.background_model_resolver.is_none());
        assert!(schedule.runtime.resume_auxiliary_resolver.is_none());
        let factory = schedule
            .runtime
            .auxiliary_model_resolver_factory
            .as_ref()
            .unwrap();
        assert_eq!(
            factory("run-key")().fast_model_name.as_deref(),
            Some("run-key")
        );
    }
}

#[cfg(test)]
#[path = "child_resume_gold_policy_tests.rs"]
mod child_resume_gold_policy_tests;
