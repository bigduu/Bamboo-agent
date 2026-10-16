//! Target-owner integration. No public cross-Session state mutation API.
use std::{collections::BTreeSet, sync::Arc};

use bamboo_agent_core::{storage::Storage, AgentError, Session};
use bamboo_domain::{
    canonical_json_bytes, has_scoped_loop_consumption, scoped_loop_consumptions,
    scoped_loop_control_input, scoped_loop_objective_origins, scoped_loop_operation_fingerprint,
    LoopControlAction, RuntimeSessionPersistence, ScopedLoopControlBinding, ScopedLoopControlClaim,
    ScopedLoopControlConsumption, ScopedLoopControlInput, SessionActivationPort, SessionInboxPort,
    SupervisorReference, MAX_SCOPED_LOOP_CONSUMPTIONS, SCOPED_LOOP_CONSUMPTION_KEY,
    SCOPED_LOOP_UNCERTAIN_KEY,
};
use sha2::{Digest, Sha256};

use super::{
    counted_loop, AgentLoopConfig, LoopControlConfig, LoopControlRequest, LoopState, LoopStatus,
};

/// Host-only, separate opt-in. The live runtime binds this template to the
/// actual target lifetime and both execution identities after taking its lease.
/// No model argument creates this value, installs it, or renews it.
#[derive(Debug, Clone)]
pub struct SupervisorLoopControlGrant {
    id: uuid::Uuid,
    supervisor: SupervisorReference,
    supervisor_tool_authority_revision: u64,
    link_revision: u64,
    allowed_actions: BTreeSet<LoopControlAction>,
    max_iterations: u32,
    max_total_rounds: u32,
}

impl SupervisorLoopControlGrant {
    pub fn new(
        supervisor: SupervisorReference,
        supervisor_tool_authority_revision: u64,
        link_revision: u64,
        allowed_actions: impl IntoIterator<Item = LoopControlAction>,
        max_iterations: u32,
        max_total_rounds: u32,
    ) -> Result<Self, AgentError> {
        let grant = Self {
            id: uuid::Uuid::new_v4(),
            supervisor,
            supervisor_tool_authority_revision,
            link_revision,
            allowed_actions: allowed_actions.into_iter().collect(),
            max_iterations,
            max_total_rounds,
        };
        grant.validate(
            counted_loop::MAX_LOOP_ITERATIONS,
            counted_loop::MAX_LOOP_ROUNDS,
        )?;
        Ok(grant)
    }

    pub fn grant_id(&self) -> uuid::Uuid {
        self.id
    }

    pub(super) fn validate(&self, iterations: u32, rounds: u32) -> Result<(), AgentError> {
        if self.supervisor.session_id != bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID
            || self.supervisor.incarnation_id.is_nil()
            || self.link_revision == 0
            || self.allowed_actions.is_empty()
            || !(1..=iterations.min(counted_loop::MAX_LOOP_ITERATIONS))
                .contains(&self.max_iterations)
            || !(self.max_iterations..=rounds.min(counted_loop::MAX_LOOP_ROUNDS))
                .contains(&self.max_total_rounds)
        {
            return Err(super::fatal("invalid separate Supervisor Loop grant"));
        }
        Ok(())
    }
}

impl LoopControlConfig {
    pub(crate) async fn register_supervisor_control(
        &self,
        session: &Session,
        storage: &Arc<dyn Storage>,
    ) -> Result<(), AgentError> {
        let Some(grant) = &self.supervisor_grant else {
            return Ok(());
        };
        if !storage.supports_scoped_loop_transport_retirement() {
            return Err(super::fatal(
                "Supervisor Loop transport retirement unsupported",
            ));
        }
        self.validate_current_owner(session).await?;
        grant.validate(self.max_iterations, self.max_total_rounds)?;
        let ownership = self
            .owner
            .ownership()
            .ok_or_else(|| super::fatal("target owner missing"))?;
        let execution = self
            .execution
            .get()
            .ok_or_else(|| super::fatal("pipeline owner missing"))?;
        if execution.cancel.is_cancelled() {
            return Err(AgentError::Cancelled);
        }
        let binding = ScopedLoopControlBinding {
            grant_id: grant.id,
            supervisor: grant.supervisor.clone(),
            supervisor_tool_authority_revision: grant.supervisor_tool_authority_revision,
            target_session_id: session.id.clone(),
            target_created_at: session.created_at,
            target_metadata_version: session.metadata_version,
            target_project_id: session
                .project_id_meta()
                .ok_or_else(|| super::fatal("target Project missing"))?
                .parse()
                .map_err(|_| super::fatal("target Project invalid"))?,
            link_revision: grant.link_revision,
            owner_run_id: ownership.run_id().into(),
            execution_id: execution.id.clone(),
            allowed_actions: grant.allowed_actions.clone(),
            max_iterations: grant.max_iterations,
            max_total_rounds: grant.max_total_rounds,
        };
        binding
            .validate()
            .map_err(|_| super::fatal("invalid live Supervisor binding"))?;
        ownership
            .router()
            .bind_supervisor_loop_control(
                &ownership,
                binding.clone(),
                execution.cancel.clone(),
                self.stop.clone(),
            )
            .await
            .map_err(|_| super::fatal("Supervisor Loop owner binding failed"))?;
        self.live_supervisor_binding
            .set(binding)
            .map_err(|_| super::fatal("Supervisor Loop binding already consumed"))?;
        Ok(())
    }
}

fn runtime_authority(
    policy: &LoopControlConfig,
) -> Result<Arc<dyn SessionActivationPort>, AgentError> {
    let owner = policy
        .owner
        .ownership()
        .ok_or_else(|| super::fatal("target owner missing"))?;
    Ok(owner.router().clone())
}

fn cancellation_wins(policy: &LoopControlConfig, error: AgentError) -> AgentError {
    if policy
        .execution_cancel()
        .is_ok_and(|token| token.is_cancelled())
    {
        AgentError::Cancelled
    } else {
        error
    }
}

/// On any failed/uncertain physical checkpoint, abort provider admission. A
/// strict canonical reload may recover observable bytes, never a successful ACK.
/// If even that read fails, the local snapshot is fenced from generic saves.
async fn checkpoint_scoped(
    session: &mut Session,
    config: &AgentLoopConfig,
    bindings: &[ScopedLoopControlBinding],
    input: Option<(Arc<dyn SessionInboxPort>, ScopedLoopControlClaim)>,
) -> Result<(), AgentError> {
    let policy = config
        .loop_control
        .as_ref()
        .ok_or_else(|| super::fatal("remote checkpoint owner absent"))?;
    let persistence = config
        .persistence
        .as_ref()
        .ok_or_else(|| super::fatal("durable checkpoint required"))?;
    if session.metadata.contains_key(SCOPED_LOOP_UNCERTAIN_KEY) {
        return Err(super::fatal(
            "prior Supervisor Loop checkpoint remains unknown",
        ));
    }
    let id = session.id.clone();
    let birth = session.created_at;
    let result = persistence
        .checkpoint_scoped_loop_control(session, bindings, runtime_authority(policy)?, input)
        .await;
    if let Err(error) = result {
        match persistence.load_loop_control_checkpoint(&id).await {
            Ok(Some(canonical)) if canonical.id == id && canonical.created_at == birth => {
                *session = canonical;
                session.metadata.remove(SCOPED_LOOP_UNCERTAIN_KEY);
            }
            _ => {
                session.metadata.insert(
                    SCOPED_LOOP_UNCERTAIN_KEY.into(),
                    "unconfirmed checkpoint; strict reload failed".into(),
                );
            }
        }
        if policy.execution_cancel()?.is_cancelled() {
            return Err(AgentError::Cancelled);
        }
        return Err(AgentError::Budget(format!(
            "scoped Loop checkpoint failed/unknown; no further admission: {error}"
        )));
    }
    Ok(())
}

/// Every subsequent running checkpoint retains both active and pending remote
/// objective authority. Terminal error/cancellation records retire admitted
/// work using the original target owner and never authorize another provider.
pub(crate) async fn persist_current_checkpoint(
    session: &mut Session,
    config: &AgentLoopConfig,
) -> Result<(), AgentError> {
    if session.metadata.contains_key(SCOPED_LOOP_UNCERTAIN_KEY) {
        return Err(super::fatal(
            "unknown scoped checkpoint forbids snapshot publication",
        ));
    }
    let bindings = scoped_loop_objective_origins(session)
        .map_err(|_| super::fatal("invalid remote objective origins"))?;
    let running = counted_loop::read_loop_state(session)?
        .is_some_and(|state| matches!(state.status, LoopStatus::Running | LoopStatus::Finalizing));
    if bindings.is_empty() || !running {
        counted_loop::persist_checkpoint(session, config.persistence.as_deref()).await
    } else {
        checkpoint_scoped(session, config, &bindings, None).await?;
        if config
            .loop_control
            .as_ref()
            .ok_or_else(|| super::fatal("target owner absent"))?
            .execution_cancel()?
            .is_cancelled()
        {
            return Err(AgentError::Cancelled);
        }
        Ok(())
    }
}

pub(crate) async fn drain(
    session: &mut Session,
    config: &AgentLoopConfig,
    rounds_spent: u32,
) -> Result<(), AgentError> {
    let Some(policy) = config.loop_control.as_ref() else {
        return Ok(());
    };
    let Some(binding) = policy.live_supervisor_binding.get() else {
        return Ok(());
    };
    let inbox = config
        .session_inbox
        .as_ref()
        .ok_or_else(|| super::fatal("Supervisor Loop Inbox absent"))?;
    let persistence = config
        .persistence
        .as_ref()
        .ok_or_else(|| super::fatal("Supervisor Loop persistence absent"))?;
    for _ in 0..MAX_SCOPED_LOOP_CONSUMPTIONS {
        if policy.execution_cancel()?.is_cancelled() {
            return Err(AgentError::Cancelled);
        }
        // Host stop revokes the grant; model Stop cancels only its generation.
        // A configured remote Stop retires its later unconsumed carriers inside
        // the guarded Storage checkpoint. This drain never applies more work
        // after Stop; host/local terminal recovery is not granted here.
        let generation_stopped = counted_loop::read_loop_state(session)?.is_some_and(|state| {
            state.owner_execution_id == binding.execution_id
                && state
                    .control
                    .as_ref()
                    .is_some_and(|control| control.stop_requested)
        });
        if policy.stop.is_cancelled() || generation_stopped {
            return Ok(());
        }
        policy
            .validate_current_owner(session)
            .await
            .map_err(|error| cancellation_wins(policy, error))?;
        let claims = persistence
            .claim_scoped_loop_controls(session, inbox.clone(), &binding.owner_run_id, 1)
            .await
            .map_err(|_| {
                cancellation_wins(policy, super::fatal("typed Supervisor Loop claim failed"))
            })?;
        if policy.execution_cancel()?.is_cancelled() {
            return Err(AgentError::Cancelled);
        }
        if policy.stop.is_cancelled() {
            return Ok(());
        }
        let Some(claim) = claims.into_iter().next() else {
            return Ok(());
        };
        apply_input(
            session,
            config,
            policy,
            binding,
            inbox.clone(),
            claim,
            rounds_spent,
        )
        .await
        .map_err(|error| cancellation_wins(policy, error))?;
    }
    // Finite receipt capacity is also the maximum work at one safe boundary.
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn apply_input(
    session: &mut Session,
    config: &AgentLoopConfig,
    policy: &LoopControlConfig,
    binding: &ScopedLoopControlBinding,
    inbox: Arc<dyn SessionInboxPort>,
    claim: ScopedLoopControlClaim,
    rounds_spent: u32,
) -> Result<(), AgentError> {
    let envelope = claim.envelope().clone();
    let input = scoped_loop_control_input(&envelope)
        .map_err(|_| super::fatal("invalid typed Loop input"))?
        .ok_or_else(|| super::fatal("Loop claim did not contain typed control"))?;
    if &input.binding != binding {
        return Err(super::fatal("remote grant/owner changed"));
    }
    let storage = config
        .storage
        .as_ref()
        .ok_or_else(|| super::fatal("canonical Loop authority absent"))?;
    let authority = storage
        .load_scoped_loop_session(binding, runtime_authority(policy)?)
        .await;
    if policy.execution_cancel()?.is_cancelled() {
        return Err(AgentError::Cancelled);
    }
    // No configuration proposal or checkpoint has started. A host stop that
    // invalidated this read is an ordinary stop boundary, not a failed task.
    if policy.stop.is_cancelled() {
        return Ok(());
    }
    authority
        .map_err(|_| super::fatal("Supervisor Loop authority revoked before application"))?
        .ok_or_else(|| super::fatal("Supervisor Loop target disappeared"))?;
    policy
        .validate_current_owner(session)
        .await
        .map_err(|error| cancellation_wins(policy, error))?;
    let mut records = scoped_loop_consumptions(session)
        .map_err(|_| super::fatal("malformed Loop consumption proof"))?;
    records.retain(|record| {
        record.binding.owner_run_id == binding.owner_run_id
            && record.binding.execution_id == binding.execution_id
    });
    let matching = has_scoped_loop_consumption(session, &envelope);
    let mut staged = session.clone();
    let mut accepted = None;
    if !matching {
        if records.len() >= MAX_SCOPED_LOOP_CONSUMPTIONS {
            return Err(super::fatal("Supervisor Loop receipt capacity exhausted"));
        }
        let proposal = propose(session, config, policy, &input, rounds_spent).await?;
        let (receipt, rejection) = match proposal {
            Ok(state) => {
                let receipt = state
                    .control
                    .as_ref()
                    .and_then(|control| control.operations.last())
                    .ok_or_else(|| super::fatal("shared Loop proposal lost receipt"))?
                    .receipt
                    .clone();
                counted_loop::stage_state(&mut staged, state.clone())?;
                accepted = Some(state);
                (
                    Some(
                        serde_json::to_value(receipt)
                            .map_err(|_| super::fatal("receipt encoding failed"))?,
                    ),
                    None,
                )
            }
            Err(code) => (None, Some(code.to_string())),
        };
        records.push(ScopedLoopControlConsumption {
            input_id: envelope.id.clone(),
            input_sha256: hex::encode(Sha256::digest(canonical_json_bytes(
                &envelope.idempotency_semantics(),
            ))),
            binding: binding.clone(),
            operation_id: input.operation_id().into(),
            configured: receipt.is_some(),
            receipt,
            rejection,
        });
        let encoded = serde_json::to_string(&records)
            .map_err(|_| super::fatal("consumption encoding failed"))?;
        if encoded.len() > 64 * 1024 {
            return Err(super::fatal("Loop consumption checkpoint exceeds bound"));
        }
        staged
            .metadata
            .insert(SCOPED_LOOP_CONSUMPTION_KEY.into(), encoded);
    }
    staged
        .session_inbox_admission_mut()
        .record(envelope.id.clone(), claim.generation());
    let mut bindings = scoped_loop_objective_origins(&staged)
        .map_err(|_| super::fatal("invalid objective provenance"))?;
    if !bindings.contains(binding) {
        bindings.push(binding.clone());
    }
    if policy.execution_cancel()?.is_cancelled() {
        return Err(AgentError::Cancelled);
    }
    if policy.stop.is_cancelled() {
        return Ok(());
    }
    // From this point onward every physical I/O/unknown result is preserved;
    // a concurrent host stop cannot turn a checkpoint error into success.
    let checkpoint = checkpoint_scoped(&mut staged, config, &bindings, Some((inbox, claim))).await;
    // The helper has either a confirmed checkpoint, a strict canonical reload,
    // or an explicit volatile no-save fence. Never replace it with the proposal.
    *session = staged;
    if checkpoint.is_err() {
        // A strict reload can reveal a partially/fully written generation even
        // though ACK was not confirmed. Bind it only so existing error/cancel
        // finalization can retire that observed generation; never continue it.
        if !session.metadata.contains_key(SCOPED_LOOP_UNCERTAIN_KEY)
            && has_scoped_loop_consumption(session, &envelope)
        {
            if let Some(state) = accepted.as_ref().filter(|state| {
                counted_loop::read_loop_state(session)
                    .ok()
                    .flatten()
                    .is_some_and(|actual| actual.generation == state.generation)
            }) {
                let _ = super::install_command_effects(policy, state, &input.command);
            }
        }
        return checkpoint;
    }
    if let Some(state) = accepted {
        super::install_command_effects(policy, &state, &input.command).map_err(super::fatal)?;
    }
    if policy.execution_cancel()?.is_cancelled() {
        return Err(AgentError::Cancelled);
    }
    Ok(())
}

async fn propose(
    session: &Session,
    config: &AgentLoopConfig,
    policy: &LoopControlConfig,
    input: &ScopedLoopControlInput,
    rounds_spent: u32,
) -> Result<Result<LoopState, &'static str>, AgentError> {
    input
        .validate()
        .map_err(|_| super::fatal("remote command outside host grant"))?;
    let execution = policy
        .execution
        .get()
        .ok_or_else(|| super::fatal("pipeline execution absent"))?;
    let current = counted_loop::read_loop_state(session)?;
    let hash = scoped_loop_operation_fingerprint(&input.binding, &input.command);
    let prior = current
        .as_ref()
        .and_then(|state| state.control.as_ref())
        .and_then(|control| {
            control
                .operations
                .iter()
                .find(|operation| operation.receipt.operation_id == input.operation_id())
        });
    if let Some(prior) = prior {
        if prior.origin.as_ref() != Some(&input.binding) || prior.payload_sha256 != hash {
            return Ok(Err("operation_payload_or_origin_conflict"));
        }
        if policy.generation() != Some(prior.receipt.generation.as_str())
            || current
                .as_ref()
                .is_none_or(|state| state.owner_execution_id != execution.id)
        {
            return Ok(Err("generation_not_current_activation"));
        }
        // A prior successful input is recovered through its consumption proof,
        // not by generating another state revision or selecting a newer receipt.
        return Ok(Err("committed_operation_requires_existing_consumption"));
    }
    if policy.stop.is_cancelled() && !matches!(input.command, LoopControlRequest::Stop { .. }) {
        return Ok(Err("host_stop_requested"));
    }
    if matches!(input.command, LoopControlRequest::Start { .. }) {
        if let Some(reason) = super::suspension::start_refusal(session, config).await {
            return Ok(Err(reason));
        }
        policy.validate_current_owner(session).await?;
        if execution.cancel.is_cancelled() {
            return Err(AgentError::Cancelled);
        }
    }
    Ok(super::accept_command_at_boundary(
        policy,
        config,
        current,
        &input.command,
        &execution.id,
        rounds_spent,
        hash,
        Some(&input.binding),
    ))
}

impl LoopControlConfig {
    /// Installed before polling the runtime. The clone shares the actual live
    /// binding and successful-cleanup bit, never recreating authority from disk.
    pub(crate) fn bind_transport_cleanup(
        &self,
        registration: &mut crate::session_activation::SessionRunRegistration,
        storage: Arc<dyn Storage>,
        inbox: Option<Arc<dyn SessionInboxPort>>,
        owner: Option<bamboo_domain::storage::RootActorRuntimeWrite>,
    ) {
        if self.supervisor_grant.is_none() {
            return;
        }
        let policy = self.clone();
        registration.set_scoped_transport_cleanup(move || {
            let policy = policy.clone();
            let storage = storage.clone();
            let inbox = inbox.clone();
            let owner = owner.clone();
            async move {
                policy
                    .retire_transport(&storage, inbox, owner.as_ref())
                    .await
            }
        });
    }

    async fn retire_transport(
        &self,
        storage: &Arc<dyn Storage>,
        inbox: Option<Arc<dyn SessionInboxPort>>,
        owner: Option<&bamboo_domain::storage::RootActorRuntimeWrite>,
    ) -> std::io::Result<()> {
        if self
            .transport_retired
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(());
        }
        let Some(binding) = self.live_supervisor_binding.get() else {
            return Ok(());
        };
        let ownership = self
            .owner
            .ownership()
            .ok_or_else(|| std::io::Error::other("target retirement owner missing"))?;
        ownership.close_scoped_transport(binding).await?;
        let inbox =
            inbox.ok_or_else(|| std::io::Error::other("target retirement Inbox missing"))?;
        let retained_binding = binding.clone();
        storage
            .retire_scoped_loop_transport(bamboo_domain::storage::ScopedLoopTransportRetirement {
                binding,
                inbox,
                owner,
                retain: Arc::new(move || {
                    ownership.retain_closed_scoped_transport(&retained_binding)
                }),
            })
            .await?;
        // Only the complete physical sweep, including ACK/cancel jobs, proves
        // success. Subsequent registration finish adds no fallible post-Complete work.
        self.transport_retired
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

pub(crate) async fn retire_terminal_transport(config: &AgentLoopConfig) -> Result<(), AgentError> {
    let Some(policy) = config
        .loop_control
        .as_ref()
        .filter(|policy| policy.supervisor_grant_id().is_some())
    else {
        return Ok(());
    };
    let storage = config
        .storage
        .as_ref()
        .ok_or_else(|| super::fatal("target retirement storage missing"))?;
    policy
        .retire_transport(
            storage,
            config.session_inbox.clone(),
            config
                .persistence
                .as_ref()
                .and_then(|persistence| persistence.root_actor_writer())
                .as_ref(),
        )
        .await
        .map_err(|error| super::fatal(&format!("target transport retirement failed: {error}")))
}
