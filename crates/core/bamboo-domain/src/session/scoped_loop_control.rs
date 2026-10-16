//! Typed live Supervisor control carried by the existing Session Inbox.
//!
//! These values describe observations and checkpoint proofs. Possessing or
//! deserializing one never grants runtime, Supervisor or persistence authority.
use std::{collections::BTreeSet, sync::Arc};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{LoopControlRequest, Session};
use crate::{
    canonical_json_bytes, ProjectId, RootActorRuntimePublisher, RootActorRuntimeWrite,
    SessionActivationPort, SessionInboxClaim, SessionInboxOwnedClaim, SessionInboxPort,
    SessionMessageBody, SessionMessageEnvelope, SessionMessageId, SessionMessageKind,
    SessionMessageSource, SessionRuntimeInstruction, SupervisorReference,
    DEFAULT_SUPERVISOR_SESSION_ID,
};

pub const SCOPED_LOOP_INSTRUCTION: &str = "supervisor_loop_control_v1";
pub const SCOPED_LOOP_SUBSYSTEM: &str = "supervisor_loop_control";
pub const SCOPED_LOOP_UNCERTAIN_KEY: &str = "runtime.scoped_loop_checkpoint_uncertain";
pub const SCOPED_LOOP_CONSUMPTION_KEY: &str = "runtime.scoped_loop_consumption_v1";
pub const MAX_SCOPED_LOOP_CONSUMPTIONS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoopControlAction {
    Status,
    Start,
    Update,
    Stop,
}

impl LoopControlAction {
    pub fn of(command: &LoopControlRequest) -> Self {
        match command {
            LoopControlRequest::Status => Self::Status,
            LoopControlRequest::Start { .. } => Self::Start,
            LoopControlRequest::Update { .. } => Self::Update,
            LoopControlRequest::Stop { .. } => Self::Stop,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedLoopControlBinding {
    pub grant_id: Uuid,
    pub supervisor: SupervisorReference,
    pub supervisor_tool_authority_revision: u64,
    pub target_session_id: String,
    pub target_created_at: DateTime<Utc>,
    pub target_metadata_version: u64,
    pub target_project_id: ProjectId,
    pub link_revision: u64,
    /// The real router registration, distinct from the pipeline UUID below.
    pub owner_run_id: String,
    pub execution_id: String,
    pub allowed_actions: BTreeSet<LoopControlAction>,
    pub max_iterations: u32,
    pub max_total_rounds: u32,
}

impl ScopedLoopControlBinding {
    pub fn validate(&self) -> Result<(), String> {
        crate::validate_supervisor_target_id(&self.target_session_id)?;
        if self.grant_id.is_nil()
            || self.supervisor.session_id != DEFAULT_SUPERVISOR_SESSION_ID
            || self.supervisor.incarnation_id.is_nil()
            || self.target_session_id == self.supervisor.session_id
            || self.link_revision == 0
            || !crate::valid_session_run_id(&self.owner_run_id)
            || !crate::valid_session_run_id(&self.execution_id)
            || self.allowed_actions.is_empty()
            || !(1..=100).contains(&self.max_iterations)
            || !(self.max_iterations..=1000).contains(&self.max_total_rounds)
        {
            return Err("invalid live Supervisor Loop binding".into());
        }
        Ok(())
    }

    pub fn authorize(&self, command: &LoopControlRequest) -> Result<(), String> {
        self.validate()?;
        command.validate()?;
        if !self
            .allowed_actions
            .contains(&LoopControlAction::of(command))
        {
            return Err("Supervisor Loop action is not host-granted".into());
        }
        let (iterations, rounds) = match command {
            LoopControlRequest::Start {
                max_iterations,
                max_total_rounds,
                ..
            } => (Some(*max_iterations), Some(*max_total_rounds)),
            LoopControlRequest::Update {
                max_iterations,
                max_total_rounds,
                ..
            } => (*max_iterations, *max_total_rounds),
            _ => (None, None),
        };
        if iterations.is_some_and(|n| n > self.max_iterations)
            || rounds.is_some_and(|n| n > self.max_total_rounds)
        {
            return Err("Supervisor Loop command exceeds its host grant".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedLoopControlInput {
    pub binding: ScopedLoopControlBinding,
    pub command: LoopControlRequest,
}

impl ScopedLoopControlInput {
    pub fn validate(&self) -> Result<(), String> {
        self.binding.authorize(&self.command)?;
        if matches!(self.command, LoopControlRequest::Status) {
            return Err("Loop status is a read, never an Inbox input".into());
        }
        Ok(())
    }

    pub fn operation_id(&self) -> &str {
        match &self.command {
            LoopControlRequest::Start { operation_id, .. }
            | LoopControlRequest::Update { operation_id, .. }
            | LoopControlRequest::Stop { operation_id, .. } => operation_id,
            LoopControlRequest::Status => "",
        }
    }

    pub fn envelope(&self) -> Result<SessionMessageEnvelope, String> {
        self.validate()?;
        Ok(SessionMessageEnvelope {
            id: SessionMessageId::stable(
                "supervisor_loop_control",
                &serde_json::json!({
                    "supervisor": self.binding.supervisor,
                    "target": self.binding.target_session_id,
                    "birth": self.binding.target_created_at,
                    "grant": self.binding.grant_id,
                    "owner": self.binding.owner_run_id,
                    "operation_id": self.operation_id(),
                }),
            ),
            source: SessionMessageSource::Runtime {
                subsystem: SCOPED_LOOP_SUBSYSTEM.into(),
            },
            target_session_id: self.binding.target_session_id.clone(),
            kind: SessionMessageKind::RuntimeInstruction,
            body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                instruction: SCOPED_LOOP_INSTRUCTION.into(),
                content: None,
                data: Some(serde_json::to_value(self).map_err(|e| e.to_string())?),
                provider_message: None,
            }),
            created_at: Utc::now(),
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: Some(self.operation_id().into()),
        })
    }
}

pub fn is_scoped_loop_control(envelope: &SessionMessageEnvelope) -> bool {
    matches!(&envelope.body, SessionMessageBody::RuntimeInstruction(instruction)
        if instruction.instruction == SCOPED_LOOP_INSTRUCTION)
}

pub fn scoped_loop_control_input(
    envelope: &SessionMessageEnvelope,
) -> Result<Option<ScopedLoopControlInput>, String> {
    if !is_scoped_loop_control(envelope) {
        return Ok(None);
    }
    let SessionMessageBody::RuntimeInstruction(instruction) = &envelope.body else {
        unreachable!()
    };
    if envelope.kind != SessionMessageKind::RuntimeInstruction
        || envelope.source
            != (SessionMessageSource::Runtime {
                subsystem: SCOPED_LOOP_SUBSYSTEM.into(),
            })
        || instruction.content.is_some()
        || instruction.provider_message.is_some()
        || envelope.thread_id.is_some()
        || envelope.in_reply_to.is_some()
    {
        return Err("invalid typed Supervisor Loop carrier".into());
    }
    let input: ScopedLoopControlInput = serde_json::from_value(
        instruction
            .data
            .clone()
            .ok_or("Supervisor Loop input is absent")?,
    )
    .map_err(|e| e.to_string())?;
    input.validate()?;
    if input.binding.target_session_id != envelope.target_session_id
        || envelope.correlation_id.as_deref() != Some(input.operation_id())
        || input.envelope()?.id != envelope.id
    {
        return Err("Supervisor Loop input identity changed".into());
    }
    Ok(Some(input))
}

pub fn scoped_loop_operation_fingerprint(
    binding: &ScopedLoopControlBinding,
    command: &LoopControlRequest,
) -> String {
    hex::encode(Sha256::digest(canonical_json_bytes(
        &serde_json::json!({"origin":binding,"command":command}),
    )))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedLoopControlConsumption {
    pub input_id: SessionMessageId,
    pub input_sha256: String,
    pub binding: ScopedLoopControlBinding,
    pub operation_id: String,
    /// The target checkpoint accepted the configuration. This does not mean
    /// a pending objective has been admitted to a provider iteration.
    pub configured: bool,
    pub receipt: Option<serde_json::Value>,
    pub rejection: Option<String>,
}

pub fn scoped_loop_consumptions(
    session: &Session,
) -> Result<Vec<ScopedLoopControlConsumption>, String> {
    let Some(bytes) = session.metadata.get(SCOPED_LOOP_CONSUMPTION_KEY) else {
        return Ok(Vec::new());
    };
    if bytes.len() > 64 * 1024 {
        return Err("Supervisor Loop consumption checkpoint is too large".into());
    }
    let records: Vec<ScopedLoopControlConsumption> =
        serde_json::from_str(bytes).map_err(|e| e.to_string())?;
    if records.len() > MAX_SCOPED_LOOP_CONSUMPTIONS {
        return Err("Supervisor Loop consumption limit exceeded".into());
    }
    Ok(records)
}

pub fn has_scoped_loop_consumption(session: &Session, envelope: &SessionMessageEnvelope) -> bool {
    let Ok(Some(input)) = scoped_loop_control_input(envelope) else {
        return false;
    };
    let digest = hex::encode(Sha256::digest(canonical_json_bytes(
        &envelope.idempotency_semantics(),
    )));
    scoped_loop_consumptions(session).is_ok_and(|records| {
        records.iter().any(|record| {
            record.input_id == envelope.id
                && record.input_sha256 == digest
                && record.binding == input.binding
                && record.operation_id == input.operation_id()
                && (record.configured == record.receipt.is_some())
                && (record.configured != record.rejection.is_some())
        })
    })
}

/// Active and pending remote objectives keep their independent origins; a
/// self command is not an implicit transfer of this authority.
pub fn scoped_loop_objective_origins(
    session: &Session,
) -> Result<Vec<ScopedLoopControlBinding>, String> {
    let Some(bytes) = session.metadata.get("runtime.counted_loop") else {
        return Ok(Vec::new());
    };
    let state: serde_json::Value = serde_json::from_str(bytes).map_err(|e| e.to_string())?;
    let Some(control) = state.get("control") else {
        return Ok(Vec::new());
    };
    let mut origins = Vec::new();
    for value in [
        control.get("objective_origin"),
        control
            .get("pending_objective")
            .and_then(|p| p.get("origin")),
    ]
    .into_iter()
    .flatten()
    {
        if value.is_null() {
            continue;
        }
        let binding: ScopedLoopControlBinding =
            serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
        binding.validate()?;
        if !origins.contains(&binding) {
            origins.push(binding);
        }
    }
    Ok(origins)
}

#[derive(Debug, Clone)]
pub enum ScopedLoopControlClaim {
    Legacy(SessionInboxClaim),
    Owned(SessionInboxOwnedClaim),
}

impl ScopedLoopControlClaim {
    pub fn envelope(&self) -> &SessionMessageEnvelope {
        match self {
            Self::Legacy(c) => &c.envelope,
            Self::Owned(c) => &c.claim.envelope,
        }
    }
    pub fn generation(&self) -> u64 {
        match self {
            Self::Legacy(c) => c.generation,
            Self::Owned(c) => c.claim.generation,
        }
    }
}

/// Reuses the existing full checkpoint and optional Inbox ACK protocol. The
/// concrete backend must retain canonical and exact runtime authority through
/// every physical write; a public load/checkpoint sequence is insufficient.
pub struct ScopedLoopControlCheckpoint<'a> {
    pub session: &'a mut Session,
    pub bindings: &'a [ScopedLoopControlBinding],
    pub activation: Arc<dyn SessionActivationPort>,
    pub owner: Option<&'a RootActorRuntimeWrite>,
    pub input: Option<(Arc<dyn SessionInboxPort>, ScopedLoopControlClaim)>,
    pub publish: RootActorRuntimePublisher,
}

#[cfg(test)]
mod objective_contract_tests;
