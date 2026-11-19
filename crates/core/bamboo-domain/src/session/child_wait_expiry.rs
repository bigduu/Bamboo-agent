//! Expiry revokes permission to wait. It makes no claim about child outcomes.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    AgentRuntimeState, ConversationGenerationError, Session, SessionInboxReceipt,
    SessionMessageBody, SessionMessageContent, SessionMessageEnvelope, SessionMessageId,
    SessionMessageKind, SessionMessageSource, SessionProviderMessage, SessionRuntimeInstruction,
    WaitingForChildrenState,
};

pub const CHILD_WAIT_EXPIRY_RECEIPT_KEY: &str = "runtime.child_wait_expiry_receipt_v1";
const SUBSYSTEM: &str = "child_wait_watchdog";
const INSTRUCTION: &str = "child_wait_expired_v1";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildWaitExpiry {
    pub target_birth: DateTime<Utc>,
    pub run_id: String,
    pub actor_activation_id: String,
    pub wait: WaitingForChildrenState,
    conversation: Option<String>,
}

impl ChildWaitExpiry {
    pub fn observe(
        session: &Session,
        runtime: &AgentRuntimeState,
        actor_activation_id: String,
        wait: WaitingForChildrenState,
    ) -> Result<Self, ConversationGenerationError> {
        Ok(Self {
            target_birth: session.created_at,
            run_id: runtime.run_id.clone(),
            actor_activation_id,
            wait,
            conversation: session.conversation_reset_sha256()?,
        })
    }

    pub fn is_due(&self, now: DateTime<Utc>) -> bool {
        self.wait.child_session_ids.is_empty()
            || self.wait.timeout_at.is_some_and(|deadline| deadline <= now)
    }

    pub fn matches_lifetime(&self, session: &Session) -> bool {
        session.created_at == self.target_birth
            && session
                .conversation_reset_sha256()
                .is_ok_and(|generation| generation == self.conversation)
    }

    pub fn envelope(&self, target: &str) -> Result<SessionMessageEnvelope, serde_json::Error> {
        let observation = serde_json::to_value(self)?;
        let id = SessionMessageId::stable(
            INSTRUCTION,
            &serde_json::json!({"target_session_id": target, "observation": observation}),
        );
        let content = SessionMessageContent::text(
            "The child wait expired. Children may still be running. Check their status before drawing conclusions or starting replacements.",
        );
        Ok(SessionMessageEnvelope {
            id,
            source: SessionMessageSource::Runtime {
                subsystem: SUBSYSTEM.into(),
            },
            target_session_id: target.into(),
            kind: SessionMessageKind::RuntimeInstruction,
            body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                instruction: INSTRUCTION.into(),
                content: Some(content.clone()),
                data: Some(observation),
                provider_message: Some(SessionProviderMessage {
                    content,
                    metadata: serde_json::Map::from_iter([
                        ("hidden_from_ui".into(), true.into()),
                        ("runtime_kind".into(), "child_wait_watchdog_resume".into()),
                    ]),
                    never_compress: false,
                }),
            }),
            created_at: self.wait.registered_at,
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: None,
        })
    }
}

impl SessionMessageEnvelope {
    fn child_wait_expiry_instruction(&self) -> Option<&SessionRuntimeInstruction> {
        match (&self.source, &self.body) {
            (
                SessionMessageSource::Runtime { subsystem },
                SessionMessageBody::RuntimeInstruction(instruction),
            ) if subsystem == SUBSYSTEM && instruction.instruction == INSTRUCTION => {
                Some(instruction)
            }
            _ => None,
        }
    }

    /// Keep an invalid expiry notice out of generic delivery as well.
    pub fn is_child_wait_expiry_notice(&self) -> bool {
        self.child_wait_expiry_instruction().is_some()
    }

    pub fn child_wait_expiry(&self) -> Option<ChildWaitExpiry> {
        let instruction = self.child_wait_expiry_instruction()?;
        let expiry: ChildWaitExpiry = serde_json::from_value(instruction.data.clone()?).ok()?;
        (expiry.envelope(&self.target_session_id).ok().as_ref() == Some(self)).then_some(expiry)
    }

    pub fn is_exact_runtime_startup(&self) -> bool {
        self.background_exec_completion_handle().is_some() || self.child_wait_expiry().is_some()
    }
}

/// The receipt and the cleared wait are written in the same runtime snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedChildWaitExpiry {
    pub expiry: ChildWaitExpiry,
    id: SessionMessageId,
    generation: u64,
}

impl CommittedChildWaitExpiry {
    pub fn new(expiry: ChildWaitExpiry, receipt: &SessionInboxReceipt) -> Self {
        Self {
            expiry,
            id: receipt.id.clone(),
            generation: receipt.generation,
        }
    }

    pub fn receipt(&self) -> SessionInboxReceipt {
        SessionInboxReceipt {
            id: self.id.clone(),
            generation: self.generation,
        }
    }

    pub fn matches_input(
        session: &Session,
        envelope: &SessionMessageEnvelope,
        generation: u64,
    ) -> bool {
        Self::from_session(session).is_some_and(|proof| {
            proof.generation == generation
                && proof.id == envelope.id
                && proof.expiry.envelope(&session.id).ok().as_ref() == Some(envelope)
                && session.last_run_status().as_deref() == Some("suspended")
                && session.agent_runtime_state.as_ref().is_some_and(|runtime| {
                    runtime.run_id == proof.expiry.run_id
                        && runtime.waiting_for_children.is_none()
                        && runtime.waiting_for_bash.is_none()
                })
        })
    }

    pub fn from_session(session: &Session) -> Option<Self> {
        let committed: Self =
            serde_json::from_str(session.metadata.get(CHILD_WAIT_EXPIRY_RECEIPT_KEY)?).ok()?;
        (committed.generation > 0
            && committed.expiry.matches_lifetime(session)
            && committed.expiry.envelope(&session.id).ok()?.id == committed.id)
            .then_some(committed)
    }
}
