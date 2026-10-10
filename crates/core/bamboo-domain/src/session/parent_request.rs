//! Typed, bounded direct-parent request proof carried by a canonical Session message.
//!
//! This contract describes an undecided request. It grants no permission and
//! cannot reconstruct the live Host scope that created a forced approval ask.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    is_matching_session_message, ActorSession, Session, SessionKind, SessionMessageBody,
    SessionMessageEnvelope, SessionMessageId, SessionMessageKind, SessionMessageSource,
};

pub const PARENT_REQUEST_VERSION: u32 = 1;
pub const PARENT_REQUEST_MAX_BYTES: usize = 8 * 1024;
const FORCED_PERMISSION_INSTRUCTION: &str = "direct_parent_forced_permission_request_v1";
const FORCED_PERMISSION_TERMINAL: &str = "direct_parent_forced_permission_terminal_v1";
const FORCED_PERMISSION_SUBSYSTEM: &str = "direct_parent_permission_review";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentRequestActor {
    pub session_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentRequestActivation {
    pub host_scope: String,
    pub attempt: u32,
    pub run: String,
    pub epoch: u64,
    pub reply: String,
}

/// This envelope is an upper bound on what a future resolver may decide. The
/// existing live reviewer only consumes ApproveOnce in its current Host scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParentRequestDelegation {
    ExactOperationOnce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParentRequestOption {
    Deny,
    ApproveOnce,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ParentRequestKind {
    ForcedPermission {
        operation_digest: String,
        policy_revision: u64,
        maximum_delegation: ParentRequestDelegation,
        options: Vec<ParentRequestOption>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentRequest {
    pub version: u32,
    pub id: SessionMessageId,
    pub generation: String,
    pub child: ParentRequestActor,
    pub parent: ParentRequestActor,
    pub root_session_id: String,
    pub project_id: Option<String>,
    pub activation: ParentRequestActivation,
    pub deadline: DateTime<Utc>,
    pub kind: ParentRequestKind,
}

/// Terminal proof for one already recorded request. A durable resolution is
/// an audit fact, not a reusable permission grant or a replacement Host scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentResolution {
    pub version: u32,
    pub id: SessionMessageId,
    pub request_id: SessionMessageId,
    pub child: ParentRequestActor,
    pub parent: ParentRequestActor,
    pub activation: ParentRequestActivation,
    pub operation_digest: String,
    pub policy_revision: u64,
    pub deadline: DateTime<Utc>,
    pub resolved_at: DateTime<Utc>,
    pub decision: ParentRequestOption,
}

impl ParentResolution {
    pub fn for_forced_permission(
        request: &ParentRequest,
        resolved_at: DateTime<Utc>,
        approved: bool,
    ) -> Self {
        let ParentRequestKind::ForcedPermission {
            operation_digest,
            policy_revision,
            ..
        } = &request.kind;
        Self {
            version: PARENT_REQUEST_VERSION,
            id: SessionMessageId::stable(
                "direct-parent-forced-terminal-v1",
                &serde_json::json!(request.id),
            ),
            request_id: request.id.clone(),
            child: request.child.clone(),
            parent: request.parent.clone(),
            activation: request.activation.clone(),
            operation_digest: operation_digest.clone(),
            policy_revision: *policy_revision,
            deadline: request.deadline,
            resolved_at,
            decision: if approved {
                ParentRequestOption::ApproveOnce
            } else {
                ParentRequestOption::Deny
            },
        }
    }

    pub fn from_forced_permission_terminal(
        request_envelope: &SessionMessageEnvelope,
        terminal: &SessionMessageEnvelope,
    ) -> Option<Self> {
        let request = ParentRequest::from_forced_permission_envelope(request_envelope)?;
        if terminal.kind != SessionMessageKind::RuntimeInstruction
            || terminal.source != request_envelope.source
            || terminal.target_session_id != request.parent.session_id
            || terminal.in_reply_to.as_ref() != Some(&request.id)
            || terminal.thread_id.is_some()
            || terminal.attempt.is_some()
            || terminal.correlation_id.is_some()
            || serde_json::to_vec(terminal).ok()?.len() > PARENT_REQUEST_MAX_BYTES
        {
            return None;
        }
        let SessionMessageBody::RuntimeInstruction(body) = &terminal.body else {
            return None;
        };
        if body.instruction != FORCED_PERMISSION_TERMINAL {
            return None;
        }
        let SessionMessageBody::RuntimeInstruction(request_body) = &request_envelope.body else {
            return None;
        };
        let data = body.data.as_ref()?;
        let resolution: Self =
            serde_json::from_value(data.get("parent_resolution")?.clone()).ok()?;
        let approved = resolution.decision == ParentRequestOption::ApproveOnce;
        if resolution != Self::for_forced_permission(&request, resolution.resolved_at, approved)
            || resolution.id != terminal.id
            || resolution.resolved_at != terminal.created_at
            || resolution.resolved_at < request_envelope.created_at
            || (approved && resolution.resolved_at >= resolution.deadline)
            || data.get("request")?.as_str()? != request.id.as_str()
            || serde_json::to_value(approved).ok()? != *data.get("approved")?
            || data.get("live")? != request_body.data.as_ref()?.get("live")?
        {
            return None;
        }
        Some(resolution)
    }
}

impl ParentRequest {
    /// Validate the complete durable forced-ask record, including the legacy
    /// fields still consumed by the live reviewer. This is a parser, not an
    /// authorization or grant-consumption API.
    pub fn from_forced_permission_envelope(envelope: &SessionMessageEnvelope) -> Option<Self> {
        if envelope.kind != SessionMessageKind::RuntimeInstruction
            || envelope.source
                != (SessionMessageSource::Runtime {
                    subsystem: FORCED_PERMISSION_SUBSYSTEM.into(),
                })
            || envelope.in_reply_to.is_some()
            || envelope.thread_id.is_some()
            || envelope.attempt.is_some()
            || envelope.correlation_id.is_some()
            || serde_json::to_vec(envelope).ok()?.len() > PARENT_REQUEST_MAX_BYTES
        {
            return None;
        }
        let SessionMessageBody::RuntimeInstruction(body) = &envelope.body else {
            return None;
        };
        if body.instruction != FORCED_PERMISSION_INSTRUCTION {
            return None;
        }
        let data = body.data.as_ref()?;
        let request: Self = serde_json::from_value(data.get("parent_request")?.clone()).ok()?;
        let ParentRequestKind::ForcedPermission {
            operation_digest,
            policy_revision,
            maximum_delegation,
            options,
        } = &request.kind;
        let live = data.get("live")?;
        let uuid_canonical = |value: &str| {
            uuid::Uuid::parse_str(value)
                .ok()
                .is_some_and(|parsed| parsed.to_string() == value)
        };
        let valid_id = |value: &str| {
            !value.is_empty()
                && value.len() <= 256
                && value.trim() == value
                && !value.contains('/')
                && !value.contains('\\')
                && !value.contains("..")
        };
        let expected_id = SessionMessageId::stable(
            "direct-parent-forced-approval-v1",
            &serde_json::json!({
                "child": request.child.session_id,
                "birth": request.child.created_at,
                "parent": request.parent.session_id,
                "parent_birth": request.parent.created_at,
                "generation": request.generation,
            }),
        );
        if request.version != PARENT_REQUEST_VERSION
            || request.id != envelope.id
            || request.id != expected_id
            || !uuid_canonical(&request.generation)
            || !uuid_canonical(&request.activation.host_scope)
            || !valid_id(&request.child.session_id)
            || !valid_id(&request.parent.session_id)
            || !valid_id(&request.root_session_id)
            || request.child.session_id == request.parent.session_id
            || request.child.created_at < request.parent.created_at
            || request.parent.session_id != envelope.target_session_id
            || serde_json::to_value(request.parent.created_at).ok()?
                != *data.get("parent_created_at")?
            || request.child.session_id != data.get("child_session_id")?.as_str()?
            || serde_json::to_value(request.child.created_at).ok()?
                != *data.get("child_created_at")?
            || request.root_session_id != data.get("root_session_id")?.as_str()?
            || serde_json::to_value(&request.project_id).ok()? != *data.get("project_id")?
            || request.generation != data.get("request_generation")?.as_str()?
            || operation_digest != data.get("operation_digest")?.as_str()?
            || !operation_digest.starts_with("stable-")
            || operation_digest.len() != 71
            || !operation_digest[7..]
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || *policy_revision != data.get("policy_revision")?.as_u64()?
            || *maximum_delegation != ParentRequestDelegation::ExactOperationOnce
            || options.as_slice() != [ParentRequestOption::Deny, ParentRequestOption::ApproveOnce]
            || request.activation.run.is_empty()
            || request.activation.run.len() > 256
            || request.activation.reply.is_empty()
            || request.activation.reply.len() > 256
            || request.activation.host_scope != live.get("host_scope")?.as_str()?
            || serde_json::to_value(request.activation.attempt).ok()? != *live.get("attempt")?
            || serde_json::to_value(&request.activation.run).ok()? != *live.get("run")?
            || serde_json::to_value(request.activation.epoch).ok()? != *live.get("epoch")?
            || serde_json::to_value(&request.activation.reply).ok()? != *live.get("reply")?
            || serde_json::to_value(request.deadline).ok()? != *live.get("deadline")?
            || request.deadline <= envelope.created_at
            || request.deadline - envelope.created_at > chrono::Duration::seconds(240)
        {
            return None;
        }
        Some(request)
    }

    /// Read one canonical proof from the owning direct parent's transcript.
    /// Both Sessions must be loaded by the caller from its trusted Store; an
    /// arbitrary supplied actor id or copied provider text cannot authorize it.
    pub fn inspect_direct_parent(
        parent: &Session,
        child: &Session,
        request_id: &SessionMessageId,
    ) -> Option<Self> {
        let parent_actor = ActorSession::from_session(parent).ok()?;
        let child_actor = ActorSession::from_session(child).ok()?;
        if child.kind != SessionKind::Child
            || child_actor.parent_actor_id.as_deref() != Some(parent.id.as_str())
            || parent_actor.root_actor_id != child_actor.root_actor_id
            || parent_actor.project_id != child_actor.project_id
            || parent_actor.spawn_depth.checked_add(1) != Some(child_actor.spawn_depth)
        {
            return None;
        }
        let mut found = None;
        for message in &parent.messages {
            if message.id != request_id.as_str()
                && message
                    .metadata
                    .as_ref()
                    .and_then(|metadata| {
                        metadata.pointer("/session_message/body/data/parent_request/id")
                    })
                    .and_then(serde_json::Value::as_str)
                    == Some(request_id.as_str())
            {
                return None;
            }
            if message.id != request_id.as_str() {
                continue;
            }
            if found.is_some() {
                return None;
            }
            let marker = message.metadata.as_ref()?.get("session_message")?;
            let envelope: SessionMessageEnvelope = serde_json::from_value(marker.clone()).ok()?;
            if !is_matching_session_message(message, &envelope) {
                return None;
            }
            let request = Self::from_forced_permission_envelope(&envelope)?;
            if request.child.session_id != child.id
                || request.child.created_at != child.created_at
                || request.parent.session_id != parent.id
                || request.parent.created_at != parent.created_at
                || request.root_session_id != child.root_session_id
                || request.project_id != child_actor.project_id
            {
                return None;
            }
            found = Some(request);
        }
        found
    }
}
