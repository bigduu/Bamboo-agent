//! Durable, typed local Child clarification addressed to its direct parent.
//!
//! The Child's pending question and paired tool result are the source of truth.
//! A copied inbox envelope or provider text is never enough to answer it.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    is_matching_session_message, ActorSession, Message, ParentRequestActor, PendingQuestion,
    PendingQuestionSource, Role, Session, SessionKind, SessionMessageBody, SessionMessageContent,
    SessionMessageEnvelope, SessionMessageId, SessionMessageKind, SessionMessageSource,
    SessionRuntimeInstruction,
};

pub const PARENT_QUESTION_VERSION: u32 = 1;
pub const PARENT_QUESTION_TEXT_MAX_BYTES: usize = 4 * 1024;
// A question appears in both the typed request and the parent-facing text.
// JSON escaping can expand each accepted input byte to six wire bytes.
pub const PARENT_QUESTION_MAX_BYTES: usize = 64 * 1024;
pub const PARENT_ANSWER_MAX_BYTES: usize = 4 * 1024;
// The typed request is bounded to 32 KiB, and its parent id also appears in
// the outer envelope. A 4 KiB answer appears twice; each byte may occupy six
// escaped JSON bytes. This bound covers all accepted request/answer pairs.
pub const PARENT_QUESTION_TERMINAL_MAX_BYTES: usize = 128 * 1024;
pub const PARENT_QUESTION_REQUEST_KEY: &str = "runtime.parent_question.request_v1";
pub const PARENT_QUESTION_RESOLUTION_KEY: &str = "runtime.parent_question.resolution_v1";
pub const PARENT_QUESTION_DEADLINE_SECONDS: i64 = 240;
const QUESTION_INSTRUCTION: &str = "direct_parent_clarification_request_v1";
const QUESTION_TERMINAL: &str = "direct_parent_clarification_terminal_v1";
const QUESTION_SUBSYSTEM: &str = "direct_parent_clarification";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParentQuestionKind {
    Clarification,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentQuestion {
    pub version: u32,
    pub kind: ParentQuestionKind,
    pub id: SessionMessageId,
    pub child: ParentRequestActor,
    pub parent: ParentRequestActor,
    pub root_session_id: String,
    pub project_id: Option<String>,
    pub tool_call_id: String,
    pub tool_name: String,
    pub tool_result_message_id: String,
    pub paired_result_digest: SessionMessageId,
    pub question: String,
    pub options: Vec<String>,
    pub allow_custom: bool,
    pub asked_at: DateTime<Utc>,
    pub issued_at: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
}

/// Written under the Child Session's compare-and-swap lock after an answer.
/// The answer is transcript data, never a permission or plan-mode receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentQuestionResolution {
    pub version: u32,
    pub request: ParentQuestion,
    pub resolved_at: DateTime<Utc>,
    pub outcome: ParentQuestionOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ParentQuestionOutcome {
    Answer { text: String },
    Expired,
}

impl ParentQuestion {
    /// Host-only construction under the canonical Child mutation lock. The
    /// caller persists this exact request alongside the pending question; a
    /// later outbox retry must reuse its id, issue time, and deadline.
    pub fn issue_at(parent: &Session, child: &Session, issued_at: DateTime<Utc>) -> Option<Self> {
        let parent_actor = ActorSession::from_session(parent).ok()?;
        let child_actor = ActorSession::from_session(child).ok()?;
        if child.kind != SessionKind::Child
            || child_actor.parent_actor_id.as_deref() != Some(parent.id.as_str())
            || parent_actor.root_actor_id != child_actor.root_actor_id
            || parent_actor.project_id != child_actor.project_id
            || parent_actor.spawn_depth.checked_add(1) != Some(child_actor.spawn_depth)
            || child.metadata.get("runtime.suspend_reason")?.as_str() != "awaiting_clarification"
        {
            return None;
        }
        let pending = child.pending_question.as_ref()?;
        if pending.source != PendingQuestionSource::DirectParent
            || pending.tool_call_id.is_empty()
            || pending.tool_call_id.len() > 256
            || pending.tool_call_id.trim() != pending.tool_call_id
            || pending.question.trim().is_empty()
        {
            return None;
        }
        let (paired_index, paired) =
            child
                .messages
                .iter()
                .enumerate()
                .rev()
                .find(|(_, message)| {
                    message.role == Role::Tool
                        && message.tool_call_id.as_deref() == Some(pending.tool_call_id.as_str())
                })?;
        if paired.content != format!("Clarification needed: {}", pending.question)
            || paired.tool_success != Some(true)
            || paired.metadata.is_some()
            || paired.content_parts.is_some()
            || paired.tool_calls.is_some()
            || (!pending.allow_custom && pending.options.is_empty())
            || pending.tool_name.is_empty()
            || pending.tool_name.len() > 256
            || !child.messages[..paired_index].iter().rev().any(|message| {
                message.role == Role::Assistant
                    && message.tool_calls.as_ref().is_some_and(|calls| {
                        calls.iter().any(|call| {
                            call.id == pending.tool_call_id
                                && call.function.name == pending.tool_name
                        })
                    })
            })
        {
            return None;
        }
        SessionMessageId::parse(&paired.id).ok()?;
        let mut question = Self {
            version: PARENT_QUESTION_VERSION,
            kind: ParentQuestionKind::Clarification,
            id: SessionMessageId::new(),
            child: ParentRequestActor {
                session_id: child.id.clone(),
                created_at: child.created_at,
            },
            parent: ParentRequestActor {
                session_id: parent.id.clone(),
                created_at: parent.created_at,
            },
            root_session_id: child.root_session_id.clone(),
            project_id: child_actor.project_id,
            tool_call_id: pending.tool_call_id.clone(),
            tool_name: pending.tool_name.clone(),
            tool_result_message_id: paired.id.clone(),
            paired_result_digest: SessionMessageId::stable(
                "direct-parent-question-result-v1",
                &serde_json::to_value(paired).ok()?,
            ),
            question: pending.question.clone(),
            options: pending.options.clone(),
            allow_custom: pending.allow_custom,
            asked_at: paired.created_at,
            issued_at,
            deadline: issued_at
                .checked_add_signed(chrono::Duration::seconds(PARENT_QUESTION_DEADLINE_SECONDS))?,
        };
        question.id = question.stable_id();
        if question.validate_shape()
            && serde_json::to_vec(&question.envelope())
                .is_ok_and(|bytes| bytes.len() <= PARENT_QUESTION_MAX_BYTES)
        {
            Some(question)
        } else {
            None
        }
    }

    /// Reconstruct the same request after a crash from the saved Child state.
    /// This does not assert that delivery to the parent succeeded.
    pub fn for_pending(parent: &Session, child: &Session) -> Option<Self> {
        let recorded: Self =
            serde_json::from_str(child.metadata.get(PARENT_QUESTION_REQUEST_KEY)?).ok()?;
        (Self::issue_at(parent, child, recorded.issued_at)? == recorded).then_some(recorded)
    }

    /// Expiration-only proof when the direct parent cannot be loaded. This
    /// never authorizes an answer or creates a parent transcript entry.
    pub fn for_orphan_pending(child: &Session) -> Option<Self> {
        let recorded: Self =
            serde_json::from_str(child.metadata.get(PARENT_QUESTION_REQUEST_KEY)?).ok()?;
        let child_actor = ActorSession::from_session(child).ok()?;
        let pending = child.pending_question.as_ref()?;
        if child.kind != SessionKind::Child
            || !recorded.validate_shape()
            || child_actor.parent_actor_id.as_deref() != Some(recorded.parent.session_id.as_str())
            || recorded.child.session_id != child.id
            || recorded.child.created_at != child.created_at
            || recorded.root_session_id != child.root_session_id
            || recorded.project_id != child_actor.project_id
            || child.metadata.get("runtime.suspend_reason")?.as_str() != "awaiting_clarification"
            || pending.source != PendingQuestionSource::DirectParent
            || pending.tool_call_id != recorded.tool_call_id
            || pending.tool_name != recorded.tool_name
            || pending.question != recorded.question
            || pending.options != recorded.options
            || pending.allow_custom != recorded.allow_custom
        {
            return None;
        }
        let (index, paired) = child
            .messages
            .iter()
            .enumerate()
            .rev()
            .find(|(_, message)| {
                message.role == Role::Tool
                    && message.tool_call_id.as_deref() == Some(recorded.tool_call_id.as_str())
            })?;
        if paired.id != recorded.tool_result_message_id
            || paired.created_at != recorded.asked_at
            || paired.content != format!("Clarification needed: {}", recorded.question)
            || paired.tool_success != Some(true)
            || paired.metadata.is_some()
            || paired.content_parts.is_some()
            || paired.tool_calls.is_some()
            || SessionMessageId::stable(
                "direct-parent-question-result-v1",
                &serde_json::to_value(paired).ok()?,
            ) != recorded.paired_result_digest
            || !child.messages[..index].iter().rev().any(|message| {
                message.role == Role::Assistant
                    && message.tool_calls.as_ref().is_some_and(|calls| {
                        calls.iter().any(|call| {
                            call.id == recorded.tool_call_id
                                && call.function.name == recorded.tool_name
                        })
                    })
            })
        {
            return None;
        }
        Some(recorded)
    }

    pub fn question_digest(
        pending: &PendingQuestion,
        tool_result_message_id: &str,
    ) -> SessionMessageId {
        SessionMessageId::stable(
            "direct-parent-question-content-v1",
            &serde_json::json!({"pending": pending, "tool_result_message_id": tool_result_message_id}),
        )
    }

    pub fn prefix_digest(messages: &[Message]) -> Option<SessionMessageId> {
        Some(SessionMessageId::stable(
            "direct-parent-question-prefix-v1",
            &serde_json::to_value(messages).ok()?,
        ))
    }

    pub fn validate_answer(&self, answer: &str) -> Result<(), &'static str> {
        if answer.trim().is_empty()
            || answer.len() > PARENT_ANSWER_MAX_BYTES
            || answer.contains('\0')
        {
            return Err("ParentQuestion answer must be non-empty bounded text");
        }
        if !self.allow_custom && !self.options.iter().any(|option| option == answer) {
            return Err("ParentQuestion answer must equal one of the offered options");
        }
        Ok(())
    }

    pub fn envelope(&self) -> SessionMessageEnvelope {
        SessionMessageEnvelope {
            id: self.id.clone(),
            source: SessionMessageSource::Runtime {
                subsystem: QUESTION_SUBSYSTEM.into(),
            },
            target_session_id: self.parent.session_id.clone(),
            kind: SessionMessageKind::RuntimeInstruction,
            body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                instruction: QUESTION_INSTRUCTION.into(),
                content: Some(SessionMessageContent::text(format!(
                    "Child {} asks its direct parent: {}\nReply with SubAgent(reply_to=\"{}\", message=<answer>).",
                    self.child.session_id, self.question, self.id
                ))),
                data: Some(serde_json::json!({"parent_request": self})),
                provider_message: None,
            }),
            created_at: self.issued_at,
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: None,
        }
    }

    pub fn from_envelope(envelope: &SessionMessageEnvelope) -> Option<Self> {
        if envelope.kind != SessionMessageKind::RuntimeInstruction
            || envelope.source
                != (SessionMessageSource::Runtime {
                    subsystem: QUESTION_SUBSYSTEM.into(),
                })
            || envelope.in_reply_to.is_some()
            || envelope.thread_id.is_some()
            || envelope.attempt.is_some()
            || envelope.correlation_id.is_some()
            || serde_json::to_vec(envelope).ok()?.len() > PARENT_QUESTION_MAX_BYTES
        {
            return None;
        }
        let SessionMessageBody::RuntimeInstruction(body) = &envelope.body else {
            return None;
        };
        if body.instruction != QUESTION_INSTRUCTION || body.provider_message.is_some() {
            return None;
        }
        let data = body.data.as_ref()?.as_object()?;
        if data.len() != 1 {
            return None;
        }
        let question: Self = serde_json::from_value(data.get("parent_request")?.clone()).ok()?;
        if !question.validate_shape()
            || question.id != envelope.id
            || question.parent.session_id != envelope.target_session_id
            || question.issued_at != envelope.created_at
            || body.content.as_ref()? != &SessionMessageContent::text(format!(
                "Child {} asks its direct parent: {}\nReply with SubAgent(reply_to=\"{}\", message=<answer>).",
                question.child.session_id, question.question, question.id
            ))
        {
            return None;
        }
        Some(question)
    }

    /// Require both the parent's canonical transcript proof and the Child's
    /// still-pending exact tool-result occurrence before answering.
    pub fn inspect_direct_parent(
        parent: &Session,
        child: &Session,
        request_id: &SessionMessageId,
    ) -> Option<Self> {
        let expected = Self::for_pending(parent, child)?;
        if &expected.id != request_id {
            return None;
        }
        let mut found = false;
        for message in &parent.messages {
            if message.id != request_id.as_str() {
                continue;
            }
            if found {
                return None;
            }
            let marker = message.metadata.as_ref()?.get("session_message")?;
            let envelope: SessionMessageEnvelope = serde_json::from_value(marker.clone()).ok()?;
            if !is_matching_session_message(message, &envelope)
                || envelope.to_provider_message().ok().is_none_or(|expected| {
                    serde_json::to_value(message).ok() != serde_json::to_value(expected).ok()
                })
                || Self::from_envelope(&envelope).as_ref() != Some(&expected)
            {
                return None;
            }
            found = true;
        }
        found.then_some(expected)
    }

    fn stable_id(&self) -> SessionMessageId {
        SessionMessageId::stable(
            "direct-parent-question-v1",
            &serde_json::json!({
                "child": self.child,
                "parent": self.parent,
                "root": self.root_session_id,
                "project": self.project_id,
                "tool_call": self.tool_call_id,
                "tool_name": self.tool_name,
                "tool_result": self.tool_result_message_id,
                "paired_result_digest": self.paired_result_digest,
                "question": self.question,
                "options": self.options,
                "allow_custom": self.allow_custom,
                "asked_at": self.asked_at,
                "issued_at": self.issued_at,
                "deadline": self.deadline,
            }),
        )
    }

    fn validate_shape(&self) -> bool {
        self.version == PARENT_QUESTION_VERSION
            && self.kind == ParentQuestionKind::Clarification
            && self.id == self.stable_id()
            && self.child.session_id != self.parent.session_id
            && !self.child.session_id.is_empty()
            && !self.parent.session_id.is_empty()
            && !self.root_session_id.is_empty()
            && SessionMessageId::parse(&self.tool_result_message_id).is_ok()
            && !self.tool_call_id.is_empty()
            && self.tool_call_id.len() <= 256
            && self.tool_call_id.trim() == self.tool_call_id
            && !self.tool_name.is_empty()
            && self.tool_name.len() <= 256
            && !self.question.trim().is_empty()
            && self.question.len() <= PARENT_QUESTION_TEXT_MAX_BYTES
            && self.issued_at <= self.deadline
            && self.deadline - self.issued_at
                == chrono::Duration::seconds(PARENT_QUESTION_DEADLINE_SECONDS)
            && self.options.len() <= 16
            && (self.allow_custom
                || (!self.options.is_empty()
                    && self.options.iter().all(|option| {
                        option.len() <= PARENT_ANSWER_MAX_BYTES && !option.contains('\0')
                    })))
            && self.options.iter().all(|option| !option.trim().is_empty())
            && serde_json::to_vec(self)
                .is_ok_and(|bytes| bytes.len() <= PARENT_QUESTION_MAX_BYTES / 2)
    }
}

impl ParentQuestionResolution {
    pub fn answered(
        question: &ParentQuestion,
        resolved_at: DateTime<Utc>,
        answer: &str,
    ) -> Option<Self> {
        if resolved_at >= question.deadline {
            return None;
        }
        question.validate_answer(answer).ok()?;
        let resolution = Self {
            version: PARENT_QUESTION_VERSION,
            request: question.clone(),
            resolved_at,
            outcome: ParentQuestionOutcome::Answer {
                text: answer.to_string(),
            },
        };
        // The Child CAS must never commit an answer whose parent terminal
        // cannot be published by the durable outbox reconciler.
        resolution.terminal_envelope()?;
        Some(resolution)
    }

    pub fn expired(question: &ParentQuestion, resolved_at: DateTime<Utc>) -> Option<Self> {
        if resolved_at < question.deadline {
            return None;
        }
        Some(Self {
            version: PARENT_QUESTION_VERSION,
            request: question.clone(),
            resolved_at,
            outcome: ParentQuestionOutcome::Expired,
        })
    }

    pub fn from_child(
        parent: &Session,
        child: &Session,
        request_id: &SessionMessageId,
    ) -> Option<Self> {
        let parent_actor = ActorSession::from_session(parent).ok()?;
        let child_actor = ActorSession::from_session(child).ok()?;
        if child_actor.parent_actor_id.as_deref() != Some(parent.id.as_str())
            || parent_actor.root_actor_id != child_actor.root_actor_id
            || parent_actor.project_id != child_actor.project_id
            || parent_actor.spawn_depth.checked_add(1) != Some(child_actor.spawn_depth)
        {
            return None;
        }
        let resolution = Self::from_orphan_child(child, request_id)?;
        if resolution.request.parent.session_id != parent.id
            || resolution.request.parent.created_at != parent.created_at
        {
            return None;
        }
        Some(resolution)
    }

    /// Child-local terminal proof used only to fail closed after a lost
    /// direct parent. The caller may block the Child; it cannot grant an
    /// answer or synthesize a parent terminal from this proof alone.
    pub fn from_orphan_child(child: &Session, request_id: &SessionMessageId) -> Option<Self> {
        let child_actor = ActorSession::from_session(child).ok()?;
        let resolution: Self =
            serde_json::from_str(child.metadata.get(PARENT_QUESTION_RESOLUTION_KEY)?).ok()?;
        let question: ParentQuestion =
            serde_json::from_str(child.metadata.get(PARENT_QUESTION_REQUEST_KEY)?).ok()?;
        if resolution.version != PARENT_QUESTION_VERSION
            || resolution.request != question
            || &question.id != request_id
            || !question.validate_shape()
            || child.kind != SessionKind::Child
            || child_actor.parent_actor_id.as_deref() != Some(question.parent.session_id.as_str())
            || question.child.session_id != child.id
            || question.child.created_at != child.created_at
            || question.root_session_id != child.root_session_id
            || question.project_id != child_actor.project_id
        {
            return None;
        }
        let paired = child
            .messages
            .iter()
            .find(|message| message.id == question.tool_result_message_id)?;
        if paired.role != Role::Tool
            || paired.tool_call_id.as_deref() != Some(question.tool_call_id.as_str())
        {
            return None;
        }
        match &resolution.outcome {
            ParentQuestionOutcome::Answer { text } => {
                if resolution.resolved_at < question.issued_at
                    || resolution.resolved_at >= question.deadline
                    || question.validate_answer(text).is_err()
                    || paired.content != *text
                    || paired.tool_success != Some(true)
                {
                    return None;
                }
            }
            ParentQuestionOutcome::Expired => {
                if resolution.resolved_at < question.deadline
                    || SessionMessageId::stable(
                        "direct-parent-question-result-v1",
                        &serde_json::to_value(paired).ok()?,
                    ) != question.paired_result_digest
                {
                    return None;
                }
            }
        }
        Some(resolution)
    }

    pub fn terminal_envelope(&self) -> Option<SessionMessageEnvelope> {
        let mut envelope = self.request.envelope();
        envelope.id = SessionMessageId::stable(
            "direct-parent-question-terminal-v1",
            &serde_json::json!(&self.request.id),
        );
        envelope.in_reply_to = Some(self.request.id.clone());
        envelope.created_at = self.resolved_at;
        let SessionMessageBody::RuntimeInstruction(body) = &mut envelope.body else {
            return None;
        };
        body.instruction = QUESTION_TERMINAL.into();
        body.content = Some(SessionMessageContent::text(match &self.outcome {
            ParentQuestionOutcome::Answer { text } => {
                format!("Direct parent answered Child clarification: {}", text)
            }
            ParentQuestionOutcome::Expired => {
                "Child clarification expired; Child is blocked awaiting new input.".into()
            }
        }));
        body.data = Some(serde_json::json!({"parent_resolution": self}));
        (serde_json::to_vec(&envelope).ok()?.len() <= PARENT_QUESTION_TERMINAL_MAX_BYTES)
            .then_some(envelope)
    }

    pub fn from_terminal(
        request: &SessionMessageEnvelope,
        terminal: &SessionMessageEnvelope,
    ) -> Option<Self> {
        let question = ParentQuestion::from_envelope(request)?;
        if terminal.kind != SessionMessageKind::RuntimeInstruction
            || terminal.source != request.source
            || terminal.target_session_id != request.target_session_id
            || terminal.in_reply_to.as_ref() != Some(&request.id)
            || terminal.thread_id.is_some()
            || terminal.attempt.is_some()
            || terminal.correlation_id.is_some()
            || serde_json::to_vec(terminal).ok()?.len() > PARENT_QUESTION_TERMINAL_MAX_BYTES
        {
            return None;
        }
        let SessionMessageBody::RuntimeInstruction(body) = &terminal.body else {
            return None;
        };
        if body.instruction != QUESTION_TERMINAL {
            return None;
        }
        let data = body.data.as_ref()?.as_object()?;
        if data.len() != 1 {
            return None;
        }
        let resolution: Self =
            serde_json::from_value(data.get("parent_resolution")?.clone()).ok()?;
        if resolution.version != PARENT_QUESTION_VERSION
            || resolution.request != question
            || resolution.resolved_at != terminal.created_at
            || resolution.terminal_envelope().as_ref() != Some(terminal)
        {
            return None;
        }
        Some(resolution)
    }

    /// Stable Child Inbox work admitted before clearing the pending question.
    /// The exact answer already lives in the paired canonical tool result;
    /// this instruction only wakes the Child after the parent reply wins.
    pub fn answer_wake_envelope(&self) -> Option<SessionMessageEnvelope> {
        let ParentQuestionOutcome::Answer { text } = &self.outcome else {
            return None;
        };
        let envelope = SessionMessageEnvelope {
            id: SessionMessageId::stable(
                "direct-parent-question-answer-wake-v1",
                &serde_json::json!(&self.request.id),
            ),
            source: SessionMessageSource::Runtime {
                subsystem: QUESTION_SUBSYSTEM.into(),
            },
            target_session_id: self.request.child.session_id.clone(),
            kind: SessionMessageKind::RuntimeInstruction,
            body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                instruction: "direct_parent_clarification_answer_v1".into(),
                content: Some(SessionMessageContent::text(
                    "The direct parent answered the pending clarification. Continue from the answered tool result.",
                )),
                data: Some(serde_json::json!({
                    "request_id": self.request.id,
                    "resolved_at": self.resolved_at,
                    "answer_digest": SessionMessageId::stable(
                        "direct-parent-question-answer-v1",
                        &serde_json::json!(text),
                    ),
                })),
                provider_message: None,
            }),
            created_at: self.resolved_at,
            thread_id: None,
            in_reply_to: Some(self.request.id.clone()),
            attempt: None,
            correlation_id: None,
        };
        (serde_json::to_vec(&envelope).ok()?.len() <= PARENT_QUESTION_MAX_BYTES).then_some(envelope)
    }

    /// A deadline terminal needs its own durable parent wake. The transcript
    /// terminal above is already recorded; this distinct id can be claimed
    /// without replaying that terminal as a second provider message.
    pub fn expiry_parent_wake_envelope(&self) -> Option<SessionMessageEnvelope> {
        if !matches!(self.outcome, ParentQuestionOutcome::Expired) {
            return None;
        }
        let envelope = SessionMessageEnvelope {
            id: SessionMessageId::stable(
                "direct-parent-question-expiry-wake-v1",
                &serde_json::json!(&self.request.id),
            ),
            source: SessionMessageSource::Runtime {
                subsystem: QUESTION_SUBSYSTEM.into(),
            },
            target_session_id: self.request.parent.session_id.clone(),
            kind: SessionMessageKind::RuntimeInstruction,
            body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                instruction: "direct_parent_clarification_expired_v1".into(),
                content: Some(SessionMessageContent::text(
                    "The Child clarification expired. The Child is blocked awaiting new input.",
                )),
                data: Some(serde_json::json!({
                    "request_id": self.request.id,
                    "resolved_at": self.resolved_at,
                })),
                provider_message: None,
            }),
            created_at: self.resolved_at,
            thread_id: None,
            in_reply_to: Some(self.request.id.clone()),
            attempt: None,
            correlation_id: None,
        };
        (serde_json::to_vec(&envelope).ok()?.len() <= PARENT_QUESTION_MAX_BYTES).then_some(envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{FunctionCall, Message, PendingQuestionSource, ToolCall};

    fn fixture() -> (Session, Session, ParentQuestion) {
        let parent = Session::new("parent", "model");
        let mut child = Session::new_child_of("child", &parent, "model", "child");
        child.messages.push(Message::assistant(
            "",
            Some(vec![ToolCall {
                id: "tool-call".into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "AskUserQuestion".into(),
                    arguments: "{}".into(),
                },
            }]),
        ));
        child.messages.push(Message::tool_result_with_status(
            "tool-call",
            "Clarification needed: Which option?",
            true,
        ));
        child.set_pending_question_with_source(
            "tool-call".into(),
            "AskUserQuestion".into(),
            "Which option?".into(),
            vec!["A".into(), "B".into()],
            true,
            PendingQuestionSource::DirectParent,
        );
        child.metadata.insert(
            "runtime.suspend_reason".into(),
            "awaiting_clarification".into(),
        );
        let question = ParentQuestion::issue_at(&parent, &child, Utc::now()).unwrap();
        child.metadata.insert(
            PARENT_QUESTION_REQUEST_KEY.into(),
            serde_json::to_string(&question).unwrap(),
        );
        (parent, child, question)
    }

    #[test]
    fn exact_child_pending_occurrence_reconstructs_stable_question() {
        let (parent, mut child, question) = fixture();
        let envelope = question.envelope();
        assert_eq!(
            ParentQuestion::from_envelope(&envelope),
            Some(question.clone())
        );
        assert_eq!(
            ParentQuestion::for_pending(&parent, &child),
            Some(question.clone())
        );
        assert!(question.validate_answer("A").is_ok());
        assert!(question.validate_answer("C").is_ok());
        let mut choice_only = question.clone();
        choice_only.allow_custom = false;
        assert!(choice_only.validate_answer("C").is_err());
        assert!(question.validate_answer("").is_err());
        assert!(question
            .validate_answer(&"x".repeat(PARENT_ANSWER_MAX_BYTES + 1))
            .is_err());
        child
            .messages
            .push(Message::tool_result_with_status("tool-call", "new", true));
        assert_ne!(ParentQuestion::for_pending(&parent, &child), Some(question));
    }

    #[test]
    fn choice_only_question_requires_offered_options_and_exact_answer() {
        let (parent, mut child, _) = fixture();
        child.pending_question.as_mut().unwrap().allow_custom = false;
        let question = ParentQuestion::issue_at(&parent, &child, Utc::now()).unwrap();
        child.metadata.insert(
            PARENT_QUESTION_REQUEST_KEY.into(),
            serde_json::to_string(&question).unwrap(),
        );
        assert_eq!(
            ParentQuestion::for_pending(&parent, &child),
            Some(question.clone())
        );
        assert_eq!(
            ParentQuestion::from_envelope(&question.envelope()),
            Some(question.clone())
        );
        assert!(question.validate_answer("A").is_ok());
        assert!(question.validate_answer("C").is_err());

        child.pending_question.as_mut().unwrap().options.clear();
        assert!(ParentQuestion::issue_at(&parent, &child, Utc::now()).is_none());
        child.pending_question.as_mut().unwrap().options =
            vec!["x".repeat(PARENT_ANSWER_MAX_BYTES + 1)];
        assert!(ParentQuestion::issue_at(&parent, &child, Utc::now()).is_none());
        child.pending_question.as_mut().unwrap().options = vec!["A\0".into()];
        assert!(ParentQuestion::issue_at(&parent, &child, Utc::now()).is_none());
        let mut invalid = question;
        invalid.options.clear();
        invalid.id = invalid.stable_id();
        assert!(ParentQuestion::from_envelope(&invalid.envelope()).is_none());
    }

    #[test]
    fn forged_envelope_and_non_parent_pending_are_rejected() {
        let (parent, mut child, question) = fixture();
        let mut envelope = question.envelope();
        envelope.target_session_id = "other".into();
        assert!(ParentQuestion::from_envelope(&envelope).is_none());
        child.pending_question.as_mut().unwrap().source = PendingQuestionSource::PauseTool;
        assert!(ParentQuestion::for_pending(&parent, &child).is_none());
    }

    #[test]
    fn full_width_ask_parent_question_survives_typed_envelope_encoding() {
        let (parent, mut child, _) = fixture();
        // Even control characters accepted by the compact facade can expand
        // sixfold when the question is serialized into its two envelope fields.
        for text in [
            "x".repeat(PARENT_QUESTION_TEXT_MAX_BYTES),
            "\u{0001}".repeat(PARENT_QUESTION_TEXT_MAX_BYTES),
        ] {
            child.messages.last_mut().unwrap().content = format!("Clarification needed: {text}");
            child.pending_question.as_mut().unwrap().question = text.clone();
            let issued = ParentQuestion::issue_at(&parent, &child, Utc::now())
                .expect("the accepted question width must fit its typed envelope");
            assert_eq!(
                ParentQuestion::from_envelope(&issued.envelope()),
                Some(issued)
            );
        }
        let too_long = "x".repeat(PARENT_QUESTION_TEXT_MAX_BYTES + 1);
        child.pending_question.as_mut().unwrap().question = too_long.clone();
        child.messages.last_mut().unwrap().content = format!("Clarification needed: {too_long}");
        assert!(ParentQuestion::issue_at(&parent, &child, Utc::now()).is_none());
    }

    #[test]
    fn maximally_escaped_question_and_answer_fit_typed_terminal() {
        let (parent, mut child, _) = fixture();
        let question_text = "\u{0001}".repeat(PARENT_QUESTION_TEXT_MAX_BYTES);
        child.messages.last_mut().unwrap().content =
            format!("Clarification needed: {question_text}");
        child.pending_question.as_mut().unwrap().question = question_text;
        let question = ParentQuestion::issue_at(&parent, &child, Utc::now()).unwrap();
        let answer = "\u{0002}".repeat(PARENT_ANSWER_MAX_BYTES);
        let resolution = ParentQuestionResolution::answered(
            &question,
            question.issued_at + chrono::Duration::milliseconds(1),
            &answer,
        )
        .unwrap();
        let terminal = resolution
            .terminal_envelope()
            .expect("every accepted question and answer must yield a terminal");
        let encoded = serde_json::to_vec(&terminal).unwrap();
        assert!(encoded.len() > 64 * 1024);
        assert!(encoded.len() <= PARENT_QUESTION_TERMINAL_MAX_BYTES);
        assert_eq!(
            ParentQuestionResolution::from_terminal(&question.envelope(), &terminal),
            Some(resolution)
        );
    }
}
