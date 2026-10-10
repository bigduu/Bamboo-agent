//! Typed, durable session-to-session delivery contract.
//!
//! A [`SessionMessageEnvelope`] belongs to Bamboo's delivery plane. It is
//! addressed by the stable logical [`Session::id`](super::Session), regardless
//! of which process, worker, or activation currently owns the session. The
//! envelope is translated into a provider-valid [`Message`] only when it is
//! admitted to reasoning context.

use std::collections::{HashSet, VecDeque};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{Message, MessagePart, Role, Session};

/// Serialize JSON with every object key sorted recursively.
///
/// Stable message ids and durable inbox idempotency receipts must use the same
/// representation: callers can construct semantically identical nested JSON
/// objects with different insertion orders (notably when serde_json's
/// `preserve_order` feature is enabled).
pub fn canonical_json_bytes(value: &serde_json::Value) -> Vec<u8> {
    fn canonicalize(value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.iter().map(canonicalize).collect())
            }
            serde_json::Value::Object(values) => {
                let mut entries = values.iter().collect::<Vec<_>>();
                entries.sort_unstable_by_key(|(key, _)| *key);
                let mut canonical = serde_json::Map::new();
                for (key, value) in entries {
                    canonical.insert(key.clone(), canonicalize(value));
                }
                serde_json::Value::Object(canonical)
            }
            scalar => scalar.clone(),
        }
    }

    // `serde_json::Value` serialization is infallible in practice. Keeping a
    // deterministic fallback makes the helper total and preserves the prior
    // stable-id contract if serde_json ever introduces a fallible value.
    serde_json::to_vec(&canonicalize(value)).unwrap_or_else(|_| b"null".to_vec())
}

/// Maximum number of recently admitted ids retained in the session checkpoint.
///
/// The transcript remains an additional, unbounded source of dedupe truth:
/// admitted messages use the envelope id as their stable [`Message::id`].
pub const SESSION_INBOX_ADMITTED_CAPACITY: usize = 4096;

/// Globally unique idempotency key supplied by the sender.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct SessionMessageId(String);

impl SessionMessageId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, SessionMessageValidationError> {
        let value = value.into();
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(SessionMessageValidationError::EmptyMessageId);
        }
        if value != trimmed {
            return Err(SessionMessageValidationError::NonCanonicalMessageId);
        }
        if value.len() > 256 {
            return Err(SessionMessageValidationError::MessageIdTooLong);
        }
        if value.contains('/') || value.contains('\\') || value.contains("..") {
            return Err(SessionMessageValidationError::UnsafeMessageId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Stable id for runtime events whose producer already has a durable
    /// semantic identity but not a UUID-form message id.
    pub fn stable(namespace: &str, value: &serde_json::Value) -> Self {
        let canonical = canonical_json_bytes(value);
        let mut digest = Sha256::new();
        digest.update((namespace.len() as u64).to_be_bytes());
        digest.update(namespace.as_bytes());
        digest.update((canonical.len() as u64).to_be_bytes());
        digest.update(canonical);
        Self(format!("stable-{}", hex::encode(digest.finalize())))
    }

    /// Stable id for the bounded compatibility migration from the legacy
    /// `pending_injected_messages` array.
    pub fn legacy(session_id: &str, index: usize, value: &serde_json::Value) -> Self {
        let stable = Self::stable(
            &format!("legacy_pending_injected_messages:{session_id}:{index}"),
            value,
        );
        Self(format!("legacy-{index:08}-{}", stable.0))
    }
}

impl<'de> Deserialize<'de> for SessionMessageId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

impl Default for SessionMessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SessionMessageId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Authenticated logical sender identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionMessageSource {
    User,
    Session { session_id: String },
    Runtime { subsystem: String },
}

/// Registered semantic kind. Adding a kind requires an explicit translation
/// policy in [`SessionMessageEnvelope::to_provider_message`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionMessageKind {
    UserInput,
    PeerMessage,
    ChildOutcome,
    RuntimeInstruction,
}

/// Bounded caller data only; this is never Skill invocation authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSkillRequest {
    pub selections: Vec<SessionSkillSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSkillSelection {
    pub id: String,
    pub source: String,
    pub revision: u64,
    #[serde(default)]
    pub args: serde_json::Value,
}

impl SessionSkillSelection {
    /// Check original borrowed fields before constructing or cloning a request.
    /// No catalog, policy, schema or current-input authority is established.
    pub fn validate_borrowed(
        id: &str,
        source: &str,
        revision: u64,
        args: &serde_json::Value,
    ) -> Result<(), SessionMessageValidationError> {
        if id.is_empty()
            || id.len() > 256
            || id.trim() != id
            || id.chars().any(char::is_control)
            || !matches!(
                source,
                "builtin" | "project" | "workspace" | "user" | "plugin"
            )
            || revision == 0
        {
            return Err(SessionMessageValidationError::KindSourceBodyMismatch);
        }
        validate_skill_args_shape(args)?;
        validate_compact_json_size(args, 8192)
    }
}

impl SessionSkillRequest {
    pub fn validate(&self) -> Result<(), SessionMessageValidationError> {
        let invalid = SessionMessageValidationError::KindSourceBodyMismatch;
        if self.selections.is_empty() || self.selections.len() > 32 {
            return Err(invalid);
        }
        if self.mode.as_ref().is_some_and(|mode| {
            mode.is_empty()
                || mode.len() > 64
                || !mode
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        }) {
            return Err(invalid);
        }
        let mut ids = HashSet::with_capacity(self.selections.len());
        for selection in &self.selections {
            SessionSkillSelection::validate_borrowed(
                &selection.id,
                &selection.source,
                selection.revision,
                &selection.args,
            )?;
            if !ids.insert(selection.id.as_str()) {
                return Err(invalid);
            }
        }
        validate_compact_json_size(self, 262144)
    }
}

// One borrowed iterator per current container path. Wide containers never
// allocate a child list, and excessive depth/nodes fail before serialization.
fn validate_skill_args_shape(
    args: &serde_json::Value,
) -> Result<(), SessionMessageValidationError> {
    enum Children<'a> {
        Array(std::slice::Iter<'a, serde_json::Value>),
        Object(serde_json::map::Values<'a>),
    }
    impl<'a> Children<'a> {
        fn next(&mut self) -> Option<&'a serde_json::Value> {
            match self {
                Self::Array(iter) => iter.next(),
                Self::Object(iter) => iter.next(),
            }
        }
    }
    let mut stack = Vec::with_capacity(64);
    let mut next = Some(args);
    let mut nodes = 0usize;
    loop {
        if let Some(value) = next.take() {
            nodes = nodes
                .checked_add(1)
                .filter(|count| *count <= 8192)
                .ok_or(SessionMessageValidationError::KindSourceBodyMismatch)?;
            let children = match value {
                serde_json::Value::Array(values) => Some(Children::Array(values.iter())),
                serde_json::Value::Object(values) => Some(Children::Object(values.values())),
                _ => None,
            };
            if let Some(children) = children {
                if stack.len() == 64 {
                    return Err(SessionMessageValidationError::KindSourceBodyMismatch);
                }
                stack.push(children);
            }
        } else if let Some(children) = stack.last_mut() {
            if let Some(value) = children.next() {
                next = Some(value);
            } else {
                stack.pop();
            }
        } else {
            return Ok(());
        }
    }
}

fn validate_compact_json_size(
    value: &impl Serialize,
    limit: usize,
) -> Result<(), SessionMessageValidationError> {
    struct Remaining(usize);
    impl std::io::Write for Remaining {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.checked_sub(bytes.len()).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Skill request byte limit")
            })?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Remaining(limit), value)
        .map_err(|_| SessionMessageValidationError::KindSourceBodyMismatch)
}

/// Provider-neutral text and multimodal content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionMessageContent {
    #[serde(default)]
    pub text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parts: Vec<MessagePart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_request: Option<SessionSkillRequest>,
}

impl SessionMessageContent {
    pub fn text(value: impl Into<String>) -> Self {
        Self {
            text: value.into(),
            parts: Vec::new(),
            skill_request: None,
        }
    }

    fn is_semantic(&self) -> bool {
        !self.text.trim().is_empty()
            || self.parts.iter().any(|part| match part {
                MessagePart::Text { text } => !text.trim().is_empty(),
                MessagePart::ImageUrl { image_url } => !image_url.url.trim().is_empty(),
            })
    }
}

/// Optional provider-facing presentation selected by a trusted runtime
/// coordinator.
///
/// The typed outcome/instruction remains the durable semantic payload. This
/// presentation preserves the exact text/multimodal shape and safe message
/// metadata that the pre-inbox path would have appended to the transcript.
/// `session_message` is reserved for the canonical envelope proof and is
/// rejected during validation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionProviderMessage {
    pub content: SessionMessageContent,
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub metadata: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub never_compress: bool,
}

/// Typed child terminal outcome carried independently of its rendered prompt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionChildOutcome {
    pub child_session_id: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_message: Option<SessionProviderMessage>,
}

/// Typed runtime instruction. `data` is namespaced by the registered
/// `instruction`; it is never treated as provider-facing role metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionRuntimeInstruction {
    pub instruction: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<SessionMessageContent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_message: Option<SessionProviderMessage>,
}

/// Semantic body; delivery metadata is deliberately not embedded here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionMessageBody {
    Content(SessionMessageContent),
    ChildOutcome(SessionChildOutcome),
    RuntimeInstruction(SessionRuntimeInstruction),
}

/// Stable, dependency-light envelope persisted in each logical session inbox.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionMessageEnvelope {
    pub id: SessionMessageId,
    pub source: SessionMessageSource,
    pub target_session_id: String,
    pub kind: SessionMessageKind,
    pub body: SessionMessageBody,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<SessionMessageId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
}

impl SessionMessageEnvelope {
    /// Carry a validated chat prompt through the existing typed instruction
    /// payload. Its User presentation and stable turn identity are retained;
    /// only the owned Root consumer applies the prompt at its checkpoint.
    pub fn with_root_chat_prompt(
        mut self,
        system_prompt: String,
    ) -> Result<Self, SessionMessageValidationError> {
        let SessionMessageBody::Content(content) = &self.body else {
            return Err(SessionMessageValidationError::KindSourceBodyMismatch);
        };
        if self.source != SessionMessageSource::User || self.kind != SessionMessageKind::UserInput {
            return Err(SessionMessageValidationError::KindSourceBodyMismatch);
        }
        if content.skill_request.is_some() {
            // New request data/placement must be bounded before either clone.
            self.validate()?;
        }
        let content = content.clone();
        self.source = SessionMessageSource::Runtime {
            subsystem: "chat".into(),
        };
        self.kind = SessionMessageKind::RuntimeInstruction;
        self.body = SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
            instruction: "root_chat_turn_v1".into(),
            content: Some(content.clone()),
            data: Some(serde_json::json!({ "system_prompt": system_prompt })),
            provider_message: Some(SessionProviderMessage {
                content,
                metadata: Default::default(),
                never_compress: false,
            }),
        });
        self.validate()?;
        Ok(self)
    }

    pub fn root_chat_prompt(&self) -> Result<Option<&str>, SessionMessageValidationError> {
        let SessionMessageBody::RuntimeInstruction(instruction) = &self.body else {
            return Ok(None);
        };
        if instruction.instruction != "root_chat_turn_v1" {
            return Ok(None);
        }
        if self.kind != SessionMessageKind::RuntimeInstruction
            || self.source
                != (SessionMessageSource::Runtime {
                    subsystem: "chat".into(),
                })
        {
            return Err(SessionMessageValidationError::KindSourceBodyMismatch);
        }
        instruction
            .data
            .as_ref()
            .and_then(|data| data.get("system_prompt"))
            .and_then(serde_json::Value::as_str)
            .filter(|prompt| !prompt.trim().is_empty())
            .map(Some)
            .ok_or(SessionMessageValidationError::EmptyRuntimeContent)
    }

    /// Reserved user-guidance correlation markers include a server-selected
    /// run fence. The fence controls scheduling, not the logical retry identity.
    pub fn is_guidance(&self) -> bool {
        self.source == SessionMessageSource::User
            && self.kind == SessionMessageKind::UserInput
            && self.correlation_id.as_deref().is_some_and(|value| {
                value == "session-guidance"
                    || value == "session-guidance-after-run"
                    || value.starts_with("session-guidance-after-run:")
            })
    }

    pub fn guidance_waits_for_run(&self, run_id: &str) -> bool {
        self.is_guidance()
            && self
                .correlation_id
                .as_deref()
                .and_then(|value| value.strip_prefix("session-guidance-after-run:"))
                == Some(run_id)
    }

    /// A child completion admitted while its parent Run still owns the turn
    /// belongs to that Run's successor. The coordinator writes this fence in
    /// the durable envelope before clearing the parent's wait.
    pub fn child_outcome_waits_for_run(&self, run_id: &str) -> bool {
        self.kind == SessionMessageKind::ChildOutcome
            && matches!(&self.source, SessionMessageSource::Runtime { subsystem }
                if subsystem == "child_completion_coordinator")
            && self
                .correlation_id
                .as_deref()
                .and_then(|value| value.strip_prefix("child_completion_after_run:"))
                == Some(run_id)
    }

    pub fn waits_for_successor_of_run(&self, run_id: &str) -> bool {
        self.guidance_waits_for_run(run_id) || self.child_outcome_waits_for_run(run_id)
    }

    pub fn user_input(target_session_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            id: SessionMessageId::new(),
            source: SessionMessageSource::User,
            target_session_id: target_session_id.into(),
            kind: SessionMessageKind::UserInput,
            body: SessionMessageBody::Content(SessionMessageContent::text(text)),
            created_at: Utc::now(),
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: None,
        }
    }

    fn validate_skill_requests(&self) -> Result<(), SessionMessageValidationError> {
        let invalid = SessionMessageValidationError::KindSourceBodyMismatch;
        // Root changes source/kind, so do not use the scheduling predicate here.
        let guidance_marker = self.correlation_id.as_deref().is_some_and(|value| {
            value == "session-guidance"
                || value == "session-guidance-after-run"
                || value.starts_with("session-guidance-after-run:")
        });
        match &self.body {
            SessionMessageBody::Content(content) => {
                if let Some(request) = &content.skill_request {
                    if self.source != SessionMessageSource::User
                        || self.kind != SessionMessageKind::UserInput
                        || guidance_marker
                    {
                        return Err(invalid);
                    }
                    request.validate()?;
                }
            }
            SessionMessageBody::ChildOutcome(outcome) => {
                if outcome
                    .provider_message
                    .as_ref()
                    .is_some_and(|message| message.content.skill_request.is_some())
                {
                    return Err(invalid);
                }
            }
            SessionMessageBody::RuntimeInstruction(instruction) => {
                let canonical = instruction
                    .content
                    .as_ref()
                    .and_then(|content| content.skill_request.as_ref());
                let presentation = instruction
                    .provider_message
                    .as_ref()
                    .and_then(|message| message.content.skill_request.as_ref());
                if canonical.is_none() && presentation.is_none() {
                    return Ok(());
                }
                if instruction.instruction != "root_chat_turn_v1"
                    || self.kind != SessionMessageKind::RuntimeInstruction
                    || self.source
                        != (SessionMessageSource::Runtime {
                            subsystem: "chat".into(),
                        })
                    || guidance_marker
                {
                    return Err(invalid);
                }
                let (Some(canonical), Some(presentation)) = (canonical, presentation) else {
                    return Err(invalid);
                };
                // Bound both trees before PartialEq can traverse their args.
                canonical.validate()?;
                presentation.validate()?;
                if canonical != presentation {
                    return Err(invalid);
                }
            }
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), SessionMessageValidationError> {
        SessionMessageId::parse(self.id.as_str())?;
        if let Some(in_reply_to) = &self.in_reply_to {
            SessionMessageId::parse(in_reply_to.as_str())
                .map_err(|_| SessionMessageValidationError::InvalidInReplyTo)?;
        }
        if self.target_session_id.trim().is_empty() {
            return Err(SessionMessageValidationError::EmptyTarget);
        }
        match (&self.source, self.kind, &self.body) {
            (
                SessionMessageSource::User,
                SessionMessageKind::UserInput,
                SessionMessageBody::Content(_),
            )
            | (
                SessionMessageSource::Session { .. },
                SessionMessageKind::PeerMessage,
                SessionMessageBody::Content(_),
            )
            | (
                SessionMessageSource::Session { .. } | SessionMessageSource::Runtime { .. },
                SessionMessageKind::ChildOutcome,
                SessionMessageBody::ChildOutcome(_),
            )
            | (
                SessionMessageSource::Runtime { .. },
                SessionMessageKind::RuntimeInstruction,
                SessionMessageBody::RuntimeInstruction(_),
            ) => {}
            _ => return Err(SessionMessageValidationError::KindSourceBodyMismatch),
        }
        match &self.source {
            SessionMessageSource::Session { session_id } if session_id.trim().is_empty() => {
                return Err(SessionMessageValidationError::EmptySource)
            }
            SessionMessageSource::Runtime { subsystem } if subsystem.trim().is_empty() => {
                return Err(SessionMessageValidationError::EmptySource)
            }
            _ => {}
        }
        match &self.body {
            SessionMessageBody::Content(content) if !content.is_semantic() => {
                return Err(SessionMessageValidationError::EmptyContent)
            }
            SessionMessageBody::ChildOutcome(outcome)
                if outcome.child_session_id.trim().is_empty() =>
            {
                return Err(SessionMessageValidationError::EmptyChildSessionId)
            }
            SessionMessageBody::ChildOutcome(outcome) if outcome.status.trim().is_empty() => {
                return Err(SessionMessageValidationError::EmptyChildStatus)
            }
            SessionMessageBody::ChildOutcome(SessionChildOutcome {
                provider_message: Some(message),
                ..
            })
            | SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                provider_message: Some(message),
                ..
            }) if !message.content.is_semantic() => {
                return Err(SessionMessageValidationError::EmptyProviderContent)
            }
            SessionMessageBody::ChildOutcome(SessionChildOutcome {
                provider_message: Some(message),
                ..
            })
            | SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                provider_message: Some(message),
                ..
            }) if message.metadata.contains_key("session_message") => {
                return Err(SessionMessageValidationError::ReservedProviderMetadata)
            }
            SessionMessageBody::RuntimeInstruction(instruction)
                if instruction.instruction.trim().is_empty() =>
            {
                return Err(SessionMessageValidationError::EmptyRuntimeInstruction)
            }
            SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                content: Some(content),
                ..
            }) if !content.is_semantic() => {
                return Err(SessionMessageValidationError::EmptyRuntimeContent)
            }
            _ => {}
        }
        self.validate_skill_requests()?;
        self.root_chat_prompt()?;
        Ok(())
    }

    /// Canonical fields that define one logical delivery for idempotency and
    /// transcript-proof purposes. Transport/retry metadata (`created_at` and
    /// `attempt`) is deliberately excluded. Provider presentation is derived
    /// by trusted runtime producers and is also excluded: the first durable
    /// envelope wins presentation for an otherwise identical typed outcome.
    pub fn idempotency_semantics(&self) -> serde_json::Value {
        let body = match &self.body {
            SessionMessageBody::Content(content) => serde_json::json!({
                "type": "content",
                "content": content,
            }),
            SessionMessageBody::ChildOutcome(outcome) => serde_json::json!({
                "type": "child_outcome",
                "child_session_id": outcome.child_session_id,
                "status": outcome.status,
                "result": outcome.result,
                "error": outcome.error,
            }),
            SessionMessageBody::RuntimeInstruction(instruction) => serde_json::json!({
                "type": "runtime_instruction",
                "instruction": instruction.instruction,
                "content": instruction.content,
                "data": instruction.data,
            }),
        };
        let correlation_id = if self.kind == SessionMessageKind::ChildOutcome
            && matches!(&self.source, SessionMessageSource::Runtime { subsystem }
                if subsystem == "child_completion_coordinator")
            && self.correlation_id.as_deref().is_some_and(|value| {
                value.starts_with("child_completion_after_run:")
                    || value.starts_with("child_completion:")
            }) {
            // The first admitted envelope owns the exact run fence across
            // duplicate completion callbacks and restarts.
            Some("child_completion")
        } else if self.is_guidance()
            && self
                .correlation_id
                .as_deref()
                .is_some_and(|value| value.starts_with("session-guidance-after-run:"))
        {
            Some("session-guidance-after-run")
        } else {
            self.correlation_id.as_deref()
        };
        serde_json::json!({
            "source": &self.source,
            "target_session_id": &self.target_session_id,
            "kind": &self.kind,
            "body": body,
            "thread_id": &self.thread_id,
            "in_reply_to": &self.in_reply_to,
            "correlation_id": correlation_id,
        })
    }

    /// Translate the delivery envelope to a replayable provider-valid user
    /// message while retaining all correlation metadata.
    pub fn to_provider_message(&self) -> Result<Message, SessionMessageValidationError> {
        self.validate()?;
        let (text, parts, provider_metadata, never_compress) = match &self.body {
            SessionMessageBody::Content(content) => (
                content.text.clone(),
                content.parts.clone(),
                serde_json::Map::new(),
                false,
            ),
            SessionMessageBody::ChildOutcome(outcome) => {
                if let Some(provider_message) = &outcome.provider_message {
                    (
                        provider_message.content.text.clone(),
                        provider_message.content.parts.clone(),
                        provider_message.metadata.clone(),
                        provider_message.never_compress,
                    )
                } else {
                    let mut text = format!(
                        "Child session `{}` finished with status `{}`.",
                        outcome.child_session_id, outcome.status
                    );
                    if let Some(result) = outcome.result.as_deref().filter(|v| !v.trim().is_empty())
                    {
                        text.push_str("\n\nResult:\n");
                        text.push_str(result);
                    }
                    if let Some(error) = outcome.error.as_deref().filter(|v| !v.trim().is_empty()) {
                        text.push_str("\n\nError:\n");
                        text.push_str(error);
                    }
                    (text, Vec::new(), serde_json::Map::new(), false)
                }
            }
            SessionMessageBody::RuntimeInstruction(instruction) => {
                if let Some(provider_message) = &instruction.provider_message {
                    (
                        provider_message.content.text.clone(),
                        provider_message.content.parts.clone(),
                        provider_message.metadata.clone(),
                        provider_message.never_compress,
                    )
                } else {
                    let text = instruction
                        .content
                        .as_ref()
                        .map(|content| content.text.clone())
                        .unwrap_or_else(|| instruction.instruction.clone());
                    let parts = instruction
                        .content
                        .as_ref()
                        .map(|content| content.parts.clone())
                        .unwrap_or_default();
                    (text, parts, serde_json::Map::new(), false)
                }
            }
        };

        let mut message = if parts.is_empty() {
            Message::user(text)
        } else {
            Message::user_with_parts(text, parts)
        };
        message.id = self.id.to_string();
        message.created_at = self.created_at;
        message.never_compress = never_compress;
        let mut metadata = provider_metadata;
        metadata.insert(
            "session_message".to_string(),
            serde_json::json!({
                "id": self.id,
                "idempotency_semantics": self.idempotency_semantics(),
                "source": self.source,
                "target_session_id": self.target_session_id,
                "kind": self.kind,
                "body": self.body,
                "created_at": self.created_at,
                "thread_id": self.thread_id,
                "in_reply_to": self.in_reply_to,
                "attempt": self.attempt,
                "correlation_id": self.correlation_id,
            }),
        );
        message.metadata = Some(serde_json::Value::Object(metadata));
        Ok(message)
    }
}

/// Unbounded durable dedupe proof carried by the transcript itself.
///
/// The bounded admission cursor may legitimately evict old ids. A permanent
/// inbox receipt can therefore be reconciled only against this typed transcript
/// marker carrying the full idempotency-defining envelope semantics, not
/// against cursor membership alone.
pub fn is_matching_session_message(message: &Message, envelope: &SessionMessageEnvelope) -> bool {
    let Ok(expected) = envelope.to_provider_message() else {
        return false;
    };
    let provider_metadata_matches = || {
        let mut actual = message.metadata.as_ref()?.as_object()?.clone();
        let mut wanted = expected.metadata.as_ref()?.as_object()?.clone();
        actual.remove("session_message");
        wanted.remove("session_message");
        Some(actual == wanted)
    };
    let canonical_marker_matches = || {
        Some(
            message.metadata.as_ref()?.get("session_message")?
                == expected.metadata.as_ref()?.get("session_message")?,
        )
    };
    message.role == Role::User
        && message.id == envelope.id.as_str()
        && message.content == expected.content
        && message.content_parts == expected.content_parts
        && message.never_compress == expected.never_compress
        && provider_metadata_matches() == Some(true)
        && canonical_marker_matches() == Some(true)
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionMessageValidationError {
    #[error("session message id must not be empty")]
    EmptyMessageId,
    #[error("session message id must not contain leading or trailing whitespace")]
    NonCanonicalMessageId,
    #[error("session message id exceeds 256 bytes")]
    MessageIdTooLong,
    #[error("session message id contains an unsafe path component")]
    UnsafeMessageId,
    #[error("target session id must not be empty")]
    EmptyTarget,
    #[error("source identity must not be empty")]
    EmptySource,
    #[error("message content must contain non-empty text or at least one part")]
    EmptyContent,
    #[error("child outcome session id must not be empty")]
    EmptyChildSessionId,
    #[error("child outcome status must not be empty")]
    EmptyChildStatus,
    #[error("runtime instruction name must not be empty")]
    EmptyRuntimeInstruction,
    #[error("runtime instruction content must contain non-empty text or at least one part")]
    EmptyRuntimeContent,
    #[error("provider presentation must contain non-empty text or at least one part")]
    EmptyProviderContent,
    #[error("provider presentation metadata uses the reserved session_message key")]
    ReservedProviderMetadata,
    #[error("in_reply_to must be a valid session message id")]
    InvalidInReplyTo,
    #[error("session message kind, source, and body are incompatible")]
    KindSourceBodyMismatch,
}

/// One durably admitted id and its mailbox sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmittedSessionMessage {
    pub id: SessionMessageId,
    pub sequence: u64,
}

/// Bounded durable cursor stored with the transcript checkpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionInboxAdmissionState {
    #[serde(default)]
    admitted: VecDeque<AdmittedSessionMessage>,
    #[serde(skip)]
    index: HashSet<SessionMessageId>,
    #[serde(default)]
    pub last_admitted_sequence: u64,
    /// Highest durable inbox generation observed by this execution while
    /// migrating compatibility ingress. This may be ahead of
    /// `last_admitted_sequence` when the transcript checkpoint failed; outer
    /// execution finalization uses that gap to hand work to one successor.
    #[serde(default)]
    pub last_observed_sequence: u64,
}

impl SessionInboxAdmissionState {
    pub fn contains(&self, id: &SessionMessageId) -> bool {
        self.index.contains(id) || self.admitted.iter().any(|entry| &entry.id == id)
    }

    pub fn contains_str(&self, id: &str) -> bool {
        self.admitted.iter().any(|entry| entry.id.as_str() == id)
    }

    pub fn record(&mut self, id: SessionMessageId, sequence: u64) -> bool {
        self.last_observed_sequence = self.last_observed_sequence.max(sequence);
        if self.contains(&id) {
            self.last_admitted_sequence = self.last_admitted_sequence.max(sequence);
            return false;
        }
        self.index.insert(id.clone());
        self.admitted
            .push_back(AdmittedSessionMessage { id, sequence });
        self.last_admitted_sequence = self.last_admitted_sequence.max(sequence);
        while self.admitted.len() > SESSION_INBOX_ADMITTED_CAPACITY {
            if let Some(evicted) = self.admitted.pop_front() {
                self.index.remove(&evicted.id);
            }
        }
        true
    }

    pub fn rebuild_index(&mut self) {
        self.index = self.admitted.iter().map(|entry| entry.id.clone()).collect();
    }

    pub fn merge_from(&mut self, other: &Self) {
        for entry in &other.admitted {
            self.record(entry.id.clone(), entry.sequence);
        }
        self.last_admitted_sequence = self
            .last_admitted_sequence
            .max(other.last_admitted_sequence);
        self.last_observed_sequence = self
            .last_observed_sequence
            .max(other.last_observed_sequence);
    }

    /// Record a durable generation before transcript admission. Unlike
    /// [`record`](Self::record), this does not claim that provider reasoning
    /// has seen the message.
    pub fn observe(&mut self, sequence: u64) {
        self.last_observed_sequence = self.last_observed_sequence.max(sequence);
    }

    /// Newest durable generation that this execution observed but has not
    /// durably admitted into the transcript yet.
    pub fn pending_activation_generation(&self) -> Option<u64> {
        (self.last_observed_sequence > self.last_admitted_sequence)
            .then_some(self.last_observed_sequence)
    }

    pub fn len(&self) -> usize {
        self.admitted.len()
    }

    pub fn is_empty(&self) -> bool {
        self.admitted.is_empty()
    }
}

/// Durable enqueue receipt. `generation` is a per-session, monotonic delivery
/// sequence used by the activation finalization handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionInboxReceipt {
    pub id: SessionMessageId,
    pub generation: u64,
}

/// How an authorized inbox prefix interacts with a durable orchestration wait.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionActivationPolicy {
    /// Child/Bash coordinators authorize execution only after their own wait
    /// transition is durably satisfied. A crash before that transition must
    /// leave the marked backlog inert.
    #[default]
    RespectSpecificWait,
    /// Explicit user/peer/runtime steering is allowed to interrupt a child or
    /// Bash wait. The real spawner applies that interruption to the latest
    /// locked control-plane snapshot before reserving a runner.
    InterruptSpecificWait,
}

/// Permission to activate this specific message, published with its delivery.
/// This never releases a coordinator's staged queue prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionInboxActivationIntent {
    version: u32,
    policy: SessionActivationPolicy,
}

impl SessionInboxActivationIntent {
    pub fn new(policy: SessionActivationPolicy) -> Self {
        Self { version: 1, policy }
    }

    pub fn policy(&self) -> Result<SessionActivationPolicy, SessionInboxError> {
        if self.version != 1 {
            return Err(SessionInboxError::InvalidClaim(
                "unsupported SessionInbox activation intent version".into(),
            ));
        }
        Ok(self.policy)
    }
}

/// Opaque claim returned to the single consumer.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionInboxClaim {
    pub envelope: SessionMessageEnvelope,
    pub generation: u64,
    /// This item's effective permission at the claim boundary. Aggregate
    /// backlog generations describe wakeups, never another item's policy.
    pub activation_policy: SessionActivationPolicy,
    pub claim_id: String,
}

/// Caller-owned identity for the opt-in storage lease protocol. Runtime
/// consumers must supply a fresh identity for each independent consumer.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionInboxConsumerId(String);

impl SessionInboxConsumerId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SessionInboxConsumerId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for SessionInboxConsumerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SessionInboxConsumerId(<opaque>)")
    }
}

/// Durable storage authority, independent of transcript/provider authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionInboxLeaseToken {
    pub consumer: SessionInboxConsumerId,
    pub epoch: u64,
    pub expires_at: DateTime<Utc>,
    pub incarnation: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionInboxOwnedClaim {
    pub claim: SessionInboxClaim,
    pub lease: SessionInboxLeaseToken,
}

/// The caller supplies a trusted clock; no background expiry driver is implied.
#[derive(Debug, Clone)]
pub struct SessionInboxLeaseRequest {
    pub consumer: SessionInboxConsumerId,
    pub now: DateTime<Utc>,
    pub duration: chrono::Duration,
}

impl SessionInboxLeaseRequest {
    pub fn expires_at(&self) -> Result<DateTime<Utc>, SessionInboxError> {
        if self.consumer.as_str().is_empty()
            || self.consumer.as_str().len() > 128
            || self.duration <= chrono::Duration::zero()
            || self.duration > chrono::Duration::hours(1)
        {
            return Err(SessionInboxError::InvalidClaim(
                "invalid Inbox lease request".into(),
            ));
        }
        self.now
            .checked_add_signed(self.duration)
            .ok_or_else(|| SessionInboxError::InvalidClaim("Inbox lease expiry overflow".into()))
    }
}

/// Bounded operational evidence, deliberately excluding identity and payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionInboxLeaseInspection {
    pub generation: u64,
    pub epoch: u64,
    pub expires_at: DateTime<Utc>,
    pub expired: bool,
    pub reclaim_count: u64,
    pub manual_retry_count: u64,
    /// Explicit consumer failures only; lease expiry is not a failure.
    pub failure_count: u32,
    pub last_error_code: Option<String>,
    pub retry_after: Option<DateTime<Utc>>,
}

/// One exact, currently claimable Inbox item. This is a wake hint, never a
/// claim or execution grant: the consumer must still pass the storage CAS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionInboxWakeCandidate {
    pub id: SessionMessageId,
    pub generation: u64,
    pub activation_policy: SessionActivationPolicy,
    pub coordinator_generation: u64,
    pub lease_epoch: Option<u64>,
    /// Durable lease expiry, retry deadline, or manual retry time that made
    /// this incarnation claimable. None means an unleased delivery.
    pub ready_after: Option<DateTime<Utc>>,
}

/// Computed under the same queue lock as `claim_owned`. Staged and dead-letter
/// items are omitted; `next_due_at` is the earliest eligible lease transition
/// skipped during the ordered scan, including when a later item is ready now.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionInboxWakeReadiness {
    pub ready: Option<SessionInboxWakeCandidate>,
    pub next_due_at: Option<DateTime<Utc>>,
}

/// A bounded, non-sensitive diagnostic code supplied by the current owner.
/// Free-form exception text, payloads and credentials must not be persisted.
#[derive(Debug, Clone)]
pub struct SessionInboxFailureReport {
    pub now: DateTime<Utc>,
    pub error_code: String,
}

impl SessionInboxFailureReport {
    pub fn validate(&self) -> Result<(), SessionInboxError> {
        if self.error_code.is_empty()
            || self.error_code.len() > 64
            || !self.error_code.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'_' | b'-' | b'.')
            })
        {
            return Err(SessionInboxError::InvalidClaim(
                "Inbox failure requires a bounded diagnostic code".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionInboxFailureOutcome {
    RetryScheduled {
        failure_count: u32,
        retry_after: DateTime<Utc>,
    },
    DeadLettered {
        failure_count: u32,
    },
}

/// Host management identity for one exact Session lifetime. A gateway must
/// prove local bypass or authenticated host credentials and authorization for
/// this Session before constructing the principal from a request.
#[derive(Clone)]
pub struct SessionInboxAdministrationPrincipal {
    target_session_id: String,
    target_created_at: DateTime<Utc>,
}

impl SessionInboxAdministrationPrincipal {
    pub fn authenticated_host_owner_for(session: &Session) -> Self {
        Self {
            target_session_id: session.id.clone(),
            target_created_at: session.created_at,
        }
    }

    pub fn authorizes(&self, session: &Session) -> bool {
        self.target_session_id == session.id && self.target_created_at == session.created_at
    }
}

/// Bounded dead-letter evidence. It intentionally excludes message payloads
/// and lease consumer identities, but retains exact retry selectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionInboxDeadLetterInspection {
    pub id: SessionMessageId,
    pub generation: u64,
    pub failure_count: u32,
    pub last_error_code: String,
    pub dead_lettered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionInboxBacklog {
    pub pending: usize,
    pub claimed: usize,
    pub generation: u64,
    /// Highest inbox generation with durable execution permission, either from
    /// its own immediate intent or the coordinator's authorized prefix.
    ///
    /// Admission alone is intentionally insufficient: child/Bash coordinators
    /// can stage several sibling outcomes while a durable wait remains armed,
    /// then authorize the accumulated prefix only after the wait policy is
    /// satisfied.
    pub activation_generation: u64,
    /// The coordinator's separately authorized prefix. Unlike per-message
    /// immediate intent, this advances only after the coordinator releases
    /// staged outcomes and lets activation recover a newly released older item.
    pub coordinator_generation: u64,
    /// Highest pending, authorized generation carrying an explicit steering
    /// policy that may interrupt a specific child/Bash wait.
    pub interrupt_generation: u64,
    /// Oldest generation still present in `new/` or `cur/`.
    pub oldest_generation: Option<u64>,
}

impl SessionInboxBacklog {
    /// True only when a durable queue item has permission from its immediate
    /// intent or the coordinator prefix. Inspection computes the highest
    /// eligible generation; it never grants intervening staged siblings.
    pub fn activation_pending(&self) -> bool {
        self.oldest_generation
            .is_some_and(|oldest| oldest <= self.activation_generation)
    }

    pub fn interrupt_pending(&self) -> bool {
        self.oldest_generation
            .is_some_and(|oldest| oldest <= self.interrupt_generation)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionInboxLimits {
    pub max_payload_bytes: usize,
    pub max_backlog: usize,
    pub max_claim_batch: usize,
}

impl Default for SessionInboxLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: 256 * 1024,
            max_backlog: 1024,
            max_claim_batch: 128,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SessionInboxError {
    #[error("session message admission was cancelled before the durable commit")]
    AdmissionCancelled,
    #[error("session inbox target not found: {0}")]
    TargetNotFound(String),
    #[error("session inbox payload is {actual} bytes, limit is {limit}")]
    PayloadTooLarge { actual: usize, limit: usize },
    #[error("session inbox backlog is full ({current}/{limit})")]
    BacklogFull { current: usize, limit: usize },
    #[error("session inbox claim is invalid: {0}")]
    InvalidClaim(String),
    #[error("session inbox storage failure: {0}")]
    Storage(String),
}

/// Durable delivery/claim/ack port. Implementations are scoped to one runtime,
/// but every address is a stable logical session id.
#[async_trait]
pub trait SessionInboxPort: Send + Sync {
    /// Host-only access to the concrete durable Inbox when an Actor-fenced
    /// transcript seed must hold the same process lock as claim and ACK.
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }

    /// Irreversibly opt this queue into owned claims. Legacy claim/ACK APIs
    /// must fail closed afterwards. This does not enable production expiry.
    async fn claim_owned(
        &self,
        _target_session_id: &str,
        _limit: usize,
        _active_run_id: Option<&str>,
        _request: &SessionInboxLeaseRequest,
    ) -> Result<Vec<SessionInboxOwnedClaim>, SessionInboxError> {
        Err(SessionInboxError::InvalidClaim(
            "owned Inbox leases unsupported".into(),
        ))
    }

    async fn renew_owned(
        &self,
        _target_session_id: &str,
        _claim: &SessionInboxOwnedClaim,
        _request: &SessionInboxLeaseRequest,
    ) -> Result<SessionInboxOwnedClaim, SessionInboxError> {
        Err(SessionInboxError::InvalidClaim(
            "owned Inbox leases unsupported".into(),
        ))
    }

    /// Requires the caller's durable transcript checkpoint, just like `ack`.
    /// Terminal retries require the exact lease identity; expiry cannot undo ACK.
    async fn ack_owned(
        &self,
        _target_session_id: &str,
        _claim: &SessionInboxOwnedClaim,
        _now: DateTime<Utc>,
    ) -> Result<(), SessionInboxError> {
        Err(SessionInboxError::InvalidClaim(
            "owned Inbox leases unsupported".into(),
        ))
    }

    async fn inspect_owned_leases(
        &self,
        _target_session_id: &str,
        _limit: usize,
        _now: DateTime<Utc>,
    ) -> Result<Vec<SessionInboxLeaseInspection>, SessionInboxError> {
        Err(SessionInboxError::InvalidClaim(
            "owned Inbox leases unsupported".into(),
        ))
    }

    /// Release only this exact live claim for recovery. Preserve its typed body
    /// and counters; an expired or replaced token cannot release a successor.
    async fn release_owned(
        &self,
        _target_session_id: &str,
        _claim: &SessionInboxOwnedClaim,
    ) -> Result<(), SessionInboxError> {
        Err(SessionInboxError::InvalidClaim(
            "owned Inbox release unsupported".into(),
        ))
    }

    /// Read exact wake readiness from the canonical queue and lease state.
    /// Backends without a joint lock and exact eligibility check fail closed.
    async fn inspect_wake_readiness(
        &self,
        _target_session_id: &str,
        _now: DateTime<Utc>,
    ) -> Result<SessionInboxWakeReadiness, SessionInboxError> {
        Err(SessionInboxError::InvalidClaim(
            "exact Inbox wake inspection unsupported".into(),
        ))
    }

    /// Only an exact, live owner may report a processing failure. An explicit
    /// report is distinct from expiry/reclaim and can schedule bounded retry.
    async fn fail_owned(
        &self,
        _target_session_id: &str,
        _claim: &SessionInboxOwnedClaim,
        _report: &SessionInboxFailureReport,
    ) -> Result<SessionInboxFailureOutcome, SessionInboxError> {
        Err(SessionInboxError::InvalidClaim(
            "owned Inbox failure reporting unsupported".into(),
        ))
    }

    async fn inspect_dead_letters(
        &self,
        _target_session_id: &str,
        _limit: usize,
        _principal: &SessionInboxAdministrationPrincipal,
    ) -> Result<Vec<SessionInboxDeadLetterInspection>, SessionInboxError> {
        Err(SessionInboxError::InvalidClaim(
            "Inbox dead-letter inspection unsupported".into(),
        ))
    }

    /// The trusted host must authorize this exact target, message and
    /// generation before requesting a manual retry.
    async fn retry_dead_letter(
        &self,
        _target_session_id: &str,
        _id: &SessionMessageId,
        _generation: u64,
        _now: DateTime<Utc>,
        _principal: &SessionInboxAdministrationPrincipal,
    ) -> Result<SessionInboxReceipt, SessionInboxError> {
        Err(SessionInboxError::InvalidClaim(
            "Inbox dead-letter retry unsupported".into(),
        ))
    }

    async fn deliver(
        &self,
        envelope: &SessionMessageEnvelope,
    ) -> Result<SessionInboxReceipt, SessionInboxError>;

    /// Check the caller's cancellation gate at the durable inbox publication
    /// point. Backends without an integrated commit fence fail closed.
    async fn deliver_with_gate(
        &self,
        _envelope: &SessionMessageEnvelope,
        _gate: &super::AdmissionGate,
    ) -> Result<SessionInboxReceipt, SessionInboxError> {
        Err(SessionInboxError::Storage(
            "cancellation-aware SessionInbox admission is unsupported".into(),
        ))
    }

    /// Publish permission for this message in the same commit as its delivery.
    /// Implementations must bind the intent to exact-id retry semantics and
    /// retain the cancellation gate at that publication boundary. It does not
    /// authorize an earlier staged sibling. Unsupported backends fail closed.
    async fn deliver_with_activation_intent(
        &self,
        _envelope: &SessionMessageEnvelope,
        _policy: SessionActivationPolicy,
        _gate: Option<&super::AdmissionGate>,
    ) -> Result<SessionInboxReceipt, SessionInboxError> {
        Err(SessionInboxError::Storage(
            "immediate SessionInbox activation intent is unsupported".into(),
        ))
    }

    /// Admit one typed Supervisor peer message while retaining canonical
    /// incarnation, relationship, Project and target lifetime authority locks
    /// through the inbox receipt. An earlier link observation is not a grant.
    /// Backends without this integrated boundary must fail closed.
    async fn deliver_supervisor_followup(
        &self,
        _supervisor: &super::SupervisorReference,
        _envelope: &SessionMessageEnvelope,
    ) -> Result<SessionInboxReceipt, SessionInboxError> {
        Err(SessionInboxError::Storage(
            "Supervisor followup admission is unsupported".into(),
        ))
    }

    /// Durably authorize execution for the queue prefix through `generation`.
    ///
    /// This is separate from [`deliver`](Self::deliver) so orchestration
    /// producers can stage outcomes without violating a persisted wait policy.
    /// Implementations must make this monotonic and idempotent.
    async fn mark_activation_eligible(
        &self,
        target_session_id: &str,
        generation: u64,
        policy: SessionActivationPolicy,
    ) -> Result<(), SessionInboxError>;

    /// Read the coordinator prefix independently from immediate per-message
    /// permission. Legacy backends have no immediate intent and need no hole
    /// recovery. Backends supporting immediate intent must expose their true
    /// coordinator prefix here. This value alone is never pending-work proof.
    async fn coordinator_activation_generation(
        &self,
        _target_session_id: &str,
    ) -> Result<u64, SessionInboxError> {
        Ok(0)
    }

    async fn claim(
        &self,
        target_session_id: &str,
        limit: usize,
    ) -> Result<Vec<SessionInboxClaim>, SessionInboxError>;

    /// Claim at a model boundary while leaving end-of-run guidance delivered
    /// during this run pending for the successor owner.
    async fn claim_for_turn(
        &self,
        target_session_id: &str,
        limit: usize,
        active_run_id: Option<&str>,
    ) -> Result<Vec<SessionInboxClaim>, SessionInboxError> {
        let claims = self.claim(target_session_id, limit).await?;
        Ok(claims
            .into_iter()
            .filter(|claim| {
                !active_run_id
                    .is_some_and(|run_id| claim.envelope.waits_for_successor_of_run(run_id))
            })
            .collect())
    }

    /// Permanent durable receipt check. Unlike the bounded in-session cursor,
    /// this tombstone must remain true for the lifetime of the logical inbox.
    async fn was_admitted(
        &self,
        target_session_id: &str,
        id: &SessionMessageId,
    ) -> Result<bool, SessionInboxError>;

    /// Persist the permanent admitted-id receipt, then remove the claimed
    /// queue item. Callers may invoke this only after the transcript checkpoint
    /// containing the same id is durable.
    async fn ack(
        &self,
        target_session_id: &str,
        claim: &SessionInboxClaim,
    ) -> Result<(), SessionInboxError>;

    /// List unclaimed user guidance in delivery order.
    async fn pending_guidance(
        &self,
        _target_session_id: &str,
    ) -> Result<Vec<SessionMessageEnvelope>, SessionInboxError> {
        Err(SessionInboxError::Storage(
            "guidance queue is unavailable".into(),
        ))
    }

    /// Cancel only an unclaimed user envelope. False means it cannot be withdrawn.
    async fn cancel_guidance(
        &self,
        _target_session_id: &str,
        _id: &SessionMessageId,
    ) -> Result<bool, SessionInboxError> {
        Err(SessionInboxError::Storage(
            "guidance cancellation is unavailable".into(),
        ))
    }

    async fn inspect(
        &self,
        target_session_id: &str,
    ) -> Result<SessionInboxBacklog, SessionInboxError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionActivationDisposition {
    ActiveNotified,
    ActivationReserved,
    ActivationCoalesced,
}

#[derive(Debug, thiserror::Error)]
pub enum SessionActivationError {
    #[error("session activation target not found: {0}")]
    TargetNotFound(String),
    #[error("session activation failed: {0}")]
    Internal(String),
}

/// Runtime-scoped activation port. The delivery service never starts a second
/// loop directly; it notifies the owner or reserves one successor activation.
#[async_trait]
pub trait SessionActivationPort: Send + Sync {
    async fn request_activation(
        &self,
        target_session_id: &str,
        inbox_generation: u64,
    ) -> Result<SessionActivationDisposition, SessionActivationError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skill_request_fixture() -> SessionSkillRequest {
        SessionSkillRequest {
            selections: vec![SessionSkillSelection {
                id: "review".into(),
                source: "builtin".into(),
                revision: 7,
                args: serde_json::json!({"query":"原样","nested":{"a":1,"b":2}}),
            }],
            mode: None,
        }
    }

    fn skill_request_envelope() -> SessionMessageEnvelope {
        let mut envelope = SessionMessageEnvelope::user_input("skill-target", "ordinary User");
        if let SessionMessageBody::Content(content) = &mut envelope.body {
            content.skill_request = Some(skill_request_fixture());
        }
        envelope
    }

    fn skill_args_at_depth(depth: usize) -> serde_json::Value {
        (0..depth).fold(serde_json::Value::Null, |value, index| {
            if index % 2 == 0 {
                serde_json::json!([value])
            } else {
                serde_json::json!({"value":value})
            }
        })
    }

    #[test]
    fn skill_request_roundtrip_preserves_data_without_changing_provider_text() {
        let envelope = skill_request_envelope();
        envelope.validate().unwrap();
        let serialized = serde_json::to_vec(&envelope).unwrap();
        let restored: SessionMessageEnvelope = serde_json::from_slice(&serialized).unwrap();
        assert_eq!(restored, envelope);
        assert_eq!(restored.clone(), envelope);
        let provider = envelope.to_provider_message().unwrap();
        assert_eq!(provider.role, Role::User);
        assert_eq!(provider.content, "ordinary User");
        assert_eq!(provider.content_parts, None);
        assert_eq!(provider.id, envelope.id.to_string());
        assert_eq!(provider.created_at, envelope.created_at);
        assert!(is_matching_session_message(&provider, &envelope));
        let data = &provider.metadata.as_ref().unwrap()["session_message"]["body"]["skill_request"];
        assert_eq!(
            data,
            &serde_json::to_value(skill_request_fixture()).unwrap()
        );
        assert!(data.get("mode").is_none());
        let default_args: SessionSkillSelection =
            serde_json::from_str(r#"{"id":"review","source":"builtin","revision":7}"#).unwrap();
        assert_eq!(default_args.args, serde_json::Value::Null);
        SessionSkillSelection::validate_borrowed(
            &default_args.id,
            &default_args.source,
            default_args.revision,
            &default_args.args,
        )
        .unwrap();
        for unknown in [
            serde_json::json!({"selections":[],"authority":true}),
            serde_json::json!({"selections":[{"id":"review","source":"builtin","revision":7,"grant":true}]}),
        ] {
            assert!(serde_json::from_value::<SessionSkillRequest>(unknown).is_err());
        }
        let mut empty = envelope;
        if let SessionMessageBody::Content(content) = &mut empty.body {
            content.text.clear();
        }
        assert_eq!(
            empty.validate(),
            Err(SessionMessageValidationError::EmptyContent)
        );
    }

    #[test]
    fn skill_request_scalar_bounds_reject_without_normalizing_or_granting() {
        let mut request = skill_request_fixture();
        for source in ["builtin", "project", "workspace", "user", "plugin"] {
            request.selections[0].source = source.into();
            request.validate().unwrap();
        }
        for id in ["", " ", " review", "review ", "a\nb", "a\0b", "a\u{7f}b"] {
            request.selections[0].id = id.into();
            assert!(request.validate().is_err(), "id={id:?}");
        }
        request.selections[0].id = "界".repeat(85) + "x";
        assert_eq!(request.selections[0].id.len(), 256);
        request.selections[0].revision = u64::MAX;
        request.validate().unwrap();
        request.selections[0].id.push('x');
        assert!(request.validate().is_err());
        request.selections[0].id = "exact Case with space".into();
        request.validate().unwrap();
        request.selections[0].revision = 0;
        assert!(request.validate().is_err());
        request.selections[0].revision = 1;
        for source in ["global", "Builtin", "builtin ", "", "agents"] {
            request.selections[0].source = source.into();
            assert!(request.validate().is_err(), "source={source}");
        }
        request.selections[0].source = "builtin".into();
        for mode in ["plan", "a-0", "-"] {
            request.mode = Some(mode.into());
            request.validate().unwrap();
        }
        request.mode = Some("a".repeat(64));
        request.validate().unwrap();
        for mode in ["", "Plan", "a b", "é", "a_1", "a\n", &"a".repeat(65)] {
            request.mode = Some(mode.into());
            assert!(request.validate().is_err(), "mode={mode:?}");
        }
        request.mode = None;
        let mut duplicate = request.selections[0].clone();
        duplicate.source = "project".into();
        request.selections.push(duplicate);
        assert!(request.validate().is_err(), "IDs unique across sources");
        request.selections.clear();
        assert!(request.validate().is_err());
        request.selections = (0..32)
            .map(|n| SessionSkillSelection {
                id: format!("skill-{n}"),
                source: "user".into(),
                revision: 1,
                args: serde_json::Value::Null,
            })
            .collect();
        request.validate().unwrap();
        request.selections.push(request.selections[0].clone());
        assert!(request.validate().is_err(), "selection count exceeds 32");
    }

    #[test]
    fn skill_request_bounds_count_container_paths_nodes_and_actual_escaped_bytes() {
        for depth in [0, 1, 64] {
            let args = skill_args_at_depth(depth);
            SessionSkillSelection::validate_borrowed("review", "builtin", 1, &args).unwrap();
        }
        for depth in [65, 180] {
            let args = skill_args_at_depth(depth);
            assert!(serde_json::to_vec(&args).unwrap().len() < 8192);
            assert!(
                SessionSkillSelection::validate_borrowed("review", "builtin", 1, &args).is_err()
            );
            let mut owned = skill_request_fixture();
            owned.selections[0].args = args;
            assert!(owned.validate().is_err());
            if depth == 180 {
                assert!(serde_json::from_slice::<serde_json::Value>(
                    &serde_json::to_vec(&owned).unwrap()
                )
                .is_err());
                let mut root_input = skill_request_envelope();
                if let SessionMessageBody::Content(content) = &mut root_input.body {
                    content.skill_request = Some(owned);
                }
                assert_eq!(
                    root_input.with_root_chat_prompt("System".into()),
                    Err(SessionMessageValidationError::KindSourceBodyMismatch)
                );
            }
        }
        let node_limit = serde_json::Value::Array(vec![serde_json::Value::Null; 8191]);
        validate_skill_args_shape(&node_limit).unwrap();
        let too_many = serde_json::Value::Array(vec![serde_json::Value::Null; 8192]);
        assert!(validate_skill_args_shape(&too_many).is_err());
        // This node-limit shape independently exceeds the stricter byte bound.
        assert!(
            SessionSkillSelection::validate_borrowed("review", "builtin", 1, &node_limit).is_err()
        );
        for value in ["x".repeat(8190), "界".repeat(2730), "\\".repeat(4095)] {
            let mut args = serde_json::Value::String(value);
            assert_eq!(serde_json::to_vec(&args).unwrap().len(), 8192);
            SessionSkillSelection::validate_borrowed("review", "builtin", 1, &args).unwrap();
            if let serde_json::Value::String(value) = &mut args {
                value.push('x');
            }
            assert_eq!(serde_json::to_vec(&args).unwrap().len(), 8193);
            assert!(
                SessionSkillSelection::validate_borrowed("review", "builtin", 1, &args).is_err()
            );
        }
        let mut object = serde_json::Map::new();
        object.insert("k".repeat(8186), serde_json::json!(0));
        let args = serde_json::Value::Object(object);
        assert_eq!(serde_json::to_vec(&args).unwrap().len(), 8192);
        SessionSkillSelection::validate_borrowed("review", "builtin", 1, &args).unwrap();
    }

    #[test]
    fn skill_request_complete_json_limit_includes_all_framing() {
        let mut request = SessionSkillRequest {
            mode: Some("plan".into()),
            selections: (0..32)
                .map(|n| SessionSkillSelection {
                    id: format!("s{n}"),
                    source: "user".into(),
                    revision: 1,
                    args: serde_json::Value::String("x".repeat(8000)),
                })
                .collect(),
        };
        let mut remaining = 262144 - serde_json::to_vec(&request).unwrap().len();
        for selection in &mut request.selections {
            let serde_json::Value::String(value) = &mut selection.args else {
                unreachable!()
            };
            let extra = remaining.min(8190 - value.len());
            value.push_str(&"x".repeat(extra));
            remaining -= extra;
        }
        assert_eq!(remaining, 0);
        assert_eq!(serde_json::to_vec(&request).unwrap().len(), 262144);
        request.validate().unwrap();
        let serde_json::Value::String(value) = &mut request.selections.last_mut().unwrap().args
        else {
            unreachable!()
        };
        value.push('x');
        assert_eq!(serde_json::to_vec(&request).unwrap().len(), 262145);
        for selection in &request.selections {
            SessionSkillSelection::validate_borrowed(
                &selection.id,
                &selection.source,
                selection.revision,
                &selection.args,
            )
            .unwrap();
        }
        assert!(
            request.validate().is_err(),
            "aggregate framing still bounded"
        );
    }

    #[test]
    fn skill_request_root_option_matrix_preserves_legacy_absent_presentation() {
        let template = skill_request_envelope()
            .with_root_chat_prompt("System".into())
            .unwrap();
        for canonical in [0, 1, 2] {
            for presentation in [0, 1, 2] {
                let mut root = template.clone();
                let SessionMessageBody::RuntimeInstruction(instruction) = &mut root.body else {
                    unreachable!()
                };
                if canonical == 0 {
                    instruction.content.as_mut().unwrap().skill_request = None;
                }
                if canonical == 2 {
                    instruction.content = None;
                }
                if presentation == 0 {
                    instruction
                        .provider_message
                        .as_mut()
                        .unwrap()
                        .content
                        .skill_request = None;
                }
                if presentation == 2 {
                    instruction.provider_message = None;
                }
                assert_eq!(
                    root.validate().is_ok(),
                    (canonical == 1) == (presentation == 1),
                    "canonical={canonical}, presentation={presentation}"
                );
            }
        }
        let mut unequal = template.clone();
        if let SessionMessageBody::RuntimeInstruction(instruction) = &mut unequal.body {
            instruction
                .provider_message
                .as_mut()
                .unwrap()
                .content
                .skill_request
                .as_mut()
                .unwrap()
                .mode = Some("plan".into());
        }
        assert_eq!(
            unequal.validate(),
            Err(SessionMessageValidationError::KindSourceBodyMismatch)
        );
        let mut different_text = template.clone();
        if let SessionMessageBody::RuntimeInstruction(instruction) = &mut different_text.body {
            instruction.content.as_mut().unwrap().text = "canonical text".into();
            instruction.provider_message.as_mut().unwrap().content.text =
                "presentation text".into();
        }
        different_text.validate().unwrap();
        assert_eq!(
            different_text.to_provider_message().unwrap().content,
            "presentation text"
        );
        if let SessionMessageBody::RuntimeInstruction(instruction) = &mut different_text.body {
            instruction.content.as_mut().unwrap().skill_request = None;
            instruction
                .provider_message
                .as_mut()
                .unwrap()
                .content
                .skill_request = None;
        }
        different_text.validate().unwrap();
        let mut oversized_presentation = template;
        if let SessionMessageBody::RuntimeInstruction(instruction) =
            &mut oversized_presentation.body
        {
            instruction
                .provider_message
                .as_mut()
                .unwrap()
                .content
                .skill_request
                .as_mut()
                .unwrap()
                .selections[0]
                .args = skill_args_at_depth(180);
        }
        assert_eq!(
            oversized_presentation.validate(),
            Err(SessionMessageValidationError::KindSourceBodyMismatch)
        );
    }

    #[test]
    fn skill_request_placement_rejects_peer_child_runtime_and_wrapped_guidance() {
        let template = skill_request_envelope();
        let mut peer = template.clone();
        peer.source = SessionMessageSource::Session {
            session_id: "peer".into(),
        };
        peer.kind = SessionMessageKind::PeerMessage;
        assert_eq!(
            peer.validate(),
            Err(SessionMessageValidationError::KindSourceBodyMismatch)
        );
        for marker in [
            "session-guidance",
            "session-guidance-after-run",
            "session-guidance-after-run:run-1",
        ] {
            let mut guidance = template.clone();
            guidance.correlation_id = Some(marker.into());
            assert!(guidance.is_guidance());
            assert_eq!(
                guidance.validate(),
                Err(SessionMessageValidationError::KindSourceBodyMismatch)
            );
            assert_eq!(
                guidance.with_root_chat_prompt("System".into()),
                Err(SessionMessageValidationError::KindSourceBodyMismatch)
            );
            let mut wrapped = template
                .clone()
                .with_root_chat_prompt("System".into())
                .unwrap();
            wrapped.correlation_id = Some(marker.into());
            assert!(!wrapped.is_guidance(), "old scheduling predicate unchanged");
            assert_eq!(
                wrapped.validate(),
                Err(SessionMessageValidationError::KindSourceBodyMismatch)
            );
        }
        let mut traced = template.clone();
        traced.correlation_id = Some("session-guidance-other".into());
        traced.validate().unwrap();
        let mut runtime = template
            .clone()
            .with_root_chat_prompt("System".into())
            .unwrap();
        if let SessionMessageBody::RuntimeInstruction(instruction) = &mut runtime.body {
            instruction.instruction = "generic".into();
        }
        assert_eq!(
            runtime.validate(),
            Err(SessionMessageValidationError::KindSourceBodyMismatch)
        );
        let SessionMessageBody::Content(content) = template.body else {
            unreachable!()
        };
        runtime.kind = SessionMessageKind::ChildOutcome;
        runtime.body = SessionMessageBody::ChildOutcome(SessionChildOutcome {
            child_session_id: "child".into(),
            status: "completed".into(),
            result: None,
            error: None,
            provider_message: Some(SessionProviderMessage {
                content,
                metadata: Default::default(),
                never_compress: false,
            }),
        });
        assert_eq!(
            runtime.validate(),
            Err(SessionMessageValidationError::KindSourceBodyMismatch)
        );
    }

    #[test]
    fn skill_request_argument_order_and_all_intent_fields_define_retry_identity() {
        let envelope = skill_request_envelope();
        let mut reordered = envelope.clone();
        if let SessionMessageBody::Content(content) = &mut reordered.body {
            let mut args = serde_json::Map::new();
            args.insert("nested".into(), serde_json::json!({"b":2,"a":1}));
            args.insert("query".into(), serde_json::json!("原样"));
            content.skill_request.as_mut().unwrap().selections[0].args =
                serde_json::Value::Object(args);
        }
        assert_eq!(
            canonical_json_bytes(&envelope.idempotency_semantics()),
            canonical_json_bytes(&reordered.idempotency_semantics())
        );
        let original = envelope.to_provider_message().unwrap();
        for changed in 0..5 {
            let mut different = envelope.clone();
            let SessionMessageBody::Content(content) = &mut different.body else {
                unreachable!()
            };
            let request = content.skill_request.as_mut().unwrap();
            match changed {
                0 => request.selections[0].id = "other".into(),
                1 => request.selections[0].source = "project".into(),
                2 => request.selections[0].revision += 1,
                3 => request.selections[0].args = serde_json::json!({"changed":true}),
                _ => request.mode = Some("plan".into()),
            }
            different.validate().unwrap();
            assert_ne!(
                canonical_json_bytes(&envelope.idempotency_semantics()),
                canonical_json_bytes(&different.idempotency_semantics())
            );
            assert!(!is_matching_session_message(&original, &different));
        }
    }

    #[test]
    fn skill_request_absence_preserves_accepted_json_and_proof_bytes() {
        let old = r#"{"text":"legacy 原样"}"#;
        for input in [
            old,
            r#"{"text":"legacy 原样","skill_request":null,"future_field":true}"#,
        ] {
            let content: SessionMessageContent = serde_json::from_str(input).unwrap();
            assert_eq!(serde_json::to_string(&content).unwrap(), old);
            assert_eq!(content, SessionMessageContent::text("legacy 原样"));
        }
        let mut envelope = SessionMessageEnvelope::user_input("legacy-target", "legacy 原样");
        envelope.id = SessionMessageId::parse("legacy-stable-id").unwrap();
        envelope.created_at = "2026-10-08T00:00:00Z".parse().unwrap();
        let semantics = r#"{"body":{"content":{"text":"legacy 原样"},"type":"content"},"correlation_id":null,"in_reply_to":null,"kind":"user_input","source":{"type":"user"},"target_session_id":"legacy-target","thread_id":null}"#;
        assert_eq!(
            canonical_json_bytes(&envelope.idempotency_semantics()),
            semantics.as_bytes()
        );
        let message = envelope.to_provider_message().unwrap();
        let proof = r#"{"attempt":null,"body":{"text":"legacy 原样","type":"content"},"correlation_id":null,"created_at":"2026-10-08T00:00:00Z","id":"legacy-stable-id","idempotency_semantics":{"body":{"content":{"text":"legacy 原样"},"type":"content"},"correlation_id":null,"in_reply_to":null,"kind":"user_input","source":{"type":"user"},"target_session_id":"legacy-target","thread_id":null},"in_reply_to":null,"kind":"user_input","source":{"type":"user"},"target_session_id":"legacy-target","thread_id":null}"#;
        assert_eq!(
            canonical_json_bytes(&message.metadata.as_ref().unwrap()["session_message"]),
            proof.as_bytes()
        );
        assert_eq!(message.content, "legacy 原样");
        assert_eq!(message.id, envelope.id.to_string());
        assert_eq!(message.created_at, envelope.created_at);
        assert!(is_matching_session_message(&message, &envelope));
    }
    use crate::{ImageUrlRef, Role};

    #[test]
    fn root_chat_prompt_keeps_turn_presentation_and_semantic_retry_identity() {
        let user = SessionMessageEnvelope::user_input("root", "visible User turn");
        let first = user
            .clone()
            .with_root_chat_prompt("new System prompt".into())
            .unwrap();
        let retry = user
            .clone()
            .with_root_chat_prompt("new System prompt".into())
            .unwrap();
        assert_eq!(first.idempotency_semantics(), retry.idempotency_semantics());
        let message = first.to_provider_message().unwrap();
        assert_eq!(message.id, user.id.as_str());
        assert_eq!(message.created_at, user.created_at);
        assert_eq!(message.role, Role::User);
        assert_eq!(message.content, "visible User turn");
        assert!(!message.never_compress);
        assert_eq!(first.root_chat_prompt().unwrap(), Some("new System prompt"));
        let changed = user
            .with_root_chat_prompt("different System prompt".into())
            .unwrap();
        assert_ne!(
            first.idempotency_semantics(),
            changed.idempotency_semantics()
        );
        let mut forged = first;
        forged.source = SessionMessageSource::Runtime {
            subsystem: "unrelated".into(),
        };
        assert!(forged.validate().is_err());
    }

    #[test]
    fn multimodal_envelope_round_trips_and_keeps_provider_role_valid() {
        let envelope = SessionMessageEnvelope {
            id: SessionMessageId::parse("msg-1").unwrap(),
            source: SessionMessageSource::User,
            target_session_id: "session-1".to_string(),
            kind: SessionMessageKind::UserInput,
            body: SessionMessageBody::Content(SessionMessageContent {
                skill_request: None,
                text: "inspect this".to_string(),
                parts: vec![
                    MessagePart::Text {
                        text: "inspect this".to_string(),
                    },
                    MessagePart::ImageUrl {
                        image_url: ImageUrlRef {
                            url: "bamboo-attachment://session-1/image-1".to_string(),
                            detail: Some("high".to_string()),
                        },
                    },
                ],
            }),
            created_at: Utc::now(),
            thread_id: Some("thread-1".to_string()),
            in_reply_to: Some(SessionMessageId::parse("msg-0").unwrap()),
            attempt: Some(2),
            correlation_id: Some("corr-1".to_string()),
        };

        let json = serde_json::to_string(&envelope).unwrap();
        let restored: SessionMessageEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, envelope);

        let message = restored.to_provider_message().unwrap();
        assert_eq!(message.role, Role::User);
        assert_eq!(message.id, "msg-1");
        assert_eq!(message.content_parts.as_ref().unwrap().len(), 2);
        let metadata = message.metadata.unwrap();
        assert_eq!(metadata["session_message"]["source"]["type"], "user");
        assert_eq!(metadata["session_message"]["thread_id"], "thread-1");
    }

    #[test]
    fn envelope_deserialization_rejects_noncanonical_and_oversized_ids() {
        let envelope = SessionMessageEnvelope::user_input("target", "hello");
        let mut wire = serde_json::to_value(&envelope).unwrap();

        wire["id"] = serde_json::Value::String(" message-id ".to_string());
        assert!(serde_json::from_value::<SessionMessageEnvelope>(wire.clone()).is_err());

        wire["id"] = serde_json::Value::String(format!("{}x", " ".repeat(300)));
        assert!(serde_json::from_value::<SessionMessageEnvelope>(wire.clone()).is_err());

        wire["id"] = serde_json::Value::String("x".repeat(257));
        assert!(serde_json::from_value::<SessionMessageEnvelope>(wire.clone()).is_err());

        wire["id"] = serde_json::Value::String("message-id".to_string());
        wire["in_reply_to"] = serde_json::Value::String(" reply-id ".to_string());
        assert!(serde_json::from_value::<SessionMessageEnvelope>(wire).is_err());
    }

    #[test]
    fn kind_source_body_mismatch_is_rejected() {
        let mut envelope = SessionMessageEnvelope::user_input("target", "hello");
        envelope.source = SessionMessageSource::Session {
            session_id: "peer".to_string(),
        };
        assert_eq!(
            envelope.validate().unwrap_err(),
            SessionMessageValidationError::KindSourceBodyMismatch
        );
    }

    #[test]
    fn semantic_body_and_reply_validation_rejects_empty_values() {
        let mut envelope = SessionMessageEnvelope::user_input("target", " ");
        assert_eq!(
            envelope.validate().unwrap_err(),
            SessionMessageValidationError::EmptyContent
        );

        envelope.body = SessionMessageBody::Content(SessionMessageContent {
            skill_request: None,
            text: String::new(),
            parts: vec![MessagePart::Text {
                text: String::new(),
            }],
        });
        assert_eq!(
            envelope.validate().unwrap_err(),
            SessionMessageValidationError::EmptyContent,
            "an empty text part must not make an empty envelope semantic"
        );
        envelope.body = SessionMessageBody::Content(SessionMessageContent {
            skill_request: None,
            text: String::new(),
            parts: vec![MessagePart::ImageUrl {
                image_url: ImageUrlRef {
                    url: "bamboo-attachment://target/image".to_string(),
                    detail: None,
                },
            }],
        });
        assert!(envelope.validate().is_ok());

        envelope.in_reply_to = Some(SessionMessageId(" ../bad ".to_string()));
        assert_eq!(
            envelope.validate().unwrap_err(),
            SessionMessageValidationError::InvalidInReplyTo
        );
    }

    #[test]
    fn child_outcome_requires_identity_and_status() {
        let mut envelope = SessionMessageEnvelope {
            id: SessionMessageId::parse("child-outcome").unwrap(),
            source: SessionMessageSource::Session {
                session_id: "parent".to_string(),
            },
            target_session_id: "target".to_string(),
            kind: SessionMessageKind::ChildOutcome,
            body: SessionMessageBody::ChildOutcome(SessionChildOutcome {
                child_session_id: " ".to_string(),
                status: "completed".to_string(),
                result: None,
                error: None,
                provider_message: None,
            }),
            created_at: Utc::now(),
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: None,
        };
        assert_eq!(
            envelope.validate().unwrap_err(),
            SessionMessageValidationError::EmptyChildSessionId
        );
        let SessionMessageBody::ChildOutcome(outcome) = &mut envelope.body else {
            unreachable!()
        };
        outcome.child_session_id = "child".to_string();
        outcome.status = String::new();
        assert_eq!(
            envelope.validate().unwrap_err(),
            SessionMessageValidationError::EmptyChildStatus
        );
    }

    #[test]
    fn runtime_instruction_requires_name_and_semantic_optional_content() {
        let mut envelope = SessionMessageEnvelope {
            id: SessionMessageId::parse("runtime").unwrap(),
            source: SessionMessageSource::Runtime {
                subsystem: "scheduler".to_string(),
            },
            target_session_id: "target".to_string(),
            kind: SessionMessageKind::RuntimeInstruction,
            body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                instruction: String::new(),
                content: None,
                data: Some(serde_json::json!({"wake": true})),
                provider_message: None,
            }),
            created_at: Utc::now(),
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: None,
        };
        assert_eq!(
            envelope.validate().unwrap_err(),
            SessionMessageValidationError::EmptyRuntimeInstruction
        );
        {
            let SessionMessageBody::RuntimeInstruction(instruction) = &mut envelope.body else {
                unreachable!()
            };
            instruction.instruction = "wake".to_string();
            instruction.content = Some(SessionMessageContent::text(" "));
        }
        assert_eq!(
            envelope.validate().unwrap_err(),
            SessionMessageValidationError::EmptyRuntimeContent
        );
        let SessionMessageBody::RuntimeInstruction(instruction) = &mut envelope.body else {
            unreachable!()
        };
        instruction.content = Some(SessionMessageContent::text("resume"));
        assert!(envelope.validate().is_ok());
    }

    #[test]
    fn durable_transcript_marker_requires_full_envelope_semantics() {
        let envelope = SessionMessageEnvelope::user_input("target", "hello");
        let message = envelope.to_provider_message().unwrap();
        assert!(is_matching_session_message(&message, &envelope));

        let mut forged = message.clone();
        forged.metadata.as_mut().unwrap()["session_message"]["target_session_id"] =
            serde_json::Value::String("other".to_string());
        assert!(!is_matching_session_message(&forged, &envelope));
        forged = message.clone();
        let mut different = envelope.clone();
        different.body = SessionMessageBody::Content(SessionMessageContent::text("different"));
        forged.metadata = different.to_provider_message().unwrap().metadata;
        assert!(!is_matching_session_message(&forged, &envelope));
        forged = message.clone();
        forged.content = "different".to_string();
        assert!(!is_matching_session_message(&forged, &envelope));
        forged = message;
        forged.role = Role::Assistant;
        assert!(!is_matching_session_message(&forged, &envelope));
    }

    #[test]
    fn provider_presentation_proof_is_exact_and_marker_is_reserved() {
        let mut provider_metadata = serde_json::Map::new();
        provider_metadata.insert("hidden_from_ui".to_string(), serde_json::Value::Bool(true));
        provider_metadata.insert(
            "runtime_kind".to_string(),
            serde_json::Value::String("guardian_verdict".to_string()),
        );
        let mut envelope = SessionMessageEnvelope {
            id: SessionMessageId::parse("child-proof").unwrap(),
            source: SessionMessageSource::Runtime {
                subsystem: "child_completion".to_string(),
            },
            target_session_id: "parent".to_string(),
            kind: SessionMessageKind::ChildOutcome,
            body: SessionMessageBody::ChildOutcome(SessionChildOutcome {
                child_session_id: "child".to_string(),
                status: "completed".to_string(),
                result: Some("approved".to_string()),
                error: None,
                provider_message: Some(SessionProviderMessage {
                    content: SessionMessageContent {
                        skill_request: None,
                        text: "exact guardian resume".to_string(),
                        parts: vec![MessagePart::ImageUrl {
                            image_url: ImageUrlRef {
                                url: "bamboo-attachment://parent/verdict".to_string(),
                                detail: Some("high".to_string()),
                            },
                        }],
                    },
                    metadata: provider_metadata,
                    never_compress: true,
                }),
            }),
            created_at: Utc::now(),
            thread_id: Some("wait-1".to_string()),
            in_reply_to: None,
            attempt: None,
            correlation_id: Some("child".to_string()),
        };

        let message = envelope.to_provider_message().unwrap();
        assert!(is_matching_session_message(&message, &envelope));

        let mut forged = message.clone();
        forged.content_parts = None;
        assert!(!is_matching_session_message(&forged, &envelope));

        let mut forged = message.clone();
        forged.never_compress = false;
        assert!(!is_matching_session_message(&forged, &envelope));

        let mut forged = message.clone();
        forged.metadata.as_mut().unwrap()["hidden_from_ui"] = serde_json::Value::Bool(false);
        assert!(!is_matching_session_message(&forged, &envelope));

        let mut forged = message;
        forged.metadata.as_mut().unwrap()["session_message"]["idempotency_semantics"]["body"]
            ["status"] = serde_json::Value::String("forged".to_string());
        assert!(!is_matching_session_message(&forged, &envelope));

        let SessionMessageBody::ChildOutcome(outcome) = &mut envelope.body else {
            unreachable!()
        };
        outcome.provider_message.as_mut().unwrap().metadata.insert(
            "session_message".to_string(),
            serde_json::json!({"forged": true}),
        );
        assert_eq!(
            envelope.validate().unwrap_err(),
            SessionMessageValidationError::ReservedProviderMetadata
        );
    }

    #[test]
    fn admission_state_is_bounded_and_mergeable() {
        let mut state = SessionInboxAdmissionState::default();
        for sequence in 1..=(SESSION_INBOX_ADMITTED_CAPACITY as u64 + 1) {
            state.record(
                SessionMessageId::parse(format!("message-{sequence}")).unwrap(),
                sequence,
            );
        }
        assert_eq!(state.len(), SESSION_INBOX_ADMITTED_CAPACITY);
        assert_eq!(
            state.last_admitted_sequence,
            SESSION_INBOX_ADMITTED_CAPACITY as u64 + 1
        );
        assert!(!state.contains(&SessionMessageId::parse("message-1").unwrap()));

        let json = serde_json::to_string(&state).unwrap();
        let mut restored: SessionInboxAdmissionState = serde_json::from_str(&json).unwrap();
        restored.rebuild_index();
        assert!(restored.contains(
            &SessionMessageId::parse(format!("message-{}", SESSION_INBOX_ADMITTED_CAPACITY + 1))
                .unwrap()
        ));
    }

    #[test]
    fn legacy_ids_are_deterministic_and_position_sensitive() {
        let value = serde_json::json!({"content": "hello"});
        assert_eq!(
            SessionMessageId::legacy("session", 0, &value),
            SessionMessageId::legacy("session", 0, &value)
        );
        assert_ne!(
            SessionMessageId::legacy("session", 0, &value),
            SessionMessageId::legacy("session", 1, &value)
        );
    }

    #[test]
    fn stable_ids_use_canonical_sha256_and_distinguish_inputs() {
        let left = serde_json::json!({"b": [2, 3], "a": 1});
        let mut right_map = serde_json::Map::new();
        right_map.insert("a".to_string(), serde_json::json!(1));
        right_map.insert("b".to_string(), serde_json::json!([2, 3]));
        let right = serde_json::Value::Object(right_map);
        let stable = SessionMessageId::stable("runtime", &left);
        assert_eq!(stable, SessionMessageId::stable("runtime", &right));
        assert_eq!(stable.as_str().len(), "stable-".len() + 64);
        assert_ne!(
            stable,
            SessionMessageId::stable("runtime", &serde_json::json!({"a": 1, "b": [2, 4]}))
        );
        assert_ne!(stable, SessionMessageId::stable("other", &left));
    }
}
