//! Child-session completion notification primitives.
//!
//! The engine owns child runner lifecycle, but parent resume policy lives in the
//! server/application layer.  This module defines the small callback boundary
//! between them so child completion is event-driven without making the engine
//! depend on `AppState`.

use async_trait::async_trait;
use bamboo_agent_core::{Message, Role, Session, SessionKind};
use bamboo_domain::{ChildContextBinding, MessagePhase};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

const TERMINAL_SOURCE_KEY: &str = "runtime.child_completion_source_v1";

/// A seal stored by the final runtime writer in the existing Session commit.
/// It is not a second outcome store: later input/output makes it unavailable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildCompletionSource {
    pub activation_run_id: String,
    pub child_session_id: String,
    pub child_created_at: DateTime<Utc>,
    pub parent_session_id: String,
    pub assignment_sha256: Option<String>,
    pub project_id: Option<String>,
    pub runtime_run_id: Option<String>,
    pub input_sha256: String,
    pub status: String,
    pub error_sha256: Option<String>,
    pub last_assistant: Option<ChildCompletionMessageSource>,
    pub result_message_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildCompletionMessageSource {
    pub message_id: String,
    pub message_sha256: String,
}

fn digest(value: impl Serialize) -> Option<String> {
    Some(hex::encode(Sha256::digest(
        serde_json::to_vec(&value).ok()?,
    )))
}

fn source_project(session: &Session) -> Option<Option<String>> {
    use crate::project_context::{ProjectContextResolver, SessionProjectIdentity};
    match ProjectContextResolver::session_project_identity(session) {
        SessionProjectIdentity::Unassigned => Some(None),
        SessionProjectIdentity::Assigned(id) => Some(Some(id.to_string())),
        SessionProjectIdentity::Invalid { .. } => None,
    }
}

fn last_assistant(session: &Session) -> Option<(usize, &Message)> {
    session
        .messages
        .iter()
        .enumerate()
        .rev()
        .find(|(_, m)| m.role == Role::Assistant)
}

fn message_source(message: &Message) -> Option<ChildCompletionMessageSource> {
    Some(ChildCompletionMessageSource {
        message_id: message.id.clone(),
        message_sha256: digest(message)?,
    })
}

fn current_final(session: &Session, index: usize, message: &Message) -> bool {
    !message.id.is_empty()
        && !message.content.trim().is_empty()
        && message.phase != Some(MessagePhase::Commentary)
        && message.tool_calls.as_ref().is_none_or(Vec::is_empty)
        && !message.compressed
        && message.compressed_by_event_id.is_none()
        && message.compression_level == 0
        && !session.messages[index + 1..]
            .iter()
            .any(|m| m.role == Role::User)
}

impl ChildCompletionSource {
    /// Stage the source in the same snapshot as the terminal save. Callers may
    /// publish it ONLY after save success and `from_committed_session` validates
    /// the lock-reconciled snapshot. Never stamp a reloaded successor snapshot.
    pub(crate) fn prepare(
        session: &mut Session,
        activation_run_id: &str,
        prior_message_ids: &HashSet<String>,
    ) {
        session.metadata.remove(TERMINAL_SOURCE_KEY);
        let source = (|| {
            let status = session.last_run_status()?;
            if session.kind != SessionKind::Child
                || activation_run_id.is_empty()
                || !matches!(
                    status.as_str(),
                    "completed" | "error" | "cancelled" | "timeout" | "skipped"
                )
            {
                return None;
            }
            // Malformed binding is not the same as a valid legacy absence.
            let binding = ChildContextBinding::from_session(session).ok()?;
            let assistant = last_assistant(session);
            let result_message_id = assistant
                .filter(|(index, message)| {
                    !prior_message_ids.contains(&message.id)
                        && current_final(session, *index, message)
                })
                .map(|(_, message)| message.id.clone());
            Some(Self {
                activation_run_id: activation_run_id.to_string(),
                child_session_id: session.id.clone(),
                child_created_at: session.created_at,
                parent_session_id: session.parent_session_id.clone()?,
                assignment_sha256: binding.map(|b| b.assignment_sha256),
                project_id: source_project(session)?,
                runtime_run_id: crate::runtime::runner::state_bridge::read_runtime_state(session)
                    .map(|state| state.run_id),
                input_sha256: digest(
                    session
                        .messages
                        .iter()
                        .filter(|m| m.role == Role::User)
                        .collect::<Vec<_>>(),
                )?,
                status,
                error_sha256: session.last_run_error().map(digest).transpose_option()?,
                last_assistant: assistant
                    .map(|(_, m)| message_source(m))
                    .transpose_option()?,
                result_message_id,
            })
        })();
        if let Some(source) = source
            .and_then(|s| serde_json::to_string(&s).ok())
            .filter(|s| s.len() <= 4096)
        {
            session
                .metadata
                .insert(TERMINAL_SOURCE_KEY.to_string(), source);
        }
    }

    /// This only observes a successful caller-owned canonical save, or a later
    /// durable snapshot. Reading a seal does not certify a separate activation.
    pub(crate) fn from_committed_session(session: &Session) -> Option<Self> {
        let raw = session.metadata.get(TERMINAL_SOURCE_KEY)?;
        if raw.len() > 4096 {
            return None;
        }
        let source: Self = serde_json::from_str(raw).ok()?;
        source.matches_snapshot(session).then_some(source)
    }

    pub(crate) fn has_source_record(session: &Session) -> bool {
        session.metadata.contains_key(TERMINAL_SOURCE_KEY)
    }

    pub(crate) fn after_final_save(session: &Session, saved: bool) -> Option<Self> {
        saved
            .then(|| Self::from_committed_session(session))
            .flatten()
    }

    fn matches_snapshot(&self, session: &Session) -> bool {
        let Some(binding) = ChildContextBinding::from_session(session).ok() else {
            return false;
        };
        session.kind == SessionKind::Child
            && session.id == self.child_session_id
            && session.created_at == self.child_created_at
            && session.parent_session_id.as_deref() == Some(self.parent_session_id.as_str())
            && session.last_run_status().as_deref() == Some(self.status.as_str())
            && session.last_run_error().map(digest).transpose_option()
                == Some(self.error_sha256.clone())
            && binding.map(|b| b.assignment_sha256) == self.assignment_sha256
            && source_project(session) == Some(self.project_id.clone())
            && crate::runtime::runner::state_bridge::read_runtime_state(session).map(|s| s.run_id)
                == self.runtime_run_id
            && digest(
                session
                    .messages
                    .iter()
                    .filter(|m| m.role == Role::User)
                    .collect::<Vec<_>>(),
            )
            .as_deref()
                == Some(self.input_sha256.as_str())
            && last_assistant(session)
                .map(|(_, m)| message_source(m))
                .transpose_option()
                == Some(self.last_assistant.clone())
            && self.result_message_id.as_ref().is_none_or(|id| {
                last_assistant(session).is_some_and(|(index, m)| {
                    &m.id == id
                        && current_final(session, index, m)
                        && session.messages.iter().filter(|m| &m.id == id).count() == 1
                })
            })
    }

    pub(crate) fn matches_completion(&self, completion: &ChildCompletion) -> bool {
        self.child_session_id == completion.child_session_id
            && self.parent_session_id == completion.parent_session_id
            && self.status == completion.status
            && completion.error.clone().map(digest).transpose_option()
                == Some(self.error_sha256.clone())
    }

    pub(crate) fn matches_parent(&self, child: &Session, parent: &Session) -> bool {
        if child.parent_session_id.as_deref() != Some(parent.id.as_str()) {
            return false;
        }
        match ChildContextBinding::from_session(child) {
            Ok(None) => true,
            Ok(Some(binding)) => {
                binding.validate_parent_sources(parent).is_ok()
                    && source_project(child) == source_project(parent)
                    && child.root_session_id == parent.root_session_id
                    && parent.spawn_depth.checked_add(1) == Some(child.spawn_depth)
            }
            Err(_) => false,
        }
    }

    pub(crate) fn result(&self, child: &Session, parent: &Session) -> Option<String> {
        if Self::from_committed_session(child).as_ref() != Some(self)
            || !self.matches_parent(child, parent)
        {
            return None;
        }
        let id = self.result_message_id.as_ref()?;
        child
            .messages
            .iter()
            .find(|m| &m.id == id)
            .map(|m| m.content.clone())
    }
}

// Option<Option<T>> has no standard transpose; keep serialization failures
// distinct from the legitimate absence of a source field.
trait TransposeOption<T> {
    fn transpose_option(self) -> Option<Option<T>>;
}
impl<T> TransposeOption<T> for Option<Option<T>> {
    fn transpose_option(self) -> Option<Option<T>> {
        match self {
            Some(value) => value.map(Some),
            None => Some(None),
        }
    }
}

/// Terminal child-session completion recorded by the child runner lifecycle.
#[derive(Debug, Clone)]
pub struct ChildCompletion {
    pub parent_session_id: String,
    pub child_session_id: String,
    /// One of: completed | cancelled | error | skipped | timeout.
    pub status: String,
    pub error: Option<String>,
    pub completed_at: DateTime<Utc>,
    /// None for synthetic/early failures or an unsuccessful terminal save.
    pub source: Option<ChildCompletionSource>,
}

/// Application-layer callback invoked when a child session reaches a terminal
/// state. Implementations must be idempotent: duplicate completion events for
/// the same child can occur when watchdog timeout races normal runner teardown.
#[async_trait]
pub trait ChildCompletionHandler: Send + Sync {
    async fn on_child_completed(&self, completion: ChildCompletion);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn final_child() -> Session {
        let mut child = Session::new_child("child", "parent", "model", "Child");
        child.add_message(Message::user("assignment A"));
        child.add_message(Message::assistant("answer A", None));
        child.set_last_run_status("completed");
        child
    }

    #[test]
    fn seal_rejects_failed_save_and_overwrites_worker_supplied_provenance() {
        let mut child = final_child();
        child.metadata.insert(
            TERMINAL_SOURCE_KEY.into(),
            "{\"activation_run_id\":\"forged\"}".into(),
        );
        ChildCompletionSource::prepare(&mut child, "host-run-A", &HashSet::new());
        assert!(ChildCompletionSource::after_final_save(&child, false).is_none());
        let source = ChildCompletionSource::after_final_save(&child, true).unwrap();
        assert_eq!(source.activation_run_id, "host-run-A");
        assert_eq!(
            source
                .result(&child, &Session::new("parent", "model"))
                .as_deref(),
            Some("answer A")
        );
    }

    #[test]
    fn seal_never_imports_an_older_answer_when_run_produced_none() {
        let mut child = final_child();
        let prior = child.messages.iter().map(|m| m.id.clone()).collect();
        child.set_last_run_status("error");
        child.set_last_run_error("provider failed before output");
        ChildCompletionSource::prepare(&mut child, "host-run-B", &prior);
        let source = ChildCompletionSource::after_final_save(&child, true).unwrap();
        assert!(source.result_message_id.is_none());
        assert!(source
            .result(&child, &Session::new("parent", "model"))
            .is_none());
    }

    #[test]
    fn seal_rejects_birth_input_result_and_binding_mutations() {
        let mut child = final_child();
        ChildCompletionSource::prepare(&mut child, "host-run-A", &HashSet::new());
        for mutation in 0..6 {
            let mut changed = child.clone();
            match mutation {
                0 => changed.created_at += chrono::Duration::seconds(1),
                1 => changed.messages[0].content.push_str("changed"),
                2 => changed.messages[1].content.push_str("changed"),
                3 => {
                    changed.messages.pop();
                }
                4 => changed.messages.push(changed.messages[1].clone()),
                _ => {
                    changed
                        .metadata
                        .insert(bamboo_domain::CHILD_PACKET_REQUIRED_KEY.into(), "v1".into());
                }
            }
            assert!(
                ChildCompletionSource::from_committed_session(&changed).is_none(),
                "mutation {mutation}"
            );
        }
        child
            .metadata
            .insert(bamboo_domain::CHILD_PACKET_REQUIRED_KEY.into(), "v1".into());
        ChildCompletionSource::prepare(&mut child, "host-run-B", &HashSet::new());
        assert!(
            !child.metadata.contains_key(TERMINAL_SOURCE_KEY),
            "invalid binding cannot retain an old seal"
        );
    }
}
