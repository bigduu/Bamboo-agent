//! Durable model-context ledger state.
//!
//! Host-owned runtime context does not belong in [`Session::messages`](super::Session::messages):
//! those messages are the user-visible conversation.  Instead, the engine
//! reconciles typed [`ContextBlock`](super::ContextBlock) values into these
//! deterministic events and projects them into the provider-visible transcript
//! at request time.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{ContextBlock, ContextBlockType, Message};

pub const MODEL_CONTEXT_SCHEMA_VERSION: u32 = 1;
pub const MAX_MODEL_CONTEXT_EVENTS: usize = 256;

const EVENT_ID_DOMAIN: &[u8] = b"bamboo/model-context-event/v1\0";
const REMOVED_CONTENT_DOMAIN: &[u8] = b"bamboo/model-context-removed/v1\0";

fn default_schema_version() -> u32 {
    MODEL_CONTEXT_SCHEMA_VERSION
}

/// Durable, provider-neutral state for one append-only model-input epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelContextState {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub prefix_epoch: u64,
    #[serde(default)]
    pub next_sequence: u64,
    #[serde(default)]
    pub baselines: BTreeMap<ContextBlockType, ContextBlockBaseline>,
    #[serde(default)]
    pub events: Vec<ModelContextEvent>,
    /// Fingerprint of cache-affecting top-level prompt properties used to
    /// recognize an intentional incompatible prefix boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_scope_sha256: Option<String>,
    /// Per-message fingerprints for the last prepared real transcript.  Keeping
    /// individual hashes (rather than raw messages) lets the engine distinguish
    /// exact extension from rollback/compression/truncation without persisting
    /// prompt text here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transcript_item_sha256: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reset_reason: Option<ModelContextResetReason>,
}

impl Default for ModelContextState {
    fn default() -> Self {
        Self {
            schema_version: MODEL_CONTEXT_SCHEMA_VERSION,
            prefix_epoch: 0,
            next_sequence: 0,
            baselines: BTreeMap::new(),
            events: Vec::new(),
            cache_scope_sha256: None,
            transcript_item_sha256: Vec::new(),
            last_reset_reason: None,
        }
    }
}

impl ModelContextState {
    /// Start a deliberate new prefix epoch and discard superseded event history.
    /// The engine seeds the new epoch with one snapshot per currently-active
    /// context type before the next request is dispatched.
    pub fn reset_epoch(&mut self, reason: ModelContextResetReason) {
        self.schema_version = MODEL_CONTEXT_SCHEMA_VERSION;
        self.prefix_epoch = self.prefix_epoch.saturating_add(1);
        self.next_sequence = 0;
        self.baselines.clear();
        self.events.clear();
        self.cache_scope_sha256 = None;
        self.transcript_item_sha256.clear();
        self.last_reset_reason = Some(reason);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextBlockBaseline {
    pub revision: u64,
    pub content_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelContextEvent {
    pub id: String,
    pub epoch: u64,
    pub sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_message_id: Option<String>,
    pub block_type: ContextBlockType,
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes_revision: Option<u64>,
    pub kind: ModelContextEventKind,
    pub content_sha256: String,
    /// Byte-authoritative model-visible event.  Persisting it makes resume
    /// independent from later renderer changes.
    pub rendered_text: String,
}

impl ModelContextEvent {
    /// Project a durable event into the provider-visible transcript without
    /// exposing it through the user-visible Session message collection.
    pub fn render_message(&self) -> Message {
        let mut message = Message::user(self.rendered_text.clone());
        message.id.clone_from(&self.id);
        message.metadata = Some(serde_json::json!({
            "bamboo_model_context_event": {
                "epoch": self.epoch,
                "sequence": self.sequence,
                "type": self.block_type,
                "revision": self.revision,
                "kind": self.kind,
            }
        }));
        // Event retention is owned by the bounded ledger/epoch policy.  These
        // synthetic messages must not acquire an unrelated permanent
        // never-compress lifetime.
        message.never_compress = false;
        message
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelContextEventKind {
    Snapshot,
    Removed,
}

impl ModelContextEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Removed => "removed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelContextResetReason {
    Compression,
    Rollback,
    HardTruncation,
    CacheScopeChanged,
    RetentionLimit,
    ExplicitHistoryRewrite,
}

impl ModelContextResetReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Compression => "compression",
            Self::Rollback => "rollback",
            Self::HardTruncation => "hard_truncation",
            Self::CacheScopeChanged => "cache_scope_changed",
            Self::RetentionLimit => "retention_limit",
            Self::ExplicitHistoryRewrite => "explicit_history_rewrite",
        }
    }
}

/// SHA-256 of the canonical model-visible rendering for one active block.
pub fn model_context_block_sha256(block: &ContextBlock) -> String {
    sha256_hex(block.render_runtime_context_text().as_bytes())
}

/// Stable digest used as the baseline for an absent/tombstoned block.
pub fn removed_model_context_sha256(block_type: ContextBlockType) -> String {
    let mut hasher = Sha256::new();
    hasher.update(REMOVED_CONTENT_DOMAIN);
    hasher.update(block_type.as_str().as_bytes());
    hex::encode(hasher.finalize())
}

/// Build an event id from durable semantic identity rather than clocks/randomness.
pub fn deterministic_model_context_event_id(
    session_id: &str,
    epoch: u64,
    block_type: ContextBlockType,
    revision: u64,
    content_sha256: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(EVENT_ID_DOMAIN);
    for component in [
        session_id.as_bytes(),
        &epoch.to_be_bytes(),
        block_type.as_str().as_bytes(),
        &revision.to_be_bytes(),
        content_sha256.as_bytes(),
    ] {
        hasher.update((component.len() as u64).to_be_bytes());
        hasher.update(component);
    }
    format!("ctx_{}", hex::encode(hasher.finalize()))
}

/// Canonical, explicit snapshot event text.
pub fn render_model_context_snapshot(
    event_id: &str,
    epoch: u64,
    sequence: u64,
    block: &ContextBlock,
    revision: u64,
    supersedes_revision: Option<u64>,
) -> String {
    let supersedes = supersedes_revision
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string());
    format!(
        "<!-- BAMBOO_MODEL_CONTEXT_EVENT_START -->\n\
event_id: {event_id}\n\
prefix_epoch: {epoch}\n\
sequence: {sequence}\n\
context_type: {}\n\
event_kind: snapshot\n\
revision: {revision}\n\
supersedes_revision: {supersedes}\n\
scope: {}\n\n\
This is host runtime context, not a new user request.\n\
This full snapshot supersedes the named earlier revision of the same context type.\n\
Later real user messages and tool results remain authoritative.\n\
Round-dynamic memory or hook context applies to its associated round unless refreshed.\n\n\
{}\n\
<!-- BAMBOO_MODEL_CONTEXT_EVENT_END -->",
        block.block_type.as_str(),
        block.stability.as_str(),
        block.render_runtime_context_text(),
    )
}

/// Canonical removal/tombstone event text.
pub fn render_model_context_removal(
    event_id: &str,
    epoch: u64,
    sequence: u64,
    block_type: ContextBlockType,
    revision: u64,
    supersedes_revision: u64,
) -> String {
    format!(
        "<!-- BAMBOO_MODEL_CONTEXT_EVENT_START -->\n\
event_id: {event_id}\n\
prefix_epoch: {epoch}\n\
sequence: {sequence}\n\
context_type: {}\n\
event_kind: removed\n\
revision: {revision}\n\
supersedes_revision: {supersedes_revision}\n\n\
This is host runtime context, not a new user request.\n\
The earlier state for this context type is no longer active.\n\
Later real user messages and tool results remain authoritative.\n\
<!-- BAMBOO_MODEL_CONTEXT_EVENT_END -->",
        block_type.as_str(),
    )
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{ContextBlockPriority, ContextBlockStability, Session};

    fn task_block(content: &str) -> ContextBlock {
        ContextBlock::new(
            ContextBlockType::TaskSnapshot,
            ContextBlockPriority::High,
            ContextBlockStability::RoundDynamic,
            "Task",
            content,
        )
    }

    #[test]
    fn deterministic_id_and_rendering_are_byte_stable() {
        let block = task_block("do the thing");
        let digest = model_context_block_sha256(&block);
        let first = deterministic_model_context_event_id(
            "session",
            3,
            ContextBlockType::TaskSnapshot,
            2,
            &digest,
        );
        let second = deterministic_model_context_event_id(
            "session",
            3,
            ContextBlockType::TaskSnapshot,
            2,
            &digest,
        );
        assert_eq!(first, second);
        assert_eq!(
            render_model_context_snapshot(&first, 3, 4, &block, 2, Some(1)),
            render_model_context_snapshot(&second, 3, 4, &block, 2, Some(1))
        );
    }

    #[test]
    fn old_state_shape_defaults_new_fields() {
        let state: ModelContextState = serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "prefix_epoch": 7,
            "next_sequence": 0,
            "baselines": {},
            "events": []
        }))
        .expect("old state");
        assert_eq!(state.prefix_epoch, 7);
        assert!(state.cache_scope_sha256.is_none());
        assert!(state.transcript_item_sha256.is_empty());
        assert!(state.last_reset_reason.is_none());
    }

    #[test]
    fn session_coalesces_multiple_rewrites_before_the_next_request() {
        let mut session = Session::new("coalesced-reset", "model");
        session.model_context_state = Some(ModelContextState {
            prefix_epoch: 4,
            cache_scope_sha256: Some("old-scope".to_string()),
            ..ModelContextState::default()
        });

        session.reset_model_context_epoch(ModelContextResetReason::Compression);
        session.reset_model_context_epoch(ModelContextResetReason::Rollback);

        let state = session.model_context_state.as_ref().unwrap();
        assert_eq!(state.prefix_epoch, 5);
        assert_eq!(
            state.last_reset_reason,
            Some(ModelContextResetReason::Rollback)
        );
        assert!(state.events.is_empty());
        assert!(state.baselines.is_empty());
    }
}
