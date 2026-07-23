//! Typed, well-known runtime metadata lifted out of `Session.metadata`.
//!
//! Historically a fixed set of well-known string keys were smuggled through the
//! open-ended `Session.metadata: HashMap<String, String>` map (e.g.
//! `subagent_type`, `last_run_status`, `workspace_path`, …). This struct gives
//! those well-known keys a typed home while the open-ended map keeps holding
//! genuinely free-form entries (`gold_config`, `responses.previous_response_id`,
//! `a2a.*`, `agent.runtime.state`, external-agent fields, …).
//!
//! Migration safety: this field is additive and optional. Old persisted JSONL
//! that only carries the legacy `metadata` map continues to deserialize cleanly
//! because the field is `#[serde(default, skip_serializing_if = "Option::is_none")]`.
//! The accessor layer in `runtime_metadata_access` performs **dual-write** (typed
//! field + legacy metadata string) and **fallback-read** (typed first, legacy
//! string second) so that un-migrated call sites still reading the raw map keep
//! observing correct values.
//!
//! Note on scope: `hidden_from_ui` and `runtime_kind` are deliberately NOT
//! modeled here. Despite appearing in the original key list, they live on
//! `Message.metadata` (a `serde_json::Value`), never on `Session.metadata`, so
//! they are not session runtime metadata. The session-level "kind" annotation is
//! the open-ended `runtime.kind` key used by external agents, which stays in the
//! free-form map untouched.

use serde::{Deserialize, Serialize};

/// Legacy `Session.metadata` string keys mirrored by [`SessionRuntimeMetadata`].
pub mod keys {
    pub const SUBAGENT_TYPE: &str = "subagent_type";
    pub const LAST_RUN_STATUS: &str = "last_run_status";
    pub const LAST_RUN_ERROR: &str = "last_run_error";
    pub const PROVIDER_NAME: &str = "provider_name";
    pub const PENDING_INJECTED_MESSAGES: &str = "pending_injected_messages";
    pub const SELECTED_SKILL_IDS: &str = "selected_skill_ids";
    pub const SKILL_MODE: &str = "skill_mode";
    /// Legacy duplicate of [`SKILL_MODE`]; read-only fallback, never written.
    pub const SKILL_MODE_LEGACY: &str = "mode";
    pub const REASONING_EFFORT: &str = "reasoning_effort";
    pub const ENHANCE_PROMPT: &str = "enhance_prompt";
    pub const TASK_LIST_VERSION: &str = "task_list_version";
    pub const TODO_LIST_VERSION: &str = "todo_list_version";
    pub const WORKSPACE_PATH: &str = "workspace_path";
    pub const PROJECT_ID: &str = "project_id";
}

/// Typed view over the well-known runtime metadata keys on a [`Session`].
///
/// Every field is `Option` and omitted from serialization when absent, so a
/// session that has never set any runtime metadata serializes no
/// `runtime_metadata` object at all. All string-valued fields hold the exact
/// same bytes that previously lived in the legacy metadata string, so the
/// dual-write mirror is value-identical.
///
/// [`Session`]: super::Session
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionRuntimeMetadata {
    /// SubAgent profile id (legacy `metadata["subagent_type"]`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagent_type: Option<String>,
    /// Last run lifecycle status, e.g. `pending`/`running`/`completed`/`cancelled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_status: Option<String>,
    /// Last run error text, if the previous run failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_error: Option<String>,
    /// Provider name pinned to this session (legacy `metadata["provider_name"]`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_name: Option<String>,
    /// Follow-up messages injected via `send_message` while a session runs.
    ///
    /// Stored on the legacy map as a JSON-encoded string of an array of message
    /// objects; here it is the decoded array. Parsing is defensive: malformed
    /// legacy JSON is treated as empty, never a panic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_injected_messages: Option<Vec<serde_json::Value>>,
    /// Selected skill ids (legacy `metadata["selected_skill_ids"]`, stored on the
    /// map as a JSON array string; here the decoded vector).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_skill_ids: Option<Vec<String>>,
    /// Canonical skill mode. Unifies the historical `skill_mode` / `mode`
    /// duplication: this is the canonical key; `mode` is a read-only fallback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_mode: Option<String>,
    /// Reasoning effort override, stored as its lowercase string form.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// System-prompt enhancement text (legacy `metadata["enhance_prompt"]`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enhance_prompt: Option<String>,
    /// Monotonic task-list version, stored as its decimal string form.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_list_version: Option<String>,
    /// Legacy alias of the task-list version (`todo_list_version`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub todo_list_version: Option<String>,
    /// Workspace directory recorded for prompt/workspace context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
    /// Stable Project identity. This does not change when the workspace changes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
}

impl SessionRuntimeMetadata {
    /// True when no typed runtime metadata is present.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}
