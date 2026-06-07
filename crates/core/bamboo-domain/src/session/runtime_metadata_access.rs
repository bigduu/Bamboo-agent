//! Symmetric accessor layer for [`SessionRuntimeMetadata`].
//!
//! Every accessor follows two rules, mirroring the existing
//! `agent_runtime_state` <-> `metadata["agent.runtime.state"]` idiom:
//!
//! - **Fallback-read**: getters read the typed `runtime_metadata` field first,
//!   then fall back to the legacy `metadata["<key>"]` string. This keeps reads
//!   correct for sessions persisted before the typed field existed.
//! - **Dual-write**: setters write BOTH the typed field AND the legacy metadata
//!   string. This keeps the ~120 un-migrated call sites that still read the raw
//!   `metadata` map correct until a future cleanup wave removes the mirror.
//!
//! Clearers remove from both planes symmetrically.

use super::runtime_metadata::{keys, SessionRuntimeMetadata};
use super::types::Session;

impl Session {
    /// Mutable handle to the typed runtime metadata, creating it on demand.
    fn runtime_metadata_mut(&mut self) -> &mut SessionRuntimeMetadata {
        self.runtime_metadata.get_or_insert_with(Default::default)
    }

    /// Drop the typed runtime metadata object if it carries no values, so an
    /// emptied session does not serialize an empty `runtime_metadata` object.
    fn prune_runtime_metadata(&mut self) {
        if self
            .runtime_metadata
            .as_ref()
            .is_some_and(SessionRuntimeMetadata::is_empty)
        {
            self.runtime_metadata = None;
        }
    }

    /// Read a typed `Option<String>` field, falling back to the legacy key.
    fn runtime_str(
        &self,
        select: impl FnOnce(&SessionRuntimeMetadata) -> Option<&String>,
        legacy_key: &str,
    ) -> Option<String> {
        self.runtime_metadata
            .as_ref()
            .and_then(select)
            .cloned()
            .or_else(|| self.metadata.get(legacy_key).cloned())
    }

    // ------------------------------------------------------------------
    // subagent_type
    // ------------------------------------------------------------------

    pub fn subagent_type(&self) -> Option<String> {
        self.runtime_str(|m| m.subagent_type.as_ref(), keys::SUBAGENT_TYPE)
    }

    pub fn set_subagent_type(&mut self, value: impl Into<String>) {
        let value = value.into();
        self.runtime_metadata_mut().subagent_type = Some(value.clone());
        self.metadata.insert(keys::SUBAGENT_TYPE.to_string(), value);
    }

    // ------------------------------------------------------------------
    // last_run_status
    // ------------------------------------------------------------------

    pub fn last_run_status(&self) -> Option<String> {
        self.runtime_str(|m| m.last_run_status.as_ref(), keys::LAST_RUN_STATUS)
    }

    pub fn set_last_run_status(&mut self, value: impl Into<String>) {
        let value = value.into();
        self.runtime_metadata_mut().last_run_status = Some(value.clone());
        self.metadata
            .insert(keys::LAST_RUN_STATUS.to_string(), value);
    }

    // ------------------------------------------------------------------
    // last_run_error
    // ------------------------------------------------------------------

    pub fn last_run_error(&self) -> Option<String> {
        self.runtime_str(|m| m.last_run_error.as_ref(), keys::LAST_RUN_ERROR)
    }

    pub fn set_last_run_error(&mut self, value: impl Into<String>) {
        let value = value.into();
        self.runtime_metadata_mut().last_run_error = Some(value.clone());
        self.metadata
            .insert(keys::LAST_RUN_ERROR.to_string(), value);
    }

    pub fn clear_last_run_error(&mut self) {
        if let Some(rm) = self.runtime_metadata.as_mut() {
            rm.last_run_error = None;
        }
        self.metadata.remove(keys::LAST_RUN_ERROR);
        self.prune_runtime_metadata();
    }

    // ------------------------------------------------------------------
    // provider_name
    // ------------------------------------------------------------------

    pub fn provider_name(&self) -> Option<String> {
        self.runtime_str(|m| m.provider_name.as_ref(), keys::PROVIDER_NAME)
    }

    pub fn set_provider_name(&mut self, value: impl Into<String>) {
        let value = value.into();
        self.runtime_metadata_mut().provider_name = Some(value.clone());
        self.metadata.insert(keys::PROVIDER_NAME.to_string(), value);
    }

    // ------------------------------------------------------------------
    // pending_injected_messages (JSON-string on the legacy map)
    // ------------------------------------------------------------------

    /// Read the pending injected messages, decoding the legacy JSON-string form
    /// defensively. Malformed legacy JSON yields `None` (never a panic).
    pub fn pending_injected_messages(&self) -> Option<Vec<serde_json::Value>> {
        if let Some(messages) = self
            .runtime_metadata
            .as_ref()
            .and_then(|m| m.pending_injected_messages.clone())
        {
            return Some(messages);
        }
        let raw = self.metadata.get(keys::PENDING_INJECTED_MESSAGES)?;
        match serde_json::from_str::<Vec<serde_json::Value>>(raw) {
            Ok(messages) => Some(messages),
            Err(_) => Some(Vec::new()),
        }
    }

    /// Set pending injected messages on both planes. The legacy mirror stores
    /// the JSON-encoded string form to preserve byte-for-byte compatibility.
    pub fn set_pending_injected_messages(&mut self, messages: Vec<serde_json::Value>) {
        let serialized = serde_json::to_string(&messages).unwrap_or_else(|_| "[]".to_string());
        self.runtime_metadata_mut().pending_injected_messages = Some(messages);
        self.metadata
            .insert(keys::PENDING_INJECTED_MESSAGES.to_string(), serialized);
    }

    /// True when there are queued injected messages on either plane.
    pub fn has_pending_injected_messages(&self) -> bool {
        if self
            .runtime_metadata
            .as_ref()
            .is_some_and(|m| m.pending_injected_messages.is_some())
        {
            return true;
        }
        self.metadata.contains_key(keys::PENDING_INJECTED_MESSAGES)
    }

    /// Take and clear pending injected messages from both planes.
    pub fn take_pending_injected_messages(&mut self) -> Option<Vec<serde_json::Value>> {
        let value = self.pending_injected_messages();
        self.clear_pending_injected_messages();
        value
    }

    pub fn clear_pending_injected_messages(&mut self) {
        if let Some(rm) = self.runtime_metadata.as_mut() {
            rm.pending_injected_messages = None;
        }
        self.metadata.remove(keys::PENDING_INJECTED_MESSAGES);
        self.prune_runtime_metadata();
    }

    // ------------------------------------------------------------------
    // selected_skill_ids (JSON-array-string on the legacy map)
    // ------------------------------------------------------------------

    /// Read selected skill ids. The typed field is preferred; the legacy
    /// fallback parses the stored JSON-array string defensively (malformed →
    /// `None`).
    pub fn selected_skill_ids(&self) -> Option<Vec<String>> {
        if let Some(ids) = self
            .runtime_metadata
            .as_ref()
            .and_then(|m| m.selected_skill_ids.clone())
        {
            return Some(ids);
        }
        let raw = self.metadata.get(keys::SELECTED_SKILL_IDS)?;
        serde_json::from_str::<Vec<String>>(raw).ok()
    }

    /// Set selected skill ids on both planes. The legacy mirror stores the
    /// JSON-array string form.
    pub fn set_selected_skill_ids(&mut self, ids: Vec<String>) {
        let serialized = serde_json::to_string(&ids).unwrap_or_else(|_| "[]".to_string());
        self.runtime_metadata_mut().selected_skill_ids = Some(ids);
        self.metadata
            .insert(keys::SELECTED_SKILL_IDS.to_string(), serialized);
    }

    pub fn clear_selected_skill_ids(&mut self) {
        if let Some(rm) = self.runtime_metadata.as_mut() {
            rm.selected_skill_ids = None;
        }
        self.metadata.remove(keys::SELECTED_SKILL_IDS);
        self.prune_runtime_metadata();
    }

    // ------------------------------------------------------------------
    // skill_mode (canonical) / mode (legacy duplicate)
    // ------------------------------------------------------------------

    /// Read the skill mode. Resolution order: typed field, then legacy
    /// `skill_mode` key, then the historical `mode` key. When both legacy keys
    /// are present and disagree, a warning is logged and `skill_mode` wins.
    pub fn skill_mode(&self) -> Option<String> {
        if let Some(mode) = self
            .runtime_metadata
            .as_ref()
            .and_then(|m| m.skill_mode.clone())
        {
            return Some(mode);
        }
        let canonical = self.metadata.get(keys::SKILL_MODE);
        let legacy = self.metadata.get(keys::SKILL_MODE_LEGACY);
        if let (Some(canonical), Some(legacy)) = (canonical, legacy) {
            if canonical != legacy {
                tracing::warn!(
                    canonical = %canonical,
                    legacy = %legacy,
                    "session metadata has divergent skill_mode and legacy mode keys; preferring skill_mode"
                );
            }
        }
        canonical.or(legacy).cloned()
    }

    /// Set the skill mode. Writes the typed field and the canonical
    /// `skill_mode` legacy key (the legacy `mode` key is never written).
    pub fn set_skill_mode(&mut self, value: impl Into<String>) {
        let value = value.into();
        self.runtime_metadata_mut().skill_mode = Some(value.clone());
        self.metadata.insert(keys::SKILL_MODE.to_string(), value);
    }

    pub fn clear_skill_mode(&mut self) {
        if let Some(rm) = self.runtime_metadata.as_mut() {
            rm.skill_mode = None;
        }
        self.metadata.remove(keys::SKILL_MODE);
        self.prune_runtime_metadata();
    }

    // ------------------------------------------------------------------
    // reasoning_effort (string form)
    // ------------------------------------------------------------------

    pub fn reasoning_effort_meta(&self) -> Option<String> {
        self.runtime_str(|m| m.reasoning_effort.as_ref(), keys::REASONING_EFFORT)
    }

    pub fn set_reasoning_effort_meta(&mut self, value: impl Into<String>) {
        let value = value.into();
        self.runtime_metadata_mut().reasoning_effort = Some(value.clone());
        self.metadata
            .insert(keys::REASONING_EFFORT.to_string(), value);
    }

    // ------------------------------------------------------------------
    // enhance_prompt
    // ------------------------------------------------------------------

    pub fn enhance_prompt(&self) -> Option<String> {
        self.runtime_str(|m| m.enhance_prompt.as_ref(), keys::ENHANCE_PROMPT)
    }

    pub fn set_enhance_prompt(&mut self, value: impl Into<String>) {
        let value = value.into();
        self.runtime_metadata_mut().enhance_prompt = Some(value.clone());
        self.metadata
            .insert(keys::ENHANCE_PROMPT.to_string(), value);
    }

    pub fn clear_enhance_prompt(&mut self) {
        if let Some(rm) = self.runtime_metadata.as_mut() {
            rm.enhance_prompt = None;
        }
        self.metadata.remove(keys::ENHANCE_PROMPT);
        self.prune_runtime_metadata();
    }

    // ------------------------------------------------------------------
    // task_list_version / todo_list_version (string form)
    // ------------------------------------------------------------------

    pub fn task_list_version_meta(&self) -> Option<String> {
        self.runtime_str(|m| m.task_list_version.as_ref(), keys::TASK_LIST_VERSION)
    }

    pub fn set_task_list_version_meta(&mut self, value: impl Into<String>) {
        let value = value.into();
        self.runtime_metadata_mut().task_list_version = Some(value.clone());
        self.metadata
            .insert(keys::TASK_LIST_VERSION.to_string(), value);
    }

    pub fn todo_list_version_meta(&self) -> Option<String> {
        self.runtime_str(|m| m.todo_list_version.as_ref(), keys::TODO_LIST_VERSION)
    }

    pub fn set_todo_list_version_meta(&mut self, value: impl Into<String>) {
        let value = value.into();
        self.runtime_metadata_mut().todo_list_version = Some(value.clone());
        self.metadata
            .insert(keys::TODO_LIST_VERSION.to_string(), value);
    }

    // ------------------------------------------------------------------
    // workspace_path
    // ------------------------------------------------------------------

    pub fn workspace_path_meta(&self) -> Option<String> {
        self.runtime_str(|m| m.workspace_path.as_ref(), keys::WORKSPACE_PATH)
    }

    pub fn set_workspace_path_meta(&mut self, value: impl Into<String>) {
        let value = value.into();
        self.runtime_metadata_mut().workspace_path = Some(value.clone());
        self.metadata
            .insert(keys::WORKSPACE_PATH.to_string(), value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Hand-written OLD-format session JSON: only the legacy `metadata` map,
    /// no `runtime_metadata` field. Includes a JSON-string
    /// `pending_injected_messages`, a JSON-array-string `selected_skill_ids`,
    /// the legacy `mode` key (not `skill_mode`), and open-ended keys that must
    /// survive untouched.
    const OLD_FORMAT_SESSION: &str = r#"{
        "id": "sess-old",
        "messages": [],
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
        "model": "gpt-test",
        "metadata": {
            "subagent_type": "researcher",
            "last_run_status": "completed",
            "provider_name": "openai",
            "workspace_path": "/tmp/ws",
            "pending_injected_messages": "[{\"content\":\"hello\"},{\"content\":\"world\"}]",
            "selected_skill_ids": "[\"pdf\",\"web\"]",
            "mode": "ask",
            "task_list_version": "7",
            "gold_config": "{\"goal\":\"x\"}",
            "a2a.foo": "bar",
            "responses.previous_response_id": "resp-123"
        }
    }"#;

    #[test]
    fn old_format_deserializes_and_typed_getters_fall_back() {
        let session: Session = serde_json::from_str(OLD_FORMAT_SESSION).unwrap();
        // No runtime_metadata in old JSON.
        assert!(session.runtime_metadata.is_none());

        // Typed getters resolve via the legacy metadata fallback.
        assert_eq!(session.subagent_type().as_deref(), Some("researcher"));
        assert_eq!(session.last_run_status().as_deref(), Some("completed"));
        assert_eq!(session.provider_name().as_deref(), Some("openai"));
        assert_eq!(session.workspace_path_meta().as_deref(), Some("/tmp/ws"));
        assert_eq!(session.task_list_version_meta().as_deref(), Some("7"));

        // skill_mode falls back to the legacy `mode` key.
        assert_eq!(session.skill_mode().as_deref(), Some("ask"));

        // JSON-string pending_injected_messages decodes into the typed vector.
        let pending = session
            .pending_injected_messages()
            .expect("pending should decode");
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0]["content"], "hello");
        assert_eq!(pending[1]["content"], "world");

        // JSON-array-string selected_skill_ids decodes.
        let ids = session
            .selected_skill_ids()
            .expect("skill ids should decode");
        assert_eq!(ids, vec!["pdf".to_string(), "web".to_string()]);
    }

    #[test]
    fn setters_dual_write_both_planes() {
        let mut session: Session = serde_json::from_str(OLD_FORMAT_SESSION).unwrap();

        session.set_subagent_type("planner");
        // Typed field updated.
        assert_eq!(
            session
                .runtime_metadata
                .as_ref()
                .and_then(|m| m.subagent_type.as_deref()),
            Some("planner")
        );
        // Legacy string mirror updated.
        assert_eq!(
            session.metadata.get("subagent_type").map(String::as_str),
            Some("planner")
        );

        session.set_skill_mode("code");
        assert_eq!(
            session
                .runtime_metadata
                .as_ref()
                .and_then(|m| m.skill_mode.as_deref()),
            Some("code")
        );
        assert_eq!(
            session.metadata.get("skill_mode").map(String::as_str),
            Some("code")
        );
        // Typed/canonical skill_mode now wins over the legacy `mode` key.
        assert_eq!(session.skill_mode().as_deref(), Some("code"));

        // pending_injected_messages dual-writes typed vec + JSON string mirror.
        session.set_pending_injected_messages(vec![json!({"content": "again"})]);
        assert_eq!(
            session
                .runtime_metadata
                .as_ref()
                .and_then(|m| m.pending_injected_messages.as_ref())
                .map(Vec::len),
            Some(1)
        );
        let raw = session.metadata.get("pending_injected_messages").unwrap();
        let decoded: Vec<serde_json::Value> = serde_json::from_str(raw).unwrap();
        assert_eq!(decoded[0]["content"], "again");

        // selected_skill_ids dual-writes typed vec + JSON string mirror.
        session.set_selected_skill_ids(vec!["audio".to_string()]);
        let raw = session.metadata.get("selected_skill_ids").unwrap();
        let decoded: Vec<String> = serde_json::from_str(raw).unwrap();
        assert_eq!(decoded, vec!["audio".to_string()]);
    }

    #[test]
    fn round_trip_preserves_open_ended_and_typed_values() {
        let mut session: Session = serde_json::from_str(OLD_FORMAT_SESSION).unwrap();
        session.set_subagent_type("planner");
        session.set_skill_mode("code");

        // Serialize -> deserialize again.
        let serialized = serde_json::to_string(&session).unwrap();
        let restored: Session = serde_json::from_str(&serialized).unwrap();

        // Open-ended keys survive untouched.
        assert_eq!(
            restored.metadata.get("gold_config").map(String::as_str),
            Some("{\"goal\":\"x\"}")
        );
        assert_eq!(
            restored.metadata.get("a2a.foo").map(String::as_str),
            Some("bar")
        );
        assert_eq!(
            restored
                .metadata
                .get("responses.previous_response_id")
                .map(String::as_str),
            Some("resp-123")
        );

        // Typed values round-trip through the new runtime_metadata object.
        assert!(restored.runtime_metadata.is_some());
        assert_eq!(restored.subagent_type().as_deref(), Some("planner"));
        assert_eq!(restored.skill_mode().as_deref(), Some("code"));
        // And the legacy mirror is still there for un-migrated readers.
        assert_eq!(
            restored.metadata.get("subagent_type").map(String::as_str),
            Some("planner")
        );
    }

    #[test]
    fn malformed_pending_injected_messages_never_panics() {
        let json = r#"{
            "id": "sess-bad",
            "messages": [],
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T00:00:00Z",
            "model": "gpt-test",
            "metadata": { "pending_injected_messages": "not-json{" }
        }"#;
        let session: Session = serde_json::from_str(json).unwrap();
        // Defensive parse: malformed legacy JSON => empty vec, not a panic.
        assert_eq!(session.pending_injected_messages(), Some(Vec::new()));
        assert!(session.has_pending_injected_messages());
    }

    #[test]
    fn divergent_skill_mode_and_mode_prefers_skill_mode() {
        let json = r#"{
            "id": "sess-div",
            "messages": [],
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T00:00:00Z",
            "model": "gpt-test",
            "metadata": { "skill_mode": "code", "mode": "ask" }
        }"#;
        let session: Session = serde_json::from_str(json).unwrap();
        assert_eq!(session.skill_mode().as_deref(), Some("code"));
    }

    #[test]
    fn take_pending_clears_both_planes() {
        let mut session: Session = serde_json::from_str(OLD_FORMAT_SESSION).unwrap();
        let taken = session.take_pending_injected_messages().unwrap();
        assert_eq!(taken.len(), 2);
        assert!(!session.has_pending_injected_messages());
        assert!(!session.metadata.contains_key("pending_injected_messages"));
        assert!(session
            .runtime_metadata
            .as_ref()
            .map(|m| m.pending_injected_messages.is_none())
            .unwrap_or(true));
    }

    #[test]
    fn empty_runtime_metadata_not_serialized() {
        let session = Session::new("sess-empty", "gpt-test");
        assert!(session.runtime_metadata.is_none());
        let json = serde_json::to_string(&session).unwrap();
        assert!(
            !json.contains("runtime_metadata"),
            "absent runtime_metadata must not serialize: {json}"
        );
    }
}
