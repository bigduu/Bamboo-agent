//! Bridge between structured [`AgentRuntimeState`] and session metadata.
//!
//! The read path prefers the structured `session.agent_runtime_state` field
//! and falls back to the legacy `session.metadata["agent.runtime.state"]` key.
//! The write path writes only to the structured field; the legacy metadata
//! mirror is no longer maintained.

use std::sync::Arc;

use bamboo_agent_core::Session;
use bamboo_domain::AgentRuntimeState;

const METADATA_KEY: &str = "agent.runtime.state";
#[cfg(test)]
const PENDING_INJECTED_MESSAGES_KEY: &str = "pending_injected_messages";

/// Read `AgentRuntimeState` from session.
///
/// Tries the structured field first, falls back to the metadata key.
#[allow(dead_code)]
pub fn read_runtime_state(session: &Session) -> Option<AgentRuntimeState> {
    session.agent_runtime_state.clone().or_else(|| {
        session
            .metadata
            .get(METADATA_KEY)
            .and_then(|raw| serde_json::from_str::<AgentRuntimeState>(raw).ok())
    })
}

/// Write `AgentRuntimeState` to the structured session field.
///
/// Only writes to `session.agent_runtime_state`. The legacy metadata mirror
/// was removed after the migration completed.
pub fn write_runtime_state(session: &mut Session, state: &AgentRuntimeState) {
    session.agent_runtime_state = Some(state.clone());
}

/// Sync runtime state fields from existing metadata keys.
///
/// This extracts values that are currently stored as individual metadata
/// entries into the structured runtime state.
pub fn sync_from_metadata(session: &Session, state: &mut AgentRuntimeState) {
    // LLM info
    if state.llm.model_name.is_none() {
        state.llm.model_name = Some(session.model.clone());
    }
    if state.llm.provider_name.is_none() {
        state.llm.provider_name = session.provider_name();
    }
    if state.llm.responses_previous_id.is_none() {
        state.llm.responses_previous_id = session
            .metadata
            .get("responses.previous_response_id")
            .cloned();
    }

    // Prompt info
    if state.prompt.composer_version.is_none() {
        state.prompt.composer_version = session
            .metadata
            .get("runtime_prompt_composer_version")
            .cloned();
    }
    if state.prompt.section_flags.is_none() {
        state.prompt.section_flags = session
            .metadata
            .get("runtime_prompt_component_flags")
            .cloned();
    }
    if state.prompt.section_lengths.is_none() {
        state.prompt.section_lengths = session
            .metadata
            .get("runtime_prompt_component_lengths")
            .cloned();
    }
    if state.prompt.section_layout.is_none() {
        state.prompt.section_layout = session
            .metadata
            .get("runtime_prompt_section_layout")
            .cloned();
    }
}

/// Result of a turn-boundary disk refresh: how many injected messages were
/// merged, and the live per-session `bypass_permissions` flag as it stands on
/// disk (the authoritative writer is `PATCH /sessions`).
#[derive(Debug, Default, Clone, Copy)]
pub struct TurnBoundaryRefresh {
    pub merged: usize,
    /// `None` when there was no storage / no on-disk session to read.
    pub disk_bypass_permissions: Option<bool>,
}

/// Merge any queued follow-up messages that were injected via `send_message`
/// while a session is running.
///
/// Thin wrapper over [`refresh_turn_boundary_from_disk`] kept for callers that
/// only need the merge count. Returns the number of messages merged.
pub async fn merge_pending_injected_messages(
    session: &mut Session,
    storage: Option<&Arc<dyn bamboo_agent_core::storage::Storage>>,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
) -> usize {
    refresh_turn_boundary_from_disk(session, storage, persistence)
        .await
        .merged
}

/// Turn-boundary refresh from the on-disk session: a SINGLE load that both
/// merges queued `send_message` injections AND reads the live
/// `bypass_permissions` flag.
///
/// The bypass read is what makes a mid-run `PATCH /sessions {bypass_permissions}`
/// take effect on the CURRENT run: the run owns a `Session` value taken at spawn
/// and never otherwise re-reads storage, so the caller applies the returned
/// `disk_bypass_permissions` to the live runtime state at each round boundary.
/// The flag is folded into the existing per-round load so large parent sessions
/// aren't deserialized twice.
pub async fn refresh_turn_boundary_from_disk(
    session: &mut Session,
    storage: Option<&Arc<dyn bamboo_agent_core::storage::Storage>>,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
) -> TurnBoundaryRefresh {
    let Some(storage) = storage else {
        return TurnBoundaryRefresh::default();
    };

    let Ok(Some(latest)) = storage.load_session(&session.id).await else {
        return TurnBoundaryRefresh::default();
    };

    let disk_bypass_permissions = Some(
        latest
            .agent_runtime_state
            .as_ref()
            .is_some_and(|state| state.bypass_permissions),
    );

    // Read via the typed accessor (prefers `runtime_metadata`, falls back to the
    // legacy `pending_injected_messages` JSON string; defensive on malformed).
    let Some(messages) = latest.pending_injected_messages() else {
        return TurnBoundaryRefresh {
            merged: 0,
            disk_bypass_permissions,
        };
    };

    let mut merged = 0usize;
    for msg in messages {
        if let Some(content) = msg.get("content").and_then(|v| v.as_str()) {
            session.add_message(bamboo_agent_core::Message::user(content.to_string()));
            merged += 1;
        }
    }

    if merged > 0 {
        // Clear on both planes (typed field + legacy string mirror).
        session.clear_pending_injected_messages();
        session.updated_at = chrono::Utc::now();

        if let Some(persistence) = persistence {
            if let Err(error) = persistence.save_runtime_session(session).await {
                tracing::warn!(
                    "[{}] Failed to persist pending injected message cleanup: {}",
                    session.id,
                    error
                );
            }
        }

        tracing::info!(
            "[{}] Merged {} injected message(s) from queued send_message at turn boundary",
            session.id,
            merged
        );
    }

    TurnBoundaryRefresh {
        merged,
        disk_bypass_permissions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::storage::Storage;
    use bamboo_domain::AgentStatusState;
    use std::collections::HashMap;
    use tokio::sync::RwLock;

    fn test_session() -> Session {
        Session::new("test-session", "test-model")
    }

    #[test]
    fn read_from_structured_field() {
        let mut session = test_session();
        let mut state = AgentRuntimeState::new("run-1");
        state.status = AgentStatusState::Running;
        session.agent_runtime_state = Some(state.clone());

        let read = read_runtime_state(&session).unwrap();
        assert_eq!(read.status, AgentStatusState::Running);
        assert_eq!(read.run_id, "run-1");
    }

    #[test]
    fn read_from_metadata_fallback() {
        let mut session = test_session();
        let state = AgentRuntimeState::new("run-2");
        session.metadata.insert(
            METADATA_KEY.to_string(),
            serde_json::to_string(&state).unwrap(),
        );

        let read = read_runtime_state(&session).unwrap();
        assert_eq!(read.run_id, "run-2");
    }

    #[test]
    fn structured_field_takes_priority() {
        let mut session = test_session();
        let mut state1 = AgentRuntimeState::new("from-field");
        state1.status = AgentStatusState::Running;
        session.agent_runtime_state = Some(state1);

        let mut state2 = AgentRuntimeState::new("from-metadata");
        state2.status = AgentStatusState::Completed;
        session.metadata.insert(
            METADATA_KEY.to_string(),
            serde_json::to_string(&state2).unwrap(),
        );

        let read = read_runtime_state(&session).unwrap();
        assert_eq!(read.run_id, "from-field");
        assert_eq!(read.status, AgentStatusState::Running);
    }

    #[test]
    fn read_returns_none_when_empty() {
        let session = test_session();
        assert!(read_runtime_state(&session).is_none());
    }

    #[test]
    fn write_only_structured_field() {
        let mut session = test_session();
        let state = AgentRuntimeState::new("run-3");

        write_runtime_state(&mut session, &state);

        assert!(session.agent_runtime_state.is_some());
        // Legacy metadata mirror is no longer written
        assert!(!session.metadata.contains_key(METADATA_KEY));
        assert_eq!(
            session.agent_runtime_state.as_ref().unwrap().run_id,
            "run-3"
        );
    }

    #[test]
    fn sync_extracts_model_name() {
        let mut session = test_session();
        session.model = "gpt-4o".to_string();
        session.metadata.insert(
            "responses.previous_response_id".to_string(),
            "resp-123".to_string(),
        );

        let mut state = AgentRuntimeState::new("run-4");
        sync_from_metadata(&session, &mut state);

        assert_eq!(state.llm.model_name, Some("gpt-4o".to_string()));
        assert_eq!(
            state.llm.responses_previous_id,
            Some("resp-123".to_string())
        );
    }

    // --- Pending injected message tests ---

    #[derive(Default)]
    struct TestStorage {
        sessions: RwLock<HashMap<String, Session>>,
    }

    #[async_trait::async_trait]
    impl Storage for TestStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            self.sessions
                .write()
                .await
                .insert(session.id.clone(), session.clone());
            Ok(())
        }

        async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            Ok(self.sessions.read().await.get(session_id).cloned())
        }

        async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
            Ok(self.sessions.write().await.remove(session_id).is_some())
        }
    }

    struct TestPersistence(Arc<dyn Storage>);

    #[async_trait::async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for TestPersistence {
        async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            self.0.save_session(session).await
        }
    }

    #[tokio::test]
    async fn merge_pending_injected_messages_merges_and_clears() {
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
            Arc::new(TestPersistence(storage.clone()));
        let mut persisted = Session::new_child("child-merge", "parent", "model", "Child");
        persisted.add_message(bamboo_agent_core::Message::system("system"));
        persisted.add_message(bamboo_agent_core::Message::user("original task"));
        persisted.metadata.insert(
            PENDING_INJECTED_MESSAGES_KEY.to_string(),
            serde_json::json!([
                {
                    "content": "queued correction",
                    "created_at": chrono::Utc::now(),
                }
            ])
            .to_string(),
        );
        storage
            .save_session(&persisted)
            .await
            .expect("persisted child should be saved");

        let mut running = persisted.clone();
        running.metadata.remove(PENDING_INJECTED_MESSAGES_KEY);

        let count =
            merge_pending_injected_messages(&mut running, Some(&storage), Some(&persistence)).await;

        assert_eq!(count, 1);
        assert_eq!(
            running
                .messages
                .last()
                .map(|message| message.content.as_str()),
            Some("queued correction")
        );
        assert!(!running.metadata.contains_key(PENDING_INJECTED_MESSAGES_KEY));
        let saved = storage
            .load_session("child-merge")
            .await
            .expect("load should succeed")
            .expect("session should exist");
        assert!(!saved.metadata.contains_key(PENDING_INJECTED_MESSAGES_KEY));

        // Second merge is a no-op
        let count2 =
            merge_pending_injected_messages(&mut running, Some(&storage), Some(&persistence)).await;
        assert_eq!(count2, 0);
    }

    #[tokio::test]
    async fn merge_pending_injected_messages_returns_zero_without_storage() {
        let mut session = test_session();
        let count = merge_pending_injected_messages(&mut session, None, None).await;
        assert_eq!(count, 0);
    }

    // #540: the turn-boundary refresh reports the live on-disk bypass flag so
    // the pipeline can adopt a mid-run PATCH into the running loop's state.
    #[tokio::test]
    async fn refresh_turn_boundary_reports_disk_bypass_permissions() {
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());

        // Persist a session with bypass ON.
        let mut persisted = Session::new("bypass-live", "model");
        let mut state = AgentRuntimeState::new("run-x");
        state.bypass_permissions = true;
        persisted.agent_runtime_state = Some(state);
        storage.save_session(&persisted).await.unwrap();

        // A running loop holds a stale copy with bypass OFF.
        let mut running = Session::new("bypass-live", "model");
        running.agent_runtime_state = Some(AgentRuntimeState::new("run-x"));

        let refresh = refresh_turn_boundary_from_disk(&mut running, Some(&storage), None).await;
        assert_eq!(refresh.disk_bypass_permissions, Some(true));
        assert_eq!(refresh.merged, 0);
    }

    #[tokio::test]
    async fn refresh_turn_boundary_reports_none_without_storage() {
        let mut session = test_session();
        let refresh = refresh_turn_boundary_from_disk(&mut session, None, None).await;
        assert_eq!(refresh.disk_bypass_permissions, None);
        assert_eq!(refresh.merged, 0);
    }
}
