//! Bridge between structured [`AgentRuntimeState`] and session metadata.
//!
//! During the migration period, runtime state is written to both the
//! structured `session.agent_runtime_state` field and the legacy
//! `session.metadata["agent.runtime.state"]` key. The read path always
//! prefers the structured field.

use bamboo_agent_core::Session;
use bamboo_domain::AgentRuntimeState;

const METADATA_KEY: &str = "agent.runtime.state";

/// Read `AgentRuntimeState` from session.
///
/// Tries the structured field first, falls back to the metadata key.
pub fn read_runtime_state(session: &Session) -> Option<AgentRuntimeState> {
    session.agent_runtime_state.clone().or_else(|| {
        session
            .metadata
            .get(METADATA_KEY)
            .and_then(|raw| serde_json::from_str::<AgentRuntimeState>(raw).ok())
    })
}

/// Write `AgentRuntimeState` to session.
///
/// Dual-writes to both the structured field and the metadata key
/// for backward compatibility during migration.
pub fn write_runtime_state(session: &mut Session, state: &AgentRuntimeState) {
    session.agent_runtime_state = Some(state.clone());
    if let Ok(serialized) = serde_json::to_string(state) {
        session
            .metadata
            .insert(METADATA_KEY.to_string(), serialized);
    }
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
        state.llm.provider_name = session.metadata.get("provider_name").cloned();
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

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::AgentStatusState;

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
    fn write_dual_writes() {
        let mut session = test_session();
        let state = AgentRuntimeState::new("run-3");

        write_runtime_state(&mut session, &state);

        assert!(session.agent_runtime_state.is_some());
        assert!(session.metadata.contains_key(METADATA_KEY));
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
}
