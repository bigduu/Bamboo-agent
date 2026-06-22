use super::request::{optional_non_empty, resolve_session_id, validate_and_normalize_model};
use super::sync_runtime_workspace;
use bamboo_agent_core::Session;

use bamboo_engine::session_app::chat::{
    clear_skill_runtime_state, resolve_base_prompt,
    resolve_copilot_conclusion_with_options_enhancement, resolve_enhance_prompt,
    resolve_selected_skill_ids, resolve_workspace_path,
};

/// Regression: `/goal off` and `/goal clear` must clear the stale runtime
/// `goal.state` (status / continuation budget / double-check eval history).
/// Previously the cleanup was gated behind `should_resume`, so only
/// `/goal <prompt>` (set-prompt) reached it and off/clear left it behind —
/// surfacing a stale "complete" badge over the history API.
#[actix_web::test]
async fn goal_off_and_clear_remove_stale_goal_state() {
    use crate::AppState;
    use bamboo_engine::session_app::chat::GoalCommand;
    use tempfile::tempdir;

    const STALE_GOAL_STATE: &str = r#"{"objective":"ship it","status":"complete","continuation_count":2,"eval_history":[{"checkpoint":"terminal","iteration":3,"decision":"achieved","confidence":"high","reasoning":"done","recorded_at":"t"}],"created_at":"t","updated_at":"t"}"#;

    let temp_dir = tempdir().expect("tempdir");
    bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state");

    for (session_id, cmd) in [
        ("goal-off-test", GoalCommand::Off),
        ("goal-clear-test", GoalCommand::Clear),
    ] {
        // Seed a session carrying a stale, finished goal.state + a spread of
        // stale `gold.*` runtime snapshot keys (incl. ones that were NOT on the
        // old explicit removal list), plus the config key which must survive.
        let mut session = Session::new(session_id, "model");
        session
            .metadata
            .insert("goal.state".to_string(), STALE_GOAL_STATE.to_string());
        for (k, v) in [
            ("gold.evaluation_count", "7"),
            ("gold.last_reasoning", "old reasoning"),
            ("gold.last_checkpoint", "terminal"),
            ("gold.last_iteration", "7"),
            ("gold.last_decision", "achieved"),
        ] {
            session.metadata.insert(k.to_string(), v.to_string());
        }
        state.save_and_cache_session(&mut session).await;

        let _ = super::handle_goal_command(&state, session_id, &cmd).await;

        let reloaded = state
            .storage
            .load_session(session_id)
            .await
            .expect("load")
            .expect("session exists");
        assert!(
            !reloaded.metadata.contains_key("goal.state"),
            "goal.state must be cleared after /goal {cmd:?}"
        );
        assert!(
            !reloaded.metadata.keys().any(|k| k.starts_with("gold.")),
            "no gold.* runtime keys may remain after /goal {cmd:?}"
        );
        // The config (key `gold_config`, no dot) is managed by the handler and
        // must still be present — the prefix wipe must not remove it.
        assert!(
            reloaded.metadata.contains_key("gold_config"),
            "gold_config must be preserved after /goal {cmd:?}"
        );
    }
}

#[test]
fn validate_and_normalize_model_rejects_empty_values() {
    let response = validate_and_normalize_model("   ").expect_err("model should be required");
    assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
}

#[test]
fn validate_and_normalize_model_trims_whitespace() {
    let model = validate_and_normalize_model("  gpt-5  ").expect("model should be accepted");
    assert_eq!(model, "gpt-5");
}

#[test]
fn optional_non_empty_returns_none_for_blank_string() {
    let value = optional_non_empty(Some("   "));
    assert_eq!(value, None);
}

#[test]
fn resolve_session_id_uses_provided_value_without_trimming() {
    let session_id = resolve_session_id(Some("  existing-id  "));
    assert_eq!(session_id, "  existing-id  ");
}

#[test]
fn resolve_base_prompt_prefers_request_and_persists_metadata() {
    let mut session = Session::new("session-1", "model");
    let base_prompt = resolve_base_prompt(&mut session, Some("request prompt"), "", "fallback");
    assert_eq!(base_prompt, "request prompt");
    assert_eq!(
        session
            .metadata
            .get("base_system_prompt")
            .map(String::as_str),
        Some("request prompt")
    );
}

#[test]
fn resolve_base_prompt_falls_back_to_existing_metadata() {
    let mut session = Session::new("session-1", "model");
    session.metadata.insert(
        "base_system_prompt".to_string(),
        "stored prompt".to_string(),
    );

    let base_prompt = resolve_base_prompt(&mut session, None, "", "fallback");
    assert_eq!(base_prompt, "stored prompt");
}

#[test]
fn resolve_base_prompt_falls_back_to_existing_system_message_before_global_default() {
    let mut session = Session::new("session-1", "model");
    session.add_message(bamboo_agent_core::Message::system("Existing system"));

    let base_prompt = resolve_base_prompt(&mut session, None, "", "global default");
    assert_eq!(base_prompt, "Existing system");
    assert_eq!(
        session
            .metadata
            .get("base_system_prompt")
            .map(String::as_str),
        Some("Existing system")
    );
}

#[test]
fn resolve_base_prompt_uses_global_default_when_missing_everywhere() {
    let mut session = Session::new("session-1", "model");
    let base_prompt = resolve_base_prompt(&mut session, None, "", "global default");
    assert_eq!(base_prompt, "global default");
    assert_eq!(
        session
            .metadata
            .get("base_system_prompt")
            .map(String::as_str),
        Some("global default")
    );
}

#[test]
fn resolve_workspace_path_uses_request_then_metadata() {
    let mut session = Session::new("session-1", "model");

    let from_request = resolve_workspace_path(&mut session, Some("/tmp/workspace"), None);
    assert_eq!(from_request.as_deref(), Some("/tmp/workspace"));
    assert_eq!(
        session.metadata.get("workspace_path").map(String::as_str),
        Some("/tmp/workspace")
    );

    let from_metadata = resolve_workspace_path(&mut session, None, None);
    assert_eq!(from_metadata.as_deref(), Some("/tmp/workspace"));
}

// NOTE: the default-work-area disk fallback used to be tested here, but it's now
// gated on NO workspace provider being registered (#38/#131) — and the provider
// is a process-global first-wins OnceLock that sibling AppState tests populate,
// so a fallback assertion can't be deterministic in the server test binary. The
// disk fallback is unit-tested directly + deterministically in bamboo-engine's
// session_app::chat (default_workspace_from_data_dir_reads_configured_work_area).

#[test]
fn sync_runtime_workspace_persists_workspace_for_tools() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let workspace = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace should exist");
    let session_id = "session-runtime-workspace";

    sync_runtime_workspace(session_id, Some(workspace.to_string_lossy().as_ref()));

    let resolved = bamboo_tools::tools::workspace_state::get_workspace(session_id)
        .expect("workspace should be stored");
    assert_eq!(resolved, workspace.canonicalize().unwrap_or(workspace));
}

#[test]
fn resolve_enhance_prompt_stores_and_clears_metadata() {
    let mut session = Session::new("session-1", "model");

    resolve_enhance_prompt(&mut session, Some("Extra guidance"));
    assert_eq!(
        session.metadata.get("enhance_prompt").map(String::as_str),
        Some("Extra guidance")
    );

    resolve_enhance_prompt(&mut session, None);
    assert!(!session.metadata.contains_key("enhance_prompt"));
}

#[test]
fn resolve_copilot_conclusion_with_options_enhancement_enabled_stores_and_clears_metadata() {
    let mut session = Session::new("session-1", "model");

    resolve_copilot_conclusion_with_options_enhancement(&mut session, Some(true));
    assert_eq!(
        session
            .metadata
            .get("copilot_conclusion_with_options_enhancement_enabled")
            .map(String::as_str),
        Some("true")
    );

    resolve_copilot_conclusion_with_options_enhancement(&mut session, Some(false));
    assert_eq!(
        session
            .metadata
            .get("copilot_conclusion_with_options_enhancement_enabled")
            .map(String::as_str),
        Some("false")
    );

    resolve_copilot_conclusion_with_options_enhancement(&mut session, None);
    assert!(!session
        .metadata
        .contains_key("copilot_conclusion_with_options_enhancement_enabled"));
}

#[test]
fn resolve_selected_skill_ids_prefers_structured_request_and_persists_as_json() {
    let mut session = Session::new("session-1", "model");
    resolve_selected_skill_ids(
        &mut session,
        Some(&[
            "pdf".to_string(),
            "skill-creator".to_string(),
            "pdf".to_string(),
        ]),
        "hello",
    );

    let stored = session
        .metadata
        .get("selected_skill_ids")
        .map(String::as_str);
    assert_eq!(stored, Some("[\"pdf\",\"skill-creator\"]"));
}

#[test]
fn resolve_selected_skill_ids_falls_back_to_legacy_hint_when_structured_field_absent() {
    let mut session = Session::new("session-1", "model");
    resolve_selected_skill_ids(
        &mut session,
        None,
        "[User explicitly selected skill: PDF Skill (ID: pdf)]\n\nPlease parse this file",
    );

    let stored = session
        .metadata
        .get("selected_skill_ids")
        .map(String::as_str);
    assert_eq!(stored, Some("[\"pdf\"]"));
}

#[test]
fn resolve_selected_skill_ids_clears_stale_metadata_when_no_selection_provided() {
    let mut session = Session::new("session-1", "model");
    session
        .metadata
        .insert("selected_skill_ids".to_string(), "[\"pdf\"]".to_string());

    resolve_selected_skill_ids(&mut session, None, "normal prompt");
    assert!(!session.metadata.contains_key("selected_skill_ids"));
}

#[test]
fn clear_skill_runtime_state_removes_loaded_skill_markers() {
    let mut session = Session::new("session-1", "model");
    session.metadata.insert(
        "skill_runtime_loaded_skill_ids".to_string(),
        r#"["demo"]"#.to_string(),
    );
    session.metadata.insert(
        "skill_runtime_last_loaded_skill_id".to_string(),
        "demo".to_string(),
    );

    clear_skill_runtime_state(&mut session);

    assert!(!session
        .metadata
        .contains_key("skill_runtime_loaded_skill_ids"));
    assert!(!session
        .metadata
        .contains_key("skill_runtime_last_loaded_skill_id"));
}
