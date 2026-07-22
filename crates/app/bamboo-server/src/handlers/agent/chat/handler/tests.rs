use super::request::{optional_non_empty, resolve_model, resolve_session_id};
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
fn resolve_model_errors_when_neither_request_nor_default_resolve() {
    let response = resolve_model(Some("   "), None).expect_err("no model should be required error");
    assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
}

#[test]
fn resolve_model_trims_whitespace_from_request_model() {
    let model = resolve_model(Some("  gpt-5  "), None).expect("model should be accepted");
    assert_eq!(model, "gpt-5");
}

/// #480: an absent/blank request model falls back to the server's resolved
/// default rather than erroring, as long as a default is available.
#[test]
fn resolve_model_falls_back_to_default_when_request_model_absent() {
    let model = resolve_model(None, Some("gpt-default")).expect("default should be used");
    assert_eq!(model, "gpt-default");
}

#[test]
fn resolve_model_falls_back_to_default_when_request_model_blank() {
    let model = resolve_model(Some("   "), Some("gpt-default")).expect("default should be used");
    assert_eq!(model, "gpt-default");
}

#[test]
fn resolve_model_prefers_explicit_request_model_over_default() {
    let model =
        resolve_model(Some("gpt-explicit"), Some("gpt-default")).expect("request model wins");
    assert_eq!(model, "gpt-explicit");
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

// ---- #480: POST /chat with an omitted `model` end-to-end ----

mod optional_model_e2e {
    use actix_web::{http::StatusCode, test, web, App};
    use serde_json::Value;
    use tempfile::tempdir;

    use crate::routes::configure_routes;
    use crate::AppState;

    async fn new_state() -> web::Data<AppState> {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        )
    }

    /// #480: omitting `model` on `POST /chat` falls back to the server's
    /// resolved default (the same resolution `GET /execute/defaults` and the
    /// connect bridge use) — the session ends up with the CONFIGURED default
    /// model, not an error and not an empty model.
    #[actix_web::test]
    async fn chat_without_model_uses_resolved_default_model() {
        let state = new_state().await;
        {
            let mut config = state.config.write().await;
            config.provider = "openai".to_string();
            config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
                model: Some("gpt-configured-default".to_string()),
                ..Default::default()
            });
        }

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({ "message": "hello" }))
                .to_request(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(resp).await;
        let session_id = body["session_id"].as_str().expect("session_id").to_string();

        let session = state
            .storage
            .load_session(&session_id)
            .await
            .expect("load")
            .expect("session exists");
        assert_eq!(session.model, "gpt-configured-default");
    }

    /// An explicit `model` on `POST /chat` is unchanged by #480 — it is used
    /// as-is even when a different server default is configured.
    #[actix_web::test]
    async fn chat_with_explicit_model_is_unchanged() {
        let state = new_state().await;
        {
            let mut config = state.config.write().await;
            config.provider = "openai".to_string();
            config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
                model: Some("gpt-configured-default".to_string()),
                ..Default::default()
            });
        }

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "message": "hello",
                    "model": "gpt-explicit-override"
                }))
                .to_request(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(resp).await;
        let session_id = body["session_id"].as_str().expect("session_id").to_string();

        let session = state
            .storage
            .load_session(&session_id)
            .await
            .expect("load")
            .expect("session exists");
        assert_eq!(session.model, "gpt-explicit-override");
    }

    /// No request model AND no server default configured → 400, not a silent
    /// empty-model session.
    #[actix_web::test]
    async fn chat_without_model_and_without_default_errors() {
        let state = new_state().await;

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({ "message": "hello" }))
                .to_request(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_web::test]
    async fn user_prompt_submit_block_returns_reason_and_persists_no_user_message() {
        let state = new_state().await;
        {
            let mut config = state.config.write().await;
            config.lifecycle_hooks = bamboo_config::LifecycleHooksConfig {
                enabled: true,
                user_prompt_submit: vec![bamboo_config::LifecycleHookGroup {
                    matcher: None,
                    hooks: vec![bamboo_config::LifecycleHookCommand {
                        hook_type: bamboo_config::LifecycleHookType::Command,
                        command: "printf 'prompt rejected by policy' >&2; exit 2".to_string(),
                        timeout_ms: bamboo_config::DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
                    }],
                }],
                ..Default::default()
            };
        }

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": "blocked-user-prompt",
                    "message": "must not persist",
                    "model": "test-model"
                }))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: Value = test::read_body_json(response).await;
        assert!(body.to_string().contains("prompt rejected by policy"));
        assert_eq!(body["hook_event"], "UserPromptSubmit");

        let session = state
            .storage
            .load_session("blocked-user-prompt")
            .await
            .expect("load")
            .expect("prepared session is persisted for hook observability");
        assert!(session
            .messages
            .iter()
            .all(|message| !matches!(message.role, bamboo_agent_core::Role::User)));
        assert_eq!(
            session
                .agent_runtime_state
                .as_ref()
                .map(|state| state.checkpoints.len()),
            Some(1)
        );
    }
}
