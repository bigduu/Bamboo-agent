use super::request::{optional_non_empty, resolve_model, resolve_session_id};
use super::sync_runtime_workspace;
use bamboo_agent_core::Session;

use bamboo_engine::session_app::chat::{
    clear_skill_runtime_state, resolve_base_prompt,
    resolve_copilot_conclusion_with_options_enhancement, resolve_enhance_prompt,
    resolve_selected_skill_ids, resolve_workspace_path,
};

#[actix_web::test]
async fn typed_workflow_candidate_is_pinned_exactly_and_stale_revision_fails_closed() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
    let state = crate::AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state");
    let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
    let review = catalog
        .entries
        .iter()
        .find(|entry| entry.id == "review" && entry.winner)
        .expect("builtin review catalog entry");
    let selection = bamboo_skills::WorkflowSelection {
        id: review.id.clone(),
        source: review.source,
        revision: review.revision,
        args: serde_json::json!({}),
    };
    let mut session = Session::new("typed-review", "model");
    let staging_id = super::pin_explicit_workflow_candidate(
        &state,
        &mut session,
        &selection,
        &std::collections::BTreeSet::new(),
    )
    .await
    .expect("pin exact typed Workflow");

    let snapshot: bamboo_skills::SkillActivationSnapshot = serde_json::from_str(
        session
            .metadata
            .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY)
            .expect("durable pre-execute snapshot"),
    )
    .expect("snapshot contract");
    assert_eq!(snapshot.skills.len(), 1);
    assert_eq!(snapshot.skills["review"].revision, review.revision);
    assert_eq!(
        session
            .metadata
            .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY)
            .map(String::as_str),
        Some("explicit")
    );
    let request_identity = serde_json::to_string(&selection).expect("selection JSON");
    assert!(!request_identity.contains(&review.description));
    assert!(!request_identity.contains("prompt"));

    state
        .skill_manager
        .release_activation_for_workspace(&staging_id, None)
        .await
        .expect("release exact candidate");

    let existing_ids = ["plan".to_string()];
    let existing = state
        .skill_manager
        .resolve_and_pin_activation_for_request_with_mode_and_budget(
            "typed-review-stale",
            &std::collections::BTreeSet::new(),
            Some(&existing_ids),
            None,
            None,
            bamboo_skills::DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
        )
        .await
        .expect("existing live activation");
    let mut stale_session = Session::new("typed-review-stale", "model");
    let stale = bamboo_skills::WorkflowSelection {
        revision: review.revision + 1,
        ..selection
    };
    let response = super::pin_explicit_workflow_candidate(
        &state,
        &mut stale_session,
        &stale,
        &std::collections::BTreeSet::new(),
    )
    .await
    .expect_err("stale revision must fail before chat persistence");
    assert_eq!(response.status(), actix_web::http::StatusCode::CONFLICT);
    assert!(!stale_session
        .metadata
        .contains_key(bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY));
    assert!(stale_session.messages.is_empty());
    let retained = state
        .skill_manager
        .pinned_activation_for_workspace("typed-review-stale", None)
        .await
        .expect("inspect existing activation")
        .expect("failed request must retain existing activation");
    assert_eq!(
        retained.descriptor.skill_revisions,
        existing.descriptor.skill_revisions
    );
    assert_eq!(retained.skills[0].id, "plan");
}

#[actix_web::test]
async fn typed_workflow_capacity_failure_is_sanitized_and_retryable() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
    let state = crate::AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state");
    let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
    let review = catalog
        .entries
        .iter()
        .find(|entry| entry.id == "review" && entry.winner)
        .expect("builtin review");
    for index in 0..256 {
        state
            .skill_manager
            .pin_current_activation_for_workspace(
                &format!("capacity-{index}"),
                None,
                &["plan".to_string()],
                None,
            )
            .await
            .expect("fill activation capacity");
    }
    let selection = bamboo_skills::WorkflowSelection {
        id: review.id.clone(),
        source: review.source,
        revision: review.revision,
        args: serde_json::json!({}),
    };
    let mut session = Session::new("capacity-overflow", "model");
    let response = super::pin_explicit_workflow_candidate(
        &state,
        &mut session,
        &selection,
        &std::collections::BTreeSet::new(),
    )
    .await
    .expect_err("capacity exhaustion must fail closed");
    assert_eq!(
        response.status(),
        actix_web::http::StatusCode::SERVICE_UNAVAILABLE
    );
    let body = actix_web::body::to_bytes(response.into_body())
        .await
        .expect("response body");
    let body: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(body["error"]["code"], "workflow_snapshot_unavailable");
    assert_eq!(
        body["error"]["message"],
        "Workflow catalog is temporarily unavailable; retry the request"
    );
    let rendered = body.to_string();
    assert!(!rendered.contains("capacity"));
    assert!(!rendered.contains(temp_dir.path().to_string_lossy().as_ref()));
    assert!(session.metadata.is_empty());
    assert!(session.messages.is_empty());
}

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

#[actix_web::test]
async fn sync_runtime_workspace_materializes_with_the_states_provider() {
    let app_home = tempfile::tempdir().expect("app home");
    let state = crate::AppState::new(app_home.path().to_path_buf())
        .await
        .expect("app state");
    let root = bamboo_config::paths::resolve_workspace_root_in(app_home.path());
    let workspace = root.join("session-runtime-workspace");
    let session_id = "session-runtime-workspace";
    assert!(!workspace.exists());

    sync_runtime_workspace(
        &state,
        session_id,
        Some(workspace.to_string_lossy().as_ref()),
        "session_fallback",
    );

    let resolved = bamboo_tools::tools::workspace_state::get_workspace(session_id)
        .expect("workspace should be stored");
    assert_eq!(resolved, workspace);
    assert!(
        resolved.is_dir(),
        "instance root should materialize fallback"
    );
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
    use async_trait::async_trait;
    use bamboo_agent_core::{AgentEvent, Session};
    use bamboo_llm::{
        LLMChunk, LLMError, LLMProvider, LLMRequestOptions, LLMStream, ProviderModelRouter,
        ProviderRegistry,
    };
    use serde_json::Value;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::Semaphore;

    use crate::routes::configure_routes;
    use crate::AppState;

    const CONCURRENCY_ASSERT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    async fn new_state() -> web::Data<AppState> {
        let temp_dir = tempdir().expect("tempdir").keep();
        bamboo_config::paths::init_bamboo_dir(temp_dir.clone());
        web::Data::new(AppState::new(temp_dir).await.expect("app state"))
    }

    async fn seed_active_instruction_workflow(
        state: &web::Data<AppState>,
        session_id: &str,
        workflow_id: &str,
    ) -> bamboo_skills::WorkflowSelection {
        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let entry = catalog
            .entries
            .iter()
            .find(|entry| entry.id == workflow_id && entry.winner)
            .expect("builtin instruction Workflow")
            .clone();
        let selection = bamboo_skills::WorkflowSelection {
            id: entry.id.clone(),
            source: entry.source,
            revision: entry.revision,
            args: serde_json::json!({}),
        };
        let ids = [entry.id.clone()];
        let activation = state
            .skill_manager
            .resolve_and_pin_activation_for_request_with_mode_and_budget(
                session_id,
                &std::collections::BTreeSet::new(),
                Some(&ids),
                None,
                None,
                bamboo_skills::DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
            )
            .await
            .expect("pin canonical live activation");
        let snapshot = state
            .skill_manager
            .store()
            .export_activation_snapshot(session_id)
            .await
            .expect("export canonical activation snapshot");
        let mut session = Session::new(session_id, "test-model");
        session.metadata.insert(
            bamboo_skills::WORKFLOW_SELECTION_METADATA_KEY.to_string(),
            serde_json::to_string(&selection).expect("selection JSON"),
        );
        bamboo_skills::persist_explicit_workflow_candidate(
            &mut session.metadata,
            &selection,
            &activation,
            &snapshot,
        )
        .expect("persist exact candidate");
        bamboo_skills::record_loaded_workflow_activation(
            &mut session.metadata,
            workflow_id,
            format!("sha256:{workflow_id}"),
        )
        .expect("publish active Workflow");
        state.save_and_cache_session(&mut session).await;
        selection
    }

    fn workflow_runtime_metadata(session: &Session) -> std::collections::BTreeMap<String, String> {
        session
            .metadata
            .iter()
            .filter(|(key, _)| {
                key.starts_with("workflow.")
                    || key.starts_with("skill_runtime_")
                    || key.as_str() == "selected_skill_ids"
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    struct BlockingTitleProvider {
        calls: AtomicUsize,
        started: Semaphore,
        release: Semaphore,
    }

    impl BlockingTitleProvider {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                started: Semaphore::new(0),
                release: Semaphore::new(0),
            })
        }
    }

    #[async_trait]
    impl LLMProvider for BlockingTitleProvider {
        async fn chat_stream(
            &self,
            _messages: &[bamboo_agent_core::Message],
            _tools: &[bamboo_agent_core::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            panic!("title generation must use request options")
        }

        async fn chat_stream_with_options(
            &self,
            _messages: &[bamboo_agent_core::Message],
            _tools: &[bamboo_agent_core::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
            options: Option<&LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            assert_eq!(
                options.and_then(|value| value.request_purpose.as_deref()),
                Some("title_generation")
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.add_permits(1);
            let _permit = self
                .release
                .acquire()
                .await
                .expect("release semaphore stays open");

            Ok(Box::pin(futures::stream::iter(vec![
                Ok(LLMChunk::Token("Generated Immediately".to_string())),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    async fn title_test_state(provider: Arc<BlockingTitleProvider>) -> web::Data<AppState> {
        let data_dir = tempdir().expect("tempdir").keep();
        bamboo_config::paths::init_bamboo_dir(data_dir.clone());
        let mut config = bamboo_llm::Config::from_data_dir(Some(data_dir.clone()));
        config.provider = "openai".to_string();
        config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
            model: Some("chat-model".to_string()),
            fast_model: Some("title-model".to_string()),
            ..Default::default()
        });

        let provider_trait: Arc<dyn LLMProvider> = provider.clone();
        let mut app_state = AppState::new_with_provider(data_dir, config, provider_trait)
            .await
            .expect("app state");
        let mut providers = HashMap::new();
        providers.insert("openai".to_string(), provider as Arc<dyn LLMProvider>);
        app_state.provider_registry =
            Arc::new(ProviderRegistry::new(providers, "openai".to_string()));
        app_state.provider_router = Arc::new(ProviderModelRouter::new(
            app_state.provider_registry.clone(),
        ));
        web::Data::new(app_state)
    }

    /// #793: a durable user message is the trigger. No `/execute` request is
    /// made, and a second message while the provider is blocked must not start
    /// duplicate title work.
    #[actix_web::test]
    async fn chat_starts_title_generation_before_execute_and_deduplicates_inflight_work() {
        let provider = BlockingTitleProvider::new();
        let state = title_test_state(provider.clone()).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let session_id = "chat-title-before-execute";
        let sender = state.get_session_event_sender(session_id).await;
        let mut title_events = sender.subscribe();

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": session_id,
                    "message": "Fix title generation timing",
                    "model": "chat-model"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);

        let _started = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            provider.started.acquire(),
        )
        .await
        .expect("title provider started without /execute")
        .expect("started semaphore stays open");

        let pending = state
            .storage
            .load_session(session_id)
            .await
            .expect("load pending session")
            .expect("session persisted");
        assert!(!pending.title_generated);
        assert_eq!(pending.title_version, 0);

        let second = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": session_id,
                    "message": "Second durable user message",
                    "model": "chat-model"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(second.status(), StatusCode::CREATED);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);

        provider.release.add_permits(1);
        let finalized = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let session = state
                    .storage
                    .load_session(session_id)
                    .await
                    .expect("load finalized session")
                    .expect("session remains present");
                if session.title_generated {
                    break session;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("title generation finalized");

        assert_eq!(finalized.title, "Generated Immediately");
        assert_eq!(finalized.title_version, 1);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);

        let event =
            tokio::time::timeout(std::time::Duration::from_millis(500), title_events.recv())
                .await
                .expect("one title event arrives")
                .expect("title event channel remains open");
        assert!(matches!(
            event,
            AgentEvent::SessionTitleUpdated {
                title_generated: true,
                ..
            }
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), title_events.recv())
                .await
                .is_err(),
            "deduplicated title work must not emit a second metadata event"
        );
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

    /// #733: a retry after the first response is lost must receive the exact
    /// first response without appending the user message a second time.
    #[actix_web::test]
    async fn idempotency_key_replays_chat_without_duplicate_message() {
        use bamboo_agent_core::Role;

        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let request = || {
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .insert_header(("Idempotency-Key", "chat-retry-733"))
                .set_json(serde_json::json!({
                    "message": "persist exactly once",
                    "model": "chat-model"
                }))
                .to_request()
        };

        let first = test::call_service(&app, request()).await;
        assert_eq!(first.status(), StatusCode::CREATED);
        let first_body = test::read_body(first).await;
        let replay = test::call_service(&app, request()).await;
        assert_eq!(replay.status(), StatusCode::CREATED);
        let replay_body = test::read_body(replay).await;
        assert_eq!(
            replay_body, first_body,
            "retry must replay exact JSON bytes"
        );

        let response: Value = serde_json::from_slice(&first_body).expect("chat response JSON");
        let session_id = response["session_id"].as_str().expect("session_id");
        let session = state
            .storage
            .load_session(session_id)
            .await
            .expect("load session")
            .expect("session exists");
        assert_eq!(
            session
                .messages
                .iter()
                .filter(|message| message.role == Role::User)
                .count(),
            1,
            "replay must not append a second user message"
        );

        let conflict = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .insert_header(("Idempotency-Key", "chat-retry-733"))
                .set_json(serde_json::json!({
                    "message": "different payload",
                    "model": "chat-model"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        let conflict: Value = test::read_body_json(conflict).await;
        assert_eq!(conflict["error"]["code"], "idempotency_key_conflict");
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
    async fn chat_checks_nested_workspace_owner_before_creating_session() {
        let state = new_state().await;
        let workspace = tempdir().expect("workspace");
        let nested = workspace.path().join("nested");
        std::fs::create_dir_all(&nested).expect("nested");
        let owner = state
            .project_store
            .create_with_bindings(
                "Owner",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: workspace.path().to_string_lossy().to_string(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .expect("Project");
        let nested = nested.to_string_lossy().to_string();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let conflict = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": "chat-cross-project",
                    "message": "must not persist",
                    "model": "test-model",
                    "workspace_path": nested.clone(),
                }))
                .to_request(),
        )
        .await;
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        let conflict: Value = test::read_body_json(conflict).await;
        assert_eq!(conflict["error"]["code"], "project_workspace_conflict");
        assert!(
            state
                .storage
                .load_session("chat-cross-project")
                .await
                .expect("load")
                .is_none(),
            "ownership failure must happen before session persistence"
        );

        let mut feed = state.account_sink.subscribe();
        let created = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": "chat-owned-project",
                    "project_id": owner.id.to_string(),
                    "message": "hello",
                    "model": "test-model",
                    "workspace_path": nested,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let created_event = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
            .await
            .expect("SessionCreated timeout")
            .expect("SessionCreated event");
        assert!(matches!(
            &created_event.event,
            bamboo_agent_core::AgentEvent::SessionCreated {
                session_id,
                project_id: Some(project_id),
                ..
            } if session_id == "chat-owned-project" && project_id == owner.id.as_str()
        ));
        let replay = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            created_event.seq.saturating_sub(1),
        )
        .expect("journal replay");
        assert!(replay.iter().any(|change| matches!(
            &change.event,
            bamboo_agent_core::AgentEvent::SessionCreated {
                session_id,
                project_id: Some(project_id),
                ..
            } if session_id == "chat-owned-project" && project_id == owner.id.as_str()
        )));
        let session = state
            .storage
            .load_session("chat-owned-project")
            .await
            .expect("load")
            .expect("session");
        let resolved = state
            .project_context_resolver
            .resolve(&session, None)
            .await
            .expect("resolve persisted Project context")
            .expect("assigned Project context");
        assert_eq!(
            resolved.binding_status,
            bamboo_engine::project_context::WorkspaceBindingStatus::Registered
        );
        let snapshot = session.prompt_snapshot.expect("immediate prompt snapshot");
        assert!(snapshot
            .project_context
            .as_deref()
            .is_some_and(|context| context.contains(owner.id.as_str())));
        assert_eq!(
            snapshot
                .effective_system_prompt
                .matches("<!-- BAMBOO_PROJECT_CONTEXT_START -->")
                .count(),
            1
        );
        assert!(
            snapshot
                .workspace_context
                .as_deref()
                .is_some_and(|context| context.contains("Binding status: registered")),
            "unexpected workspace context: {:?}",
            snapshot.workspace_context
        );
    }

    #[actix_web::test]
    async fn chat_invalid_workspace_is_400_and_has_no_session_side_effect() {
        let state = new_state().await;
        let fixture = tempdir().expect("fixture");
        let missing = fixture.path().join("missing");
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
                    "session_id": "chat-invalid-workspace",
                    "message": "must not persist",
                    "model": "test-model",
                    "workspace_path": missing,
                }))
                .to_request(),
        )
        .await;
        let status = response.status();
        let body: Value = test::read_body_json(response).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body={body}");
        assert_eq!(body["error"]["code"], "workspace_invalid");
        assert!(state
            .storage
            .load_session("chat-invalid-workspace")
            .await
            .expect("load")
            .is_none());
    }

    #[actix_web::test]
    async fn chat_explicit_workspace_switch_requires_existing_project_binding() {
        let state = new_state().await;
        let project_path = tempdir().expect("Project path");
        let unbound = tempdir().expect("unbound workspace");
        let project = state
            .project_store
            .create_with_project_path(
                "Assigned Project",
                None,
                project_path.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("Project");
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
                    "session_id": "chat-unbound-workspace",
                    "project_id": project.id,
                    "message": "must not persist",
                    "model": "test-model",
                    "workspace_path": unbound.path(),
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["code"], "project_workspace_unbound");
        assert_eq!(body["session_project_id"], project.id.as_str());
        assert!(
            state
                .storage
                .load_session("chat-unbound-workspace")
                .await
                .expect("load")
                .is_none(),
            "binding validation must happen before chat creates the session"
        );
    }

    #[actix_web::test]
    async fn queued_chat_omitting_workspace_preserves_authoritative_workspace_update() {
        let state = new_state().await;
        let fixture = tempdir().expect("fixture");
        let workspace_a = fixture.path().join("workspace-a");
        let workspace_b = fixture.path().join("workspace-b");
        std::fs::create_dir_all(&workspace_a).unwrap();
        std::fs::create_dir_all(&workspace_b).unwrap();
        let session_id = "chat-workspace-lock-barrier";
        let mut session = Session::new(session_id, "test-model");
        session.set_workspace_path_meta(workspace_a.to_string_lossy().into_owned());
        state.storage.save_session(&session).await.unwrap();
        state.sessions.insert(
            session_id.to_string(),
            std::sync::Arc::new(parking_lot::RwLock::new(session)),
        );
        bamboo_agent_core::workspace_state::set_workspace(
            session_id,
            workspace_a.canonicalize().unwrap(),
        );
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        // Hold the same lock used by Workspace/PATCH writes. Poll chat until it
        // reaches the lock after its lock-free preflight, then commit workspace
        // B before allowing chat's authoritative reload to proceed.
        let guard = state.persistence.acquire_lock(session_id).await;
        let chat = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": session_id,
                    "message": "workspace field intentionally omitted",
                    "model": "test-model"
                }))
                .to_request(),
        );
        tokio::pin!(chat);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut chat)
                .await
                .is_err(),
            "chat should wait at the per-session transaction lock"
        );

        let mut latest = state
            .persistence
            .storage()
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        latest.set_workspace_path_meta(workspace_b.to_string_lossy().into_owned());
        state
            .persistence
            .storage()
            .save_session(&latest)
            .await
            .unwrap();
        state.sessions.insert(
            session_id.to_string(),
            std::sync::Arc::new(parking_lot::RwLock::new(latest)),
        );
        bamboo_agent_core::workspace_state::set_workspace(
            session_id,
            workspace_b.canonicalize().unwrap(),
        );
        drop(guard);

        let response = chat.await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let persisted = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        let workspace_b = workspace_b.canonicalize().unwrap();
        assert_eq!(
            persisted.workspace_path_meta().as_deref(),
            Some(bamboo_config::paths::path_to_display_string(&workspace_b).as_str())
        );
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace(session_id).as_deref(),
            Some(workspace_b.as_path())
        );
    }

    #[actix_web::test]
    async fn chat_membership_authority_is_durable_storage_not_stale_cache() {
        let state = new_state().await;
        let project_a_path = tempdir().expect("Project A path");
        let project_b_path = tempdir().expect("Project B path");
        let project_a = state
            .project_store
            .create_with_project_path(
                "Project A",
                None,
                project_a_path.path().to_string_lossy(),
                Vec::new(),
            )
            .unwrap();
        let project_b = state
            .project_store
            .create_with_project_path(
                "Project B",
                None,
                project_b_path.path().to_string_lossy(),
                Vec::new(),
            )
            .unwrap();
        let session_id = "chat-authoritative-project-storage";
        let mut durable = Session::new(session_id, "test-model");
        durable.set_project_id_meta(project_b.id.to_string());
        state.storage.save_session(&durable).await.unwrap();
        let mut stale_cache = durable.clone();
        stale_cache.set_project_id_meta(project_a.id.to_string());
        stale_cache.updated_at = chrono::Utc::now() + chrono::Duration::hours(1);
        state.sessions.insert(
            session_id.to_string(),
            std::sync::Arc::new(parking_lot::RwLock::new(stale_cache)),
        );
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
                    "session_id": session_id,
                    "message": "use durable membership",
                    "model": "test-model"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let persisted = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted.project_id_meta().as_deref(),
            Some(project_b.id.as_str())
        );
    }

    #[actix_web::test]
    async fn chat_uses_assigned_project_path_before_foreign_configured_default() {
        let state = new_state().await;
        let workspace = tempdir().expect("foreign default workspace");
        let other_workspace = tempdir().expect("assigned Project path");
        let owner = state
            .project_store
            .create_with_project_path(
                "Default Owner",
                None,
                workspace.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("owner Project");
        let other = state
            .project_store
            .create_with_project_path(
                "Other Project",
                None,
                other_workspace.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("other Project");
        state.config.write().await.default_work_area = Some(bamboo_config::DefaultWorkAreaConfig {
            path: Some(workspace.path().to_string_lossy().into_owned()),
        });
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
                    "session_id": "chat-project-default",
                    "project_id": other.id.to_string(),
                    "message": "use Project path",
                    "model": "test-model"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let persisted = state
            .storage
            .load_session("chat-project-default")
            .await
            .expect("load")
            .expect("session");
        assert_eq!(
            persisted.workspace_path_meta().as_deref(),
            Some(
                bamboo_config::paths::path_to_display_string(
                    &other_workspace.path().canonicalize().unwrap()
                )
                .as_str()
            )
        );
        assert_ne!(
            persisted.project_id_meta().as_deref(),
            Some(owner.id.as_str())
        );
    }

    #[actix_web::test]
    async fn chat_persists_same_project_configured_default_and_prompt_marker() {
        let state = new_state().await;
        let workspace = tempdir().expect("default workspace");
        let foreign_default = tempdir().expect("foreign global default");
        let project = state
            .project_store
            .create_with_project_path(
                "Default Owner",
                None,
                workspace.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("Project");
        state.config.write().await.default_work_area = Some(bamboo_config::DefaultWorkAreaConfig {
            path: Some(foreign_default.path().to_string_lossy().into_owned()),
        });
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
                    "session_id": "chat-default-owned",
                    "project_id": project.id.to_string(),
                    "message": "hello",
                    "model": "test-model"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let canonical = workspace.path().canonicalize().expect("canonical");
        let canonical_display = bamboo_config::paths::path_to_display_string(&canonical);
        let session = state
            .storage
            .load_session("chat-default-owned")
            .await
            .expect("load")
            .expect("session");
        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some(canonical_display.as_str())
        );
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace("chat-default-owned").as_deref(),
            Some(canonical.as_path())
        );
        let snapshot = session.prompt_snapshot.expect("prompt snapshot");
        assert!(snapshot.workspace_context.as_deref().is_some_and(|value| {
            value.contains("Binding status: registered")
                && value.contains("Workspace source: project_default")
        }));
        assert_eq!(
            snapshot
                .effective_system_prompt
                .matches("BAMBOO_WORKSPACE_CONTEXT_START")
                .count(),
            1
        );
    }

    #[actix_web::test]
    async fn chat_project_without_path_fails_before_session_side_effects() {
        let state = new_state().await;
        let project = state
            .project_store
            .create("Legacy unconfigured", None)
            .expect("legacy Project");
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
                    "session_id": "chat-project-path-missing",
                    "project_id": project.id.to_string(),
                    "message": "must not persist",
                    "model": "test-model"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["code"], "project_path_missing");
        assert!(state
            .storage
            .load_session("chat-project-path-missing")
            .await
            .expect("load")
            .is_none());
        assert!(
            bamboo_agent_core::workspace_state::peek_workspace("chat-project-path-missing")
                .is_none()
        );
    }

    #[actix_web::test]
    async fn user_prompt_submit_block_preserves_existing_workflow_and_persists_no_user_message() {
        let state = new_state().await;
        let session_id = "blocked-user-prompt";
        seed_active_instruction_workflow(&state, session_id, "plan").await;
        let before = state
            .storage
            .load_session(session_id)
            .await
            .expect("load seeded session")
            .expect("seeded session");
        let expected_workflow_metadata = workflow_runtime_metadata(&before);
        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let review = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review" && entry.winner)
            .expect("builtin review Workflow");
        {
            let mut config = state.config.write().await;
            config.lifecycle_hooks = bamboo_config::LifecycleHooksConfig {
                enabled: true,
                user_prompt_submit: vec![bamboo_config::LifecycleHookGroup {
                    enabled: true,
                    matcher: None,
                    hooks: vec![bamboo_config::LifecycleHookHandler::command(
                        "printf 'prompt rejected by policy' >&2; exit 2",
                        bamboo_config::DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
                    )],
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
                    "session_id": session_id,
                    "message": "must not persist",
                    "model": "test-model",
                    "workflow_selection": {
                        "id": review.id,
                        "source": review.source,
                        "revision": review.revision,
                        "args": {}
                    }
                }))
                .to_request(),
        )
        .await;

        let status = response.status();
        let body: Value = test::read_body_json(response).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body={body}");
        assert!(body.to_string().contains("prompt rejected by policy"));
        assert_eq!(body["hook_event"], "UserPromptSubmit");

        let session = state
            .storage
            .load_session(session_id)
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
        assert_eq!(
            workflow_runtime_metadata(&session),
            expected_workflow_metadata,
            "rejected typed selection must not disturb durable Workflow authority"
        );
        let live = state
            .skill_manager
            .pinned_activation_for_workspace(session_id, None)
            .await
            .expect("inspect canonical activation")
            .expect("existing activation remains pinned");
        assert_eq!(live.skills.len(), 1);
        assert_eq!(live.skills[0].id, "plan");
    }

    #[actix_web::test]
    async fn image_rejection_preserves_existing_workflow_and_persists_no_user_message() {
        let state = new_state().await;
        let session_id = "rejected-image-workflow";
        seed_active_instruction_workflow(&state, session_id, "plan").await;
        let before = state
            .storage
            .load_session(session_id)
            .await
            .expect("load seeded session")
            .expect("seeded session");
        let expected_workflow_metadata = workflow_runtime_metadata(&before);
        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let review = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review" && entry.winner)
            .expect("builtin review Workflow");
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
                    "session_id": session_id,
                    "message": "must not persist",
                    "model": "test-model",
                    "workflow_selection": {
                        "id": review.id,
                        "source": review.source,
                        "revision": review.revision,
                        "args": {}
                    },
                    "images": [{
                        "base64": "not-valid-base64%%%",
                        "type": "image/png"
                    }]
                }))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let after = state
            .storage
            .load_session(session_id)
            .await
            .expect("reload session")
            .expect("session remains");
        assert!(after
            .messages
            .iter()
            .all(|message| !matches!(message.role, bamboo_agent_core::Role::User)));
        assert_eq!(
            workflow_runtime_metadata(&after),
            expected_workflow_metadata,
            "failed attachment must not publish speculative Workflow metadata"
        );
        let live = state
            .skill_manager
            .pinned_activation_for_workspace(session_id, None)
            .await
            .expect("inspect canonical activation")
            .expect("existing activation remains pinned");
        assert_eq!(live.skills[0].id, "plan");
    }

    #[actix_web::test]
    async fn running_session_rejects_typed_workflow_replacement_without_mutation() {
        let state = new_state().await;
        let session_id = "running-workflow-replacement";
        seed_active_instruction_workflow(&state, session_id, "plan").await;
        let before = state
            .storage
            .load_session(session_id)
            .await
            .expect("load seeded session")
            .expect("seeded session");
        let expected_workflow_metadata = workflow_runtime_metadata(&before);
        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let review = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review" && entry.winner)
            .expect("builtin review Workflow");
        let mut runner = crate::app_state::AgentRunner::new();
        runner.status = crate::app_state::AgentStatus::Running;
        state
            .agent_runners
            .write()
            .await
            .insert(session_id.to_string(), runner);

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
                    "session_id": session_id,
                    "message": "must wait",
                    "model": "test-model",
                    "workflow_selection": {
                        "id": review.id,
                        "source": review.source,
                        "revision": review.revision,
                        "args": {}
                    }
                }))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(
            body["error"]["code"],
            "workflow_activation_running_conflict"
        );
        let after = state
            .storage
            .load_session(session_id)
            .await
            .expect("reload session")
            .expect("session remains");
        assert_eq!(
            workflow_runtime_metadata(&after),
            expected_workflow_metadata
        );
        assert!(after.messages.is_empty());
        let live = state
            .skill_manager
            .pinned_activation_for_workspace(session_id, None)
            .await
            .expect("inspect canonical activation")
            .expect("existing activation remains pinned");
        assert_eq!(live.skills[0].id, "plan");
    }

    #[actix_web::test]
    async fn runner_reserved_during_typed_chat_rejects_commit_without_mutation() {
        let state = new_state().await;
        let session_id = "workflow-reserved-before-commit";
        seed_active_instruction_workflow(&state, session_id, "plan").await;
        let before = state
            .storage
            .load_session(session_id)
            .await
            .expect("load seeded session")
            .expect("seeded session");
        let expected_workflow_metadata = workflow_runtime_metadata(&before);
        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let review = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review" && entry.winner)
            .expect("builtin review Workflow");
        let barrier = super::super::install_workflow_commit_test_barrier(session_id);
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
                    "session_id": session_id,
                    "message": "must lose the reservation race",
                    "model": "test-model",
                    "workflow_selection": {
                        "id": review.id,
                        "source": review.source,
                        "revision": review.revision,
                        "args": {}
                    }
                }))
                .to_request(),
        );
        tokio::pin!(response);

        let reached = barrier.reached.acquire();
        tokio::pin!(reached);
        let reached_permit = tokio::time::timeout(CONCURRENCY_ASSERT_TIMEOUT, async {
            tokio::select! {
                permit = &mut reached => permit.expect("workflow commit barrier remains open"),
                early = &mut response => panic!(
                    "typed chat completed before the commit barrier: {}",
                    early.status()
                ),
            }
        })
        .await
        .expect("typed chat reaches the deterministic pre-commit barrier");
        reached_permit.forget();

        let mut runner = crate::app_state::AgentRunner::new();
        runner.status = crate::app_state::AgentStatus::Pending;
        state
            .agent_runners
            .write()
            .await
            .insert(session_id.to_string(), runner);
        barrier.resume.add_permits(1);

        let response = tokio::time::timeout(CONCURRENCY_ASSERT_TIMEOUT, &mut response)
            .await
            .expect("typed chat rejects the newly reserved runner");
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(
            body["error"]["code"],
            "workflow_activation_running_conflict"
        );

        let after = state
            .storage
            .load_session(session_id)
            .await
            .expect("reload session")
            .expect("session remains");
        assert_eq!(
            workflow_runtime_metadata(&after),
            expected_workflow_metadata,
            "a late runner reservation must leave durable Workflow authority unchanged"
        );
        assert!(after
            .messages
            .iter()
            .all(|message| !matches!(message.role, bamboo_agent_core::Role::User)));
        let live = state
            .skill_manager
            .pinned_activation_for_workspace(session_id, None)
            .await
            .expect("inspect canonical activation")
            .expect("existing activation remains pinned");
        assert_eq!(live.skills[0].id, "plan");
    }

    #[actix_web::test]
    async fn cancelled_typed_chat_finishes_the_committed_pin_handoff() {
        let state = new_state().await;
        let session_id = "workflow-cancelled-after-save";
        seed_active_instruction_workflow(&state, session_id, "plan").await;
        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let review = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review" && entry.winner)
            .expect("builtin review Workflow");
        let barrier = super::super::install_workflow_post_save_test_barrier(session_id);
        let mut feed = state.account_sink.subscribe();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        {
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/chat")
                    .set_json(serde_json::json!({
                        "session_id": session_id,
                        "message": "commit despite response cancellation",
                        "model": "test-model",
                        "workflow_selection": {
                            "id": review.id,
                            "source": review.source,
                            "revision": review.revision,
                            "args": {}
                        }
                    }))
                    .to_request(),
            );
            tokio::pin!(response);
            let reached = barrier.reached.acquire();
            tokio::pin!(reached);
            let reached_permit = tokio::time::timeout(CONCURRENCY_ASSERT_TIMEOUT, async {
                tokio::select! {
                    permit = &mut reached => permit.expect("post-save barrier remains open"),
                    early = &mut response => panic!(
                        "typed chat completed before the post-save barrier: {}",
                        early.status()
                    ),
                }
            })
            .await
            .expect("typed chat reaches the deterministic post-save barrier");
            reached_permit.forget();

            let live_before_handoff = state
                .skill_manager
                .pinned_activation_for_workspace(session_id, None)
                .await
                .expect("inspect old canonical pin")
                .expect("old activation remains until durable save returns");
            assert_eq!(live_before_handoff.skills[0].id, "plan");
            // Dropping the Actix response future simulates a disconnected
            // client. The detached commit task must retain both locks and
            // finish cache/feed/pin publication.
        }
        barrier.resume.add_permits(1);

        let event = tokio::time::timeout(CONCURRENCY_ASSERT_TIMEOUT, async {
            loop {
                let event = feed.recv().await.expect("account feed remains open");
                if matches!(
                    &event.event,
                    bamboo_agent_core::AgentEvent::MessageAppended { session_id: id, .. }
                        if id == session_id
                ) {
                    break event;
                }
            }
        })
        .await
        .expect("detached typed commit publishes its durable message");
        assert!(matches!(
            event.event,
            bamboo_agent_core::AgentEvent::MessageAppended { .. }
        ));

        let persisted = state
            .storage
            .load_session(session_id)
            .await
            .expect("reload committed session")
            .expect("committed session");
        let selection: bamboo_skills::WorkflowSelection = serde_json::from_str(
            persisted
                .metadata
                .get(bamboo_skills::WORKFLOW_SELECTION_METADATA_KEY)
                .expect("committed typed selection"),
        )
        .expect("selection JSON");
        assert_eq!(selection.id, "review");
        assert!(persisted.messages.iter().any(|message| {
            matches!(message.role, bamboo_agent_core::Role::User)
                && message.content == "commit despite response cancellation"
        }));
        assert!(state
            .skill_manager
            .pinned_activation_for_workspace(session_id, None)
            .await
            .expect("inspect released canonical pin")
            .is_none());
        let guard = tokio::time::timeout(
            CONCURRENCY_ASSERT_TIMEOUT,
            state.persistence.acquire_lock(session_id),
        )
        .await
        .expect("detached commit releases the persistence lock");
        drop(guard);
    }

    #[actix_web::test]
    async fn typed_instruction_secret_args_fail_before_any_session_or_prompt_persistence() {
        const SECRET_MARKER: &str = "sk-instruction-secret-must-never-persist";
        let state = new_state().await;
        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let review = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review" && entry.winner)
            .expect("builtin review Workflow");
        let session_id = "secret-workflow-args";
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
                    "session_id": session_id,
                    "message": "review this",
                    "model": "test-model",
                    "workflow_selection": {
                        "id": review.id,
                        "source": review.source,
                        "revision": review.revision,
                        "args": {"scope": SECRET_MARKER}
                    }
                }))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["code"], "workflow_selection_invalid");
        assert!(!body.to_string().contains(SECRET_MARKER));
        assert!(state
            .storage
            .load_session(session_id)
            .await
            .expect("load rejected session")
            .is_none());
        assert!(state.sessions.get(session_id).is_none());
        let durable_files = walkdir::WalkDir::new(&state.app_data_dir)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file())
            .filter_map(|entry| std::fs::read(entry.path()).ok())
            .collect::<Vec<_>>();
        assert!(durable_files.iter().all(|bytes| !bytes
            .windows(SECRET_MARKER.len())
            .any(|window| window == SECRET_MARKER.as_bytes())));
    }
}
