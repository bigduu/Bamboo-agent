use std::path::Path;

use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;
use bamboo_agent_core::Session;
use bamboo_engine::auto_dream::{run_project_auto_dream_once, AutoDreamContext};
use bamboo_storage::{CleanupMode, CleanupResult};
use bamboo_memory::memory_store::MemoryStore;
use bamboo_tools::tools::workspace_state;

use super::super::types::CleanupRequest;

/// `POST /api/v1/sessions/{session_id}/clear`
pub async fn clear_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();
    let cleared = state
        .session_store
        .clear_session(&session_id)
        .await
        .map_err(|error| {
            actix_web::error::ErrorInternalServerError(format!("Failed to clear session: {error}"))
        })?;

    if !cleared {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        })));
    }

    // Publish onto the account change feed so other clients drop their cached
    // messages for this session and refetch lazily.
    state.account_sink.record(
        Some(&session_id),
        &bamboo_agent_core::AgentEvent::SessionCleared {
            session_id: session_id.clone(),
        },
    );

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "session_id": session_id
    })))
}

async fn load_session_from_state_or_storage(
    state: &AppState,
    session_id: &str,
) -> Result<Option<Session>> {
    let in_memory = {
        bamboo_engine::read_cached_session(&state.sessions, session_id)
    };
    if in_memory.is_some() {
        return Ok(in_memory);
    }

    state
        .storage
        .load_session(session_id)
        .await
        .map_err(|error| {
            actix_web::error::ErrorInternalServerError(format!("Failed to load session: {error}"))
        })
}

fn resolve_session_project_key(session_id: &str, session: &Session) -> Option<String> {
    session
        .workspace_path_meta()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .and_then(|path| MemoryStore::derive_project_key_from_workspace(Some(Path::new(&path))))
        .or_else(|| {
            workspace_state::get_workspace(session_id).and_then(|path| {
                MemoryStore::derive_project_key_from_workspace(Some(path.as_path()))
            })
        })
}

/// `POST /api/v1/sessions/{session_id}/project-dream/run`
pub async fn run_project_dream(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();
    let Some(session) = load_session_from_state_or_storage(&state, &session_id).await? else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        })));
    };

    let Some(project_key) = resolve_session_project_key(&session_id, &session) else {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Project scope unavailable for session",
            "session_id": session_id
        })));
    };

    let ctx = AutoDreamContext {
        session_store: state.session_store.clone(),
        storage: state.storage.clone(),
        provider: state.get_provider().await,
        config: state.config.clone(),
        provider_registry: state.provider_registry.clone(),
    };

    let result = run_project_auto_dream_once(&ctx, &project_key)
        .await
        .map_err(|error| {
            actix_web::error::ErrorInternalServerError(format!(
                "Failed to run project Dream generation: {error}"
            ))
        })?;

    let response = match result {
        Some(result) => serde_json::json!({
            "success": true,
            "session_id": session_id,
            "project_key": project_key,
            "dream_generated": true,
            "used_model": result.used_model,
            "session_count": result.session_count,
            "note_path": result.note_path.to_string_lossy().to_string(),
            "notebook_chars": result.notebook_chars,
        }),
        None => serde_json::json!({
            "success": true,
            "session_id": session_id,
            "project_key": project_key,
            "dream_generated": false,
            "message": "No project Dream update was needed"
        }),
    };

    Ok(HttpResponse::Ok().json(response))
}

/// `POST /api/v1/sessions/cleanup`
pub async fn cleanup_sessions(
    state: web::Data<AppState>,
    req: web::Json<CleanupRequest>,
) -> Result<HttpResponse> {
    let mode = match req.mode.trim().to_ascii_lowercase().as_str() {
        "all" => CleanupMode::All,
        "empty" => CleanupMode::Empty,
        "children" => CleanupMode::Children,
        other => {
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": "Invalid cleanup mode",
                "mode": other
            })));
        }
    };

    let result: CleanupResult = state
        .session_store
        .cleanup(mode, req.keep_pinned)
        .await
        .map_err(|error| {
            actix_web::error::ErrorInternalServerError(format!("Cleanup failed: {error}"))
        })?;

    if !result.deleted_session_ids.is_empty() {
        // Best-effort cancel any in-flight executions.
        {
            let mut runners = state.agent_runners.write().await;
            for session_id in &result.deleted_session_ids {
                if let Some(runner) = runners.remove(session_id) {
                    runner.cancel_token.cancel();
                }
            }
        }
        {
            let mut tokens = state.cancel_tokens.write().await;
            for session_id in &result.deleted_session_ids {
                if let Some(token) = tokens.remove(session_id) {
                    token.cancel();
                }
            }
        }
        for session_id in &result.deleted_session_ids {
            state.sessions.remove(session_id);
        }
        {
            let mut senders = state.session_event_senders.write().await;
            for session_id in &result.deleted_session_ids {
                senders.remove(session_id);
            }
        }
        // Publish onto the account change feed so other clients drop the
        // cleaned-up sessions from their list.
        for session_id in &result.deleted_session_ids {
            state.account_sink.record(
                Some(session_id),
                &bamboo_agent_core::AgentEvent::SessionDeleted {
                    session_id: session_id.clone(),
                },
            );
        }
    }

    Ok(HttpResponse::Ok().json(result))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use actix_web::{http::StatusCode, test, web, App};
    use async_trait::async_trait;
    use futures::stream;
    use serde_json::Value;
    use tempfile::tempdir;

    use super::*;
    use crate::routes::configure_routes;
    use crate::AppState;
    use bamboo_agent_core::{ConversationSummary, Message};
    use bamboo_llm::Config;
    use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};

    #[derive(Clone)]
    struct SequenceProvider {
        responses: Arc<Mutex<Vec<String>>>,
    }

    impl SequenceProvider {
        fn new(responses: Vec<String>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses)),
            }
        }
    }

    #[async_trait]
    impl LLMProvider for SequenceProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            let next = self.responses.lock().expect("lock poisoned").remove(0);
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token(next)),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    async fn build_test_app_state(
        data_dir: std::path::PathBuf,
        provider: Arc<dyn LLMProvider>,
    ) -> web::Data<AppState> {
        let config = Config {
            memory: Some(bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                ..bamboo_config::MemoryConfig::default()
            }),
            ..Config::default()
        };
        web::Data::new(
            AppState::new_with_provider(data_dir, config, provider)
                .await
                .expect("app state should initialize"),
        )
    }

    #[actix_web::test]
    async fn run_project_dream_endpoint_generates_project_dream_for_session_project() {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let workspace = temp_dir.path().join("workspace-http-project-dream");
        std::fs::create_dir_all(&workspace).expect("workspace dir");
        let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);

        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            "## Current durable context\n- HTTP project dream generated\n\n## Cross-session patterns\n- None\n\n## Active threads to remember\n- None\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None".to_string(),
            "{\"candidates\":[]}".to_string(),
        ]));
        let app_state = build_test_app_state(temp_dir.path().to_path_buf(), provider).await;

        let mut session = bamboo_agent_core::Session::new("session-http-project-dream", "model");
        session.title = "HTTP Project Dream".to_string();
        session.metadata.insert(
            "workspace_path".to_string(),
            workspace.to_string_lossy().to_string(),
        );
        session.conversation_summary = Some(ConversationSummary::new(
            "Project-scoped session for manual dream generation.",
            3,
            120,
        ));
        session.add_message(Message::user("Generate project dream now."));
        app_state
            .storage
            .save_session(&session)
            .await
            .expect("save session");

        let app = test::init_service(
            App::new()
                .app_data(app_state.clone())
                .configure(configure_routes),
        )
        .await;
        let req = test::TestRequest::post()
            .uri("/api/v1/sessions/session-http-project-dream/project-dream/run")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body.get("success").and_then(Value::as_bool), Some(true));
        assert_eq!(
            body.get("session_id").and_then(Value::as_str),
            Some("session-http-project-dream")
        );
        assert_eq!(
            body.get("project_key").and_then(Value::as_str),
            Some(project_key.as_str())
        );
        assert_eq!(
            body.get("dream_generated").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            body.get("used_model").and_then(Value::as_str),
            Some("fast-model")
        );

        let memory = MemoryStore::new(temp_dir.path());
        let project_dream = memory
            .read_project_dream_view(&project_key)
            .await
            .expect("read project dream")
            .expect("project dream should exist");
        assert!(project_dream.contains("HTTP project dream generated"));
    }

    #[actix_web::test]
    async fn lifecycle_mutations_publish_change_feed_events() {
        use bamboo_engine::events::journal;

        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![]));
        let app_state = build_test_app_state(temp_dir.path().to_path_buf(), provider).await;

        let app = test::init_service(
            App::new()
                .app_data(app_state.clone())
                .configure(configure_routes),
        )
        .await;

        // Create a session via the route.
        let create = test::TestRequest::post()
            .uri("/api/v1/sessions")
            .set_json(serde_json::json!({ "title": "Feed test" }))
            .to_request();
        let resp = test::call_service(&app, create).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        let session_id = body["session"]["id"]
            .as_str()
            .expect("session id")
            .to_string();

        // Clear it.
        let clear = test::TestRequest::post()
            .uri(&format!("/api/v1/sessions/{session_id}/clear"))
            .to_request();
        assert_eq!(
            test::call_service(&app, clear).await.status(),
            StatusCode::OK
        );

        // Delete it.
        let delete = test::TestRequest::delete()
            .uri(&format!("/api/v1/sessions/{session_id}"))
            .to_request();
        assert_eq!(
            test::call_service(&app, delete).await.status(),
            StatusCode::OK
        );

        // Let the single writer task drain.
        for _ in 0..100 {
            if app_state.account_sink.latest_seq() >= 3 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let events = journal::read_since(app_state.account_sink.events_dir(), 0).unwrap();
        let kinds: Vec<&str> = events
            .iter()
            .map(|ce| match &ce.event {
                bamboo_agent_core::AgentEvent::SessionCreated { .. } => "created",
                bamboo_agent_core::AgentEvent::SessionCleared { .. } => "cleared",
                bamboo_agent_core::AgentEvent::SessionDeleted { .. } => "deleted",
                _ => "other",
            })
            .collect();
        assert_eq!(
            kinds,
            vec!["created", "cleared", "deleted"],
            "events: {kinds:?}"
        );
        // All carry the right session id and monotonic seq.
        assert!(events
            .iter()
            .all(|ce| ce.session_id.as_deref() == Some(session_id.as_str())));
        assert_eq!(
            events.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[actix_web::test]
    async fn run_project_dream_endpoint_returns_bad_request_when_project_scope_is_unavailable() {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![]));
        let app_state = build_test_app_state(temp_dir.path().to_path_buf(), provider).await;

        let mut session = bamboo_agent_core::Session::new("session-no-project-scope", "model");
        session.title = "No Project Scope".to_string();
        session.conversation_summary =
            Some(ConversationSummary::new("No workspace metadata.", 1, 40));
        session.add_message(Message::user(
            "Try to generate project dream without workspace.",
        ));
        app_state
            .storage
            .save_session(&session)
            .await
            .expect("save session");

        let app = test::init_service(
            App::new()
                .app_data(app_state.clone())
                .configure(configure_routes),
        )
        .await;
        let req = test::TestRequest::post()
            .uri("/api/v1/sessions/session-no-project-scope/project-dream/run")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(
            body.get("error").and_then(Value::as_str),
            Some("Project scope unavailable for session")
        );
        assert_eq!(
            body.get("session_id").and_then(Value::as_str),
            Some("session-no-project-scope")
        );
    }
}
