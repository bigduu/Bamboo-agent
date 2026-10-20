use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;
use bamboo_agent_core::{Session, Storage};
use bamboo_engine::auto_dream::{run_project_auto_dream_once_for_project, AutoDreamContext};
use bamboo_engine::project_context::ProjectContextResolver;
use bamboo_memory::memory_store::MemoryStore;
use bamboo_storage::{CleanupFailure, CleanupMode, CleanupResult};

use super::super::types::{CleanupRequest, SessionSummary};

/// `POST /api/v1/sessions/{session_id}/clear`
pub async fn clear_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();
    let _persistence_guard = state.persistence.acquire_lock(&session_id).await;
    // A warm cache is an observation, never permission to rewrite a newer
    // durable contract. Storage rechecks under its cross-process writer guards.
    if let Some(canonical) = state
        .storage
        .load_session(&session_id)
        .await
        .map_err(|error| {
            crate::error::json_internal_server_error(format!(
                "Failed to load session before clear: {error}"
            ))
        })?
    {
        if let Some(conflict) = SessionSummary::execution_conflict(&canonical) {
            return Ok(conflict);
        }
    }
    let cleared = match state.session_store.clear_session(&session_id).await {
        Ok(cleared) => cleared,
        Err(error) => {
            // Another process may publish a future contract after preflight.
            // Present its precise report, without classifying arbitrary IO or
            // Actor authority errors as compatibility conflicts.
            if let Ok(Some(canonical)) = state.storage.load_session(&session_id).await {
                if let Some(conflict) = SessionSummary::execution_conflict(&canonical) {
                    return Ok(conflict);
                }
            }
            return Err(crate::error::json_internal_server_error(format!(
                "Failed to clear session: {error}"
            )));
        }
    };

    if !cleared {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("Session not found"),
            "session_id": session_id
        })));
    }
    let _ = state.browser.close(&session_id).await;

    // History/chat treat the process cache as authoritative while no runner is
    // active. Replace it with the just-cleared durable snapshot before
    // publishing SessionCleared; otherwise the old transcript can remain
    // visible and a later cache-based save can write it back to disk. The
    // persistence guard stays held across clear, reload, cache publication, and
    // the account-feed event, preserving one process-local mutation boundary.
    // Evict first so a rare post-clear reload failure cannot leave the stale
    // pre-clear transcript authoritative in memory. A successful reload below
    // immediately installs the replacement.
    state.sessions.remove(&session_id);
    let cleared_session = state
        .session_store
        .load_session(&session_id)
        .await
        .map_err(|error| {
            crate::error::json_internal_server_error(format!(
                "Failed to reload cleared session: {error}"
            ))
        })?
        .ok_or_else(|| {
            crate::error::json_internal_server_error(
                "Cleared session disappeared before cache publication",
            )
        })?;
    state.sessions.insert(
        session_id.clone(),
        std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(cleared_session)),
    );

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
    let in_memory = { bamboo_engine::read_cached_session(&state.sessions, session_id) };
    if in_memory.is_some() {
        return Ok(in_memory);
    }

    state
        .storage
        .load_session(session_id)
        .await
        .map_err(|error| {
            crate::error::json_internal_server_error(format!("Failed to load session: {error}"))
        })
}

fn dream_memory_store(state: &AppState) -> MemoryStore {
    state.memory_store.clone()
}

/// `POST /api/v1/sessions/{session_id}/project-dream/run`
pub async fn run_project_dream(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();
    let Some(session) = load_session_from_state_or_storage(&state, &session_id).await? else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("Session not found"),
            "session_id": session_id
        })));
    };

    let project_id = match ProjectContextResolver::session_project_identity(&session) {
        bamboo_engine::project_context::SessionProjectIdentity::Assigned(project_id) => project_id,
        bamboo_engine::project_context::SessionProjectIdentity::Unassigned => {
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": crate::error::error_value(
                    "Project Dream writes require an assigned Project"
                ),
                "session_id": session_id
            })));
        }
        bamboo_engine::project_context::SessionProjectIdentity::Invalid { raw, message } => {
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": crate::error::error_value(format!(
                    "Session carries an invalid Project identity '{raw}': {message}"
                )),
                "session_id": session_id
            })));
        }
    };

    let ctx = AutoDreamContext {
        session_store: state.session_store.clone(),
        storage: state.storage.clone(),
        memory: dream_memory_store(&state),
        provider: state.get_provider().await,
        config: state.config.clone(),
        provider_registry: state.provider_registry.clone(),
    };
    let result = run_project_auto_dream_once_for_project(&ctx, &project_id)
        .await
        .map_err(|error| {
            crate::error::json_internal_server_error(format!(
                "Failed to run project Dream generation: {error}"
            ))
        })?;

    let response = match result {
        Some(result) => serde_json::json!({
            "success": true,
            "session_id": session_id,
            "project_id": project_id,
            "dream_generated": true,
            "used_model": result.used_model,
            "session_count": result.session_count,
            "generated_at": result.generated_at,
            "source_generation": result.source_generation,
            "notebook_chars": result.notebook_chars,
        }),
        None => serde_json::json!({
            "success": true,
            "session_id": session_id,
            "project_id": project_id,
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
                "error": crate::error::error_value("Invalid cleanup mode"),
                "mode": other
            })));
        }
    };

    let outcome = tokio::spawn(cleanup_owned(state, mode, req.keep_pinned))
        .await
        .map_err(|_| crate::error::json_internal_server_error("Session cleanup interrupted"))?;
    if outcome.storage_error.is_some() || !outcome.browser_cleanup_pending.is_empty() {
        return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "error": crate::error::error_value("Session cleanup incomplete. Check the reported session IDs before retrying deletion."),
            "storage_cleanup_error": outcome.storage_error,
            "deleted_count": outcome.deleted.deleted_count,
            "deleted_session_ids": outcome.deleted.deleted_session_ids,
            "browser_cleanup_pending_session_ids": outcome.browser_cleanup_pending,
            "unconfirmed_session_ids": outcome.unconfirmed_session_ids
        })));
    }
    Ok(HttpResponse::Ok().json(outcome.deleted))
}

struct CleanupOutcome {
    deleted: CleanupResult,
    browser_cleanup_pending: Vec<String>,
    storage_error: Option<String>,
    unconfirmed_session_ids: Vec<String>,
}

// The owned task includes storage deletion and every browser retirement, so a
// disconnected HTTP caller cannot abandon profiles after storage has committed.
async fn cleanup_owned(
    state: web::Data<AppState>,
    mode: CleanupMode,
    keep_pinned: bool,
) -> CleanupOutcome {
    let result = state
        .session_store
        .cleanup_with_progress(mode, keep_pinned)
        .await;
    finish_cleanup(state, result).await
}

async fn finish_cleanup(
    state: web::Data<AppState>,
    result: std::result::Result<CleanupResult, CleanupFailure>,
) -> CleanupOutcome {
    let mut unconfirmed_session_ids = Vec::new();
    let (result, storage_error) = match result {
        Ok(result) => (result, None),
        Err(failure) => {
            tracing::warn!(error = %failure.error, "session cleanup partially failed; checking canonical deletions");
            let mut deleted_session_ids = Vec::new();
            for session_id in failure.candidate_session_ids {
                // The index/cache alone is not proof of deletion. Purge only
                // IDs whose canonical storage now confirms absence.
                match state.storage.load_session(&session_id).await {
                    Ok(None) => deleted_session_ids.push(session_id),
                    Ok(Some(_)) => {}
                    Err(_) => unconfirmed_session_ids.push(session_id),
                }
            }
            deleted_session_ids.sort();
            (
                CleanupResult {
                    deleted_count: deleted_session_ids.len(),
                    deleted_session_ids,
                },
                Some(failure.error.to_string()),
            )
        }
    };

    let mut browser_cleanup_pending = Vec::new();
    if !result.deleted_session_ids.is_empty() {
        for session_id in &result.deleted_session_ids {
            if let Err(error) = state.browser.retire(session_id).await {
                tracing::warn!(%session_id, %error, "deleted session browser cleanup incomplete; retry deletion by ID");
                browser_cleanup_pending.push(session_id.clone());
            }
        }
        // Best-effort cancel any in-flight executions.
        {
            let mut runners = state.agent_runners.write().await;
            for session_id in &result.deleted_session_ids {
                if let Some(runner) =
                    bamboo_engine::runtime::execution::runner_lifecycle::remove_runner_entry(
                        &mut runners,
                        session_id,
                    )
                    .await
                {
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

    CleanupOutcome {
        deleted: result,
        browser_cleanup_pending,
        storage_error,
        unconfirmed_session_ids,
    }
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

    #[cfg(unix)]
    #[tokio::test]
    async fn successful_legacy_child_cleanup_does_not_purge_a_surviving_canonical_directory() {
        use std::os::unix::fs::PermissionsExt;
        let home = tempdir().unwrap();
        let state = web::Data::new(AppState::new(home.path().into()).await.unwrap());
        let root = Session::new("canonical-kept-root", "model");
        let child = Session::new_child_of("canonical-kept-child", &root, "model", "child");
        state.session_store.save_session(&root).await.unwrap();
        state.session_store.save_session(&child).await.unwrap();
        state.session_store.flush_search_index().await;
        state.sessions.insert(child.id.clone(), Arc::new(bamboo_engine::SessionSnapshot::new(child.clone())));
        let directory = home.path().join("sessions").join(&root.id).join("children").join(&child.id);
        let main = directory.join("session.json");
        let before = std::fs::read(&main).unwrap();
        let profile = crate::browser::test_profile_directory(&home.path().join("browser-profiles"), &child.id);
        std::fs::write(profile.join("Cookies"), b"preserve synthetic state").unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o500)).unwrap();
        let outcome = cleanup_owned(state.clone(), CleanupMode::Children, false).await;
        if directory.exists() {
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        assert_eq!(std::fs::read(&main).unwrap(), before);
        assert_eq!(std::fs::read(profile.join("Cookies")).unwrap(), b"preserve synthetic state");
        assert!(state.sessions.contains_key(&child.id));
        assert!(outcome.deleted.deleted_session_ids.is_empty());
        assert_eq!(outcome.unconfirmed_session_ids, vec![child.id]);
    }

    #[tokio::test]
    async fn partial_storage_cleanup_still_purges_canonically_deleted_profiles_only() {
        let directory = tempdir().unwrap();
        let state = web::Data::new(AppState::new(directory.path().to_path_buf()).await.unwrap());
        let deleted = Session::new("partial-browser-deleted", "model");
        let kept = Session::new("partial-browser-kept", "model");
        let profile_root = directory.path().join("browser-profiles");
        let deleted_profile = crate::browser::test_profile_directory(&profile_root, &deleted.id);
        let kept_profile = crate::browser::test_profile_directory(&profile_root, &kept.id);
        for (session, profile) in [(&deleted, &deleted_profile), (&kept, &kept_profile)] {
            state.session_store.save_session(session).await.unwrap();
            std::fs::write(profile.join("Cookies"), b"synthetic state").unwrap();
        }
        // Reproduce storage's partial-commit boundary with a real canonical
        // deletion followed by an injected later storage error. No browser
        // cleanup has run, and the other canonical Session remains live.
        assert!(state.storage.delete_session(&deleted.id).await.unwrap());
        let outcome = finish_cleanup(
            state.clone(),
            Err(CleanupFailure {
                error: std::io::Error::other("synthetic later cleanup failure"),
                candidate_session_ids: vec![deleted.id.clone(), kept.id.clone()],
            }),
        )
        .await;
        assert_eq!(
            outcome.deleted.deleted_session_ids,
            vec![deleted.id.clone()]
        );
        assert!(outcome.storage_error.is_some());
        assert!(outcome.browser_cleanup_pending.is_empty());
        assert!(outcome.unconfirmed_session_ids.is_empty());
        assert!(!deleted_profile.exists());
        assert!(kept_profile.join("Cookies").exists());
        assert!(state
            .storage
            .load_session(&kept.id)
            .await
            .unwrap()
            .is_some());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bulk_cleanup_reports_failed_profiles_continues_other_ids_and_allows_retry() {
        use std::os::unix::fs::OpenOptionsExt;
        let directory = tempdir().unwrap();
        let state = web::Data::new(AppState::new(directory.path().to_path_buf()).await.unwrap());
        let blocked = Session::new("cleanup-browser-blocked", "model");
        let other = Session::new("cleanup-browser-other", "model");
        let profile_root = directory.path().join("browser-profiles");
        let blocked_profile = crate::browser::test_profile_directory(&profile_root, &blocked.id);
        let other_profile = crate::browser::test_profile_directory(&profile_root, &other.id);
        for (session, profile) in [(&blocked, &blocked_profile), (&other, &other_profile)] {
            state.session_store.save_session(session).await.unwrap();
            std::fs::write(profile.join("Cookies"), b"synthetic state").unwrap();
        }
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(blocked_profile.join(".bamboo-profile.lock"))
            .unwrap();
        fs2::FileExt::try_lock_exclusive(&lock).unwrap();

        let response = cleanup_sessions(
            state.clone(),
            web::Json(CleanupRequest {
                mode: "all".into(),
                keep_pinned: false,
            }),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body["browser_cleanup_pending_session_ids"],
            serde_json::json!([blocked.id.clone()])
        );
        for id in [&blocked.id, &other.id] {
            assert!(body["deleted_session_ids"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!(id)));
            assert!(state.storage.load_session(id).await.unwrap().is_none());
        }
        assert!(blocked_profile.join("Cookies").exists());
        assert!(!other_profile.exists());
        drop(lock);
        let retried = crate::handlers::agent::delete::handler(state, web::Path::from(blocked.id))
            .await
            .unwrap();
        assert_eq!(retried.status(), StatusCode::NOT_FOUND);
        assert!(!blocked_profile.exists());
    }

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
        let mut config = Config::default();
        *config.memory_mut() = Some(bamboo_config::MemoryConfig {
            background_model: Some("fast-model".to_string()),
            // Disable the background maintenance tickers in tests. L4 made them
            // default-ON, so with a background_model set the AppState builder
            // spawns real auto_dream/gardener tasks whose first tick fires
            // immediately and calls the mock provider — racing the endpoint's
            // own dream call and draining the finite `SequenceProvider` queue,
            // which flakily panics on `remove(0)`. These endpoint tests drive
            // dream generation explicitly (the endpoint runs regardless of the
            // flag), so the background tickers must stay quiet.
            auto_dream_enabled: false,
            gardener_enabled: false,
            dedup_gardener_enabled: false,
            ..bamboo_config::MemoryConfig::default()
        });
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

        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            "{\"candidates\":[]}".to_string(),
            "## Current durable context\n- HTTP project dream generated\n\n## Cross-session patterns\n- None\n\n## Active threads to remember\n- None\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None".to_string(),
        ]));
        let app_state = build_test_app_state(temp_dir.path().to_path_buf(), provider).await;
        let project = app_state
            .project_store
            .create_with_bindings(
                "HTTP Project Dream",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: workspace.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .expect("create Project");

        let mut session = bamboo_agent_core::Session::new("session-http-project-dream", "model");
        session.title = "HTTP Project Dream".to_string();
        session.set_project_id_meta(project.id.to_string());
        session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
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
            body.get("project_id").and_then(Value::as_str),
            Some(project.id.as_str())
        );
        assert_eq!(
            body.get("dream_generated").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            body.get("used_model").and_then(Value::as_str),
            Some("fast-model")
        );
        assert!(body.get("generated_at").and_then(Value::as_str).is_some());
        assert_eq!(
            body.get("source_generation")
                .and_then(Value::as_str)
                .map(str::len),
            Some(64)
        );
        assert!(body.get("note_path").is_none());

        let memory = app_state.memory_store.for_project(&project.id);
        let project_dream = memory
            .read_dream_snapshot(
                bamboo_memory::memory_store::MemoryScope::Project,
                Some(project.id.as_str()),
            )
            .await
            .expect("read project Dream snapshot")
            .snapshot
            .expect("project dream should exist");
        assert!(project_dream
            .content
            .contains("HTTP project dream generated"));
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
        // POST /sessions now returns 201 Created (#251 finding 3).
        assert_eq!(resp.status(), StatusCode::CREATED);
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
        // Canonical nested error envelope (#251 finding 2), not the old flat
        // `{"error": "<string>"}` shape.
        assert_eq!(body["error"]["type"], "api_error");
        assert_eq!(
            body["error"]["message"].as_str(),
            Some("Project Dream writes require an assigned Project")
        );
        assert_eq!(
            body.get("session_id").and_then(Value::as_str),
            Some("session-no-project-scope")
        );
    }
}
