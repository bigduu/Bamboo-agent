use actix_web::{web, HttpResponse, Result};
use uuid::Uuid;

use crate::app_state::AppState;

use super::super::super::types::{CopySessionResponse, SessionSummary};

/// `POST /api/v1/sessions/{session_id}/copy`
///
/// Copies the durable session snapshot and its attachments into a new,
/// independent root session. The storage operation owns rollback: an error
/// never leaves a target session, attachment directory, or index projection.
pub async fn copy_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let source_id = path.into_inner();
    let new_id = Uuid::new_v4().to_string();
    // SessionStoreV2 owns the cross-process lifecycle/atomic-publication
    // boundary. Pair it with the same per-session lock used by chat/runtime
    // writers so the source transcript and attachments form one snapshot.
    let _source_guard = state.persistence.acquire_lock(&source_id).await;
    let _target_guard = state.persistence.acquire_lock(&new_id).await;

    let (copied, projection_guard) = match state
        .session_store
        .copy_session_with_projection_guard(&source_id, &new_id)
        .await
    {
        Ok(Some(result)) => result,
        Ok(None) => {
            return Ok(HttpResponse::NotFound().json(serde_json::json!({
                "error": crate::error::error_value("Session not found"),
                "session_id": source_id,
            })));
        }
        Err(error) => {
            tracing::error!(
                source_session_id = %source_id,
                target_session_id = %new_id,
                %error,
                "failed to copy session"
            );
            return Err(crate::error::json_internal_server_error(
                "Failed to copy session",
            ));
        }
    };

    let Some(entry) = state.session_store.get_index_entry(&new_id).await else {
        tracing::error!(
            source_session_id = %source_id,
            target_session_id = %new_id,
            "copied session is missing from the session index"
        );
        return Err(crate::error::json_internal_server_error(
            "Copied session is missing from the session index",
        ));
    };
    drop(_source_guard);

    // The store committed the authoritative copy. Mirror it into the process
    // cache only after the durable transaction succeeds.
    state.sessions.insert(
        new_id.clone(),
        std::sync::Arc::new(parking_lot::RwLock::new(copied.clone())),
    );

    // Copy preserves the source's workspace assignment. Publish the copied
    // identity separately so Workspace tools never consult the source id.
    if let Some(workspace) = copied.workspace_path_meta().map(std::path::PathBuf::from) {
        state
            .workspace_resolver
            .publish_resolved_workspace(&new_id, workspace, "session_copy");
    }

    // Account-feed consumers use the ordinary creation event to insert the
    // copied root into their session list without polling.
    state.account_sink.record(
        Some(&new_id),
        &bamboo_agent_core::AgentEvent::SessionCreated {
            session_id: new_id.clone(),
            project_id: copied.project_id_meta(),
            title: copied.title.clone(),
            kind: copied.kind,
            created_at: copied.created_at,
        },
    );

    let mut summary = SessionSummary::from_entry(entry, false);
    // List summaries intentionally omit the provider because it is not indexed;
    // this mutation already owns the complete copied Session, so its response
    // can satisfy the full-summary contract without widening the index schema.
    summary.provider = copied.provider_name();
    // Keep the cross-process lifecycle boundary through every process-local
    // projection. Delete/clear cannot observe the copied id and then race an
    // older cache/feed publication back into existence.
    drop(projection_guard);
    Ok(HttpResponse::Created().json(CopySessionResponse { session: summary }))
}

#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test, web, App};
    use bamboo_agent_core::{storage::Storage, AgentEvent, Message, Session};
    use bamboo_domain::SessionPermissionMode;
    use serde_json::Value;

    use super::*;
    use crate::routes::configure_routes;

    async fn new_state() -> web::Data<AppState> {
        let data_dir = tempfile::tempdir().expect("tempdir").keep();
        bamboo_config::paths::init_bamboo_dir(data_dir.clone());
        web::Data::new(AppState::new(data_dir).await.expect("app state"))
    }

    #[actix_web::test]
    async fn copy_endpoint_returns_independent_root_summary_and_updates_projections() {
        let state = new_state().await;
        let workspace = tempfile::tempdir().expect("workspace").keep();
        let parent = Session::new("copy-parent", "gpt-parent");
        let mut source = Session::new_child_of("copy-source", &parent, "gpt-copy", "Research");
        source.add_message(Message::user("preserve this transcript"));
        source.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
        source.metadata.insert(
            "gold_config".to_string(),
            serde_json::json!({"temperature": 0.2}).to_string(),
        );
        source.set_provider_name("openai-instance");
        source.set_project_id_meta("project-copy");
        let runtime = source
            .agent_runtime_state
            .get_or_insert_with(Default::default);
        runtime.set_permission_mode(SessionPermissionMode::Auto);
        state
            .session_store
            .save_session(&source)
            .await
            .expect("save source");

        let mut feed = state.account_sink.subscribe();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions/copy-source/copy")
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(response).await;
        let copied_id = body["session"]["id"]
            .as_str()
            .expect("copied session id")
            .to_string();
        assert_ne!(copied_id, source.id);
        assert_eq!(body["session"]["kind"], "root");
        assert!(body["session"]["parent_session_id"].is_null());
        assert_eq!(body["session"]["root_session_id"], copied_id);
        assert_eq!(body["session"]["message_count"], 1);
        assert_eq!(body["session"]["provider"], "openai-instance");
        assert_eq!(body["session"]["project_id"], "project-copy");
        assert_eq!(body["session"]["permission_mode"], "auto");
        assert_eq!(
            body["session"]["workspace_path"],
            workspace.to_string_lossy().as_ref()
        );
        assert!(state.sessions.contains_key(&copied_id));
        assert_eq!(
            state
                .workspace_resolver
                .resolve_session_workspace_candidate(&copied_id, None)
                .as_deref(),
            Some(workspace.as_path())
        );

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
            .await
            .expect("account feed timeout")
            .expect("account feed event");
        assert!(matches!(
            &event.event,
            AgentEvent::SessionCreated { session_id, kind, .. }
                if session_id == &copied_id && *kind == bamboo_agent_core::SessionKind::Root
        ));
    }

    #[actix_web::test]
    async fn copy_endpoint_returns_canonical_not_found_without_creating_a_session() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions/missing/copy")
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["session_id"], "missing");
        assert_eq!(body["error"]["message"], "Session not found");
        assert!(state.session_store.list_index_entries().await.is_empty());
    }

    #[actix_web::test]
    async fn copy_endpoint_drops_only_unconsumed_runtime_resume_messages() {
        let state = new_state().await;
        let mut source = Session::new("copy-runtime-tail", "gpt-copy");
        source.add_message(Message::user("visible request"));

        let mut consumed_resume = Message::user("consumed child completion");
        consumed_resume.id = "consumed-runtime-resume".to_string();
        consumed_resume.metadata = Some(serde_json::json!({
            "hidden_from_ui": true,
            "runtime_kind": "child_completion_resume"
        }));
        source.add_message(consumed_resume);
        source.add_message(Message::assistant("visible result", None));

        let mut pending_resume = Message::user("pending retry");
        pending_resume.metadata = Some(serde_json::json!({
            "runtime_kind": "retry_resume"
        }));
        source.add_message(pending_resume);
        state
            .session_store
            .save_session(&source)
            .await
            .expect("save source");

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions/copy-runtime-tail/copy")
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(response).await;
        let copied_id = body["session"]["id"].as_str().expect("copied id");
        let copied = state
            .session_store
            .load_session(copied_id)
            .await
            .expect("load copied session")
            .expect("copied session exists");

        assert_eq!(copied.messages.len(), 3);
        assert_eq!(copied.messages[1].id, "consumed-runtime-resume");
        assert!(matches!(
            copied.messages.last().map(|message| &message.role),
            Some(bamboo_agent_core::Role::Assistant)
        ));
        let snapshot =
            bamboo_engine::session_app::types::ServerExecuteSnapshot::from_session(&copied);
        assert!(!snapshot.has_pending_user_message);
    }

    #[actix_web::test]
    async fn copied_session_clear_replaces_cache_and_history() {
        let state = new_state().await;
        let mut source = Session::new("copy-then-clear", "gpt-copy");
        source.add_message(Message::system("keep system"));
        source.add_message(Message::user("remove user"));
        source.add_message(Message::assistant("remove assistant", None));
        state
            .session_store
            .save_session(&source)
            .await
            .expect("save source");

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let copy_response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions/copy-then-clear/copy")
                .to_request(),
        )
        .await;
        assert_eq!(copy_response.status(), StatusCode::CREATED);
        let copy_body: Value = test::read_body_json(copy_response).await;
        let copied_id = copy_body["session"]["id"]
            .as_str()
            .expect("copied id")
            .to_string();

        let clear_response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/sessions/{copied_id}/clear"))
                .to_request(),
        )
        .await;
        assert_eq!(clear_response.status(), StatusCode::OK);

        let cached = bamboo_engine::read_cached_session(&state.sessions, &copied_id)
            .expect("cleared session remains cached");
        assert_eq!(cached.messages.len(), 1);
        assert!(matches!(
            cached.messages[0].role,
            bamboo_agent_core::Role::System
        ));

        let history_response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/sessions/{copied_id}/history"))
                .to_request(),
        )
        .await;
        assert_eq!(history_response.status(), StatusCode::OK);
        let history: Value = test::read_body_json(history_response).await;
        assert_eq!(history["messages"].as_array().expect("messages").len(), 1);
        assert_eq!(history["messages"][0]["role"], "system");
        assert_eq!(history["messages"][0]["content"], "keep system");
    }
}
