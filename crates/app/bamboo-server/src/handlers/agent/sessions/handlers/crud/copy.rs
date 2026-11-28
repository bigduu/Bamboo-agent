use actix_web::{http::StatusCode, web, HttpResponse, Result};
use bamboo_storage::v2::SessionForkError;
use serde::Deserialize;
use uuid::Uuid;

use crate::{app_state::AppState, error::json_error_with_code};

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
    duplicate_session(&state, path.into_inner(), None).await
}

#[derive(Deserialize)]
pub struct ForkSessionRequest {
    up_to_message_id: String,
}

/// Create a fresh Root with the stored conversation through the selected message.
pub async fn fork_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
    request: web::Json<ForkSessionRequest>,
) -> Result<HttpResponse> {
    duplicate_session(&state, path.into_inner(), Some(&request.up_to_message_id)).await
}

async fn duplicate_session(
    state: &AppState,
    source_id: String,
    fork_anchor: Option<&str>,
) -> Result<HttpResponse> {
    let new_id = Uuid::new_v4().to_string();
    // SessionStoreV2 owns the cross-process lifecycle/atomic-publication
    // boundary. Pair it with the same per-session lock used by chat/runtime
    // writers so the source transcript and attachments form one snapshot.
    let _source_guard = state.persistence.acquire_lock(&source_id).await;
    let _target_guard = state.persistence.acquire_lock(&new_id).await;
    let event_slot = match state.account_sink.reserve_session_mutation().await {
        Ok(slot) => slot,
        Err(error) => return Ok(crate::events::session_event_queue_unavailable(error)),
    };

    let snapshot = match fork_anchor {
        Some(message_id) => {
            state
                .session_store
                .fork_session_with_projection_guard(&source_id, &new_id, message_id)
                .await
        }
        None => state
            .session_store
            .copy_session_with_projection_guard(&source_id, &new_id)
            .await
            .map_err(SessionForkError::Io),
    };
    let (copied, projection_guard) = match snapshot {
        Ok(Some(result)) => result,
        Ok(None) if fork_anchor.is_some() => {
            return Ok(json_error_with_code(
                StatusCode::NOT_FOUND,
                "Source session not found".into(),
                Some("session_fork_source_not_found"),
            ));
        }
        Ok(None) => {
            return Ok(HttpResponse::NotFound().json(serde_json::json!({
                "error": crate::error::error_value("Session not found"),
                "session_id": source_id,
            })));
        }
        Err(SessionForkError::AnchorNotFound) => {
            return Ok(json_error_with_code(
                StatusCode::NOT_FOUND,
                "The selected message is no longer in this session".into(),
                Some("session_fork_message_not_found"),
            ));
        }
        Err(SessionForkError::InvalidAnchor) => {
            return Ok(json_error_with_code(
                StatusCode::BAD_REQUEST,
                "The selected message is not a valid fork point".into(),
                Some("session_fork_invalid_message"),
            ));
        }
        Err(SessionForkError::Io(error)) => {
            tracing::error!(
                source_session_id = %source_id,
                target_session_id = %new_id,
                fork = fork_anchor.is_some(),
                %error,
                "failed to duplicate session"
            );
            let message = if fork_anchor.is_some() {
                "Failed to fork session"
            } else {
                "Failed to copy session"
            };
            return Err(crate::error::json_internal_server_error(message));
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
        std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(copied.clone())),
    );

    // Copy preserves the source's workspace assignment. Publish the copied
    // identity separately so Workspace tools never consult the source id.
    if let Some(workspace) = copied.workspace_path_meta().map(std::path::PathBuf::from) {
        let origin = if fork_anchor.is_some() {
            "session_fork"
        } else {
            "session_copy"
        };
        state
            .workspace_resolver
            .publish_resolved_workspace(&new_id, workspace, origin);
    }

    // Account-feed consumers use the ordinary creation event to insert the
    // copied root into their session list without polling.
    event_slot.enqueue(
        &new_id,
        bamboo_agent_core::AgentEvent::SessionCreated {
            session_id: new_id.clone(),
            project_id: copied.project_id_meta(),
            category: copied.category().map(str::to_owned),
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
    async fn session_mutation_routes_wait_for_account_capacity_before_persisting() {
        use std::time::Duration;

        let state = new_state().await;
        let mut root = Session::new("synthetic-feed-root", "test-model");
        let anchor = Message::user("synthetic retained history");
        let anchor_id = anchor.id.clone();
        root.add_message(anchor);
        let child = Session::new_child_of("synthetic-feed-child", &root, "test-model", "Task");
        state.session_store.save_session(&root).await.unwrap();
        state.session_store.save_session(&child).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let mut feed = state.account_sink.subscribe();
        let filler = AgentEvent::SessionDeleted {
            session_id: "synthetic-held-slot".into(),
        };
        let requests = [
            (
                "copy",
                test::TestRequest::post().uri("/api/v1/sessions/synthetic-feed-root/copy"),
                StatusCode::CREATED,
            ),
            (
                "fork",
                test::TestRequest::post()
                    .uri("/api/v1/sessions/synthetic-feed-root/fork")
                    .set_json(serde_json::json!({"up_to_message_id": anchor_id})),
                StatusCode::CREATED,
            ),
            (
                "rename",
                test::TestRequest::patch()
                    .uri("/api/v1/sessions/synthetic-feed-root")
                    .set_json(serde_json::json!({"title": "Renamed"})),
                StatusCode::OK,
            ),
            (
                "pin",
                test::TestRequest::patch()
                    .uri("/api/v1/sessions/synthetic-feed-root")
                    .set_json(serde_json::json!({"pinned": true})),
                StatusCode::OK,
            ),
            (
                "clear",
                test::TestRequest::post().uri("/api/v1/sessions/synthetic-feed-root/clear"),
                StatusCode::OK,
            ),
            (
                "delete",
                test::TestRequest::delete().uri("/api/v1/sessions/synthetic-feed-root"),
                StatusCode::OK,
            ),
        ];
        let mut premature = Vec::new();
        for (operation, req, expected_status) in requests {
            let before = state
                .session_store
                .load_session(&root.id)
                .await
                .unwrap()
                .unwrap();
            let index_count = state.session_store.list_index_entries().await.len();
            let dropped_before = state.account_sink.dropped_count();
            let mut held = Vec::new();
            // The default sink has 1024 slots. Reserve every slot without
            // running fake writers or blocking the real writer task.
            for _ in 0..1024 {
                held.push(
                    state
                        .account_sink
                        .reserve_session_event("synthetic-held-slot", &filler)
                        .await
                        .unwrap()
                        .unwrap(),
                );
            }
            let request = test::call_service(&app, req.to_request());
            tokio::pin!(request);
            let early = tokio::time::timeout(Duration::from_millis(500), &mut request).await;
            if let Ok(response) = &early {
                premature.push((
                    operation,
                    response.status(),
                    state.account_sink.dropped_count() - dropped_before,
                ));
            } else {
                let current = state
                    .session_store
                    .load_session(&root.id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(current.metadata_version, before.metadata_version);
                assert_eq!(current.title, before.title);
                assert_eq!(current.pinned, before.pinned);
                assert_eq!(current.messages.len(), before.messages.len());
                assert_eq!(
                    state.session_store.list_index_entries().await.len(),
                    index_count
                );
            }
            drop(held);
            let response = match early {
                Ok(response) => response,
                Err(_) => request.await,
            };
            assert_eq!(response.status(), expected_status);
            if premature.iter().any(|(name, _, _)| *name == operation) {
                continue;
            }
            let copied_id = if matches!(operation, "copy" | "fork") {
                let body: Value = test::read_body_json(response).await;
                Some(body["session"]["id"].as_str().unwrap().to_owned())
            } else {
                None
            };
            let event_count = if operation == "delete" { 2 } else { 1 };
            let mut matched = std::collections::HashSet::new();
            tokio::time::timeout(Duration::from_secs(5), async {
                while matched.len() < event_count {
                    let change = feed.recv().await.unwrap();
                    let id = match (&change.event, operation) {
                        (AgentEvent::SessionCreated { session_id, .. }, "copy" | "fork")
                            if copied_id.as_ref() == Some(session_id) =>
                        {
                            session_id
                        }
                        (
                            AgentEvent::SessionTitleUpdated {
                                session_id, title, ..
                            },
                            "rename",
                        ) if session_id == &root.id && title == "Renamed" => session_id,
                        (
                            AgentEvent::SessionPinnedUpdated {
                                session_id,
                                pinned: true,
                                ..
                            },
                            "pin",
                        ) if session_id == &root.id => session_id,
                        (AgentEvent::SessionCleared { session_id }, "clear")
                            if session_id == &root.id =>
                        {
                            session_id
                        }
                        (AgentEvent::SessionDeleted { session_id }, "delete")
                            if session_id == &root.id || session_id == &child.id =>
                        {
                            session_id
                        }
                        _ => continue,
                    };
                    assert_eq!(change.session_id.as_ref(), Some(id));
                    assert!(bamboo_engine::events::journal::read_since(
                        state.account_sink.events_dir(),
                        change.seq - 1,
                    )
                    .unwrap()
                    .iter()
                    .any(|written| written.seq == change.seq));
                    matched.insert(id.clone());
                }
            })
            .await
            .unwrap_or_else(|_| panic!("missing account notifications for {operation}"));
        }
        assert!(
            premature.is_empty(),
            "mutations succeeded while the queue was full (operation, HTTP status, dropped events): {premature:?}"
        );
        assert_eq!(state.account_sink.dropped_count(), 0);
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
    async fn fork_endpoint_publishes_an_independent_root_through_the_selected_message() {
        let state = new_state().await;
        let mut source = Session::new("fork-source", "gpt-fork");
        let first = Message::user("original question");
        let selected = Message::assistant("keep this answer", None);
        let future = Message::user("exclude this question");
        source.add_message(first.clone());
        source.add_message(selected.clone());
        source.add_message(future.clone());
        source.set_project_id_meta("project-fork");
        state.session_store.save_session(&source).await.unwrap();

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
                .uri("/api/v1/sessions/fork-source/fork")
                .set_json(serde_json::json!({"up_to_message_id": &selected.id}))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(response).await;
        let fork_id = body["session"]["id"].as_str().unwrap();
        assert_ne!(fork_id, source.id);
        assert_eq!(body["session"]["kind"], "root");
        assert_eq!(body["session"]["root_session_id"], fork_id);
        assert!(body["session"]["parent_session_id"].is_null());
        assert_eq!(body["session"]["project_id"], "project-fork");
        assert_eq!(body["session"]["message_count"], 2);
        assert_eq!(body["session"]["is_running"], false);
        assert!(state.sessions.contains_key(fork_id));

        let persisted = state
            .session_store
            .load_session(fork_id)
            .await
            .unwrap()
            .unwrap();
        let original = state
            .session_store
            .load_session(&source.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted
                .messages
                .iter()
                .map(|message| &message.id)
                .collect::<Vec<_>>(),
            [&first.id, &selected.id]
        );
        assert_eq!(
            original
                .messages
                .iter()
                .map(|message| &message.id)
                .collect::<Vec<_>>(),
            [&first.id, &selected.id, &future.id]
        );
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
            .await
            .expect("account feed timeout")
            .expect("account feed event");
        assert!(matches!(
            &event.event,
            AgentEvent::SessionCreated { session_id, kind, .. }
                if session_id == fork_id && *kind == bamboo_agent_core::SessionKind::Root
        ));
    }

    #[actix_web::test]
    async fn fork_endpoint_distinguishes_missing_sources_and_invalid_or_stale_messages() {
        let state = new_state().await;
        let mut source = Session::new("fork-source", "gpt-fork");
        let system = Message::system("system instructions");
        source.add_message(system.clone());
        state.session_store.save_session(&source).await.unwrap();

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        for (source_id, message_id, status, code) in [
            (
                "absent",
                "message",
                StatusCode::NOT_FOUND,
                "session_fork_source_not_found",
            ),
            (
                "fork-source",
                "stale",
                StatusCode::NOT_FOUND,
                "session_fork_message_not_found",
            ),
            (
                "fork-source",
                system.id.as_str(),
                StatusCode::BAD_REQUEST,
                "session_fork_invalid_message",
            ),
        ] {
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&format!("/api/v1/sessions/{source_id}/fork"))
                    .set_json(serde_json::json!({"up_to_message_id": message_id}))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), status);
            let body: Value = test::read_body_json(response).await;
            assert_eq!(body["error"]["code"], code);
        }
        assert_eq!(state.session_store.list_index_entries().await.len(), 1);
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
