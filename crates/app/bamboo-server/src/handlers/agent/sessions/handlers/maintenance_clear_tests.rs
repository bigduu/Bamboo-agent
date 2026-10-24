//! Synthetic HTTP fixtures only; no external provider or user data is used.

use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use actix_web::{body::to_bytes, http::StatusCode, test, web, App};
use async_trait::async_trait;
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFinish, ActorDirectoryPort, Message, Session,
    SessionAuthorityConflict, DEFAULT_SUPERVISOR_SESSION_ID,
};
use bamboo_llm::{LLMError, LLMProvider, LLMStream};
use serde_json::Value;

use super::{
    clear_authority_conflict, clear_blocked_response, clear_session, clear_unknown_response,
};

#[derive(Default)]
struct NoCalls(AtomicUsize);

#[async_trait]
impl LLMProvider for NoCalls {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(LLMError::Api(
            "clear fixture must not call a provider".into(),
        ))
    }
}

async fn state() -> (tempfile::TempDir, web::Data<crate::AppState>, Arc<NoCalls>) {
    let home = tempfile::tempdir().unwrap();
    let mut config = bamboo_llm::Config::from_data_dir(Some(home.path().into()));
    *config.memory_mut() = Some(bamboo_config::MemoryConfig {
        auto_dream_enabled: false,
        gardener_enabled: false,
        dedup_gardener_enabled: false,
        ..Default::default()
    });
    let probe = Arc::new(NoCalls::default());
    let state = crate::AppState::new_with_provider(home.path().into(), config, probe.clone())
        .await
        .unwrap();
    (home, web::Data::new(state), probe)
}

fn files(directory: &Path) -> Vec<Option<Vec<u8>>> {
    [
        "session.json",
        "runtime.json",
        "actor-authority.json",
        "actor-authority.initialized.json",
        "root-tool-authority.json",
        "supervisor-authority.json",
        ".search-index-revision",
        "attachments/sentinel",
    ]
    .map(|name| std::fs::read(directory.join(name)).ok())
    .to_vec()
}

async fn feed_barrier(state: &crate::AppState) -> u64 {
    let marker = format!("clear-contract-barrier-{}", uuid::Uuid::new_v4());
    assert!(
        state
            .account_sink
            .record_confirmed(
                Some(&marker),
                &bamboo_agent_core::AgentEvent::SessionCreated {
                    session_id: marker.clone(),
                    project_id: None,
                    title: "clear contract barrier".into(),
                    kind: bamboo_domain::SessionKind::Root,
                    created_at: chrono::Utc::now(),
                },
            )
            .await
    );
    state.account_sink.latest_seq()
}

#[actix_web::test]
async fn clear_classifies_typed_authority_only_and_never_claims_unknown_success() {
    let typed = std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        SessionAuthorityConflict("synthetic activated context".into()),
    );
    let plain = std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Session authority conflict: synthetic activated context",
    );
    assert!(clear_authority_conflict(&typed));
    assert!(!clear_authority_conflict(&plain));
    let blocked = clear_blocked_response("fixture", &typed);
    assert_eq!(blocked.status(), StatusCode::CONFLICT);
    let body: Value =
        serde_json::from_slice(&to_bytes(blocked.into_body()).await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "session_clear_authority_conflict");
    assert_eq!(body["error"]["type"], "api_error");
    assert_eq!(body["status"], "blocked");
    assert_eq!(body["session_id"], "fixture");
    assert_eq!(body["refresh_required"], true);
    assert_eq!(body["retryable"], false);
    assert!(body.get("success").is_none());

    // The same helper serves storage errors, failed canonical reloads and a
    // disappeared post-clear Session. None proves an unexecuted operation.
    for message in [
        plain.to_string(),
        "reload failed".into(),
        "session disappeared".into(),
    ] {
        let unknown = clear_unknown_response("fixture", message);
        assert_eq!(unknown.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body: Value =
            serde_json::from_slice(&to_bytes(unknown.into_body()).await.unwrap()).unwrap();
        assert_eq!(body["error"]["code"], "session_clear_outcome_unknown");
        assert_eq!(body["error"]["type"], "api_error");
        assert_eq!(body["status"], "outcome_unknown");
        assert_eq!(body["refresh_required"], true);
        assert_eq!(body["retryable"], false);
        assert!(body.get("success").is_none());
        assert!(body.get("not_executed").is_none());
    }
}

#[actix_web::test]
async fn http_clear_authority_refusal_preserves_ordinary_and_supervisor_state() {
    for supervisor in [false, true] {
        for status in ["running", "finished", "retired", "unknown"] {
            let (home, state, probe) = state().await;
            let mut session = if supervisor {
                state
                    .storage
                    .get_or_create_default_supervisor("model")
                    .await
                    .unwrap();
                state
                    .storage
                    .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
                    .await
                    .unwrap()
                    .unwrap()
            } else {
                Session::new("clear-ordinary", "model")
            };
            session.add_message(Message::user("retained conversation"));
            state.storage.save_session(&session).await.unwrap();
            let id = session.id.clone();
            let directory = home.path().join("sessions").join(&id);
            std::fs::write(directory.join("attachments/sentinel"), b"retained").unwrap();
            let now = chrono::Utc::now();
            let activation = state
                .session_store
                .claim_activation(&ActorActivationClaim {
                    actor_id: id.clone(),
                    run_id: "synthetic-clear-run".into(),
                    lease_owner: "synthetic-clear-owner".into(),
                    lease_expires_at: now + chrono::Duration::minutes(5),
                    inbox_generation: 0,
                    placement_ref: None,
                    now,
                })
                .await
                .unwrap();
            state
                .session_store
                .start_activation(&activation.fence(), now)
                .await
                .unwrap();
            match status {
                "finished" => {
                    state
                        .session_store
                        .finish_activation(
                            &activation.fence(),
                            now,
                            ActorActivationFinish::Succeeded,
                        )
                        .await
                        .unwrap();
                }
                "retired" => {
                    state.session_store.retire_actor(&id, now).await.unwrap();
                }
                "unknown" => {
                    std::fs::write(directory.join("actor-authority.json"), b"unknown").unwrap()
                }
                _ => {}
            }
            let warm = Arc::new(bamboo_engine::SessionSnapshot::new(session));
            let before_cache = serde_json::to_value(&*warm.read()).unwrap();
            state.sessions.insert(id.clone(), warm.clone());
            let before_files = files(&directory);
            let before_feed = feed_barrier(&state).await;
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .route("/sessions/{id}/clear", web::post().to(clear_session)),
            )
            .await;
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&format!("/sessions/{id}/clear"))
                    .to_request(),
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::CONFLICT,
                "{supervisor} {status}"
            );
            let body: Value = test::read_body_json(response).await;
            assert_eq!(body["error"]["code"], "session_clear_authority_conflict");
            assert_eq!(body["status"], "blocked");
            assert_eq!(files(&directory), before_files, "{supervisor} {status}");
            assert!(Arc::ptr_eq(&state.sessions.get(&id).unwrap(), &warm));
            assert_eq!(serde_json::to_value(&*warm.read()).unwrap(), before_cache);
            assert_eq!(feed_barrier(&state).await, before_feed + 1);
            assert!(state.agent_runners.read().await.is_empty());
            assert_eq!(probe.0.load(Ordering::SeqCst), 0);
        }
    }
}

#[actix_web::test]
async fn http_clear_io_failure_evicts_stale_cache_without_success_or_clear_event() {
    let (home, state, probe) = state().await;
    let mut session = Session::new("clear-io-failure", "model");
    session.add_message(Message::user("retained conversation"));
    state.storage.save_session(&session).await.unwrap();
    let id = session.id.clone();
    state.sessions.insert(
        id.clone(),
        Arc::new(bamboo_engine::SessionSnapshot::new(session)),
    );
    let directory = home.path().join("sessions").join(&id);
    let before_files = files(&directory);
    std::fs::remove_dir_all(directory.join("attachments")).unwrap();
    std::fs::write(directory.join("attachments"), b"synthetic cleanup failure").unwrap();
    let before_feed = feed_barrier(&state).await;
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/sessions/{id}/clear", web::post().to(clear_session)),
    )
    .await;
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri(&format!("/sessions/{id}/clear"))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["error"]["code"], "session_clear_outcome_unknown");
    assert_eq!(body["status"], "outcome_unknown");
    assert!(body.get("success").is_none());
    assert!(!state.sessions.contains_key(&id));
    assert_eq!(files(&directory), before_files);
    assert_eq!(feed_barrier(&state).await, before_feed + 1);
    assert_eq!(probe.0.load(Ordering::SeqCst), 0);
}
