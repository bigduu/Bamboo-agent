//! Existing HTTP and storage APIs; the provider is an in-process counter only.
use actix_web::{http::StatusCode, test, web, App};
use async_trait::async_trait;
use bamboo_domain::{Message, Session};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
use serde_json::{json, Value};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Default)]
struct Probe(AtomicUsize);

#[async_trait]
impl LLMProvider for Probe {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token("local fixture only".into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn state() -> (tempfile::TempDir, web::Data<crate::AppState>, Arc<Probe>) {
    let home = tempfile::tempdir().unwrap();
    let mut config = bamboo_llm::Config::from_data_dir(Some(home.path().into()));
    config.provider = "openai".into();
    config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
        model: Some("test-model".into()),
        ..Default::default()
    });
    let probe = Arc::new(Probe::default());
    let state = crate::AppState::new_with_provider(home.path().into(), config, probe.clone())
        .await
        .unwrap();
    (home, web::Data::new(state), probe)
}

// Model an on-disk legacy/future producer rather than attempting a forbidden
// migration through the current host writer. Identity/proof fields stay intact.
async fn stamp(home: &Path, id: &str, version: Option<u32>) {
    for filename in ["session.json", "runtime.json"] {
        let path = home.join("sessions").join(id).join(filename);
        let mut raw: Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        raw.as_object_mut()
            .unwrap()
            .remove("_bamboo_main_authority");
        match version {
            Some(version) => raw["session_version"] = json!(version),
            None => {
                raw.as_object_mut().unwrap().remove("session_version");
            }
        }
        tokio::fs::write(path, serde_json::to_vec_pretty(&raw).unwrap())
            .await
            .unwrap();
    }
}

async fn pair(home: &Path, id: &str) -> (Vec<u8>, Vec<u8>) {
    let dir = home.join("sessions").join(id);
    (
        tokio::fs::read(dir.join("session.json")).await.unwrap(),
        tokio::fs::read(dir.join("runtime.json")).await.unwrap(),
    )
}

#[actix_web::test]
async fn session_version_http_future_is_readable_but_chat_and_execute_refuse_without_mutation() {
    for ultra in [false, true] {
        let (home, state, probe) = state().await;
        let id = format!("future-http-{ultra}");
        let mut session = Session::new(&id, "test-model");
        session.set_root_orchestration_only(ultra).unwrap();
        session.add_message(Message::user("Existing preserved history"));
        state.storage.save_session(&session).await.unwrap();
        stamp(home.path(), &id, Some(99)).await;
        let before = pair(home.path(), &id).await;
        let backlog = state.session_inbox.inspect(&id).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/sessions/{id}", web::get().to(super::super::get_session))
                .route(
                    "/history/{id}",
                    web::get().to(crate::handlers::agent::history::handler),
                )
                .route(
                    "/chat",
                    web::post().to(crate::handlers::agent::chat::handler),
                )
                .route(
                    "/execute/{id}",
                    web::post().to(crate::handlers::agent::execute::handler),
                ),
        )
        .await;
        for request in [
            test::TestRequest::post()
                .uri("/chat")
                .set_json(
                    json!({"session_id":id,"message":"must not enter Inbox","model":"test-model"}),
                )
                .to_request(),
            test::TestRequest::post()
                .uri(&format!("/execute/{id}"))
                .set_json(json!({}))
                .to_request(),
        ] {
            let response = test::call_service(&app, request).await;
            assert_eq!(response.status(), StatusCode::CONFLICT);
            let body: Value = test::read_body_json(response).await;
            assert_eq!(body["error"]["code"], "session_execution_incompatible");
            assert_eq!(body["compatibility"]["history_readable"], true);
            assert_eq!(body["compatibility"]["execution_allowed"], false);
        }
        let detail = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/sessions/{id}"))
                .to_request(),
        )
        .await;
        assert_eq!(detail.status(), StatusCode::OK);
        let detail: Value = test::read_body_json(detail).await;
        assert_eq!(
            detail["session"]["execution_compatibility"]["status"],
            "conflict"
        );
        assert_eq!(
            detail["session"]["execution_compatibility"]["session_version"],
            99
        );
        let history = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/history/{id}"))
                .to_request(),
        )
        .await;
        assert_eq!(history.status(), StatusCode::OK);
        let history: Value = test::read_body_json(history).await;
        assert!(history["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["content"] == "Existing preserved history"));
        assert_eq!(pair(home.path(), &id).await, before);
        assert_eq!(state.session_inbox.inspect(&id).await.unwrap(), backlog);
        assert!(state.agent_runners.read().await.is_empty());
        assert_eq!(probe.0.load(Ordering::SeqCst), 0);
    }
}

#[actix_web::test]
async fn session_version_http_legacy_detail_is_compatible_and_does_not_stamp_data() {
    let (home, state, probe) = state().await;
    let id = "legacy-http";
    state
        .storage
        .save_session(&Session::new(id, "test-model"))
        .await
        .unwrap();
    stamp(home.path(), id, None).await;
    let before = pair(home.path(), id).await;
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/sessions/{id}", web::get().to(super::super::get_session)),
    )
    .await;
    let detail = test::call_service(
        &app,
        test::TestRequest::get()
            .uri(&format!("/sessions/{id}"))
            .to_request(),
    )
    .await;
    assert_eq!(detail.status(), StatusCode::OK);
    let detail: Value = test::read_body_json(detail).await;
    let report = &detail["session"]["execution_compatibility"];
    assert_eq!(report["status"], "legacy_compatible");
    assert!(report["session_version"].is_null());
    assert_eq!(report["execution_allowed"], true);
    assert_eq!(report["conflicts"], json!([]));
    assert_eq!(pair(home.path(), id).await, before);
    assert_eq!(probe.0.load(Ordering::SeqCst), 0);
}

#[actix_web::test]
async fn session_version_future_storage_reads_survive_reopen_without_rewriting() {
    use bamboo_agent_core::storage::Storage;
    let home = tempfile::tempdir().unwrap();
    let store = bamboo_storage::SessionStoreV2::new(home.path().into())
        .await
        .unwrap();
    let id = "future-storage";
    let mut session = Session::new(id, "model");
    session.add_message(Message::user("future history"));
    store.save_session(&session).await.unwrap();
    stamp(home.path(), id, Some(99)).await;
    let before = pair(home.path(), id).await;
    drop(store);
    let store = bamboo_storage::SessionStoreV2::new(home.path().into())
        .await
        .unwrap();
    let loaded = store.load_session(id).await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(&loaded).unwrap()["session_version"],
        99
    );
    assert_eq!(loaded.messages[0].content, "future history");
    assert_eq!(pair(home.path(), id).await, before);
}

#[actix_web::test]
async fn session_version_http_future_workflow_stays_opaque_on_detail_read() {
    let (home, state, probe) = state().await;
    let id = "future-workflow-detail";
    let mut session = Session::new(id, "test-model");
    session.metadata.insert(
        bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY.into(),
        r#"{"future_workflow_state":true}"#.into(),
    );
    session.add_message(Message::user(
        "readable without interpreting retained future state",
    ));
    state.storage.save_session(&session).await.unwrap();
    stamp(home.path(), id, Some(99)).await;
    let before = pair(home.path(), id).await;
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/sessions/{id}", web::get().to(super::super::get_session)),
    )
    .await;
    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri(&format!("/sessions/{id}"))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let detail: Value = test::read_body_json(response).await;
    assert_eq!(
        detail["session"]["execution_compatibility"]["status"],
        "conflict"
    );
    assert_eq!(
        detail["session"]["execution_compatibility"]["history_readable"],
        true
    );
    assert_eq!(pair(home.path(), id).await, before);
    assert_eq!(probe.0.load(Ordering::SeqCst), 0);
}

fn clear_files(home: &Path, directory: &Path) -> Vec<Option<Vec<u8>>> {
    let mut files = [
        "session.json",
        "runtime.json",
        "root-tool-authority.json",
        "actor-authority.json",
        "actor-authority.initialized.json",
        ".search-index-revision",
        "attachments/sentinel",
    ]
    .map(|name| std::fs::read(directory.join(name)).ok())
    .to_vec();
    files.push(std::fs::read(home.join("sessions.json")).ok());
    files
}

// A confirmed unrelated marker drains the same FIFO as SessionCleared. This
// observes publication deterministically; the marker itself is excluded from
// each assertion about the target session's clear event.
async fn clear_feed_barrier(state: &crate::AppState) -> u64 {
    let marker = format!("clear-feed-barrier-{}", uuid::Uuid::new_v4());
    assert!(
        state
            .account_sink
            .record_confirmed(
                Some(&marker),
                &bamboo_agent_core::AgentEvent::SessionCreated {
                    session_id: marker.clone(),
                    project_id: None,
                    title: "clear feed barrier".into(),
                    kind: bamboo_domain::SessionKind::Root,
                    created_at: chrono::Utc::now(),
                },
            )
            .await
    );
    state.account_sink.latest_seq()
}

#[actix_web::test]
async fn session_version_writer_http_clear_uses_fresh_contract_and_keeps_cache_feed_and_files() {
    let mut failures = Vec::new();
    for child in [false, true] {
        let (home, state, probe) = state().await;
        let root = Session::new("clear-http-root", "test-model");
        state.storage.save_session(&root).await.unwrap();
        let mut session = if child {
            Session::new_child_of("clear-http-child", &root, "test-model", "child")
        } else {
            root
        };
        session.add_message(Message::user("retained clear history"));
        state.storage.save_session(&session).await.unwrap();
        let id = session.id.clone();
        let warm = Arc::new(bamboo_engine::SessionSnapshot::new(session));
        state.sessions.insert(id.clone(), warm.clone());
        let directory = if child {
            home.path()
                .join("sessions/clear-http-root/children")
                .join(&id)
        } else {
            home.path().join("sessions").join(&id)
        };
        std::fs::write(
            directory.join("attachments/sentinel"),
            b"retained attachment",
        )
        .unwrap();
        let main = directory.join("session.json");
        let mut raw: Value = serde_json::from_slice(&std::fs::read(&main).unwrap()).unwrap();
        raw.as_object_mut()
            .unwrap()
            .remove("_bamboo_main_authority");
        raw["session_version"] = json!(99);
        raw["future_writer_extension"] = json!({"retained": true});
        std::fs::write(main, serde_json::to_vec_pretty(&raw).unwrap()).unwrap();
        let before = clear_files(home.path(), &directory);
        let cache_before = serde_json::to_value(&*warm.read()).unwrap();
        let feed_before = clear_feed_barrier(&state).await;
        let app = test::init_service(App::new().app_data(state.clone()).route(
            "/sessions/{id}/clear",
            web::post().to(crate::handlers::agent::sessions::clear_session),
        ))
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/sessions/{id}/clear"))
                .to_request(),
        )
        .await;
        let status = response.status();
        let body: Value = test::read_body_json(response).await;
        let refused = status == StatusCode::CONFLICT
            && body["error"]["code"] == "session_execution_incompatible"
            && body["compatibility"]["session_version"] == 99;
        let files_retained = clear_files(home.path(), &directory) == before;
        let cache = state.sessions.get(&id).unwrap();
        let cache_retained = Arc::ptr_eq(&cache, &warm)
            && serde_json::to_value(&*cache.read()).unwrap() == cache_before;
        let no_publication = clear_feed_barrier(&state).await == feed_before + 1;
        assert!(state.agent_runners.read().await.is_empty());
        assert_eq!(probe.0.load(Ordering::SeqCst), 0);
        eprintln!("HTTP clear contract child={child}: refused={refused} files_retained={files_retained} cache_retained={cache_retained} no_publication={no_publication}");
        if !refused || !files_retained || !cache_retained || !no_publication {
            failures.push(format!("child={child}: status={status}, body={body}, files={files_retained}, cache={cache_retained}, no_publication={no_publication}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[actix_web::test]
async fn session_version_writer_http_clear_compatible_legacy_keeps_existing_success_publication() {
    for stamp_value in [None, Some(1)] {
        let (home, state, probe) = state().await;
        let id = "compatible-clear-http";
        let mut session = Session::new(id, "test-model");
        session.add_message(Message::user("clear compatible history"));
        state.storage.save_session(&session).await.unwrap();
        stamp(home.path(), id, stamp_value).await;
        let session = state.storage.load_session(id).await.unwrap().unwrap();
        let warm = Arc::new(bamboo_engine::SessionSnapshot::new(session));
        state.sessions.insert(id.into(), warm.clone());
        let feed_before = clear_feed_barrier(&state).await;
        let app = test::init_service(App::new().app_data(state.clone()).route(
            "/sessions/{id}/clear",
            web::post().to(crate::handlers::agent::sessions::clear_session),
        ))
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/sessions/{id}/clear"))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!Arc::ptr_eq(&state.sessions.get(id).unwrap(), &warm));
        let cleared = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(cleared.messages.is_empty());
        assert_eq!(cleared.session_version, stamp_value);
        assert_eq!(clear_feed_barrier(&state).await, feed_before + 2);
        assert_eq!(probe.0.load(Ordering::SeqCst), 0);
    }
}
