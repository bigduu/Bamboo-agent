use std::path::Path;

use super::*;
use actix_web::http::header;
use actix_web::{test, App};
use bamboo_agent_core::AgentEvent;
use bamboo_engine::events::journal;
use serde_json::{json, Value};
use tempfile::tempdir;

use crate::routes::configure_routes;

async fn app_state(home: &Path) -> web::Data<AppState> {
    web::Data::new(Box::pin(AppState::new(home.to_path_buf())).await.unwrap())
}

#[actix_web::test]
async fn initial_category_is_committed_and_replay_returns_later_edits() {
    let home = tempdir().unwrap();
    let first_workspace = tempdir().unwrap();
    let later_workspace = tempdir().unwrap();
    let state = app_state(home.path()).await;
    let first_project = state
        .project_store
        .create_with_project_path(
            "Original project",
            None,
            first_workspace.path().to_string_lossy(),
            Vec::new(),
        )
        .unwrap();
    let later_project = state
        .project_store
        .create_with_project_path(
            "Later project",
            None,
            later_workspace.path().to_string_lossy(),
            Vec::new(),
        )
        .unwrap();
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    let payload = json!({
        "title": "Before any messages",
        "project_id": first_project.id,
        "category": "  Support  ",
    });
    let create_request = |payload: &Value| {
        test::TestRequest::post()
            .uri("/api/v1/sessions")
            .insert_header((IDEMPOTENCY_KEY_HEADER, "categorized-create"))
            .set_json(payload)
            .to_request()
    };
    let first = test::call_service(&app, create_request(&payload)).await;
    assert_eq!(first.status(), StatusCode::CREATED);
    let first: Value = test::read_body_json(first).await;
    let id = first["session"]["id"].as_str().unwrap();
    assert_eq!(first["session"]["category"], "Support");
    assert_eq!(first["session"]["project_id"], first_project.id.as_str());
    let durable = state.storage.load_session(id).await.unwrap().unwrap();
    assert!(durable.messages.is_empty());
    assert_eq!(durable.category(), Some("Support"));
    assert_eq!(durable.workspace_path_meta(), first_project.project_path);
    let entry = state.session_store.get_index_entry(id).await.unwrap();
    assert_eq!(entry.category.as_deref(), Some("Support"));

    let categories_path = format!("/api/v1/projects/{}/session-categories", first_project.id);
    let expected_categories = json!({"categories": [{"name": "Support", "session_count": 1}]});
    let first_counts: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get().uri(&categories_path).to_request(),
    )
    .await;
    assert_eq!(first_counts, expected_categories);

    for (field, changed) in [
        ("category", json!("Investigation")),
        ("project_id", json!(later_project.id)),
    ] {
        let mut changed_payload = payload.clone();
        changed_payload[field] = changed;
        let conflict = test::call_service(&app, create_request(&changed_payload)).await;
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        let conflict: Value = test::read_body_json(conflict).await;
        assert_eq!(conflict["error"]["code"], "idempotency_key_conflict");
    }
    drop(app);
    state.shutdown().await;
    drop(state);

    let reopened = app_state(home.path()).await;
    let app = test::init_service(
        App::new()
            .app_data(reopened.clone())
            .configure(configure_routes),
    )
    .await;
    let session_path = format!("/api/v1/sessions/{id}");
    let status_request = || {
        test::TestRequest::get()
            .uri("/api/v1/session-create-operations/categorized-create")
            .to_request()
    };
    let detail = test::call_service(
        &app,
        test::TestRequest::get().uri(&session_path).to_request(),
    )
    .await;
    assert_eq!(detail.status(), StatusCode::OK);
    let mut etag = detail.headers().get(header::ETAG).unwrap().clone();
    let detail: Value = test::read_body_json(detail).await;
    assert_eq!(detail["session"]["category"], "Support");
    let counts: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get().uri(&categories_path).to_request(),
    )
    .await;
    assert_eq!(counts, expected_categories);
    let status: Value = test::call_and_read_body_json(&app, status_request()).await;
    assert_eq!(status["status"], "succeeded");
    assert_eq!(status["session"]["category"], "Support");

    let edits = [
        (
            json!({"category": "Investigation"}),
            json!("Investigation"),
            &first_project.id,
        ),
        (
            json!({"project_id": later_project.id}),
            json!("Investigation"),
            &later_project.id,
        ),
        (json!({"category": null}), Value::Null, &later_project.id),
    ];
    for (patch, category, project) in edits {
        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&session_path)
                .insert_header((header::IF_MATCH, etag))
                .set_json(patch)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        etag = response.headers().get(header::ETAG).unwrap().clone();
        let replay = test::call_service(&app, create_request(&payload)).await;
        assert_eq!(replay.status(), StatusCode::OK);
        let replay: Value = test::read_body_json(replay).await;
        assert_eq!(replay["session"]["id"], id);
        assert_eq!(replay["session"]["project_id"], project.as_str());
        assert_eq!(replay["session"]["category"], category);
        let status: Value = test::call_and_read_body_json(&app, status_request()).await;
        assert_eq!(status["status"], "succeeded");
        assert_eq!(status["session"]["id"], id);
        assert_eq!(status["session"]["project_id"], project.as_str());
        assert_eq!(status["session"]["category"], category);
    }
    let durable = reopened.storage.load_session(id).await.unwrap().unwrap();
    assert_eq!(durable.category(), None);
    assert_eq!(
        durable.project_id_meta().as_deref(),
        Some(later_project.id.as_str())
    );
    assert_eq!(durable.workspace_path_meta(), later_project.project_path);
    let records = journal::read_since(reopened.account_sink.events_dir(), 0).unwrap();
    let creations: Vec<_> = records
        .into_iter()
        .filter_map(|change| match change.event {
            AgentEvent::SessionCreated {
                session_id,
                project_id,
                category,
                ..
            } if session_id == id => Some((project_id, category)),
            _ => None,
        })
        .collect();
    assert_eq!(
        creations,
        [(
            Some(first_project.id.to_string()),
            Some("Support".to_string())
        )]
    );
    assert_eq!(reopened.session_store.list_index_entries().await.len(), 1);
    reopened.shutdown().await;
}

#[actix_web::test]
async fn omitted_and_null_category_replay_a_pre_upgrade_receipt() {
    // The pre-upgrade typed request serialized all eleven original fields.
    const LEGACY_FINGERPRINT: &str =
        "e4918eae7eeb48c05bcf4dfd646d2d4fb7dbc8339de90a1632b9d9c7913263da";
    let home = tempdir().unwrap();
    let state = app_state(home.path()).await;
    let session = Session::new(Uuid::new_v4().to_string(), "model");
    state.storage.save_session(&session).await.unwrap();
    let key = "legacy-receipt";
    let mut operation = SessionCreateOperationRecord::pending(
        key_digest(key),
        LEGACY_FINGERPRINT.into(),
        session.id.clone(),
    );
    operation.mark_succeeded();
    state
        .session_create_operations
        .save(&operation)
        .await
        .unwrap();
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    for payload in [
        json!({"title": "Legacy create"}),
        json!({"title": "Legacy create", "category": null}),
    ] {
        let request: CreateSessionRequest = serde_json::from_value(payload.clone()).unwrap();
        assert_eq!(payload_fingerprint(&request).unwrap(), LEGACY_FINGERPRINT);
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                .set_json(payload)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["session"]["id"], session.id);
        assert_eq!(body["session"]["category"], Value::Null);
    }
    assert_eq!(state.session_store.list_index_entries().await.len(), 1);
    assert!(journal::read_since(state.account_sink.events_dir(), 0)
        .unwrap()
        .iter()
        .all(|entry| !matches!(&entry.event, AgentEvent::SessionCreated { .. })));
    state.shutdown().await;
}

#[actix_web::test]
async fn invalid_initial_category_or_assignment_creates_no_session_or_event() {
    let home = tempdir().unwrap();
    let state = app_state(home.path()).await;
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    let invalid = [
        (json!({"category": "  "}), StatusCode::BAD_REQUEST),
        (
            json!({"category": "with\ncontrol"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"category": "文".repeat(81)}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"category": "Support", "project_id": bamboo_domain::ProjectId::new()}),
            StatusCode::NOT_FOUND,
        ),
        (
            json!({"category": "Support", "workspace_path": "not/absolute"}),
            StatusCode::BAD_REQUEST,
        ),
    ];
    for (body, status) in invalid {
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(body)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), status);
    }
    assert!(state.session_store.list_index_entries().await.is_empty());
    assert!(journal::read_since(state.account_sink.events_dir(), 0)
        .unwrap()
        .iter()
        .all(|entry| !matches!(&entry.event, AgentEvent::SessionCreated { .. })));
    state.shutdown().await;
}
