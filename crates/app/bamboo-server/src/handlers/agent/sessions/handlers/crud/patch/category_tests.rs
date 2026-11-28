use std::{path::Path, sync::Arc, time::Duration};

use actix_web::{
    http::{header, StatusCode},
    test, web, App,
};
use bamboo_agent_core::{AgentEvent, Message, Session};
use bamboo_domain::{ProjectId, WorkspaceBinding, SESSION_CATEGORY_METADATA_KEY};
use bamboo_engine::{
    events::{journal, ChangeEvent},
    runtime::execution::runner_state::{AgentRunner, AgentStatus},
};
use chrono::Utc;
use serde_json::{json, Value};
use tokio::sync::broadcast;

use crate::{app_state::AppState, routes::configure_routes};

fn patch(id: &str, revision: Option<u64>, body: Value) -> test::TestRequest {
    let mut req = test::TestRequest::patch().uri(&format!("/api/v1/sessions/{id}"));
    if let Some(revision) = revision {
        req = req.insert_header((header::IF_MATCH, format!("\"{revision}\"")));
    }
    req.set_json(body)
}

fn detail(id: &str) -> test::TestRequest {
    test::TestRequest::get().uri(&format!("/api/v1/sessions/{id}"))
}

fn project_categories(id: &ProjectId) -> test::TestRequest {
    test::TestRequest::get().uri(&format!("/api/v1/projects/{id}/session-categories"))
}

async fn new_state(home: &Path) -> web::Data<AppState> {
    web::Data::new(Box::pin(AppState::new(home.to_path_buf())).await.unwrap())
}

async fn category_event(feed: &mut broadcast::Receiver<Arc<ChangeEvent>>) -> Arc<ChangeEvent> {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let event = feed.recv().await.unwrap();
            if matches!(event.event, AgentEvent::SessionCategoryUpdated { .. }) {
                break event;
            }
        }
    })
    .await
    .expect("category reaches the durable account feed")
}

#[actix_web::test]
async fn category_http_edits_converge_through_runtime_saves_project_moves_and_replay() {
    let home = tempfile::tempdir().unwrap();
    let left_workspace = tempfile::tempdir().unwrap();
    let left_worktree = tempfile::tempdir().unwrap();
    let right_workspace = tempfile::tempdir().unwrap();
    let unassigned_workspace = tempfile::tempdir().unwrap();
    let state = new_state(home.path()).await;
    let left = state
        .project_store
        .create_with_project_path(
            "Left Project",
            None,
            left_workspace.path().to_string_lossy(),
            vec![WorkspaceBinding {
                path: left_worktree.path().to_string_lossy().into_owned(),
                label: None,
                git_common_dir: None,
            }],
        )
        .unwrap()
        .id;
    let right = state
        .project_store
        .create_with_project_path(
            "Right Project",
            None,
            right_workspace.path().to_string_lossy(),
            vec![],
        )
        .unwrap()
        .id;

    let mut root = Session::new("category-root", "test-model");
    root.set_project_id_meta(left.to_string());
    root.set_workspace_path_meta(left_workspace.path().to_string_lossy());
    state.storage.save_session(&root).await.unwrap();
    let mut child = Session::new_child_of("category-child", &root, "test-model", "Child");
    child.set_project_id_meta(left.to_string());
    state.storage.save_session(&child).await.unwrap();
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    let cursor = state.account_sink.latest_seq();
    let mut feed = state.account_sink.subscribe();

    let original: Value = test::call_and_read_body_json(&app, detail(&root.id).to_request()).await;
    assert!(original["session"]
        .as_object()
        .unwrap()
        .contains_key("category"));
    assert_eq!(original["session"]["category"], Value::Null);
    for (id, revision, body, status) in [
        (
            root.id.as_str(),
            None,
            json!({"category": "Support"}),
            StatusCode::PRECONDITION_REQUIRED,
        ),
        (
            root.id.as_str(),
            Some(0),
            json!({"category": "Support", "title": "Mixed"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            root.id.as_str(),
            Some(0),
            json!({"category": null, "pinned": true}),
            StatusCode::BAD_REQUEST,
        ),
        (
            root.id.as_str(),
            Some(0),
            json!({"category": "Support", "project_id": null}),
            StatusCode::BAD_REQUEST,
        ),
        (
            root.id.as_str(),
            Some(0),
            json!({"category": "Support", "title": null}),
            StatusCode::BAD_REQUEST,
        ),
        (
            root.id.as_str(),
            Some(0),
            json!({"category": "  "}),
            StatusCode::BAD_REQUEST,
        ),
        (
            root.id.as_str(),
            Some(0),
            json!({"category": "Support\tOps"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            root.id.as_str(),
            Some(0),
            json!({"category": "研".repeat(81)}),
            StatusCode::BAD_REQUEST,
        ),
        (
            child.id.as_str(),
            Some(0),
            json!({"category": "Support"}),
            StatusCode::CONFLICT,
        ),
    ] {
        let response = test::call_service(&app, patch(id, revision, body).to_request()).await;
        assert_eq!(response.status(), status, "{id}, revision {revision:?}");
    }
    assert_eq!(
        state
            .storage
            .load_session(&root.id)
            .await
            .unwrap()
            .unwrap()
            .metadata_version,
        0
    );

    let mut stale = state.storage.load_session(&root.id).await.unwrap().unwrap();
    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Running;
    state
        .agent_runners
        .write()
        .await
        .insert(root.id.clone(), runner);
    let response = test::call_service(
        &app,
        patch(&root.id, Some(0), json!({"category": "  Support  "})).to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(header::ETAG).unwrap(), "\"1\"");
    let saved: Value = test::read_body_json(response).await;
    assert_eq!(saved["session"]["category"], "Support");
    assert_eq!(saved["session"]["is_running"], true);
    let set = category_event(&mut feed).await;

    let listed: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri("/api/v1/sessions?kind=root")
            .to_request(),
    )
    .await;
    assert!(listed["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| { row["id"] == root.id && row["category"] == "Support" }));

    let stale_retry = test::call_service(
        &app,
        patch(&root.id, Some(0), json!({"category": "Support"})).to_request(),
    )
    .await;
    assert_eq!(stale_retry.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(stale_retry.headers().get(header::ETAG).unwrap(), "\"1\"");
    for unchanged in [json!({"category": "Support"}), json!({}), json!([null])] {
        let no_op =
            test::call_service(&app, patch(&root.id, Some(1), unchanged).to_request()).await;
        assert_eq!(no_op.status(), StatusCode::OK);
        assert_eq!(no_op.headers().get(header::ETAG).unwrap(), "\"1\"");
        let unchanged: Value = test::read_body_json(no_op).await;
        assert_eq!(unchanged["session"]["category"], "Support");
    }

    stale.add_message(Message::user("Keep the runner message"));
    stale
        .metadata
        .insert("fixture.runtime-note".into(), "kept".into());
    state
        .persistence
        .merge_save_runtime(&mut stale)
        .await
        .unwrap();
    assert_eq!(stale.category(), Some("Support"));
    let categories: Value =
        test::call_and_read_body_json(&app, project_categories(&left).to_request()).await;
    assert_eq!(
        categories,
        json!({"categories": [{"name": "Support", "session_count": 1}]})
    );
    assert!(state.agent_runners.write().await.remove(&root.id).is_some());

    let response = test::call_service(
        &app,
        patch(&root.id, Some(1), json!({"category": "調査"})).to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(header::ETAG).unwrap(), "\"2\"");
    let changed_receipt: Value = test::read_body_json(response).await;
    let changed = category_event(&mut feed).await;
    for (version, body, project) in [
        (
            2,
            json!({"workspace_path": left_worktree.path()}),
            json!(left),
        ),
        (
            3,
            json!({"project_id": null, "workspace_path": unassigned_workspace.path()}),
            Value::Null,
        ),
        (
            4,
            json!({"project_id": right, "workspace_path": right_workspace.path()}),
            json!(right),
        ),
    ] {
        let response =
            test::call_service(&app, patch(&root.id, Some(version), body).to_request()).await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "Project/workspace edit at {version}"
        );
        assert_eq!(
            response
                .headers()
                .get(header::ETAG)
                .unwrap()
                .to_str()
                .unwrap(),
            format!("\"{}\"", version + 1)
        );
        let detail: Value = test::read_body_json(response).await;
        assert_eq!(detail["session"]["category"], "調査");
        assert_eq!(detail["session"]["project_id"], project);
    }
    let left_counts: Value =
        test::call_and_read_body_json(&app, project_categories(&left).to_request()).await;
    assert_eq!(left_counts, json!({"categories": []}));
    drop(app);
    state.shutdown().await;
    drop(state);

    let reopened = new_state(home.path()).await;
    let app = test::init_service(
        App::new()
            .app_data(reopened.clone())
            .configure(configure_routes),
    )
    .await;
    let mut feed = reopened.account_sink.subscribe();
    let mut before_clear = reopened
        .storage
        .load_session(&root.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before_clear.category(), Some("調査"));
    let listed: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri("/api/v1/sessions?kind=root")
            .to_request(),
    )
    .await;
    assert!(listed["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| { row["id"] == root.id && row["category"] == "調査" }));
    assert_eq!(
        before_clear
            .metadata
            .get("fixture.runtime-note")
            .map(String::as_str),
        Some("kept")
    );
    assert!(before_clear
        .messages
        .iter()
        .any(|message| message.content == "Keep the runner message"));
    let right_counts: Value =
        test::call_and_read_body_json(&app, project_categories(&right).to_request()).await;
    assert_eq!(
        right_counts,
        json!({"categories": [{"name": "調査", "session_count": 1}]})
    );

    let clear = test::call_service(
        &app,
        patch(&root.id, Some(5), json!({"category": null})).to_request(),
    )
    .await;
    assert_eq!(clear.status(), StatusCode::OK);
    assert_eq!(clear.headers().get(header::ETAG).unwrap(), "\"6\"");
    let receipt: Value = test::read_body_json(clear).await;
    assert_eq!(receipt["session"]["category"], Value::Null);
    let cleared = category_event(&mut feed).await;
    before_clear.add_message(Message::user("Keep the later stale runner message"));
    reopened
        .persistence
        .merge_save_runtime(&mut before_clear)
        .await
        .unwrap();
    assert_eq!(before_clear.category(), None);
    assert_eq!(before_clear.metadata_version, 6);
    let right_counts: Value =
        test::call_and_read_body_json(&app, project_categories(&right).to_request()).await;
    assert_eq!(right_counts, json!({"categories": []}));

    let replay = journal::read_since(reopened.account_sink.events_dir(), cursor).unwrap();
    let category_events: Vec<_> = replay
        .iter()
        .filter(|change| matches!(change.event, AgentEvent::SessionCategoryUpdated { .. }))
        .collect();
    assert_eq!(category_events.len(), 3);
    for (replayed, live, category, project, revision, updated_at) in [
        (
            category_events[0],
            set,
            json!("Support"),
            left.clone(),
            1,
            &saved["session"]["updated_at"],
        ),
        (
            category_events[1],
            changed,
            json!("調査"),
            left,
            2,
            &changed_receipt["session"]["updated_at"],
        ),
        (
            category_events[2],
            cleared,
            Value::Null,
            right,
            6,
            &receipt["session"]["updated_at"],
        ),
    ] {
        assert_eq!(replayed.seq, live.seq);
        let payload = serde_json::to_value(&replayed.event).unwrap();
        assert_eq!(payload, serde_json::to_value(&live.event).unwrap());
        assert_eq!(payload["session_id"], root.id);
        assert_eq!(payload["category"], category);
        assert_eq!(payload["metadata_version"], revision);
        assert_eq!(payload["project_id"], project.as_str());
        assert_eq!(&payload["updated_at"], updated_at);
    }
    assert_eq!(
        category_events[2].event.session_id(),
        Some(root.id.as_str())
    );
    reopened.shutdown().await;
}

#[actix_web::test]
async fn category_counts_use_the_full_root_index_for_the_exact_project() {
    let home = tempfile::tempdir().unwrap();
    let state = new_state(home.path()).await;
    let project = state.project_store.create("Project", None).unwrap().id;
    let other = state
        .project_store
        .create("Other Project", None)
        .unwrap()
        .id;

    let mut oldest = Session::new("old-category-root", "test-model");
    oldest.set_project_id_meta(project.to_string());
    oldest.created_at = Utc::now() - chrono::Duration::days(1);
    oldest.updated_at = oldest.created_at;
    oldest
        .metadata
        .insert(SESSION_CATEGORY_METADATA_KEY.into(), "Support".into());
    state.storage.save_session(&oldest).await.unwrap();
    for index in 0..200 {
        let mut root = Session::new(format!("category-filler-{index}"), "test-model");
        root.set_project_id_meta(project.to_string());
        if index == 0 {
            root.metadata
                .insert(SESSION_CATEGORY_METADATA_KEY.into(), "Build".into());
        }
        state.storage.save_session(&root).await.unwrap();
    }
    let mut child = Session::new_child_of("legacy-category-child", &oldest, "test-model", "Child");
    child.set_project_id_meta(project.to_string());
    child
        .metadata
        .insert(SESSION_CATEGORY_METADATA_KEY.into(), "Support".into());
    state.storage.save_session(&child).await.unwrap();
    let mut unrelated = Session::new("other-project-category", "test-model");
    unrelated.set_project_id_meta(other.to_string());
    unrelated
        .metadata
        .insert(SESSION_CATEGORY_METADATA_KEY.into(), "Support".into());
    state.storage.save_session(&unrelated).await.unwrap();

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    let first_page: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri("/api/v1/sessions?kind=root")
            .to_request(),
    )
    .await;
    assert_eq!(first_page["sessions"].as_array().unwrap().len(), 200);
    assert!(first_page["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["id"] != oldest.id));
    let counts: Value =
        test::call_and_read_body_json(&app, project_categories(&project).to_request()).await;
    assert_eq!(
        counts,
        json!({"categories": [
            {"name": "Build", "session_count": 1},
            {"name": "Support", "session_count": 1},
        ]})
    );
    let missing =
        test::call_service(&app, project_categories(&ProjectId::new()).to_request()).await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    state.shutdown().await;
}
