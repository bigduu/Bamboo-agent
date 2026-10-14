use super::*;

use actix_web::{body::to_bytes, http::header, test, App};
use bamboo_domain::{Session, Storage};
use serde_json::{json, Value};
use std::sync::Arc;

const SECTION: &str = "section:2d567c47-a081-4a8d-9c9e-246bce470c9f";
const EMPTY_SECTION: &str = "section:28f595c0-21dc-4a86-8b83-5f8b258386fa";

async fn fixture() -> (tempfile::TempDir, web::Data<AppState>, ProjectManifest) {
    let dir = tempfile::tempdir().unwrap();
    let state = Box::pin(AppState::new(dir.path().to_path_buf()))
        .await
        .unwrap();
    let project = state.project_store.create("Zenith", None).unwrap();
    (dir, web::Data::new(state), project)
}

async fn save_root(state: &AppState, id: &str, project: Option<&ProjectId>) -> Session {
    let mut session = Session::new(id, "test-model");
    if let Some(project) = project {
        session.set_project_id_meta(project.to_string());
    }
    state.storage.save_session(&session).await.unwrap();
    session
}

fn snapshot(assignments: Value) -> Value {
    json!({
        "sections": [{"id": SECTION, "name": " Support "}, {"id": EMPTY_SECTION, "name": "Empty"}],
        "session_sections": assignments,
        "hidden_legacy": false
    })
}

async fn patch(
    state: &web::Data<AppState>,
    id: &ProjectId,
    revision: Option<u64>,
    body: Value,
) -> HttpResponse {
    let mut feed = state.account_sink.subscribe();
    let mut request = test::TestRequest::patch();
    if let Some(revision) = revision {
        request = request.insert_header((header::IF_MATCH, format!("\"{revision}\"")));
    }
    let response = patch_project(
        state.clone(),
        web::Path::from(id.to_string()),
        request.to_http_request(),
        web::Json(serde_json::from_value(body).unwrap()),
    )
    .await
    .unwrap();
    if response.status().is_success() {
        // Project writes enqueue their existing durable event; wait for its
        // confirmed journal/broadcast boundary before asserting event absence
        // for a later rejected request.
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(&event.event, AgentEvent::ProjectUpdated { project_id, .. } if project_id == id.as_str())
        );
    }
    response
}

async fn body(response: HttpResponse) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body()).await.unwrap()).unwrap()
}

fn assert_unchanged(state: &AppState, project: &ProjectManifest, cursor: u64) {
    assert_eq!(state.project_store.get(&project.id).unwrap(), *project);
    assert_eq!(state.account_sink.latest_seq(), cursor);
}

#[actix_web::test]
async fn sidebar_support_is_explicit_and_snapshot_reopens_with_empty_sections_and_durable_event() {
    let (dir, state, project) = fixture().await;
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/projects", web::get().to(list_projects))
            .route("/projects/{id}", web::get().to(get_project)),
    )
    .await;
    for uri in ["/projects".to_string(), format!("/projects/{}", project.id)] {
        let response =
            test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;
        let value: Value = test::read_body_json(response).await;
        let value = if uri == "/projects" {
            &value["projects"][0]
        } else {
            &value
        };
        assert_eq!(value.get("sidebar"), Some(&Value::Null));
    }
    save_root(&state, "assigned", Some(&project.id)).await;
    save_root(&state, "unmoved", Some(&project.id)).await;
    let mut feed = state.account_sink.subscribe();
    let cursor = state.account_sink.latest_seq();
    let response = patch(
        &state,
        &project.id,
        Some(1),
        json!({"sidebar": snapshot(json!({"assigned": SECTION}))}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(header::ETAG).unwrap(), "\"2\"");
    let saved: ProjectManifest = serde_json::from_value(body(response).await).unwrap();
    let sidebar = saved.sidebar.as_ref().unwrap();
    assert_eq!(sidebar.sections.len(), 2);
    assert_eq!(sidebar.sections[0].name, "Support");
    assert!(!sidebar.session_sections.contains_key("unmoved"));
    let event = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(&event.event, AgentEvent::ProjectUpdated { project_id, revision: 2 } if project_id == project.id.as_str())
    );
    let replay =
        bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), cursor)
            .unwrap();
    assert!(replay.iter().any(|change| matches!(&change.event, AgentEvent::ProjectUpdated { project_id, revision: 2 } if project_id == project.id.as_str())));
    let reopened = bamboo_projects::ProjectStore::open(dir.path()).unwrap();
    assert_eq!(reopened.get(&project.id).unwrap(), saved);
    assert_eq!(reopened.list().unwrap(), vec![saved.clone()]);
    assert_eq!(
        reopened.index().unwrap().projects[&project.id].sidebar,
        saved.sidebar
    );
}

#[actix_web::test]
async fn sidebar_rejects_foreign_child_missing_and_unassigned_roots_without_side_effects() {
    let (_dir, state, project) = fixture().await;
    let foreign = state.project_store.create("Other", None).unwrap();
    let root = save_root(&state, "assigned", Some(&project.id)).await;
    save_root(&state, "foreign", Some(&foreign.id)).await;
    save_root(&state, "unassigned", None).await;
    let child = Session::new_child_of("child", &root, "test-model", "Child");
    state.storage.save_session(&child).await.unwrap();
    let cursor = state.account_sink.latest_seq();
    for id in ["foreign", "child", "missing", "unassigned"] {
        for target in [json!(SECTION), Value::Null] {
            let response = patch(
                &state,
                &project.id,
                Some(1),
                json!({"sidebar": snapshot(json!({id: target}))}),
            )
            .await;
            assert_eq!(response.status(), StatusCode::CONFLICT, "{id}");
            assert_unchanged(&state, &project, cursor);
        }
    }
}

#[actix_web::test]
async fn sidebar_cas_and_mixed_mutations_reject_before_writing() {
    let (_dir, state, project) = fixture().await;
    let cursor = state.account_sink.latest_seq();
    for (revision, status) in [
        (None, StatusCode::PRECONDITION_REQUIRED),
        (Some(9), StatusCode::PRECONDITION_FAILED),
    ] {
        let response = patch(
            &state,
            &project.id,
            revision,
            json!({"sidebar": snapshot(json!({}))}),
        )
        .await;
        assert_eq!(response.status(), status);
        assert_unchanged(&state, &project, cursor);
    }
    for extra in [
        json!({"name":"Changed"}),
        json!({"description":null}),
        json!({"section":null}),
        json!({"project_path":"/tmp"}),
        json!({"import_sidebar":snapshot(json!({}))}),
    ] {
        let mut request = json!({"sidebar":snapshot(json!({}))});
        request
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let response = patch(&state, &project.id, Some(1), request).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_unchanged(&state, &project, cursor);
    }
    for field in ["sidebar", "import_sidebar"] {
        assert!(serde_json::from_value::<PatchProjectRequest>(json!({field:null})).is_err());
    }
    let response = patch(
        &state,
        &project.id,
        Some(1),
        json!({"sidebar":snapshot(json!({}))}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let saved = state.project_store.get(&project.id).unwrap();
    let cursor = state.account_sink.latest_seq();
    let stale = patch(
        &state,
        &project.id,
        Some(1),
        json!({"sidebar":snapshot(json!({}))}),
    )
    .await;
    assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);
    assert_unchanged(&state, &saved, cursor);
}

#[actix_web::test]
async fn sidebar_deletion_clears_assignments_even_after_root_disappears() {
    let (_dir, state, project) = fixture().await;
    save_root(&state, "assigned", Some(&project.id)).await;
    assert_eq!(
        patch(
            &state,
            &project.id,
            Some(1),
            json!({"sidebar":snapshot(json!({"assigned":SECTION}))})
        )
        .await
        .status(),
        StatusCode::OK
    );
    state.storage.delete_session("assigned").await.unwrap();
    let mut renamed = snapshot(json!({"assigned":SECTION}));
    renamed["sections"][0]["name"] = json!("Renamed");
    assert_eq!(
        patch(&state, &project.id, Some(2), json!({"sidebar":renamed}))
            .await
            .status(),
        StatusCode::OK
    );
    let response = patch(
        &state,
        &project.id,
        Some(3),
        json!({"sidebar":{
            "sections":[], "session_sections":{"assigned":SECTION}, "hidden_legacy":false
        }}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        body(response).await["sidebar"]["session_sections"]["assigned"],
        Value::Null
    );
    assert_eq!(
        patch(
            &state,
            &project.id,
            Some(4),
            json!({"sidebar":snapshot(json!({"assigned":null}))})
        )
        .await
        .status(),
        StatusCode::OK
    );
}

#[actix_web::test]
async fn sidebar_import_filters_confirmed_stale_assignments_and_cannot_overwrite() {
    let (dir, state, project) = fixture().await;
    let foreign = state.project_store.create("Other", None).unwrap();
    let root = save_root(&state, "assigned", Some(&project.id)).await;
    save_root(&state, "unmoved", Some(&project.id)).await;
    save_root(&state, "foreign", Some(&foreign.id)).await;
    let child = Session::new_child_of("child", &root, "test-model", "Child");
    state.storage.save_session(&child).await.unwrap();
    let response = patch(
        &state,
        &project.id,
        Some(1),
        json!({"import_sidebar":snapshot(json!({
            "assigned":"legacy", "foreign":SECTION, "missing":SECTION, "child":null
        }))}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let saved = state.project_store.get(&project.id).unwrap();
    let sidebar = saved.sidebar.as_ref().unwrap();
    assert_eq!(sidebar.session_sections.len(), 1);
    assert_eq!(sidebar.session_sections.get("assigned"), Some(&None));
    assert_eq!(sidebar.sections.len(), 2);
    let cursor = state.account_sink.latest_seq();
    let response = patch(
        &state,
        &project.id,
        Some(2),
        json!({"import_sidebar":snapshot(json!({"unmoved":SECTION}))}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_unchanged(&state, &saved, cursor);
    assert_eq!(
        bamboo_projects::ProjectStore::open(dir.path())
            .unwrap()
            .get(&project.id)
            .unwrap(),
        saved
    );
}

#[actix_web::test]
async fn sidebar_unicode_lowercase_collisions_reject_without_writes_or_events() {
    let (_dir, state, project) = fixture().await;
    let cursor = state.account_sink.latest_seq();
    for (first, second) in [("Ärger", "ärger"), ("Straße", "STRAẞE")] {
        let mut requested = snapshot(json!({}));
        requested["sections"][0]["name"] = json!(first);
        requested["sections"][1]["name"] = json!(second);
        for field in ["sidebar", "import_sidebar"] {
            let response = patch(&state, &project.id, Some(1), json!({field: requested})).await;
            assert_eq!(response.status(), StatusCode::CONFLICT);
            assert_unchanged(&state, &project, cursor);
        }
    }
}

#[actix_web::test]
async fn sidebar_import_preserves_distinct_lowercase_names_ids_and_assignments_on_reopen() {
    let (dir, state, project) = fixture().await;
    save_root(&state, "sharp-s-root", Some(&project.id)).await;
    save_root(&state, "ascii-root", Some(&project.id)).await;
    let mut requested = snapshot(json!({"sharp-s-root": SECTION, "ascii-root": EMPTY_SECTION}));
    requested["sections"][0]["name"] = json!("Straße");
    requested["sections"][1]["name"] = json!("STRASSE");
    let response = patch(
        &state,
        &project.id,
        Some(1),
        json!({"import_sidebar": requested}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body(response).await["sidebar"], requested);

    // An ordinary metadata write must retain both accepted buckets and their
    // independent root assignments before a subsequent store reopen.
    let response = patch(
        &state,
        &project.id,
        Some(2),
        json!({"name": "Renamed Project"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let saved: ProjectManifest = serde_json::from_value(body(response).await).unwrap();
    assert_eq!(serde_json::to_value(&saved.sidebar).unwrap(), requested);
    let manifest: Value = serde_json::from_slice(
        &std::fs::read(state.project_store.paths().manifest_path(&project.id)).unwrap(),
    )
    .unwrap();
    assert_eq!(manifest["sidebar"], requested);

    let reopened = bamboo_projects::ProjectStore::open(dir.path()).unwrap();
    assert_eq!(reopened.get(&project.id).unwrap(), saved);
    assert_eq!(reopened.list().unwrap(), vec![saved.clone()]);
    assert_eq!(
        serde_json::to_value(&reopened.index().unwrap().projects[&project.id].sidebar).unwrap(),
        requested
    );
}

#[actix_web::test]
async fn empty_sidebar_import_is_durable_and_blocks_other_browser_replay() {
    let (dir, state, project) = fixture().await;
    let empty = json!({"sections":[],"session_sections":{},"hidden_legacy":false});
    assert_eq!(
        patch(
            &state,
            &project.id,
            Some(1),
            json!({"import_sidebar":empty})
        )
        .await
        .status(),
        StatusCode::OK
    );
    let reopened = bamboo_projects::ProjectStore::open(dir.path())
        .unwrap()
        .get(&project.id)
        .unwrap();
    assert_eq!(reopened.sidebar, Some(ProjectSidebar::default()));
    let cursor = state.account_sink.latest_seq();
    assert_eq!(
        patch(
            &state,
            &project.id,
            Some(2),
            json!({"import_sidebar":snapshot(json!({}))})
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    assert_unchanged(&state, &reopened, cursor);
}

struct FailingControlPlane;

#[actix_web::test]
async fn sidebar_import_preserves_ids_when_legacy_and_custom_display_names_collide() {
    let (_dir, state, project) = fixture().await;
    save_root(&state, "assigned", Some(&project.id)).await;
    let project = state
        .project_store
        .update(&project.id, 1, |project| {
            project.section = Some("Support".into());
            Ok(())
        })
        .unwrap();
    let response = patch(
        &state,
        &project.id,
        Some(2),
        json!({"import_sidebar":snapshot(json!({"assigned":"legacy"}))}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let saved = state.project_store.get(&project.id).unwrap();
    assert_eq!(
        saved.sidebar.as_ref().unwrap().session_sections["assigned"].as_deref(),
        Some("legacy")
    );
    // Changing the independent legacy field clears its stale assignments.
    let response = patch(&state, &project.id, Some(3), json!({"section":null})).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        body(response).await["sidebar"]["session_sections"]["assigned"],
        Value::Null
    );
}

#[async_trait::async_trait]
impl Storage for FailingControlPlane {
    async fn save_session(&self, _: &Session) -> std::io::Result<()> {
        unreachable!()
    }
    async fn load_session(&self, _: &str) -> std::io::Result<Option<Session>> {
        unreachable!()
    }
    async fn delete_session(&self, _: &str) -> std::io::Result<bool> {
        unreachable!()
    }
    async fn load_runtime_control_plane(&self, _: &str) -> std::io::Result<Option<Session>> {
        Err(std::io::Error::other("injected control-plane read failure"))
    }
}

#[actix_web::test]
async fn sidebar_import_read_error_never_commits_an_empty_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = Box::pin(AppState::new(dir.path().to_path_buf()))
        .await
        .unwrap();
    let project = state.project_store.create("Zenith", None).unwrap();
    state.storage = Arc::new(FailingControlPlane);
    let state = web::Data::new(state);
    let cursor = state.account_sink.latest_seq();
    for field in ["sidebar", "import_sidebar"] {
        let response = patch(
            &state,
            &project.id,
            Some(1),
            json!({field:snapshot(json!({"assigned":SECTION}))}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_unchanged(&state, &project, cursor);
    }
}
