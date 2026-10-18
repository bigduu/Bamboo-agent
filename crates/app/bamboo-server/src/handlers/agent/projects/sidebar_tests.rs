use super::*;

use actix_web::{body::to_bytes, http::header, test, App};
use bamboo_domain::{ProjectSidebarSection, Session, Storage};
use fs2::FileExt;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const SECTION: &str = "section:2d567c47-a081-4a8d-9c9e-246bce470c9f";
const EMPTY_SECTION: &str = "section:28f595c0-21dc-4a86-8b83-5f8b258386fa";

/// Hold the production account writer's actual cross-process journal claim.
/// Unlock on unwind too, so a failed assertion cannot strand its blocking job.
struct HeldAccountJournalClaim(std::fs::File);

impl HeldAccountJournalClaim {
    fn acquire(events_dir: &std::path::Path) -> Self {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(events_dir.join(".account-journal.lock"))
            .unwrap();
        FileExt::lock_exclusive(&file).unwrap();
        Self(file)
    }
}

impl Drop for HeldAccountJournalClaim {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

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
        // HTTP success only attempts to enqueue the asynchronous event; it
        // promises neither queue admission nor one-second journal/broadcast latency.
        // This bounded fixture wait still requires the exact response revision and
        // both real publication boundaries before later absence assertions.
        let expected_revision = response
            .headers()
            .get(header::ETAG)
            .expect("successful project PATCH has its actual revision")
            .to_str()
            .unwrap()
            .trim_matches('"')
            .parse::<u64>()
            .unwrap();
        let event = tokio::time::timeout(std::time::Duration::from_secs(8), feed.recv())
            .await
            .expect("project PATCH did not reach durable/live publication")
            .expect("project account tail stays open and unlagged");
        let matches_update = |event: &AgentEvent| {
            matches!(event, AgentEvent::ProjectUpdated { project_id, revision }
                if project_id == id.as_str() && *revision == expected_revision)
        };
        assert!(
            matches_update(&event.event),
            "exact ProjectUpdated broadcast"
        );
        let replay = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            event.seq.saturating_sub(1),
        )
        .expect("read the actual project-event journal");
        assert!(
            replay
                .iter()
                .any(|durable| { durable.seq == event.seq && matches_update(&durable.event) }),
            "the broadcast has its exact durable journal row"
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

fn large_snapshot(sections: usize, assignments: usize) -> ProjectSidebar {
    let sections = (0..sections)
        .map(|index| ProjectSidebarSection {
            id: format!("section:{}", uuid::Uuid::from_u128(index as u128 + 1)),
            name: format!("Section {index}"),
        })
        .collect::<Vec<_>>();
    let target = sections.first().map(|section| section.id.clone());
    ProjectSidebar {
        sections,
        session_sections: (0..assignments)
            .map(|index| (format!("root-{index}"), target.clone()))
            .collect(),
        hidden_legacy: false,
    }
}

fn snapshot_with_json_bytes(bytes: usize) -> ProjectSidebar {
    let mut sidebar = large_snapshot(1, 0);
    let target = sidebar.sections[0].id.clone();
    sidebar
        .session_sections
        .insert(String::new(), Some(target.clone()));
    let overhead = serde_json::to_vec(&sidebar).unwrap().len();
    sidebar.session_sections.clear();
    sidebar
        .session_sections
        .insert("r".repeat(bytes - overhead), Some(target));
    assert_eq!(serde_json::to_vec(&sidebar).unwrap().len(), bytes);
    sidebar
}

fn project_disk_snapshot(state: &AppState, id: &ProjectId) -> Vec<Option<Vec<u8>>> {
    let paths = state.project_store.paths();
    let home = paths.project_home(id);
    [
        paths.manifest_path(id),
        home.join("project.json.bak"),
        paths.manifest_revision_path(id),
        paths.index_path(),
        home.join("project.json.migrated-v2"),
    ]
    .into_iter()
    .map(|path| match std::fs::read(path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("fixture read failed: {error}"),
    })
    .collect()
}

async fn failing_control_plane_fixture() -> (
    tempfile::TempDir,
    web::Data<AppState>,
    ProjectManifest,
    Arc<AtomicUsize>,
) {
    let dir = tempfile::tempdir().unwrap();
    let mut state = Box::pin(AppState::new(dir.path().to_path_buf()))
        .await
        .unwrap();
    // Keep deliberately rewritten legacy fixtures outside the store captured
    // by AppState's background watchers.
    state.project_store = Arc::new(
        bamboo_projects::ProjectStore::open(dir.path().join("isolated-registry")).unwrap(),
    );
    let project = state.project_store.create("Zenith", None).unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    state.storage = Arc::new(FailingControlPlane {
        reads: reads.clone(),
    });
    (dir, web::Data::new(state), project, reads)
}

#[actix_web::test]
async fn oversized_sidebar_requests_reject_before_storage_migration_or_events() {
    let (_dir, state, project, reads) = failing_control_plane_fixture().await;
    let manifest_path = state.project_store.paths().manifest_path(&project.id);
    let mut legacy: Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    legacy["schema_version"] = json!(2);
    std::fs::write(&manifest_path, serde_json::to_vec_pretty(&legacy).unwrap()).unwrap();
    let before = project_disk_snapshot(&state, &project.id);
    let cursor = state.account_sink.latest_seq();
    let mut escaped = large_snapshot(1, 1);
    escaped.sections[0].name = "\0".repeat(MAX_SIDEBAR_JSON_BYTES / 6 + 1);
    assert!(serde_json::to_vec(&escaped).unwrap().len() > MAX_SIDEBAR_JSON_BYTES);
    let oversized = [
        large_snapshot(MAX_SIDEBAR_SECTIONS + 1, 1),
        large_snapshot(1, MAX_SIDEBAR_ASSIGNMENTS + 1),
        snapshot_with_json_bytes(MAX_SIDEBAR_JSON_BYTES + 1),
        escaped,
    ];
    for field in ["sidebar", "import_sidebar"] {
        for sidebar in &oversized {
            let response = patch(&state, &project.id, Some(1), json!({field: sidebar})).await;
            assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
            assert_eq!(reads.load(Ordering::SeqCst), 0);
            // Do not call get here: the assertion itself would migrate v2.
            assert_eq!(project_disk_snapshot(&state, &project.id), before);
            assert_eq!(state.account_sink.latest_seq(), cursor);
        }
    }
}

#[actix_web::test]
async fn sidebar_size_boundaries_reach_normal_membership_validation() {
    let (_dir, state, project, reads) = failing_control_plane_fixture().await;
    let before = project_disk_snapshot(&state, &project.id);
    let cursor = state.account_sink.latest_seq();
    let allowed = [
        large_snapshot(MAX_SIDEBAR_SECTIONS, 1),
        large_snapshot(1, MAX_SIDEBAR_ASSIGNMENTS),
        snapshot_with_json_bytes(MAX_SIDEBAR_JSON_BYTES),
    ];
    for field in ["sidebar", "import_sidebar"] {
        for sidebar in &allowed {
            reads.store(0, Ordering::SeqCst);
            let response = patch(&state, &project.id, Some(1), json!({field: sidebar})).await;
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(reads.load(Ordering::SeqCst), 1);
            assert_eq!(project_disk_snapshot(&state, &project.id), before);
            assert_eq!(state.account_sink.latest_seq(), cursor);
        }
    }
}

#[actix_web::test]
async fn stored_over_limit_sidebar_remains_readable_and_metadata_edits_preserve_it() {
    let (dir, state, project) = fixture().await;
    let mut sidebar = large_snapshot(MAX_SIDEBAR_SECTIONS + 1, MAX_SIDEBAR_ASSIGNMENTS + 1);
    sidebar.session_sections.insert(
        "r".repeat(MAX_SIDEBAR_JSON_BYTES),
        Some(sidebar.sections[0].id.clone()),
    );
    let stored = state
        .project_store
        .update(&project.id, 1, |project| {
            project.sidebar = Some(sidebar.clone());
            Ok(())
        })
        .unwrap();
    let reopened = bamboo_projects::ProjectStore::open(dir.path()).unwrap();
    assert_eq!(reopened.get(&project.id).unwrap(), stored);
    assert_eq!(reopened.list().unwrap(), vec![stored]);

    let response = patch(
        &state,
        &project.id,
        Some(2),
        json!({"name": "Renamed Project"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let saved: ProjectManifest = serde_json::from_value(body(response).await).unwrap();
    assert_eq!(saved.sidebar, Some(sidebar));
    let reopened = bamboo_projects::ProjectStore::open(dir.path()).unwrap();
    assert_eq!(reopened.get(&project.id).unwrap(), saved);
    assert_eq!(
        reopened.index().unwrap().projects[&project.id].sidebar,
        saved.sidebar
    );
}

#[actix_web::test]
async fn metadata_patch_waits_for_durable_event_behind_real_journal_claim() {
    let (_dir, state, project) = fixture().await;
    let mut feed = state.account_sink.subscribe();
    let cursor = state.account_sink.latest_seq();
    let claim = HeldAccountJournalClaim::acquire(state.account_sink.events_dir());
    let mut pending = Box::pin(patch(
        &state,
        &project.id,
        Some(1),
        json!({"name": "Renamed after durable publication"}),
    ));

    // Drive the metadata-only handler through its synchronous write and queue
    // admission before starting the controlled delay. Neither is publication.
    assert!(futures::poll!(pending.as_mut()).is_pending());
    let saved = state.project_store.get(&project.id).unwrap();
    assert_eq!(saved.revision, 2);
    assert_eq!(saved.name, "Renamed after durable publication");
    assert_eq!(state.account_sink.dropped_count(), 0);

    // The actual writer remains behind its OS claim for longer than the old
    // one-second fixture deadline. The PATCH helper must still be pending.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(1_100), pending.as_mut())
            .await
            .is_err()
    );
    assert_eq!(state.account_sink.latest_seq(), cursor);
    assert!(matches!(
        feed.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    assert!(
        bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), cursor)
            .unwrap()
            .is_empty()
    );

    drop(claim);
    let response = tokio::time::timeout(std::time::Duration::from_secs(8), pending)
        .await
        .expect("the real journal writer completes after its claim is released");
    assert_eq!(response.status(), StatusCode::OK);
    let returned: ProjectManifest = serde_json::from_value(body(response).await).unwrap();
    assert_eq!(returned, saved);
    let event = feed
        .try_recv()
        .expect("the helper returns only after the actual broadcast");
    assert!(
        matches!(&event.event, AgentEvent::ProjectUpdated { project_id, revision: 2 } if project_id == project.id.as_str())
    );
    let replay =
        bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), cursor)
            .unwrap();
    assert!(replay.iter().any(|durable| {
        durable.seq == event.seq
            && matches!(&durable.event, AgentEvent::ProjectUpdated { project_id, revision: 2 } if project_id == project.id.as_str())
    }));
    assert_eq!(state.account_sink.dropped_count(), 0);
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

struct FailingControlPlane {
    reads: Arc<AtomicUsize>,
}

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
        self.reads.fetch_add(1, Ordering::SeqCst);
        Err(std::io::Error::other("injected control-plane read failure"))
    }
}

#[actix_web::test]
async fn sidebar_import_read_error_never_commits_an_empty_snapshot() {
    let (_dir, state, project, reads) = failing_control_plane_fixture().await;
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
    assert_eq!(reads.load(Ordering::SeqCst), 2);
}
