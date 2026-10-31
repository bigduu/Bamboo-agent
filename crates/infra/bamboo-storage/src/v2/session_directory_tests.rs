//! Coverage for the Supervisor session directory inputs: the index mirror of
//! `AgentRuntimeState.last_human_input`, and the directory port's merge of
//! management links with the authorized-Project index.

use super::*;
use bamboo_domain::{
    HumanInputMark, Storage, SupervisorManagementMutation, SupervisorManagementRequest,
    SupervisorReference,
};

#[tokio::test]
async fn index_entry_mirrors_last_human_input_mark() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let mut session = Session::new("mirror-human-input", "model");
    let at = chrono::Utc::now();
    let mut runtime = bamboo_domain::AgentRuntimeState::new("run-1");
    runtime.set_last_human_input(HumanInputMark::user(at));
    session.agent_runtime_state = Some(runtime);
    store.save_session(&session).await.unwrap();

    let entry = store.get_index_entry("mirror-human-input").await.unwrap();
    let mark = entry.last_human_input.clone().expect("mark is mirrored");
    assert_eq!(mark.at, at);
    assert_eq!(mark.source, bamboo_domain::HumanInputSource::User);

    // Round-trip through JSON keeps the field optional and stable.
    let restored: SessionIndexEntry =
        serde_json::from_value(serde_json::to_value(&entry).unwrap()).unwrap();
    assert_eq!(restored.last_human_input, Some(mark));
}

#[tokio::test]
async fn legacy_index_entry_without_human_input_deserializes() {
    let legacy = serde_json::json!({
        "id": "legacy", "kind": "root", "rel_path": "sessions/legacy",
        "title": "old", "pinned": false, "parent_session_id": null,
        "root_session_id": "legacy", "spawn_depth": 0, "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z", "last_activity_at": "2026-01-01T00:00:00Z",
        "message_count": 0, "has_attachments": false
    });
    let entry: SessionIndexEntry = serde_json::from_value(legacy).unwrap();
    assert!(entry.last_human_input.is_none());
}

#[tokio::test]
async fn save_without_human_input_leaves_mirror_absent() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let session = Session::new("no-human-input", "model");
    store.save_session(&session).await.unwrap();
    let entry = store.get_index_entry("no-human-input").await.unwrap();
    assert!(entry.last_human_input.is_none());
}

/// Management-state fixture: one supervisor, one allowed project, one link.
async fn supervised_fixture() -> (
    tempfile::TempDir,
    SessionStoreV2,
    SupervisorReference,
    String,
) {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let reference: SupervisorReference = (&store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap())
        .into();
    store
        .mutate_supervisor_management(&SupervisorManagementRequest {
            supervisor: reference.clone(),
            expected_state_revision: 0,
            mutation: SupervisorManagementMutation::ConfigureProjectScope {
                allowed_projects: ["project-a".parse().unwrap()].into(),
            },
        })
        .await
        .unwrap();
    let mut target = Session::new("linked-root", "model");
    target.set_project_id_meta("project-a");
    target.set_workspace_path_meta("/workspace/linked-root");
    store.save_session(&target).await.unwrap();
    store
        .mutate_supervisor_management(&SupervisorManagementRequest {
            supervisor: reference.clone(),
            expected_state_revision: 1,
            mutation: SupervisorManagementMutation::Attach {
                target_session_id: "linked-root".into(),
            },
        })
        .await
        .unwrap();
    (home, store, reference, "project-a".into())
}

#[tokio::test]
async fn link_attach_mutation_shape_matches_directory_expectations() {
    let (home, store, reference, project) = supervised_fixture().await;
    assert!(home.path().exists());
    let session = store
        .load_session("linked-root")
        .await
        .unwrap()
        .expect("target persisted");
    let authority = store
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .expect("supervisor persisted");
    let state = authority.supervisor_management.as_ref().unwrap();
    assert!(state.links.contains_key("linked-root"));
    assert!(state
        .allowed_projects
        .contains(&project.parse::<bamboo_domain::ProjectId>().unwrap()));
    assert_eq!(state.incarnation_id, reference.incarnation_id);
    assert_eq!(session.id, "linked-root");
}
