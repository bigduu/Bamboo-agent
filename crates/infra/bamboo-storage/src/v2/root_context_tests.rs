//! Root Project revisions and creation identity at the raw storage boundary.

use super::*;
use bamboo_domain::{
    AgentRuntimeState, Message, SessionAuthorityConflict, SessionPermissionMode, TaskItem,
    ROOT_MODE_OPERATION_HISTORY_LIMIT,
};
use tempfile::TempDir;

struct Fixture {
    first: SessionStoreV2,
    second: SessionStoreV2,
    home: TempDir,
}

impl Fixture {
    async fn new(initial: &Session, runtime: bool) -> Self {
        let home = tempfile::tempdir().unwrap();
        let first = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        save(&first, initial, runtime).await.unwrap();
        // Load the published index before testing a second independent writer.
        // Otherwise its runtime save would take the new-session full-save fallback.
        let second = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        assert!(second.get_index_entry(&initial.id).await.is_some());
        Self {
            first,
            second,
            home,
        }
    }

    async fn finish(self) {
        self.first.flush_search_index().await;
        self.second.flush_search_index().await;
        drop(self.first);
        drop(self.second);
        self.home.close().unwrap();
    }
}

fn root() -> Session {
    let mut session = Session::new("project-root", "original-model");
    session.metadata_version = 7;
    session.set_project_id_meta("project-a");
    session.set_workspace_path_meta("/workspace/project-a");
    session.agent_runtime_state = Some(AgentRuntimeState::default());
    session
        .agent_runtime_state
        .as_mut()
        .unwrap()
        .set_permission_mode(SessionPermissionMode::Auto);
    session.add_message(Message::system("Preserved system context"));
    session.add_message(Message::user("Preserved user history"));
    session
}

fn directory(store: &SessionStoreV2, id: &str) -> PathBuf {
    store.sessions_root_dir().join(id)
}

async fn files(store: &SessionStoreV2, id: &str) -> [Option<Vec<u8>>; 4] {
    let directory = directory(store, id);
    [
        fs::read(directory.join("session.json")).await.ok(),
        fs::read(directory.join(RUNTIME_SIDECAR_FILE)).await.ok(),
        fs::read(directory.join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE))
            .await
            .ok(),
        fs::read(store.index_path()).await.ok(),
    ]
}

async fn save(store: &SessionStoreV2, session: &Session, runtime: bool) -> io::Result<()> {
    if runtime {
        store.save_runtime_state(session).await
    } else {
        store.save_session(session).await
    }
}

async fn reject_without_writes(store: &SessionStoreV2, candidate: &Session) {
    let before = files(store, &candidate.id).await;
    for runtime in [false, true] {
        let error = save(store, candidate, runtime).await.unwrap_err();
        assert!(
            error
                .get_ref()
                .is_some_and(|error| error.is::<SessionAuthorityConflict>()),
            "runtime={runtime}: {error:?}"
        );
        assert_eq!(
            files(store, &candidate.id).await,
            before,
            "rejected runtime={runtime} writer changed canonical files or the index"
        );
    }
}

async fn assert_context(store: &SessionStoreV2, expected: &Session) {
    let current = store.load_session(&expected.id).await.unwrap().unwrap();
    assert_eq!(current.project_id_meta(), expected.project_id_meta());
    assert_eq!(current.metadata_version, expected.metadata_version);
    assert_eq!(current.created_at, expected.created_at);
    assert_eq!(current.model, expected.model);
    assert_eq!(
        serde_json::to_value(&current.messages).unwrap(),
        serde_json::to_value(&expected.messages).unwrap()
    );
    assert_eq!(
        current
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .effective_permission_mode(),
        expected
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .effective_permission_mode()
    );
}

async fn assert_root_read_unavailable(store: &SessionStoreV2, id: &str) {
    for error in [
        store.load_session(id).await.unwrap_err(),
        store.load_runtime_control_plane(id).await.unwrap_err(),
    ] {
        assert!(
            error
                .get_ref()
                .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()),
            "{error:?}"
        );
        assert!(error.to_string().contains("canonical runtime file"));
    }
}

#[tokio::test]
async fn independent_stores_reject_stale_project_full_and_runtime_snapshots() {
    for authoritative_runtime in [false, true] {
        let initial = root();
        let fixture = Fixture::new(&initial, false).await;
        let mut stale = fixture
            .second
            .load_session(&initial.id)
            .await
            .unwrap()
            .unwrap();
        let mut current = initial.clone();
        current.metadata_version += 1;
        current.set_project_id_meta("project-b");
        current.set_workspace_path_meta("/workspace/project-b");
        save(&fixture.first, &current, authoritative_runtime)
            .await
            .unwrap();

        stale.model = "stale-model".into();
        stale.messages.clear();
        stale.agent_runtime_state = Some(AgentRuntimeState::default());
        reject_without_writes(&fixture.second, &stale).await;
        assert_context(&fixture.first, &current).await;
        fixture.finish().await;
    }
}

#[tokio::test]
async fn independent_stores_cannot_undo_root_tool_authority_with_stale_snapshots() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let stale = fixture
        .second
        .load_session(&initial.id)
        .await
        .unwrap()
        .unwrap();
    let mut selected = stale.clone();
    selected.set_root_orchestration_only(true).unwrap();
    assert!(fixture.first.save_runtime_state(&selected).await.is_err());
    fixture.first.save_session(&selected).await.unwrap();
    assert!(fixture
        .second
        .load_session(&initial.id)
        .await
        .unwrap()
        .unwrap()
        .root_orchestration_only_enabled());

    reject_without_writes(&fixture.second, &stale).await;
    let mut same_revision_false = selected.clone();
    same_revision_false.root_orchestration_only = false;
    reject_without_writes(&fixture.second, &same_revision_false).await;

    let mut disabled = fixture
        .second
        .load_session(&initial.id)
        .await
        .unwrap()
        .unwrap();
    disabled.set_root_orchestration_only(false).unwrap();
    assert!(fixture.second.save_runtime_state(&disabled).await.is_err());
    fixture.second.save_session(&disabled).await.unwrap();
    assert_eq!(disabled.root_tool_authority_revision, 2);
    assert!(!fixture
        .first
        .load_session(&initial.id)
        .await
        .unwrap()
        .unwrap()
        .root_orchestration_only_enabled());
    reject_without_writes(&fixture.first, &selected).await;
    fixture.finish().await;
}

#[tokio::test]
async fn root_reads_fail_closed_after_runtime_sidecar_loss_or_corruption() {
    for selected in [false, true] {
        for corrupt in [false, true] {
            let home = tempfile::tempdir().unwrap();
            let first = SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap();
            let mut session = root();
            if selected {
                session.set_root_orchestration_only(true).unwrap();
            }
            first.save_session(&session).await.unwrap();
            first.flush_search_index().await;
            drop(first);

            let runtime = home
                .path()
                .join("sessions")
                .join(&session.id)
                .join(RUNTIME_SIDECAR_FILE);
            if corrupt {
                fs::write(&runtime, b"{invalid").await.unwrap();
            } else {
                fs::remove_file(&runtime).await.unwrap();
            }
            let reopened = SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap();
            assert!(reopened.get_index_entry(&session.id).await.is_some());
            for error in [
                reopened.load_session(&session.id).await.unwrap_err(),
                reopened
                    .load_runtime_control_plane(&session.id)
                    .await
                    .unwrap_err(),
                reopened
                    .recover_root_session_from_disk(&session.id)
                    .await
                    .unwrap_err(),
            ] {
                assert!(
                    error
                        .get_ref()
                        .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()),
                    "selected={selected}, corrupt={corrupt}: {error:?}"
                );
                assert!(error.to_string().contains("canonical runtime file"));
            }
            reopened.flush_search_index().await;
            drop(reopened);
            home.close().unwrap();
        }
    }
}

#[tokio::test]
async fn restored_stale_valid_root_sidecar_cannot_reopen_or_overwrite_tool_authority() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let runtime = directory(&fixture.first, &initial.id).join(RUNTIME_SIDECAR_FILE);
    let stale_runtime = fs::read(&runtime).await.unwrap();
    let mut selected = initial.clone();
    selected.set_root_orchestration_only(true).unwrap();
    fixture.first.save_session(&selected).await.unwrap();
    fs::write(&runtime, stale_runtime).await.unwrap();

    for error in [
        fixture.second.load_session(&initial.id).await.unwrap_err(),
        fixture
            .second
            .load_runtime_control_plane(&initial.id)
            .await
            .unwrap_err(),
    ] {
        assert!(
            error
                .get_ref()
                .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()),
            "{error:?}"
        );
        assert!(error.to_string().contains("stale"));
    }
    let before = files(&fixture.first, &initial.id).await;
    for candidate in [&initial, &selected] {
        for runtime_only in [false, true] {
            let error = save(&fixture.second, candidate, runtime_only)
                .await
                .unwrap_err();
            assert!(
                error
                    .get_ref()
                    .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()),
                "runtime={runtime_only}: {error:?}"
            );
            assert_eq!(files(&fixture.first, &initial.id).await, before);
        }
    }
    fixture.finish().await;
}

#[tokio::test]
async fn root_control_plane_rejects_child_kind_and_foreign_id_sidecars_after_reopen() {
    for foreign_id in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let first = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        let mut selected = root();
        selected.set_root_orchestration_only(true).unwrap();
        first.save_session(&selected).await.unwrap();

        let child = Session::new_child_of("genuine-child", &selected, "model", "child");
        first.save_session(&child).await.unwrap();
        let runtime = directory(&first, &selected.id).join(RUNTIME_SIDECAR_FILE);
        if foreign_id {
            let mut other = Session::new("other-root", "model");
            other.set_root_orchestration_only(true).unwrap();
            first.save_session(&other).await.unwrap();
            fs::copy(
                directory(&first, &other.id).join(RUNTIME_SIDECAR_FILE),
                &runtime,
            )
            .await
            .unwrap();
        } else {
            let bytes = fs::read(&runtime).await.unwrap();
            let mut side: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            side["kind"] = serde_json::json!("child");
            fs::write(&runtime, serde_json::to_vec(&side).unwrap())
                .await
                .unwrap();
        }
        first.flush_search_index().await;
        drop(first);

        let reopened = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        for error in [
            reopened.load_session(&selected.id).await.unwrap_err(),
            reopened
                .load_runtime_control_plane(&selected.id)
                .await
                .unwrap_err(),
        ] {
            assert!(
                error
                    .get_ref()
                    .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()),
                "foreign_id={foreign_id}: {error:?}"
            );
        }
        assert_eq!(
            reopened
                .load_runtime_control_plane(&child.id)
                .await
                .unwrap()
                .unwrap()
                .kind,
            SessionKind::Child
        );
        reopened.flush_search_index().await;
        drop(reopened);
        home.close().unwrap();
    }
}

#[tokio::test]
async fn project_changes_require_exactly_the_next_revision() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    for revision in [initial.metadata_version, initial.metadata_version + 2] {
        let mut divergent = initial.clone();
        divergent.set_project_id_meta("project-b");
        divergent.metadata_version = revision;
        reject_without_writes(&fixture.second, &divergent).await;
    }
    assert_context(&fixture.first, &initial).await;
    fixture.finish().await;
}

#[tokio::test]
async fn project_aba_removal_and_reassignment_advance_without_reviving_old_snapshots() {
    for runtime in [false, true] {
        let initial = root();
        let fixture = Fixture::new(&initial, false).await;
        let mut current = initial.clone();
        for project in [
            Some("project-b"),
            Some("project-a"),
            None,
            Some("project-a"),
        ] {
            let previous = current.clone();
            current.metadata_version = current.metadata_version.checked_add(1).unwrap();
            if let Some(project) = project {
                current.set_project_id_meta(project);
            } else {
                current.clear_project_id_meta();
            }
            save(&fixture.first, &current, runtime).await.unwrap();
            reject_without_writes(&fixture.second, &previous).await;
            reject_without_writes(&fixture.second, &initial).await;
            assert_context(&fixture.first, &current).await;
        }
        fixture.finish().await;
    }
}

#[tokio::test]
async fn unchanged_project_accepts_newer_ui_revision_and_rejects_lower_revision() {
    for runtime in [false, true] {
        let initial = root();
        let fixture = Fixture::new(&initial, false).await;
        let mut current = initial.clone();
        current.metadata_version += 3;
        current.title = "New UI title".into();
        current.pinned = true;
        save(&fixture.second, &current, runtime).await.unwrap();
        let persisted = fixture
            .first
            .load_session(&current.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.title, "New UI title");
        assert!(persisted.pinned);
        assert_context(&fixture.first, &current).await;

        let mut stale = current.clone();
        stale.metadata_version -= 1;
        reject_without_writes(&fixture.first, &stale).await;
        current
            .metadata
            .insert("last_run_status".into(), "completed".into());
        save(&fixture.first, &current, runtime).await.unwrap();
        assert_context(&fixture.second, &current).await;
        fixture.finish().await;
    }
}

#[tokio::test]
async fn project_revision_overflow_cannot_publish_a_wrapped_or_equal_revision() {
    let mut initial = root();
    initial.metadata_version = u64::MAX;
    let fixture = Fixture::new(&initial, false).await;
    for revision in [0, u64::MAX] {
        let mut candidate = initial.clone();
        candidate.metadata_version = revision;
        candidate.set_project_id_meta("project-b");
        reject_without_writes(&fixture.second, &candidate).await;
    }
    assert_context(&fixture.first, &initial).await;
    fixture.finish().await;
}

#[tokio::test]
async fn root_creation_time_is_immutable_even_with_a_newer_revision() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let mut candidate = initial.clone();
    candidate.created_at += chrono::Duration::seconds(1);
    candidate.metadata_version += 1;
    reject_without_writes(&fixture.second, &candidate).await;
    assert_context(&fixture.first, &initial).await;
    fixture.finish().await;
}

#[tokio::test]
async fn deleting_and_recreating_a_root_does_not_revalidate_its_old_snapshot() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let stale = fixture
        .second
        .load_session(&initial.id)
        .await
        .unwrap()
        .unwrap();
    assert!(fixture.first.delete_session(&initial.id).await.unwrap());
    let mut replacement = fixture
        .first
        .recreate_root_session(&initial.id, "replacement-model")
        .await
        .unwrap();
    replacement.agent_runtime_state = initial.agent_runtime_state.clone();
    replacement.messages = vec![Message::user("Replacement history")];
    fixture.first.save_session(&replacement).await.unwrap();

    reject_without_writes(&fixture.second, &stale).await;
    let mut stale_newer_revision = stale;
    stale_newer_revision.metadata_version += 1;
    reject_without_writes(&fixture.second, &stale_newer_revision).await;
    assert_context(&fixture.first, &replacement).await;
    fixture.finish().await;
}

#[tokio::test]
async fn strict_root_authority_rejects_divergent_main_and_runtime_creation_times() {
    for file in ["session.json", RUNTIME_SIDECAR_FILE] {
        let initial = root();
        let fixture = Fixture::new(&initial, false).await;
        let path = directory(&fixture.first, &initial.id).join(file);
        let mut contents: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).await.unwrap()).unwrap();
        contents["created_at"] =
            serde_json::json!(initial.created_at + chrono::Duration::seconds(1));
        fs::write(path, serde_json::to_vec(&contents).unwrap())
            .await
            .unwrap();
        let before = files(&fixture.first, &initial.id).await;
        assert!(
            fixture
                .second
                .load_root_authority(&initial.id)
                .await
                .is_err(),
            "{file}"
        );
        assert_eq!(files(&fixture.first, &initial.id).await, before);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn root_read_and_history_fallback_cannot_revive_missing_or_corrupt_runtime_authority() {
    for missing in [false, true] {
        let initial = root();
        let fixture = Fixture::new(&initial, false).await;
        let mut current = initial.clone();
        current.metadata_version += 1;
        current.set_project_id_meta("project-b");
        fixture.first.save_runtime_state(&current).await.unwrap();
        let path = directory(&fixture.first, &initial.id).join(RUNTIME_SIDECAR_FILE);
        if missing {
            fs::remove_file(path).await.unwrap();
        } else {
            fs::write(path, b"invalid runtime JSON").await.unwrap();
        }

        assert_root_read_unavailable(&fixture.second, &initial.id).await;
        assert!(fixture
            .second
            .load_root_authority(&initial.id)
            .await
            .is_err());
        reject_without_writes(&fixture.second, &initial).await;
        reject_without_writes(&fixture.second, &current).await;
        fixture.finish().await;
    }
}

#[tokio::test]
async fn new_roots_and_ordinary_runtime_updates_preserve_model_history_and_permissions() {
    for runtime in [false, true] {
        let initial = root();
        let fixture = Fixture::new(&initial, runtime).await;
        assert_context(&fixture.second, &initial).await;
        let main_before = files(&fixture.first, &initial.id).await[0].clone();
        let mut current = initial.clone();
        current
            .metadata
            .insert("last_run_status".into(), "completed".into());
        fixture.second.save_runtime_state(&current).await.unwrap();
        assert_eq!(files(&fixture.first, &initial.id).await[0], main_before);
        assert_context(&fixture.first, &current).await;
        let authority = fixture
            .first
            .load_root_authority(&current.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(authority.created_at, current.created_at);
        assert_eq!(authority.metadata_version, current.metadata_version);
        assert_eq!(authority.project_id_meta(), current.project_id_meta());
        assert!(authority.messages.is_empty());
        fixture.finish().await;
    }
}

async fn make_runtime_unavailable(store: &SessionStoreV2, id: &str, missing: bool) {
    let path = directory(store, id).join(RUNTIME_SIDECAR_FILE);
    if missing {
        fs::remove_file(path).await.unwrap();
    } else {
        fs::write(path, b"invalid runtime authority").await.unwrap();
    }
}

#[tokio::test]
async fn clear_rejects_unavailable_root_authority_before_deleting_attachments() {
    for missing in [false, true] {
        let initial = root();
        let fixture = Fixture::new(&initial, false).await;
        let attachment = directory(&fixture.first, &initial.id)
            .join("attachments")
            .join("preserve.txt");
        fs::write(&attachment, b"Preserved attachment")
            .await
            .unwrap();
        make_runtime_unavailable(&fixture.first, &initial.id, missing).await;
        let before = files(&fixture.first, &initial.id).await;

        assert!(fixture.first.clear_session(&initial.id).await.is_err());
        assert_eq!(files(&fixture.first, &initial.id).await, before);
        assert_eq!(fs::read(attachment).await.unwrap(), b"Preserved attachment");
        assert_root_read_unavailable(&fixture.second, &initial.id).await;
        fixture.finish().await;
    }
}

#[tokio::test]
async fn migration_without_a_marker_cannot_reconstruct_missing_root_authority() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let mut current = initial.clone();
    current.metadata_version += 1;
    current.set_project_id_meta("project-b");
    fixture.first.save_runtime_state(&current).await.unwrap();
    make_runtime_unavailable(&fixture.first, &initial.id, true).await;
    let marker = fixture.home.path().join(RUNTIME_SIDECAR_MIGRATION_MARKER);
    assert!(!marker.exists());
    let before = files(&fixture.first, &initial.id).await;

    assert!(fixture.first.migrate_runtime_sidecars().await.is_err());
    assert_eq!(files(&fixture.first, &initial.id).await, before);
    assert!(!marker.exists());
    assert_root_read_unavailable(&fixture.second, &initial.id).await;
    fixture.finish().await;
}

#[tokio::test]
async fn task_cas_rejects_unavailable_root_authority_before_publishing_any_endpoint_or_journal() {
    for missing in [false, true] {
        let mut original = root();
        original.task_list = Some(TaskList {
            session_id: original.id.clone(),
            title: "Original task".into(),
            items: vec![TaskItem {
                id: "task-1".into(),
                description: "Original work".into(),
                ..Default::default()
            }],
            created_at: original.created_at,
            updated_at: original.updated_at,
        });
        original.set_task_list_version_meta("1");
        let fixture = Fixture::new(&original, false).await;
        let mut child = Session::new_child("child-context", &original.id, "model", "child");
        child.task_list = original.task_list.clone();
        child.set_task_list_version_meta("1");
        child.add_message(Message::user("Preserved child history"));
        fixture.first.save_session(&child).await.unwrap();

        let mut updated = original.clone();
        updated.task_list.as_mut().unwrap().items[0].description = "Stale task result".into();
        updated.set_task_list_version_meta("2");
        let mut child_updated = child.clone();
        child_updated.task_list = updated.task_list.clone();
        child_updated.set_task_list_version_meta("2");
        let mut current = original.clone();
        current.metadata_version += 1;
        current.set_project_id_meta("project-b");
        current.task_list.as_mut().unwrap().items[0].description = "Latest work".into();
        current.set_task_list_version_meta("3");
        fixture.first.save_runtime_state(&current).await.unwrap();
        make_runtime_unavailable(&fixture.first, &original.id, missing).await;

        let root_before = files(&fixture.first, &original.id).await;
        let child_dir = directory(&fixture.first, &original.id)
            .join("children")
            .join(&child.id);
        let child_main = fs::read(child_dir.join("session.json")).await.unwrap();
        let child_runtime = fs::read(child_dir.join(RUNTIME_SIDECAR_FILE))
            .await
            .unwrap();
        assert!(fixture
            .first
            .take_runtime_task_durability_events()
            .is_empty());
        assert!(fixture
            .first
            .save_task_control_plane_if_matches(&original, &updated)
            .await
            .is_err());
        assert!(fixture
            .first
            .save_task_control_planes_atomically(&child, &child_updated, &original, &updated)
            .await
            .is_err());
        assert!(fixture
            .first
            .take_runtime_task_durability_events()
            .is_empty());
        assert!(fixture
            .first
            .runtime_task_journal_paths()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(files(&fixture.first, &original.id).await, root_before);
        assert_eq!(
            fs::read(child_dir.join("session.json")).await.unwrap(),
            child_main
        );
        assert_eq!(
            fs::read(child_dir.join(RUNTIME_SIDECAR_FILE))
                .await
                .unwrap(),
            child_runtime
        );
        assert_root_read_unavailable(&fixture.second, &original.id).await;
        fixture.finish().await;
    }
}

#[tokio::test]
async fn full_creation_retry_accepts_only_empty_known_directories_without_canonical_files() {
    for contents in [
        "empty",
        "unknown-file",
        "nonempty-children",
        "nonempty-attachments",
        "symlink",
    ] {
        if contents == "symlink" && !cfg!(unix) {
            continue;
        }
        let initial = root();
        let fixture = Fixture::new(&initial, false).await;
        let mut candidate = initial.clone();
        candidate.id = "partial-root".into();
        candidate.root_session_id = candidate.id.clone();
        let partial = directory(&fixture.first, &candidate.id);
        fs::create_dir_all(partial.join("children")).await.unwrap();
        fs::create_dir(partial.join("attachments")).await.unwrap();
        let preserved_file = match contents {
            "unknown-file" => Some(partial.join("unknown.json")),
            "nonempty-children" => Some(partial.join("children").join("preserve.txt")),
            "nonempty-attachments" => Some(partial.join("attachments").join("preserve.txt")),
            "symlink" => {
                #[cfg(unix)]
                {
                    let external = fixture.home.path().join("external-attachments");
                    fs::create_dir(&external).await.unwrap();
                    fs::remove_dir(partial.join("attachments")).await.unwrap();
                    std::os::unix::fs::symlink(external, partial.join("attachments")).unwrap();
                }
                None
            }
            "empty" => None,
            _ => unreachable!(),
        };
        if let Some(path) = preserved_file.as_ref() {
            fs::write(path, b"Preserved partial-root bytes")
                .await
                .unwrap();
        }

        if contents == "empty" {
            fixture.second.save_session(&candidate).await.unwrap();
            assert_context(&fixture.second, &candidate).await;
            let authority = fixture
                .first
                .load_root_authority(&candidate.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(authority.created_at, candidate.created_at);
            assert_eq!(authority.project_id_meta(), candidate.project_id_meta());
        } else {
            reject_without_writes(&fixture.second, &candidate).await;
            if let Some(path) = preserved_file {
                assert_eq!(
                    fs::read(path).await.unwrap(),
                    b"Preserved partial-root bytes"
                );
            }
            if contents == "symlink" {
                assert!(fs::symlink_metadata(partial.join("attachments"))
                    .await
                    .unwrap()
                    .file_type()
                    .is_symlink());
            }
        }
        assert_context(&fixture.first, &initial).await;
        fixture.finish().await;
    }
}

#[tokio::test]
async fn missing_main_allows_only_full_retry_with_the_exact_runtime_root_context() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let original_main = files(&fixture.first, &initial.id).await[0].clone();
    fs::remove_file(directory(&fixture.first, &initial.id).join("session.json"))
        .await
        .unwrap();
    let before = files(&fixture.first, &initial.id).await;
    assert!(fixture.second.load_session(&initial.id).await.is_err());
    assert!(fixture
        .second
        .load_runtime_control_plane(&initial.id)
        .await
        .is_err());
    for changed in [
        "created_at",
        "identity",
        "project",
        "project-next",
        "revision",
    ] {
        let mut candidate = initial.clone();
        match changed {
            "created_at" => candidate.created_at += chrono::Duration::seconds(1),
            "identity" => candidate.root_session_id = "other-root".into(),
            "project" => candidate.set_project_id_meta("project-b"),
            "project-next" => {
                candidate.set_project_id_meta("project-b");
                candidate.metadata_version += 1;
            }
            "revision" => candidate.metadata_version += 1,
            _ => unreachable!(),
        }
        reject_without_writes(&fixture.second, &candidate).await;
    }

    let error = fixture
        .second
        .save_runtime_state(&initial)
        .await
        .unwrap_err();
    assert!(error
        .get_ref()
        .is_some_and(|error| error.is::<SessionAuthorityConflict>()));
    assert_eq!(files(&fixture.first, &initial.id).await, before);
    fixture.second.save_session(&initial).await.unwrap();
    let completed = files(&fixture.first, &initial.id).await;
    assert_eq!(completed[0], original_main);
    assert_eq!(completed[1], before[1]);
    assert_context(&fixture.first, &initial).await;
    let authority = fixture
        .first
        .load_root_authority(&initial.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(authority.created_at, initial.created_at);
    assert_eq!(authority.project_id_meta(), initial.project_id_meta());
    assert_eq!(authority.metadata_version, initial.metadata_version);
    fixture.finish().await;
}

#[tokio::test]
async fn runtime_save_with_a_stale_index_preserves_canonical_history_and_task_generation() {
    for global_entry_missing in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let stale_store = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        let publisher = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        let mut stale = root();
        stale.task_list = Some(TaskList {
            session_id: stale.id.clone(),
            title: "Old task".into(),
            items: vec![TaskItem::default()],
            created_at: stale.created_at,
            updated_at: stale.updated_at,
        });
        stale.set_task_list_version_meta("1");
        let mut published = stale.clone();
        published.add_message(Message::user(
            "New history absent from the runtime candidate",
        ));
        published.task_list.as_mut().unwrap().title = "Current task".into();
        published.set_task_list_version_meta("3");
        publisher.save_session(&published).await.unwrap();
        if global_entry_missing {
            publisher
                .update_index(|index| {
                    index.sessions.remove(&published.id);
                    Ok(())
                })
                .await
                .unwrap();
        }
        assert!(stale_store.get_index_entry(&stale.id).await.is_none());
        let before = files(&publisher, &published.id).await;
        let error = stale_store.save_runtime_state(&stale).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(files(&publisher, &published.id).await, before);
        assert!(stale_store.get_index_entry(&stale.id).await.is_none());

        let mut candidate = stale;
        candidate.task_list = published.task_list.clone();
        candidate.set_task_list_version_meta("3");
        candidate.set_project_id_meta("project-b");
        candidate.metadata_version += 1;
        assert!(candidate.messages.len() < published.messages.len());
        stale_store.save_runtime_state(&candidate).await.unwrap();
        assert_eq!(files(&publisher, &published.id).await[0], before[0]);
        let authority = publisher
            .load_root_authority(&published.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(authority.project_id_meta().as_deref(), Some("project-b"));
        assert_eq!(authority.metadata_version, candidate.metadata_version);
        assert_eq!(authority.created_at, published.created_at);
        assert_eq!(authority.task_list_version_meta().as_deref(), Some("3"));
        let local_entry = stale_store.get_index_entry(&published.id).await.unwrap();
        assert_eq!(local_entry.message_count, published.messages.len());
        assert_eq!(local_entry.project_id.as_deref(), Some("project-b"));
        let global: SessionsIndex =
            serde_json::from_slice(&fs::read(stale_store.index_path()).await.unwrap()).unwrap();
        assert_eq!(
            global.sessions[&published.id].message_count,
            published.messages.len()
        );
        assert_eq!(
            global.sessions[&published.id].project_id.as_deref(),
            Some("project-b")
        );
        published.set_project_id_meta("project-b");
        published.metadata_version = candidate.metadata_version;
        assert_context(&stale_store, &published).await;
        Fixture {
            first: publisher,
            second: stale_store,
            home,
        }
        .finish()
        .await;
    }
}

#[tokio::test]
async fn root_tool_proof_crash_order_fails_closed_for_enable_and_explicit_disable() {
    use root_context::RootToolProofFault;

    for enable in [true, false] {
        for fault in [
            RootToolProofFault::Prepared,
            RootToolProofFault::Runtime,
            RootToolProofFault::Main,
            RootToolProofFault::Committed,
        ] {
            let home = tempfile::tempdir().unwrap();
            let writer = SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap();
            let mut initial = root();
            if !enable {
                initial.set_root_orchestration_only(true).unwrap();
            }
            writer.save_session(&initial).await.unwrap();
            let mut incoming = initial.clone();
            incoming.set_root_orchestration_only(enable).unwrap();
            *writer.root_tool_proof_fault.lock().unwrap() = Some(fault);
            assert!(writer.save_session(&incoming).await.is_err());
            writer.flush_search_index().await;
            drop(writer);

            let reopened = SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap();
            if fault == RootToolProofFault::Committed {
                for loaded in [
                    reopened.load_session(&incoming.id).await.unwrap().unwrap(),
                    reopened
                        .load_runtime_control_plane(&incoming.id)
                        .await
                        .unwrap()
                        .unwrap(),
                ] {
                    assert_eq!(loaded.root_orchestration_only, enable);
                    assert_eq!(
                        loaded.root_tool_authority_revision,
                        incoming.root_tool_authority_revision
                    );
                }
                reject_without_writes(&reopened, &initial).await;
            } else {
                for error in [
                    reopened.load_session(&incoming.id).await.unwrap_err(),
                    reopened
                        .load_runtime_control_plane(&incoming.id)
                        .await
                        .unwrap_err(),
                    reopened
                        .recover_root_session_from_disk(&incoming.id)
                        .await
                        .unwrap_err(),
                ] {
                    assert!(
                        error
                            .get_ref()
                            .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()),
                        "enable={enable}, fault={fault:?}: {error:?}"
                    );
                }
                reject_without_writes(&reopened, &initial).await;
                reject_without_writes(&reopened, &incoming).await;
            }
            reopened.flush_search_index().await;
            drop(reopened);
            home.close().unwrap();
        }
    }
}

#[tokio::test]
async fn missing_corrupt_or_stale_root_tool_proof_never_reopens_authority() {
    for damage in ["missing", "corrupt", "stale"] {
        let home = tempfile::tempdir().unwrap();
        let writer = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        let initial = root();
        writer.save_session(&initial).await.unwrap();
        let proof =
            directory(&writer, &initial.id).join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE);
        let stale_proof = fs::read(&proof).await.unwrap();
        let mut selected = initial.clone();
        selected.set_root_orchestration_only(true).unwrap();
        writer.save_session(&selected).await.unwrap();
        match damage {
            "missing" => fs::remove_file(&proof).await.unwrap(),
            "corrupt" => fs::write(&proof, b"{bad").await.unwrap(),
            "stale" => fs::write(&proof, stale_proof).await.unwrap(),
            _ => unreachable!(),
        }
        writer.flush_search_index().await;
        drop(writer);
        let reopened = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        for error in [
            reopened.load_session(&selected.id).await.unwrap_err(),
            reopened
                .load_runtime_control_plane(&selected.id)
                .await
                .unwrap_err(),
        ] {
            assert!(
                error
                    .get_ref()
                    .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()),
                "damage={damage}: {error:?}"
            );
        }
        reject_without_writes(&reopened, &initial).await;
        reject_without_writes(&reopened, &selected).await;
        reopened.flush_search_index().await;
        drop(reopened);
        home.close().unwrap();
    }
}

#[tokio::test]
async fn one_time_root_tool_proof_migration_requires_a_valid_existing_pair() {
    let home = tempfile::tempdir().unwrap();
    let writer = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let initial = root();
    writer.save_session(&initial).await.unwrap();
    let proof = directory(&writer, &initial.id).join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE);
    fs::remove_file(&proof).await.unwrap();
    fs::remove_file(home.path().join(".root_tool_authority_proof_v1"))
        .await
        .unwrap();
    writer.flush_search_index().await;
    drop(writer);

    let migrated = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert!(proof.exists());
    assert!(migrated.load_session(&initial.id).await.unwrap().is_some());
    fs::remove_file(&proof).await.unwrap();
    migrated.flush_search_index().await;
    drop(migrated);
    let later = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert!(!proof.exists());
    assert!(later.load_session(&initial.id).await.is_err());
    later.flush_search_index().await;
    drop(later);
    home.close().unwrap();
}

fn mode_request(
    root: &Session,
    operation_id: &str,
    expected_epoch: u64,
    enabled: bool,
    action: RootModeOperationAction,
) -> RootModeOperationRequest {
    RootModeOperationRequest {
        session_id: root.id.clone(),
        operation_id: format!("{expected_epoch}:{operation_id}"),
        birth_token: root.root_mode_birth_token(),
        expected_epoch,
        requested_enabled: enabled,
        action,
    }
}

/// Model the pre-mode final writer's v1-only proof guard. Serde deliberately
/// ignores the new fields, as the older deployed reader did; version rejection
/// is therefore what prevents that writer from erasing a policy-neutral fence.
async fn legacy_v1_save_if_proof_matches(
    store: &SessionStoreV2,
    incoming: &Session,
) -> io::Result<()> {
    #[derive(Deserialize)]
    struct LegacyProof {
        version: u32,
        state: String,
        id: String,
        created_at: DateTime<Utc>,
        authority_identity: SessionAuthorityIdentity,
        root_orchestration_only: bool,
        root_tool_authority_revision: u64,
    }
    let proof: LegacyProof = serde_json::from_slice(
        &fs::read(
            directory(store, &incoming.id).join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE),
        )
        .await?,
    )
    .unwrap();
    if proof.version != 1
        || proof.state != "committed"
        || proof.id != incoming.id
        || proof.created_at != incoming.created_at
        || proof.authority_identity != incoming.authority_identity
        || proof.root_orchestration_only != incoming.root_orchestration_only
        || proof.root_tool_authority_revision != incoming.root_tool_authority_revision
    {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "legacy v1 proof guard",
        ));
    }
    store.save_session(incoming).await
}

#[tokio::test]
async fn first_terminal_root_mode_fence_upgrades_legacy_proof_and_blocks_v1_writer() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let proof_path =
        directory(&fixture.first, &initial.id).join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE);
    let legacy = serde_json::json!({
        "version": 1,
        "state": "committed",
        "id": initial.id,
        "created_at": initial.created_at,
        "authority_identity": initial.authority_identity,
        "root_orchestration_only": initial.root_orchestration_only,
        "root_tool_authority_revision": initial.root_tool_authority_revision,
    });
    fs::write(&proof_path, serde_json::to_vec(&legacy).unwrap())
        .await
        .unwrap();
    assert!(fixture
        .first
        .load_session(&initial.id)
        .await
        .unwrap()
        .is_some());

    let recover = mode_request(
        &initial,
        &Uuid::new_v4().to_string(),
        0,
        true,
        RootModeOperationAction::Recover,
    );
    let fenced = fixture.first.root_mode_operation(&recover).await.unwrap();
    let proof: serde_json::Value =
        serde_json::from_slice(&fs::read(&proof_path).await.unwrap()).unwrap();
    assert_eq!(proof["version"], 2);
    let durable = fixture
        .first
        .load_session(&initial.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(durable.root_tool_authority_revision, 0);
    let before = files(&fixture.first, &initial.id).await;
    assert!(legacy_v1_save_if_proof_matches(&fixture.second, &durable)
        .await
        .is_err());
    assert_eq!(files(&fixture.first, &initial.id).await, before);

    let restarted = SessionStoreV2::new(fixture.home.path().into())
        .await
        .unwrap();
    assert_eq!(
        restarted.root_mode_operation(&recover).await.unwrap(),
        fenced
    );
    restarted.flush_search_index().await;
    drop(restarted);

    let mut downgraded = proof;
    downgraded["version"] = serde_json::json!(1);
    fs::write(&proof_path, serde_json::to_vec(&downgraded).unwrap())
        .await
        .unwrap();
    let before = files(&fixture.first, &initial.id).await;
    assert!(fixture.second.load_session(&initial.id).await.is_err());
    assert!(fixture.second.save_session(&durable).await.is_err());
    assert_eq!(files(&fixture.first, &initial.id).await, before);
    fixture.finish().await;
}

#[tokio::test]
async fn recovery_fence_prevents_late_root_mode_commit_across_stores_and_restart() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let operation_id = Uuid::new_v4().to_string();
    let select = mode_request(
        &initial,
        &operation_id,
        0,
        true,
        RootModeOperationAction::Select,
    );
    let recover = RootModeOperationRequest {
        action: RootModeOperationAction::Recover,
        ..select.clone()
    };
    let fence = fixture.first.root_mode_operation(&recover).await.unwrap();
    assert!(matches!(
        fence,
        RootModeOperationDecision::Terminal(RootModeOperationReceipt {
            outcome: RootModeOperationOutcome::Fenced,
            ..
        })
    ));
    let late = fixture.second.root_mode_operation(&select).await.unwrap();
    assert!(matches!(
        late,
        RootModeOperationDecision::Terminal(RootModeOperationReceipt {
            outcome: RootModeOperationOutcome::Fenced,
            ..
        })
    ));
    let durable = fixture
        .second
        .load_session(&initial.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!durable.root_orchestration_only_enabled());
    assert_eq!(durable.root_tool_authority_revision, 0);
    assert_eq!(durable.root_mode_transition_epoch, 1);
    assert_eq!(durable.messages.len(), initial.messages.len());
    let restarted = SessionStoreV2::new(fixture.home.path().to_path_buf())
        .await
        .unwrap();
    assert_eq!(
        restarted.root_mode_operation(&recover).await.unwrap(),
        fence
    );
    restarted.flush_search_index().await;
    drop(restarted);
    fixture.finish().await;
}

#[tokio::test]
async fn policy_neutral_recovery_rejects_stale_main_on_authority_read() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let main_path = directory(&fixture.first, &initial.id).join("session.json");
    let stale_main = fs::read(&main_path).await.unwrap();
    let recover = mode_request(
        &initial,
        &Uuid::new_v4().to_string(),
        0,
        true,
        RootModeOperationAction::Recover,
    );
    fixture.first.root_mode_operation(&recover).await.unwrap();
    let current_main = fs::read(&main_path).await.unwrap();
    assert!(fixture
        .second
        .load_root_authority(&initial.id)
        .await
        .unwrap()
        .is_some());

    fs::write(&main_path, &stale_main).await.unwrap();
    let error = fixture
        .second
        .load_root_authority(&initial.id)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);

    fs::write(&main_path, current_main).await.unwrap();
    assert!(fixture
        .second
        .load_root_authority(&initial.id)
        .await
        .unwrap()
        .is_some());
    fixture.finish().await;
}

#[tokio::test]
async fn started_select_blocked_before_admission_cannot_pass_recovery_fence() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let select = mode_request(
        &initial,
        &Uuid::new_v4().to_string(),
        0,
        true,
        RootModeOperationAction::Select,
    );
    let recover = RootModeOperationRequest {
        action: RootModeOperationAction::Recover,
        ..select.clone()
    };
    let started = std::sync::Arc::new(tokio::sync::Notify::new());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let first = &fixture.first;
    let second = &fixture.second;
    let (selected, fenced) = tokio::join!(
        async {
            started.notify_one();
            // The caller has started its Select but is paused before V2
            // admission, like a timed-out request still queued for storage.
            release.notified().await;
            first.root_mode_operation(&select).await.unwrap()
        },
        async {
            started.notified().await;
            let fenced = second.root_mode_operation(&recover).await.unwrap();
            release.notify_one();
            fenced
        }
    );
    assert_eq!(selected, fenced);
    assert!(matches!(
        fenced,
        RootModeOperationDecision::Terminal(RootModeOperationReceipt {
            outcome: RootModeOperationOutcome::Fenced,
            ..
        })
    ));
    let durable = fixture
        .first
        .load_session(&initial.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!durable.root_orchestration_only_enabled());
    assert_eq!(durable.root_mode_transition_epoch, 1);
    fixture.finish().await;
}

#[tokio::test]
async fn stale_full_and_runtime_saves_cannot_erase_terminal_root_mode_receipt() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let select = mode_request(
        &initial,
        &Uuid::new_v4().to_string(),
        0,
        true,
        RootModeOperationAction::Select,
    );
    fixture.first.root_mode_operation(&select).await.unwrap();
    assert!(fixture.second.save_session(&initial).await.is_err());
    assert!(fixture.second.save_runtime_state(&initial).await.is_err());

    let mut fresh = fixture
        .second
        .load_session(&initial.id)
        .await
        .unwrap()
        .unwrap();
    fresh.set_last_run_status("succeeded");
    fixture.second.save_runtime_state(&fresh).await.unwrap();
    let durable = fixture
        .first
        .load_session(&initial.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(durable.root_mode_transition_epoch, 1);
    assert_eq!(durable.root_mode_operations.len(), 1);
    assert!(durable.root_orchestration_only_enabled());
    assert_eq!(durable.root_tool_authority_revision, 1);
    fixture.finish().await;
}

#[tokio::test]
async fn root_mode_commit_replays_and_conflicting_concurrent_revision_cannot_win() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let first = mode_request(
        &initial,
        &Uuid::new_v4().to_string(),
        0,
        true,
        RootModeOperationAction::Select,
    );
    let committed = fixture.first.root_mode_operation(&first).await.unwrap();
    assert!(matches!(
        committed,
        RootModeOperationDecision::Terminal(RootModeOperationReceipt {
            outcome: RootModeOperationOutcome::Committed,
            ..
        })
    ));
    assert_eq!(
        fixture.second.root_mode_operation(&first).await.unwrap(),
        committed
    );
    let different_payload = RootModeOperationRequest {
        requested_enabled: false,
        ..first.clone()
    };
    assert_eq!(
        fixture
            .second
            .root_mode_operation(&different_payload)
            .await
            .unwrap(),
        RootModeOperationDecision::OperationConflict
    );
    let stale = mode_request(
        &initial,
        &Uuid::new_v4().to_string(),
        0,
        false,
        RootModeOperationAction::Select,
    );
    assert!(matches!(
        fixture.second.root_mode_operation(&stale).await.unwrap(),
        RootModeOperationDecision::FencedBySuccessor {
            current_epoch: 1,
            ..
        }
    ));
    let durable = fixture
        .first
        .load_session(&initial.id)
        .await
        .unwrap()
        .unwrap();
    assert!(durable.root_orchestration_only_enabled());
    assert_eq!(durable.root_tool_authority_revision, 1);
    assert_eq!(durable.root_mode_transition_epoch, 1);
    assert_eq!(durable.messages.len(), initial.messages.len());
    fixture.finish().await;
}

#[tokio::test]
async fn root_mode_recovery_waits_for_cross_process_commit_and_reads_terminal_result() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let (reached, release) = fixture
        .first
        .pause_full_save_before_filesystem_commit_for_test(&initial.id);
    let select = mode_request(
        &initial,
        &Uuid::new_v4().to_string(),
        0,
        true,
        RootModeOperationAction::Select,
    );
    let recover = RootModeOperationRequest {
        action: RootModeOperationAction::Recover,
        ..select.clone()
    };
    let first = &fixture.first;
    let second = &fixture.second;
    let (selected, recovered) = tokio::join!(
        async { first.root_mode_operation(&select).await.unwrap() },
        async {
            reached.wait().await;
            let pending = second.root_mode_operation(&recover);
            tokio::pin!(pending);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), &mut pending)
                    .await
                    .is_err()
            );
            release.wait().await;
            pending.await.unwrap()
        }
    );
    assert_eq!(selected, recovered);
    fixture.finish().await;
}

#[tokio::test]
async fn root_mode_commit_holds_birth_against_delete_and_old_token_cannot_retarget_recreation() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let (reached, release) = fixture
        .first
        .pause_full_save_before_filesystem_commit_for_test(&initial.id);
    let select = mode_request(
        &initial,
        &Uuid::new_v4().to_string(),
        0,
        true,
        RootModeOperationAction::Select,
    );
    let first = &fixture.first;
    let second = &fixture.second;
    let (selected, deleted) = tokio::join!(
        async { first.root_mode_operation(&select).await.unwrap() },
        async {
            reached.wait().await;
            let deletion = second.delete_session_recursive(&initial.id, true);
            tokio::pin!(deletion);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), &mut deletion)
                    .await
                    .is_err()
            );
            release.wait().await;
            deletion.await.unwrap()
        }
    );
    assert!(matches!(selected, RootModeOperationDecision::Terminal(_)));
    assert!(deleted);
    let recreated = fixture
        .second
        .recreate_root_session(&initial.id, "new-model")
        .await
        .unwrap();
    assert_ne!(
        recreated.root_mode_birth_token(),
        initial.root_mode_birth_token()
    );
    assert_eq!(
        fixture.second.root_mode_operation(&select).await.unwrap(),
        RootModeOperationDecision::BirthMismatch
    );
    fixture.finish().await;
}

#[tokio::test]
async fn evicted_root_mode_receipt_cannot_reuse_its_operation_identity_at_new_epoch() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let first_uuid = Uuid::new_v4().to_string();
    let first = mode_request(
        &initial,
        &first_uuid,
        0,
        true,
        RootModeOperationAction::Select,
    );
    fixture.first.root_mode_operation(&first).await.unwrap();
    for epoch in 1..=ROOT_MODE_OPERATION_HISTORY_LIMIT as u64 {
        let next = mode_request(
            &initial,
            &Uuid::new_v4().to_string(),
            epoch,
            epoch % 2 == 0,
            RootModeOperationAction::Select,
        );
        fixture.second.root_mode_operation(&next).await.unwrap();
    }
    let durable = fixture
        .first
        .load_session(&initial.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(durable.root_mode_transition_epoch, 9);
    assert_eq!(
        durable.root_mode_operations.len(),
        ROOT_MODE_OPERATION_HISTORY_LIMIT
    );
    assert!(durable.root_mode_operation(&first.operation_id).is_none());
    assert!(matches!(
        fixture.second.root_mode_operation(&first).await.unwrap(),
        RootModeOperationDecision::FencedBySuccessor {
            current_epoch: 9,
            ..
        }
    ));
    let reepoch = RootModeOperationRequest {
        expected_epoch: 9,
        requested_enabled: !durable.root_orchestration_only_enabled(),
        ..first
    };
    assert_eq!(
        fixture
            .second
            .root_mode_operation(&reepoch)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let after = fixture
        .second
        .load_session(&initial.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.root_mode_transition_epoch, 9);
    assert_eq!(
        after.root_tool_authority_revision,
        durable.root_tool_authority_revision
    );
    assert!(
        fs::metadata(
            directory(&fixture.first, &initial.id)
                .join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE)
        )
        .await
        .unwrap()
        .len()
            <= 4096
    );
    fixture.finish().await;
}

#[tokio::test]
async fn migration_does_not_commit_a_legacy_sidecar_ahead_of_main_selection() {
    for enable in [true, false] {
        let home = tempfile::tempdir().unwrap();
        let writer = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        let mut main = root();
        if !enable {
            main.set_root_orchestration_only(true).unwrap();
        }
        writer.save_session(&main).await.unwrap();
        let mut side = main.clone();
        side.set_root_orchestration_only(enable).unwrap();
        let directory = directory(&writer, &main.id);
        fs::write(
            directory.join(RUNTIME_SIDECAR_FILE),
            serde_json::to_vec_pretty(&runtime_sidecar_snapshot(&side)).unwrap(),
        )
        .await
        .unwrap();
        fs::remove_file(directory.join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE))
            .await
            .unwrap();
        fs::remove_file(home.path().join(".root_tool_authority_proof_v1"))
            .await
            .unwrap();
        writer.flush_search_index().await;
        drop(writer);

        let reopened = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        assert!(!directory
            .join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE)
            .exists());
        for error in [
            reopened.load_session(&main.id).await.unwrap_err(),
            reopened
                .load_runtime_control_plane(&main.id)
                .await
                .unwrap_err(),
        ] {
            assert!(
                error
                    .get_ref()
                    .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()),
                "enable={enable}: {error:?}"
            );
        }
        reopened.flush_search_index().await;
        drop(reopened);
        home.close().unwrap();
    }
}

#[tokio::test]
async fn migration_does_not_prove_a_structurally_damaged_legacy_main() {
    let home = tempfile::tempdir().unwrap();
    let writer = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let initial = root();
    writer.save_session(&initial).await.unwrap();
    let directory = directory(&writer, &initial.id);
    let main = directory.join("session.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&fs::read(&main).await.unwrap()).unwrap();
    value["messages"] = serde_json::json!(42);
    fs::write(&main, serde_json::to_vec(&value).unwrap())
        .await
        .unwrap();
    fs::remove_file(directory.join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE))
        .await
        .unwrap();
    fs::remove_file(home.path().join(".root_tool_authority_proof_v1"))
        .await
        .unwrap();
    writer.flush_search_index().await;
    drop(writer);
    let reopened = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert!(!directory
        .join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE)
        .exists());
    assert!(reopened
        .load_runtime_control_plane(&initial.id)
        .await
        .is_err());
    reopened.flush_search_index().await;
    drop(reopened);
    home.close().unwrap();
}

#[tokio::test]
async fn legacy_main_only_root_without_index_reports_recovery_not_absence() {
    let home = tempfile::tempdir().unwrap();
    let writer = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let initial = root();
    writer.save_session(&initial).await.unwrap();
    fs::remove_file(directory(&writer, &initial.id).join(RUNTIME_SIDECAR_FILE))
        .await
        .unwrap();
    fs::remove_file(writer.index_path()).await.unwrap();
    writer.flush_search_index().await;
    drop(writer);

    let reopened = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    for error in [
        reopened.load_session(&initial.id).await.unwrap_err(),
        reopened
            .load_runtime_control_plane(&initial.id)
            .await
            .unwrap_err(),
    ] {
        assert!(
            error
                .get_ref()
                .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()),
            "{error:?}"
        );
        assert!(error.to_string().contains("Root index is missing"));
    }
    reopened.flush_search_index().await;
    drop(reopened);
    home.close().unwrap();
}

#[tokio::test]
async fn retained_revoked_root_miss_uses_bounded_identity_evidence() {
    let home = tempfile::tempdir().unwrap();
    let writer = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let mut initial = root();
    initial.add_message(Message::user("x".repeat(4 * 1024 * 1024)));
    let foreign = Session::new("foreign-root", "model");
    writer.save_session(&initial).await.unwrap();
    writer.save_session(&foreign).await.unwrap();
    writer.flush_search_index().await;
    {
        let _lifecycle = writer.lock_session_lifecycle_exclusive().await.unwrap();
        let _task = writer
            .lock_runtime_task_transaction_exclusive()
            .await
            .unwrap();
        assert!(writer.revoke_root_lifetime(&initial.id).await.unwrap());
    }
    fs::remove_file(writer.index_path()).await.unwrap();
    drop(writer);

    let reopened = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert!(reopened.get_index_entry(&initial.id).await.is_none());
    let retained = directory(&reopened, &initial.id);
    let runtime = retained.join(RUNTIME_SIDECAR_FILE);
    let original_runtime = fs::read(&runtime).await.unwrap();
    for _ in 0..8 {
        assert!(reopened
            .load_runtime_control_plane(&initial.id)
            .await
            .unwrap()
            .is_none());
        assert!(reopened.load_session(&initial.id).await.unwrap().is_none());
    }
    // A revoked miss must not parse the potentially transcript-sized main.
    fs::write(retained.join("session.json"), b"{invalid")
        .await
        .unwrap();
    assert!(reopened
        .load_runtime_control_plane(&initial.id)
        .await
        .unwrap()
        .is_none());
    assert!(reopened.load_session(&initial.id).await.unwrap().is_none());

    fs::copy(
        directory(&reopened, &foreign.id).join(RUNTIME_SIDECAR_FILE),
        &runtime,
    )
    .await
    .unwrap();
    for error in [
        reopened
            .load_runtime_control_plane(&initial.id)
            .await
            .unwrap_err(),
        reopened.load_session(&initial.id).await.unwrap_err(),
    ] {
        assert!(error
            .get_ref()
            .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()));
    }
    fs::write(&runtime, original_runtime).await.unwrap();
    fs::remove_file(retained.join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE))
        .await
        .unwrap();
    assert!(reopened
        .load_runtime_control_plane(&initial.id)
        .await
        .is_err());
    reopened.flush_search_index().await;
    drop(reopened);
    home.close().unwrap();
}

#[tokio::test]
async fn legacy_root_proof_migration_precedes_prepared_task_journal_recovery() {
    let home = tempfile::tempdir().unwrap();
    let writer = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let initial = root();
    let child = Session::new_child("z-task-child", &initial.id, "model", "child");
    writer.save_session(&initial).await.unwrap();
    writer.save_session(&child).await.unwrap();
    let proof = directory(&writer, &initial.id).join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE);
    fs::remove_file(&proof).await.unwrap();
    fs::remove_file(home.path().join(".root_tool_authority_proof_v1"))
        .await
        .unwrap();
    let transaction_id = Uuid::new_v4().to_string();
    let journal = RuntimeTaskTransactionJournal {
        version: RUNTIME_TASK_TRANSACTION_VERSION,
        transaction_id,
        first: TaskControlPlaneUndo {
            session_id: initial.id.clone(),
            task_list: None,
            task_list_version: String::new(),
        },
        second: TaskControlPlaneUndo {
            session_id: child.id.clone(),
            task_list: None,
            task_list_version: String::new(),
        },
    };
    writer.write_runtime_task_journal(&journal).await.unwrap();
    writer.flush_search_index().await;
    drop(writer);

    let reopened = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert!(proof.exists());
    assert!(reopened
        .runtime_task_journal_paths()
        .await
        .unwrap()
        .is_empty());
    assert!(reopened.load_session(&initial.id).await.unwrap().is_some());
    reopened.flush_search_index().await;
    drop(reopened);
    home.close().unwrap();
}

#[tokio::test]
async fn legacy_root_proof_migration_precedes_committed_copy_recovery() {
    let home = tempfile::tempdir().unwrap();
    let writer = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let source = Session::new("copy-source", "model");
    writer.save_session(&source).await.unwrap();
    let copied = writer
        .copy_session(&source.id, "copy-target")
        .await
        .unwrap()
        .unwrap();
    let journal = SessionCopyTransactionJournal {
        version: SESSION_COPY_TRANSACTION_VERSION,
        transaction_id: Uuid::new_v4().to_string(),
        source_id: source.id.clone(),
        target_id: copied.id.clone(),
    };
    let prepared = writer.write_session_copy_journal(&journal).await.unwrap();
    let committed = prepared.with_extension("committed");
    atomic_rename(&prepared, &committed).await.unwrap();
    sync_parent_directory_entry(&committed).await.unwrap();
    writer
        .update_index(|index| {
            index.sessions.remove(&copied.id);
            Ok(())
        })
        .await
        .unwrap();
    for id in [&source.id, &copied.id] {
        fs::remove_file(directory(&writer, id).join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE))
            .await
            .unwrap();
    }
    fs::remove_file(home.path().join(".root_tool_authority_proof_v1"))
        .await
        .unwrap();
    writer.flush_search_index().await;
    drop(writer);

    let reopened = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert!(reopened.get_index_entry(&copied.id).await.is_some());
    assert!(reopened.load_session(&source.id).await.unwrap().is_some());
    assert!(reopened.load_session(&copied.id).await.unwrap().is_some());
    assert!(reopened
        .session_copy_journal_paths()
        .await
        .unwrap()
        .is_empty());
    reopened.flush_search_index().await;
    drop(reopened);
    home.close().unwrap();
}

#[tokio::test]
async fn invalid_proof_during_index_rebuild_retains_recovery_lookup_without_sidecar_projection() {
    let home = tempfile::tempdir().unwrap();
    let writer = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let mut selected = root();
    selected.set_root_orchestration_only(true).unwrap();
    writer.save_session(&selected).await.unwrap();
    let proof = directory(&writer, &selected.id).join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE);
    fs::write(&proof, b"{invalid").await.unwrap();
    fs::write(writer.index_path(), b"{invalid").await.unwrap();
    writer.flush_search_index().await;
    drop(writer);

    let rebuilt = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert!(rebuilt.get_index_entry(&selected.id).await.is_some());
    let error = rebuilt.load_session(&selected.id).await.unwrap_err();
    assert!(error
        .get_ref()
        .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()));
    assert!(rebuilt
        .load_runtime_control_plane(&selected.id)
        .await
        .is_err());
    rebuilt.flush_search_index().await;
    drop(rebuilt);
    home.close().unwrap();
}

#[tokio::test]
async fn full_root_reads_reject_child_kind_and_foreign_identity_even_after_index_rebuild() {
    for tamper in ["child-kind", "foreign-id"] {
        let home = tempfile::tempdir().unwrap();
        let writer = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        let mut selected = root();
        selected.set_root_orchestration_only(true).unwrap();
        writer.save_session(&selected).await.unwrap();
        let foreign = Session::new("foreign-proof-root", "model");
        writer.save_session(&foreign).await.unwrap();
        let target = directory(&writer, &selected.id);
        match tamper {
            "child-kind" => {
                let main = target.join("session.json");
                let mut value: serde_json::Value =
                    serde_json::from_slice(&fs::read(&main).await.unwrap()).unwrap();
                value["kind"] = serde_json::json!("child");
                fs::write(main, serde_json::to_vec(&value).unwrap())
                    .await
                    .unwrap();
            }
            "foreign-id" => {
                let source = directory(&writer, &foreign.id);
                for file in ["session.json", RUNTIME_SIDECAR_FILE] {
                    fs::copy(source.join(file), target.join(file))
                        .await
                        .unwrap();
                }
            }
            _ => unreachable!(),
        }
        fs::remove_file(target.join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE))
            .await
            .unwrap();
        assert!(writer.load_session(&selected.id).await.is_err());
        assert!(writer
            .load_runtime_control_plane(&selected.id)
            .await
            .is_err());
        fs::write(writer.index_path(), b"{invalid").await.unwrap();
        writer.flush_search_index().await;
        drop(writer);

        let rebuilt = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        assert!(!matches!(
            rebuilt.load_session(&selected.id).await,
            Ok(Some(_))
        ));
        assert!(!matches!(
            rebuilt.load_runtime_control_plane(&selected.id).await,
            Ok(Some(_))
        ));
        rebuilt.flush_search_index().await;
        drop(rebuilt);
        home.close().unwrap();
    }
}

#[tokio::test]
async fn indexed_runtime_save_preserves_tool_proof_when_main_is_later_damaged() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let main = directory(&fixture.first, &initial.id).join("session.json");
    let proof =
        directory(&fixture.first, &initial.id).join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE);
    let proof_before = fs::read(&proof).await.unwrap();
    fs::write(&main, b"{invalid").await.unwrap();
    let mut runtime = initial.clone();
    runtime
        .metadata
        .insert("last_run_status".into(), "completed".into());
    fixture.second.save_runtime_state(&runtime).await.unwrap();
    assert_eq!(fs::read(&proof).await.unwrap(), proof_before);
    assert_eq!(fs::read(&main).await.unwrap(), b"{invalid");
    assert!(fixture.second.load_session(&initial.id).await.is_err());
    assert!(fixture.second.save_session(&runtime).await.is_err());
    let control = fixture
        .second
        .load_runtime_control_plane(&initial.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        control.root_orchestration_only,
        initial.root_orchestration_only
    );
    assert_eq!(
        control.root_tool_authority_revision,
        initial.root_tool_authority_revision
    );
    fixture.finish().await;
}

#[tokio::test]
async fn full_save_does_not_overwrite_a_structurally_invalid_main_transcript() {
    let initial = root();
    let fixture = Fixture::new(&initial, false).await;
    let main = directory(&fixture.first, &initial.id).join("session.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&fs::read(&main).await.unwrap()).unwrap();
    value["messages"] = serde_json::json!(42);
    let damaged = serde_json::to_vec(&value).unwrap();
    fs::write(&main, &damaged).await.unwrap();
    let error = fixture.second.save_session(&initial).await.unwrap_err();
    assert!(error
        .get_ref()
        .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()));
    assert_eq!(fs::read(&main).await.unwrap(), damaged);
    fixture.finish().await;
}

#[tokio::test]
async fn selected_root_copy_has_an_independent_durable_proof() {
    let home = tempfile::tempdir().unwrap();
    let writer = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let mut selected = root();
    selected.set_root_orchestration_only(true).unwrap();
    writer.save_session(&selected).await.unwrap();
    let copied = writer
        .copy_session(&selected.id, "copied-selected-root")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        copied.root_orchestration_only,
        selected.root_orchestration_only
    );
    assert_eq!(
        copied.root_tool_authority_revision,
        selected.root_tool_authority_revision
    );
    let source_proof = fs::read(
        directory(&writer, &selected.id).join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE),
    )
    .await
    .unwrap();
    let copy_proof =
        fs::read(directory(&writer, &copied.id).join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE))
            .await
            .unwrap();
    assert_ne!(source_proof, copy_proof);
    writer.flush_search_index().await;
    drop(writer);
    let reopened = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert!(reopened
        .load_runtime_control_plane(&copied.id)
        .await
        .unwrap()
        .unwrap()
        .root_orchestration_only_enabled());
    reopened.flush_search_index().await;
    drop(reopened);
    home.close().unwrap();
}

#[tokio::test]
async fn root_control_plane_read_cost_is_bounded_for_large_transcript() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let small = Session::new("small-proof-root", "model");
    store.save_session(&small).await.unwrap();
    let mut large = Session::new("large-proof-root", "model");
    large.add_message(Message::user("x".repeat(4 * 1024 * 1024)));
    store.save_session(&large).await.unwrap();
    let large_dir = directory(&store, &large.id);
    assert!(
        fs::metadata(large_dir.join("session.json"))
            .await
            .unwrap()
            .len()
            > 4 * 1024 * 1024
    );
    assert!(
        fs::metadata(large_dir.join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE))
            .await
            .unwrap()
            .len()
            < 4096
    );
    let mut durations = Vec::new();
    for id in [&small.id, &large.id] {
        let started = std::time::Instant::now();
        for _ in 0..20 {
            let side = store.load_runtime_control_plane(id).await.unwrap().unwrap();
            assert!(side.messages.is_empty());
        }
        durations.push(started.elapsed());
    }
    eprintln!(
        "Root control-plane 20 reads: small={:?}, 4MiB={:?}",
        durations[0], durations[1]
    );
    let started = std::time::Instant::now();
    store.save_runtime_state(&large).await.unwrap();
    eprintln!("Root 4MiB runtime-only save: {:?}", started.elapsed());
    store.flush_search_index().await;
    drop(store);
    home.close().unwrap();
}
