//! Root Project revisions and creation identity at the raw storage boundary.

use super::*;
use bamboo_domain::{
    AgentRuntimeState, Message, SessionAuthorityConflict, SessionPermissionMode, TaskItem,
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

async fn files(store: &SessionStoreV2, id: &str) -> [Option<Vec<u8>>; 3] {
    let directory = directory(store, id);
    [
        fs::read(directory.join("session.json")).await.ok(),
        fs::read(directory.join(RUNTIME_SIDECAR_FILE)).await.ok(),
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
    let mut replacement = initial.clone();
    replacement.created_at += chrono::Duration::seconds(1);
    replacement.model = "replacement-model".into();
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
async fn history_fallback_cannot_revive_missing_or_corrupt_root_runtime_authority() {
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

        let compatible = fixture
            .second
            .load_session(&initial.id)
            .await
            .unwrap()
            .unwrap();
        assert_context(&fixture.second, &initial).await;
        assert!(fixture
            .second
            .load_root_authority(&initial.id)
            .await
            .is_err());
        reject_without_writes(&fixture.second, &compatible).await;
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
        assert_context(&fixture.second, &initial).await;
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
    assert_context(&fixture.second, &initial).await;
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
        assert_context(&fixture.second, &original).await;
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
