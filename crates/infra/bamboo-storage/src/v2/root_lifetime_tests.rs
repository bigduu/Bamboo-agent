//! Independent-store acceptance for the deletion commit, not index-only mocks.

use super::*;
use bamboo_domain::{Message, SessionAuthorityConflict};

async fn store(home: &Path) -> SessionStoreV2 {
    SessionStoreV2::new(home.to_path_buf()).await.unwrap()
}

fn original() -> Session {
    let mut root = Session::new("lifetime-root", "original-model");
    root.set_project_id_meta("project-a");
    root.metadata_version = 7;
    root.add_message(Message::user("Original lifetime history"));
    root
}

async fn reject(store: &SessionStoreV2, snapshot: &Session) {
    let index = fs::read(store.index_path()).await.unwrap();
    let revision = store.persistence_metrics().full_save.total.count;
    for runtime in [false, true] {
        let error = if runtime {
            store.save_runtime_state(snapshot).await
        } else {
            store.save_session(snapshot).await
        }
        .unwrap_err();
        assert!(
            error
                .get_ref()
                .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()),
            "{error:?}"
        );
        assert_eq!(fs::read(store.index_path()).await.unwrap(), index);
    }
    assert_eq!(store.persistence_metrics().full_save.total.count, revision);
}

async fn revoke_without_removing(store: &SessionStoreV2, id: &str) {
    let _lifecycle = store.lock_session_lifecycle_exclusive().await.unwrap();
    let _task = store
        .lock_runtime_task_transaction_exclusive()
        .await
        .unwrap();
    assert!(store.revoke_root_lifetime(id).await.unwrap());
}

#[tokio::test]
async fn deleted_root_rejects_old_full_and_warm_or_cold_runtime_snapshots_across_restart() {
    let home = tempfile::tempdir().unwrap();
    let first = store(home.path()).await;
    let old = original();
    first.save_session(&old).await.unwrap();
    let warm = store(home.path()).await;
    let snapshot = warm.load_session(&old.id).await.unwrap().unwrap();
    assert!(first.delete_session(&old.id).await.unwrap());
    first.flush_search_index().await;
    let cold = store(home.path()).await;
    for independent in [&warm, &cold] {
        reject(independent, &snapshot).await;
        assert!(independent
            .load_root_authority(&old.id)
            .await
            .unwrap()
            .is_none());
        assert!(independent.load_session(&old.id).await.unwrap().is_none());
        assert!(independent
            .load_runtime_control_plane(&old.id)
            .await
            .unwrap()
            .is_none());
        assert!(!independent.sessions_root_dir().join(&old.id).exists());
    }
    let revocation = cold.root_revocation(&old.id).await.unwrap().unwrap();
    assert_eq!(revocation, old.created_at);
}

#[tokio::test]
async fn trusted_recreation_generates_a_blank_birth_and_preserves_complete_retry() {
    let home = tempfile::tempdir().unwrap();
    let first = store(home.path()).await;
    let old = original();
    first.save_runtime_state(&old).await.unwrap(); // Never-persisted runtime fallback.
    assert!(first.load_root_authority(&old.id).await.unwrap().is_some());
    assert!(first.recreate_root_session(&old.id, "model").await.is_err());
    first.delete_session(&old.id).await.unwrap();

    // Even a fresh arbitrary snapshot cannot claim a reused ID through save.
    reject(&first, &Session::new(&old.id, "new-model")).await;
    let mut replacement = first
        .recreate_root_session(&old.id, "new-model")
        .await
        .unwrap();
    assert!(replacement.created_at > old.created_at);
    assert!(replacement.messages.is_empty());
    assert_eq!(replacement.project_id_meta(), None);
    assert_eq!(replacement.metadata_version, 0);
    assert!(replacement.authority_identity.is_ordinary());
    replacement.add_message(Message::user("New lifetime history"));
    first.save_session(&replacement).await.unwrap();
    let independent = store(home.path()).await;
    let retry = independent
        .recreate_root_session(&old.id, "ignored-model")
        .await
        .unwrap();
    assert_eq!(retry.created_at, replacement.created_at);
    assert_eq!(retry.model, "new-model");
    assert_eq!(retry.messages.len(), 1);
    reject(&independent, &old).await;

    independent.delete_session(&old.id).await.unwrap();
    reject(&first, &old).await;
    reject(&first, &replacement).await;
    let third = first
        .recreate_root_session(&old.id, "third-model")
        .await
        .unwrap();
    assert!(third.created_at > replacement.created_at);
    assert_ne!(third.created_at, old.created_at);
    reject(&independent, &old).await;
    reject(&independent, &replacement).await;
    first.flush_search_index().await;
    independent.flush_search_index().await;
}

#[tokio::test]
async fn committed_revocation_hides_partial_deletion_from_read_rebuild_copy_and_migration() {
    for corrupt_index in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let first = store(home.path()).await;
        let old = original();
        let child = Session::new_child_of("lifetime-child", &old, "model", "child");
        first.save_session(&old).await.unwrap();
        first.save_session(&child).await.unwrap();
        first.flush_search_index().await;
        revoke_without_removing(&first, &old.id).await;
        assert!(first
            .sessions_root_dir()
            .join(&old.id)
            .join("session.json")
            .exists());
        for id in [&old.id, &child.id] {
            assert!(first.load_session(id).await.unwrap().is_none());
            assert!(first
                .load_runtime_control_plane(id)
                .await
                .unwrap()
                .is_none());
        }
        assert!(first.load_root_authority(&old.id).await.unwrap().is_none());
        assert!(first
            .probe_root_session_from_disk(&old.id)
            .await
            .unwrap()
            .is_none());
        assert!(first
            .recover_root_session_from_disk(&old.id)
            .await
            .unwrap()
            .is_none());
        assert!(first
            .copy_session(&old.id, "copy-of-revoked")
            .await
            .is_err());
        reject(&first, &old).await;
        reject(&first, &child).await;

        // An interrupted physical removal can leave only the old main file.
        let runtime = first
            .sessions_root_dir()
            .join(&old.id)
            .join(RUNTIME_SIDECAR_FILE);
        fs::remove_file(&runtime).await.unwrap();
        assert!(first.migrate_runtime_sidecars().await.is_err());
        assert!(!runtime.exists());
        if corrupt_index {
            fs::write(first.index_path(), b"{invalid").await.unwrap();
        }
        let restarted = store(home.path()).await;
        assert!(restarted.get_index_entry(&old.id).await.is_none());
        assert!(restarted.get_index_entry(&child.id).await.is_none());
        assert!(restarted
            .load_root_authority(&old.id)
            .await
            .unwrap()
            .is_none());
        let recreated = restarted
            .recreate_root_session(&old.id, "recreated")
            .await
            .unwrap();
        assert!(recreated.created_at > old.created_at);
        assert!(!restarted
            .sessions_root_dir()
            .join(&old.id)
            .join("children")
            .join(&child.id)
            .exists());
        first.flush_search_index().await;
        restarted.flush_search_index().await;
    }
}

#[tokio::test]
async fn stale_index_delete_revokes_canonical_birth_and_reset_preserves_all_revocations() {
    let home = tempfile::tempdir().unwrap();
    let stale = store(home.path()).await;
    let writer = store(home.path()).await;
    let old = original();
    writer.save_session(&old).await.unwrap();
    assert!(stale.get_index_entry(&old.id).await.is_none());
    assert!(stale.delete_session(&old.id).await.unwrap());
    reject(&writer, &old).await;
    let new = writer
        .recreate_root_session(&old.id, "recreated")
        .await
        .unwrap();
    let unindexed = Session::new("reset-unindexed", "model");
    writer.save_session(&unindexed).await.unwrap();
    stale.dev_reset().await.unwrap();
    for snapshot in [&old, &new, &unindexed] {
        reject(&writer, snapshot).await;
        assert!(writer
            .load_root_authority(&snapshot.id)
            .await
            .unwrap()
            .is_none());
    }
    let restarted = store(home.path()).await;
    assert!(restarted.root_revocation(&old.id).await.unwrap().unwrap() >= new.created_at);
    assert!(restarted
        .root_revocation(&unindexed.id)
        .await
        .unwrap()
        .is_some());
    writer.flush_search_index().await;
    stale.flush_search_index().await;
}

#[tokio::test]
async fn deletion_retry_removes_remaining_files_after_birth_files_and_index_are_gone() {
    let home = tempfile::tempdir().unwrap();
    let first = store(home.path()).await;
    let old = original();
    first.save_session(&old).await.unwrap();
    first.flush_search_index().await;
    revoke_without_removing(&first, &old.id).await;
    let directory = first.sessions_root_dir().join(&old.id);
    fs::write(
        directory.join("attachments").join("remaining.txt"),
        b"remaining",
    )
    .await
    .unwrap();
    fs::remove_file(directory.join("session.json"))
        .await
        .unwrap();
    fs::remove_file(directory.join(RUNTIME_SIDECAR_FILE))
        .await
        .unwrap();
    let restarted = store(home.path()).await;
    assert!(restarted.get_index_entry(&old.id).await.is_none());
    assert!(restarted.delete_session(&old.id).await.unwrap());
    assert!(!directory.exists());
    assert!(!restarted.delete_session(&old.id).await.unwrap());
    reject(&first, &old).await;
}

#[tokio::test]
async fn recreation_index_failure_preserves_complete_pair_for_restart_retry() {
    let home = tempfile::tempdir().unwrap();
    let first = store(home.path()).await;
    let old = original();
    first.save_session(&old).await.unwrap();
    first.delete_session(&old.id).await.unwrap();
    first.flush_search_index().await;
    let index = fs::read(first.index_path()).await.unwrap();
    fs::remove_file(first.index_path()).await.unwrap();
    fs::create_dir(first.index_path()).await.unwrap();
    assert!(first
        .recreate_root_session(&old.id, "new-model")
        .await
        .is_err());
    let published = first.load_root_authority(&old.id).await.unwrap().unwrap();
    assert!(published.created_at > old.created_at);
    fs::remove_dir(first.index_path()).await.unwrap();
    fs::write(first.index_path(), index).await.unwrap();
    let restarted = store(home.path()).await;
    let retry = restarted
        .recreate_root_session(&old.id, "ignored-model")
        .await
        .unwrap();
    assert_eq!(retry.created_at, published.created_at);
    assert_eq!(retry.model, "new-model");
    assert!(restarted.get_index_entry(&old.id).await.is_some());
    reject(&restarted, &old).await;
}

async fn recreate_for_fault_test(
    store: &SessionStoreV2,
    id: &str,
    model: &str,
    supervisor: bool,
) -> io::Result<Session> {
    if supervisor {
        store.get_or_create_default_supervisor(model).await?;
        Ok(store.probe_root_session_from_disk(id).await?.unwrap())
    } else {
        store.recreate_root_session(id, model).await
    }
}

#[tokio::test]
async fn ordinary_and_supervisor_recreation_crash_boundaries_keep_one_published_lifetime() {
    use root_lifetime::RootPublicationFault;

    for supervisor in [false, true] {
        for fault in [
            RootPublicationFault::BeforePublish,
            RootPublicationFault::BeforeIndex,
        ] {
            let home = tempfile::tempdir().unwrap();
            let first = store(home.path()).await;
            let mut old = if supervisor {
                first
                    .get_or_create_default_supervisor("old-model")
                    .await
                    .unwrap();
                first
                    .probe_root_session_from_disk(DEFAULT_SUPERVISOR_SESSION_ID)
                    .await
                    .unwrap()
                    .unwrap()
            } else {
                original()
            };
            old.add_message(Message::user("Old conversation"));
            first.save_session(&old).await.unwrap();
            first.flush_search_index().await;
            revoke_without_removing(&first, &old.id).await;
            assert!(first.sessions_root_dir().join(&old.id).exists());
            *first.root_publication_fault.lock().unwrap() = Some(fault);
            assert!(
                recreate_for_fault_test(&first, &old.id, "new-model", supervisor)
                    .await
                    .is_err()
            );
            reject(&first, &old).await;

            let published = first.load_root_authority(&old.id).await.unwrap();
            if fault == RootPublicationFault::BeforePublish {
                assert!(published.is_none());
                // Model an abrupt process exit leaving a complete but
                // unpublished staging directory outside canonical sessions/.
                let staging = home
                    .path()
                    .join(format!(".root-recreation-{}", Uuid::new_v4()));
                fs::create_dir(&staging).await.unwrap();
                let mut staged = Session::new(&old.id, "unpublished-model");
                staged.created_at = Utc::now() + chrono::Duration::days(365);
                if supervisor {
                    staged.authority_identity = SessionAuthorityIdentity::Supervisor {
                        incarnation_id: Uuid::new_v4(),
                    };
                }
                let bytes = serde_json::to_vec(&staged).unwrap();
                fs::write(staging.join("session.json"), &bytes)
                    .await
                    .unwrap();
                fs::write(staging.join(RUNTIME_SIDECAR_FILE), &bytes)
                    .await
                    .unwrap();
            } else {
                let mut full = first
                    .probe_root_session_from_disk(&old.id)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(full.created_at > old.created_at);
                full.add_message(Message::user("New conversation preserved on retry"));
                first.save_session(&full).await.unwrap();
            }

            let restarted = store(home.path()).await;
            if fault == RootPublicationFault::BeforePublish {
                assert!(restarted
                    .load_root_authority(&old.id)
                    .await
                    .unwrap()
                    .is_none());
            }
            let retry = recreate_for_fault_test(&restarted, &old.id, "new-model", supervisor)
                .await
                .unwrap();
            assert!(retry.created_at > old.created_at);
            assert_eq!(retry.model, "new-model");
            if let Some(published) = published {
                assert_eq!(retry.created_at, published.created_at);
                assert_eq!(retry.authority_identity, published.authority_identity);
                assert_eq!(retry.messages.len(), 1);
            } else {
                assert!(retry.messages.is_empty());
            }
            reject(&restarted, &old).await;
            first.flush_search_index().await;
            restarted.flush_search_index().await;
        }
    }
}

#[tokio::test]
async fn old_root_cannot_bypass_revocation_as_a_child_or_an_empty_creation_layout() {
    let home = tempfile::tempdir().unwrap();
    let first = store(home.path()).await;
    let old = original();
    first.save_session(&old).await.unwrap();
    first.delete_session(&old.id).await.unwrap();
    let other = Session::new("other-root", "model");
    first.save_session(&other).await.unwrap();
    let as_child = Session::new_child_of(&old.id, &other, "model", "old-root-as-child");
    reject(&first, &as_child).await;
    let empty = first.sessions_root_dir().join(&old.id);
    fs::create_dir_all(empty.join("children")).await.unwrap();
    fs::create_dir(empty.join("attachments")).await.unwrap();
    reject(&first, &old).await;
    reject(&first, &Session::new(&old.id, "new-model")).await;
    first.flush_search_index().await;
}

#[tokio::test]
async fn damaged_revocation_fails_closed_without_writes_or_authority() {
    for damage in ["corrupt", "wrong-id", "version", "directory"] {
        let home = tempfile::tempdir().unwrap();
        let first = store(home.path()).await;
        let old = original();
        first.save_session(&old).await.unwrap();
        first.delete_session(&old.id).await.unwrap();
        let evidence = home
            .path()
            .join(".root-revocations")
            .join(format!("{}.json", old.id));
        match damage {
            "directory" => {
                fs::remove_file(&evidence).await.unwrap();
                fs::create_dir(&evidence).await.unwrap();
            }
            "corrupt" => fs::write(&evidence, b"{invalid").await.unwrap(),
            _ => {
                let mut value: serde_json::Value =
                    serde_json::from_slice(&fs::read(&evidence).await.unwrap()).unwrap();
                value[if damage == "wrong-id" {
                    "session_id"
                } else {
                    "version"
                }] = if damage == "wrong-id" {
                    serde_json::json!("other")
                } else {
                    serde_json::json!(9)
                };
                fs::write(&evidence, serde_json::to_vec(&value).unwrap())
                    .await
                    .unwrap();
            }
        }
        reject(&first, &old).await;
        assert!(first.load_root_authority(&old.id).await.is_err());
        assert!(first
            .recreate_root_session(&old.id, "new-model")
            .await
            .is_err());
        assert!(!first.sessions_root_dir().join(&old.id).exists());
        first.flush_search_index().await;
    }
}

#[tokio::test]
async fn recreation_survives_clock_rollback_and_rejects_birth_overflow() {
    for birth in [
        Utc::now() + chrono::Duration::days(365),
        DateTime::<Utc>::MAX_UTC,
    ] {
        let home = tempfile::tempdir().unwrap();
        let first = store(home.path()).await;
        let mut old = original();
        old.created_at = birth;
        first.save_session(&old).await.unwrap();
        first.delete_session(&old.id).await.unwrap();
        let recreated = first.recreate_root_session(&old.id, "new-model").await;
        if birth == DateTime::<Utc>::MAX_UTC {
            assert!(recreated.is_err());
            assert!(!first.sessions_root_dir().join(&old.id).exists());
        } else {
            assert!(recreated.unwrap().created_at > birth);
        }
        reject(&first, &old).await;
        first.flush_search_index().await;
    }
}

#[tokio::test]
async fn child_runtime_lifetime_probe_uses_small_parent_runtime_when_history_is_large_and_invalid()
{
    let home = tempfile::tempdir().unwrap();
    let first = store(home.path()).await;
    let old = original();
    first.save_session(&old).await.unwrap();
    first.delete_session(&old.id).await.unwrap();
    let parent = first
        .recreate_root_session(&old.id, "new-model")
        .await
        .unwrap();
    let mut child = Session::new_child_of("runtime-child", &parent, "model", "child");
    first.save_session(&child).await.unwrap();
    let child_directory = first
        .sessions_root_dir()
        .join(&parent.id)
        .join("children")
        .join(&child.id);
    let original_child_main = fs::read(child_directory.join("session.json"))
        .await
        .unwrap();
    let parent_main = first
        .sessions_root_dir()
        .join(&parent.id)
        .join("session.json");
    fs::write(&parent_main, vec![b'x'; 8 * 1024 * 1024])
        .await
        .unwrap();
    child.model = "updated-runtime-model".into();
    first.save_runtime_state(&child).await.unwrap();
    let loaded = first
        .load_runtime_control_plane(&child.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.model, "updated-runtime-model");
    assert_eq!(
        fs::read(child_directory.join("session.json"))
            .await
            .unwrap(),
        original_child_main
    );
    assert_eq!(
        fs::metadata(&parent_main).await.unwrap().len(),
        8 * 1024 * 1024
    );
    assert!(first.load_root_authority(&parent.id).await.is_err());
    first.flush_search_index().await;
}

#[tokio::test]
async fn child_runtime_lifetime_probe_fails_closed_without_publishing_unavailable_parent_state() {
    for damage in [
        "revoked-birth",
        "missing-runtime",
        "corrupt-runtime",
        "nonregular-runtime",
        "wrong-root",
        "missing-main",
        "nonregular-main",
        "corrupt-cutoff",
        "nonregular-cutoff",
    ] {
        let home = tempfile::tempdir().unwrap();
        let first = Arc::new(store(home.path()).await);
        let old = original();
        first.save_session(&old).await.unwrap();
        first.delete_session(&old.id).await.unwrap();
        let parent = first
            .recreate_root_session(&old.id, "new-model")
            .await
            .unwrap();
        let mut child = Session::new_child_of("runtime-child", &parent, "model", "child");
        first.save_session(&child).await.unwrap();
        first.flush_search_index().await;
        let parent_directory = first.sessions_root_dir().join(&parent.id);
        let parent_runtime = parent_directory.join(RUNTIME_SIDECAR_FILE);
        let parent_main = parent_directory.join("session.json");
        let cutoff_path = home
            .path()
            .join(".root-revocations")
            .join(format!("{}.json", parent.id));
        let child_directory = parent_directory.join("children").join(&child.id);
        let original_child_main = fs::read(child_directory.join("session.json"))
            .await
            .unwrap();
        let original_child_runtime = fs::read(child_directory.join(RUNTIME_SIDECAR_FILE))
            .await
            .unwrap();
        match damage {
            "missing-runtime" => fs::remove_file(&parent_runtime).await.unwrap(),
            "corrupt-runtime" => fs::write(&parent_runtime, b"{invalid").await.unwrap(),
            "nonregular-runtime" => {
                fs::remove_file(&parent_runtime).await.unwrap();
                fs::create_dir(&parent_runtime).await.unwrap();
            }
            "missing-main" => fs::remove_file(&parent_main).await.unwrap(),
            "nonregular-main" => {
                fs::remove_file(&parent_main).await.unwrap();
                fs::create_dir(&parent_main).await.unwrap();
            }
            "corrupt-cutoff" => fs::write(&cutoff_path, b"{invalid").await.unwrap(),
            "nonregular-cutoff" => {
                fs::remove_file(&cutoff_path).await.unwrap();
                fs::create_dir(&cutoff_path).await.unwrap();
            }
            _ => {
                let mut damaged = parent.clone();
                if damage == "revoked-birth" {
                    damaged.created_at = old.created_at;
                } else {
                    damaged.root_session_id = "wrong-root".into();
                }
                fs::write(&parent_runtime, serde_json::to_vec(&damaged).unwrap())
                    .await
                    .unwrap();
            }
        }
        reject(&first, &child).await;
        let published = AtomicBool::new(false);
        let locked = crate::session_merge::LockedSessionStore::new(first.clone());
        assert!(locked
            .save_runtime_only_and_publish(&mut child, |_| {
                published.store(true, Ordering::SeqCst);
            })
            .await
            .is_err());
        assert!(!published.load(Ordering::SeqCst));
        assert_eq!(
            fs::read(child_directory.join("session.json"))
                .await
                .unwrap(),
            original_child_main
        );
        assert_eq!(
            fs::read(child_directory.join(RUNTIME_SIDECAR_FILE))
                .await
                .unwrap(),
            original_child_runtime
        );
    }
}

#[tokio::test]
async fn startup_isolates_a_corrupt_recreated_root_and_preserves_full_save_repair() {
    for rebuild in [false, true] {
        for corrupt_runtime in [false, true] {
            let home = tempfile::tempdir().unwrap();
            let first = store(home.path()).await;
            let old = original();
            first.save_session(&old).await.unwrap();
            first.delete_session(&old.id).await.unwrap();
            let mut recreated = first
                .recreate_root_session(&old.id, "recreated-model")
                .await
                .unwrap();
            recreated.add_message(Message::user("Repairable new lifetime history"));
            first.save_session(&recreated).await.unwrap();
            let child =
                Session::new_child_of("repairable-child", &recreated, "child-model", "child");
            first.save_session(&child).await.unwrap();
            let mut healthy = Session::new("unrelated-healthy-root", "healthy-model");
            healthy.add_message(Message::user("Healthy history"));
            first.save_session(&healthy).await.unwrap();
            first.flush_search_index().await;
            let directory = first.sessions_root_dir().join(&recreated.id);
            let evidence = home
                .path()
                .join(".root-revocations")
                .join(format!("{}.json", recreated.id));
            let before_evidence = fs::read(&evidence).await.unwrap();
            let runtime_path = directory.join(RUNTIME_SIDECAR_FILE);
            let before_runtime = fs::read(&runtime_path).await.unwrap();
            let child_main = directory
                .join("children")
                .join(&child.id)
                .join("session.json");
            let before_child = fs::read(&child_main).await.unwrap();
            let damaged_path = if corrupt_runtime {
                runtime_path.clone()
            } else {
                directory.join("session.json")
            };
            fs::write(&damaged_path, b"{invalid").await.unwrap();
            if rebuild {
                fs::write(first.index_path(), b"{invalid").await.unwrap();
            }

            let restarted = store(home.path()).await;
            assert!(restarted.get_index_entry(&recreated.id).await.is_none());
            assert!(restarted.get_index_entry(&child.id).await.is_none());
            let loaded = restarted.load_session(&healthy.id).await.unwrap().unwrap();
            assert_eq!(loaded.model, healthy.model);
            assert_eq!(loaded.messages.len(), 1);
            assert!(restarted.load_root_authority(&recreated.id).await.is_err());
            assert_eq!(fs::read(&evidence).await.unwrap(), before_evidence);
            assert_eq!(fs::read(&damaged_path).await.unwrap(), b"{invalid");
            assert_eq!(fs::read(&child_main).await.unwrap(), before_child);

            // Main-only corruption retains the exact-runtime full-save repair
            // contract. Missing/corrupt runtime authority cannot be restored by
            // an ordinary writer; the fixture must explicitly repair that
            // canonical evidence before any full save can proceed.
            if corrupt_runtime {
                reject(&restarted, &recreated).await;
                fs::write(&runtime_path, &before_runtime).await.unwrap();
            } else {
                assert_eq!(fs::read(&runtime_path).await.unwrap(), before_runtime);
            }
            restarted.save_session(&recreated).await.unwrap();
            let repaired = restarted
                .load_root_authority(&recreated.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(repaired.created_at, recreated.created_at);
            assert_eq!(
                restarted
                    .load_session(&recreated.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .messages
                    .len(),
                1
            );
            assert!(restarted.load_session(&healthy.id).await.unwrap().is_some());
            assert_eq!(fs::read(&evidence).await.unwrap(), before_evidence);
            reject(&restarted, &old).await;
            restarted.flush_search_index().await;
        }
    }
}
