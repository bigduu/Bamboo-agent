//! Known retry identities survive loss or rollback of the entries subtree.
//! These tests use the real file adapter and actually try to reacquire credit.
use super::*;
use bamboo_orchestration::{ready_recovery::VersionedReadyRetry, ReadyBlockKind};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

fn membership_hash(key: &ReadyRetryKey) -> String {
    content_hash(&canonical_bytes(key).unwrap())
}

fn entries(root: &Path) -> PathBuf {
    root.join("ready-retry-v1/entries")
}

fn copy_plain_tree(source: &Path, destination: &Path) {
    std::fs::create_dir(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let kind = entry.file_type().unwrap();
        let target = destination.join(entry.file_name());
        assert!(
            !kind.is_symlink(),
            "the controlled snapshot contains no links"
        );
        if kind.is_dir() {
            copy_plain_tree(&entry.path(), &target);
        } else {
            assert!(kind.is_file());
            std::fs::copy(entry.path(), &target).unwrap();
            std::fs::File::open(target).unwrap().sync_all().unwrap();
        }
    }
    std::fs::File::open(destination)
        .unwrap()
        .sync_all()
        .unwrap();
}

fn outside_entries(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(base: &Path, directory: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let relative = path.strip_prefix(base).unwrap();
            if relative == Path::new("entries") {
                continue;
            }
            let kind = entry.file_type().unwrap();
            assert!(!kind.is_symlink());
            if kind.is_dir() {
                visit(base, &path, out);
            } else {
                assert!(kind.is_file());
                out.insert(relative.to_path_buf(), std::fs::read(path).unwrap());
            }
        }
    }
    let base = root.join("ready-retry-v1");
    let mut result = BTreeMap::new();
    visit(&base, &base, &mut result);
    result
}

async fn charged(
    store: &FileReadyRetryStore,
    key: &ReadyRetryKey,
    record: ReadyRetryRecord,
) -> VersionedReadyRetry {
    let first = store.compare_exchange(key, None, initial()).await.unwrap();
    store
        .compare_exchange(key, Some(first.revision), record)
        .await
        .unwrap()
}

fn renewed_window(original: &VersionedReadyRetry) -> ReadyRetryRecord {
    ReadyRetryRecord {
        expires_at_ms: original.record.expires_at_ms + 60_000,
        ..initial()
    }
}

struct ErasedBudgetProbe {
    case: &'static str,
    original: VersionedReadyRetry,
    load: bamboo_tickets::Result<Option<VersionedReadyRetry>>,
    reset: bamboo_tickets::Result<VersionedReadyRetry>,
    after: bamboo_tickets::Result<Option<VersionedReadyRetry>>,
    healthy: bamboo_tickets::Result<Option<VersionedReadyRetry>>,
    expected_healthy: VersionedReadyRetry,
    metadata_unchanged: bool,
}

#[tokio::test]
async fn deleting_both_entry_markers_and_values_cannot_refund_any_charged_budget() {
    let exhausted = ReadyRetryRecord {
        attempts: 3,
        phase: ReadyRetryPhase::Deferred,
        block_kind: Some(ReadyBlockKind::Transient),
        reason: "all three original admission attempts are consumed".into(),
        ..reserved()
    };
    let unknown = ReadyRetryRecord {
        attempts: 2,
        phase: ReadyRetryPhase::Quarantined,
        block_kind: Some(ReadyBlockKind::OutcomeUnknown),
        reason: "the original execution outcome needs canonical reconciliation".into(),
        ..reserved()
    };
    let mut probes = Vec::new();
    for (case, record) in [
        ("reserved", reserved()),
        ("exhausted_deferred", exhausted),
        ("quarantined", unknown),
    ] {
        let root = tempfile::tempdir().unwrap();
        let store = FileReadyRetryStore::new(root.path());
        let known = key(case);
        let healthy_key = key("healthy-unrelated-work");
        let expected_healthy = charged(&store, &healthy_key, reserved()).await;
        let original = charged(&store, &known, record).await;
        assert_eq!(store.load(&known).await.unwrap(), Some(original.clone()));
        let metadata_before = outside_entries(root.path());
        drop(store);

        // Remove both co-located pieces. A later independent membership root
        // is deliberately untouched; the old layout loses its only evidence.
        let hash = membership_hash(&known);
        match std::fs::remove_file(entries(root.path()).join(format!("{hash}.key"))) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => panic!("remove the entries marker: {error}"),
        }
        std::fs::remove_dir_all(entries(root.path()).join(hash)).unwrap();
        std::fs::File::open(entries(root.path()))
            .unwrap()
            .sync_all()
            .unwrap();
        let reopened = FileReadyRetryStore::new(root.path());
        let load = reopened.load(&known).await;
        // Do not assert on load first: the regression is a real successful
        // CAS(None), revision reset and extended window, not merely None.
        let reset = reopened
            .compare_exchange(&known, None, renewed_window(&original))
            .await;
        let after = reopened.load(&known).await;
        let healthy = reopened.load(&healthy_key).await;
        let metadata_unchanged = metadata_before == outside_entries(root.path());
        eprintln!("erased {case}: load={load:?}; fresh_cas={reset:?}");
        probes.push(ErasedBudgetProbe {
            case,
            original,
            load,
            reset,
            after,
            healthy,
            expected_healthy,
            metadata_unchanged,
        });
    }
    // Complete all three actual reset attempts before any behavior assertion.
    for probe in probes {
        assert!(
            probe.reset.is_err(),
            "{} credit was recreated: original={:?}; CAS(None, fresh extended window)={:?}",
            probe.case,
            probe.original,
            probe.reset
        );
        assert!(
            probe.load.is_err(),
            "known lost identity looked absent: {}",
            probe.case
        );
        assert!(
            probe.after.is_err(),
            "failed CAS must not recreate a record: {}",
            probe.case
        );
        assert_eq!(probe.healthy.unwrap(), Some(probe.expected_healthy));
        assert!(
            probe.metadata_unchanged,
            "existing metadata changed: {}",
            probe.case
        );
    }
}

#[tokio::test]
async fn restoring_only_an_older_entries_tree_cannot_reinitialize_a_later_work() {
    let root = tempfile::tempdir().unwrap();
    let store = FileReadyRetryStore::new(root.path());
    let anchor_key = key("anchor-present-before-snapshot");
    let anchor = charged(&store, &anchor_key, reserved()).await;
    let earlier = root.path().join("earlier-entries");
    copy_plain_tree(&entries(root.path()), &earlier);
    let known = key("charged-after-entries-snapshot");
    let original = charged(&store, &known, reserved()).await;
    assert_eq!(store.load(&known).await.unwrap(), Some(original.clone()));
    let metadata_before = outside_entries(root.path());
    drop(store);

    // Restore valid old envelopes and the anchor, while retaining state.json
    // and every byte outside entries, including any independent membership.
    std::fs::remove_dir_all(entries(root.path())).unwrap();
    copy_plain_tree(&earlier, &entries(root.path()));
    std::fs::File::open(root.path().join("ready-retry-v1"))
        .unwrap()
        .sync_all()
        .unwrap();
    let untouched = metadata_before == outside_entries(root.path());
    let reopened = FileReadyRetryStore::new(root.path());
    let load = reopened.load(&known).await;
    let reset = reopened
        .compare_exchange(&known, None, renewed_window(&original))
        .await;
    let after = reopened.load(&known).await;
    let anchor_after = reopened.load(&anchor_key).await;
    // Fail closed per known lost key, rather than disabling a healthy store.
    let unseen = key("genuinely-unseen-after-entries-restore");
    let unseen_before = reopened.load(&unseen).await;
    let fresh = reopened.compare_exchange(&unseen, None, initial()).await;
    let fresh_after = reopened.load(&unseen).await;
    eprintln!("restored entries: load={load:?}; fresh_cas={reset:?}");

    assert!(
        reset.is_err(),
        "restored entries refunded original={original:?}; CAS(None, fresh extended window)={reset:?}"
    );
    assert!(
        load.is_err(),
        "an older subtree cannot prove this known key is new"
    );
    assert!(
        after.is_err(),
        "the rejected CAS must leave the lost record unavailable"
    );
    assert!(
        untouched,
        "the fixture changed bytes outside the entries subtree"
    );
    assert_eq!(anchor_after.unwrap(), Some(anchor));
    assert_eq!(unseen_before.unwrap(), None);
    let fresh = fresh.expect("a genuinely unseen key retains its independent budget");
    assert_eq!(fresh.revision, 1);
    assert_eq!(fresh.record, initial());
    assert_eq!(fresh_after.unwrap(), Some(fresh));
}

fn protected_membership(root: &Path, key: &ReadyRetryKey) -> PathBuf {
    root.join("ready-retry-v1/membership")
        .join(membership_hash(key))
}

fn leaf_state(root: &Path, key: &ReadyRetryKey) -> PathBuf {
    entries(root).join(membership_hash(key)).join("state.json")
}

fn checksummed_json(path: &Path, state: serde_json::Value) {
    let envelope = serde_json::json!({
        "sha256": content_hash(&canonical_bytes(&state).unwrap()), "state": state
    });
    std::fs::write(path, serde_json::to_vec(&envelope).unwrap()).unwrap();
}

fn seed_schema2(root: &Path, key: &ReadyRetryKey, record: ReadyRetryRecord) -> VersionedReadyRetry {
    let value = VersionedReadyRetry {
        revision: 2,
        record,
    };
    let hash = membership_hash(key);
    std::fs::create_dir_all(entries(root).join(&hash)).unwrap();
    std::fs::write(entries(root).join(format!("{hash}.key")), []).unwrap();
    checksummed_json(
        &root.join("ready-retry-v1/state.json"),
        serde_json::json!({"schema":2,"revision":0,"entries":{}}),
    );
    checksummed_json(
        &leaf_state(root, key),
        serde_json::json!({"schema":2,"key":key,"value":value}),
    );
    value
}

#[cfg(unix)]
#[tokio::test]
async fn protected_membership_damage_cannot_recreate_credit() {
    use std::os::unix::fs::symlink;
    for case in [
        "root_missing",
        "root_file",
        "root_symlink",
        "key_missing",
        "key_nonempty",
        "key_directory",
        "key_symlink",
    ] {
        let root = tempfile::tempdir().unwrap();
        let store = FileReadyRetryStore::new(root.path());
        let known = key("protected-known");
        let good = key("protected-healthy");
        let original = charged(&store, &known, reserved()).await;
        let healthy = charged(&store, &good, reserved()).await;
        let header = root.path().join("ready-retry-v1/state.json");
        let header_before = std::fs::read(&header).unwrap();
        let leaf_before = std::fs::read(leaf_state(root.path(), &known)).unwrap();
        let membership = root.path().join("ready-retry-v1/membership");
        let marker = protected_membership(root.path(), &known);
        let saved = root.path().join("saved-membership-fixture");
        drop(store);
        match case {
            "root_missing" => std::fs::remove_dir_all(&membership).unwrap(),
            "root_file" => {
                std::fs::rename(&membership, &saved).unwrap();
                std::fs::write(&membership, b"not a directory").unwrap();
            }
            "root_symlink" => {
                std::fs::rename(&membership, &saved).unwrap();
                symlink(&saved, &membership).unwrap();
            }
            "key_missing" => std::fs::remove_file(&marker).unwrap(),
            "key_nonempty" => std::fs::write(&marker, b"corrupt marker").unwrap(),
            "key_directory" => {
                std::fs::remove_file(&marker).unwrap();
                std::fs::create_dir(&marker).unwrap();
            }
            "key_symlink" => {
                std::fs::rename(&marker, &saved).unwrap();
                symlink(&saved, &marker).unwrap();
            }
            _ => unreachable!(),
        }
        let reopened = FileReadyRetryStore::new(root.path());
        let load = reopened.load(&known).await;
        let reset = reopened
            .compare_exchange(&known, None, renewed_window(&original))
            .await;
        let good_after = reopened.load(&good).await;
        assert!(
            reset.is_err(),
            "{case}: a damaged membership admitted CAS: {reset:?}"
        );
        assert!(
            load.is_err(),
            "{case}: damaged membership looked absent or healthy"
        );
        if case.starts_with("root_") {
            assert!(
                good_after.is_err(),
                "a missing or untrusted root cannot prove any membership"
            );
        } else {
            assert_eq!(good_after.unwrap(), Some(healthy), "{case}");
        }
        assert_eq!(std::fs::read(&header).unwrap(), header_before, "{case}");
        assert_eq!(
            std::fs::read(leaf_state(root.path(), &known)).unwrap(),
            leaf_before,
            "{case}"
        );
    }
}

#[tokio::test]
async fn schema2_keeps_known_budgets_but_never_proves_an_absent_identity_is_new() {
    for record in [
        reserved(),
        ReadyRetryRecord {
            attempts: 3,
            phase: ReadyRetryPhase::Deferred,
            ..reserved()
        },
        ReadyRetryRecord {
            phase: ReadyRetryPhase::Quarantined,
            block_kind: Some(ReadyBlockKind::OutcomeUnknown),
            ..reserved()
        },
    ] {
        let root = tempfile::tempdir().unwrap();
        let known = key("schema2-existing");
        let original = seed_schema2(root.path(), &known, record);
        let header = root.path().join("ready-retry-v1/state.json");
        let header_before = std::fs::read(&header).unwrap();
        let store = FileReadyRetryStore::new(root.path());
        assert_eq!(store.load(&known).await.unwrap(), Some(original.clone()));
        let mut next = original.record.clone();
        next.last_seen_at_ms += 1;
        next.reason.push_str("; same historical budget observed");
        let updated = store
            .compare_exchange(&known, Some(original.revision), next.clone())
            .await
            .unwrap();
        assert_eq!(updated.record, next);
        assert_eq!(updated.revision, original.revision + 1);
        assert_eq!(
            FileReadyRetryStore::new(root.path())
                .load(&known)
                .await
                .unwrap(),
            Some(updated.clone())
        );
        let leaf: serde_json::Value =
            serde_json::from_slice(&std::fs::read(leaf_state(root.path(), &known)).unwrap())
                .unwrap();
        assert_eq!(
            leaf["state"]["schema"], 2,
            "an existing record stays in its original format"
        );
        let missing = key("schema2-never-observed");
        let missing_load = store.load(&missing).await;
        let missing_cas = store.compare_exchange(&missing, None, initial()).await;
        drop(store);
        let hash = membership_hash(&known);
        std::fs::remove_file(entries(root.path()).join(format!("{hash}.key"))).unwrap();
        std::fs::remove_dir_all(entries(root.path()).join(hash)).unwrap();
        let reopened = FileReadyRetryStore::new(root.path());
        let lost_load = reopened.load(&known).await;
        let lost_cas = reopened
            .compare_exchange(&known, None, renewed_window(&updated))
            .await;
        assert!(missing_load.is_err());
        assert!(
            missing_cas.is_err(),
            "schema2 absence cannot confer fresh credit: {missing_cas:?}"
        );
        assert!(lost_load.is_err());
        assert!(
            lost_cas.is_err(),
            "already-erased schema2 keys cannot be backfilled: {lost_cas:?}"
        );
        assert_eq!(std::fs::read(header).unwrap(), header_before);
        assert!(
            !root.path().join("ready-retry-v1/membership").exists(),
            "no automatic scan, upgrade or backfill"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn schema2_rejects_any_independent_membership_instead_of_accepting_a_partial_upgrade() {
    use std::os::unix::fs::symlink;
    for case in ["directory", "file", "symlink"] {
        let root = tempfile::tempdir().unwrap();
        let known = key("schema2-mixed-layout");
        let original = seed_schema2(root.path(), &known, reserved());
        let leaf_before = std::fs::read(leaf_state(root.path(), &known)).unwrap();
        let membership = root.path().join("ready-retry-v1/membership");
        match case {
            "directory" => std::fs::create_dir(&membership).unwrap(),
            "file" => std::fs::write(&membership, []).unwrap(),
            "symlink" => {
                let target = root.path().join("unrelated-empty-directory");
                std::fs::create_dir(&target).unwrap();
                symlink(target, &membership).unwrap();
            }
            _ => unreachable!(),
        }
        let store = FileReadyRetryStore::new(root.path());
        let read = store.load(&known).await;
        let update = store
            .compare_exchange(&known, Some(original.revision), original.record.clone())
            .await;
        let fresh = store
            .compare_exchange(&key("schema2-mixed-unseen"), None, initial())
            .await;
        assert!(read.is_err(), "{case}: schema2 accepted mixed membership");
        assert!(
            update.is_err(),
            "{case}: schema2 updated through mixed membership"
        );
        assert!(
            fresh.is_err(),
            "{case}: partial upgrade admitted a fresh key"
        );
        assert_eq!(
            std::fs::read(leaf_state(root.path(), &known)).unwrap(),
            leaf_before
        );
    }
}

#[tokio::test]
async fn first_membership_sync_fault_retains_known_identity_without_entries_or_fresh_credit() {
    for point in [
        ReadyRetryFilePhase::BeforeMembershipSync,
        ReadyRetryFilePhase::AfterMembershipSync,
    ] {
        let root = tempfile::tempdir().unwrap();
        let known = key("first-membership-fault");
        let store = FileReadyRetryStore::with_publication_fault(
            root.path(),
            Arc::new(move |phase| {
                if phase == point {
                    Err(io::Error::other(
                        "controlled membership synchronization fault",
                    ))
                } else {
                    Ok(())
                }
            }),
        );
        let failed = store.compare_exchange(&known, None, reserved()).await;
        let marker = protected_membership(root.path(), &known);
        let metadata = std::fs::symlink_metadata(&marker).unwrap();
        let reopened = FileReadyRetryStore::new(root.path());
        let read = reopened.load(&known).await;
        let reset = reopened.compare_exchange(&known, None, initial()).await;
        let hash = membership_hash(&known);
        assert!(failed.is_err(), "{point:?}");
        assert!(metadata.is_file() && !metadata.file_type().is_symlink());
        assert_eq!(metadata.len(), 0);
        assert!(
            read.is_err(),
            "{point:?}: an incomplete membership looked new"
        );
        assert!(
            reset.is_err(),
            "{point:?}: failed initialization was retried as new credit: {reset:?}"
        );
        assert!(!entries(root.path()).join(&hash).exists());
        assert!(!entries(root.path()).join(format!("{hash}.key")).exists());
        assert!(
            marker.is_file(),
            "fault handling must never clean the known identity"
        );
    }
}

struct MembershipWriterRelease(Option<std::sync::mpsc::Sender<()>>);
impl Drop for MembershipWriterRelease {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

#[tokio::test]
async fn cancelled_membership_writer_retains_the_lock_until_the_original_charge_is_durable() {
    let root = tempfile::tempdir().unwrap();
    let known = key("cancelled-membership-writer");
    let write_key = known.clone();
    let (entered, receive) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, wait) = std::sync::mpsc::channel();
    let wait = Mutex::new(wait);
    let mut release_on_drop = MembershipWriterRelease(Some(release));
    let store = FileReadyRetryStore::with_publication_fault(
        root.path(),
        Arc::new(move |phase| {
            if phase == ReadyRetryFilePhase::AfterMembershipSync {
                entered.lock().unwrap().take().unwrap().send(()).unwrap();
                wait.lock().unwrap().recv().unwrap();
            }
            Ok(())
        }),
    );
    let task =
        tokio::spawn(async move { store.compare_exchange(&write_key, None, reserved()).await });
    tokio::time::timeout(Duration::from_secs(5), receive)
        .await
        .unwrap()
        .unwrap();
    assert!(protected_membership(root.path(), &known).is_file());
    assert!(!leaf_state(root.path(), &known).exists());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let reader = FileReadyRetryStore::new(root.path());
    assert!(matches!(
        reader.load(&known).await,
        Err(Error::ResourceBlocked(_))
    ));
    assert!(matches!(
        reader.compare_exchange(&known, None, initial()).await,
        Err(Error::ResourceBlocked(_))
    ));
    release_on_drop.0.take().unwrap().send(()).unwrap();
    let persisted = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match reader.load(&known).await {
                Ok(Some(value)) => break value,
                Ok(None) => panic!("cancelled caller refunded the original in-flight charge"),
                Err(Error::ResourceBlocked(_)) => tokio::task::yield_now().await,
                Err(error) => panic!("original writer did not finish: {error}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(persisted.revision, 1);
    assert_eq!(persisted.record, reserved());
    assert!(matches!(
        reader.compare_exchange(&known, None, initial()).await,
        Err(Error::RevisionConflict)
    ));
}
