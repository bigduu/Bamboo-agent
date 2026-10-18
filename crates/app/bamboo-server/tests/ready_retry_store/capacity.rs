//! Capacity and per-Work durability regressions over the real file adapter.
//! Controlled filesystem fixtures only; no provider/admission claim is made.
use super::*;
use bamboo_orchestration::{ready_recovery::VersionedReadyRetry, ReadyBlockKind};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::atomic::AtomicUsize,
};

fn capacity_hash(key: &ReadyRetryKey) -> String {
    content_hash(&canonical_bytes(key).unwrap())
}
fn capacity_marker(root: &Path) -> PathBuf {
    root.join("ready-retry-v1/state.json")
}
fn capacity_leaf(root: &Path, key: &ReadyRetryKey) -> PathBuf {
    root.join("ready-retry-v1/entries")
        .join(capacity_hash(key))
        .join("state.json")
}
fn capacity_membership(root: &Path, key: &ReadyRetryKey) -> PathBuf {
    root.join("ready-retry-v1/entries")
        .join(format!("{}.key", capacity_hash(key)))
}
fn capacity_json(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}
fn capacity_write_envelope(path: &Path, mut envelope: Value) {
    envelope["sha256"] = content_hash(&canonical_bytes(&envelope["state"]).unwrap()).into();
    std::fs::write(path, serde_json::to_vec(&envelope).unwrap()).unwrap();
}
fn capacity_assert_checksum(envelope: &Value) {
    assert_eq!(
        envelope["sha256"],
        content_hash(&canonical_bytes(&envelope["state"]).unwrap())
    );
}
#[cfg(unix)]
fn capacity_lock_inode(root: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(root.join("ready-retry-v1.lock"))
        .unwrap()
        .ino()
}
fn capacity_legacy_record(index: usize) -> ReadyRetryRecord {
    let mut record = initial();
    record.identity_hash = content_hash(format!("legacy-identity-{index}").as_bytes());
    record.authority_epoch = 1 + (index % 7) as u64;
    record.created_at_ms += index as u64;
    record.expires_at_ms = record.created_at_ms + 60_000;
    record.last_seen_at_ms = record.created_at_ms + 100;
    record.next_check_at_ms = record.created_at_ms + 1000;
    record.reason = format!("original durable metadata {index}");
    match index % 4 {
        0 => {}
        1 => {
            record.attempts = 1;
            record.phase = ReadyRetryPhase::Reserved;
            record.request = Some(ReadyRetryRequest::Intent {
                dispatch_key: format!("legacy/original/{index}"),
                spec_hash: "b".repeat(64),
            });
        }
        2 => {
            record.attempts = 3;
            record.phase = ReadyRetryPhase::Deferred;
            record.request = Some(ReadyRetryRequest::Start {
                operation_id: format!("legacy-start-{index}"),
                request_hash: "c".repeat(64),
            });
            record.last_seen_at_ms = record.expires_at_ms + 1;
            record.next_check_at_ms = record.expires_at_ms;
            record.block_kind = Some(ReadyBlockKind::Configuration);
            record.reason = format!("original deadline already expired {index}");
        }
        _ => {
            record.attempts = 2;
            record.phase = ReadyRetryPhase::Quarantined;
            record.request = Some(ReadyRetryRequest::Intent {
                dispatch_key: format!("legacy/unknown/{index}"),
                spec_hash: "d".repeat(64),
            });
            record.block_kind = Some(ReadyBlockKind::OutcomeUnknown);
            record.reason = format!("original execution outcome is unknown {index}");
        }
    }
    record
}

async fn capacity_seed_legacy(
    root: &Path,
    count: usize,
) -> Vec<(ReadyRetryKey, VersionedReadyRetry)> {
    let store = FileReadyRetryStore::new(root);
    assert!(store.load(&key("bootstrap")).await.unwrap().is_none());
    let mut entries = BTreeMap::new();
    let mut expected = vec![];
    for index in 0..count {
        let stable_key = key(&format!("legacy-{index}"));
        let value = VersionedReadyRetry {
            revision: 1 + (index % 7) as u64,
            record: capacity_legacy_record(index),
        };
        entries.insert(
            capacity_hash(&stable_key),
            json!({"key":stable_key,"value":value}),
        );
        expected.push((stable_key, value));
    }
    let state = json!({"schema":1,"revision":1024,"entries":entries});
    let envelope = json!({"sha256":content_hash(&canonical_bytes(&state).unwrap()),"state":state});
    let path = capacity_marker(root);
    std::fs::write(&path, serde_json::to_vec(&envelope).unwrap()).unwrap();
    std::fs::File::open(&path).unwrap().sync_all().unwrap();
    std::fs::File::open(path.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
    if let Some((stable_key, value)) = expected.first() {
        assert_eq!(store.load(stable_key).await.unwrap().as_ref(), Some(value));
    }
    expected
}

#[tokio::test]
async fn full_mixed_schema1_preserves_every_legacy_budget_while_new_work_grows_in_leaves() {
    let root = tempfile::tempdir().unwrap();
    let expected = capacity_seed_legacy(root.path(), 1024).await;
    let before = capacity_json(&capacity_marker(root.path()));
    assert_eq!(before["state"]["entries"].as_object().unwrap().len(), 1024);
    #[cfg(unix)]
    let inode = capacity_lock_inode(root.path());
    let publications = Arc::new(AtomicUsize::new(0));
    let observe = publications.clone();
    let store = FileReadyRetryStore::with_publication_fault(
        root.path(),
        Arc::new(move |phase| {
            if phase == ReadyRetryFilePhase::BeforeFileSync {
                observe.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }),
    );
    let mut new_records = vec![];
    for index in 0..6 {
        let stable_key = key(&format!("new-leaf-{index}"));
        let mut record = reserved();
        record.reason = format!("new Work charge {index}");
        let value = store
            .compare_exchange(&stable_key, None, record)
            .await
            .unwrap();
        assert_eq!(value.revision, 1);
        new_records.push((stable_key, value));
    }
    assert_eq!(
        publications.load(Ordering::SeqCst),
        7,
        "one marker activation, then six independent leaf publications"
    );
    let marker = capacity_json(&capacity_marker(root.path()));
    capacity_assert_checksum(&marker);
    assert_eq!(marker["state"]["schema"], 3);
    assert_eq!(marker["state"]["revision"], before["state"]["revision"]);
    assert_eq!(marker["state"]["entries"], before["state"]["entries"], "all 1024 entries, revisions, requests and old windows survive without deletion or migration");
    assert_ne!(
        marker["state"]["schema"], 1,
        "the original schema-1 reader rejects this marker; no old binary is executed by this test"
    );
    let reopened = FileReadyRetryStore::new(root.path());
    for index in [0, 1, 2, 3, 1023] {
        let (stable_key, value) = &expected[index];
        assert_eq!(
            reopened.load(stable_key).await.unwrap().as_ref(),
            Some(value)
        );
        assert!(!capacity_leaf(root.path(), stable_key)
            .parent()
            .unwrap()
            .exists());
        assert!(matches!(
            reopened.compare_exchange(stable_key, None, initial()).await,
            Err(Error::RevisionConflict)
        ));
    }
    for (stable_key, value) in &new_records {
        assert_eq!(
            reopened.load(stable_key).await.unwrap().as_ref(),
            Some(value)
        );
        let leaf = capacity_json(&capacity_leaf(root.path(), stable_key));
        capacity_assert_checksum(&leaf);
        assert_eq!(
            leaf["state"]["key"],
            serde_json::to_value(stable_key).unwrap()
        );
        assert_eq!(leaf["state"]["value"], serde_json::to_value(value).unwrap());
        assert_eq!(
            std::fs::metadata(capacity_membership(root.path(), stable_key))
                .unwrap()
                .len(),
            0
        );
    }
    // Updating an old key stays in the bounded original map. Neither its
    // original attempt/window/request nor any of the new leaf charges is reset.
    let (old_key, old_value) = &expected[1];
    let mut next = old_value.record.clone();
    next.last_seen_at_ms += 1;
    next.reason.push_str("; observed again");
    let updated = store
        .compare_exchange(old_key, Some(old_value.revision), next.clone())
        .await
        .unwrap();
    assert_eq!(updated.record, next);
    assert_eq!(updated.revision, old_value.revision + 1);
    let after = capacity_json(&capacity_marker(root.path()));
    assert_eq!(after["state"]["entries"].as_object().unwrap().len(), 1024);
    let mut expected_entries = before["state"]["entries"].clone();
    expected_entries[capacity_hash(old_key)]["value"] = serde_json::to_value(&updated).unwrap();
    assert_eq!(after["state"]["entries"], expected_entries);
    assert!(!capacity_leaf(root.path(), old_key)
        .parent()
        .unwrap()
        .exists());
    for (stable_key, value) in &new_records {
        assert_eq!(
            reopened.load(stable_key).await.unwrap().as_ref(),
            Some(value)
        );
    }
    let (quarantined_key, quarantined) = &expected[3];
    let mut attempted_reset = quarantined.record.clone();
    attempted_reset.phase = ReadyRetryPhase::Ready;
    assert!(reopened
        .compare_exchange(quarantined_key, Some(quarantined.revision), attempted_reset)
        .await
        .is_err());
    assert_eq!(
        reopened.load(quarantined_key).await.unwrap().as_ref(),
        Some(quarantined)
    );
    #[cfg(unix)]
    assert_eq!(capacity_lock_inode(root.path()), inode);
}

#[tokio::test]
async fn leaf_cas_is_per_work_and_reopen_cannot_refund_a_charged_window() {
    let root = tempfile::tempdir().unwrap();
    let store = FileReadyRetryStore::new(root.path());
    let a = key("same-work");
    let b = ReadyRetryKey {
        scope_id: "fixture/other-scope".into(),
        ..a.clone()
    };
    let first = store.compare_exchange(&a, None, reserved()).await.unwrap();
    let other = store.compare_exchange(&b, None, initial()).await.unwrap();
    let marker_bytes = std::fs::read(capacity_marker(root.path())).unwrap();
    let mut next = first.record.clone();
    next.phase = ReadyRetryPhase::Deferred;
    next.last_seen_at_ms = 1500;
    next.reason = "original intent awaits bounded recheck".into();
    let second = store
        .compare_exchange(&a, Some(first.revision), next)
        .await
        .unwrap();
    assert_eq!(second.revision, 2);
    assert!(matches!(
        store
            .compare_exchange(&a, Some(first.revision), reserved())
            .await,
        Err(Error::RevisionConflict)
    ));
    for mutation in ["attempts", "identity", "deadline"] {
        let mut invalid = second.record.clone();
        match mutation {
            "attempts" => invalid.attempts = 0,
            "identity" => invalid.identity_hash = "e".repeat(64),
            _ => invalid.expires_at_ms += 1,
        }
        assert!(
            store
                .compare_exchange(&a, Some(second.revision), invalid)
                .await
                .is_err(),
            "{mutation}"
        );
    }
    let reopened = FileReadyRetryStore::new(root.path());
    assert_eq!(reopened.load(&a).await.unwrap(), Some(second));
    assert_eq!(reopened.load(&b).await.unwrap(), Some(other));
    assert_eq!(
        std::fs::read(capacity_marker(root.path())).unwrap(),
        marker_bytes,
        "leaf CAS does not rewrite or grow the legacy map"
    );
}

#[tokio::test]
async fn initialized_sharded_layout_loss_or_legacy_collision_is_never_an_empty_budget() {
    for case in [
        "marker",
        "entries-root",
        "leaf-state",
        "whole-key-directory",
        "membership-missing",
        "membership-nonempty",
        "membership-directory",
        "duplicate-legacy",
        "legacy-marker-only",
        "legacy-directory-only",
        "schema1-with-leaf",
    ] {
        let root = tempfile::tempdir().unwrap();
        let stable_key = key("existing-leaf");
        let store = FileReadyRetryStore::new(root.path());
        let saved = store
            .compare_exchange(&stable_key, None, reserved())
            .await
            .unwrap();
        let leaf_path = capacity_leaf(root.path(), &stable_key);
        match case {
            "marker" => std::fs::remove_file(capacity_marker(root.path())).unwrap(),
            "entries-root" => {
                std::fs::remove_dir_all(root.path().join("ready-retry-v1/entries")).unwrap()
            }
            "leaf-state" => std::fs::remove_file(&leaf_path).unwrap(),
            "whole-key-directory" => std::fs::remove_dir_all(leaf_path.parent().unwrap()).unwrap(),
            "membership-missing" => {
                std::fs::remove_file(capacity_membership(root.path(), &stable_key)).unwrap()
            }
            "membership-nonempty" => {
                std::fs::write(capacity_membership(root.path(), &stable_key), b"not-empty").unwrap()
            }
            "membership-directory" => {
                let membership = capacity_membership(root.path(), &stable_key);
                std::fs::remove_file(&membership).unwrap();
                std::fs::create_dir(membership).unwrap();
            }
            _ => {
                let mut envelope = capacity_json(&capacity_marker(root.path()));
                if matches!(
                    case,
                    "duplicate-legacy" | "legacy-marker-only" | "legacy-directory-only"
                ) {
                    envelope["state"]["revision"] = saved.revision.into();
                    envelope["state"]["entries"][capacity_hash(&stable_key)] =
                        json!({"key":stable_key,"value":saved});
                    if case == "legacy-marker-only" {
                        envelope["state"]["schema"] = 1.into();
                        std::fs::remove_dir_all(leaf_path.parent().unwrap()).unwrap();
                    } else if case == "legacy-directory-only" {
                        envelope["state"]["schema"] = 1.into();
                        std::fs::remove_file(capacity_membership(root.path(), &stable_key))
                            .unwrap();
                    }
                } else {
                    envelope["state"]["schema"] = 1.into();
                }
                capacity_write_envelope(&capacity_marker(root.path()), envelope);
            }
        }
        let reopened = FileReadyRetryStore::new(root.path());
        assert!(reopened.load(&stable_key).await.is_err(), "{case}");
        assert!(
            reopened
                .compare_exchange(&stable_key, None, initial())
                .await
                .is_err(),
            "{case}"
        );
        if matches!(case, "marker" | "entries-root") {
            assert!(
                reopened.load(&key("never-seen")).await.is_err(),
                "missing schema3 authority must not return None: {case}"
            );
        }
    }
}

#[tokio::test]
async fn corrupt_or_oversized_leaf_cannot_reset_credit_or_poison_a_healthy_leaf() {
    for case in [
        "truncated",
        "schema",
        "wrong-key",
        "checksum",
        "zero-revision",
        "invalid-record",
        "oversized",
    ] {
        let root = tempfile::tempdir().unwrap();
        let bad_key = key("damaged-leaf");
        let good_key = key("healthy-leaf");
        let store = FileReadyRetryStore::new(root.path());
        store
            .compare_exchange(&bad_key, None, reserved())
            .await
            .unwrap();
        let healthy = store
            .compare_exchange(&good_key, None, reserved())
            .await
            .unwrap();
        let path = capacity_leaf(root.path(), &bad_key);
        let mut envelope = capacity_json(&path);
        match case {
            "truncated" => std::fs::write(&path, b"{\"state\":").unwrap(),
            "oversized" => {
                // Valid JSON with unchanged checksum, padded beyond the leaf
                // limit but far below the legacy 8MiB limit: size, not parsing,
                // must reject this record.
                let mut bytes = serde_json::to_vec(&envelope).unwrap();
                bytes.resize(64 * 1024 + 1, b' ');
                std::fs::write(&path, bytes).unwrap();
            }
            "checksum" => {
                envelope["sha256"] = "0".repeat(64).into();
                std::fs::write(&path, serde_json::to_vec(&envelope).unwrap()).unwrap();
            }
            _ => {
                match case {
                    "schema" => envelope["state"]["schema"] = 99.into(),
                    "wrong-key" => envelope["state"]["key"]["work_id"] = "another-work".into(),
                    "zero-revision" => envelope["state"]["value"]["revision"] = 0.into(),
                    _ => envelope["state"]["value"]["record"]["attempts"] = 4.into(),
                }
                capacity_write_envelope(&path, envelope);
            }
        }
        let reopened = FileReadyRetryStore::new(root.path());
        assert!(reopened.load(&bad_key).await.is_err(), "{case}");
        assert!(
            reopened
                .compare_exchange(&bad_key, None, initial())
                .await
                .is_err(),
            "{case}"
        );
        assert_eq!(
            reopened.load(&good_key).await.unwrap(),
            Some(healthy),
            "unrelated Work remains readable: {case}"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn sharded_entries_directory_key_directory_and_leaf_symlinks_are_rejected() {
    use std::os::unix::fs::symlink;
    for case in ["entries", "key-directory", "state", "membership"] {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let stable_key = key("linked-leaf");
        let store = FileReadyRetryStore::new(root.path());
        store
            .compare_exchange(&stable_key, None, reserved())
            .await
            .unwrap();
        let leaf_path = capacity_leaf(root.path(), &stable_key);
        let original = std::fs::read(&leaf_path).unwrap();
        let target = match case {
            "entries" => root.path().join("ready-retry-v1/entries"),
            "key-directory" => leaf_path.parent().unwrap().to_path_buf(),
            "membership" => capacity_membership(root.path(), &stable_key),
            _ => leaf_path.clone(),
        };
        let saved = outside.path().join("saved");
        std::fs::rename(&target, &saved).unwrap();
        symlink(&saved, &target).unwrap();
        let reopened = FileReadyRetryStore::new(root.path());
        assert!(reopened.load(&stable_key).await.is_err(), "{case}");
        assert!(
            reopened
                .compare_exchange(&stable_key, None, initial())
                .await
                .is_err(),
            "{case}"
        );
        let outside_leaf = match case {
            "entries" => saved.join(capacity_hash(&stable_key)).join("state.json"),
            "key-directory" => saved.join("state.json"),
            _ => saved,
        };
        let expected = if case == "membership" {
            vec![]
        } else {
            original
        };
        assert_eq!(
            std::fs::read(outside_leaf).unwrap(),
            expected,
            "no linked target was rewritten"
        );
    }
}

#[tokio::test]
async fn marker_and_first_leaf_faults_preserve_legacy_charges_and_publication_order() {
    for (phase, occurrence, expected) in [
        (ReadyRetryFilePhase::BeforeReplace, 1, "old-marker"),
        (ReadyRetryFilePhase::AfterDirectorySync, 1, "schema3-only"),
        (ReadyRetryFilePhase::BeforeReplace, 2, "missing-leaf"),
        (ReadyRetryFilePhase::AfterReplace, 2, "charged-leaf"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let legacy = capacity_seed_legacy(root.path(), 4).await;
        let original_entries =
            capacity_json(&capacity_marker(root.path()))["state"]["entries"].clone();
        let seen = Arc::new(AtomicUsize::new(0));
        let seen_phase = seen.clone();
        let marker_seen_before_leaf = Arc::new(AtomicBool::new(false));
        let marker_seen = marker_seen_before_leaf.clone();
        let observed_root = root.path().to_path_buf();
        let store = FileReadyRetryStore::with_publication_fault(
            root.path(),
            Arc::new(move |observed| {
                if observed == phase && seen_phase.fetch_add(1, Ordering::SeqCst) + 1 == occurrence
                {
                    if occurrence == 2 {
                        let marker = capacity_json(&capacity_marker(&observed_root));
                        capacity_assert_checksum(&marker);
                        marker_seen.store(marker["state"]["schema"] == 3, Ordering::SeqCst);
                    }
                    return Err(io::Error::other("controlled marker/leaf publication fault"));
                }
                Ok(())
            }),
        );
        let new_key = key("first-sharded-charge");
        assert!(
            store
                .compare_exchange(&new_key, None, reserved())
                .await
                .is_err(),
            "{expected}"
        );
        let marker = capacity_json(&capacity_marker(root.path()));
        assert_eq!(marker["state"]["entries"], original_entries);
        let reopened = FileReadyRetryStore::new(root.path());
        for (stable_key, value) in &legacy {
            assert_eq!(
                reopened.load(stable_key).await.unwrap().as_ref(),
                Some(value)
            );
        }
        if occurrence == 2 {
            assert!(
                marker_seen_before_leaf.load(Ordering::SeqCst),
                "schema3 marker was published before leaf contents"
            );
            assert_eq!(
                std::fs::metadata(capacity_membership(root.path(), &new_key))
                    .unwrap()
                    .len(),
                0
            );
            assert!(
                std::fs::read_dir(capacity_leaf(root.path(), &new_key).parent().unwrap())
                    .unwrap()
                    .all(|entry| !entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with("pending-"))
            );
        }
        match expected {
            "old-marker" => {
                assert_eq!(marker["state"]["schema"], 1);
                assert!(reopened.load(&new_key).await.unwrap().is_none());
            }
            "schema3-only" => {
                assert_eq!(marker["state"]["schema"], 3);
                assert!(reopened.load(&new_key).await.unwrap().is_none());
            }
            "missing-leaf" => {
                assert!(capacity_leaf(root.path(), &new_key)
                    .parent()
                    .unwrap()
                    .is_dir());
                assert!(!capacity_leaf(root.path(), &new_key).exists());
                assert!(reopened.load(&new_key).await.is_err());
                assert!(
                    reopened
                        .compare_exchange(&new_key, None, reserved())
                        .await
                        .is_err(),
                    "failed first leaf cannot become an empty budget"
                );
            }
            _ => {
                let charged = reopened.load(&new_key).await.unwrap().unwrap();
                assert_eq!(charged.record, reserved());
                assert_eq!(charged.revision, 1);
                assert!(matches!(
                    reopened.compare_exchange(&new_key, None, initial()).await,
                    Err(Error::RevisionConflict)
                ));
            }
        }
    }
}

#[tokio::test]
async fn uncertain_format_activation_must_be_resynced_before_any_leaf_can_be_written() {
    for barrier_fault in [
        ReadyRetryFilePhase::BeforeFormatSync,
        ReadyRetryFilePhase::AfterFormatSync,
    ] {
        let root = tempfile::tempdir().unwrap();
        let legacy = capacity_seed_legacy(root.path(), 4).await;
        let before = capacity_json(&capacity_marker(root.path()))["state"]["entries"].clone();
        let new_key = key("uncertain-format-work");
        let activation = FileReadyRetryStore::with_publication_fault(
            root.path(),
            Arc::new(|phase| {
                if phase == ReadyRetryFilePhase::AfterReplace {
                    return Err(io::Error::other("format rename result is uncertain"));
                }
                Ok(())
            }),
        );
        assert!(activation
            .compare_exchange(&new_key, None, reserved())
            .await
            .is_err());
        assert_eq!(
            capacity_json(&capacity_marker(root.path()))["state"]["schema"],
            3
        );
        assert!(!capacity_membership(root.path(), &new_key).exists());

        // Even though the format is already visible, a failed durability
        // barrier must prevent both permanent membership and record creation.
        let blocked = FileReadyRetryStore::with_publication_fault(
            root.path(),
            Arc::new(move |phase| {
                if phase == barrier_fault {
                    return Err(io::Error::other("controlled format resync failure"));
                }
                Ok(())
            }),
        );
        assert!(blocked
            .compare_exchange(&new_key, None, reserved())
            .await
            .is_err());
        assert!(!capacity_membership(root.path(), &new_key).exists());
        assert!(!capacity_leaf(root.path(), &new_key)
            .parent()
            .unwrap()
            .exists());

        let synced = Arc::new(AtomicBool::new(false));
        let observed = synced.clone();
        let recovered = FileReadyRetryStore::with_publication_fault(
            root.path(),
            Arc::new(move |phase| {
                if phase == ReadyRetryFilePhase::AfterFormatSync {
                    observed.store(true, Ordering::SeqCst);
                }
                if phase == ReadyRetryFilePhase::BeforeFileSync {
                    assert!(
                        observed.load(Ordering::SeqCst),
                        "leaf before format durability"
                    );
                }
                Ok(())
            }),
        );
        let charged = recovered
            .compare_exchange(&new_key, None, reserved())
            .await
            .unwrap();
        assert!(synced.load(Ordering::SeqCst));
        let reopened = FileReadyRetryStore::new(root.path());
        assert_eq!(reopened.load(&new_key).await.unwrap(), Some(charged));
        for (stable_key, original) in legacy {
            assert_eq!(reopened.load(&stable_key).await.unwrap(), Some(original));
        }
        assert_eq!(
            capacity_json(&capacity_marker(root.path()))["state"]["entries"],
            before
        );
    }
}

struct CapacityRelease(Option<std::sync::mpsc::Sender<()>>);
impl CapacityRelease {
    fn release(&mut self) {
        self.0.take().unwrap().send(()).unwrap();
    }
}
impl Drop for CapacityRelease {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

#[tokio::test]
async fn cancelling_first_leaf_creation_retains_the_stable_writer_until_its_charge_is_durable() {
    let root = tempfile::tempdir().unwrap();
    let legacy = capacity_seed_legacy(root.path(), 4).await;
    #[cfg(unix)]
    let inode = capacity_lock_inode(root.path());
    let (entered, receive) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, wait) = std::sync::mpsc::channel();
    let mut release = CapacityRelease(Some(release));
    let wait = Mutex::new(wait);
    let replacements = AtomicUsize::new(0);
    let store = FileReadyRetryStore::with_publication_fault(
        root.path(),
        Arc::new(move |phase| {
            if phase == ReadyRetryFilePhase::BeforeReplace
                && replacements.fetch_add(1, Ordering::SeqCst) == 1
            {
                entered.lock().unwrap().take().unwrap().send(()).unwrap();
                wait.lock().unwrap().recv().unwrap();
            }
            Ok(())
        }),
    );
    let new_key = key("cancelled-first-leaf");
    let write_key = new_key.clone();
    let writer =
        tokio::spawn(async move { store.compare_exchange(&write_key, None, reserved()).await });
    tokio::time::timeout(Duration::from_secs(5), receive)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        capacity_json(&capacity_marker(root.path()))["state"]["schema"],
        3
    );
    assert_eq!(
        std::fs::metadata(capacity_membership(root.path(), &new_key))
            .unwrap()
            .len(),
        0
    );
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    let reopened = FileReadyRetryStore::new(root.path());
    assert!(matches!(
        reopened.load(&new_key).await,
        Err(Error::ResourceBlocked(_))
    ));
    assert!(
        matches!(
            reopened.load(&legacy[1].0).await,
            Err(Error::ResourceBlocked(_))
        ),
        "the original stable lock protects both legacy and leaf paths"
    );
    release.release();
    let charged = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match reopened.load(&new_key).await {
                Ok(Some(value)) => break value,
                Err(Error::ResourceBlocked(_)) => tokio::task::yield_now().await,
                other => panic!(
                    "first leaf publication did not finish with its original charge: {other:?}"
                ),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(charged.record, reserved());
    assert_eq!(charged.revision, 1);
    assert_eq!(
        reopened.load(&legacy[1].0).await.unwrap().as_ref(),
        Some(&legacy[1].1)
    );
    assert!(matches!(
        reopened.compare_exchange(&new_key, None, initial()).await,
        Err(Error::RevisionConflict)
    ));
    #[cfg(unix)]
    assert_eq!(capacity_lock_inode(root.path()), inode);
}
