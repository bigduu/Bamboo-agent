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
