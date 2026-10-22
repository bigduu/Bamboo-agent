//! Real filesystem/CAS/fault-boundary checks using disposable Host roots only.
use bamboo_orchestration::ready_recovery::{
    ReadyRetryKey, ReadyRetryPhase, ReadyRetryRecord, ReadyRetryRequest, ReadyRetryStore,
};
use bamboo_server::app_state::ticket_application::{FileReadyRetryStore, ReadyRetryFilePhase};
use bamboo_tickets::{canonical_bytes, content_hash, Error};
use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

fn key(work: &str) -> ReadyRetryKey {
    ReadyRetryKey {
        scope_id: "fixture/scope".into(),
        work_id: work.into(),
    }
}
fn initial() -> ReadyRetryRecord {
    ReadyRetryRecord {
        identity_hash: "a".repeat(64),
        authority_epoch: 1,
        created_at_ms: 1000,
        expires_at_ms: 61_000,
        last_seen_at_ms: 1000,
        next_check_at_ms: 1000,
        attempts: 0,
        phase: ReadyRetryPhase::Ready,
        request: None,
        block_kind: None,
        reason: String::new(),
    }
}
fn reserved() -> ReadyRetryRecord {
    ReadyRetryRecord {
        attempts: 1,
        phase: ReadyRetryPhase::Reserved,
        request: Some(ReadyRetryRequest::Intent {
            dispatch_key: "fixture/original/1".into(),
            spec_hash: "b".repeat(64),
        }),
        next_check_at_ms: 2000,
        ..initial()
    }
}

#[tokio::test]
async fn durable_round_trip_cas_and_monotonic_transition_preserve_the_lock() {
    let root = tempfile::tempdir().unwrap();
    let store = FileReadyRetryStore::new(root.path());
    assert!(store.load(&key("work")).await.unwrap().is_none());
    #[cfg(unix)]
    let inode = {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(root.path().join("ready-retry-v1.lock"))
            .unwrap()
            .ino()
    };
    let first = store
        .compare_exchange(&key("work"), None, initial())
        .await
        .unwrap();
    assert_eq!(first.revision, 1);
    assert!(matches!(
        store.compare_exchange(&key("work"), None, initial()).await,
        Err(Error::RevisionConflict)
    ));
    let second = store
        .compare_exchange(&key("work"), Some(1), reserved())
        .await
        .unwrap();
    assert_eq!(second.revision, 2);
    assert!(
        store
            .compare_exchange(&key("work"), Some(2), initial())
            .await
            .is_err(),
        "attempts cannot be refunded"
    );
    let mut extended = reserved();
    extended.expires_at_ms += 1;
    assert!(store
        .compare_exchange(&key("work"), Some(2), extended)
        .await
        .is_err());
    let reopened = FileReadyRetryStore::new(root.path());
    assert_eq!(reopened.load(&key("work")).await.unwrap(), Some(second));
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            std::fs::metadata(root.path().join("ready-retry-v1.lock"))
                .unwrap()
                .ino(),
            inode
        );
    }
}

#[tokio::test]
async fn missing_corrupt_future_and_oversized_metadata_are_never_empty_budgets() {
    for case in ["missing", "truncated", "future", "checksum", "oversized"] {
        let root = tempfile::tempdir().unwrap();
        let store = FileReadyRetryStore::new(root.path());
        store
            .compare_exchange(&key("work"), None, reserved())
            .await
            .unwrap();
        let path = root.path().join("ready-retry-v1/state.json");
        match case {
            "missing" => std::fs::remove_file(&path).unwrap(),
            "truncated" => std::fs::write(&path, b"{\"state\":").unwrap(),
            "oversized" => std::fs::write(&path, vec![b' '; 8 * 1024 * 1024 + 1]).unwrap(),
            _ => {
                let mut envelope: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                if case == "future" {
                    envelope["state"]["schema"] = 99.into();
                } else {
                    envelope["sha256"] = "0".repeat(64).into();
                }
                std::fs::write(&path, serde_json::to_vec(&envelope).unwrap()).unwrap();
            }
        }
        assert!(
            FileReadyRetryStore::new(root.path())
                .load(&key("work"))
                .await
                .is_err(),
            "{case}"
        );
        assert!(
            FileReadyRetryStore::new(root.path())
                .compare_exchange(&key("work"), None, initial())
                .await
                .is_err(),
            "{case}"
        );
    }
}

#[tokio::test]
async fn failed_publication_keeps_the_old_or_new_charged_record_without_rollback() {
    for point in [
        ReadyRetryFilePhase::BeforeFileSync,
        ReadyRetryFilePhase::BeforeReplace,
        ReadyRetryFilePhase::AfterReplace,
        ReadyRetryFilePhase::BeforeDirectorySync,
        ReadyRetryFilePhase::AfterDirectorySync,
    ] {
        let root = tempfile::tempdir().unwrap();
        let active = Arc::new(AtomicBool::new(false));
        let flag = active.clone();
        let store = FileReadyRetryStore::with_publication_fault(
            root.path(),
            Arc::new(move |phase| {
                if flag.load(Ordering::SeqCst) && phase == point {
                    return Err(io::Error::other("controlled publication fault"));
                }
                Ok(())
            }),
        );
        store
            .compare_exchange(&key("work"), None, initial())
            .await
            .unwrap();
        active.store(true, Ordering::SeqCst);
        assert!(store
            .compare_exchange(&key("work"), Some(1), reserved())
            .await
            .is_err());
        let actual = FileReadyRetryStore::new(root.path())
            .load(&key("work"))
            .await
            .unwrap()
            .unwrap();
        let replaced = matches!(
            point,
            ReadyRetryFilePhase::AfterReplace
                | ReadyRetryFilePhase::BeforeDirectorySync
                | ReadyRetryFilePhase::AfterDirectorySync
        );
        assert_eq!(actual.record, if replaced { reserved() } else { initial() });
        assert_eq!(actual.revision, if replaced { 2 } else { 1 });
        assert!(std::fs::read_dir(root.path().join("ready-retry-v1"))
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("pending-")));
    }
}

#[tokio::test]
async fn cancelled_caller_cannot_release_its_blocking_writer_or_refund_the_charge() {
    let root = tempfile::tempdir().unwrap();
    let active = Arc::new(AtomicBool::new(false));
    let flag = active.clone();
    let (entered, receive) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, wait) = std::sync::mpsc::channel();
    let wait = Mutex::new(wait);
    let store = FileReadyRetryStore::with_publication_fault(
        root.path(),
        Arc::new(move |phase| {
            if flag.load(Ordering::SeqCst) && phase == ReadyRetryFilePhase::BeforeReplace {
                entered.lock().unwrap().take().unwrap().send(()).unwrap();
                wait.lock().unwrap().recv().unwrap();
            }
            Ok(())
        }),
    );
    store
        .compare_exchange(&key("work"), None, initial())
        .await
        .unwrap();
    active.store(true, Ordering::SeqCst);
    let task = tokio::spawn(async move {
        store
            .compare_exchange(&key("work"), Some(1), reserved())
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), receive)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let reader = FileReadyRetryStore::new(root.path());
    assert!(matches!(
        reader.load(&key("work")).await,
        Err(Error::ResourceBlocked(_))
    ));
    release.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match reader.load(&key("work")).await {
                Ok(value) => break value.unwrap(),
                Err(Error::ResourceBlocked(_)) => tokio::task::yield_now().await,
                Err(error) => panic!("{error}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(result.record, reserved());
    assert_eq!(result.revision, 2);
}

#[tokio::test]
async fn concurrent_first_reader_keeps_bootstrap_under_the_stable_lock() {
    let root = tempfile::tempdir().unwrap();
    let (entered, receive) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, wait) = std::sync::mpsc::channel();
    let wait = Mutex::new(wait);
    let first = FileReadyRetryStore::with_publication_fault(
        root.path(),
        Arc::new(move |phase| {
            if phase == ReadyRetryFilePhase::BeforeReplace {
                entered.lock().unwrap().take().unwrap().send(()).unwrap();
                wait.lock().unwrap().recv().unwrap();
            }
            Ok(())
        }),
    );
    let task = tokio::spawn(async move { first.load(&key("work")).await });
    tokio::time::timeout(Duration::from_secs(5), receive)
        .await
        .unwrap()
        .unwrap();
    let second = FileReadyRetryStore::new(root.path());
    assert!(matches!(
        second.load(&key("work")).await,
        Err(Error::ResourceBlocked(_))
    ));
    release.send(()).unwrap();
    assert!(task.await.unwrap().unwrap().is_none());
    assert!(second.load(&key("work")).await.unwrap().is_none());
    second
        .compare_exchange(&key("work"), None, reserved())
        .await
        .unwrap();
    assert_eq!(
        second
            .load(&key("work"))
            .await
            .unwrap()
            .unwrap()
            .record
            .attempts,
        1
    );
}

#[tokio::test]
async fn full_store_grows_without_evicting_an_exhausted_or_live_record() {
    let root = tempfile::tempdir().unwrap();
    let store = FileReadyRetryStore::new(root.path());
    store.load(&key("seed")).await.unwrap();
    let mut entries = serde_json::Map::new();
    for index in 0..1024 {
        let key = key(&format!("work-{index}"));
        entries.insert(
            content_hash(&canonical_bytes(&key).unwrap()),
            serde_json::json!({"key":key,"value":{"revision":1,"record":reserved()}}),
        );
    }
    let state = serde_json::json!({"schema":1,"revision":1024,"entries":entries});
    let envelope =
        serde_json::json!({"sha256":content_hash(&canonical_bytes(&state).unwrap()),"state":state});
    std::fs::write(
        root.path().join("ready-retry-v1/state.json"),
        serde_json::to_vec(&envelope).unwrap(),
    )
    .unwrap();
    let overflow = store
        .compare_exchange(&key("overflow"), None, initial())
        .await
        .unwrap();
    assert_eq!(
        store.load(&key("work-0")).await.unwrap().unwrap().record,
        reserved()
    );
    assert_eq!(store.load(&key("overflow")).await.unwrap(), Some(overflow));
}

#[tokio::test]
async fn stable_scope_and_work_identity_are_isolated_and_invalid_records_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let store = FileReadyRetryStore::new(root.path());
    store
        .compare_exchange(&key("work"), None, reserved())
        .await
        .unwrap();
    let other = ReadyRetryKey {
        scope_id: "fixture/another-scope".into(),
        ..key("work")
    };
    assert!(store.load(&other).await.unwrap().is_none());
    let mut invalid = initial();
    invalid.attempts = 4;
    assert!(store.compare_exchange(&other, None, invalid).await.is_err());
    assert_eq!(
        store
            .load(&key("work"))
            .await
            .unwrap()
            .unwrap()
            .record
            .attempts,
        1
    );
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_directory_lock_and_record_are_rejected() {
    use std::os::unix::fs::symlink;
    for target in ["directory", "lock", "record"] {
        let root = tempfile::tempdir().unwrap();
        let destination = tempfile::tempdir().unwrap();
        let store = FileReadyRetryStore::new(root.path());
        match target {
            "directory" => symlink(destination.path(), root.path().join("ready-retry-v1")).unwrap(),
            "lock" => {
                std::fs::write(destination.path().join("lock"), b"").unwrap();
                symlink(
                    destination.path().join("lock"),
                    root.path().join("ready-retry-v1.lock"),
                )
                .unwrap();
            }
            _ => {
                store.load(&key("work")).await.unwrap();
                let record = root.path().join("ready-retry-v1/state.json");
                std::fs::rename(&record, destination.path().join("state")).unwrap();
                symlink(destination.path().join("state"), record).unwrap();
            }
        }
        assert!(store.load(&key("work")).await.is_err(), "{target}");
    }
}

#[path = "ready_retry_store/capacity.rs"]
mod capacity;

#[path = "ready_retry_store/membership.rs"]
mod membership;

#[path = "ready_retry_store/lock_identity.rs"]
mod lock_identity;
