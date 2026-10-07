use bamboo_tickets::{store::FileStore, *};

#[test]
fn optional_local_plan_fields_preserve_older_canonical_bytes() {
    let step = serde_json::json!({"id":"s","parent":null,"title":"Old step","completed":false});
    let parsed: LocalStep = serde_json::from_value(step.clone()).unwrap();
    assert_eq!(
        canonical_bytes(&parsed).unwrap(),
        canonical_bytes(&step).unwrap()
    );
    let receipt = serde_json::json!({"operation_id":"old","principal":"worker","request_hash":"h","committed_seq":3,"ids":{}});
    let parsed: OperationReceipt = serde_json::from_value(receipt.clone()).unwrap();
    assert_eq!(
        canonical_bytes(&parsed).unwrap(),
        canonical_bytes(&receipt).unwrap()
    );
    let command = serde_json::json!({"operation_id":"old", "binding":binding(), "expected_seq":0,"expected_epoch":1,"operations":[]});
    let parsed: Command = serde_json::from_value(command.clone()).unwrap();
    assert_eq!(
        canonical_bytes(&parsed).unwrap(),
        canonical_bytes(&command).unwrap()
    );
}
use std::{
    fs,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

fn binding() -> ScopeBinding {
    ScopeBinding {
        scope_id: "fixture".into(),
        supervisor_session_id: "supervisor".into(),
        binding_revision: 1,
    }
}

#[test]
fn process_lock_fixed_snapshot_full_hash_verification_and_readonly_backup() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("scope");
    let mut store = FileStore::open(&root, binding()).unwrap();
    assert!(FileStore::open(&root, binding()).is_err());
    let (first, mut snapshot) = store.published.clone().unwrap();
    snapshot.seq += 1;
    let next = store.publish(snapshot).unwrap();
    assert_eq!(store.load_snapshot(&first).unwrap().seq, 0);
    assert_eq!(store.load_snapshot(&next).unwrap().seq, 1);
    assert_eq!(store.export(&dir.path().join("backup")).unwrap(), next);
    let backup = FileStore::open(&dir.path().join("backup"), binding()).unwrap();
    assert!(matches!(backup.health, Health::ReadOnly { .. }));
    drop(store);
    let manifest_path = fs::read_dir(root.join("manifests"))
        .unwrap()
        .map(|p| p.unwrap().path())
        .find(|p| !p.file_name().unwrap().to_string_lossy().starts_with('.'))
        .unwrap();
    fs::write(manifest_path, b"truncated").unwrap();
    // Corrupt the currently referenced commit as well; no max-revision guessing.
    fs::write(root.join("commits").join(next), b"truncated").unwrap();
    let store = FileStore::open(&root, binding()).unwrap();
    assert!(matches!(store.health, Health::ReadOnly { .. }));
    assert!(store.published.is_none());
}

#[test]
fn every_failed_publication_boundary_keeps_whole_snapshots_and_uncertain_head_readonly() {
    let dir = tempfile::tempdir().unwrap();
    let mut measure = FileStore::open(&dir.path().join("measure"), binding()).unwrap();
    let trace = Arc::new(AtomicUsize::new(0));
    let count = trace.clone();
    measure.set_fault(Some(Arc::new(move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })));
    let mut snapshot = measure.published.as_ref().unwrap().1.clone();
    snapshot.seq = 1;
    measure.publish(snapshot).unwrap();
    let boundaries = trace.load(Ordering::SeqCst);
    for boundary in 0..boundaries {
        let root = dir.path().join(format!("fault-{boundary}"));
        let mut store = FileStore::open(&root, binding()).unwrap();
        let mut snapshot = store.published.as_ref().unwrap().1.clone();
        snapshot.seq = 1;
        let count = Arc::new(AtomicUsize::new(0));
        store.set_fault(Some(Arc::new(move |_| {
            if count.fetch_add(1, Ordering::SeqCst) == boundary {
                Err(std::io::Error::other("injected failure"))
            } else {
                Ok(())
            }
        })));
        assert!(store.publish(snapshot).is_err());
        assert_eq!(store.published.as_ref().unwrap().1.seq, 0);
        drop(store);
        let store = FileStore::open(&root, binding()).unwrap();
        assert_eq!(store.health, Health::Writable);
        assert!(store.published.as_ref().unwrap().1.seq <= 1);
    }
    eprintln!("verified {boundaries} file-authority I/O failure boundaries");
}

#[test]
fn retry_flushes_reused_object_directories_before_acknowledging_head() {
    for failed_object in 1..=3 {
        let dir = tempfile::tempdir().unwrap();
        let mut store = FileStore::open(dir.path(), binding()).unwrap();
        let original = store.published.clone().unwrap();
        let mut next = original.1.clone();
        next.seq = 1;
        let renamed = Arc::new(AtomicUsize::new(0));
        let count = renamed.clone();
        store.set_fault(Some(Arc::new(move |point| {
            if point == FaultPoint::AfterObjectRename {
                count.fetch_add(1, Ordering::SeqCst);
            }
            if point == FaultPoint::BeforeDirectorySync
                && count.load(Ordering::SeqCst) == failed_object
            {
                return Err(std::io::Error::other("object directory not durable"));
            }
            Ok(())
        })));
        assert!(store.publish(next.clone()).is_err());
        assert_eq!(store.published.as_ref().unwrap().0, original.0);
        assert_eq!(
            canonical_bytes(&store.published.as_ref().unwrap().1).unwrap(),
            canonical_bytes(&original.1).unwrap()
        );
        assert_eq!(store.health, Health::Writable);

        let synced = Arc::new(AtomicUsize::new(0));
        let count = synced.clone();
        store.set_fault(Some(Arc::new(move |point| {
            // All objects up to the failed directory already exist. A retry
            // must flush each reused directory before writing anything new.
            assert_ne!(point, FaultPoint::BeforeHeadRename);
            if point == FaultPoint::BeforeDirectorySync
                && count.fetch_add(1, Ordering::SeqCst) + 1 == failed_object
            {
                return Err(std::io::Error::other("retry directory still not durable"));
            }
            if count.load(Ordering::SeqCst) < failed_object {
                assert_ne!(point, FaultPoint::BeforeObjectRename);
            }
            Ok(())
        })));
        assert!(store.publish(next.clone()).is_err());
        assert_eq!(store.published.as_ref().unwrap().0, original.0);
        assert_eq!(
            canonical_bytes(&store.published.as_ref().unwrap().1).unwrap(),
            canonical_bytes(&original.1).unwrap()
        );
        assert_eq!(store.health, Health::Writable);
        assert_eq!(
            fs::read_to_string(dir.path().join("HEAD")).unwrap(),
            original.0
        );

        store.set_fault(None);
        let published = store.publish(next.clone()).unwrap();
        drop(store);
        let reopened = FileStore::open(dir.path(), binding()).unwrap();
        assert_eq!(reopened.published.as_ref().unwrap().0, published);
        assert_eq!(
            canonical_bytes(&reopened.published.as_ref().unwrap().1).unwrap(),
            canonical_bytes(&next).unwrap()
        );
    }
}

#[test]
fn missing_head_and_symlink_never_create_replacement_authority() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("scope");
    drop(FileStore::open(&root, binding()).unwrap());
    fs::remove_file(root.join("HEAD")).unwrap();
    let store = FileStore::open(&root, binding()).unwrap();
    assert!(matches!(store.health, Health::ReadOnly { .. }));
    drop(store);
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("/etc/passwd", root.join("HEAD")).unwrap();
        let store = FileStore::open(&root, binding()).unwrap();
        assert!(matches!(store.health, Health::ReadOnly { .. }));
        assert!(store.published.is_none());
    }
}

#[test]
fn canonical_nested_request_hash_is_order_independent() {
    let first: serde_json::Value = serde_json::from_str(r#"{"b":{"y":2,"x":1},"a":0}"#).unwrap();
    let second: serde_json::Value = serde_json::from_str(r#"{"a":0,"b":{"x":1,"y":2}}"#).unwrap();
    assert_eq!(
        canonical_bytes(&first).unwrap(),
        canonical_bytes(&second).unwrap()
    );
}
