//! Stored behavior stamps survive reopen; ordinary checkpoints cannot migrate
//! or replace a conflicting execution snapshot. All data belongs to TempDir.
use super::SessionStoreV2;
use bamboo_domain::{storage::Storage, Message, Session};
use serde_json::{json, Value};
use std::path::Path;

async fn change_stamp(home: &Path, id: &str, main: Option<u32>, runtime: Option<u32>) {
    for (name, version) in [("session.json", main), ("runtime.json", runtime)] {
        let path = home.join("sessions").join(id).join(name);
        let mut raw: Value = serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        match version {
            Some(version) => raw["session_version"] = json!(version),
            None => { raw.as_object_mut().unwrap().remove("session_version"); }
        }
        tokio::fs::write(path, serde_json::to_vec_pretty(&raw).unwrap()).await.unwrap();
    }
}

async fn pair(home: &Path, id: &str) -> [Vec<u8>; 2] {
    let directory = home.join("sessions").join(id);
    [tokio::fs::read(directory.join("session.json")).await.unwrap(),
     tokio::fs::read(directory.join("runtime.json")).await.unwrap()]
}

#[tokio::test]
async fn session_version_store_overlay_preserves_main_and_conflict_after_reopen() {
    for (main, runtime) in [(Some(99), Some(1)), (Some(1), Some(99)), (None, Some(99))] {
        let home = tempfile::tempdir().unwrap();
        let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
        let mut session = Session::new("pair", "model");
        session.add_message(Message::user("history survives a behavior conflict"));
        storage.save_session(&session).await.unwrap();
        change_stamp(home.path(), "pair", main, runtime).await;
        let before = pair(home.path(), "pair").await;
        drop(storage);
        let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
        let loaded = storage.load_session("pair").await.unwrap().unwrap();
        assert_eq!(loaded.session_version, main, "Main owns the behavior stamp");
        assert!(!loaded.execution_compatibility().execution_allowed);
        assert_eq!(loaded.messages[0].content, "history survives a behavior conflict");
        assert_eq!(pair(home.path(), "pair").await, before);
    }
}

async fn conflicting_checkpoint(runtime_only: bool) {
    let home = tempfile::tempdir().unwrap();
    let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
    let session = Session::new("future", "model");
    storage.save_session(&session).await.unwrap();
    change_stamp(home.path(), "future", Some(99), Some(99)).await;
    let before = pair(home.path(), "future").await;
    let mut loaded = storage.load_session("future").await.unwrap().unwrap();
    loaded.title = "must not publish".into();
    loaded.add_message(Message::user("must not append"));
    let result = if runtime_only {
        storage.save_runtime_state(&loaded).await
    } else {
        storage.save_session(&loaded).await
    };
    let error = result.expect_err("a conflicting behavior cannot checkpoint replacement state");
    assert!(error.to_string().contains("session_execution_incompatible"), "{error}");
    assert_eq!(pair(home.path(), "future").await, before);
}

#[tokio::test]
async fn session_version_store_future_full_checkpoint_has_no_effect() {
    conflicting_checkpoint(false).await;
}

#[tokio::test]
async fn session_version_store_future_runtime_checkpoint_has_no_effect() {
    conflicting_checkpoint(true).await;
}

#[tokio::test]
async fn session_version_store_legacy_checkpoints_preserve_missing_stamp() {
    let home = tempfile::tempdir().unwrap();
    let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
    storage.save_session(&Session::new("legacy", "model")).await.unwrap();
    change_stamp(home.path(), "legacy", None, None).await;
    let mut loaded = storage.load_session("legacy").await.unwrap().unwrap();
    assert!(loaded.execution_compatibility().execution_allowed);
    loaded.title = "valid legacy runtime update".into();
    storage.save_runtime_state(&loaded).await.unwrap();
    loaded.add_message(Message::user("valid legacy continuation"));
    storage.save_session(&loaded).await.unwrap();
    drop(storage);
    let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
    let loaded = storage.load_session("legacy").await.unwrap().unwrap();
    assert!(loaded.session_version.is_none());
    assert!(loaded.execution_compatibility().execution_allowed);
    assert_eq!(loaded.messages[0].content, "valid legacy continuation");
    for bytes in pair(home.path(), "legacy").await {
        let raw: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(raw.get("session_version").is_none());
    }
}

#[tokio::test]
async fn session_version_store_ordinary_save_cannot_upgrade_an_existing_stamp() {
    let home = tempfile::tempdir().unwrap();
    let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
    storage.save_session(&Session::new("legacy", "model")).await.unwrap();
    change_stamp(home.path(), "legacy", None, None).await;
    let mut loaded = storage.load_session("legacy").await.unwrap().unwrap();
    let before = pair(home.path(), "legacy").await;
    loaded.session_version = Some(1);
    let error = storage.save_session(&loaded).await.expect_err("ordinary save is not migration");
    assert!(error.to_string().contains("session_version_change_requires_new_session"), "{error}");
    assert_eq!(pair(home.path(), "legacy").await, before);
}

#[tokio::test]
async fn session_version_store_compatible_main_and_side_preserve_legacy_execution() {
    for (main, runtime) in [(None, Some(1)), (Some(1), None)] {
        let home = tempfile::tempdir().unwrap();
        let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
        storage.save_session(&Session::new("compatible-pair", "model")).await.unwrap();
        change_stamp(home.path(), "compatible-pair", main, runtime).await;
        let mut loaded = storage.load_session("compatible-pair").await.unwrap().unwrap();
        assert_eq!(loaded.session_version, main);
        assert!(loaded.execution_compatibility().execution_allowed);
        loaded.title = "compatible checkpoint".into();
        storage.save_runtime_state(&loaded).await.unwrap();
        let loaded = storage.load_session("compatible-pair").await.unwrap().unwrap();
        assert_eq!(loaded.session_version, main);
        assert!(loaded.execution_compatibility().execution_allowed);
    }
}

#[tokio::test]
async fn session_version_store_rejected_checkpoint_does_not_publish_cache_snapshot() {
    use crate::session_merge::LockedSessionStore;
    use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

    for runtime_only in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let storage = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        storage.save_session(&Session::new("future-cache", "model")).await.unwrap();
        change_stamp(home.path(), "future-cache", Some(99), Some(99)).await;
        let mut loaded = storage.load_session("future-cache").await.unwrap().unwrap();
        loaded.title = "must not publish into cache".into();
        let before = pair(home.path(), "future-cache").await;
        let locked = LockedSessionStore::new(storage);
        let published = AtomicBool::new(false);
        let result = if runtime_only {
            locked.save_runtime_only_and_publish(&mut loaded, |_| {
                published.store(true, Ordering::SeqCst);
            }).await
        } else {
            locked.merge_save_runtime_and_publish(&mut loaded, |_, _| {
                published.store(true, Ordering::SeqCst);
            }).await
        };
        assert!(result.is_err());
        assert!(!published.load(Ordering::SeqCst));
        assert_eq!(pair(home.path(), "future-cache").await, before);
    }
}
