//! Stored behavior stamps survive reopen; ordinary checkpoints cannot migrate
//! or replace a conflicting execution snapshot. All data belongs to TempDir.
use super::SessionStoreV2;
use bamboo_domain::{storage::Storage, Message, Session};
use serde_json::{json, Value};
use std::path::Path;

async fn change_stamp(home: &Path, id: &str, main: Option<u32>, runtime: Option<u32>) {
    for (name, version) in [("session.json", main), ("runtime.json", runtime)] {
        let path = home.join("sessions").join(id).join(name);
        let mut raw: Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        if name == "session.json" {
            // Flat legacy Main keeps the original identity/proof fields while
            // avoiding a stale compact header after this fixture-only rewrite.
            raw.as_object_mut()
                .unwrap()
                .remove("_bamboo_main_authority");
        }
        match version {
            Some(version) => raw["session_version"] = json!(version),
            None => {
                raw.as_object_mut().unwrap().remove("session_version");
            }
        }
        let encoded = serde_json::to_vec_pretty(&raw).unwrap();
        if name == "session.json" {
            assert!(super::compact_main::validate_full_main(&encoded)
                .expect("fixture is valid flat legacy Main")
                .is_none());
        }
        tokio::fs::write(path, encoded).await.unwrap();
    }
}

async fn pair(home: &Path, id: &str) -> [Vec<u8>; 2] {
    let directory = home.join("sessions").join(id);
    [
        tokio::fs::read(directory.join("session.json"))
            .await
            .unwrap(),
        tokio::fs::read(directory.join("runtime.json"))
            .await
            .unwrap(),
    ]
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
        assert_eq!(
            loaded.messages[0].content,
            "history survives a behavior conflict"
        );
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
    assert!(
        error.to_string().contains("session_execution_incompatible"),
        "{error}"
    );
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
    storage
        .save_session(&Session::new("legacy", "model"))
        .await
        .unwrap();
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
    storage
        .save_session(&Session::new("legacy", "model"))
        .await
        .unwrap();
    change_stamp(home.path(), "legacy", None, None).await;
    let mut loaded = storage.load_session("legacy").await.unwrap().unwrap();
    let before = pair(home.path(), "legacy").await;
    loaded.session_version = Some(1);
    let error = storage
        .save_session(&loaded)
        .await
        .expect_err("ordinary save is not migration");
    assert!(
        error
            .to_string()
            .contains("session_version_change_requires_new_session"),
        "{error}"
    );
    assert_eq!(pair(home.path(), "legacy").await, before);
}

#[tokio::test]
async fn session_version_store_compatible_main_and_side_preserve_legacy_execution() {
    for (main, runtime) in [(None, Some(1)), (Some(1), None)] {
        let home = tempfile::tempdir().unwrap();
        let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
        storage
            .save_session(&Session::new("compatible-pair", "model"))
            .await
            .unwrap();
        change_stamp(home.path(), "compatible-pair", main, runtime).await;
        let mut loaded = storage
            .load_session("compatible-pair")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.session_version, main);
        assert!(loaded.execution_compatibility().execution_allowed);
        loaded.title = "compatible checkpoint".into();
        storage.save_runtime_state(&loaded).await.unwrap();
        let loaded = storage
            .load_session("compatible-pair")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.session_version, main);
        assert!(loaded.execution_compatibility().execution_allowed);
    }
}

#[tokio::test]
async fn session_version_store_rejected_checkpoint_does_not_publish_cache_snapshot() {
    use crate::session_merge::LockedSessionStore;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    for runtime_only in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let storage = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        storage
            .save_session(&Session::new("future-cache", "model"))
            .await
            .unwrap();
        change_stamp(home.path(), "future-cache", Some(99), Some(99)).await;
        let mut loaded = storage.load_session("future-cache").await.unwrap().unwrap();
        loaded.title = "must not publish into cache".into();
        let before = pair(home.path(), "future-cache").await;
        let locked = LockedSessionStore::new(storage);
        let published = AtomicBool::new(false);
        let result = if runtime_only {
            locked
                .save_runtime_only_and_publish(&mut loaded, |_| {
                    published.store(true, Ordering::SeqCst);
                })
                .await
        } else {
            locked
                .merge_save_runtime_and_publish(&mut loaded, |_, _| {
                    published.store(true, Ordering::SeqCst);
                })
                .await
        };
        assert!(result.is_err());
        assert!(!published.load(Ordering::SeqCst));
        assert_eq!(pair(home.path(), "future-cache").await, before);
    }
}

async fn rejected_context_merge(runtime_only: bool, durable_conflict: bool, stamp: Option<u32>) {
    use crate::session_merge::LockedSessionStore;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    let home = tempfile::tempdir().unwrap();
    let storage = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let mut incoming = Session::new("context-merge", "model");
    incoming.session_version = stamp;
    incoming.model_context_state = Some(bamboo_domain::ModelContextState {
        state_revision: 7,
        ..Default::default()
    });
    storage.save_session(&incoming).await.unwrap();
    if durable_conflict {
        let path = home.path().join("sessions/context-merge/runtime.json");
        let mut raw: Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        raw["model_context_state"]["schema_version"] = json!(99);
        tokio::fs::write(path, serde_json::to_vec_pretty(&raw).unwrap())
            .await
            .unwrap();
    } else {
        incoming
            .model_context_state
            .as_mut()
            .unwrap()
            .schema_version = 99;
    }
    let before_memory = serde_json::to_value(&incoming).unwrap();
    let before_disk = pair(home.path(), &incoming.id).await;
    let locked = LockedSessionStore::new(storage);
    let published = AtomicBool::new(false);
    let result = if runtime_only {
        locked
            .save_runtime_only_and_publish(&mut incoming, |_| {
                published.store(true, Ordering::SeqCst);
            })
            .await
    } else {
        locked
            .merge_save_runtime_and_publish(&mut incoming, |_, _| {
                published.store(true, Ordering::SeqCst);
            })
            .await
    };
    let error =
        result.expect_err("merge must reject incompatible input before adopting any fields");
    assert!(
        error
            .to_string()
            .contains("unsupported_model_context_version"),
        "{error}"
    );
    assert_eq!(serde_json::to_value(&incoming).unwrap(), before_memory);
    assert_eq!(pair(home.path(), &incoming.id).await, before_disk);
    assert!(!published.load(Ordering::SeqCst));
}

#[tokio::test]
async fn session_version_merge_cannot_adopt_over_an_unknown_incoming_schema() {
    for runtime_only in [false, true] {
        for stamp in [None, Some(1)] {
            rejected_context_merge(runtime_only, false, stamp).await;
        }
    }
}

#[tokio::test]
async fn session_version_merge_rejects_unknown_durable_schema_before_mutating_the_caller() {
    for runtime_only in [false, true] {
        for stamp in [None, Some(1)] {
            rejected_context_merge(runtime_only, true, stamp).await;
        }
    }
}

#[tokio::test]
async fn session_version_store_stale_checkpoint_cannot_replace_an_unknown_main() {
    use crate::session_merge::LockedSessionStore;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    for runtime_only in [false, true] {
        for through_wrapper in [false, true] {
            let home = tempfile::tempdir().unwrap();
            let storage = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
            let mut stale = Session::new("stale-main-stamp", "model");
            stale.model_context_state = Some(bamboo_domain::ModelContextState {
                state_revision: 7,
                ..Default::default()
            });
            storage.save_session(&stale).await.unwrap();
            change_stamp(home.path(), &stale.id, Some(99), Some(1)).await;
            let before = pair(home.path(), &stale.id).await;
            stale.title = "must not replace future Main or sidecar".into();
            stale.model_context_state = None;
            let before_memory = serde_json::to_value(&stale).unwrap();
            let published = AtomicBool::new(false);
            let result = if through_wrapper {
                let locked = LockedSessionStore::new(storage.clone());
                if runtime_only {
                    locked
                        .save_runtime_only_and_publish(&mut stale, |_| {
                            published.store(true, Ordering::SeqCst);
                        })
                        .await
                } else {
                    locked
                        .merge_save_runtime_and_publish(&mut stale, |_, _| {
                            published.store(true, Ordering::SeqCst);
                        })
                        .await
                }
            } else if runtime_only {
                storage.save_runtime_state(&stale).await
            } else {
                storage.save_session(&stale).await
            };
            let error = result
                .expect_err("a compatible caller is not authority over a future canonical Main");
            assert!(
                error.to_string().contains("session_execution_incompatible"),
                "{error}"
            );
            assert_eq!(serde_json::to_value(&stale).unwrap(), before_memory);
            assert_eq!(pair(home.path(), &stale.id).await, before);
            assert!(!published.load(Ordering::SeqCst));
        }
    }
}

struct CompatibleIoFailureStorage {
    retained: Session,
    saves: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl Storage for CompatibleIoFailureStorage {
    async fn save_session(&self, _: &Session) -> std::io::Result<()> {
        self.saves.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(std::io::Error::other("ordinary compatible save IO failure"))
    }

    async fn load_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        assert_eq!(id, self.retained.id);
        Ok(Some(self.retained.clone()))
    }

    async fn delete_session(&self, _: &str) -> std::io::Result<bool> {
        panic!("compatible save fixture must not delete a Session")
    }
}

#[tokio::test]
async fn session_version_compatible_io_failure_preserves_existing_publication_contract() {
    use crate::session_merge::LockedSessionStore;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    // The compatibility fence must preserve the established generic-IO policy:
    // adopt valid durable fields into the caller, publish failure only when the
    // selected public API requests it, and preserve the original write error.
    for mode in ["runtime", "runtime_success_only", "full"] {
        let mut caller = Session::new("compatible-io-failure", "model");
        caller.add_message(Message::user("Unchanged caller history"));
        let mut retained = caller.clone();
        retained.metadata_version += 1;
        retained.title = "Newer durable metadata".into();
        retained.model_context_state = Some(bamboo_domain::ModelContextState {
            state_revision: 7,
            ..Default::default()
        });
        let storage = Arc::new(CompatibleIoFailureStorage {
            retained: retained.clone(),
            saves: AtomicUsize::new(0),
        });
        let locked = LockedSessionStore::new(storage.clone());
        let publications = AtomicUsize::new(0);
        let publish = |saved: &Session| {
            assert_eq!(saved.title, retained.title);
            assert_eq!(saved.model_context_state, retained.model_context_state);
            publications.fetch_add(1, Ordering::SeqCst);
        };
        let result = match mode {
            "runtime" => {
                locked
                    .save_runtime_only_and_publish(&mut caller, publish)
                    .await
            }
            "runtime_success_only" => {
                locked
                    .save_runtime_only_and_publish_on_success(&mut caller, publish)
                    .await
            }
            "full" => {
                locked
                    .merge_save_runtime_and_publish(&mut caller, |saved, committed| {
                        assert!(!committed);
                        publish(saved);
                    })
                    .await
            }
            _ => unreachable!(),
        };
        let error = result.expect_err("the injected IO failure must remain visible");
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert_eq!(error.to_string(), "ordinary compatible save IO failure");
        assert_eq!(storage.saves.load(Ordering::SeqCst), 1);
        assert_eq!(caller.title, retained.title);
        assert_eq!(caller.model_context_state, retained.model_context_state);
        assert_eq!(
            serde_json::to_value(&caller.messages).unwrap(),
            serde_json::to_value(&retained.messages).unwrap()
        );
        let expected_publications = if mode == "runtime_success_only" { 0 } else { 1 };
        assert_eq!(publications.load(Ordering::SeqCst), expected_publications);
    }
}


async fn missing_main_repair_retains_contract(conflict: Option<&str>) {
    let home = tempfile::tempdir().unwrap();
    let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
    let mut caller = Session::new("missing-main-contract", "model");
    caller.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState::new("retained-run"));
    caller.model_context_state = Some(bamboo_domain::ModelContextState::default());
    caller.add_message(Message::user("retained history"));
    storage.save_session(&caller).await.unwrap();
    let directory = home.path().join("sessions").join(&caller.id);
    let runtime_path = directory.join("runtime.json");
    if let Some(conflict) = conflict {
        let mut raw: Value =
            serde_json::from_slice(&tokio::fs::read(&runtime_path).await.unwrap()).unwrap();
        match conflict {
            "session" => raw["session_version"] = json!(99),
            "runtime" => raw["agent_runtime_state"]["version"] = json!(99),
            "context" => raw["model_context_state"]["schema_version"] = json!(99),
            _ => unreachable!(),
        }
        tokio::fs::write(&runtime_path, serde_json::to_vec_pretty(&raw).unwrap())
            .await
            .unwrap();
        // The fixture remains a legal same-birth Root with a readable future
        // contract, not a failure caused by broken actor/authority evidence.
        let retained = storage.load_session(&caller.id).await.unwrap().unwrap();
        assert!(!retained.execution_compatibility().execution_allowed);
    }
    let runtime_before = tokio::fs::read(&runtime_path).await.unwrap();
    tokio::fs::remove_file(directory.join("session.json"))
        .await
        .unwrap();
    assert!(caller.execution_compatibility().execution_allowed);
    let result = storage.save_session(&caller).await;
    if conflict.is_some() {
        let error = result.expect_err("full repair cannot reset an unknown retained contract");
        assert!(
            error.to_string().contains("session_execution_incompatible"),
            "{error}"
        );
        assert!(!directory.join("session.json").exists());
        assert_eq!(tokio::fs::read(runtime_path).await.unwrap(), runtime_before);
    } else {
        result.expect("a compatible interrupted Main creation remains repairable");
        let restored = storage.load_session(&caller.id).await.unwrap().unwrap();
        assert!(restored.execution_compatibility().execution_allowed);
        assert_eq!(restored.messages, caller.messages);
        assert_eq!(restored.created_at, caller.created_at);
    }
}

#[tokio::test]
async fn session_version_missing_main_preserves_unknown_side_stamp() {
    missing_main_repair_retains_contract(Some("session")).await;
}

#[tokio::test]
async fn session_version_missing_main_preserves_unknown_runtime_schema() {
    missing_main_repair_retains_contract(Some("runtime")).await;
}

#[tokio::test]
async fn session_version_missing_main_preserves_unknown_context_schema() {
    missing_main_repair_retains_contract(Some("context")).await;
}

#[tokio::test]
async fn session_version_missing_main_compatible_repair_remains_usable() {
    missing_main_repair_retains_contract(None).await;
}
