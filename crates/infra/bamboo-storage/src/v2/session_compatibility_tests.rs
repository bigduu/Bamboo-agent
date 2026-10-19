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
    let proof_path = directory.join(super::root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE);
    let proof_before = tokio::fs::read(&proof_path).await.unwrap();
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
        assert_eq!(tokio::fs::read(proof_path).await.unwrap(), proof_before);
    } else {
        result.expect("a compatible interrupted Main creation remains repairable");
        let restored = storage.load_session(&caller.id).await.unwrap().unwrap();
        assert!(restored.execution_compatibility().execution_allowed);
        assert_eq!(
            serde_json::to_value(&restored.messages).unwrap(),
            serde_json::to_value(&caller.messages).unwrap()
        );
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

// Task's dedicated CAS and undo paths must obey the same retained contract as
// ordinary checkpoints. These fixtures only alter API-created TempDir data.
const TASK_CONTRACT_CONFLICTS: [&str; 4] = ["session", "runtime", "context", "main"];

async fn task_member_directory(storage: &SessionStoreV2, id: &str) -> std::path::PathBuf {
    storage.abs_path_from_rel(&storage.resolve_rel_path(id).await.unwrap())
}

async fn task_contract_bytes(
    home: &Path,
) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    let mut bytes = std::collections::BTreeMap::new();
    for directory in ["sessions/tx-root", "sessions/tx-root/children/tx-child"] {
        for name in [
            "session.json",
            "runtime.json",
            super::root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE,
        ] {
            let path = home.join(directory).join(name);
            match tokio::fs::read(&path).await {
                Ok(raw) => {
                    bytes.insert(path, raw);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!("read Task fixture: {error}"),
            }
        }
    }
    let mut journals =
        match tokio::fs::read_dir(home.join(super::RUNTIME_TASK_TRANSACTION_DIR)).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return bytes,
            Err(error) => panic!("read Task journals: {error}"),
        };
    while let Some(entry) = journals.next_entry().await.unwrap() {
        if entry.file_type().await.unwrap().is_file() {
            bytes.insert(entry.path(), tokio::fs::read(entry.path()).await.unwrap());
        }
    }
    bytes
}

async fn task_set_stamp(storage: &SessionStoreV2, id: &str, main: bool, stamp: Option<u32>) {
    let path = task_member_directory(storage, id).await.join(if main {
        "session.json"
    } else {
        "runtime.json"
    });
    let mut raw: Value = serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
    if main {
        raw.as_object_mut()
            .unwrap()
            .remove("_bamboo_main_authority");
    }
    match stamp {
        Some(stamp) => raw["session_version"] = json!(stamp),
        None => {
            raw.as_object_mut().unwrap().remove("session_version");
        }
    }
    let encoded = serde_json::to_vec_pretty(&raw).unwrap();
    if main {
        assert!(super::compact_main::validate_full_main(&encoded)
            .unwrap()
            .is_none());
    }
    tokio::fs::write(path, encoded).await.unwrap();
}

fn task_future_caller(session: &mut Session, conflict: &str) {
    match conflict {
        "session" => session.session_version = Some(99),
        "runtime" => {
            let mut state = bamboo_domain::AgentRuntimeState::new("retained-task-run");
            state.version = 99;
            session.agent_runtime_state = Some(state);
        }
        "context" => {
            session.model_context_state = Some(bamboo_domain::ModelContextState {
                schema_version: 99,
                ..Default::default()
            })
        }
        _ => unreachable!(),
    }
    assert!(!session.execution_compatibility().execution_allowed);
}

async fn task_future_contract(storage: &SessionStoreV2, id: &str, conflict: &str) {
    if conflict == "main" || conflict == "session" {
        task_set_stamp(storage, id, conflict == "main", Some(99)).await;
    } else {
        let path = task_member_directory(storage, id)
            .await
            .join("runtime.json");
        let mut session: Session =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        task_future_caller(&mut session, conflict);
        tokio::fs::write(path, serde_json::to_vec_pretty(&session).unwrap())
            .await
            .unwrap();
    }
    // Check the edited document is parseable future state, preserving the
    // existing identity/proof; pending undo intentionally blocks public reads.
    let path = task_member_directory(storage, id)
        .await
        .join(if conflict == "main" {
            "session.json"
        } else {
            "runtime.json"
        });
    let parsed: Session = serde_json::from_slice(&tokio::fs::read(path).await.unwrap()).unwrap();
    assert!(!parsed.execution_compatibility().execution_allowed);
}

fn record_task_refusal(
    label: &str,
    result: std::io::Result<()>,
    unchanged: bool,
    unpublished: bool,
    failures: &mut Vec<String>,
) {
    let refused = result
        .as_ref()
        .is_err_and(|error| error.to_string().contains("session_execution_incompatible"));
    eprintln!("Task contract {label}: compatibility_refused={refused} bytes_retained={unchanged} no_publication={unpublished}");
    if !refused || !unchanged || !unpublished {
        failures.push(format!(
            "{label}: result={result:?}, retained={unchanged}, unpublished={unpublished}"
        ));
    }
}

#[tokio::test]
async fn session_version_task_single_cas_preflights_retained_main_and_side() {
    let mut failures = Vec::new();
    for id in ["tx-child", "tx-root"] {
        for conflict in TASK_CONTRACT_CONFLICTS {
            let home = tempfile::tempdir().unwrap();
            let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
            let (child, child_updated, root, root_updated) =
                super::tests::seed_runtime_task_transaction_pair(&storage)
                    .await
                    .unwrap();
            task_future_contract(&storage, id, conflict).await;
            let readable = storage.load_session(id).await.unwrap().unwrap();
            assert!(!readable.execution_compatibility().execution_allowed);
            assert_eq!(readable.messages.len(), 1);
            let before = task_contract_bytes(home.path()).await;
            storage.take_runtime_task_durability_events();
            let (original, updated) = if id == "tx-child" {
                (&child, &child_updated)
            } else {
                (&root, &root_updated)
            };
            let result = storage
                .save_task_control_plane_if_matches(original, updated)
                .await
                .map(|_| ());
            record_task_refusal(
                &format!("single/{id}/{conflict}"),
                result,
                before == task_contract_bytes(home.path()).await,
                storage.take_runtime_task_durability_events().is_empty(),
                &mut failures,
            );
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn session_version_task_pair_preflights_both_members_before_journal() {
    let mut failures = Vec::new();
    for id in ["tx-child", "tx-root"] {
        for conflict in TASK_CONTRACT_CONFLICTS {
            let home = tempfile::tempdir().unwrap();
            let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
            let (child, child_updated, root, root_updated) =
                super::tests::seed_runtime_task_transaction_pair(&storage)
                    .await
                    .unwrap();
            task_future_contract(&storage, id, conflict).await;
            let before = task_contract_bytes(home.path()).await;
            storage.take_runtime_task_durability_events();
            let result = storage
                .save_task_control_planes_atomically(&child, &child_updated, &root, &root_updated)
                .await
                .map(|_| ());
            record_task_refusal(
                &format!("pair/{id}/{conflict}"),
                result,
                before == task_contract_bytes(home.path()).await,
                storage.take_runtime_task_durability_events().is_empty(),
                &mut failures,
            );
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn session_version_task_cas_refuses_future_incoming_snapshots() {
    let mut failures = Vec::new();
    for paired in [false, true] {
        for conflict in ["session", "runtime", "context"] {
            let home = tempfile::tempdir().unwrap();
            let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
            let (child, child_updated, mut root, mut root_updated) =
                super::tests::seed_runtime_task_transaction_pair(&storage)
                    .await
                    .unwrap();
            task_future_caller(&mut root, conflict);
            task_future_caller(&mut root_updated, conflict);
            let before = task_contract_bytes(home.path()).await;
            storage.take_runtime_task_durability_events();
            let result = if paired {
                storage
                    .save_task_control_planes_atomically(
                        &child,
                        &child_updated,
                        &root,
                        &root_updated,
                    )
                    .await
            } else {
                storage
                    .save_task_control_plane_if_matches(&root, &root_updated)
                    .await
            };
            record_task_refusal(
                &format!("incoming/paired={paired}/{conflict}"),
                result.map(|_| ()),
                before == task_contract_bytes(home.path()).await,
                storage.take_runtime_task_durability_events().is_empty(),
                &mut failures,
            );
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

async fn seed_task_undo(
    storage: &SessionStoreV2,
    marker: super::RuntimeTaskJournalMarkerState,
) -> std::path::PathBuf {
    use super::{
        RuntimeTaskDurabilityEvent as Event, RuntimeTaskTransactionJournal, TaskControlPlaneUndo,
    };
    let (child, child_updated, root, root_updated) =
        super::tests::seed_runtime_task_transaction_pair(storage)
            .await
            .unwrap();
    let journal = RuntimeTaskTransactionJournal {
        version: super::RUNTIME_TASK_TRANSACTION_VERSION,
        transaction_id: uuid::Uuid::new_v4().to_string(),
        first: TaskControlPlaneUndo {
            session_id: child.id,
            task_list: child.task_list,
            task_list_version: "1".into(),
        },
        second: TaskControlPlaneUndo {
            session_id: root.id,
            task_list: root.task_list,
            task_list_version: "1".into(),
        },
    };
    let guard = storage
        .lock_runtime_task_transaction_exclusive()
        .await
        .unwrap();
    let prepared = storage.write_runtime_task_journal(&journal).await.unwrap();
    storage
        .write_existing_runtime_sidecar_durable_unchecked(
            &child_updated,
            Event::FirstUpdatedSidecarPublished,
            &guard,
        )
        .await
        .unwrap();
    storage
        .write_existing_runtime_sidecar_durable_unchecked(
            &root_updated,
            Event::SecondUpdatedSidecarPublished,
            &guard,
        )
        .await
        .unwrap();
    let path = match marker {
        super::RuntimeTaskJournalMarkerState::Prepared => prepared,
        super::RuntimeTaskJournalMarkerState::Committing
        | super::RuntimeTaskJournalMarkerState::Committed => {
            let extension = if marker == super::RuntimeTaskJournalMarkerState::Committing {
                "committing"
            } else {
                "committed"
            };
            let path = prepared.with_extension(extension);
            super::atomic_rename(&prepared, &path).await.unwrap();
            super::sync_parent_directory_entry(&path).await.unwrap();
            path
        }
    };
    drop(guard);
    storage.take_runtime_task_durability_events();
    path
}

#[tokio::test]
async fn session_version_task_recovery_preflights_all_members_before_undo() {
    use super::RuntimeTaskJournalMarkerState::{Committing, Prepared};
    let mut failures = Vec::new();
    for marker in [Prepared, Committing] {
        for id in ["tx-child", "tx-root"] {
            for conflict in TASK_CONTRACT_CONFLICTS {
                let home = tempfile::tempdir().unwrap();
                let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
                seed_task_undo(&storage, marker).await;
                task_future_contract(&storage, id, conflict).await;
                let before = task_contract_bytes(home.path()).await;
                let result = storage
                    .recover_task_control_plane_transaction("tx-child", "tx-root")
                    .await;
                record_task_refusal(
                    &format!("recover/{marker:?}/{id}/{conflict}"),
                    result,
                    before == task_contract_bytes(home.path()).await,
                    storage.take_runtime_task_durability_events().is_empty(),
                    &mut failures,
                );
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn session_version_task_orphan_recovery_retains_both_members_on_conflict() {
    use super::RuntimeTaskJournalMarkerState::{Committing, Prepared};
    let mut failures = Vec::new();
    for marker in [Prepared, Committing] {
        for id in ["tx-child", "tx-root"] {
            for conflict in TASK_CONTRACT_CONFLICTS {
                let home = tempfile::tempdir().unwrap();
                let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
                seed_task_undo(&storage, marker).await;
                task_future_contract(&storage, id, conflict).await;
                let before = task_contract_bytes(home.path()).await;
                drop(storage);
                let result = SessionStoreV2::new(home.path().into()).await.map(|_| ());
                record_task_refusal(
                    &format!("reopen/{marker:?}/{id}/{conflict}"),
                    result,
                    before == task_contract_bytes(home.path()).await,
                    true,
                    &mut failures,
                );
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn session_version_task_legacy_stamps_keep_cas_and_undo_usable() {
    use super::RuntimeTaskJournalMarkerState::{Committing, Prepared};
    for missing_main in [false, true] {
        for operation in ["single", "pair", "prepared", "committing"] {
            let home = tempfile::tempdir().unwrap();
            let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
            let mut staged = None;
            if operation == "single" || operation == "pair" {
                staged = Some(
                    super::tests::seed_runtime_task_transaction_pair(&storage)
                        .await
                        .unwrap(),
                );
            } else {
                seed_task_undo(
                    &storage,
                    if operation == "prepared" {
                        Prepared
                    } else {
                        Committing
                    },
                )
                .await;
            }
            for id in ["tx-child", "tx-root"] {
                task_set_stamp(&storage, id, missing_main, None).await;
            }
            let before = task_contract_bytes(home.path()).await;
            if let Some((child, child_updated, root, root_updated)) = staged {
                let applied = if operation == "single" {
                    storage
                        .save_task_control_plane_if_matches(&child, &child_updated)
                        .await
                } else {
                    storage
                        .save_task_control_planes_atomically(
                            &child,
                            &child_updated,
                            &root,
                            &root_updated,
                        )
                        .await
                }
                .unwrap();
                assert!(applied);
            } else {
                storage
                    .recover_task_control_plane_transaction("tx-child", "tx-root")
                    .await
                    .unwrap();
            }
            let after = task_contract_bytes(home.path()).await;
            for (path, raw) in before.iter().filter(|(path, _)| {
                path.file_name().is_some_and(|name| {
                    name == "session.json"
                        || name == super::root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE
                })
            }) {
                assert_eq!(after.get(path), Some(raw));
            }
            assert!(storage
                .runtime_task_journal_paths()
                .await
                .unwrap()
                .is_empty());
            for id in ["tx-child", "tx-root"] {
                let loaded = storage.load_session(id).await.unwrap().unwrap();
                assert!(loaded.execution_compatibility().execution_allowed);
                assert_eq!(
                    loaded.session_version,
                    if missing_main { None } else { Some(1) }
                );
                assert_eq!(loaded.messages.len(), 1);
                let side: Value = serde_json::from_slice(
                    &tokio::fs::read(
                        task_member_directory(&storage, id)
                            .await
                            .join("runtime.json"),
                    )
                    .await
                    .unwrap(),
                )
                .unwrap();
                // Narrow CAS preserves the side encoding. Existing undo uses
                // the canonical Main stamp; both None and v1 remain compatible.
                let side_stamp = if operation == "single" || operation == "pair" {
                    if missing_main {
                        Some(1)
                    } else {
                        None
                    }
                } else if missing_main {
                    None
                } else {
                    Some(1)
                };
                assert_eq!(
                    side.get("session_version").and_then(Value::as_u64),
                    side_stamp
                );
                let generation =
                    if operation == "pair" || (operation == "single" && id == "tx-child") {
                        "2"
                    } else {
                        "1"
                    };
                assert_eq!(loaded.task_list_version_meta().as_deref(), Some(generation));
            }
            eprintln!("Task compatible missing_main={missing_main}/{operation}: passed");
        }
    }
}

#[tokio::test]
async fn session_version_task_committed_marker_only_cleans_up_future_history() {
    use super::{RuntimeTaskDurabilityEvent as Event, RuntimeTaskJournalMarkerState::Committed};
    for id in ["tx-child", "tx-root"] {
        for conflict in TASK_CONTRACT_CONFLICTS {
            let home = tempfile::tempdir().unwrap();
            let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
            let journal = seed_task_undo(&storage, Committed).await;
            task_future_contract(&storage, id, conflict).await;
            let mut expected = task_contract_bytes(home.path()).await;
            expected.remove(&journal);
            drop(storage);
            let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
            assert_eq!(
                reopened.take_runtime_task_durability_events(),
                vec![Event::JournalDeactivated]
            );
            assert_eq!(task_contract_bytes(home.path()).await, expected);
            let loaded = reopened.load_session(id).await.unwrap().unwrap();
            assert!(!loaded.execution_compatibility().execution_allowed);
            assert_eq!(loaded.task_list_version_meta().as_deref(), Some("2"));
            assert_eq!(loaded.messages.len(), 1);
        }
    }
}

#[tokio::test]
async fn session_version_task_rejection_does_not_publish_locked_store_callback() {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    let home = tempfile::tempdir().unwrap();
    let storage = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let (_, _, _, updated) = super::tests::seed_runtime_task_transaction_pair(&storage)
        .await
        .unwrap();
    task_future_contract(&storage, "tx-root", "main").await;
    let before = task_contract_bytes(home.path()).await;
    storage.take_runtime_task_durability_events();
    let locked = crate::session_merge::LockedSessionStore::new(storage.clone());
    let published = AtomicBool::new(false);
    let result = locked
        .update_task_list_control_plane_and_publish(
            "tx-root",
            updated.task_list.as_ref().unwrap(),
            "2",
            |_| {
                published.store(true, Ordering::SeqCst);
            },
        )
        .await;
    let mut failures = Vec::new();
    record_task_refusal(
        "locked-store/Main99",
        result.map(|_| ()),
        before == task_contract_bytes(home.path()).await,
        !published.load(Ordering::SeqCst)
            && storage.take_runtime_task_durability_events().is_empty(),
        &mut failures,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// Exercise maintenance/Host writers through their existing public APIs. Raw
// extension fields model a parseable future producer; identity/proofs are kept.
pub(super) fn writer_future_fields(raw: &mut Value, conflict: &str) {
    match conflict {
        "main" | "session" => raw["session_version"] = json!(99),
        "runtime" => {
            let mut state = bamboo_domain::AgentRuntimeState::new("future-writer-run");
            state.version = 99;
            raw["agent_runtime_state"] = serde_json::to_value(state).unwrap();
        }
        "context" => {
            raw["model_context_state"] = serde_json::to_value(bamboo_domain::ModelContextState {
                schema_version: 99,
                ..Default::default()
            })
            .unwrap();
        }
        _ => unreachable!(),
    }
}

pub(super) async fn writer_future_contract(directory: &Path, conflict: &str) {
    for name in ["session.json", "runtime.json"] {
        let path = directory.join(name);
        let mut raw: Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        raw["future_writer_extension"] = json!({"retained": true, "file": name});
        if name == "session.json" {
            raw.as_object_mut()
                .unwrap()
                .remove("_bamboo_main_authority");
        }
        if (conflict == "main") == (name == "session.json") {
            writer_future_fields(&mut raw, conflict);
        }
        let bytes = serde_json::to_vec_pretty(&raw).unwrap();
        if name == "session.json" {
            assert!(super::compact_main::validate_full_main(&bytes)
                .unwrap()
                .is_none());
        }
        tokio::fs::write(path, bytes).await.unwrap();
    }
}

pub(super) fn writer_contract_bytes(
    storage: &SessionStoreV2,
    directory: &Path,
) -> Vec<Option<Vec<u8>>> {
    let mut snapshot = [
        "session.json",
        "runtime.json",
        super::root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE,
        super::supervisor_proof::SUPERVISOR_PROOF_FILE,
        "actor-authority.json",
        "actor-authority.initialized.json",
        super::SEARCH_INDEX_REVISION_FILE,
        ".actor-tree-revision.json",
        "attachments/sentinel",
    ]
    .map(|name| std::fs::read(directory.join(name)).ok())
    .to_vec();
    snapshot.push(std::fs::read(storage.index_path()).ok());
    snapshot
}

#[tokio::test]
async fn session_version_writer_clear_rejects_future_root_and_child_without_rewriting() {
    let mut failures = Vec::new();
    for id in ["tx-root", "tx-child"] {
        for conflict in TASK_CONTRACT_CONFLICTS {
            let home = tempfile::tempdir().unwrap();
            let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
            super::tests::seed_runtime_task_transaction_pair(&storage)
                .await
                .unwrap();
            let directory = task_member_directory(&storage, id).await;
            tokio::fs::write(
                directory.join("attachments/sentinel"),
                b"retained attachment",
            )
            .await
            .unwrap();
            writer_future_contract(&directory, conflict).await;
            let loaded = storage.load_session(id).await.unwrap().unwrap();
            assert!(!loaded.execution_compatibility().execution_allowed);
            assert_eq!(loaded.messages.len(), 1);
            let before = writer_contract_bytes(&storage, &directory);
            let result = storage.clear_session(id).await;
            let refused = result.as_ref().is_err_and(|error| {
                error.kind() == std::io::ErrorKind::Unsupported
                    && error.to_string().contains("session_execution_incompatible")
            });
            let retained = writer_contract_bytes(&storage, &directory) == before;
            let readable = storage
                .load_session(id)
                .await
                .unwrap()
                .unwrap()
                .messages
                .len()
                == 1;
            eprintln!("clear contract {id}/{conflict}: refused={refused} retained={retained} readable={readable}");
            if !refused || !retained || !readable {
                failures.push(format!(
                    "{id}/{conflict}: {result:?}, retained={retained}, readable={readable}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn session_version_writer_clear_compatible_none_v1_preserves_main_stamp() {
    for id in ["tx-root", "tx-child"] {
        for (main, side) in [(None, Some(1)), (Some(1), None)] {
            let home = tempfile::tempdir().unwrap();
            let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
            super::tests::seed_runtime_task_transaction_pair(&storage)
                .await
                .unwrap();
            task_set_stamp(&storage, id, true, main).await;
            task_set_stamp(&storage, id, false, side).await;
            let directory = task_member_directory(&storage, id).await;
            let proof = tokio::fs::read(
                directory.join(super::root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE),
            )
            .await
            .ok();
            tokio::fs::write(
                directory.join("attachments/sentinel"),
                b"clear this attachment",
            )
            .await
            .unwrap();
            assert!(storage.clear_session(id).await.unwrap());
            let loaded = storage.load_session(id).await.unwrap().unwrap();
            assert_eq!(loaded.session_version, main);
            assert!(loaded.execution_compatibility().execution_allowed);
            assert!(loaded.messages.is_empty());
            assert!(!directory.join("attachments/sentinel").exists());
            assert_eq!(
                tokio::fs::read(
                    directory.join(super::root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE)
                )
                .await
                .ok(),
                proof
            );
            let raw: Value = serde_json::from_slice(
                &tokio::fs::read(directory.join("session.json"))
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(
                raw.get("session_version").and_then(Value::as_u64),
                main.map(u64::from)
            );
        }
    }
}

#[tokio::test]
async fn session_version_copied_root_selects_current_contract_without_changing_source() {
    let mut failures = Vec::new();
    for id in ["tx-root", "tx-child"] {
        for contract in [
            "current", "v1", "legacy", "main", "session", "runtime", "context",
        ] {
            let home = tempfile::tempdir().unwrap();
            let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
            super::tests::seed_runtime_task_transaction_pair(&storage)
                .await
                .unwrap();
            let directory = task_member_directory(&storage, id).await;
            tokio::fs::write(
                directory.join("attachments/sentinel"),
                b"source copy attachment",
            )
            .await
            .unwrap();
            if contract == "legacy" || contract == "v1" {
                let stamp = (contract == "v1").then_some(1);
                task_set_stamp(&storage, id, true, stamp).await;
                task_set_stamp(&storage, id, false, stamp).await;
            } else if contract != "current" {
                writer_future_contract(&directory, contract).await;
            }
            let source = storage.load_session(id).await.unwrap().unwrap();
            let mut before = writer_contract_bytes(&storage, &directory);
            // The new independent Root legitimately adds its own index entry.
            // Only the source lifetime/files must remain byte-for-byte intact.
            let _ = before.pop();
            let copied = storage
                .copy_session(id, "new-contract-copy")
                .await
                .unwrap()
                .unwrap();
            let fresh = SessionStoreV2::new(home.path().into()).await.unwrap();
            let reopened = fresh.load_session(&copied.id).await.unwrap().unwrap();
            let mut after = writer_contract_bytes(&storage, &directory);
            let _ = after.pop();
            let current = copied.session_version == Some(bamboo_domain::CURRENT_SESSION_VERSION)
                && copied.observed_runtime_session_version.is_none()
                && copied.execution_compatibility().execution_allowed
                && reopened.session_version == Some(bamboo_domain::CURRENT_SESSION_VERSION)
                && reopened.execution_compatibility().execution_allowed;
            let preserved = before == after;
            let independent = copied.id != source.id
                && copied.created_at != source.created_at
                && copied.kind == bamboo_domain::SessionKind::Root
                && copied.root_session_id == copied.id
                && copied.parent_session_id.is_none()
                && copied.parent_created_at.is_none()
                && copied.spawn_depth == 0
                && copied.authority_identity.is_ordinary()
                && copied.supervisor_management.is_none();
            assert_eq!(
                serde_json::to_value(&reopened.messages).unwrap(),
                serde_json::to_value(&source.messages).unwrap()
            );
            assert_eq!(
                tokio::fs::read(
                    storage
                        .sessions_root_dir()
                        .join(&copied.id)
                        .join("attachments/sentinel")
                )
                .await
                .unwrap(),
                b"source copy attachment"
            );
            for name in ["session.json", "runtime.json"] {
                let bytes =
                    tokio::fs::read(storage.sessions_root_dir().join(&copied.id).join(name))
                        .await
                        .unwrap();
                let raw: Value = serde_json::from_slice(&bytes).unwrap();
                if raw["session_version"] != json!(bamboo_domain::CURRENT_SESSION_VERSION) {
                    failures.push(format!(
                        "{id}/{contract}/{name}: new Root was not stamped with the current contract"
                    ));
                }
            }
            eprintln!("copy contract {id}/{contract}: current={current} source_retained={preserved} independent={independent}");
            if !current || !preserved || !independent {
                failures.push(format!("{id}/{contract}: returned={:?}, observed={:?}, reopened={:?}, source_retained={preserved}, independent={independent}", copied.session_version, copied.observed_runtime_session_version, reopened.session_version));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\\n"));
}

#[tokio::test]
async fn versioned_spawn_known_contract_mismatch_survives_reopen_and_refuses_both_checkpoints() {
    for (main, runtime) in [
        (Some(1), Some(2)),
        (Some(2), Some(1)),
        (None, Some(2)),
        (Some(2), None),
    ] {
        let home = tempfile::tempdir().unwrap();
        let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
        let mut session = Session::new("known-version-pair", "model");
        session.add_message(Message::user(
            "retain goal and history for explicit continuation",
        ));
        session.metadata.insert(
            "retained_pending_operation".into(),
            "never consume on a contract conflict".into(),
        );
        storage.save_session(&session).await.unwrap();
        change_stamp(home.path(), &session.id, main, runtime).await;
        let before = pair(home.path(), &session.id).await;
        drop(storage);
        let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
        let loaded = reopened.load_session(&session.id).await.unwrap().unwrap();
        let report = loaded.execution_compatibility();
        assert!(report.history_readable && !report.execution_allowed);
        assert!(report
            .conflicts
            .iter()
            .any(|c| c.code == "session_version_mismatch"));
        assert!(report
            .conflicts
            .iter()
            .all(|c| c.code != "unsupported_session_version"));
        assert_eq!(loaded.messages[0].content, session.messages[0].content);
        assert!(reopened.save_session(&loaded).await.is_err());
        assert!(reopened.save_runtime_state(&loaded).await.is_err());
        assert_eq!(pair(home.path(), &session.id).await, before);
    }
}

#[tokio::test]
async fn versioned_spawn_runtime_checkpoint_cannot_reinterpret_a_known_contract() {
    for (retained, incoming) in [
        (Some(2), None),
        (Some(2), Some(1)),
        (Some(1), Some(2)),
        (None, Some(2)),
    ] {
        let home = tempfile::tempdir().unwrap();
        let storage = SessionStoreV2::new(home.path().into()).await.unwrap();
        let mut session = Session::new("immutable-contract", "model");
        session.session_version = retained;
        session.add_message(Message::user("Keep the original instruction and contract"));
        storage.save_session(&session).await.unwrap();
        let before = pair(home.path(), &session.id).await;
        let mut stale: Session =
            serde_json::from_value(serde_json::to_value(&session).unwrap()).unwrap();
        stale.session_version = incoming;
        stale
            .metadata
            .insert("not_published".into(), "rejected runtime contract".into());
        let error = storage
            .save_runtime_state(&stale)
            .await
            .expect_err("a runtime checkpoint must not change the retained tool contract");
        assert!(
            error
                .to_string()
                .contains("session_version_change_requires_new_session"),
            "{error}"
        );
        assert_eq!(pair(home.path(), &session.id).await, before);
        let fresh = SessionStoreV2::new(home.path().into()).await.unwrap();
        let loaded = fresh.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(loaded.session_version, retained);
        assert!(loaded.execution_compatibility().execution_allowed);
    }
}
