//! Codec and actual Main publishers/readers; no prefix-only permission grant.
use super::*;
use bamboo_domain::{
    Message, SupervisorManagedLink, SupervisorManagementMutation, SupervisorManagementRequest,
    SupervisorManagementState, SupervisorReference, MAX_SUPERVISOR_LINKS, MAX_SUPERVISOR_PROJECTS,
};
use serde_json::{json, value::RawValue, Value};
use std::collections::{BTreeMap, BTreeSet};

fn flat(session: &Session) -> Vec<u8> {
    serde_json::to_vec_pretty(session).unwrap()
}
fn encoded(session: &Session) -> Vec<u8> {
    compact_main::serialize_main(session).unwrap()
}
fn section(raw: &[u8]) -> &[u8] {
    let text = std::str::from_utf8(raw).unwrap();
    let fields: BTreeMap<String, &RawValue> = serde_json::from_str(text).unwrap();
    let envelope = fields[compact_main::MEMBER];
    let end = envelope.get().as_ptr() as usize - text.as_ptr() as usize + envelope.get().len() + 1;
    assert!(raw[..end].ends_with(b"},"));
    &raw[..end]
}
fn payload(raw: &[u8]) -> Value {
    serde_json::from_slice::<Value>(raw).unwrap()[compact_main::MEMBER]["payload"].clone()
}
fn framed(payload: &str, body: &[u8]) -> Vec<u8> {
    let mut result = format!(
        "{{\"_bamboo_main_authority\":{{\"version\":1,\"payload_bytes\":\"{:010}\",\"payload\":{payload}}},",
        payload.len()
    ).into_bytes();
    result.extend_from_slice(&body[1..]);
    result
}
fn with_payload(value: &Value, session: &Session) -> Vec<u8> {
    framed(&serde_json::to_string(value).unwrap(), &flat(session))
}
fn corrupt_flat(raw: &[u8], key: &str, value: Value) -> Vec<u8> {
    let mut body: Value = serde_json::from_slice(raw).unwrap();
    body.as_object_mut().unwrap().remove(compact_main::MEMBER);
    body[key] = value;
    let mut result = section(raw).to_vec();
    result.extend_from_slice(&serde_json::to_vec(&body).unwrap()[1..]);
    result
}
fn files(directory: &Path) -> Vec<Option<Vec<u8>>> {
    [
        "session.json",
        RUNTIME_SIDECAR_FILE,
        root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE,
        supervisor_proof::SUPERVISOR_PROOF_FILE,
        "attachments/keep.bin",
    ]
    .map(|name| std::fs::read(directory.join(name)).ok())
    .into()
}
fn assert_pair(directory: &Path, expected: &Session) {
    let main = std::fs::read(directory.join("session.json")).unwrap();
    assert!(compact_main::validate_full_main(&main).unwrap().is_some());
    assert_eq!(payload(&main), payload(&encoded(expected)));
    let side: Value =
        serde_json::from_slice(&std::fs::read(directory.join(RUNTIME_SIDECAR_FILE)).unwrap())
            .unwrap();
    assert!(side.get(compact_main::MEMBER).is_none());
    assert_eq!(side["id"], expected.id);
    let side: Session = serde_json::from_value(side).unwrap();
    assert_eq!(side.created_at, expected.created_at);
    assert_eq!(side.authority_identity, expected.authority_identity);
}

#[test]
fn compact_exact_whitelist_defaults_project_precedence_and_private_history() {
    let mut session = Session::new("raw identity 🪷", "private-model");
    session.root_session_id.clear(); // Preserve a valid legacy spelling before normalization.
    session.parent_session_id = Some(String::new());
    session.metadata_version = u64::MAX;
    session.title = format!("\n\u{202e}{}\u{2068}", "🪷".repeat(180));
    session
        .metadata
        .insert("project_id".into(), "shadowed-project".into());
    session
        .runtime_metadata
        .get_or_insert_with(Default::default)
        .project_id = Some("typed-project".into());
    session.add_message(Message::user("private transcript".repeat(100_000)));
    session
        .metadata
        .insert("private.prompt".into(), "private prompt".into());
    let raw = encoded(&session);
    let compact = section(&raw);
    let value = payload(&raw);
    assert_eq!(value.as_object().unwrap().len(), 15);
    for key in ["parent_session_id", "project_id", "supervisor_management"] {
        assert!(value.get(key).is_some());
    }
    assert_eq!(value["parent_session_id"], "");
    assert_eq!(value["root_session_id"], "");
    assert_eq!(value["metadata_version"], u64::MAX);
    assert_eq!(value["title_label"].as_str().unwrap().chars().count(), 160);
    assert_eq!(value["project_id"], "typed-project");
    for secret in ["private transcript", "private prompt", "private-model"] {
        assert!(!std::str::from_utf8(compact).unwrap().contains(secret));
    }
    assert!(compact.len() < 4096);
    assert!(compact_main::decode_v1_section(compact, compact.len()).is_ok());
    let projected = compact_main::decode_v1_section(compact, compact.len())
        .unwrap()
        .into_snapshot_session();
    assert_eq!(section(&encoded(&projected)), compact);
    assert!(projected.messages.is_empty());
    assert!(!projected.metadata.contains_key("private.prompt"));
    assert!(compact_main::decode_v1_section(compact, compact.len() - 1).is_err());
    assert!(compact_main::validate_full_main(&raw).unwrap().is_some());
    session.runtime_metadata.as_mut().unwrap().project_id = None;
    assert_eq!(
        payload(&encoded(&session))["project_id"],
        "shadowed-project"
    );
    session.metadata.remove("project_id");
    assert!(payload(&encoded(&session))["project_id"].is_null());
    assert!(compact_main::validate_full_main(&flat(&session))
        .unwrap()
        .is_none());
}

#[test]
fn compact_missing_null_unknown_nested_duplicate_and_numeric_shapes_reject() {
    let session = Session::new("codec", "model");
    let raw = encoded(&session);
    let original = payload(&raw);
    for key in original.as_object().unwrap().keys() {
        let mut changed = original.clone();
        changed.as_object_mut().unwrap().remove(key);
        assert!(
            compact_main::validate_full_main(&with_payload(&changed, &session)).is_err(),
            "missing {key}"
        );
    }
    for (key, value) in [
        ("unknown", json!(true)),
        ("kind", json!("planner")),
        ("metadata_version", json!(1.0)),
        ("spawn_depth", json!(u64::MAX)),
        ("root_orchestration_only", json!(0)),
        ("authority_identity", json!({"kind":"ordinary","extra":1})),
        ("title_label", json!("\u{202e}bad")),
    ] {
        let mut changed = original.clone();
        changed[key] = value;
        assert!(
            compact_main::validate_full_main(&with_payload(&changed, &session)).is_err(),
            "shape {key}"
        );
    }
    let text = serde_json::to_string(&original).unwrap();
    for duplicate in ["\"id\":\"codec\",", "\"parent_session_id\":null,"] {
        let repeated = text.replacen('{', &format!("{{{duplicate}"), 1);
        assert!(compact_main::validate_full_main(&framed(&repeated, &flat(&session))).is_err());
    }
    let nested = text.replace(
        "\"kind\":\"ordinary\"",
        "\"kind\":\"ordinary\",\"kind\":\"ordinary\"",
    );
    assert!(compact_main::validate_full_main(&framed(&nested, &flat(&session))).is_err());
    let mut prefix = section(&raw).to_vec();
    prefix.extend_from_slice(b"\"id\":\"codec\",");
    prefix.extend_from_slice(&flat(&session)[1..]);
    assert!(compact_main::validate_full_main(&prefix).is_err());
    // Duplicate known fields in an absent-section legacy file retain legacy serde behavior.
    let legacy = b"{\"model\":\"ignored\",\"model\":\"ignored\"}";
    assert!(compact_main::validate_full_main(legacy).unwrap().is_none());
}

#[test]
fn compact_frame_corruption_never_becomes_legacy_absence() {
    let session = Session::new("frame", "model");
    let raw = encoded(&session);
    let text = std::str::from_utf8(&raw).unwrap();
    let length = payload(&raw).to_string().len();
    for changed in [
        format!(" {text}"),
        format!("\u{feff}{text}"),
        text.replacen("_bamboo_main_authority", "\\u005fbamboo_main_authority", 1),
        text.replacen("\"version\":1", "\"version\":2", 1),
        text.replacen(&format!("{length:010}"), &format!("{:010}", length - 1), 1),
        text.replacen(&format!("{length:010}"), &format!("{:010}", length + 1), 1),
        text.replacen(&format!("{length:010}"), "9999999999", 1),
        text.replacen(&format!("{length:010}"), "00000000x1", 1),
        text.replacen("\"payload\":", "\"extra\":0,\"payload\":", 1),
        text.replacen("},\n", "} ,\n", 1),
    ] {
        assert!(compact_main::validate_full_main(changed.as_bytes()).is_err());
    }
    let value: Value = serde_json::from_slice(&raw).unwrap();
    // Even an otherwise valid envelope reordered by a generic writer is not canonical v1.
    assert!(compact_main::validate_full_main(&serde_json::to_vec_pretty(&value).unwrap()).is_err());
    let duplicate = format!(
        "{}\"_bamboo_main_authority\":{},{}",
        std::str::from_utf8(section(&raw)).unwrap(),
        value[compact_main::MEMBER],
        std::str::from_utf8(&flat(&session)[1..]).unwrap()
    );
    assert!(compact_main::validate_full_main(duplicate.as_bytes()).is_err());
    for end in [1, 50, section(&raw).len() - 1] {
        assert!(compact_main::decode_v1_section(&raw[..end], compact_main::SECTION_CAP).is_err());
    }
    let bad_body = [section(&raw), b"invalid body"].concat();
    assert!(compact_main::decode_v1_section(&bad_body, compact_main::SECTION_CAP).is_ok());
    assert!(compact_main::validate_full_main(&bad_body).is_err());
    // Pure prefix observation cannot prove a matching unseen flat birth/Project.
    let changed = corrupt_flat(&raw, "created_at", json!("2001-01-01T00:00:00Z"));
    assert!(compact_main::decode_v1_section(section(&changed), compact_main::SECTION_CAP).is_ok());
    assert!(compact_main::validate_full_main(&changed).is_err());
}

#[test]
fn compact_max_management_receipts_preserve_tombstones_and_bound_rejects() {
    let mut session = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
    let incarnation_id = Uuid::new_v4();
    session.authority_identity = SessionAuthorityIdentity::Supervisor { incarnation_id };
    let projects: BTreeSet<ProjectId> = (0..MAX_SUPERVISOR_PROJECTS)
        .map(|i| format!("project-{i}").parse().unwrap())
        .collect();
    let project = projects.first().unwrap().clone();
    session.supervisor_management = Some(SupervisorManagementState {
        schema_version: 1,
        incarnation_id,
        revision: u64::MAX,
        allowed_projects: projects,
        links: (0..MAX_SUPERVISOR_LINKS)
            .map(|i| {
                (
                    format!("{i:04}{}", "x".repeat(252)),
                    SupervisorManagedLink {
                        revision: u64::MAX,
                        enabled: false,
                        target_created_at: session.created_at,
                        target_project_id: project.clone(),
                        target_metadata_version: u64::MAX,
                    },
                )
            })
            .collect(),
    });
    for _ in 0..8 {
        let epoch = session.root_mode_transition_epoch;
        session
            .record_root_mode_operation(RootModeOperationReceipt {
                operation_id: format!("{epoch}:{}", Uuid::new_v4()),
                expected_epoch: epoch,
                resulting_epoch: epoch + 1,
                requested_enabled: false,
                enabled_at_completion: false,
                tool_authority_revision: 0,
                outcome: RootModeOperationOutcome::Fenced,
            })
            .unwrap();
    }
    let raw = encoded(&session);
    assert!(compact_main::validate_full_main(&raw).unwrap().is_some());
    let projected = compact_main::decode_v1_section(section(&raw), raw.len())
        .unwrap()
        .into_snapshot_session();
    assert_eq!(section(&encoded(&projected)), section(&raw));
    assert_eq!(
        payload(&raw)["root_mode_operations"]
            .as_array()
            .unwrap()
            .len(),
        8
    );
    assert_eq!(
        payload(&raw)["supervisor_management"]["links"]
            .as_object()
            .unwrap()
            .len(),
        MAX_SUPERVISOR_LINKS
    );
    let mut repeated = payload(&raw);
    let first = repeated["supervisor_management"]["allowed_projects"][0].clone();
    repeated["supervisor_management"]["allowed_projects"]
        .as_array_mut()
        .unwrap()
        .push(first);
    assert!(compact_main::validate_full_main(&with_payload(&repeated, &session)).is_err());
    session
        .root_mode_operations
        .push(session.root_mode_operations[0].clone());
    assert!(compact_main::serialize_main(&session).is_err());
    session.root_mode_operations.pop();
    session
        .supervisor_management
        .as_mut()
        .unwrap()
        .links
        .values_mut()
        .next()
        .unwrap()
        .revision = 0;
    assert!(compact_main::serialize_main(&session).is_err());
    let mut huge = Session::new("large", "model");
    huge.set_project_id_meta("x".repeat(compact_main::SECTION_CAP));
    assert!(compact_main::serialize_main(&huge)
        .unwrap_err()
        .to_string()
        .contains("section budget"));
}

#[tokio::test]
async fn compact_every_named_publisher_reopens_exact_main_and_legacy_runtime() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let mut session = Session::new("publishers", "model");
    session.add_message(Message::system("system"));
    session.add_message(Message::user("history"));
    store.save_session(&session).await.unwrap();
    let dir = home.path().join("sessions/publishers");
    assert_pair(&dir, &session);
    session.title = "full updated".into();
    session.metadata_version += 1;
    store.save_session(&session).await.unwrap();
    assert_pair(&dir, &session);
    let operation = RootModeOperationRequest {
        session_id: session.id.clone(),
        operation_id: format!("0:{}", Uuid::new_v4()),
        birth_token: session.root_mode_birth_token(),
        expected_epoch: 0,
        requested_enabled: true,
        action: RootModeOperationAction::Select,
    };
    assert!(matches!(
        store.root_mode_operation(&operation).await.unwrap(),
        RootModeOperationDecision::Terminal(_)
    ));
    session = store.load_session(&session.id).await.unwrap().unwrap();
    assert_pair(&dir, &session);
    session.title = "newer Runtime title".into();
    session.metadata_version += 1;
    store.save_runtime_state(&session).await.unwrap();
    store.clear_session(&session.id).await.unwrap();
    let cleared = store.load_session(&session.id).await.unwrap().unwrap();
    assert_pair(&dir, &cleared);
    assert_eq!(cleared.title, "newer Runtime title");
    assert_eq!(cleared.messages.len(), 1);
    let copied = store
        .copy_session(&session.id, "copy")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(copied.created_at, session.created_at);
    assert_pair(&home.path().join("sessions/copy"), &copied);
    let fallback = Session::new("fallback", "model");
    store.save_runtime_state(&fallback).await.unwrap();
    assert_pair(&home.path().join("sessions/fallback"), &fallback);
    store.delete_session(&session.id).await.unwrap();
    let recreated = store
        .recreate_root_session(&session.id, "model")
        .await
        .unwrap();
    assert_ne!(recreated.created_at, session.created_at);
    assert_pair(&dir, &recreated);
    let supervisor = store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let supervisor_session = store
        .load_session(&supervisor.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_pair(
        &home.path().join("sessions").join(&supervisor.session_id),
        &supervisor_session,
    );
    drop(store);
    let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
    for id in [
        "publishers",
        "copy",
        "fallback",
        DEFAULT_SUPERVISOR_SESSION_ID,
    ] {
        let actual = reopened.load_session(id).await.unwrap().unwrap();
        assert_pair(&home.path().join("sessions").join(id), &actual);
    }
}

#[tokio::test]
async fn compact_capacity_full_clear_copy_preflight_preserves_own_state_and_attachments() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let mut huge = Session::new("oversized-new", "model");
    huge.set_project_id_meta("x".repeat(compact_main::SECTION_CAP));
    assert!(store
        .save_session(&huge)
        .await
        .unwrap_err()
        .to_string()
        .contains("section budget"));
    assert!(!home.path().join("sessions/oversized-new").exists());
    let mut source = Session::new("source", "model");
    source.add_message(Message::user("preserve history"));
    store.save_session(&source).await.unwrap();
    let directory = home.path().join("sessions/source");
    std::fs::write(directory.join("attachments/keep.bin"), b"attachment").unwrap();
    let before = files(&directory);
    let mut huge_full = source.clone();
    huge_full.set_project_id_meta("x".repeat(compact_main::SECTION_CAP));
    huge_full.metadata_version += 1;
    assert!(store
        .save_session(&huge_full)
        .await
        .unwrap_err()
        .to_string()
        .contains("section budget"));
    assert_eq!(files(&directory), before);
    // Legacy pathological input remains readable, but a new clear/copy cannot
    // publish a truncated section or start its own destructive work.
    source.set_project_id_meta("x".repeat(compact_main::SECTION_CAP));
    std::fs::write(directory.join("session.json"), flat(&source)).unwrap();
    std::fs::write(
        directory.join(RUNTIME_SIDECAR_FILE),
        serde_json::to_vec_pretty(&runtime_sidecar_snapshot(&source)).unwrap(),
    )
    .unwrap();
    let before = files(&directory);
    assert!(store
        .clear_session("source")
        .await
        .unwrap_err()
        .to_string()
        .contains("section budget"));
    assert_eq!(files(&directory), before);
    assert!(store
        .copy_session("source", "copy-overflow")
        .await
        .unwrap_err()
        .to_string()
        .contains("section budget"));
    assert_eq!(files(&directory), before);
    assert!(!home.path().join("sessions/copy-overflow").exists());
    let path = home.path().join(SESSION_COPY_TRANSACTION_DIR);
    assert!(!path.exists() || std::fs::read_dir(path).unwrap().next().is_none());
    assert!(!std::fs::read_dir(home.path()).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .contains("copy-staging")));
}

#[tokio::test]
async fn compact_actual_main_authority_readers_reject_contradiction_without_repair() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let session = Session::new("corrupt", "model");
    store.save_session(&session).await.unwrap();
    let directory = home.path().join("sessions/corrupt");
    let original = std::fs::read(directory.join("session.json")).unwrap();
    let damaged = corrupt_flat(&original, "metadata_version", json!(1));
    std::fs::write(directory.join("session.json"), &damaged).unwrap();
    let before = files(&directory);
    assert!(store
        .load_session_from_dir(&directory, "corrupt")
        .await
        .is_none());
    assert!(store
        .load_session_from_dir_strict(&directory, "corrupt", SessionKind::Root, "corrupt")
        .await
        .is_err());
    assert!(store
        .load_authoritative_root_session("corrupt")
        .await
        .is_err());
    assert!(store.load_session_unlocked("corrupt").await.is_err());
    assert!(store.canonical_root_birth("corrupt").await.is_err());
    assert!(store
        .load_root_authority_unchecked("corrupt")
        .await
        .is_err());
    assert!(SessionStoreV2::read_runtime_task_recovery_main_at(
        &directory,
        "corrupt",
        SessionKind::Root,
        "corrupt"
    )
    .await
    .is_err());
    let lifecycle = store.lock_default_writer_lifecycle().await.unwrap();
    let task = store.lock_runtime_task_sidecar_shared().await.unwrap();
    let writer = store
        .acquire_session_maintenance_lock("corrupt")
        .await
        .unwrap();
    let guards = DefaultWriterGuards::shared(lifecycle, task, writer);
    assert!(store
        .validate_root_context_for_full_save(&session)
        .await
        .is_err());
    drop(guards);
    assert!(store.copy_session("corrupt", "no-copy").await.is_err());
    assert!(!home.path().join("sessions/no-copy").exists());
    assert_eq!(files(&directory), before);
    // A runtime-absent fallback must not reinterpret a contradictory Main.
    std::fs::remove_file(directory.join(RUNTIME_SIDECAR_FILE)).unwrap();
    let before = files(&directory);
    assert!(store
        .load_runtime_control_plane_unchecked("corrupt")
        .await
        .is_err());
    let entry = store.get_index_entry("corrupt").await.unwrap();
    assert!(!store.migrate_runtime_candidate(&entry).await.unwrap());
    assert_eq!(files(&directory), before);
    // Rebuild cannot publish corrupt Main into its index projection.
    assert!(store
        .load_session_from_dir(&directory, "corrupt")
        .await
        .is_none());
}

#[tokio::test]
async fn compact_proof_migrations_and_legacy_supervisor_classification_never_mint_from_mismatch() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let root = Session::new("proof-root", "model");
    store.save_session(&root).await.unwrap();
    let directory = home.path().join("sessions/proof-root");
    let main_path = directory.join("session.json");
    let damaged = corrupt_flat(
        &std::fs::read(&main_path).unwrap(),
        "metadata_version",
        json!(5),
    );
    std::fs::write(&main_path, &damaged).unwrap();
    std::fs::remove_file(directory.join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE)).unwrap();
    std::fs::remove_file(home.path().join(".root_tool_authority_proof_v1")).unwrap();
    let before = files(&directory);
    store
        .migrate_root_tool_authority_proofs(false)
        .await
        .unwrap();
    assert_eq!(files(&directory), before);
    assert!(!directory
        .join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE)
        .exists());

    let receipt = store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let directory = home.path().join("sessions").join(receipt.session_id);
    let main_path = directory.join("session.json");
    let damaged = corrupt_flat(
        &std::fs::read(&main_path).unwrap(),
        "metadata_version",
        json!(5),
    );
    std::fs::write(&main_path, &damaged).unwrap();
    std::fs::remove_file(directory.join(supervisor_proof::SUPERVISOR_PROOF_FILE)).unwrap();
    std::fs::remove_file(
        home.path()
            .join(supervisor_proof::SUPERVISOR_PROOF_MIGRATION_MARKER),
    )
    .unwrap();
    let before = files(&directory);
    store.migrate_supervisor_proof(false).await.unwrap();
    assert_eq!(files(&directory), before);
    assert!(!directory
        .join(supervisor_proof::SUPERVISOR_PROOF_FILE)
        .exists());

    let legacy_home = tempfile::tempdir().unwrap();
    let legacy = SessionStoreV2::new(legacy_home.path().into())
        .await
        .unwrap();
    let directory = legacy_home
        .path()
        .join("sessions")
        .join(DEFAULT_SUPERVISOR_SESSION_ID);
    std::fs::create_dir(&directory).unwrap();
    let ordinary = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
    let damaged = corrupt_flat(&encoded(&ordinary), "metadata_version", json!(9));
    std::fs::write(directory.join("session.json"), damaged).unwrap();
    let before = files(&directory);
    assert_eq!(
        legacy
            .get_or_create_default_supervisor("model")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(files(&directory), before);
}

#[tokio::test]
async fn compact_actor_default_classifier_rejects_unseen_flat_tamper_snapshot_observes_frame() {
    use bamboo_domain::{
        ActorDirectoryPort, ActorSnapshotLimits, ActorSnapshotPort, ActorSnapshotPrincipal,
    };
    let home = tempfile::tempdir().unwrap();
    let canonical = home.path().canonicalize().unwrap();
    let store = SessionStoreV2::new(canonical.clone()).await.unwrap();
    let session = Session::new("actor", "model");
    store.save_session(&session).await.unwrap();
    store.inspect_actor("actor").await.unwrap();
    let directory = canonical.join("sessions/actor");
    let damaged = corrupt_flat(
        &std::fs::read(directory.join("session.json")).unwrap(),
        "metadata_version",
        json!(7),
    );
    std::fs::write(directory.join("session.json"), &damaged).unwrap();
    let before = files(&directory);
    assert!(store
        .check_default_actor_context(&session, &directory, true)
        .await
        .is_err());
    assert!(store.check_default_actor_clear(&directory).await.is_err());
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    assert_eq!(
        store
            .actor_subtree_snapshot(
                ActorSnapshotPrincipal::host_owner(),
                "actor",
                "actor",
                ActorSnapshotLimits::default()
            )
            .await
            .unwrap()
            .nodes[0]
            .revision
            .session_metadata_version,
        session.metadata_version
    );
    // The graph observer does not read the altered flat metadata version.
    assert!(compact_main::validate_full_main(&damaged).is_err());
    assert_eq!(files(&directory), before);
}

#[tokio::test]
async fn compact_runtime_task_management_and_sidecar_reconstruction_retain_exact_main() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let mut root = Session::new("retained", "model");
    store.save_session(&root).await.unwrap();
    let directory = home.path().join("sessions/retained");
    let original = std::fs::read(directory.join("session.json")).unwrap();
    root.metadata
        .insert("ordinary.runtime".into(), "changed".into());
    store.save_runtime_state(&root).await.unwrap();
    assert_eq!(
        std::fs::read(directory.join("session.json")).unwrap(),
        original
    );
    let mut task_updated = root.clone();
    task_updated.task_list = Some(super::tests::transaction_task_list(
        "retained",
        "task update",
    ));
    task_updated.set_task_list_version_meta("1");
    assert!(store
        .save_task_control_plane_if_matches(&root, &task_updated)
        .await
        .unwrap());
    assert_eq!(
        std::fs::read(directory.join("session.json")).unwrap(),
        original
    );
    let child_parent = Session::new("migration-parent", "model");
    store.save_session(&child_parent).await.unwrap();
    let child = Session::new_child_of("retained-child", &child_parent, "model", "child");
    store.save_session(&child).await.unwrap();
    // A proof-protected Root with missing Runtime retains its existing rejection.
    std::fs::remove_file(directory.join(RUNTIME_SIDECAR_FILE)).unwrap();
    let entry = store.get_index_entry("retained").await.unwrap();
    assert!(store.migrate_runtime_candidate(&entry).await.is_err());
    assert_eq!(
        std::fs::read(directory.join("session.json")).unwrap(),
        original
    );
    assert!(!directory.join(RUNTIME_SIDECAR_FILE).exists());
    // Supported Child migration rebuilds Runtime but never compact-prepares Main.
    let directory = home
        .path()
        .join("sessions/migration-parent/children/retained-child");
    let original = std::fs::read(directory.join("session.json")).unwrap();
    std::fs::remove_file(directory.join(RUNTIME_SIDECAR_FILE)).unwrap();
    let entry = store.get_index_entry(&child.id).await.unwrap();
    assert!(store.migrate_runtime_candidate(&entry).await.unwrap());
    assert_eq!(
        std::fs::read(directory.join("session.json")).unwrap(),
        original
    );
    assert!(serde_json::from_slice::<Value>(
        &std::fs::read(directory.join(RUNTIME_SIDECAR_FILE)).unwrap()
    )
    .unwrap()
    .get(compact_main::MEMBER)
    .is_none());
    let bootstrap = store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let directory = home.path().join("sessions").join(&bootstrap.session_id);
    let original = std::fs::read(directory.join("session.json")).unwrap();
    store
        .mutate_supervisor_management(&SupervisorManagementRequest {
            supervisor: SupervisorReference::from(&bootstrap),
            expected_state_revision: 0,
            mutation: SupervisorManagementMutation::ConfigureProjectScope {
                allowed_projects: BTreeSet::from(["project".parse().unwrap()]),
            },
        })
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(directory.join("session.json")).unwrap(),
        original
    );
    assert!(compact_main::validate_full_main(&original)
        .unwrap()
        .is_some());
}

struct ReleaseDefault(Arc<super::default_actor_context_tests::DefaultWriteHook>);
impl Drop for ReleaseDefault {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_started_main_replace_keeps_old_or_new_whole_encoding_after_caller_abort() {
    use super::default_actor_context_tests::DefaultWriteHook;
    for phase in [
        DurableWritePhase::BeforeReplace,
        DurableWritePhase::AfterReplace,
    ] {
        let home = tempfile::tempdir().unwrap();
        let first = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let original = Session::new("barrier", "model");
        first.save_session(&original).await.unwrap();
        let directory = home.path().join("sessions/barrier");
        let old = std::fs::read(directory.join("session.json")).unwrap();
        let second = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let mut next = original.clone();
        next.title = "committed 🪷".into();
        next.metadata_version = 1;
        let new = encoded(&next);
        let hook = DefaultWriteHook::install(&first, "session.json", phase, false);
        let _release = ReleaseDefault(hook.clone());
        let save = tokio::spawn({
            let first = first.clone();
            async move { first.save_session(&next).await }
        });
        tokio::task::spawn_blocking({
            let hook = hook.clone();
            move || hook.wait()
        })
        .await
        .unwrap();
        let observed = std::fs::read(directory.join("session.json")).unwrap();
        assert_eq!(
            observed,
            if phase == DurableWritePhase::BeforeReplace {
                old
            } else {
                new.clone()
            }
        );
        assert!(compact_main::validate_full_main(&observed)
            .unwrap()
            .is_some());
        save.abort();
        assert!(save.await.unwrap_err().is_cancelled());
        let successor = tokio::spawn(async move { second.load_root_authority("barrier").await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !successor.is_finished(),
            "physical writer guards released before started job terminal"
        );
        hook.release();
        let loaded = successor.await.unwrap().unwrap().unwrap();
        assert_eq!(loaded.title, "committed 🪷");
        assert_eq!(std::fs::read(directory.join("session.json")).unwrap(), new);
    }
}
