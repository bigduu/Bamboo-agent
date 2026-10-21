use super::*;
use serde_json::json;
use tempfile::TempDir;

fn fixture() -> TempDir {
    let key = crate::encryption::set_test_encryption_key([0x51; 32]);
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("config.json"), b"{}").unwrap();
    let facade = crate::ConfigFacade::open_or_migrate(dir.path()).unwrap();
    drop(facade);
    drop(key);
    dir
}

fn synthetic_candidate(snapshot: &ShareTransactionSnapshot) -> ShareTransactionCandidate {
    let mut providers: Value =
        serde_json::from_slice(snapshot.providers.as_deref().unwrap()).unwrap();
    providers["revision"] = json!(snapshot.revisions.providers + 1);
    providers["data"]["provider_instances"] = json!({
        "shared-demo": {"provider_type":"openai","label":"Synthetic demo","model":"synthetic-model",
            "credential_ref":"provider_instance.shared-demo.api_key","enabled":true}
    });
    let env = json!({"schema_version":1,"revision":snapshot.revisions.env + 1,"data":[{
        "name":"SYNTHETIC_TOKEN","value":"","secret":true,"configured":true,
        "credential_ref":"env.SYNTHETIC_TOKEN.value"
    }]});
    let mut credentials: Value =
        serde_json::from_slice(snapshot.credentials.as_deref().unwrap()).unwrap();
    credentials["revision"] = json!(snapshot.revisions.credentials + 1);
    // The transaction boundary accepts already-prepared ciphertext and never
    // attempts decryption. These deliberately synthetic opaque values exercise
    // that boundary without reading keys, environment variables or user files.
    for reference in [
        "provider_instance.shared-demo.api_key",
        "env.SYNTHETIC_TOKEN.value",
    ] {
        credentials["data"]["entries"][reference] = json!({
            "ciphertext":"synthetic-prepared-ciphertext", "source":"user",
            "updated_at":"2026-10-21T00:00:00Z", "key_version":1
        });
    }
    ShareTransactionCandidate {
        providers: serde_json::to_vec_pretty(&providers).unwrap(),
        env: serde_json::to_vec_pretty(&env).unwrap(),
        credentials: serde_json::to_vec_pretty(&credentials).unwrap(),
    }
}

fn originals(dir: &Path) -> Vec<Option<Vec<u8>>> {
    MEMBERS
        .iter()
        .map(|name| bounded_read(&dir.join(name)).unwrap())
        .collect()
}

fn tree(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(base: &Path, current: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(current).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                walk(base, &entry.path(), out);
            } else {
                out.insert(
                    entry.path().strip_prefix(base).unwrap().to_path_buf(),
                    std::fs::read(entry.path()).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    walk(dir, dir, &mut result);
    result
}

#[test]
fn commit_replay_returns_original_receipt_without_replanning_or_revision_change() {
    let dir = fixture();
    let expected = read_share_snapshot(dir.path()).unwrap().revisions;
    let id = Uuid::new_v4();
    let hash = sha256(b"synthetic-encrypted-envelope-and-decisions");
    let first = commit_share_transaction(dir.path(), id, &hash, expected, |snapshot| {
        Ok(synthetic_candidate(snapshot))
    })
    .unwrap();
    let after = tree(dir.path());
    let replay = commit_share_transaction(dir.path(), id, &hash, ShareRevisions::default(), |_| {
        panic!("a replay must not plan again")
    })
    .unwrap();
    assert_eq!(first, replay);
    assert_eq!(after, tree(dir.path()));
    assert_eq!(
        read_share_operation(dir.path(), id).unwrap(),
        ShareOperationStatus::Committed {
            operation_id: id,
            revisions: first.revisions
        }
    );
    assert!(commit_share_transaction(
        dir.path(),
        id,
        &sha256(b"different-envelope"),
        first.revisions,
        |_| panic!("ID collision must not plan")
    )
    .is_err());
    assert_eq!(after, tree(dir.path()));
}

#[test]
fn stale_providers_env_or_credentials_and_cancelled_plans_change_no_configuration() {
    let dir = fixture();
    let expected = read_share_snapshot(dir.path()).unwrap().revisions;
    let before = originals(dir.path());
    for stale in [
        ShareRevisions {
            providers: expected.providers + 1,
            ..expected
        },
        ShareRevisions {
            env: expected.env + 1,
            ..expected
        },
        ShareRevisions {
            credentials: expected.credentials + 1,
            ..expected
        },
    ] {
        let result = commit_share_transaction(
            dir.path(),
            Uuid::new_v4(),
            &sha256(b"envelope"),
            stale,
            |_| panic!("CAS must precede planning"),
        );
        assert!(matches!(result, Err(ConfigStoreError::Conflict { .. })));
        assert_eq!(before, originals(dir.path()));
    }
    assert!(commit_share_transaction(
        dir.path(),
        Uuid::new_v4(),
        &sha256(b"envelope"),
        expected,
        |_| Err(invalid())
    )
    .is_err());
    assert_eq!(before, originals(dir.path()));
}

#[test]
fn every_staging_and_precommit_crash_leaves_configuration_unchanged() {
    for fault in [
        ShareFault::AfterStage(0),
        ShareFault::AfterStage(1),
        ShareFault::AfterStage(2),
        ShareFault::AfterStage(3),
        ShareFault::AfterStaging,
        ShareFault::AfterJournal,
    ] {
        let dir = fixture();
        let expected = read_share_snapshot(dir.path()).unwrap().revisions;
        let before = originals(dir.path());
        let id = Uuid::new_v4();
        let hash = sha256(b"synthetic-envelope");
        let result = commit_inner(
            dir.path(),
            id,
            &hash,
            expected,
            |snapshot| Ok(synthetic_candidate(snapshot)),
            fault,
        );
        assert!(result.is_err());
        assert!(!matches!(
            result,
            Err(ConfigStoreError::CommitIndeterminate(_))
        ));
        assert_eq!(before, originals(dir.path()));
        assert_eq!(
            read_share_operation(dir.path(), id).unwrap(),
            ShareOperationStatus::NotFound { operation_id: id }
        );
        // A normal retry discards only its orphaned uncommitted staging.
        let receipt = commit_share_transaction(dir.path(), id, &hash, expected, |snapshot| {
            Ok(synthetic_candidate(snapshot))
        })
        .unwrap();
        assert_eq!(receipt.revisions.providers, expected.providers + 1);
    }
}

#[test]
fn every_committed_member_and_completion_crash_recovers_one_operation() {
    for fault in [
        ShareFault::AfterManifest,
        ShareFault::AfterInstall(0),
        ShareFault::AfterInstall(1),
        ShareFault::AfterInstall(2),
        ShareFault::AfterInstall(3),
        ShareFault::AfterComplete,
        ShareFault::AfterJournalCleanup,
        ShareFault::AfterCleanup,
    ] {
        let dir = fixture();
        let expected = read_share_snapshot(dir.path()).unwrap().revisions;
        let id = Uuid::new_v4();
        let hash = sha256(b"synthetic-envelope");
        let result = commit_inner(
            dir.path(),
            id,
            &hash,
            expected,
            |snapshot| Ok(synthetic_candidate(snapshot)),
            fault,
        );
        assert!(matches!(
            result,
            Err(ConfigStoreError::CommitIndeterminate(_))
        ));
        if matches!(
            fault,
            ShareFault::AfterManifest | ShareFault::AfterInstall(_)
        ) {
            assert_eq!(
                read_share_operation(dir.path(), id).unwrap(),
                ShareOperationStatus::Pending { operation_id: id }
            );
            assert!(read_share_snapshot(dir.path()).is_err());
            assert!(crate::ensure_provider_mcp_migration_ready(dir.path()).is_err());
        }
        recover_pending_config_transaction(dir.path()).unwrap();
        let receipt = commit_share_transaction(dir.path(), id, &hash, expected, |_| {
            panic!("recovered replay must not plan")
        })
        .unwrap();
        assert_eq!(
            receipt.revisions,
            ShareRevisions {
                providers: expected.providers + 1,
                env: expected.env + 1,
                credentials: expected.credentials + 1
            }
        );
        assert_eq!(
            read_share_operation(dir.path(), id).unwrap(),
            ShareOperationStatus::Committed {
                operation_id: id,
                revisions: receipt.revisions
            }
        );
        assert!(read_share_snapshot(dir.path()).is_ok());
    }
}

#[test]
fn status_and_snapshot_reads_never_create_or_repair_files() {
    let empty = tempfile::tempdir().unwrap();
    let id = Uuid::new_v4();
    let before = tree(empty.path());
    assert_eq!(
        read_share_operation(empty.path(), id).unwrap(),
        ShareOperationStatus::NotFound { operation_id: id }
    );
    assert!(read_share_snapshot(empty.path()).is_err());
    assert_eq!(before, tree(empty.path()));
    let initialized = fixture();
    let before = tree(initialized.path());
    read_share_snapshot(initialized.path()).unwrap();
    read_share_operation(initialized.path(), id).unwrap();
    assert_eq!(before, tree(initialized.path()));
}

#[test]
fn raw_editor_change_before_manifest_is_detected_without_overwriting_it() {
    let dir = fixture();
    let expected = read_share_snapshot(dir.path()).unwrap().revisions;
    let env_before = bounded_read(&dir.path().join(ENV_FILE)).unwrap();
    let credentials_before = bounded_read(&dir.path().join(CREDENTIALS_FILE)).unwrap();
    let edited = commit_share_transaction(
        dir.path(),
        Uuid::new_v4(),
        &sha256(b"synthetic-envelope"),
        expected,
        |snapshot| {
            let candidate = synthetic_candidate(snapshot);
            let mut raw: Value =
                serde_json::from_slice(snapshot.providers.as_deref().unwrap()).unwrap();
            raw["data"]["provider"] = json!("gemini");
            std::fs::write(
                dir.path().join(PROVIDERS_FILE),
                serde_json::to_vec(&raw).unwrap(),
            )
            .unwrap();
            Ok(candidate)
        },
    );
    assert!(edited.is_err());
    let raw: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap()).unwrap();
    assert_eq!(raw["data"]["provider"], "gemini");
    assert_eq!(
        env_before,
        bounded_read(&dir.path().join(ENV_FILE)).unwrap()
    );
    assert_eq!(
        credentials_before,
        bounded_read(&dir.path().join(CREDENTIALS_FILE)).unwrap()
    );
}

#[test]
fn committed_staged_tamper_and_external_edits_fail_closed_before_any_install() {
    for tamper_stage in [false, true] {
        let dir = fixture();
        let expected = read_share_snapshot(dir.path()).unwrap().revisions;
        let id = Uuid::new_v4();
        commit_inner(
            dir.path(),
            id,
            &sha256(b"synthetic-envelope"),
            expected,
            |snapshot| Ok(synthetic_candidate(snapshot)),
            ShareFault::AfterManifest,
        )
        .unwrap_err();
        if tamper_stage {
            std::fs::write(
                dir.path()
                    .join(format!("{STAGE_PREFIX}{id}"))
                    .join(ENV_FILE),
                b"{}",
            )
            .unwrap();
        } else {
            std::fs::write(dir.path().join(PROVIDERS_FILE), b"external-editor-canary").unwrap();
        }
        let before = originals(dir.path());
        assert!(recover_pending_config_transaction(dir.path()).is_err());
        assert_eq!(before, originals(dir.path()));
        assert!(read_share_snapshot(dir.path()).is_err());
    }
}

#[test]
fn oversized_staged_member_is_rejected_before_allocation_or_install() {
    let dir = fixture();
    let expected = read_share_snapshot(dir.path()).unwrap().revisions;
    let id = Uuid::new_v4();
    commit_inner(
        dir.path(),
        id,
        &sha256(b"envelope"),
        expected,
        |snapshot| Ok(synthetic_candidate(snapshot)),
        ShareFault::AfterManifest,
    )
    .unwrap_err();
    let staged = dir
        .path()
        .join(format!("{STAGE_PREFIX}{id}"))
        .join(ENV_FILE);
    std::fs::OpenOptions::new()
        .write(true)
        .open(staged)
        .unwrap()
        .set_len(MAX_DOCUMENT_BYTES + 1)
        .unwrap();
    let before = originals(dir.path());
    assert!(recover_pending_config_transaction(dir.path()).is_err());
    assert_eq!(before, originals(dir.path()));
}

#[test]
fn completion_rechecks_all_member_hashes_without_changing_historical_receipt_rules() {
    let dir = fixture();
    let expected = read_share_snapshot(dir.path()).unwrap().revisions;
    let id = Uuid::new_v4();
    commit_inner(
        dir.path(),
        id,
        &sha256(b"envelope"),
        expected,
        |snapshot| Ok(synthetic_candidate(snapshot)),
        ShareFault::AfterInstall(3),
    )
    .unwrap_err();
    let manifest: MigrationManifest =
        serde_json::from_slice(&std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap()).unwrap();
    std::fs::write(
        dir.path().join(PROVIDERS_FILE),
        b"raw-editor-after-final-install",
    )
    .unwrap();
    assert!(finish(dir.path(), manifest).is_err());
    assert_eq!(
        read_share_operation(dir.path(), id).unwrap(),
        ShareOperationStatus::Pending { operation_id: id }
    );
}

#[test]
fn manifest_rejects_extra_member_unknown_scope_and_missing_receipt() {
    for change in [0, 1, 2] {
        let dir = fixture();
        let expected = read_share_snapshot(dir.path()).unwrap().revisions;
        let id = Uuid::new_v4();
        commit_inner(
            dir.path(),
            id,
            &sha256(b"envelope"),
            expected,
            |snapshot| Ok(synthetic_candidate(snapshot)),
            ShareFault::AfterManifest,
        )
        .unwrap_err();
        let path = dir.path().join(MANIFEST_FILE);
        let mut value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        match change {
            0 => {
                let mut extra = value["files"][1].clone();
                extra["name"] = json!("hooks.json");
                value["files"].as_array_mut().unwrap().push(extra);
            }
            1 => value["migration_scope"] = json!("future_share"),
            _ => {
                value["files"].as_array_mut().unwrap().pop();
            }
        }
        std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
        let before = originals(dir.path());
        assert!(recover_pending_config_transaction(dir.path()).is_err());
        assert_eq!(before, originals(dir.path()));
    }
}

#[cfg(unix)]
#[test]
fn stage_or_target_symlink_cannot_redirect_recovery() {
    for target_link in [false, true] {
        let dir = fixture();
        let outside = tempfile::tempdir().unwrap();
        let canary = outside.path().join("canary");
        std::fs::write(&canary, b"unchanged-canary").unwrap();
        let expected = read_share_snapshot(dir.path()).unwrap().revisions;
        let id = Uuid::new_v4();
        commit_inner(
            dir.path(),
            id,
            &sha256(b"envelope"),
            expected,
            |snapshot| Ok(synthetic_candidate(snapshot)),
            ShareFault::AfterManifest,
        )
        .unwrap_err();
        let path = if target_link {
            dir.path().join(ENV_FILE)
        } else {
            dir.path()
                .join(format!("{STAGE_PREFIX}{id}"))
                .join(ENV_FILE)
        };
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&canary, path).unwrap();
        assert!(recover_pending_config_transaction(dir.path()).is_err());
        assert_eq!(std::fs::read(&canary).unwrap(), b"unchanged-canary");
    }
}

#[test]
fn presence_aware_base_never_equates_missing_with_empty_file() {
    let missing = StagedFile {
        name: OPERATIONS_FILE.into(),
        staged_name: OPERATIONS_FILE.into(),
        sha256: sha256(b"candidate"),
        original_sha256: None,
        original_present: Some(false),
        migration_generation: None,
        sensitive: false,
        install_mode: InstallMode::Migration,
        expected_revision: None,
        transaction_base_sha256: None,
        touched_credential_refs: Vec::new(),
        required_credential_refs: Vec::new(),
        touched_env_names: Vec::new(),
    };
    assert!(base_matches(None, &missing));
    assert!(!base_matches(Some(b""), &missing));
    let present = StagedFile {
        original_sha256: Some(sha256(b"")),
        original_present: Some(true),
        ..missing
    };
    assert!(!base_matches(None, &present));
    assert!(base_matches(Some(b""), &present));
}

#[test]
fn credential_removal_or_unrelated_mutation_is_rejected_before_staging() {
    let dir = fixture();
    let expected = read_share_snapshot(dir.path()).unwrap().revisions;
    let before = originals(dir.path());
    assert!(commit_share_transaction(dir.path(), Uuid::new_v4(), &sha256(b"envelope"), expected, |snapshot| {
        let mut candidate = synthetic_candidate(snapshot);
        let mut credentials: Value = serde_json::from_slice(&candidate.credentials).unwrap();
        credentials["data"]["entries"]["unrelated.machine.password"] = json!({"ciphertext":"canary","source":"user","updated_at":"2026-10-21T00:00:00Z","key_version":1});
        candidate.credentials = serde_json::to_vec(&credentials).unwrap();
        Ok(candidate)
    }).is_err());
    assert_eq!(before, originals(dir.path()));

    let receipt = commit_share_transaction(
        dir.path(),
        Uuid::new_v4(),
        &sha256(b"valid-envelope"),
        expected,
        |snapshot| Ok(synthetic_candidate(snapshot)),
    )
    .unwrap();
    let before = originals(dir.path());
    assert!(commit_share_transaction(
        dir.path(),
        Uuid::new_v4(),
        &sha256(b"deletion-envelope"),
        receipt.revisions,
        |snapshot| {
            let mut credentials: Value =
                serde_json::from_slice(snapshot.credentials.as_deref().unwrap()).unwrap();
            credentials["revision"] = json!(snapshot.revisions.credentials + 1);
            credentials["data"]["entries"]
                .as_object_mut()
                .unwrap()
                .remove("provider_instance.shared-demo.api_key");
            Ok(ShareTransactionCandidate {
                providers: snapshot.providers.clone().unwrap(),
                env: snapshot.env.clone().unwrap(),
                credentials: serde_json::to_vec(&credentials).unwrap(),
            })
        }
    )
    .is_err());
    assert_eq!(before, originals(dir.path()));
}

#[test]
fn receipt_capacity_rejects_new_operations_and_preserves_old_replay() {
    let dir = fixture();
    let expected = read_share_snapshot(dir.path()).unwrap().revisions;
    let hash = sha256(b"synthetic-envelope");
    let mut ledger = OperationLedger::default();
    for _ in 0..MAX_SHARE_OPERATIONS {
        let operation_id = Uuid::new_v4();
        ledger.operations.insert(
            operation_id,
            StoredReceipt {
                request_sha256: hash.clone(),
                receipt: ShareOperationReceipt {
                    operation_id,
                    revisions: expected,
                },
            },
        );
    }
    std::fs::write(
        dir.path().join(OPERATIONS_FILE),
        serde_json::to_vec(&ledger).unwrap(),
    )
    .unwrap();
    let before = tree(dir.path());
    assert!(
        commit_share_transaction(dir.path(), Uuid::new_v4(), &hash, expected, |_| panic!(
            "full ledger must reject before planning"
        ))
        .is_err()
    );
    let old = ledger.operations.values().next().unwrap().receipt.clone();
    assert_eq!(
        commit_share_transaction(dir.path(), old.operation_id, &hash, expected, |_| panic!(
            "old receipt must replay at capacity"
        ))
        .unwrap(),
        old
    );
    assert_eq!(before, tree(dir.path()));
}

fn ordinary_provider_section(dir: &Path) -> Arc<crate::LiveSection<ProvidersSection>> {
    Arc::new(
        crate::LiveSection::open(
            "providers",
            crate::AtomicJsonStore::new(dir.join(PROVIDERS_FILE), 1),
            ProvidersSection::default(),
            |_| Ok(()),
        )
        .unwrap(),
    )
}

fn pause_at(
    dir: &Path,
    point: ShareHookPoint,
) -> (std::sync::mpsc::Receiver<()>, Arc<std::sync::Barrier>) {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let release = Arc::new(std::sync::Barrier::new(2));
    let hook_release = release.clone();
    test_hooks().lock().unwrap().insert(
        (dir.to_path_buf(), point),
        Box::new(move || {
            entered_tx.send(()).unwrap();
            hook_release.wait();
        }),
    );
    (entered_rx, release)
}

#[test]
fn normal_provider_writer_waits_for_all_share_members_and_keeps_layout_active() {
    let dir = fixture();
    let expected = read_share_snapshot(dir.path()).unwrap().revisions;
    let ordinary = ordinary_provider_section(dir.path());
    let mut candidate = ordinary.snapshot().data.as_ref().clone();
    candidate.provider = Some("gemini".into());
    let (entered, release) = pause_at(dir.path(), ShareHookPoint::AfterProviderInstall);
    let path = dir.path().to_path_buf();
    let share = std::thread::spawn(move || {
        commit_share_transaction(
            &path,
            Uuid::new_v4(),
            &sha256(b"envelope"),
            expected,
            |snapshot| Ok(synthetic_candidate(snapshot)),
        )
    });
    entered
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = ordinary.commit(expected.providers, candidate);
        result_tx
            .send(matches!(result, Err(ConfigStoreError::Conflict { .. })))
            .unwrap();
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let blocked = result_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err();
    release.wait();
    let receipt = share.join().unwrap().unwrap();
    let conflicted_after_completion = result_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    writer.join().unwrap();
    assert!(blocked);
    assert!(conflicted_after_completion);
    assert!(crate::section_layout_is_active(dir.path()).unwrap());
    // A later supported ordinary transaction may advance a completed member;
    // the share receipt is historical and must not revoke initial provenance.
    let later = ordinary_provider_section(dir.path());
    let mut candidate = later.snapshot().data.as_ref().clone();
    candidate.provider = Some("gemini".into());
    later
        .commit(receipt.revisions.providers, candidate)
        .unwrap();
    assert!(crate::section_layout_is_active(dir.path()).unwrap());
    recover_pending_config_transaction(dir.path()).unwrap();
    assert!(crate::section_layout_is_active(dir.path()).unwrap());
}

#[test]
fn queued_stale_writer_rechecks_pending_guard_after_manifest_crash() {
    let dir = fixture();
    let expected = read_share_snapshot(dir.path()).unwrap().revisions;
    let ordinary = ordinary_provider_section(dir.path());
    let mut candidate = ordinary.snapshot().data.as_ref().clone();
    candidate.provider = Some("gemini".into());
    // This is the outer guard used by the existing provider route. It becomes
    // stale while that route waits for its per-file commit lock.
    crate::ensure_provider_mcp_migration_ready(dir.path()).unwrap();
    let before = originals(dir.path());
    let (entered, release) = pause_at(dir.path(), ShareHookPoint::AfterManifest);
    let path = dir.path().to_path_buf();
    let id = Uuid::new_v4();
    let share = std::thread::spawn(move || {
        commit_inner(
            &path,
            id,
            &sha256(b"envelope"),
            expected,
            |snapshot| Ok(synthetic_candidate(snapshot)),
            ShareFault::AfterManifest,
        )
    });
    entered
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = ordinary.commit(expected.providers, candidate);
        result_tx.send(matches!(result, Err(ConfigStoreError::Validation(ref message)) if message == "configuration-share transaction is pending")).unwrap();
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let blocked = result_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err();
    release.wait();
    assert!(matches!(
        share.join().unwrap(),
        Err(ConfigStoreError::CommitIndeterminate(_))
    ));
    let rejected_pending = result_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    writer.join().unwrap();
    assert!(blocked);
    assert!(rejected_pending);
    assert_eq!(before, originals(dir.path()));
    // Loader/quarantine/revision-normalization paths take the same guarded
    // file lock and must also remain unable to modify a pending member.
    assert!(
        crate::AtomicJsonStore::<ProvidersSection>::new(dir.path().join(PROVIDERS_FILE), 1)
            .load()
            .is_err()
    );
    assert_eq!(before, originals(dir.path()));
    recover_pending_config_transaction(dir.path()).unwrap();
    assert!(matches!(
        read_share_operation(dir.path(), id).unwrap(),
        ShareOperationStatus::Committed { .. }
    ));
    assert!(crate::section_layout_is_active(dir.path()).unwrap());
}
