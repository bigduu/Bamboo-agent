#![cfg(unix)]
use bamboo_tickets::*;
use std::{collections::BTreeSet, fs, path::Path};

struct Fixture {
    _temp: tempfile::TempDir,
    service: TicketService,
    worker: Authority,
    assignment: String,
    workspace: ExecutionWorkspace,
}
fn execute(
    s: &TicketService,
    who: &Authority,
    id: &str,
    ops: Vec<Operation>,
) -> Result<OperationReceipt> {
    let c = s.prepare_command(who, id, ops)?;
    s.execute(who, &c)
}
fn fixture() -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("worker");
    fs::create_dir(&path).unwrap();
    let path = path.canonicalize().unwrap().to_string_lossy().into_owned();
    let binding = ScopeBinding {
        scope_id: "files".into(),
        supervisor_session_id: "root".into(),
        binding_revision: 1,
    };
    let service = TicketService::open(temp.path().join("authority"), binding.clone()).unwrap();
    let host = Authority::from_verified_host(
        binding.clone(),
        Principal::Supervisor {
            session_id: "root".into(),
        },
    );
    let runtime = Authority::from_verified_host(binding.clone(), Principal::Runtime);
    let workspace = ExecutionWorkspace {
        repo: "repo".into(),
        base_commit: "a".repeat(40),
        branch: "worker".into(),
        worktree: path.clone(),
        write_roots: vec![path.clone()],
        claims: BTreeSet::from([format!("worktree:{path}")]),
    };
    let r = execute(
        &service,
        &host,
        "start",
        vec![
            Operation::Create {
                temp_id: "w".into(),
                kind: TicketKind::Work,
                parent: None,
                depends_on: BTreeSet::new(),
                contract: Contract {
                    title: "code".into(),
                    objective: "bounded patch".into(),
                    constraints: vec![],
                    acceptance: vec!["code".into()],
                    user_acceptance_required: true,
                    allowed_tools: BTreeSet::from(["Task".into(), "Read".into(), "Write".into()]),
                },
            },
            Operation::Ready {
                work_id: "w".into(),
            },
            Operation::Start {
                work_id: "w".into(),
                temp_id: "a".into(),
                workspace: Some(workspace.clone()),
            },
        ],
    )
    .unwrap();
    let assignment = r.ids["a"].clone();
    let snapshot = service.published().unwrap().1;
    let a = &snapshot.assignments[&assignment];
    let receipt = RuntimeReceipt {
        dispatch_key: a.dispatch_key.clone(),
        spec_hash: snapshot.intents[&a.dispatch_key].spec_hash.clone(),
        run_id: "run".into(),
        session_id: "child".into(),
    };
    execute(
        &service,
        &runtime,
        "admit",
        vec![
            Operation::Admitted {
                assignment_id: assignment.clone(),
                receipt,
            },
            Operation::Running {
                assignment_id: assignment.clone(),
            },
        ],
    )
    .unwrap();
    let worker = Authority::from_verified_host(
        binding,
        Principal::Worker {
            assignment_id: assignment.clone(),
            generation: 1,
            run_id: "run".into(),
            session_id: "child".into(),
        },
    );
    Fixture {
        _temp: temp,
        service,
        worker,
        assignment,
        workspace,
    }
}
fn write(path: &Path, text: &str, hash: Option<String>) -> FileOperation {
    FileOperation::Write {
        file_path: path.to_string_lossy().into_owned(),
        content: text.into(),
        expected_sha256: hash,
    }
}

#[test]
fn content_edits_preserve_existing_ordinary_modes_and_new_files_are_private() {
    use std::os::unix::fs::PermissionsExt;
    let f = fixture();
    for mode in [0o755, 0o644] {
        let path = Path::new(&f.workspace.worktree).join(format!("script-{mode}.sh"));
        fs::write(&path, "#!/bin/sh\nexit 1\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        let op = write(
            &path,
            "#!/bin/sh\nexit 0\n",
            Some(content_hash(&fs::read(&path).unwrap())),
        );
        f.service
            .workspace_file(&f.worker, &format!("mode-{mode}"), &op)
            .unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
            mode
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "#!/bin/sh\nexit 0\n");
        f.service
            .workspace_file(&f.worker, &format!("mode-{mode}"), &op)
            .unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
            mode
        );
    }
    let path = Path::new(&f.workspace.worktree).join("new.sh");
    f.service
        .workspace_file(&f.worker, "new-private", &write(&path, "private", None))
        .unwrap();
    assert_eq!(
        fs::metadata(path).unwrap().permissions().mode() & 0o7777,
        0o600
    );
}

#[test]
fn actual_file_write_receipt_replay_changed_payload_and_full_manifest_backup() {
    let f = fixture();
    let path = Path::new(&f.workspace.worktree).join("code.rs");
    let op = write(&path, "fn answer() -> u8 { 42 }", None);
    let reply = f.service.workspace_file(&f.worker, "write-1", &op).unwrap();
    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        "fn answer() -> u8 { 42 }"
    );
    assert_eq!(
        f.service.workspace_file(&f.worker, "write-1", &op).unwrap(),
        reply
    );
    assert!(matches!(
        f.service
            .workspace_file(&f.worker, "write-1", &write(&path, "different", None)),
        Err(Error::IdempotencyConflict)
    ));
    let (commit, s) = f.service.published().unwrap();
    assert_eq!(s.schema, 4);
    assert_eq!(
        s.assignments[&f.assignment]
            .effects
            .values()
            .next()
            .unwrap()
            .state,
        EffectState::Succeeded
    );
    let artifact = reply.artifact.unwrap();
    assert_eq!(
        f.service
            .read_artifact(
                &Authority::from_verified_host(s.binding.clone(), Principal::Runtime),
                &artifact,
                FILE_BYTES_LIMIT
            )
            .unwrap(),
        fs::read(&path).unwrap()
    );
    let backup = f._temp.path().join("backup");
    assert_eq!(f.service.export(&backup).unwrap(), commit);
    let restored = TicketService::open_offline(&backup, s.binding).unwrap();
    assert_eq!(restored.published().unwrap().1.schema, 4);
}

#[test]
fn schema_four_file_evidence_survives_stopped_offline_transfer_without_downgrade() {
    let f = fixture();
    let path = Path::new(&f.workspace.worktree).join("code.rs");
    let artifact = f
        .service
        .workspace_file(&f.worker, "write", &write(&path, "immutable code", None))
        .unwrap()
        .artifact
        .unwrap();
    let s = f.service.published().unwrap().1;
    let runtime = Authority::from_verified_host(s.binding.clone(), Principal::Runtime);
    execute(
        &f.service,
        &runtime,
        "stop",
        vec![
            Operation::OutcomeUnknown {
                assignment_id: f.assignment.clone(),
                reason: "deterministic stopped transfer fixture".into(),
            },
            Operation::ConfirmStopped {
                assignment_id: f.assignment.clone(),
                effects_reconciled: true,
            },
        ],
    )
    .unwrap();
    let root = f._temp.path().join("authority");
    let copy = f._temp.path().join("copy");
    drop(f.service);
    let source = TicketService::open_offline(&root, s.binding).unwrap();
    let (commit, s) = source.published().unwrap();
    let request = MigrationRequest {
        operation_id: "transfer".into(),
        binding: s.binding.clone(),
        expected_commit: commit,
        expected_seq: s.seq,
        expected_epoch: s.authority_epoch,
        source_root: root.canonicalize().unwrap().to_string_lossy().into_owned(),
        destination_root: canonical_migration_destination(&copy).unwrap(),
        supervisor_snapshot_hash: None,
    };
    let user = Authority::from_verified_host(
        s.binding.clone(),
        Principal::User {
            user_id: "human".into(),
        },
    );
    let proof = StoppedScopeProof::from_verified_host(s.binding.clone());
    source
        .retire_for_migration(&user, &request, &proof)
        .unwrap();
    source
        .export_retired_migration(&user, &request, &proof)
        .unwrap();
    let destination = TicketService::open_offline(&copy, s.binding).unwrap();
    destination
        .activate_migrated_copy(&source, &user, &request, &proof)
        .unwrap();
    assert_eq!(destination.published().unwrap().1.schema, 4);
    assert_eq!(
        destination
            .read_artifact(&user, &artifact, FILE_BYTES_LIMIT)
            .unwrap(),
        b"immutable code"
    );
}

#[test]
fn stale_read_cannot_overwrite_concurrent_file_change() {
    let f = fixture();
    let path = Path::new(&f.workspace.worktree).join("code.rs");
    fs::write(&path, "old").unwrap();
    let read = f
        .service
        .workspace_file(
            &f.worker,
            "read",
            &FileOperation::Read {
                file_path: path.to_string_lossy().into_owned(),
            },
        )
        .unwrap();
    fs::write(&path, "changed outside run").unwrap();
    assert!(matches!(
        f.service
            .workspace_file(&f.worker, "write", &write(&path, "new", Some(read.sha256))),
        Err(Error::RevisionConflict)
    ));
    assert_eq!(fs::read_to_string(path).unwrap(), "changed outside run");
    assert!(f.service.published().unwrap().1.assignments[&f.assignment]
        .effects
        .is_empty());
}

#[test]
fn complete_read_respects_encoded_host_reply_budget_without_truncation() {
    let f = fixture();
    let path = Path::new(&f.workspace.worktree).join("bounded.txt");
    let seq = f.service.published().unwrap().1.seq;
    for content in ["a".repeat(FILE_BYTES_LIMIT), "\u{0001}".repeat(4096)] {
        assert!(content.len() <= FILE_BYTES_LIMIT);
        fs::write(&path, &content).unwrap();
        assert!(matches!(
            f.service.workspace_file(
                &f.worker,
                "read-too-large",
                &FileOperation::Read {
                    file_path: path.to_string_lossy().into_owned(),
                }
            ),
            Err(Error::ContextBudgetExceeded)
        ));
        assert_eq!(fs::read_to_string(&path).unwrap(), content);
    }
    let content = "a".repeat(16000);
    fs::write(&path, &content).unwrap();
    let reply = f
        .service
        .workspace_file(
            &f.worker,
            "read-bounded",
            &FileOperation::Read {
                file_path: path.to_string_lossy().into_owned(),
            },
        )
        .unwrap();
    assert_eq!(reply.content.as_deref(), Some(content.as_str()));
    assert!(
        serde_json::to_vec(&serde_json::json!({"result": reply}))
            .unwrap()
            .len()
            <= 16384
    );
    assert_eq!(f.service.published().unwrap().1.seq, seq);
}

#[test]
fn runtime_control_cache_cannot_be_read_or_overwritten() {
    let f = fixture();
    let cache = Path::new(&f.workspace.worktree).join(".bamboo");
    fs::create_dir(&cache).unwrap();
    let path = cache.join("private-state.json");
    fs::write(&path, "private runtime state").unwrap();
    for component in [".bamboo", ".BAMBOO", ".Bamboo", ".git", ".GIT", ".Git"] {
        let alias = Path::new(&f.workspace.worktree)
            .join(component)
            .join("private-state.json");
        for op in [
            FileOperation::Read {
                file_path: alias.to_string_lossy().into_owned(),
            },
            write(
                &alias,
                "corrupt",
                Some(content_hash(b"private runtime state")),
            ),
        ] {
            assert!(matches!(
                f.service.workspace_file(&f.worker, "control-cache", &op),
                Err(Error::ScopeDenied(_))
            ));
        }
    }
    assert_eq!(fs::read_to_string(path).unwrap(), "private runtime state");
    assert!(f.service.published().unwrap().1.assignments[&f.assignment]
        .effects
        .is_empty());
}

#[test]
fn traversal_symlinks_hardlinks_siblings_authority_and_git_are_denied() {
    use std::os::unix::fs::symlink;
    let f = fixture();
    let root = Path::new(&f.workspace.worktree);
    let outside = f._temp.path().join("outside");
    fs::write(&outside, "protected").unwrap();
    symlink(&outside, root.join("link")).unwrap();
    symlink(f._temp.path(), root.join("directory")).unwrap();
    fs::hard_link(&outside, root.join("hardlink")).unwrap();
    fs::create_dir(root.join(".git")).unwrap();
    let paths = [
        root.join("../outside"),
        root.join("link"),
        root.join("directory/outside"),
        root.join("hardlink"),
        root.join(".git/config"),
        outside.clone(),
        f._temp.path().join("authority/HEAD"),
    ];
    for (i, path) in paths.iter().enumerate() {
        assert!(
            f.service
                .workspace_file(
                    &f.worker,
                    &format!("attack-{i}"),
                    &write(path, "corrupt", None)
                )
                .is_err(),
            "{}",
            path.display()
        );
    }
    assert_eq!(fs::read_to_string(outside).unwrap(), "protected");
    assert!(f.service.published().unwrap().1.assignments[&f.assignment]
        .effects
        .is_empty());
}

#[test]
fn cancelled_generation_cannot_write_and_unstopped_owner_blocks_same_worktree() {
    let f = fixture();
    let snapshot = f.service.published().unwrap().1;
    let host = Authority::from_verified_host(
        snapshot.binding,
        Principal::Supervisor {
            session_id: "root".into(),
        },
    );
    let a = &snapshot.assignments[&f.assignment];
    execute(
        &f.service,
        &host,
        "cancel",
        vec![Operation::Cancel {
            work_id: a.work_id.clone(),
        }],
    )
    .unwrap();
    assert!(matches!(
        f.service.workspace_file(
            &f.worker,
            "late",
            &write(
                &Path::new(&f.workspace.worktree).join("late"),
                "old generation",
                None
            )
        ),
        Err(Error::ScopeDenied(_))
    ));
    let contract = snapshot.tickets[&a.work_id].contract.clone();
    execute(
        &f.service,
        &host,
        "other",
        vec![
            Operation::Create {
                temp_id: "w".into(),
                kind: TicketKind::Work,
                parent: None,
                depends_on: BTreeSet::new(),
                contract,
            },
            Operation::Ready {
                work_id: "w".into(),
            },
        ],
    )
    .unwrap();
    let other = f
        .service
        .published()
        .unwrap()
        .1
        .tickets
        .values()
        .find(|w| w.id != a.work_id)
        .unwrap()
        .id
        .clone();
    assert!(matches!(
        execute(
            &f.service,
            &host,
            "blocked",
            vec![Operation::Start {
                work_id: other,
                temp_id: "a".into(),
                workspace: Some(f.workspace)
            }]
        ),
        Err(Error::ResourceBlocked(_))
    ));
}

#[cfg(feature = "test-utils")]
#[test]
fn file_changed_but_receipt_publication_failed_never_repeats_effect() {
    use std::sync::Arc;
    let f = fixture();
    let path = Path::new(&f.workspace.worktree).join("code.rs");
    let op = write(&path, "new content", None);
    f.service.set_operation_publication_fault(
        "worker-file/".into(),
        Arc::new(|point| {
            if point == FaultPoint::BeforeHeadRename {
                Err(std::io::Error::from_raw_os_error(28))
            } else {
                Ok(())
            }
        }),
    );
    assert!(f.service.workspace_file(&f.worker, "write", &op).is_err());
    assert_eq!(fs::read_to_string(&path).unwrap(), "new content");
    f.service.set_publication_fault(None);
    assert!(matches!(
        f.service.workspace_file(&f.worker, "write", &op),
        Err(Error::ResourceBlocked(_))
    ));
    assert!(matches!(
        f.service.workspace_file(
            &f.worker,
            "new-call-id",
            &write(&path, "new content", Some(content_hash(b"new content"))),
        ),
        Err(Error::ResourceBlocked(_))
    ));
    assert_eq!(
        f.service.published().unwrap().1.assignments[&f.assignment]
            .effects
            .values()
            .next()
            .unwrap()
            .state,
        EffectState::Started
    );
    let binding = f.service.published().unwrap().1.binding;
    let root = f._temp.path().join("authority");
    drop(f.service);
    let restarted = TicketService::open(&root, binding).unwrap();
    let result = restarted.workspace_file(&f.worker, "write", &op);
    assert!(
        matches!(result, Err(Error::ScopeDenied(_))),
        "restart health {:?}, result {:?}",
        restarted.health(),
        result
    );
    assert_eq!(fs::read_to_string(path).unwrap(), "new content");
}

#[cfg(feature = "test-utils")]
fn unknown_file(f: &Fixture, name: &str) -> String {
    use std::sync::Arc;
    let path = Path::new(&f.workspace.worktree).join(name);
    f.service.set_operation_publication_fault(
        "worker-file/".into(),
        Arc::new(|point| {
            if point == FaultPoint::BeforeHeadRename {
                Err(std::io::Error::from_raw_os_error(28))
            } else {
                Ok(())
            }
        }),
    );
    assert!(f
        .service
        .workspace_file(&f.worker, name, &write(&path, "intended code", None))
        .is_err());
    f.service.set_publication_fault(None);
    f.service.published().unwrap().1.assignments[&f.assignment]
        .effects
        .iter()
        .find(|(_, e)| {
            e.file_intent
                .as_ref()
                .unwrap()
                .canonical_request
                .contains(name)
        })
        .unwrap()
        .0
        .clone()
}

#[cfg(feature = "test-utils")]
fn stopped(f: &Fixture) {
    let s = f.service.published().unwrap().1;
    execute(
        &f.service,
        &Authority::from_verified_host(s.binding, Principal::Runtime),
        "actual-stop",
        vec![Operation::RuntimeStopped {
            assignment_id: f.assignment.clone(),
            receipt: s.assignments[&f.assignment].runtime.clone().unwrap(),
            completed: false,
        }],
    )
    .unwrap();
}

#[cfg(feature = "test-utils")]
fn user(f: &Fixture) -> Authority {
    Authority::from_verified_host(
        f.service.published().unwrap().1.binding,
        Principal::User {
            user_id: "operator".into(),
        },
    )
}

#[cfg(feature = "test-utils")]
#[test]
fn explicit_observed_file_ack_requires_stop_and_releases_only_without_retry_or_acceptance() {
    use std::os::unix::fs::MetadataExt;
    let f = fixture();
    let effect = unknown_file(&f, "recover.rs");
    let who = user(&f);
    assert!(f
        .service
        .file_reconciliation_plan(
            &who,
            &f.assignment,
            &effect,
            "file-reconcile/one",
            "verified bytes"
        )
        .unwrap()
        .request
        .is_none());
    assert!(matches!(
        f.service
            .file_reconciliation_plan(&f.worker, &f.assignment, &effect, "x", "e"),
        Err(Error::ScopeDenied(_))
    ));
    stopped(&f);
    let plan = f
        .service
        .file_reconciliation_plan(
            &who,
            &f.assignment,
            &effect,
            "file-reconcile/one",
            "verified intended code after stop",
        )
        .unwrap();
    let request = plan.request.unwrap();
    let path = Path::new(&f.workspace.worktree).join("recover.rs");
    let inode = fs::metadata(&path).unwrap().ino();
    let receipt = f.service.reconcile_file_effect(&who, &request).unwrap();
    assert_eq!(receipt.ids["resource_released"], "true");
    assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
    assert_eq!(fs::read_to_string(&path).unwrap(), "intended code");
    let s = f.service.published().unwrap().1;
    let a = &s.assignments[&f.assignment];
    let w = &s.tickets[&a.work_id];
    assert_eq!(a.state, AssignmentState::Failed);
    assert_eq!(w.state, WorkState::Blocked);
    assert!(w.active_assignment.is_none());
    assert!(s.submissions.is_empty());
    assert!(a.effects[&effect]
        .provider_receipt
        .as_ref()
        .unwrap()
        .starts_with("local-file-observed:"));
    assert_eq!(
        f.service.reconcile_file_effect(&who, &request).unwrap(),
        receipt
    );
    let mut changed = request.clone();
    changed.evidence.push('!');
    assert!(matches!(
        f.service.reconcile_file_effect(&who, &changed),
        Err(Error::IdempotencyConflict)
    ));
    let other = Authority::from_verified_host(
        s.binding,
        Principal::User {
            user_id: "other".into(),
        },
    );
    assert!(matches!(
        f.service.reconcile_file_effect(&other, &request),
        Err(Error::ScopeDenied(_))
    ));
    assert!(matches!(
        f.service.authorize_tool(&f.worker, "Write"),
        Err(Error::ScopeDenied(_))
    ));
    let host = Authority::from_verified_host(
        f.service.published().unwrap().1.binding,
        Principal::Supervisor {
            session_id: "root".into(),
        },
    );
    execute(
        &f.service,
        &host,
        "explicit-next",
        vec![
            Operation::Ready {
                work_id: a.work_id.clone(),
            },
            Operation::Start {
                work_id: a.work_id.clone(),
                temp_id: "next".into(),
                workspace: Some(f.workspace.clone()),
            },
        ],
    )
    .unwrap();
    assert_eq!(
        f.service.published().unwrap().1.tickets[&a.work_id].generation,
        2
    );
}

#[cfg(feature = "test-utils")]
#[test]
fn changed_file_stale_plan_and_one_of_two_effects_never_release_unresolved_claims() {
    let mut f = fixture();
    let first = unknown_file(&f, "one.rs");
    // Inspect an existing full ledger with multiple uncertain file intents.
    // The current live port prevents creating another Write after uncertainty.
    let root = f._temp.path().join("authority");
    let binding = f.service.published().unwrap().1.binding;
    drop(f.service);
    let mut store = store::FileStore::open(&root, binding.clone()).unwrap();
    let mut s = store.published.as_ref().unwrap().1.clone();
    s.seq += 1;
    let a = s.assignments.get_mut(&f.assignment).unwrap();
    let mut second_effect = a.effects[&first].clone();
    let second_path = Path::new(&f.workspace.worktree).join("two.rs");
    fs::write(&second_path, "intended code").unwrap();
    let canonical = canonical_bytes(&write(&second_path, "intended code", None)).unwrap();
    second_effect.action_fingerprint = content_hash(&canonical);
    second_effect
        .file_intent
        .as_mut()
        .unwrap()
        .canonical_request = String::from_utf8(canonical).unwrap();
    let second = format!(
        "worker-file/{}",
        content_hash(b"fixture-second-frozen-intent")
    );
    a.effects.insert(second.clone(), second_effect);
    a.record_revision += 1;
    a.updated_seq = s.seq;
    store.publish(s).unwrap();
    drop(store);
    f.service = TicketService::open(&root, binding).unwrap();
    stopped(&f);
    let who = user(&f);
    let r = f
        .service
        .file_reconciliation_plan(
            &who,
            &f.assignment,
            &first,
            "file-reconcile/first",
            "inspect",
        )
        .unwrap()
        .request
        .unwrap();
    let path = Path::new(&f.workspace.worktree).join("one.rs");
    fs::write(&path, "different").unwrap();
    assert!(matches!(
        f.service.reconcile_file_effect(&who, &r),
        Err(Error::RevisionConflict)
    ));
    assert!(f
        .service
        .file_reconciliation_plan(&who, &f.assignment, &first, "other", "inspect")
        .unwrap()
        .request
        .is_none());
    fs::write(&path, "intended code").unwrap();
    let receipt = f.service.reconcile_file_effect(&who, &r).unwrap();
    assert_eq!(receipt.ids["resource_released"], "false");
    assert_eq!(
        f.service.published().unwrap().1.assignments[&f.assignment].state,
        AssignmentState::OutcomeUnknown
    );
    let mut stale = r;
    stale.operation_id = "file-reconcile/stale".into();
    assert!(matches!(
        f.service.reconcile_file_effect(&who, &stale),
        Err(Error::RevisionConflict)
    ));
    let r = f
        .service
        .file_reconciliation_plan(
            &who,
            &f.assignment,
            &second,
            "file-reconcile/second",
            "inspect",
        )
        .unwrap()
        .request
        .unwrap();
    assert_eq!(
        f.service.reconcile_file_effect(&who, &r).unwrap().ids["resource_released"],
        "true"
    );
}

#[cfg(feature = "test-utils")]
#[test]
fn metadata_failure_replays_frozen_observation_receipt_and_schema_three_remains_quarantined() {
    use std::sync::Arc;
    let f = fixture();
    let effect = unknown_file(&f, "recover.rs");
    stopped(&f);
    let who = user(&f);
    let r = f
        .service
        .file_reconciliation_plan(
            &who,
            &f.assignment,
            &effect,
            "file-reconcile/fault",
            "inspect",
        )
        .unwrap()
        .request
        .unwrap();
    f.service.set_operation_publication_fault(
        "file-reconcile/".into(),
        Arc::new(|p| {
            if p == FaultPoint::BeforeHeadRename {
                Err(std::io::Error::from_raw_os_error(13))
            } else {
                Ok(())
            }
        }),
    );
    assert!(f.service.reconcile_file_effect(&who, &r).is_err());
    f.service.set_publication_fault(None);
    assert_eq!(
        f.service.published().unwrap().1.assignments[&f.assignment].effects[&effect].state,
        EffectState::Started
    );
    let path = Path::new(&f.workspace.worktree).join("recover.rs");
    assert_eq!(fs::read_to_string(&path).unwrap(), "intended code");
    let root = f._temp.path().join("authority");
    let binding = f.service.published().unwrap().1.binding;
    drop(f.service);
    let service = TicketService::open_offline(&root, binding.clone()).unwrap();
    let receipt = service.reconcile_file_effect(&who, &r).unwrap();
    assert!(matches!(service.health(), Health::ReadOnly { .. }));
    drop(service);
    let service = TicketService::open_offline(&root, binding.clone()).unwrap();
    assert_eq!(service.reconcile_file_effect(&who, &r).unwrap(), receipt);
    drop(service);
    // Produce the exact supported prior format with complete immutable objects,
    // not a malformed/truncated on-disk edit. A legacy unknown cannot be cleared.
    let mut store = store::FileStore::open(&root, binding.clone()).unwrap();
    let mut s = store.published.as_ref().unwrap().1.clone();
    s.seq += 1;
    s.schema = 3;
    let e = s
        .assignments
        .get_mut(&f.assignment)
        .unwrap()
        .effects
        .get_mut(&effect)
        .unwrap();
    e.file_intent = None;
    e.state = EffectState::Started;
    e.provider_receipt = None;
    s.assignments.get_mut(&f.assignment).unwrap().state = AssignmentState::OutcomeUnknown;
    s.receipts.remove(&effect);
    s.receipts.remove(&r.operation_id);
    store.publish(s).unwrap();
    drop(store);
    let service = TicketService::open_offline(&root, binding).unwrap();
    assert!(service
        .file_reconciliation_plan(&who, &f.assignment, &effect, "legacy", "inspect")
        .unwrap()
        .request
        .is_none());
}

#[cfg(feature = "test-utils")]
#[test]
fn offline_observation_uncertain_head_keeps_readonly_until_verified_reopen() {
    use std::{os::unix::fs::MetadataExt, sync::Arc};
    let f = fixture();
    let effect = unknown_file(&f, "uncertain.rs");
    stopped(&f);
    let who = user(&f);
    let r = f
        .service
        .file_reconciliation_plan(
            &who,
            &f.assignment,
            &effect,
            "file-reconcile/uncertain",
            "inspect",
        )
        .unwrap()
        .request
        .unwrap();
    let root = f._temp.path().join("authority");
    let binding = f.service.published().unwrap().1.binding;
    let path = Path::new(&f.workspace.worktree).join("uncertain.rs");
    let inode = fs::metadata(&path).unwrap().ino();
    assert!(TicketService::open_offline(&root, binding.clone()).is_err());
    drop(f.service);
    let service = TicketService::open_offline(&root, binding.clone()).unwrap();
    service.set_operation_publication_fault(
        "file-reconcile/".into(),
        Arc::new(|point| {
            if point == FaultPoint::AfterHeadRename {
                Err(std::io::Error::from_raw_os_error(13))
            } else {
                Ok(())
            }
        }),
    );
    assert!(service.reconcile_file_effect(&who, &r).is_err());
    service.set_publication_fault(None);
    assert!(matches!(
        service.reconcile_file_effect(&who, &r),
        Err(Error::AuthorityUnavailable(_))
    ));
    assert!(service
        .file_reconciliation_plan(&who, &f.assignment, &effect, "fresh", "inspect")
        .unwrap()
        .request
        .is_none());
    assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
    drop(service);
    let service = TicketService::open_offline(&root, binding).unwrap();
    assert_eq!(
        service.reconcile_file_effect(&who, &r).unwrap().ids["resource_released"],
        "true"
    );
    assert!(matches!(service.health(), Health::ReadOnly { .. }));
    assert_eq!(fs::metadata(path).unwrap().ino(), inode);
}
