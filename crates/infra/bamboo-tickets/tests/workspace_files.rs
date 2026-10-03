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
    assert_eq!(s.schema, 3);
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
    assert_eq!(restored.published().unwrap().1.schema, 3);
}

#[test]
fn schema_three_file_evidence_survives_stopped_offline_transfer_without_downgrade() {
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
    assert_eq!(destination.published().unwrap().1.schema, 3);
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
