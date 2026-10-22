//! Native process termination and file-lock acceptance. The parent launches
//! only this test executable; child roles never start a service or provider.

use super::*;
use std::io::{BufRead, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

const PROBE: &str =
    "credential_migration::share_transaction::tests::native::native_share_process_probe";
const ENABLE: &str = "BAMBOO_SHARE_NATIVE_TEST";
const PREFIX: &str = "bamboo-share-native:";
const WAIT: Duration = Duration::from_secs(10);
const CRASH_EXIT: i32 = 73;
const HASH_INPUT: &[u8] = b"synthetic-native-share-envelope";
const CUTS: [ShareFault; 14] = [
    ShareFault::AfterStage(0),
    ShareFault::AfterStage(1),
    ShareFault::AfterStage(2),
    ShareFault::AfterStage(3),
    ShareFault::AfterStaging,
    ShareFault::AfterJournal,
    ShareFault::AfterManifest,
    ShareFault::AfterInstall(0),
    ShareFault::AfterInstall(1),
    ShareFault::AfterInstall(2),
    ShareFault::AfterInstall(3),
    ShareFault::AfterComplete,
    ShareFault::AfterJournalCleanup,
    ShareFault::AfterCleanup,
];

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    root: PathBuf,
    role: String,
    cut: usize,
    operation_id: Uuid,
    expected: ShareRevisions,
}

struct ProbeChild {
    child: Child,
    input: ChildStdin,
    frames: Receiver<Option<String>>,
    reader: Option<std::thread::JoinHandle<()>>,
    reaped: bool,
}

impl ProbeChild {
    fn spawn(spec: &Spec) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", PROBE, "--nocapture", "--test-threads=1"])
            .env_clear()
            .env("RUST_MIN_STACK", "8388608")
            .env(ENABLE, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = child.stdout.take().unwrap();
        let (tx, frames) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufReader::new(output).lines() {
                let Ok(line) = line else {
                    break;
                };
                // libtest may print its test-name prefix on the same line.
                if let Some(index) = line.find(PREFIX) {
                    if tx
                        .send(Some(line[index + PREFIX.len()..].to_string()))
                        .is_err()
                    {
                        return;
                    }
                }
            }
            let _ = tx.send(None);
        });
        let mut probe = Self {
            child,
            input,
            frames,
            reader: Some(reader),
            reaped: false,
        };
        probe.send(&serde_json::to_string(spec).unwrap());
        probe
    }

    fn send(&mut self, line: &str) {
        writeln!(self.input, "{line}").unwrap();
        self.input.flush().unwrap();
    }

    fn expect(&self, frame: &str) {
        assert_eq!(
            self.frames
                .recv_timeout(WAIT)
                .expect("native child timed out"),
            Some(frame.into())
        );
    }

    fn finish(&mut self, code: i32) {
        // EOF is observed before wait, so a stuck role reaches the timeout
        // while we still own its Child handle and can terminate only that PID.
        assert_eq!(
            self.frames
                .recv_timeout(WAIT)
                .expect("native child did not exit"),
            None
        );
        let status = self.child.wait().unwrap();
        self.reaped = true;
        assert_eq!(status.code(), Some(code));
    }
}

impl Drop for ProbeChild {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

fn emit(frame: &str) {
    println!("{PREFIX}{frame}");
    std::io::stdout().flush().unwrap();
}

fn input_line() -> String {
    let mut line = String::new();
    assert!(std::io::stdin().read_line(&mut line).unwrap() > 0);
    line.trim_end().into()
}

fn hashes(root: &Path) -> Vec<Option<String>> {
    originals(root)
        .iter()
        .map(|bytes| bytes.as_deref().map(sha256))
        .collect()
}

fn advanced(revisions: ShareRevisions) -> ShareRevisions {
    ShareRevisions {
        providers: revisions.providers + 1,
        env: revisions.env + 1,
        credentials: revisions.credentials + 1,
    }
}

fn recover_and_check(spec: &Spec) {
    recover_pending_config_transaction(&spec.root).unwrap();
    let hash = sha256(HASH_INPUT);
    if spec.cut < 6 {
        assert_eq!(
            read_share_operation(&spec.root, spec.operation_id).unwrap(),
            ShareOperationStatus::NotFound {
                operation_id: spec.operation_id
            }
        );
        commit_share_transaction(
            &spec.root,
            spec.operation_id,
            &hash,
            spec.expected,
            |snapshot| Ok(synthetic_candidate(snapshot)),
        )
        .unwrap();
    }
    let committed = ShareOperationStatus::Committed {
        operation_id: spec.operation_id,
        revisions: advanced(spec.expected),
    };
    assert_eq!(
        read_share_operation(&spec.root, spec.operation_id).unwrap(),
        committed
    );
    assert_eq!(
        read_share_snapshot(&spec.root).unwrap().revisions,
        advanced(spec.expected)
    );
    assert!(crate::section_layout_is_active(&spec.root).unwrap());
    let before = tree(&spec.root);
    let replay = commit_share_transaction(
        &spec.root,
        spec.operation_id,
        &hash,
        ShareRevisions::default(),
        |_| panic!("native replay replanned"),
    )
    .unwrap();
    assert_eq!(replay.revisions, advanced(spec.expected));
    assert!(matches!(
        commit_share_transaction(
            &spec.root,
            Uuid::new_v4(),
            &hash,
            spec.expected,
            |_| panic!("native stale CAS replanned")
        ),
        Err(ConfigStoreError::Conflict { .. })
    ));
    assert!(
        matches!(commit_share_transaction(&spec.root, spec.operation_id, &sha256(b"different-synthetic-envelope"), spec.expected, |_| panic!("native reused ID replanned")), Err(ConfigStoreError::Validation(ref message)) if message == SHARE_OPERATION_ID_REUSED)
    );
    assert_tree_unchanged(&before, &spec.root);
    let ledger = ledger_from_bytes(
        bounded_read(&spec.root.join(OPERATIONS_FILE))
            .unwrap()
            .as_deref(),
    )
    .unwrap();
    assert_eq!(ledger.operations.len(), 1);
    assert!(ledger.operations.contains_key(&spec.operation_id));
    assert!(!spec.root.join(JOURNAL_FILE).exists());
    assert!(!spec
        .root
        .join(format!("{STAGE_PREFIX}{}", spec.operation_id))
        .exists());
    emit("RECOVERED_REPLAY_CAS");
}

#[test]
fn native_share_process_probe() {
    if std::env::var_os(ENABLE).is_none() {
        return;
    }
    let _key = crate::encryption::set_test_encryption_key([0x51; 32]);
    let spec: Spec = serde_json::from_str(&input_line()).unwrap();
    assert!(spec.cut < CUTS.len());
    match spec.role.as_str() {
        "importer" => {
            let root = spec.root.clone();
            test_hooks().lock().unwrap().insert(
                (root, ShareHookPoint::Fault(CUTS[spec.cut])),
                Box::new(|| {
                    emit("AT_CUT");
                    assert_eq!(input_line(), "CRASH");
                    // This runs inside the existing fault branch while migration
                    // and member guards are alive. No return, unwind or Drop.
                    std::process::exit(CRASH_EXIT);
                }),
            );
            let _ = commit_inner(
                &spec.root,
                spec.operation_id,
                &sha256(HASH_INPUT),
                spec.expected,
                |snapshot| Ok(synthetic_candidate(snapshot)),
                CUTS[spec.cut],
            );
            panic!("native crash hook was not reached");
        }
        "writer" => {
            let ordinary = ordinary_provider_section(&spec.root);
            let mut candidate = ordinary.snapshot().data.as_ref().clone();
            candidate.provider = Some("gemini".into());
            crate::ensure_provider_mcp_migration_ready(&spec.root).unwrap();
            emit("WRITER_READY");
            assert_eq!(input_line(), "GO");
            // One assertion, not polling: prove a different process currently
            // owns the OS member lock before entering the supported writer.
            let lock = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(spec.root.join("providers.json.lock"))
                .unwrap();
            assert!(matches!(
                lock.try_lock(),
                Err(std::fs::TryLockError::WouldBlock)
            ));
            drop(lock);
            emit("OS_LOCK_BLOCKED");
            assert!(
                matches!(ordinary.commit(spec.expected.providers, candidate), Err(ConfigStoreError::Validation(ref message)) if message == "configuration-share transaction is pending")
            );
            emit("WRITER_REJECTED_PENDING");
        }
        "recover" => recover_and_check(&spec),
        _ => panic!("unknown native probe role"),
    }
}

fn native_case(root: &Path, cut: usize) {
    let snapshot = read_share_snapshot(root).unwrap();
    let candidate = synthetic_candidate(&snapshot);
    let expected_hashes = vec![
        Some(sha256(&candidate.credentials)),
        Some(sha256(&candidate.providers)),
        Some(sha256(&candidate.env)),
    ];
    let before = hashes(root);
    let mut spec = Spec {
        root: root.into(),
        role: "writer".into(),
        cut,
        operation_id: Uuid::from_u128(0x2118_0000_0000_0000 + cut as u128 + 1),
        expected: snapshot.revisions,
    };
    let mut writer = if CUTS[cut] == ShareFault::AfterManifest {
        let probe = ProbeChild::spawn(&spec);
        probe.expect("WRITER_READY");
        Some(probe)
    } else {
        None
    };
    spec.role = "importer".into();
    let mut importer = ProbeChild::spawn(&spec);
    importer.expect("AT_CUT");
    if let Some(writer) = writer.as_mut() {
        writer.send("GO");
        writer.expect("OS_LOCK_BLOCKED");
        // A supported stale writer cannot complete while importer is alive.
        assert!(matches!(
            writer.frames.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
    }
    importer.send("CRASH");
    importer.finish(CRASH_EXIT);
    if let Some(writer) = writer.as_mut() {
        writer.expect("WRITER_REJECTED_PENDING");
        writer.finish(0);
    }
    let status = read_share_operation(root, spec.operation_id).unwrap();
    if cut < 6 {
        assert_eq!(before, hashes(root));
        assert_eq!(
            status,
            ShareOperationStatus::NotFound {
                operation_id: spec.operation_id
            }
        );
    } else if cut < 11 {
        assert_eq!(
            status,
            ShareOperationStatus::Pending {
                operation_id: spec.operation_id
            }
        );
        assert!(read_share_snapshot(root).is_err());
        assert!(crate::ensure_provider_mcp_migration_ready(root).is_err());
    } else {
        assert_eq!(
            status,
            ShareOperationStatus::Committed {
                operation_id: spec.operation_id,
                revisions: advanced(spec.expected)
            }
        );
    }
    spec.role = "recover".into();
    let mut recovery = ProbeChild::spawn(&spec);
    recovery.expect("RECOVERED_REPLAY_CAS");
    recovery.finish(0);
    assert_eq!(&hashes(root)[..3], expected_hashes.as_slice());
}

#[test]
fn native_process_crash_matrix_recovery_replay_and_cas() {
    for cut in 0..CUTS.len() {
        let dir = fixture();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            native_case(dir.path(), cut)
        }));
        if let Err(error) = result {
            // Child guards already killed/reaped only this case's processes.
            // Retain synthetic evidence rather than delete the failing store.
            eprintln!("native share cut {cut} evidence: {}", dir.keep().display());
            std::panic::resume_unwind(error);
        }
    }
}
