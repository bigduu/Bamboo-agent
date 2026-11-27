use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::{self, Read},
    os::windows::io::AsRawHandle,
    path::{Component, Path, PathBuf},
    process::{self, Command, ExitStatus, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use serde_json::{json, Value};
use windows_sys::Win32::{
    Foundation::{ERROR_FILE_NOT_FOUND, ERROR_LOCK_VIOLATION},
    System::Threading::TerminateProcess,
};

use super::super::super::{head::HeadReplaceBoundary, WindowsScope};
use super::super::{
    read_bounded, token, ProbeChild, FIXTURE, POLL, RECEIPT, ROLE, SID, WAIT, WRITER,
};
use super::{
    address_from_head, assert_rename, canonical_bytes, entries, graph_samples, lock_observation,
    publish_graph, read_head, rename_json, traverse, verify_public_gate, volume_receipt,
    windows_version, write_json_bounded, Categories, GraphProof, Identity, Observation,
    GRAPH_REPLY_LIMIT, HEAD_LENGTH, HEAD_STAGE,
};

const CASE: &str = "native_standard_user_ticket_graph_process_exit";
const TEST: &str = "store::windows_writer::tests::graph::process_exit::native_standard_user_ticket_graph_process_exit";
const CUT_ENV: &str = "BAMBOO_TICKET_GRAPH_PROCESS_CUT";
const CUT_EXIT_CODE: i32 = 0x4254;
const CHILD_REPLY_LIMIT: u64 = 8192;
const EXIT_WAIT: Duration = Duration::from_secs(5);
const CUT_INPUT_WAIT: Duration = Duration::from_secs(35);
const SELECTED: [&str; 4] = [
    "selected.commit",
    "selected.manifest",
    "selected.header",
    "selected.ticket",
];
const PREVIOUS: [&str; 4] = [
    "previous.commit",
    "previous.manifest",
    "previous.header",
    "previous.ticket",
];
const STAGED: [&str; 4] = [
    "staged.commit",
    "staged.manifest",
    "staged.header",
    "staged.ticket",
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Cut {
    Complete,
    BeforeReplace,
    AfterReplace,
    AfterScopeFlush,
}

impl Cut {
    const ALL: [Self; 4] = [
        Self::Complete,
        Self::BeforeReplace,
        Self::AfterReplace,
        Self::AfterScopeFlush,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::BeforeReplace => "before-head-replace",
            Self::AfterReplace => "after-head-replace",
            Self::AfterScopeFlush => "after-scope-flush",
        }
    }
}

#[test]
#[ignore = "requires the exact-source Windows Ticket standard-user owner"]
fn native_standard_user_ticket_graph_process_exit() {
    let expected_sid = env::var(SID).expect("expected ordinary local user's SID");
    let fixture = PathBuf::from(env::var_os(FIXTURE).expect("disposable fixture path"));
    let receipt = PathBuf::from(env::var_os(RECEIPT).expect("probe receipt path"));
    assert!(
        fixture.is_absolute()
            && receipt.is_absolute()
            && fixture.parent() == receipt.parent()
            && fixture != receipt
            && [&fixture, &receipt].iter().all(|path| !path
                .components()
                .any(|part| matches!(part, Component::ParentDir))),
        "fixture and new receipt must be absolute siblings"
    );
    let token = token::attest(&expected_sid);
    match env::var(ROLE).as_deref() {
        Ok("owner") => coordinate(&expected_sid, &fixture, &receipt, &token),
        Ok("process-writer") => {
            let name = env::var(CUT_ENV).expect("explicit writer cut");
            let cut = Cut::ALL
                .into_iter()
                .find(|case| case.name() == name)
                .expect("known process writer cut");
            write_trial(&fixture, &receipt, &token, cut);
        }
        Ok("process-verifier") => {
            assert!(
                env::var_os(CUT_ENV).is_none(),
                "verifier derives its path from named HEAD"
            );
            verify_trial(&fixture, &receipt, &token);
        }
        other => panic!("unexpected process exit fixture role {other:?}"),
    }
}

fn coordinate(expected_sid: &str, fixture: &Path, receipt: &Path, token: &token::Token) {
    let root = WindowsScope::open_existing(fixture).expect("retain NTFS process fixture root");
    let scope = Identity::from(root.identity());
    let volume = volume_receipt(fixture, &scope);
    let gate = verify_public_gate(&root, fixture);
    let mut cases = BTreeMap::new();
    let mut scope_ids = BTreeSet::new();
    for cut in Cut::ALL {
        let result = run_trial(expected_sid, fixture, token, cut);
        let writer_scope = &result["writer"]["scope"];
        assert_eq!(writer_scope["volume_serial"], scope.volume_serial);
        let scope_id = writer_scope["file_id"]
            .as_str()
            .expect("trial physical scope ID");
        assert_ne!(scope_id, scope.file_id);
        assert!(
            scope_ids.insert(scope_id.to_owned()),
            "each trial must have a distinct physical scope"
        );
        cases.insert(cut.name(), result);
    }
    write_json_bounded(
        receipt,
        &json!({
            "schema": 1, "probe": "rust-graph-process-exit", "case": CASE,
            "owner_pid": process::id(), "token": token, "volume": volume,
            "windows_version": windows_version(), "process_architecture": "X64",
            "ordered_crash_persistence_proven": false, "verdict": "available",
            "file_store_gate": gate, "process_exit": { "qualified": true, "cases": cases },
        }),
        GRAPH_REPLY_LIMIT,
    );
}

fn run_trial(expected_sid: &str, root: &Path, token: &token::Token, cut: Cut) -> Value {
    let name = cut.name();
    let fixture = root.join(name);
    let writer_reply = root.join(format!("{name}.writer.json"));
    let verify_reply = root.join(format!("{name}.verifier.json"));
    fs::create_dir(&fixture).expect("create new scratch scope for this cut");

    let mut writer = spawn(
        expected_sid,
        &fixture,
        &writer_reply,
        "process-writer",
        Some(cut),
    );
    let writer_pid = writer.0.id();
    let writer_frame = read_frame(&mut writer, &writer_reply, Instant::now() + WAIT);
    assert_child(&writer_frame, writer_pid, token);
    assert_eq!(writer_frame["phase"], name);

    let (expected_exit, held, terminated) = if cut == Cut::Complete {
        assert_eq!(writer_frame["state"], "complete");
        (0, None, None)
    } else {
        assert_eq!(writer_frame["state"], "cut_reached");
        assert!(
            writer
                .0
                .try_wait()
                .expect("poll owned cut writer")
                .is_none(),
            "cut writer must still be alive"
        );
        let observed = WindowsScope::open_existing(&fixture).expect("open cut scope independently");
        assert_eq!(
            writer_frame["scope"],
            json!(Identity::from(observed.identity()))
        );
        let named = observed
            .inspect_file(WRITER)
            .expect("inspect exact cut lock name");
        assert_eq!(
            writer_frame["writer"]["file"],
            json!(Identity::from(named.identity))
        );
        assert_eq!(writer_frame["writer"]["link_count"], named.link_count);
        let denied = observed
            .acquire_writer(false)
            .expect_err("cut owner must retain the actual writer lock");
        assert_eq!(denied.raw_os_error(), Some(ERROR_LOCK_VIOLATION as i32));
        // SAFETY: this is the exact retained process handle returned by the owned spawn.
        // The child acknowledged its cut and the independent lock observation was denied.
        let result = unsafe { TerminateProcess(writer.0.as_raw_handle(), CUT_EXIT_CODE as u32) };
        assert_ne!(
            result,
            0,
            "{name}: native termination failed: {}",
            io::Error::last_os_error()
        );
        (CUT_EXIT_CODE, denied.raw_os_error(), Some(0))
    };
    let writer_exit = await_exit(&mut writer, Instant::now() + EXIT_WAIT);
    assert_eq!(
        writer_exit.code(),
        Some(expected_exit),
        "{name}: exact writer exit status"
    );
    // Keep the exited process handle while launching the verifier so the
    // reported writer PID cannot be recycled between these two spawns.
    drop(writer.0.stdin.take());

    let mut verifier = spawn(
        expected_sid,
        &fixture,
        &verify_reply,
        "process-verifier",
        None,
    );
    let verifier_pid = verifier.0.id();
    let limit = Instant::now() + WAIT;
    let verified = read_frame(&mut verifier, &verify_reply, limit);
    let verification_exit = await_exit(&mut verifier, limit);
    assert!(
        verification_exit.success(),
        "{name}: verifier exited {verification_exit}; frame: {verified}"
    );
    assert_child(&verified, verifier_pid, token);
    assert_ne!(
        verifier_pid, writer_pid,
        "{name}: reopen must be a distinct process"
    );
    assert_eq!(verified["scope"], writer_frame["scope"]);
    assert_eq!(verified["writer"], writer_frame["writer"]);
    assert_eq!(verified["lock_attempts"], 1);
    if cut == Cut::BeforeReplace {
        assert_eq!(verified["head"], writer_frame["old_head"]);
        assert_eq!(verified["selected"], writer_frame["initial"]);
        assert_eq!(verified["stage"], writer_frame["head_stage"]);
        assert_eq!(verified["staged_graph"], writer_frame["successor"]);
        assert!(verified["previous"].is_null());
        assert!(verified["stage_absence_win32_error"].is_null());
    } else {
        assert_eq!(verified["head"], writer_frame["head_stage"]);
        assert_eq!(verified["selected"], writer_frame["successor"]);
        assert_eq!(verified["previous"], writer_frame["initial"]);
        assert!(verified["stage"].is_null());
        assert!(verified["staged_graph"].is_null());
        assert_eq!(verified["stage_absence_win32_error"], ERROR_FILE_NOT_FOUND);
    }

    json!({
        "writer": writer_frame, "verifier": verified, "writer_exit_code": writer_exit.code(),
        "held_lock_win32_error": held, "termination_win32_error": terminated,
    })
}

fn write_trial(fixture: &Path, reply: &Path, token: &token::Token, cut: Cut) {
    let scope = WindowsScope::open_existing(fixture).expect("retain plain NTFS trial scope");
    let writer = scope
        .acquire_writer(true)
        .expect("acquire actual trial writer");
    let lock = lock_observation(&scope, &writer);
    assert_eq!(entries(fixture), [WRITER]);
    let categories = Categories::create(fixture, &writer);
    let head = writer
        .open_head_scope()
        .expect("retain writable HEAD scope");
    assert_eq!(head.identity(), scope.identity());

    let [initial, successor] = graph_samples();
    let (initial, _) = publish_graph(&categories, &initial, None, "initial");
    let old_id = head
        .seed_head(initial.commit_hash.as_bytes())
        .expect("seed and synchronize initial HEAD");
    let old_head = read_head(&head);
    assert_eq!(old_head.identity, old_id);
    assert_eq!(address_from_head(&old_head), initial.commit_hash);

    let (successor, _) = publish_graph(
        &categories,
        &successor,
        Some(initial.commit_hash.clone()),
        "successor",
    );
    let staged = head
        .stage(HEAD_STAGE, successor.commit_hash.as_bytes(), true)
        .expect("synchronize the replacement HEAD file");
    let next_head = head
        .observe(HEAD_STAGE, HEAD_LENGTH)
        .expect("observe bound successor HEAD stage");
    assert_eq!(next_head.identity, staged.identity());
    assert_ne!(next_head.identity, old_id);
    assert_eq!(address_from_head(&next_head), successor.commit_hash);
    let reader = head
        .open_reader(true)
        .expect("retain independent compatible old HEAD reader");
    let mut order = vec![
        "open_scope",
        "acquire_writer",
        "initial.ticket.category_flush",
        "initial.header.category_flush",
        "initial.manifest.category_flush",
        "initial.commit.category_flush",
        "initial.head.scope_flush",
        "successor.ticket.category_flush",
        "successor.header.category_flush",
        "successor.manifest.category_flush",
        "successor.commit.category_flush",
        "head.stage_file_flush",
    ];
    let mut frame = json!({
        "pid": process::id(), "token": token, "scope": Identity::from(scope.identity()),
        "writer": super::writer_json(&lock), "phase": cut.name(),
        "initial": compact(&initial), "successor": compact(&successor),
        "old_head": Observation::from(&old_head), "head_stage": Observation::from(&next_head),
        "head_rename": null, "scope_flush_win32_error": null,
    });

    let replaced = head
        .replace_observed(&staged, &reader, |boundary| {
            let reached = match boundary {
                HeadReplaceBoundary::BeforeReplace => Cut::BeforeReplace,
                HeadReplaceBoundary::AfterReplace(raw) => {
                    assert_rename(raw, 65, 3);
                    frame["head_rename"] = rename_json(raw);
                    order.push("head.replace");
                    Cut::AfterReplace
                }
                HeadReplaceBoundary::AfterScopeFlush(raw) => {
                    assert_eq!(frame["head_rename"], rename_json(raw));
                    frame["scope_flush_win32_error"] = json!(0);
                    order.push("head.scope_flush");
                    Cut::AfterScopeFlush
                }
            };
            if cut == reached {
                frame["state"] = json!("cut_reached");
                frame["operation_order"] = json!(&order);
                publish_frame(reply, &frame);
                await_termination(cut);
            }
        })
        .unwrap_or_else(|error| panic!("{}: HEAD replace operation failed: {error}", cut.name()));
    assert_rename(replaced.rename, 65, 3);
    let installed = replaced
        .installation
        .expect("completed HEAD rename")
        .expect("verify completed HEAD and scope barrier");
    // A cut must terminate in the hook. No cut may silently fall through.
    assert!(
        cut == Cut::Complete,
        "{}: expected cut callback",
        cut.name()
    );
    assert_eq!(installed.old.identity, old_id);
    assert_eq!(installed.old.bytes, old_head.bytes);
    assert_eq!(installed.scope_flush_win32_error, 0);
    assert_eq!(
        installed.stage_absence_win32_error,
        ERROR_FILE_NOT_FOUND as i32
    );
    let observed = read_head(&head);
    assert_eq!(
        Observation::from(&observed),
        Observation::from(&installed.named)
    );
    assert_eq!(Observation::from(&observed), Observation::from(&next_head));
    order.push("head.read_named");
    frame["state"] = json!("complete");
    frame["operation_order"] = json!(order);
    publish_frame(reply, &frame);
}

fn verify_trial(fixture: &Path, reply: &Path, token: &token::Token) {
    let scope = WindowsScope::open_existing(fixture).expect("fresh readback scope");
    let named_lock = scope
        .inspect_file(WRITER)
        .expect("observe existing named lock before reacquisition");
    let writer = scope.acquire_writer(false).unwrap_or_else(|error| {
        publish_frame(reply, &json!({
            "pid": process::id(), "token": token, "scope": Identity::from(scope.identity()),
            "writer": { "file": Identity::from(named_lock.identity), "link_count": named_lock.link_count,
                "win32_error": error.raw_os_error() },
            "lock_attempts": 1, "operation_order": ["open_scope"], "failure": "acquire_writer",
        }));
        panic!("verifier first acquisition of the existing lock failed: {error}");
    });
    assert_eq!(named_lock.identity, writer.identity());
    let acquired = lock_observation(&scope, &writer);
    let head = writer.open_head_scope().expect("fresh writable HEAD scope");
    let named = read_head(&head);
    let mut order = vec!["open_scope", "acquire_writer", "read_head"];
    let categories = Categories::open(&writer);
    let selected = read_graph(&categories, address_from_head(&named), &mut order, SELECTED);

    let mut previous = None;
    let mut stage = None;
    let mut staged_graph = None;
    let mut absent = None;
    if selected.sequence == 1 {
        let staged = head
            .observe(HEAD_STAGE, HEAD_LENGTH)
            .expect("observe retained stage independently");
        order.push("read_stage");
        // This second graph is inspected as a stage witness. Named HEAD selected the old graph.
        let candidate = read_graph(&categories, address_from_head(&staged), &mut order, STAGED);
        assert_eq!(candidate.sequence, 2);
        assert_eq!(
            candidate.parent.as_deref(),
            Some(selected.commit_hash.as_str())
        );
        staged_graph = Some(compact(&candidate));
        stage = Some(Observation::from(&staged));
    } else {
        let parent = read_graph(
            &categories,
            selected
                .parent
                .as_deref()
                .expect("selected commit links its predecessor"),
            &mut order,
            PREVIOUS,
        );
        assert_eq!(parent.sequence, 1);
        previous = Some(compact(&parent));
        let missing = head
            .observe(HEAD_STAGE, HEAD_LENGTH)
            .expect_err("post-rename stage name must be absent");
        assert_eq!(missing.raw_os_error(), Some(ERROR_FILE_NOT_FOUND as i32));
        order.push("stage_absent");
        absent = missing.raw_os_error();
    }
    publish_frame(
        reply,
        &json!({
            "pid": process::id(), "token": token, "scope": Identity::from(scope.identity()),
            "writer": super::writer_json(&acquired), "lock_attempts": 1,
            "head": Observation::from(&named), "selected": compact(&selected), "previous": previous,
            "stage": stage, "staged_graph": staged_graph, "stage_absence_win32_error": absent,
            "operation_order": order,
        }),
    );
}

fn read_graph(
    categories: &Categories<'_, '_>,
    address: &str,
    order: &mut Vec<&'static str>,
    phases: [&'static str; 4],
) -> GraphProof {
    let (proof, reconstructed) = traverse(categories, address, order, phases)
        .expect("follow disk-selected canonical commit, manifest and objects");
    let expected = graph_samples();
    let expected = match proof.sequence {
        1 => &expected[0],
        2 => &expected[1],
        other => panic!("unexpected reconstructed sequence {other}"),
    };
    assert_eq!(reconstructed, canonical_bytes(expected).unwrap());
    proof
}

fn compact(proof: &GraphProof) -> Value {
    let objects: BTreeMap<_, _> = proof
        .objects
        .iter()
        .map(|(role, object)| {
            (
                *role,
                json!({ "address": object.address, "identity": object.observed.identity,
            "link_count": object.observed.link_count }),
            )
        })
        .collect();
    json!({
        "schema": proof.schema, "sequence": proof.sequence, "binding": proof.binding,
        "parent": proof.parent, "commit_hash": proof.commit_hash, "manifest_hash": proof.manifest_hash,
        "snapshot_sha256": proof.snapshot_sha256, "objects": objects,
    })
}

fn spawn(
    expected_sid: &str,
    fixture: &Path,
    reply: &Path,
    role: &str,
    cut: Option<Cut>,
) -> ProbeChild {
    let executable = env::current_exe().expect("same staged exact test executable");
    let mut command = Command::new(executable);
    command.env_clear();
    for key in ["SystemRoot", "WINDIR", "PATH", "TEMP", "TMP"] {
        if let Some(value) = env::var_os(key) {
            command.env(key, value);
        }
    }
    if let Some(cut) = cut {
        command.env(CUT_ENV, cut.name());
    }
    ProbeChild(
        command
            .args([
                "--exact",
                TEST,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(SID, expected_sid)
            .env(FIXTURE, fixture)
            .env(RECEIPT, reply)
            .env(ROLE, role)
            .stdin(if cut.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::null())
            .spawn()
            .unwrap_or_else(|error| panic!("spawn exact {role} child: {error}")),
    )
}

fn await_termination(cut: Cut) -> ! {
    // Parent keeps stdin open during the cutoff. A byte, EOF or expired
    // observation window fails the trial without the reserved exit code.
    let (send, receive) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let input = io::stdin().lock().read_exact(&mut [0]);
        let _ = send.send(input);
    });
    let result = receive.recv_timeout(CUT_INPUT_WAIT);
    panic!(
        "{}: cut wait ended before native termination: {result:?}",
        cut.name()
    );
}

fn publish_frame(path: &Path, frame: &Value) {
    let pending = path.with_extension("pending");
    write_json_bounded(&pending, frame, CHILD_REPLY_LIMIT);
    fs::rename(pending, path).expect("publish bounded complete child frame atomically");
}

fn read_frame(child: &mut ProbeChild, path: &Path, deadline: Instant) -> Value {
    let pid = child.0.id();
    let data = loop {
        match read_bounded(path, CHILD_REPLY_LIMIT) {
            Ok(data) => break data,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => panic!("process {pid}: read complete frame {path:?}: {error}"),
        }
        if let Some(status) = child.0.try_wait().expect("poll owned child for frame") {
            break read_bounded(path, CHILD_REPLY_LIMIT).unwrap_or_else(|error| {
                panic!("process {pid} exited {status} before frame {path:?}: {error}")
            });
        }
        assert!(
            Instant::now() < deadline,
            "process {pid}: frame {path:?} timed out"
        );
        thread::sleep(POLL);
    };
    serde_json::from_slice(&data).expect("read bounded child JSON")
}

fn await_exit(child: &mut ProbeChild, deadline: Instant) -> ExitStatus {
    loop {
        if let Some(status) = child.0.try_wait().expect("poll owned child exit") {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "process {}: exit deadline",
            child.0.id()
        );
        thread::sleep(POLL);
    }
}

fn assert_child(frame: &Value, pid: u32, token: &token::Token) {
    assert_ne!(pid, process::id());
    assert_eq!(frame["pid"], pid);
    assert_eq!(frame["token"], json!(token));
    assert_eq!(frame["writer"]["win32_error"], 0);
    assert_eq!(frame["writer"]["link_count"], 1);
    assert_eq!(
        frame["scope"]["volume_serial"],
        frame["writer"]["file"]["volume_serial"]
    );
}
