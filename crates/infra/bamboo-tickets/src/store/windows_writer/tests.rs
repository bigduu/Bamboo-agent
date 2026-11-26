mod immutable;
mod token;

use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    mem::size_of,
    os::windows::fs::OpenOptionsExt,
    path::{Component, Path, PathBuf, Prefix},
    process::{self, Child, Command, Stdio},
    ptr, thread,
    time::{Duration, Instant},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use windows_sys::Win32::{
    Foundation::{ERROR_LOCK_VIOLATION, ERROR_SHARING_VIOLATION},
    Storage::FileSystem::{
        GetDriveTypeW, GetVolumeInformationW, DELETE, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    },
    System::SystemInformation::{
        GetNativeSystemInfo, OSVERSIONINFOW, PROCESSOR_ARCHITECTURE_AMD64, SYSTEM_INFO,
    },
};

use super::{entry_identity, FileIdentity, WindowsScope, WRITER_LINK_ERROR};
use crate::{store::FileStore, Error, ScopeBinding};

const SID: &str = "BAMBOO_TICKET_WRITER_EXPECTED_SID";
const FIXTURE: &str = "BAMBOO_TICKET_WRITER_FIXTURE";
const RECEIPT: &str = "BAMBOO_TICKET_WRITER_RECEIPT";
const ROLE: &str = "BAMBOO_TICKET_WRITER_ROLE";
const CASE: &str = "native_standard_user_scope_and_writer";
const TEST: &str = "store::windows_writer::tests::native_standard_user_scope_and_writer";
const WRITER: &str = "writer.lock";
const ALIAS: &str = "writer-alias.lock";
const WITNESS: &[u8] = b"Ticket Rust writer hard-link witness\n";
const REPLY_LIMIT: u64 = 8192;
const WAIT: Duration = Duration::from_secs(15);
const POLL: Duration = Duration::from_millis(20);

#[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
struct Identity {
    volume_serial: String,
    file_id: String,
}

impl From<FileIdentity> for Identity {
    fn from(id: FileIdentity) -> Self {
        Self {
            volume_serial: format!("{:016x}", id.volume_serial),
            file_id: hex::encode(id.file_id),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
struct LockObservation {
    pid: u32,
    scope: Identity,
    file: Identity,
    win32_error: u32,
}

#[derive(Debug, Deserialize, Serialize)]
struct ChildReply {
    phase: String,
    token: token::Token,
    observation: LockObservation,
}

#[test]
#[ignore = "requires the exact-source Windows Ticket standard-user owner"]
fn native_standard_user_scope_and_writer() {
    let expected_sid = env::var(SID).expect("expected ordinary local user's SID");
    let fixture = PathBuf::from(env::var_os(FIXTURE).expect("disposable fixture path"));
    let receipt = PathBuf::from(env::var_os(RECEIPT).expect("probe receipt path"));
    assert!(fixture.is_absolute() && receipt.is_absolute());
    assert_eq!(
        fixture.parent(),
        receipt.parent(),
        "fixture and receipt must be siblings"
    );
    assert_ne!(fixture, receipt);
    assert!(!fixture
        .components()
        .any(|part| matches!(part, Component::ParentDir)));
    assert!(!receipt
        .components()
        .any(|part| matches!(part, Component::ParentDir)));

    let token = token::attest(&expected_sid);
    match env::var(ROLE).as_deref() {
        Ok("owner") => owner(&expected_sid, &fixture, &receipt, token),
        Ok("contender") => contender(&fixture, &receipt, token),
        role => panic!("expected the native probe owner or contender role, got {role:?}"),
    }
}

fn owner(expected_sid: &str, fixture: &Path, receipt: &Path, token: token::Token) {
    let root =
        WindowsScope::open_existing(fixture).expect("open the original plain fixed NTFS fixture");
    let root_id = Identity::from(root.identity());
    let version = windows_version();
    let volume = volume_receipt(fixture, &root_id);
    let gate = verify_public_gate(&root, fixture);

    let writer_path = fixture.join("writer");
    let replies_path = fixture.join("replies");
    fs::create_dir(&writer_path).expect("create writer control scope");
    fs::create_dir(&replies_path).expect("create probe reply directory");
    let scope = WindowsScope::open_existing(&writer_path).expect("retain writer control scope");
    let owned = scope
        .acquire_writer(true)
        .expect("acquire unowned writer lock");
    let owner_observation = LockObservation {
        pid: process::id(),
        scope: scope.identity().into(),
        file: owned.identity().into(),
        win32_error: 0,
    };
    let observed = scope
        .inspect_file(WRITER)
        .expect("inspect literal writer lock");
    assert_eq!(Identity::from(observed.identity), owner_observation.file);
    assert_eq!(observed.link_count, 1);

    // Request DELETE access without using it to distinguish name retention
    // from byte-range exclusivity on the same physical file.
    let writer_file = writer_path.join(WRITER);
    let open_for_delete = || {
        OpenOptions::new()
            .access_mode(DELETE | FILE_READ_ATTRIBUTES)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&writer_file)
    };
    let held_delete_error =
        open_for_delete().expect_err("writer must deny a concurrent DELETE open");
    assert_eq!(
        held_delete_error.raw_os_error(),
        Some(ERROR_SHARING_VIOLATION as i32)
    );

    let mut child = ProbeChild::spawn(expected_sid, &writer_path, &replies_path);
    let held = child.reply(&replies_path, "held");
    assert_contender(
        &held,
        &token,
        child.0.id(),
        &owner_observation,
        ERROR_LOCK_VIOLATION,
    );

    drop(owned);
    let released_delete_file = {
        let file = open_for_delete().expect("released writer must permit a DELETE open");
        Identity::from(
            entry_identity(&file, false).expect("read file identity through the DELETE handle"),
        )
    };
    assert_eq!(released_delete_file, owner_observation.file);
    child.release();
    let released = child.reply(&replies_path, "released");
    assert_contender(&released, &token, child.0.id(), &owner_observation, 0);
    child.finish();

    let final_file = scope
        .inspect_file(WRITER)
        .expect("read retained writer lock after the contender exits");
    assert_eq!(Identity::from(final_file.identity), owner_observation.file);
    assert_eq!(final_file.link_count, 1);
    let hard_link = verify_hard_link(fixture);

    let result = json!({
        "schema": 1,
        "probe": "rust-writer",
        "case": CASE,
        "owner_pid": process::id(),
        "process_architecture": "X64",
        "windows_version": version,
        "token": token,
        "volume": volume,
        "ordered_crash_persistence_proven": false,
        "verdict": "available",
        "file_store_gate": gate,
        "writer": {
            "positive": owner_observation,
            "held": held.observation,
            "released": released.observation,
            "contender_token": held.token,
            "final_file": Identity::from(final_file.identity),
            "lock_name_delete_open": {
                "held_win32_error": held_delete_error.raw_os_error(),
                "released_win32_error": 0,
                "released_file": released_delete_file,
            },
            "hard_link": hard_link,
        },
    });
    write_json(receipt, &result);
}

fn contender(fixture: &Path, replies: &Path, token: token::Token) {
    let scope =
        WindowsScope::open_existing(fixture).expect("contender opens the same retained scope");
    let before = scope
        .inspect_file(WRITER)
        .expect("contender sees the actual writer name");
    assert_eq!(before.link_count, 1);
    let Err(error) = scope.acquire_writer(false) else {
        panic!("blocking owner must exclude contender");
    };
    assert_eq!(error.raw_os_error(), Some(ERROR_LOCK_VIOLATION as i32));
    let mut reply = ChildReply {
        phase: "held".into(),
        token,
        observation: LockObservation {
            pid: process::id(),
            scope: scope.identity().into(),
            file: before.identity.into(),
            win32_error: ERROR_LOCK_VIOLATION,
        },
    };
    publish_child_reply(replies, &reply);

    let (signal, waiting) = std::sync::mpsc::sync_channel(1);
    thread::spawn(move || {
        let mut byte = [0];
        let read = io::stdin().read_exact(&mut byte);
        let _ = signal.send(read.map(|()| byte));
    });
    assert_eq!(
        waiting
            .recv_timeout(WAIT)
            .expect("owner release deadline")
            .expect("owner release byte"),
        [1]
    );

    let guard = scope
        .acquire_writer(false)
        .expect("same contender acquires after owner release");
    let after = scope
        .inspect_file(WRITER)
        .expect("contender reopens literal writer name");
    assert_eq!(after.link_count, 1);
    assert_eq!(Identity::from(after.identity), reply.observation.file);
    assert_eq!(Identity::from(guard.identity()), reply.observation.file);
    reply.phase = "released".into();
    reply.observation.file = guard.identity().into();
    reply.observation.win32_error = 0;
    drop(guard);
    publish_child_reply(replies, &reply);
}

fn assert_contender(
    reply: &ChildReply,
    owner_token: &token::Token,
    child_pid: u32,
    owner: &LockObservation,
    expected_error: u32,
) {
    let observed = &reply.observation;
    assert_eq!(&reply.token, owner_token);
    assert_ne!(observed.pid, owner.pid);
    assert_eq!(observed.pid, child_pid);
    assert_eq!(observed.scope, owner.scope);
    assert_eq!(observed.file, owner.file);
    assert_eq!(observed.win32_error, expected_error);
}

fn verify_public_gate(root: &WindowsScope, fixture: &Path) -> Value {
    let before = entries(fixture);
    assert!(before.is_empty(), "expected a fresh disposable fixture");
    assert!(
        !FileStore::is_platform_supported(),
        "Windows runtime gate unexpectedly enabled"
    );
    let missing = fixture.join("file-store-must-not-create");
    let binding = ScopeBinding {
        scope_id: "windows-writer-native-probe".into(),
        supervisor_session_id: "windows-writer-native-probe".into(),
        binding_revision: 0,
    };
    let mut errors = Vec::new();
    for path in [fixture, missing.as_path()] {
        let error = FileStore::open(path, binding.clone())
            .err()
            .expect("public FileStore must refuse");
        let Error::AuthorityUnavailable(reason) = error else {
            panic!("unexpected Windows FileStore refusal: {error}");
        };
        assert_eq!(reason, "filesystem platform not validated");
        errors.push(reason);
    }
    assert!(matches!(missing.symlink_metadata(), Err(e) if e.kind() == io::ErrorKind::NotFound));
    let after = entries(fixture);
    let reopened = WindowsScope::open_existing(fixture).expect("reopen unchanged empty fixture");
    let original = Identity::from(root.identity());
    assert_eq!(Identity::from(reopened.identity()), original);
    assert_eq!(before, after);
    json!({
        "platform_supported": false,
        "existing_scope_error": errors[0],
        "missing_scope_error": errors[1],
        "missing_scope_remained_absent": true,
        "fixture": original,
        "entries_before": before,
        "entries_after": after,
    })
}

fn verify_hard_link(fixture: &Path) -> Value {
    let path = fixture.join("hardlink");
    fs::create_dir(&path).expect("create separate hard-link control scope");
    let scope = WindowsScope::open_existing(&path).expect("retain the hard-link control scope");
    let original = path.join(WRITER);
    let alias = path.join(ALIAS);
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&original)
        .expect("create the original single-link writer")
        .write_all(WITNESS)
        .expect("write the witness");
    let single = scope
        .inspect_file(WRITER)
        .expect("read single-link native file identity");
    assert_eq!(single.link_count, 1);
    let baseline = scope
        .acquire_writer(false)
        .expect("single-link writer positive control");
    let file_id = Identity::from(baseline.identity());
    assert_eq!(Identity::from(single.identity), file_id);
    drop(baseline);

    fs::hard_link(&original, &alias).expect("create an actual same-volume Windows hard link");
    let before = link_snapshot(&scope, &path, &file_id);

    // The raw lock control makes the validation order observable. A premature
    // LockFileEx would report contention against the same physical lock.
    let open_raw = |path| {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("open the raw lock order control")
    };
    let holder = open_raw(&original);
    FileExt::try_lock_exclusive(&holder).expect("hold the raw writer lock order control");
    let raw_alias = open_raw(&alias);
    let busy = FileExt::try_lock_exclusive(&raw_alias).expect_err("same-file alias contends");
    assert_eq!(busy.raw_os_error(), Some(ERROR_LOCK_VIOLATION as i32));

    let Err(refusal) = scope.acquire_writer(false) else {
        panic!("multi-link writer must be refused");
    };
    assert_eq!(refusal.kind(), io::ErrorKind::InvalidData);
    assert_eq!(refusal.raw_os_error(), None);
    assert_eq!(refusal.to_string(), WRITER_LINK_ERROR);
    FileExt::unlock(&holder).expect("release the raw writer lock order control");
    let after = link_snapshot(&scope, &path, &file_id);
    assert_eq!(before, after);

    json!({
        "scope": Identity::from(scope.identity()),
        "single_link": { "file": file_id, "link_count": single.link_count, "win32_error": 0 },
        "create_hard_link_win32_error": 0,
        "raw_alias_lock_contended_win32_error": busy.raw_os_error(),
        "before": before,
        "after": after,
        "rejection": {
            "kind": "InvalidData",
            "stage": "single-link-validation",
            "reason": refusal.to_string(),
            "raw_os_error": refusal.raw_os_error(),
            "guard_returned": false,
            "lock_attempted": false,
        },
    })
}

fn link_snapshot(scope: &WindowsScope, path: &Path, file_id: &Identity) -> Value {
    let names = entries(path);
    assert_eq!(names, [ALIAS, WRITER]);
    let observe = |name| {
        let native = scope
            .inspect_file(name)
            .expect("inspect actual hard-link name");
        let identity = Identity::from(native.identity);
        assert_eq!(&identity, file_id);
        assert_eq!(native.link_count, 2);
        let bytes =
            read_bounded(&path.join(name), WITNESS.len() as u64).expect("read hard-link witness");
        assert_eq!(bytes, WITNESS);
        json!({ "file": identity, "link_count": native.link_count, "bytes_hex": hex::encode(bytes) })
    };
    json!({ "entries": names, "original": observe(WRITER), "alias": observe(ALIAS) })
}

fn entries(path: &Path) -> Vec<String> {
    let mut names: Vec<_> = fs::read_dir(path)
        .expect("read the owned fixture entries")
        .take(17)
        .map(|entry| {
            entry
                .expect("read fixture entry")
                .file_name()
                .into_string()
                .expect("fixture entry name")
        })
        .collect();
    assert!(names.len() <= 16, "unexpected entries in the owned fixture");
    names.sort_unstable();
    names
}

fn volume_receipt(path: &Path, identity: &Identity) -> Value {
    let Some(Component::Prefix(prefix)) = path.components().next() else {
        panic!("fixture must have a drive prefix");
    };
    let drive = match prefix.kind() {
        Prefix::Disk(letter) | Prefix::VerbatimDisk(letter) => letter,
        _ => panic!("fixture must be on a fixed local drive"),
    };
    let root = [u16::from(drive), u16::from(b':'), u16::from(b'\\'), 0];
    let mut name = [0; 32];
    // SAFETY: The root is terminated; name is a writable buffer.
    let drive_type = unsafe { GetDriveTypeW(root.as_ptr()) };
    let success = unsafe {
        GetVolumeInformationW(
            root.as_ptr(),
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            name.as_mut_ptr(),
            name.len() as u32,
        )
    };
    assert_ne!(
        success,
        0,
        "read fixture filesystem: {}",
        io::Error::last_os_error()
    );
    let end = name
        .iter()
        .position(|part| *part == 0)
        .expect("terminated filesystem name");
    let file_system = String::from_utf16(&name[..end]).expect("filesystem name");
    assert_eq!(drive_type, 3);
    assert_eq!(file_system, "NTFS");
    json!({
        "file_system": file_system,
        "drive_type_code": drive_type,
        "volume_serial": identity.volume_serial,
        "fixture_directory_file_id": identity.file_id,
    })
}

#[link(name = "ntdll")]
unsafe extern "system" {
    fn RtlGetVersion(info: *mut OSVERSIONINFOW) -> i32;
}

fn windows_version() -> String {
    assert_eq!(env::consts::ARCH, "x86_64");
    let mut system = SYSTEM_INFO::default();
    // SAFETY: system is writable; GetNativeSystemInfo initializes the architecture member.
    unsafe { GetNativeSystemInfo(&mut system) };
    assert_eq!(
        unsafe { system.Anonymous.Anonymous.wProcessorArchitecture },
        PROCESSOR_ARCHITECTURE_AMD64
    );
    let mut version = OSVERSIONINFOW {
        dwOSVersionInfoSize: size_of::<OSVERSIONINFOW>() as u32,
        ..OSVERSIONINFOW::default()
    };
    // SAFETY: The version record is writable and carries its required size.
    assert_eq!(unsafe { RtlGetVersion(&mut version) }, 0);
    format!(
        "{}.{}.{}.0",
        version.dwMajorVersion, version.dwMinorVersion, version.dwBuildNumber
    )
}

fn publish_child_reply(path: &Path, reply: &ChildReply) {
    let target = path.join(format!("{}.json", reply.phase));
    let temporary = target.with_extension("pending");
    write_json(&temporary, reply);
    fs::rename(temporary, target).expect("publish one complete bounded contender reply");
}

fn write_json(path: &Path, data: &impl Serialize) {
    let payload = serde_json::to_vec(data).expect("serialize native probe evidence");
    assert!(
        payload.len() as u64 <= REPLY_LIMIT,
        "native probe output exceeded its bound"
    );
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .expect("create native probe receipt")
        .write_all(&payload)
        .expect("write native probe receipt");
}

fn read_bounded(path: &Path, limit: u64) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "native probe read exceeded its bound",
        ));
    }
    Ok(bytes)
}

struct ProbeChild(Child);

impl ProbeChild {
    fn spawn(expected_sid: &str, fixture: &Path, replies: &Path) -> Self {
        let binary = env::current_exe().expect("current exact test binary");
        Self(
            Command::new(binary)
                .args([
                    "--exact",
                    TEST,
                    "--ignored",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(SID, expected_sid)
                .env(FIXTURE, fixture)
                .env(RECEIPT, replies)
                .env(ROLE, "contender")
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .spawn()
                .expect("start native contender from the same exact test binary"),
        )
    }

    fn reply(&mut self, path: &Path, phase: &str) -> ChildReply {
        let reply = path.join(format!("{phase}.json"));
        let until = Instant::now() + WAIT;
        let data = loop {
            match read_bounded(&reply, REPLY_LIMIT) {
                Ok(data) => break data,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => panic!("contender {phase} reply: {error}"),
            }
            if let Some(status) = self.0.try_wait().expect("poll contender") {
                // The child may publish and exit between the file read and poll.
                break read_bounded(&reply, REPLY_LIMIT)
                    .unwrap_or_else(|e| panic!("contender exited {status} before {phase}: {e}"));
            }
            assert!(
                Instant::now() < until,
                "timed out waiting for contender {phase}"
            );
            thread::sleep(POLL);
        };
        let parsed: ChildReply = serde_json::from_slice(&data).expect("complete contender reply");
        assert_eq!(parsed.phase, phase);
        parsed
    }

    fn release(&mut self) {
        self.0
            .stdin
            .take()
            .expect("contender release pipe")
            .write_all(&[1])
            .expect("signal owner release");
    }

    fn finish(&mut self) {
        let until = Instant::now() + WAIT;
        loop {
            if let Some(status) = self.0.try_wait().expect("poll contender exit") {
                assert!(status.success(), "contender failed: {status}");
                return;
            }
            assert!(Instant::now() < until, "timed out awaiting contender exit");
            thread::sleep(POLL);
        }
    }
}

impl Drop for ProbeChild {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
            let until = Instant::now() + Duration::from_secs(3);
            while matches!(self.0.try_wait(), Ok(None)) && Instant::now() < until {
                thread::sleep(POLL);
            }
        }
    }
}
