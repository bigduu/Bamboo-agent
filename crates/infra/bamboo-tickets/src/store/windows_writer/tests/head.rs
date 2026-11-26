use std::{
    env,
    path::{Component, PathBuf},
    process,
};

use serde::Serialize;
use serde_json::{json, Value};
use windows_sys::Win32::Foundation::{
    ERROR_ACCESS_DENIED, ERROR_FILE_NOT_FOUND, ERROR_SHARING_VIOLATION, STATUS_ACCESS_DENIED,
    STATUS_SHARING_VIOLATION,
};

use super::super::{
    head::{HeadReader, HeadScope, HeldHeadObservation},
    immutable::{ObjectObservation, RenameReceipt, StagedObject},
    FileIdentity, WindowsScope,
};
use super::{
    entries, token, verify_public_gate, volume_receipt, windows_version, write_json, Identity,
    FIXTURE, RECEIPT, ROLE, SID, WRITER,
};

const CASE: &str = "native_standard_user_head_replacement";
const HEAD: &str = "HEAD";
const OLD: &[u8; 64] = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const NEW: &[u8; 64] = b"fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
const NO_DELETE: &str = "head-no-delete-stage";
const NO_READER_SHARE: &str = "head-no-reader-share-stage";
const POSITIVE_STAGE: &str = "head-replace-stage";

#[derive(Debug, PartialEq, Eq, Serialize)]
struct Held {
    identity: Identity,
    bytes_hex: String,
}

impl Held {
    fn exact(observed: HeldHeadObservation, id: FileIdentity, bytes: &[u8]) -> Self {
        assert_eq!(Identity::from(observed.identity), Identity::from(id));
        assert_eq!(observed.bytes, bytes, "held HEAD bytes changed");
        Self {
            identity: observed.identity.into(),
            bytes_hex: hex::encode(observed.bytes),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct Named {
    identity: Identity,
    link_count: u32,
    bytes_hex: String,
}

impl Named {
    fn exact(observed: ObjectObservation, id: FileIdentity, bytes: &[u8]) -> Self {
        assert_eq!(Identity::from(observed.identity), Identity::from(id));
        assert_eq!(
            observed.link_count, 1,
            "named HEAD or stage must retain one link"
        );
        assert_eq!(observed.bytes, bytes, "named HEAD or stage bytes changed");
        Self {
            identity: observed.identity.into(),
            link_count: observed.link_count,
            bytes_hex: hex::encode(observed.bytes),
        }
    }
}

#[test]
#[ignore = "requires the exact-source Windows Ticket standard-user owner"]
fn native_standard_user_head_replacement() {
    assert_eq!(env::var(ROLE).as_deref(), Ok("owner"));
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
    let scope = WindowsScope::open_existing(&fixture).expect("retain the plain fixed NTFS scope");
    let scope_id = scope.identity();
    let version = windows_version();
    let volume = volume_receipt(&fixture, &scope_id.into());
    let gate = verify_public_gate(&scope, &fixture);

    let writer = scope
        .acquire_writer(true)
        .expect("acquire the actual Ticket writer");
    let writer_id = writer.identity();
    assert_eq!(writer_id.volume_serial, scope_id.volume_serial);
    let initial_lock = scope
        .inspect_file(WRITER)
        .expect("inspect exact writer name");
    assert_eq!(
        Identity::from(initial_lock.identity),
        Identity::from(writer_id)
    );
    assert_eq!(initial_lock.link_count, 1);

    let head = writer
        .open_head_scope()
        .expect("retain writable scope under the writer");
    assert_eq!(Identity::from(head.identity()), Identity::from(scope_id));
    assert_eq!(entries(&fixture), [WRITER]);
    let old_id = head
        .seed_head(OLD)
        .expect("create and synchronize scratch HEAD");
    assert_eq!(old_id.volume_serial, scope_id.volume_serial);
    assert_ne!(Identity::from(old_id), Identity::from(writer_id));
    let initial = json!({
        "named": observe(&head, HEAD, old_id, OLD),
        "scope_flush_win32_error": 0,
        "operation_order": ["exclusive_seed_head", "write_old_head", "sync_old_head",
            "verify_seeded_head", "scope_flush", "reopen_named_head"],
    });

    let (no_source_delete, no_delete_id) = denied(
        &head,
        old_id,
        NO_DELETE,
        false,
        true,
        STATUS_ACCESS_DENIED,
        ERROR_ACCESS_DENIED,
    );
    let (no_reader_delete_share, no_share_id) = denied(
        &head,
        old_id,
        NO_READER_SHARE,
        true,
        false,
        STATUS_SHARING_VIOLATION,
        ERROR_SHARING_VIOLATION,
    );
    let (replace, replacement_id) = replace(&head, old_id);
    assert_ne!(Identity::from(no_delete_id), Identity::from(no_share_id));
    assert_ne!(Identity::from(replacement_id), Identity::from(no_delete_id));
    assert_ne!(Identity::from(replacement_id), Identity::from(no_share_id));
    // Negative-control stages remain intact even after the separate positive replacement.
    observe(&head, NO_DELETE, no_delete_id, NEW);
    observe(&head, NO_READER_SHARE, no_share_id, NEW);
    let entries_after = entries(&fixture);
    assert_eq!(entries_after, [HEAD, NO_DELETE, NO_READER_SHARE, WRITER]);

    let final_lock = scope
        .inspect_file(WRITER)
        .expect("reopen the same held writer name");
    assert_eq!(
        Identity::from(final_lock.identity),
        Identity::from(writer_id)
    );
    assert_eq!(final_lock.link_count, 1);

    write_json(
        &receipt,
        &json!({
            "schema": 1,
            "probe": "rust-head",
            "case": CASE,
            "owner_pid": process::id(),
            "process_architecture": "X64",
            "windows_version": version,
            "token": token,
            "volume": volume,
            "ordered_crash_persistence_proven": false,
            "verdict": "available",
            "file_store_gate": gate,
            "head": {
                "scope": Identity::from(scope_id),
                "writer": { "file": Identity::from(writer_id), "link_count": initial_lock.link_count, "win32_error": 0 },
                "writer_final_file": Identity::from(final_lock.identity),
                "entries_after": entries_after,
                "initial": initial,
                "no_source_delete": no_source_delete,
                "no_reader_delete_share": no_reader_delete_share,
                "replace": replace,
            },
        }),
    );
}

struct Prepared {
    stage: StagedObject,
    reader: HeadReader,
    old_reader: Held,
    named: Named,
    staged: Named,
}

fn prepare(
    head: &HeadScope<'_, '_>,
    old: FileIdentity,
    stage_name: &str,
    source_delete_access: bool,
    reader_delete_share: bool,
) -> Prepared {
    let stage = head
        .stage(stage_name, NEW, source_delete_access)
        .expect("exclusively create, write and synchronize the HEAD stage");
    let new = stage.identity();
    assert_eq!(new.volume_serial, head.identity().volume_serial);
    assert_ne!(Identity::from(new), Identity::from(old));

    let reader = head
        .open_reader(reader_delete_share)
        .expect("open independent old HEAD reader");
    let old_reader = Held::exact(
        reader.observe(OLD.len()).expect("read held old HEAD"),
        old,
        OLD,
    );
    let named = observe(head, HEAD, old, OLD);
    let staged = observe(head, stage_name, new, NEW);
    Prepared {
        stage,
        reader,
        old_reader,
        named,
        staged,
    }
}

fn denied(
    head: &HeadScope<'_, '_>,
    old: FileIdentity,
    stage_name: &str,
    source_delete_access: bool,
    reader_delete_share: bool,
    expected_status: i32,
    expected_win32: u32,
) -> (Value, FileIdentity) {
    let case = prepare(
        head,
        old,
        stage_name,
        source_delete_access,
        reader_delete_share,
    );
    let replacement = head
        .replace(&case.stage, &case.reader)
        .expect("reach the actual native HEAD rename");
    assert_eq!(replacement.rename.ntstatus, expected_status);
    assert_eq!(replacement.rename.win32_error, expected_win32 as i32);
    assert!(
        replacement.installation.is_none(),
        "denied rename must not enter the success path"
    );

    let old_after = Held::exact(
        case.reader
            .observe(OLD.len())
            .expect("reread the held old HEAD"),
        old,
        OLD,
    );
    let named_after = observe(head, HEAD, old, OLD);
    let new_id = case.stage.identity();
    let staged_after = observe(head, stage_name, new_id, NEW);
    assert_eq!(old_after, case.old_reader);
    assert_eq!(named_after, case.named);
    assert_eq!(staged_after, case.staged);
    let event = json!({
        "stage_name": stage_name,
        "source_delete_access": source_delete_access,
        "reader_delete_share": reader_delete_share,
        "old_reader_before": case.old_reader,
        "old_reader_after": old_after,
        "named_before": case.named,
        "named_after": named_after,
        "staged_before": case.staged,
        "staged_after": staged_after,
        "rename": rename(&replacement.rename),
        "scope_flush_win32_error": null,
        "operation_order": ["exclusive_stage_create", "stage_write_all", "stage_sync", "open_old_reader",
            "observe_held_old", "observe_named_old", "observe_stage", "revalidate_held_old",
            "revalidate_named_old", "revalidate_stage", "native_posix_replace",
            "reread_held_old", "reopen_named_old", "reopen_stage"],
    });
    (event, new_id)
}

fn replace(head: &HeadScope<'_, '_>, old: FileIdentity) -> (Value, FileIdentity) {
    let case = prepare(head, old, POSITIVE_STAGE, true, true);
    let replacement = head
        .replace(&case.stage, &case.reader)
        .expect("reach actual native POSIX replacement");
    assert_eq!(replacement.rename.ntstatus, 0);
    assert_eq!(replacement.rename.win32_error, 0);
    let installed = replacement
        .installation
        .expect("successful native rename installs HEAD")
        .expect("scope barrier and exact held, named and absent-source evidence");
    let new_id = case.stage.identity();
    let old_after = Held::exact(installed.old, old, OLD);
    let named_after = Named::exact(installed.named, new_id, NEW);
    assert_eq!(old_after, case.old_reader);
    assert_eq!(named_after, case.staged);
    assert_eq!(installed.scope_flush_win32_error, 0);
    assert_eq!(
        installed.stage_absence_win32_error,
        ERROR_FILE_NOT_FOUND as i32
    );

    let reopened_old = Held::exact(
        case.reader
            .observe(OLD.len())
            .expect("read the same unlinked old handle"),
        old,
        OLD,
    );
    assert_eq!(reopened_old, old_after);
    assert_eq!(observe(head, HEAD, new_id, NEW), named_after);
    let Err(missing) = head.observe(POSITIVE_STAGE, 0) else {
        panic!("the former stage name must be absent");
    };
    assert_eq!(
        missing.raw_os_error(),
        Some(installed.stage_absence_win32_error)
    );
    let event = json!({
        "stage_name": POSITIVE_STAGE,
        "source_delete_access": true,
        "reader_delete_share": true,
        "old_reader_before": case.old_reader,
        "old_reader_after": old_after,
        "named_before": case.named,
        "named_after": named_after,
        "staged_before": case.staged,
        "rename": rename(&replacement.rename),
        "scope_flush_win32_error": installed.scope_flush_win32_error,
        "stage_absence_win32_error": installed.stage_absence_win32_error,
        "operation_order": ["exclusive_stage_create", "stage_write_all", "stage_sync", "open_old_reader",
            "observe_held_old", "observe_named_old", "observe_stage", "revalidate_held_old",
            "revalidate_named_old", "revalidate_stage", "native_posix_replace", "scope_flush",
            "reread_held_old", "observe_named_new", "check_stage_absence",
            "recheck_held_old", "reopen_named_new", "recheck_stage_absence"],
    });
    (event, new_id)
}

fn observe(head: &HeadScope<'_, '_>, name: &str, id: FileIdentity, bytes: &[u8]) -> Named {
    Named::exact(
        head.observe(name, bytes.len())
            .expect("fresh relative named open"),
        id,
        bytes,
    )
}

fn rename(result: &RenameReceipt) -> Value {
    assert_eq!(result.information_class, 65);
    assert_eq!(result.flags, 3);
    json!({
        "ntstatus": format!("{:08x}", result.ntstatus as u32),
        "win32_error": result.win32_error,
        "information_class": result.information_class,
        "flags": result.flags,
    })
}
