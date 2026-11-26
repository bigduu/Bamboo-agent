use std::{
    env, fs, io,
    path::{Component, PathBuf},
    process,
};

use serde::Serialize;
use serde_json::{json, Value};
use windows_sys::Win32::Foundation::{
    ERROR_ALREADY_EXISTS, ERROR_FILE_NOT_FOUND, STATUS_OBJECT_NAME_COLLISION,
};

use super::super::{
    immutable::{ImmutableCategory, ObjectObservation, RenameReceipt, StagedObject},
    FileIdentity, WindowsScope,
};
use super::{
    entries, token, verify_public_gate, volume_receipt, windows_version, write_json, Identity,
    FIXTURE, RECEIPT, ROLE, SID, WRITER,
};
use crate::content_hash;

const CASE: &str = "native_standard_user_immutable_publication";
const CATEGORY: &str = "objects";
const INSERT_STAGE: &str = "insert-stage";
const COLLISION_STAGE: &str = "collision-stage";
const OBJECT: &[u8] = b"immutable Rust Ticket object\n";
const REQUIRED: &[u8] = b"required Rust Ticket object\n";
const CORRUPT: &[u8] = b"damaged Rust Ticket object!\n";

#[derive(Debug, PartialEq, Eq, Serialize)]
struct Observed {
    identity: Identity,
    link_count: u32,
    bytes_hex: String,
}

impl Observed {
    fn exact(observation: ObjectObservation, expected: FileIdentity, bytes: &[u8]) -> Self {
        let identity = observation.identity.into();
        assert_eq!(
            identity,
            Identity::from(expected),
            "immutable file identity changed"
        );
        assert_eq!(
            observation.link_count, 1,
            "immutable file lost single-link identity"
        );
        assert_eq!(observation.bytes, bytes, "immutable file bytes differ");
        Self {
            identity,
            link_count: observation.link_count,
            bytes_hex: hex::encode(observation.bytes),
        }
    }
}

#[test]
#[ignore = "requires the exact-source Windows Ticket standard-user owner"]
fn native_standard_user_immutable_publication() {
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
    let scope = WindowsScope::open_existing(&fixture).expect("retain the plain fixed NTFS fixture");
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

    let category_path = fixture.join(CATEGORY);
    fs::create_dir(&category_path).expect("create the owned immutable category");
    let category = writer
        .open_category(CATEGORY)
        .expect("retain writable category from the writer");
    let category_id = category.identity();
    assert_eq!(category_id.volume_serial, scope_id.volume_serial);
    assert_ne!(Identity::from(category_id), Identity::from(scope_id));
    assert!(entries(&category_path).is_empty());

    let digest = content_hash(OBJECT);
    let conflict_digest = content_hash(REQUIRED);
    assert_ne!(digest, conflict_digest);
    let (insert, original_id) = insert(&category, &digest);
    let (collision, mismatched_stage) =
        collision(&category, &digest, original_id, &conflict_digest);
    let reuse = reuse(&category, &digest, original_id);
    let conflict = conflict(&category, &conflict_digest);

    let category_entries = entries(&category_path);
    let mut expected_entries = [digest, conflict_digest, COLLISION_STAGE.into()];
    expected_entries.sort_unstable();
    assert_eq!(category_entries, expected_entries);
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
            "probe": "rust-immutable",
            "case": CASE,
            "owner_pid": process::id(),
            "process_architecture": "X64",
            "windows_version": version,
            "token": token,
            "volume": volume,
            "ordered_crash_persistence_proven": false,
            "verdict": "available",
            "file_store_gate": gate,
            "immutable": {
                "scope": Identity::from(scope_id),
                "writer": { "file": Identity::from(writer_id), "link_count": initial_lock.link_count, "win32_error": 0 },
                "category": Identity::from(category_id),
                "writer_final_file": Identity::from(final_lock.identity),
                "category_entries": category_entries,
                "insert": insert,
                "collision": collision,
            "mismatched_stage": mismatched_stage,
                "reuse": reuse,
                "conflict": conflict,
            },
        }),
    );
}

fn insert(category: &ImmutableCategory<'_, '_>, digest: &str) -> (Value, FileIdentity) {
    let stage = category
        .stage(INSERT_STAGE, OBJECT)
        .expect("exclusively create and sync insert stage");
    let id = stage.identity();
    assert_eq!(id.volume_serial, category.identity().volume_serial);
    let before = observe(category, INSERT_STAGE, id, OBJECT);

    let publication = category
        .publish_no_replace(&stage, digest)
        .expect("publish held insert stage");
    assert_eq!(publication.rename.ntstatus, 0);
    assert_eq!(publication.rename.win32_error, 0);
    let installed = publication
        .installation
        .expect("successful rename installs the object")
        .expect("published identity, source absence and category flush");
    let published = Observed::exact(installed.named, id, OBJECT);
    assert_eq!(published, before);
    assert_eq!(
        installed.stage_absence_win32_error,
        ERROR_FILE_NOT_FOUND as i32
    );
    assert_eq!(installed.category_flush_win32_error, 0);

    assert_eq!(observe(category, digest, id, OBJECT), published);
    assert_eq!(
        absence_win32(category, INSERT_STAGE),
        installed.stage_absence_win32_error
    );

    let event = json!({
        "digest": digest,
        "stage_name": INSERT_STAGE,
        "stage_before": before,
        "published": published,
        "rename": rename(&publication.rename),
        "stage_absence_win32_error": installed.stage_absence_win32_error,
        "category_flush_win32_error": installed.category_flush_win32_error,
        "operation_order": ["exclusive_stage_create", "stage_write_all", "stage_sync", "observe_stage",
            "rename_no_replace", "observe_published", "check_stage_absence", "category_flush",
            "reopen_published", "recheck_stage_absence"],
    });
    (event, id)
}

fn collision(
    category: &ImmutableCategory<'_, '_>,
    digest: &str,
    original: FileIdentity,
    mismatched_digest: &str,
) -> (Value, Value) {
    let stage = category
        .stage(COLLISION_STAGE, OBJECT)
        .expect("create a distinct synchronized stage for the same digest");
    let id = stage.identity();
    assert_eq!(id.volume_serial, category.identity().volume_serial);
    assert_ne!(Identity::from(id), Identity::from(original));
    let mismatch = mismatched_stage(category, &stage, mismatched_digest);
    let original_before = observe(category, digest, original, OBJECT);
    let staged_before = observe(category, COLLISION_STAGE, id, OBJECT);

    let attempt = category
        .publish_no_replace(&stage, digest)
        .expect("observe native create-only collision");
    assert_eq!(attempt.rename.ntstatus, STATUS_OBJECT_NAME_COLLISION);
    assert_eq!(attempt.rename.win32_error, ERROR_ALREADY_EXISTS as i32);
    assert!(
        attempt.installation.is_none(),
        "collision entered publication success path"
    );
    let original_after = observe(category, digest, original, OBJECT);
    let staged_after = observe(category, COLLISION_STAGE, id, OBJECT);
    assert_eq!(original_before, original_after);
    assert_eq!(staged_before, staged_after);

    let event = json!({
        "digest": digest,
        "stage_name": COLLISION_STAGE,
        "original_before": original_before,
        "staged_before": staged_before,
        "original_after": original_after,
        "staged_after": staged_after,
        "rename": rename(&attempt.rename),
        "category_flush_win32_error": null,
        "operation_order": ["exclusive_stage_create", "stage_write_all", "stage_sync", "check_mismatched_digest",
            "observe_original_before", "observe_stage_before", "rename_no_replace",
            "observe_original_after", "observe_stage_after"],
    });
    (event, mismatch)
}

fn mismatched_stage(
    category: &ImmutableCategory<'_, '_>,
    stage: &StagedObject,
    digest: &str,
) -> Value {
    let id = stage.identity();
    let before = observe(category, COLLISION_STAGE, id, OBJECT);
    let target_before = absence_win32(category, digest);
    let Err(error) = category.publish_no_replace(stage, digest) else {
        panic!("a stage with a different digest must be refused before the native rename");
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.raw_os_error(), None);
    assert_eq!(
        error.to_string(),
        "immutable digest does not match staged bytes"
    );
    let target_after = absence_win32(category, digest);
    let canonical_alias = digest.to_ascii_uppercase();
    let Err(alias_error) = category.stage(&canonical_alias, OBJECT) else {
        panic!("a case-insensitive canonical digest alias must be refused as a stage name");
    };
    assert_eq!(alias_error.kind(), io::ErrorKind::InvalidInput);
    let canonical_alias_absence = absence_win32(category, &canonical_alias);
    let after = observe(category, COLLISION_STAGE, id, OBJECT);
    assert_eq!(before, after);

    json!({
        "digest": digest,
        "stage_name": COLLISION_STAGE,
        "before": before,
        "after": after,
        "target_absence_before_win32_error": target_before,
        "target_absence_after_win32_error": target_after,
        "error_kind": "InvalidData",
        "reason": error.to_string(),
        "raw_os_error": error.raw_os_error(),
        "category_flush_win32_error": null,
        "canonical_alias_stage_error_kind": "InvalidInput",
        "canonical_alias_absence_win32_error": canonical_alias_absence,
        "operation_order": ["observe_stage_before", "check_target_absence_before", "reject_mismatched_digest",
            "check_target_absence_after", "reject_canonical_alias_stage", "check_canonical_alias_absence",
            "observe_stage_after"],
    })
}

fn reuse(category: &ImmutableCategory<'_, '_>, digest: &str, original: FileIdentity) -> Value {
    let reuse = category
        .reuse(digest, OBJECT)
        .expect("validate and reuse exact existing bytes");
    let observed = Observed::exact(reuse.observed, original, OBJECT);
    assert!(reuse.matched);
    assert_eq!(reuse.category_flush_win32_error, Some(0));
    let after = observe(category, digest, original, OBJECT);
    assert_eq!(observed, after);

    json!({
        "digest": digest,
        "observed": observed,
        "after": after,
        "matched": reuse.matched,
        "category_flush_win32_error": reuse.category_flush_win32_error,
        "operation_order": ["observe_existing", "validate_exact_bytes", "category_flush", "reopen_existing"],
    })
}

fn conflict(category: &ImmutableCategory<'_, '_>, digest: &str) -> Value {
    assert_ne!(content_hash(CORRUPT), digest);
    let id = category
        .seed_conflicting_fixture(digest, CORRUPT)
        .expect("seed actual wrong bytes at the expected digest name");
    assert_eq!(id.volume_serial, category.identity().volume_serial);
    let before = observe(category, digest, id, CORRUPT);

    let attempt = category
        .reuse(digest, REQUIRED)
        .expect("read the actual conflicting bytes");
    let observed = Observed::exact(attempt.observed, id, CORRUPT);
    assert!(!attempt.matched);
    assert_eq!(attempt.category_flush_win32_error, None);
    let after = observe(category, digest, id, CORRUPT);
    assert_eq!(before, observed);
    assert_eq!(observed, after);

    json!({
        "digest": digest,
        "expected_bytes_hex": hex::encode(REQUIRED),
        "before": before,
        "observed": observed,
        "after": after,
        "matched": attempt.matched,
        "category_flush_win32_error": attempt.category_flush_win32_error,
        "operation_order": ["seed_existing_wrong_bytes", "observe_before", "observe_existing",
            "reject_different_bytes", "reopen_existing"],
    })
}

fn observe(
    category: &ImmutableCategory<'_, '_>,
    name: &str,
    id: FileIdentity,
    bytes: &[u8],
) -> Observed {
    Observed::exact(
        category
            .observe(name, bytes.len())
            .expect("read exact named file"),
        id,
        bytes,
    )
}

fn absence_win32(category: &ImmutableCategory<'_, '_>, name: &str) -> i32 {
    let Err(error) = category.observe(name, 0) else {
        panic!("{name} must be absent");
    };
    let raw = error.raw_os_error();
    assert_eq!(raw, Some(ERROR_FILE_NOT_FOUND as i32), "{name}: {error}");
    raw.expect("checked native absence")
}

fn rename(result: &RenameReceipt) -> Value {
    json!({
        "ntstatus": format!("{:08x}", result.ntstatus as u32),
        "win32_error": result.win32_error,
    })
}
