mod process_exit;

use std::{
    collections::BTreeMap,
    env,
    ffi::OsStr,
    fs,
    io::{Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    process,
};

use serde::Serialize;
use serde_json::{json, Map, Value};
use windows_sys::{
    Wdk::Storage::FileSystem::FILE_OPEN,
    Win32::{
        Foundation::ERROR_FILE_NOT_FOUND,
        Storage::FileSystem::{FILE_GENERIC_WRITE, FILE_READ_ATTRIBUTES},
    },
};

use super::super::{
    entry_identity,
    head::HeadScope,
    immutable::{
        observe_regular, read_bounded, ImmutableCategory, ObjectObservation, RenameReceipt,
    },
    open_relative, WindowsScope, WriterGuard, DIRECTORY_ACCESS,
};
use super::{
    entries, token, verify_public_gate, volume_receipt, windows_version, write_json_bounded,
    Identity, FIXTURE, RECEIPT, ROLE, SID, WRITER,
};
use crate::{
    canonical_bytes, content_hash,
    store::{valid_hash, Commit, Manifest},
    Contract, ScopeBinding, Snapshot, Ticket, TicketKind, WorkState,
};

const CASE: &str = "native_standard_user_ticket_graph_publication";
const TICKET: &str = "graph-ticket";
const TICKET_KEY: &str = "tickets:graph-ticket";
const HEAD: &str = "HEAD";
const HEAD_STAGE: &str = "graph-head-stage";
const STALE_STAGE: &str = ".HEAD-graph-stale";
const HEAD_LENGTH: usize = 64;
const RECORD_LIMIT: usize = 2048;
const GRAPH_REPLY_LIMIT: u64 = 32 * 1024;
const CATEGORIES: [&str; 3] = ["objects", "manifests", "commits"];
const EMPTY_SECTIONS: [&str; 6] = [
    "assignments",
    "requests",
    "submissions",
    "receipts",
    "intents",
    "resolutions",
];

#[derive(Debug, PartialEq, Eq, Serialize)]
struct Observation {
    identity: Identity,
    link_count: u32,
    bytes_hex: String,
}

impl From<&ObjectObservation> for Observation {
    fn from(observed: &ObjectObservation) -> Self {
        assert_eq!(observed.link_count, 1);
        Self {
            identity: observed.identity.into(),
            link_count: observed.link_count,
            bytes_hex: hex::encode(&observed.bytes),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct Record {
    category: &'static str,
    address: String,
    #[serde(flatten)]
    observed: Observation,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct GraphProof {
    schema: u32,
    sequence: u64,
    parent: Option<String>,
    binding: ScopeBinding,
    commit_hash: String,
    manifest_hash: String,
    snapshot_sha256: String,
    objects: BTreeMap<&'static str, Record>,
}

struct Categories<'writer, 'scope> {
    objects: ImmutableCategory<'writer, 'scope>,
    manifests: ImmutableCategory<'writer, 'scope>,
    commits: ImmutableCategory<'writer, 'scope>,
}

impl<'writer, 'scope> Categories<'writer, 'scope> {
    fn create(fixture: &Path, writer: &'writer WriterGuard<'scope>) -> Self {
        for category in CATEGORIES {
            fs::create_dir(fixture.join(category)).expect("create plain scratch category");
        }
        Self::open(writer)
    }

    fn open(writer: &'writer WriterGuard<'scope>) -> Self {
        Self {
            objects: writer.open_category("objects").expect("open objects"),
            manifests: writer.open_category("manifests").expect("open manifests"),
            commits: writer.open_category("commits").expect("open commits"),
        }
    }
}

#[test]
#[ignore = "requires the exact-source Windows Ticket standard-user owner"]
fn native_standard_user_ticket_graph_publication() {
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
    let version = windows_version();
    let [initial_snapshot, successor_snapshot] = graph_samples();
    let initial_snapshot_bytes = canonical_bytes(&initial_snapshot).unwrap();
    let successor_snapshot_bytes = canonical_bytes(&successor_snapshot).unwrap();

    let (scope_id, volume, gate, initial_lock, initial, successor, first, second, final_head) = {
        let scope = WindowsScope::open_existing(&fixture).expect("retain fixed NTFS scope");
        let scope_id = scope.identity();
        let volume = volume_receipt(&fixture, &scope_id.into());
        let gate = verify_public_gate(&scope, &fixture);
        let writer = scope.acquire_writer(true).expect("acquire initial writer");
        assert_eq!(writer.identity().volume_serial, scope_id.volume_serial);
        assert_eq!(entries(&fixture), [WRITER]);
        let initial_lock = lock_observation(&scope, &writer);
        let categories = Categories::create(&fixture, &writer);
        let head = writer
            .open_head_scope()
            .expect("retain writable HEAD scope");
        assert_eq!(head.identity(), scope_id);

        let (first, publications) = publish_graph(&categories, &initial_snapshot, None, "initial");
        let old_address = first.commit_hash.as_bytes();
        let old_head_id = head
            .seed_head(old_address)
            .expect("seed initial HEAD and sync scope");
        let old_head = read_head(&head);
        assert_eq!(old_head.bytes, old_address);
        assert_eq!(old_head.identity, old_head_id);
        let initial = json!({
            "graph": first, "head": Observation::from(&old_head), "publications": publications,
            "head_scope_flush_win32_error": 0,
            "operation_order": ["ticket.category_flush", "header.category_flush",
                "manifest.category_flush", "commit.category_flush", "head.seed_scope_flush",
                "head.read_named"],
        });

        let (second, publications) = publish_graph(
            &categories,
            &successor_snapshot,
            Some(first.commit_hash.clone()),
            "successor",
        );
        let new_address = second.commit_hash.as_bytes();
        let stage = head
            .stage(HEAD_STAGE, new_address, true)
            .expect("sync successor HEAD stage");
        let stage_before = head
            .observe(HEAD_STAGE, HEAD_LENGTH)
            .expect("open successor HEAD stage");
        assert_eq!(stage_before.bytes, new_address);
        assert_eq!(stage_before.identity, stage.identity());
        let previous_reader = head
            .open_reader(true)
            .expect("retain independent old HEAD reader");
        let replaced = head
            .replace(&stage, &previous_reader)
            .expect("replace HEAD through native handle");
        assert_rename(replaced.rename, 65, 3);
        let installed = replaced
            .installation
            .expect("successful replacement")
            .expect("verify and sync successor HEAD");
        assert_eq!(installed.old.identity, old_head_id);
        assert_eq!(installed.old.bytes, old_address);
        assert_eq!(
            installed.stage_absence_win32_error,
            ERROR_FILE_NOT_FOUND as i32
        );
        assert_eq!(installed.scope_flush_win32_error, 0);
        let final_head = read_head(&head);
        assert_eq!(final_head.bytes, new_address);
        assert_ne!(final_head.identity, old_head_id);
        assert_eq!(
            Observation::from(&final_head),
            Observation::from(&stage_before)
        );
        assert_eq!(
            Observation::from(&final_head),
            Observation::from(&installed.named)
        );
        let final_head = Observation::from(&final_head);
        let successor = json!({
            "graph": second, "head": final_head, "publications": publications,
            "head_stage": Observation::from(&stage_before),
            "head_rename": rename_json(replaced.rename),
            "head_stage_absence_win32_error": installed.stage_absence_win32_error,
            "head_scope_flush_win32_error": installed.scope_flush_win32_error,
            "operation_order": ["ticket.category_flush", "header.category_flush",
                "manifest.category_flush", "commit.category_flush", "head.stage_file_flush",
                "head.replace", "head.scope_flush", "head.read_named"],
        });
        assert_eq!(
            entries(&fixture),
            [HEAD, "commits", "manifests", "objects", WRITER]
        );

        // No original scope, lock, category, stage or reader handle leaves this block.
        (
            scope_id,
            volume,
            gate,
            initial_lock,
            initial,
            successor,
            first,
            second,
            final_head,
        )
    };

    let fresh_scope = WindowsScope::open_existing(&fixture).expect("reopen fixed NTFS scope");
    let mut reopen_order = vec!["open_scope"];
    assert_eq!(fresh_scope.identity(), scope_id);
    let fresh_writer = fresh_scope
        .acquire_writer(false)
        .expect("reacquire existing lock on first attempt");
    reopen_order.push("acquire_writer");
    let reopened_lock = lock_observation(&fresh_scope, &fresh_writer);
    assert_eq!(reopened_lock, initial_lock);
    let head = fresh_writer
        .open_head_scope()
        .expect("reopen retained HEAD scope");
    let named_head = read_head(&head);
    reopen_order.push("read_head");
    assert_eq!(Observation::from(&named_head), final_head);
    let categories = Categories::open(&fresh_writer);
    let (selected, selected_snapshot) = traverse(
        &categories,
        address_from_head(&named_head),
        &mut reopen_order,
        [
            "selected.commit",
            "selected.manifest",
            "selected.header",
            "selected.ticket",
        ],
    )
    .expect("verify graph selected by freshly read HEAD");
    let (previous, previous_snapshot) = traverse(
        &categories,
        selected
            .parent
            .as_deref()
            .expect("successor carries disk parent"),
        &mut reopen_order,
        [
            "previous.commit",
            "previous.manifest",
            "previous.header",
            "previous.ticket",
        ],
    )
    .expect("verify prior graph from actual commit parent");
    assert_eq!(selected, second);
    assert_eq!(previous, first);
    assert_eq!(selected_snapshot, successor_snapshot_bytes);
    assert_eq!(previous_snapshot, initial_snapshot_bytes);

    let reopened = json!({
        "scope": Identity::from(fresh_scope.identity()),
        "writer": writer_json(&reopened_lock),
        "head": Observation::from(&named_head),
        "selected": selected,
        "previous": previous,
        "operation_order": reopen_order,
    });
    let bad = corrupt_reachable(
        &fresh_scope,
        &fresh_writer,
        &head,
        &categories,
        &selected,
        &previous,
    );

    write_json_bounded(
        &receipt,
        &json!({
            "schema": 1, "probe": "rust-graph", "case": CASE, "owner_pid": process::id(),
            "process_architecture": "X64", "windows_version": version, "token": token, "volume": volume,
            "ordered_crash_persistence_proven": false, "verdict": "available", "file_store_gate": gate,
            "graph": {
                "scope": Identity::from(scope_id),
                "writer": writer_json(&initial_lock),
                "initial": initial, "successor": successor, "reopen": reopened, "corruption": bad,
            },
        }),
        GRAPH_REPLY_LIMIT,
    );
}

fn graph_samples() -> [Snapshot; 2] {
    [sample(1, "First checkpoint"), sample(2, "Final checkpoint")]
}

fn sample(sequence: u64, title: &str) -> Snapshot {
    let mut snapshot = Snapshot::empty(ScopeBinding {
        scope_id: "graph-scope".into(),
        supervisor_session_id: "graph-supervisor".into(),
        binding_revision: 1,
    });
    snapshot.seq = sequence;
    snapshot.graph_revision = sequence;
    snapshot.authority_epoch = 1;
    snapshot.tickets.insert(
        TICKET.into(),
        Ticket {
            id: TICKET.into(),
            scope_id: snapshot.binding.scope_id.clone(),
            kind: TicketKind::Work,
            parent: None,
            depends_on: Default::default(),
            record_revision: sequence,
            contract_revision: sequence,
            generation: 1,
            contract: Contract {
                title: title.into(),
                objective: "Verify the addressed Ticket".into(),
                constraints: Vec::new(),
                acceptance: Vec::new(),
                user_acceptance_required: false,
                allowed_tools: Default::default(),
            },
            draft_brief: None,
            priority: Default::default(),
            expected_completion_at: None,
            state: WorkState::Draft,
            blocked: None,
            paused: false,
            archived: false,
            active_assignment: None,
            current_submission: None,
            accepted_submission: None,
            updated_seq: sequence,
            import_source: None,
        },
    );
    snapshot
}

fn publish_graph(
    categories: &Categories<'_, '_>,
    snapshot: &Snapshot,
    parent: Option<String>,
    phase: &str,
) -> (GraphProof, [Value; 4]) {
    // This probe covers the real one-Ticket Snapshot shape and its empty sections.
    let mut header: Map<String, Value> =
        serde_json::from_slice(&canonical_bytes(snapshot).unwrap()).unwrap();
    assert!(EMPTY_SECTIONS
        .iter()
        .all(|section| header.remove(*section) == Some(json!({}))));
    assert_eq!(header.remove("tickets"), Some(json!(snapshot.tickets)));
    assert_eq!(snapshot.tickets.len(), 1);
    let (ticket, ticket_publication) = publish(
        &categories.objects,
        "objects",
        "ticket",
        phase,
        &canonical_bytes(&snapshot.tickets[TICKET]).unwrap(),
    );
    let (header, header_publication) = publish(
        &categories.objects,
        "objects",
        "header",
        phase,
        &canonical_bytes(&header).unwrap(),
    );
    let manifest = Manifest {
        schema: snapshot.schema,
        objects: BTreeMap::from([
            ("header".into(), header.address.clone()),
            (TICKET_KEY.into(), ticket.address.clone()),
        ]),
    };
    let (manifest, manifest_publication) = publish(
        &categories.manifests,
        "manifests",
        "manifest",
        phase,
        &canonical_bytes(&manifest).unwrap(),
    );
    let commit = Commit {
        schema: snapshot.schema,
        seq: snapshot.seq,
        parent: parent.clone(),
        manifest: manifest.address.clone(),
    };
    let (commit, commit_publication) = publish(
        &categories.commits,
        "commits",
        "commit",
        phase,
        &canonical_bytes(&commit).unwrap(),
    );
    let graph = GraphProof {
        schema: snapshot.schema,
        sequence: snapshot.seq,
        parent,
        binding: snapshot.binding.clone(),
        commit_hash: commit.address.clone(),
        manifest_hash: manifest.address.clone(),
        snapshot_sha256: content_hash(&canonical_bytes(snapshot).unwrap()),
        objects: graph_records(ticket, header, manifest, commit),
    };
    (
        graph,
        [
            ticket_publication,
            header_publication,
            manifest_publication,
            commit_publication,
        ],
    )
}

fn graph_records(
    ticket: Record,
    header: Record,
    manifest: Record,
    commit: Record,
) -> BTreeMap<&'static str, Record> {
    BTreeMap::from([
        ("ticket", ticket),
        ("header", header),
        ("manifest", manifest),
        ("commit", commit),
    ])
}

fn publish(
    directory: &ImmutableCategory<'_, '_>,
    category: &'static str,
    kind: &str,
    phase: &str,
    bytes: &[u8],
) -> (Record, Value) {
    assert!(bytes.len() <= RECORD_LIMIT);
    let address = content_hash(bytes);
    let stage = directory
        .stage(&format!("{phase}-{kind}-stage"), bytes)
        .expect("stage and flush immutable file");
    let publication = directory
        .publish_no_replace(&stage, &address)
        .expect("publish immutable name through same stage handle");
    assert_rename(publication.rename, 10, 0);
    let installed = publication
        .installation
        .expect("successful no-replace rename")
        .expect("verify named record, stage absence and category flush");
    assert_eq!(installed.named.bytes, bytes);
    assert_eq!(installed.named.identity, stage.identity());
    assert_eq!(
        installed.stage_absence_win32_error,
        ERROR_FILE_NOT_FOUND as i32
    );
    assert_eq!(installed.category_flush_win32_error, 0);
    let proof = json!({
        "kind": kind, "category": category, "address": address, "stage_identity": Identity::from(stage.identity()),
        "file_flush_win32_error": 0, "rename": rename_json(publication.rename),
        "stage_absence_win32_error": installed.stage_absence_win32_error,
        "category_flush_win32_error": installed.category_flush_win32_error,
    });
    (
        Record {
            category,
            address,
            observed: Observation::from(&installed.named),
        },
        proof,
    )
}

#[derive(Debug)]
struct ChecksumMismatch {
    category: &'static str,
    address: String,
    observed_sha256: String,
    observed: ObjectObservation,
}

fn read_record(
    directory: &ImmutableCategory<'_, '_>,
    category: &'static str,
    address: &str,
) -> Result<(Record, Vec<u8>), ChecksumMismatch> {
    assert!(valid_hash(address), "malformed digest in graph: {address}");
    let observed = directory
        .observe(address, RECORD_LIMIT)
        .expect("reopen bounded named graph record");
    assert!(
        observed.bytes.len() <= RECORD_LIMIT,
        "record exceeded fixture bound"
    );
    let actual = content_hash(&observed.bytes);
    if actual != address {
        return Err(ChecksumMismatch {
            category,
            address: address.into(),
            observed_sha256: actual,
            observed,
        });
    }
    Ok((
        Record {
            category,
            address: address.into(),
            observed: Observation::from(&observed),
        },
        observed.bytes,
    ))
}

fn traverse(
    categories: &Categories<'_, '_>,
    address: &str,
    order: &mut Vec<&'static str>,
    phases: [&'static str; 4],
) -> Result<(GraphProof, Vec<u8>), ChecksumMismatch> {
    let (commit, commit_bytes) = read_record(&categories.commits, "commits", address)?;
    order.push(phases[0]);
    let document: Commit = serde_json::from_slice(&commit_bytes).expect("real Commit schema");
    assert_eq!(canonical_bytes(&document).unwrap(), commit_bytes);
    assert_eq!(document.schema, 1);
    match document.seq {
        1 => assert!(document.parent.is_none()),
        2 => assert!(document.parent.as_deref().is_some_and(valid_hash)),
        other => panic!("unexpected fixture sequence {other}"),
    }

    let (manifest, manifest_bytes) =
        read_record(&categories.manifests, "manifests", &document.manifest)?;
    order.push(phases[1]);
    let mapping: Manifest = serde_json::from_slice(&manifest_bytes).expect("real Manifest schema");
    assert_eq!(canonical_bytes(&mapping).unwrap(), manifest_bytes);
    assert_eq!(mapping.schema, document.schema);
    assert_eq!(
        mapping
            .objects
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["header", TICKET_KEY]
    );

    let (header, header_bytes) =
        read_record(&categories.objects, "objects", &mapping.objects["header"])?;
    order.push(phases[2]);
    let mut header_fields: Map<String, Value> =
        serde_json::from_slice(&header_bytes).expect("canonical Snapshot header");
    assert_eq!(canonical_bytes(&header_fields).unwrap(), header_bytes);
    let (ticket, ticket_bytes) =
        read_record(&categories.objects, "objects", &mapping.objects[TICKET_KEY])?;
    order.push(phases[3]);
    let ticket_data: Ticket = serde_json::from_slice(&ticket_bytes).expect("real Ticket record");
    assert_eq!(canonical_bytes(&ticket_data).unwrap(), ticket_bytes);
    assert_eq!(ticket_data.id, TICKET);
    assert_eq!(ticket_data.updated_seq, document.seq);
    assert!(header_fields
        .insert("tickets".into(), json!({ TICKET: ticket_data }))
        .is_none());
    for section in EMPTY_SECTIONS {
        assert!(header_fields.insert(section.into(), json!({})).is_none());
    }
    let snapshot: Snapshot =
        serde_json::from_value(Value::Object(header_fields)).expect("reconstruct real Snapshot");
    assert_eq!(snapshot.schema, document.schema);
    assert_eq!(snapshot.seq, document.seq);
    assert_eq!(snapshot.tickets[TICKET].scope_id, snapshot.binding.scope_id);
    let snapshot_bytes = canonical_bytes(&snapshot).unwrap();
    let proof = GraphProof {
        schema: document.schema,
        sequence: document.seq,
        parent: document.parent,
        binding: snapshot.binding,
        commit_hash: commit.address.clone(),
        manifest_hash: manifest.address.clone(),
        snapshot_sha256: content_hash(&snapshot_bytes),
        objects: graph_records(ticket, header, manifest, commit),
    };
    Ok((proof, snapshot_bytes))
}

fn corrupt_reachable(
    scope: &WindowsScope,
    writer: &WriterGuard<'_>,
    head: &HeadScope<'_, '_>,
    categories: &Categories<'_, '_>,
    selected: &GraphProof,
    previous: &GraphProof,
) -> Value {
    let stale_id = head
        .stage(STALE_STAGE, previous.commit_hash.as_bytes(), true)
        .expect("stage the prior valid commit without publishing it")
        .identity();
    let stale_before = head
        .observe(STALE_STAGE, HEAD_LENGTH)
        .expect("reopen stale HEAD stage");
    assert_eq!(stale_before.identity, stale_id);
    assert_eq!(address_from_head(&stale_before), previous.commit_hash);
    let head_before = read_head(head);
    assert_ne!(head_before.identity, stale_id);
    assert_eq!(address_from_head(&head_before), selected.commit_hash);
    let writer_before = lock_observation(scope, writer);
    let target = &selected.objects["ticket"];
    let address = &target.address;
    let object_before = categories
        .objects
        .observe(address, RECORD_LIMIT)
        .expect("read reachable object before corruption");
    assert_eq!(Observation::from(&object_before), target.observed);

    // Only the disposable, already-attested object may be overwritten in this control.
    let replacement = hex::decode(&previous.objects["ticket"].observed.bytes_hex).unwrap();
    assert_eq!(replacement.len(), object_before.bytes.len());
    assert_ne!(replacement, object_before.bytes);
    let directory = open_relative(
        scope.directory(),
        OsStr::new("objects"),
        true,
        DIRECTORY_ACCESS,
        FILE_OPEN,
    )
    .expect("open retained objects category by name");
    assert_eq!(
        entry_identity(&directory, true).unwrap(),
        categories.objects.identity()
    );
    let mut injection = open_relative(
        &directory,
        OsStr::new(address),
        false,
        FILE_GENERIC_WRITE | FILE_READ_ATTRIBUTES,
        FILE_OPEN,
    )
    .expect("open exact existing scratch object for deliberate corruption");
    assert_eq!(
        observe_regular(scope, &injection).unwrap().identity,
        object_before.identity
    );
    injection
        .write_all(&replacement)
        .expect("overwrite only the reachable object bytes");
    injection.sync_all().expect("sync intentional corruption");
    drop(injection);
    let injected = categories
        .objects
        .observe(address, RECORD_LIMIT)
        .expect("observe injected bytes independently");
    assert_eq!(injected.bytes, replacement);
    assert_eq!(injected.identity, object_before.identity);

    let named_head = read_head(head);
    assert_eq!(
        Observation::from(&named_head),
        Observation::from(&head_before)
    );
    let mut reached = Vec::new();
    let error = traverse(
        categories,
        address_from_head(&named_head),
        &mut reached,
        ["commit", "manifest", "header", "ticket"],
    )
    .expect_err("fresh HEAD-led traversal must reject reachable checksum corruption");
    assert_eq!(reached, ["commit", "manifest", "header"]);
    assert_eq!(error.category, "objects");
    assert_eq!(&error.address, address);
    assert_eq!(error.observed_sha256, previous.objects["ticket"].address);
    assert_eq!(
        Observation::from(&error.observed),
        Observation::from(&injected)
    );
    let head_after = read_head(head);
    let writer_after = lock_observation(scope, writer);
    let object_after = categories
        .objects
        .observe(address, RECORD_LIMIT)
        .expect("reread corrupted object without repair");
    assert_eq!(
        Observation::from(&head_after),
        Observation::from(&head_before)
    );
    assert_eq!(writer_after, writer_before);
    assert_eq!(
        Observation::from(&object_after),
        Observation::from(&injected)
    );

    let stale_after = head
        .observe(STALE_STAGE, HEAD_LENGTH)
        .expect("reread unadopted stale stage");
    assert_eq!(
        Observation::from(&stale_after),
        Observation::from(&stale_before)
    );

    json!({
        "kind": "object_hash_mismatch", "address": error.address, "observed_sha256": error.observed_sha256,
        "head_before": Observation::from(&head_before), "head_after": Observation::from(&head_after),
        "writer_before": writer_before, "writer_after": writer_after,
        "object_before": Observation::from(&object_before), "injected": Observation::from(&injected),
        "object_after": Observation::from(&object_after), "stale_stage_name": STALE_STAGE,
        "stale_stage_before": Observation::from(&stale_before), "stale_stage_after": Observation::from(&stale_after),
        "operation_order": ["stage_stale_head", "read_stale_stage_before", "read_head_before",
            "read_writer_before", "read_object_before", "inject_object_bytes", "sync_injected_object",
            "traverse_named_head", "reject_object_checksum", "read_head_after", "read_writer_after",
            "read_object_after", "read_stale_stage_after"],
    })
}

fn read_head(head: &HeadScope<'_, '_>) -> ObjectObservation {
    head.observe(HEAD, HEAD_LENGTH)
        .expect("read bounded named HEAD")
}

fn address_from_head(head: &ObjectObservation) -> &str {
    let digest = std::str::from_utf8(&head.bytes).expect("lowercase digests in ASCII HEAD");
    assert!(
        valid_hash(digest),
        "HEAD must contain exactly one bare digest"
    );
    digest
}

fn lock_observation(scope: &WindowsScope, writer: &WriterGuard<'_>) -> Observation {
    let observed = scope.inspect_file(WRITER).expect("open bound writer name");
    assert_eq!(observed.identity, writer.identity());
    (&writer.file)
        .seek(SeekFrom::Start(0))
        .expect("rewind held writer file");
    let bytes = read_bounded(&writer.file, 0).expect("read empty lock through its owned handle");
    assert!(bytes.is_empty());
    Observation::from(&ObjectObservation {
        identity: observed.identity,
        link_count: observed.link_count,
        bytes,
    })
}

fn writer_json(lock: &Observation) -> Value {
    json!({ "file": lock.identity, "link_count": lock.link_count, "win32_error": 0 })
}

fn assert_rename(receipt: RenameReceipt, information_class: i32, flags: u32) {
    assert_eq!(receipt.ntstatus, 0);
    assert_eq!(receipt.win32_error, 0);
    assert_eq!(receipt.information_class, information_class);
    assert_eq!(receipt.flags, flags);
}

fn rename_json(receipt: RenameReceipt) -> Value {
    json!({
        "ntstatus": format!("{:08x}", receipt.ntstatus as u32),
        "win32_error": receipt.win32_error,
        "information_class": receipt.information_class,
        "flags": receipt.flags,
    })
}
