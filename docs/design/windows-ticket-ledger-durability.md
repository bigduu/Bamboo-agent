# Windows Ticket ledger durability

Proposed in [#3109](https://github.com/bigduu/Bamboo-agent/issues/3109) as the contract and
native evidence for [#3101](https://github.com/bigduu/Bamboo-agent/issues/3101).

The first candidate is a Ticket scope on a fixed local NTFS volume opened by an ordinary,
non-elevated Windows user. Windows acceptance requires exclusive writer ownership and ordered
durable publication of a complete ledger snapshot. Windows FileStore admission currently fails
before opening or initializing a scope. Windows Work Review depends on later ledger admission.
[Work Review #2570](https://github.com/bigduu/Bamboo-agent/issues/2570)

## Current ledger authority

[FileStore](../../crates/infra/bamboo-tickets/src/store.rs) admits macOS and Linux, retains the
physical `writer.lock` handle with an exclusive OS lock, and publishes one authoritative `HEAD`.
The explicit empty initializer checks the directory before taking the lock and checks its
contents again under the lock. It creates the category directories and synchronizes the scope
before publishing the first snapshot. Native support must also account for the parent entry when
a newly created scope directory itself needs to survive interruption.

Publication has the following order.

1. Write the complete bytes to a new staging file in the destination directory and synchronize
   the file. Publish the immutable content-addressed name, then synchronize that category
   directory. Reuse checks the existing bytes and repeats the directory barrier. A prior failed
   attempt might have installed the name without synchronizing its directory.
2. Finish the object set and verify referenced artifact blobs before publishing the manifest and
   commit with the same protocol. The commit names the manifest and prior published commit.
3. Write and synchronize a staged HEAD containing the commit hash. Replace `HEAD` in the scope
   directory and synchronize that directory. Only then update the in-memory published snapshot
   and allow the service to return its operation receipt.
4. After an error following HEAD replacement, fence the current writer read-only and retain its
   last confirmed in-memory snapshot. On reopen, use the named `HEAD` and validate the content
   hashes, manifest, sequence, ancestry, binding, and snapshot. Missing HEAD with known history,
   invalid ancestry, or an invalid binding fails closed. Staging files and unreferenced commits
   carry no authority.

[TicketService](../../crates/infra/bamboo-tickets/src/service.rs) publishes an operation and its
idempotency receipt in the same snapshot. A retry returns the original receipt only after
verifying the original principal, scope, operation ID, and canonical request. A different
principal is denied before receipt disclosure. Reusing the operation ID with a different request
returns a conflict. Ordinary writable reopen advances the authority epoch, isolates interrupted
assignments, and expires outstanding approvals through another published snapshot.

The shared platform predicate also affects Host availability and explicit initialization, and
[offline open](../../crates/infra/bamboo-tickets/src/migration.rs) uses the same writer gate.
Offline open validates an existing HEAD without the live epoch transition. Backup, migration,
export, and file reconciliation have their own publication and recovery paths. Qualify those
routes independently before enabling them on Windows. [Host
initialization](../../crates/app/bamboo-server/src/app_state/ticket_application/initialization.rs)
· [Ticket CLI](../../src/ticket_cli.rs)

## Candidate Windows contract

### Scope and writer identity

- Admit a fully qualified local drive path and verify a fixed local NTFS volume through the
  retained handles. Record the actual volume and filesystem, and check that the scope, its
  parents, category directories, and files have the admitted volume identity. Reject unknown or
  unsupported volumes, network and device paths, substituted drive roots, removable media, and
  reparse points, including ancestor junctions and volume mount aliases.
- Retain the scope and category directory handles. Resolve one literal component at a time
  relative to a retained parent. Validate regular files and directories by handle, use explicit
  Windows name semantics, and reject alternate data streams and multiply linked authority files.
  Use volume ID and file ID plus current-name revalidation to detect substitution. Equivalent
  spellings of the same physical scope must contend for the same writer lock.
- Open and validate the ledger's `writer.lock` relative to the scope with compatible access and
  sharing. Use a nonblocking exclusive OS lock on that file and retain the
  handle and lock for the lifetime of the writer. Sharing and directory identity protections
  must keep the lock name bound to the locked file throughout that lifetime. An equivalent
  protocol must exclude another writer before either can publish, including during a name
  replacement race. Lock errors, replaced lock files, scope identity changes, and failed
  validation deny authority. A fault fence keeps the lock while the current instance is
  read-only. Release the lock on normal close and verify reacquisition after the owning process
  exits.

The [read-only Windows Project
adapter](../../crates/infra/bamboo-projects/src/read_only_windows.rs) supplies a precedent for
literal handle-relative opens, identity and link-count checks, and retained ancestor
verification. The Ticket writer also needs mutation-specific sharing and exclusion proofs.
[NtCreateFile](https://learn.microsoft.com/en-us/windows/win32/api/winternl/nf-winternl-ntcreatefile) ·
[FILE_ID_INFO](https://learn.microsoft.com/en-us/windows/desktop/api/winbase/ns-winbase-file_id_info) ·
[GetVolumeInformationByHandleW](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getvolumeinformationbyhandlew) ·
[LockFileEx](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex)

### File and namespace barriers

- Create staging files exclusively under validated destination handles and synchronize their
  full contents before publishing a name. Publish immutable names without replacing an existing
  hash. For an existing hash, verify the original bytes and repeat the containing-directory
  barrier. Prove that each object, manifest, and commit name is durable before HEAD can
  reference that graph.
- Use a single same-volume replacement for HEAD and prove that callers see a complete old or new
  file. Apply the scope namespace barrier and verify that the named HEAD retains the expected
  file identity before exposing the new snapshot or receipt. Initialization also needs evidence
  for the category directories and each new directory entry introduced on the path from an
  existing durable parent. Failed or unavailable barriers preclude write admission.
- Treat a replacement error with uncertain namespace effects the same as a failed
  post-replacement barrier. Keep the writer fenced, preserve the last confirmed snapshot, and
  require reopen against validated named state. Use the ordinary retry path to return a receipt
  already present in the recovered graph. Cleanup of abandoned staging must never promote it to
  HEAD.

The [Windows Session Inbox
anchor](../../crates/infra/bamboo-storage/src/session_inbox_anchor.rs) supplies a
handle-relative rename precedent using `NtSetInformationFile` and a retained destination
directory. That operation establishes name and sharing semantics. Ordered namespace persistence
requires its own evidence. Microsoft documents writable file handles for `FlushFileBuffers` and
requires administrative privileges to flush an entire volume. The candidate uses an ordinary
user token, so volume flush is outside its contract. A native probe must determine whether
directory handles can be flushed under that token and what persistence guarantees those
operations have.
[FILE_RENAME_INFORMATION](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_rename_information) ·
[FlushFileBuffers](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)

`ReplaceFileW` documents an unsupported write-through option and error cases that can change
names. `MoveFileExW` documents write-through behavior for a move performed by copying and
deleting. Qualifying ordered publication across the Ticket category directories and HEAD
requires the actual selected operations, accepted access and sharing modes, and every namespace
barrier used by the ledger.
[ReplaceFileW](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-replacefilew) ·
[MoveFileExW](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw)

## Native acceptance

Start with native Windows x64 on a dedicated NTFS fixture under a standard user token. Bind the
job to an explicit source SHA, Git tree, Cargo.lock, runner and compiler architecture, Windows
build, token privileges, and the filesystem and volume identity observed by handle. Keep the
command, exact executed test names, per-operation results and Windows error codes, and
before/after contents and identities. Capture denial outcomes with the same rigor as successful
operations. The existing [Ticket platform adapter
workflow](../../.github/workflows/ticket-platform-adapter.yml) already has an exact-source
Windows route for Project reads and a workspace-write refusal. A later probe can extend that
owner with actual ledger tests.

Measure the initial Windows directory flush capability using the probe input on that owner.
Set `branch` to the published probe branch and `sha` to its exact commit.

```sh
gh workflow run ticket-platform-adapter.yml -R bigduu/Bamboo-agent --ref "$branch" \
  -f windows_ticket_probe_sha="$sha"
```

The workflow launches the probe under a temporary standard local account and prints the native
receipt in the Windows job log. The portable PowerShell script can also run directly under an
existing standard account with `-ExpectedSid`, a fresh `-FixtureDirectory` on local NTFS, and
an unused sibling `-ReceiptPath`. Its verdict describes the exact directory access mask, flags,
token, and filesystem tested. The scratch rename and warm reopen record operational behavior.
The receipt explicitly leaves ordered crash persistence unproven.

Add `-f windows_ticket_probe_case=writer-lock` to select the writer fixture. Its receipt records
independent process and handle contention, reacquisition after release and process exit, and
the ordinary and POSIX name tests. A changed lock identity or inconclusive name control yields
an unavailable writer verdict. The default case remains `directory-flush`.

Select `-f windows_ticket_probe_case=publication` for the handle-relative publication fixture.
It measures a create-only immutable name and a same-volume scratch HEAD replacement. The receipt
reports the old handle's identity and bytes, the newly opened HEAD, and the preservation results
for both denied rename controls. The fixture observes those operations under the actual token
and filesystem. Ordered crash persistence remains a separate qualification.

Select `-f windows_ticket_probe_case=publication-process-exit` for the warm-reopen fixture.
It uses one uninterrupted publication and three separate scopes where the controller terminates
the writer while it still holds the lock. It cuts before HEAD replacement, after the native
replacement succeeds but before the scope flush, and after that flush but before a success reply.
A fresh process acquires the existing lock and follows named HEAD through the checksum-linked
commit, manifest, and object. The receipt includes the staged name and graph before replacement,
and reads the new HEAD and prior graph after replacement. The running OS and mounted volume are
shared across each exit and reopen. OS-crash, remount, and power-loss persistence require separate
evidence.

Select `-f windows_ticket_probe_case=scope-identity` for the scope admission controls. The
ordinary local user creates one ancestor junction between owned scratch directories and one
hard-link alias of an existing scratch writer lock. The receipt checks actual tags, identities,
link counts and bytes, and records where the normal writer open refuses each alias. The fixture
removes the junction itself before recursive scratch cleanup.

Select `-f windows_ticket_probe_case=rust-writer` for the private Rust writer qualification.
The Windows owner builds one locked Ticket library test executable, records its digest, and
stages it with read and execute access for the disposable local standard user. The exact test
checks anchored fixed-NTFS scope and writer identity, native Rust lock contention and
reacquisition across processes, and two-link rejection. It also exercises the public FileStore
platform refusal before disk mutation. The Rust primitive is compiled for the Windows test
target. Windows runtime admission and ordered crash persistence remain separate.

Select `-f windows_ticket_probe_case=rust-immutable` for the private Rust immutable publication
test. The same native owner runs one exact test under the disposable standard user. The test
holds the Rust writer and a single writable NTFS category, stages and synchronizes a file, and
publishes the digest through a create-only rename on the same handle. It verifies the new name,
the category barrier, native collision and name preservation, exact-content reuse, and the
refusal to reuse conflicting bytes. This measures the immutable publication operations. HEAD
replacement and crash persistence remain separate.

Select `-f windows_ticket_probe_case=rust-head` for the private Rust HEAD replacement test.
It stages a successor, replaces literal HEAD relative to the retained writable scope, and
flushes that scope. A held reader with compatible sharing must retain the old file identity and
bytes while a new relative open reads the successor. The test also applies native rename to a
source lacking DELETE and to a target with a reader lacking delete sharing, and confirms both
names and their bytes remain intact on refusal. Full graph recovery and crash persistence
remain separate.

Select `-f windows_ticket_probe_case=rust-graph` for the private Rust Ticket graph test.
It uses canonical Ticket snapshot data, manifests and commits under the retained native writer.
The test publishes two revisions, observes the object, manifest, commit and scope barriers,
then closes the writer and opens fresh handles in the same process. The verifier starts at
named HEAD and validates the current and parent graphs by checksum. A final scratch control
corrupts a referenced object and verifies that readback refuses the graph while preserving
HEAD, the corrupted file and an intact stale stage. Process exit and OS-crash or remount persistence have separate qualification.

Select `-f windows_ticket_probe_case=rust-graph-process-exit` to reopen the canonical Rust
Ticket graph after its writer exits. The same exact test executable runs the coordinator,
writer and independent verifier under the ordinary local user. Separate scopes cover a clean
publication and three stops around native HEAD replacement and scope synchronization. The
coordinator waits for the actual writer to exit before starting the verifier. The verifier
reacquires the same physical lock and reads named HEAD and its canonical graph from disk.
Before replacement, it reads the initial graph and the intact staged successor. After
replacement, it reads the successor and parent graph and confirms the staging name is absent.
The OS and NTFS mount stay live across writer exit and verifier reopen.

| Evidence | Acceptance |
| --- | --- |
| Scope and writer | With a writer held, a second open in the same process and an independent process are denied before mutation through both the original name and an equivalent spelling. Attempt to replace the lock name and acquire a second writer while the first lock is held. A fresh process can take ownership after clean release and after actual owner exit. Ancestor or leaf substitution, junctions, alternate streams, hard links, and wrong volume or file identity fail closed without writing to a foreign scope. |
| Namespace primitives | Real native operations create, synchronize, publish, and reopen each staging and final file through the admitted handles. Prove exclusive immutable publication, identical reuse, conflicting content rejection, complete HEAD replacement under concurrent readers with compatible sharing, and file, category, scope, and applicable parent-directory barriers. Exercise an incompatible sharing mode and access denial with preserved old authority. An unsupported or inconclusive namespace barrier is a failed qualification. |
| Publication interruption | Inject failure and terminate an actual process before and after each real write, file flush, publication, and containing-directory flush. Include repeated per-object phases, immutable-name reuse, initial publication and existing HEAD replacement. Apply the phase oracle below. Verify that an uncertain result fences the current instance read-only while it retains the writer lock. A stale staging file or orphan commit cannot become authority. |
| Receipts and Host recovery | Lose the response after durable publication and retry the exact operation. Confirm the original receipt and one logical mutation. Deny a different principal or scope before disclosing the receipt, and reject an altered request. Verify that reopen revalidates the binding, advances its epoch, and fences any interrupted execution before the Host grants new authority. Include a corrupt or missing HEAD control that denies mutation. |

Use the current fault tests as behavioral oracles. [File authority
tests](../../crates/infra/bamboo-tickets/tests/file_authority.rs) cover writer exclusion, whole
snapshots, reused-directory flushes and strict initialization. [Service contract
tests](../../crates/infra/bamboo-tickets/tests/service_contract.rs) cover the post-rename
read-only fence, actual process exit at the physical publication boundaries, and exact receipt
replay. [Host publication tests](../../tests/ticket_runtime_publication.rs) cover interruption
of live intent, admission, and result handling. The Host process fixtures are Unix-gated today.
Windows acceptance requires the named native cases to actually execute.

Check the recovered graph before normal Host reopen publishes a new authority epoch. Before HEAD
replacement, the old graph must remain exact. An interrupted first publication at that phase
must leave no authority and a partially initialized scope must deny mutation. At an uncertain
replacement or before the confirmed HEAD barrier, any recovered HEAD must select an exact
complete old or new graph, with initial publication also allowing no HEAD. Ambiguous or damaged
state must fail closed. A lost or torn existing HEAD in an otherwise supported interruption
trial fails qualification. After the successful HEAD barrier, only the new graph qualifies,
including when the operation response was lost.

A process-exit test exercises handle release and reopen while the operating system may still
cache writes. Qualify ordered namespace persistence with a documented platform guarantee for
each selected barrier and native crash/remount evidence on an isolated test volume. Physical
power-loss assurance also depends on the tested device, controller, and cache behavior. Record
the exact level tested and the guarantee used for each barrier. A successful rename, a
successful flush return, or a warm reopen alone leaves ordered crash persistence unproven. Keep
Windows admission closed while required durability evidence is missing.

## Delivery sequence

1. Run a focused native capability probe under the proposed NTFS and token conditions. Publish
   exact-source evidence and a supported or unsupported verdict for each required operation and
   barrier.
2. Qualify the FileStore open and publication adapter against the established contract and fault
   matrix. Keep Host admission guarded while recovery acceptance is pending.
3. Verify native Host reopen, epoch fencing, and receipt behavior before enabling the qualified
   Windows scope. Preserve the separate gates for offline commands, workspace file operations,
   and Work Review.
