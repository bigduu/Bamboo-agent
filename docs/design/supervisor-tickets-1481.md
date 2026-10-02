# Supervisor tickets implementation ledger (#1481)

## P0 baseline (2026-10-02)

Isolated checkout: `/Users/bigduu/Documents/Codex/2026-10-02/task/bamboo-1481`.
Local branch: `bamboo/feat/1481-supervisor-tickets`.
Base: `8d7e675502f6643d2fe17af002413eb8045ed726`, also the GitHub `dev`
commit resolved during this audit. No pre-existing #1481 branch, PR or
implementation task was found. Zenith and the #791 worktrees are dirty and
are deliberately not used as sources of uncommitted implementation.

The #1479/#1480 owner uses `bamboo/fix/1479-791-child-result-continuity`.
Its committed base is still `8d7e6755`; its implementation is uncommitted.
Both issues are open and locked. Do not copy or reimplement those patches.
#1479 remains a hard gate for live dispatch and P5/P9 acceptance. #1480 is
only a gate for later same-Actor continuation. #1478 is also open/locked;
its macOS unwind repair is not in this base and must not be duplicated.

## Slice boundaries and interfaces

This tracker is executed as independently testable local slices. The first
slice is P1/P2/P4: a file-authoritative TicketService and a deterministic
create/dispatch/submit/accept fixture. P3 changes the existing child Task
boundary. Runtime integration (P5), bounded reads (P6), semantic resolution
(P7), Lotus (P8), and live acceptance (P9) have separate evidence. A passing
domain test is never recorded as end-to-end completion.

- Keep `SupervisorSessionService` and its Storage-owned scope proof intact.
  A trusted host adapter must verify that proof before constructing service
  authority; a model/client cannot submit a trusted identity in JSON.
- Add an independent `bamboo-tickets` infrastructure crate. It stores work
  contracts, not Jiandu memory or Session transcripts. A single process lock
  and unique HEAD govern the scope. No second per-ticket HEAD or index writer.
- `child_session/actions.rs` currently clones the entire parent TaskList;
  `runner/tool_execution/task/taskwrite.rs` routes Child writes to the root.
  Replace both together, preserving old Root Task compatibility.
- Runtime has PlacementScheduler/FileHostRegistry and fenced Actor admission,
  but no persistent `ensure_dispatch(key, spec)` / `query_dispatch(key)` port.
  The bridge must fail closed until admission can prove stable key identity.
  A fake Runtime is only a deterministic contract test.
- Inbox `SessionMessageEnvelope` already has thread_id/in_reply_to/
  correlation_id. ChatRequest does not yet carry them. Those routing fields
  never confer approval. Pending approval binds the full action fingerprint
  and exact request/prompt/assignment/generation.
- Supervisor model calls occur outside the TicketService lock. Ingress and
  resolutions are durable; one short decision turn at a time; Workers do not
  hold the Supervisor's turn or a transaction lock.
- The store is excluded from Worker write roots. Same-UID arbitrary shell
  cannot be represented as a proven security boundary. Live code execution
  stays disabled until the runtime adapter proves filesystem enforcement.

## Verification and rollback

Use deterministic service/fake-worker tests separately from real model
semantic evaluation, Host/four-Worker fixtures and Lotus browser evidence.
Test process lock, complete manifest/hash checks, CAS/idempotency, every
publication fault boundary, late generations, dependency invalidation and
unknown effects. Local APFS is the initial filesystem target; do not infer
NFS, sync-drive or Windows durability support from it.

Mutation and dispatch are independent, default-off host capabilities. Closing
dispatch does not discard submissions. An unsupported schema or damaged HEAD
opens no writable authority. Rollback preserves immutable history/receipts.
No push, PR, merge, deployment, production migration, permission or credential
changes are authorized in this task.

## Evidence status

P0 baseline and interface audit recorded. P1–P9 and A1–A12 remain unaccepted
until individual test and runtime evidence is recorded below.

### Local slice 1: domain and file authority

The foundational `bamboo-tickets` crate contains the separate version axes and
content-addressed full revisions/manifest/commit/HEAD, a lifetime OS writer
lock, fixed commit reads and verified readonly exports. Its focused storage
suite has four passing tests, including every publication I/O boundary.
`cargo clippy -p bamboo-tickets --offline --locked --all-targets -- -D warnings`
passes. Only local APFS process-crash/flush behavior is evidenced here; power
loss and other filesystem types are not accepted.

Ticket command implementation is a subsequent local branch/worktree. During
its initial focused verification, twelve tests passed, including 48 actual
process-exit boundaries and a second-process writer-lock rejection. That
evidence does not accept live dispatch, OS Worker isolation, model routing or UI.

### Local slice 2: application commands

Worktree: `bamboo-1481-commands`; branch: `bamboo/feat/1481-ticket-commands`,
based on foundational commit `dc442538`. TicketService owns atomic typed
batches, receipt deduplication before CAS, trusted-host identity checks,
private LocalPlans, exact approval consumption, retained stale submissions,
dependency/Goal invalidation and explicit legacy import. It checks canonical
worktree claims and excludes its authority directory from declared write roots.
These checks are not OS sandbox enforcement and are not presented as such.

Focused suite: 4 file-authority tests plus 15 service tests, including one
subprocess helper exercised by the real process-crash test. The crash test
checks 48 actual exit windows plus a competing OS writer. The deterministic
entry point `cargo run -p bamboo-tickets --offline --locked --example single_work`
computes 2+3 in a fixture thread, submits its hash and accepts the exact
submission only after fixture-user evidence; output is labelled
`deterministic_service_fixture`. No live Runtime or provider is used.

P3 is still open: ordinary/native Task execution is not yet wired to this
service LocalPlan. P5 is still gated by #1479 and the missing real admission
key/query port. P7 model evaluation, P8 Lotus, and P9 real acceptance remain
unrun. Import currently produces a blocked record requiring review; it does
not migrate running Session ownership or create acceptance for legacy output.
Artifact URI/hash syntax is checked, but artifact bytes need a trusted host
artifact resolver before automated quality acceptance can be enabled.
