# Explicit owner control for finite Ready Work

Issue #2452; audited base `d24b4e3a3a8a1b057b926b0b943c15f4e1c2cbb5`.
This is a source capability and controlled-provider acceptance slice. It does
not enable an installed application or complete the broader #1481/#2040/#2049
trackers. The local installed application's read-only scope observation on
2026-11-02 still reported `ticket_mutation_disabled`, mutation/dispatch disabled,
and no initialization permission. No local setting or legacy Supervisor changed.

## Existing mechanisms, verified in source

| Responsibility | Existing owner/source | Evidence and boundary |
| --- | --- | --- |
| Single file authority, CAS, operation receipts, generation, Assignment/DispatchIntent, Submission and acceptance | `bamboo-tickets/src/{store,service,model}.rs` | #1481 is a historical tracker, not a missing implementation list. `submitted` requires explicit exact-submission acceptance; accepted dependencies and Goal evidence retain their separate rules. |
| Project/Work context and deterministic claiming | `bamboo-tickets/src/{project,claiming}.rs`, `tests/{project_board,supervisor_claims,project_claim_sprint}.rs` | #2041/#2042 have delivered mechanisms. Project metadata supplies no directory permission. Work context and isolated Git workspace are checked again at admission. |
| Ready selection and finite recovery | `bamboo-orchestration/src/{lib,ready_recovery}.rs` and server `ticket_application/{scheduling,admission,ready_retry_store}.rs` | The trusted Rust installer already exists. Normal startup installs no policy. Unknown outcomes retain their original dispatch identity; retry metadata never recovers executable permission. |
| Cumulative token budget | `bamboo-tickets/src/execution_budget.rs`; `bamboo-engine/src/ticket_token_budget{,/provider}.rs` | #2262 and Native integration already exist. Only canonical Ticket HEAD stores limits, reservations and cumulative spending. Unknown usage retains its reservation. Rounds are an additional run ceiling, not money accounting. |
| Canonical worker and durable Inbox | `bamboo-engine/src/{ticket_runtime,ticket_worker_plan}.rs`; server `tools/ticket_dispatch_adapter.rs` | Existing native one-shot worker/Task/submission path, including stopped-process proof. This slice creates no worker adapter or second task queue. #1794 remains a broader open tracker. |
| Wait, directory and lifecycle integration | Engine `sdk/tests.rs`; server `app_state/{parent_question_reconcile,supervisor_directory_tests}.rs`, `tests/{native_work_acceptance,native_work_interruption,native_work_project_isolation,ticket_writer_lifecycle}.rs` | #1479/#1480 and #2265/#2269/#2280 have delivery records. Current-source execution results must be reported separately from those historical records. |
| Program patrol | `bamboo-orchestration/src/patrol/{host,program}.rs` and server `ticket_application/patrol_adapter` | Program observation/delivery calls no model. `host.rs` currently supplies `None` for started, heartbeat, progress and expected wait. Those missing facts remain unknown; a heartbeat cannot prove progress. |

Paths in the table are relative to each named crate's root. Source presence and
historical delivery do not prove the installed app is using formal Work.

## Owner protocol

`GET /api/v1/tickets/ready-policy` observes the process identity, control revision,
policy metadata, remaining receipt capacity and unfinished revocation. `active`
means only an unrevoked, unexpired scheduler lease; it is `null` when the scheduler
is busy and cannot be observed without waiting. `execution_progress` is unknown.
Observation never initializes a scope or installs authority. The control remains
readable/revocable when Work feature flags are disabled or scope health degrades.

`PUT` uses the existing Ticket owner-access gate, rejecting verified run-scoped
Worker credentials and unauthenticated remote access. It is never a model tool.

```json
{
  "process_id": "<GET process_id>",
  "operation_id": "owner-install-1",
  "expected_revision": 0,
  "action": {
    "kind": "install",
    "policy": {
      "binding": "<canonical ScopeBinding object>",
      "observed": "<canonical snapshot object: commit, seq, authority_epoch>",
      "project_id": "<ProjectId>",
      "grants": [
        {
          "work_id": "<WorkId>",
          "contract_revision": 1,
          "context_hash": "<canonical immutable context hash>",
          "workspace": "<existing ExecutionWorkspace object>"
        }
      ],
      "capacity": 1,
      "max_rounds": 4,
      "lifetime_secs": 60
    }
  }
}
```

Placeholders marked object must be replaced with actual objects. Revoke uses the
same top-level fields and `"action":{"kind":"revoke"}`, with a fresh operation
ID and the currently observed control revision. Revoke does not require a healthy
scope, enabled dispatch, a model call, or a new budget decision.

Install binds one Project and 1–64 explicit generation-zero Ready Works, exact
contract/context/workspace, capacity 1–3, a finite configured native round ceiling,
and a 1–3600 second lifetime. It requires preconfigured canonical Work/day/total
token limits with remaining capacity. It neither configures limits nor reserves
an estimated fee. Actual provider requests reserve and settle through the existing
native Ticket budget adapter, including conservative unknown usage.

An exact operation replay returns its original immutable receipt before revision
checks. It never reinstalls or extends permission, including after revoke/expiry.
Changed payload conflicts. New decisions use CAS on the process-local revision.
Receipts are bounded to 128 per process, with one slot reserved for revocation;
they are never evicted into a possible new grant. Restart rotates process identity
and retains no policy or owner-control receipt. Canonical Work, dispatch and
budget receipts remain in Ticket HEAD and are reconciled through existing APIs.

Revoke uses the existing scheduler cancellation/drain barrier. Already admitted
workers retain the normal explicit stop/ACK/resource protocol. If the HTTP waiter
is cancelled during drain, the pending revoke remains observable; retry that same
operation to finish it. New installations cannot pass an unfinished drain. A
receipt means an authorization decision, never worker start, progress or acceptance.

## Remaining independent production work

- #2049 owns missing runtime started/heartbeat/valid-progress/wait observations,
  bounded diagnostics and their UI projections. Do not fabricate those values or
  enable semantic model patrol because this owner endpoint exists.
- Canonical cumulative budget configuration is still a trusted Rust User/Host
  port. #2453 owns a separate owner HTTP setup slice; frontend setup remains separate. This
  endpoint deliberately refuses to invent an allowance when setup is missing.
- Existing finite recovery and stop reconciliation remain the #2046 path. This
  installer grants no new generation or replacement worker and does not resolve
  unknown execution by re-dispatching. Scope repair, legacy migration, UI and
  ongoing production operation require their own evidence and focused changes.
- No real external model, Windows/Linux native acceptance, product publication
  or installed-app replacement is performed by this slice.

## Current-source verification

The owner cases live in the existing `ready_work_scheduler` integration target,
with temporary canonical FileStore/SessionStoreV2/Inbox/Host, actual isolated Git
worktrees and an in-process controlled provider. Explicit initialization uses the
HTTP route against a previously existing empty Supervisor; no store or runtime
receipt is seeded. Work and Project setup uses the existing application service.
Budget configuration is an explicit test-native User decision through the real
ledger port. Submission is generated by the real worker, followed by exact owner
HTTP acceptance, rather than a hand-written runtime receipt.

Run focused cases with:

```sh
RUST_MIN_STACK=8388608 cargo test --locked -p bamboo-server --test ready_work_scheduler owner_control
```

The existing default `server-tests-1` shard owns all six owner integration cases;
the Worker-auth library case belongs to default `server-handlers`. No additive
feature-only case, separate workflow, or package configuration is added. The
existing `ticket-runtime-fixtures` cfg fingerprints the whole TicketApplication
source file, so its source digest and ownership routing guard are updated
together in `scripts/run-feature-tests{,.test}.cjs`.

The final Issue/PR records exact source and executed counts for the affected
crate suites and directory/wait/lifecycle regressions. Until those runs complete,
test presence is not a green acceptance result. Controlled provider evidence
does not establish autonomous model judgment or cross-platform acceptance.
