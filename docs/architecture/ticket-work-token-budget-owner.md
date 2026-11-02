# Explicit owner setup for canonical Work token limits

Issue #2453. Independent dev source starts at
`b6a6f5ecd468f41aa61b690e96ed4578f1457d3e`, including directory #2456 and
Supervisor wait/renewal #2458. Ready-policy owner HTTP control remains separate
in #2452/PR #2464. No current-user setting, store, legacy Supervisor, installed
application, release or gitlink is changed by this slice.

## Existing authority, new entrance

`bamboo-tickets/src/execution_budget.rs` already owns finite Work/day/scope
limits, explicit User configuration history, provider reservations and settlement
in canonical Ticket HEAD. Native provider attribution already reserves before
entry and conservatively retains unknown usage. This slice adds owner HTTP
configuration/readback over those mechanisms, without a new schema, ledger,
Task queue, allowance default, model-facing tool or executable grant.

`GET /api/v1/tickets/token-budget` uses the existing Ticket owner-access gate.
It returns the sealed binding, current snapshot/authority epoch, budget revision,
current configuration and bounded accounting counters. An unconfigured scope has
revision zero, null configuration/accounting, and no default allowance. The
endpoint never initializes the scope, rolls a clock, writes a checkpoint or calls
a model. An uninitialized scope requires the existing explicit initialization API.

`?configuration_revision=N` additionally reads one original configuration receipt
from canonical history. It returns an error when absent. There are at most two
configuration records in a response, each with at most 64 Work limits. Current
Work charges are likewise bounded; the at-most-1024 provider reservations are
counted, not copied into the response or replaced with a second index.

Accounting distinguishes pending usage, explicitly unknown usage, known usage
and retained over-cap reports. Pending/unknown reservations keep their full cap.
`accounting_halted` reports positive over-cap evidence. `daily_charged` belongs
to `accounting_utc_day`, which may precede today; a read does not roll the ledger
or infer a fresh allowance. Charged tokens are not a monetary estimate or proof
of task progress, completion, stop or acceptance.

## Configuration identity and fencing

`PUT /api/v1/tickets/token-budget` accepts:

```json
{
  "binding": "<actual binding object from GET>",
  "authority_epoch": 1,
  "expected_revision": 0,
  "limits": {
    "work_tokens": {"<existing Work ID>": 100000},
    "utc_day_tokens": 200000,
    "total_tokens": 500000
  }
}
```

Replace the binding placeholder with the actual object. Every limit is explicit,
positive and at most 1,000,000,000,000 tokens, using existing domain validation.
The 1–64 Work IDs must exist. Existing charged Works cannot be erased or reduced
below their retained charge. Mutation must be enabled explicitly; this endpoint
does not enable dispatch or configure providers/models.

The scope's next budget configuration revision (`expected_revision + 1`) is the
decision identity. No unrelated client operation UUID is accepted or forgotten
on restart. Current binding/User authority and authority epoch are checked under
the original Ticket transaction lock alongside budget revision CAS. Concurrent
identical decisions return one immutable `WorkTokenConfiguration`; changed limits
or a different User for the same decision conflict. Replaying an earlier decision
returns its original revision/limits/User/committed sequence without restoring
those old limits over a later configuration or rewriting accumulated spending.
The original trusted Rust configuration port keeps its existing strict CAS behavior.

Writer restart advances authority epoch. An old HTTP envelope is rejected.
An authenticated owner can observe the fresh epoch and read the original decision
by revision, or submit that same decision identity with the fresh authority fence
to recover its original receipt. Neither action installs a policy or readmits
unknown execution. Before/after-HEAD publication failures retain canonical store
health and require its existing reconciliation/reopen path when uncertain.
Configuration history stays bounded to its original 128 records; old decisions
are not evicted to permit new budget writes or reset unknown spending.

## Verification boundaries

Five canonical FileStore cases extend the existing `execution_budget` target in
default `bamboo-tickets`/`packages-0`. Four owner HTTP cases extend the existing
`native_work_token_budget` target in default `server-tests-0`; the verified Worker
credential rejection case belongs to default `server-handlers`. Feature-sensitive
source ownership is refreshed with its routing guard; four additive contexts and
their 20 explicit cases are unchanged.

The independent dev-based native fixture explicitly initializes a disposable
scope, creates/prepares Work and configures limits through HTTP. The standalone
budget producer used the existing trusted Rust policy port. The Ready owner
slice connects this same fixture to actual Ready GET/PUT and original receipt
replay, then exercises canonical Inbox/Host/Task submission and explicit HTTP
User acceptance. Each result must identify its actual standalone or combined
source; local diagnostic caller patches remain distinct from admitted source.
No fallback route or synthetic worker receipt is used.

Unknown-usage exhaustion, concurrent/changed/stale requests, scope/Worker denial,
restart, original receipts and no automatic re-dispatch have controlled coverage.
Exact executed commands/results belong in the Issue/PR evidence; source presence
alone is not an acceptance result. External real-model judgment, additional
native-platform diagnostics, optimized macOS unwind acceptance, UI and missing
started/heartbeat/valid-progress measurements remain separate. #2469 supplies
verified current wait observations; these are not progress measurements.
Canonical FileStore supports macOS/Linux, without Windows store authority.
