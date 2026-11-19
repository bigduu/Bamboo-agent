# Owner patrol status

The owner can inspect saved patrol bookkeeping for one current Work attempt with
`POST /api/v1/tickets/patrol/status`:

```json
{
  "project_id": "<current Project ID>",
  "work_id": "<Work ID>",
  "assignment_id": "<current Assignment ID>"
}
```

Unknown request fields are rejected. The Host derives generation and the original
DispatchSpec authority epoch from the canonical Ticket ledger and returns that
Scope. These IDs select a read; they confer no authority. The endpoint requires
the existing verified owner access. Worker run credentials and unauthenticated
requests cannot use it.

The response identifies the observed Ticket commit, cursor and execution-facts
revision, the selected Scope, and a bounded checkpoint summary.

| `state` | Meaning |
| --- | --- |
| `not_checked` | No checkpoint exists at the authorized path. This does not mean healthy. |
| `busy` | The existing directory lease is held exclusively. Retry the read later. |
| `available` | The saved checkpoint refers to the same execution-facts revision. |
| `stale` | The saved checkpoint predates the current execution facts. |
| `corrupt` | Existing bytes fail the checkpoint codec, checksum or semantic validation. |
| `unavailable` | The read cannot establish a safe, bounded record or namespace. |
| `unsupported` | The platform or a newer record schema is unsupported. |

A checkpoint ahead of the current Ticket cursor or execution-facts revision
fails with a conflict. A concurrent Ticket commit or Host authority change also
fails the read instead of combining different source snapshots. Budget-only
commits can advance the Ticket cursor without making unchanged execution facts
stale.

The summary includes saved checkpoint version/source/cursor, last/next check
times, incident identity/reason/phase, diagnostic attempt count and latest
diagnosis time, whether accounting settlement is pending, and escalation
identity/reason/delivery count/queued/next-delivery time. The response excludes
diagnostic notes, evidence bodies, model requests, reservation receipts and
provider configuration. It is capped at 8 KiB.

These are saved policy facts, not a current runtime-health claim. An incident
with a resolved phase remains visible as history. `queued` means the decision
item was staged in the Supervisor Inbox; it does not prove consumption,
acceptance or recovered progress. A saved next-check timestamp does not prove a
scheduler or worker is currently active. Missing start, heartbeat and effective
progress measurements remain unknown.

## Read and persistence boundary

The reader opens existing namespace components through directory descriptors
without following symbolic links. It briefly attempts a nonblocking shared
lease on the same directory inode used by the existing exclusive writer. It
validates the named lock and record identity, file type/link count and byte bound,
and decodes the existing v1/v2 checksummed record. Missing, unsafe or unreadable
state is never replaced with an empty healthy checkpoint.

The reader creates no directories, lock files or records, performs no fsync,
repair, reconcile, ACK, reservation, model call or worker activation, and writes
no Ticket, Actor or Inbox state. It does not recover an execution grant from a
checkpoint. Blocking file I/O runs outside Ticket locks; fresh Host and canonical
current-attempt checks surround it, including absent/busy/error results.

## Validation ownership

The physical cases remain in the existing checkpoint test target. Host/HTTP
cases use the default server library owners and controlled local providers; no
new workflow, feature context or paid model is introduced. Local iteration uses:

```sh
cargo test --locked -p bamboo-server --lib patrol_adapter::
cargo test --locked -p bamboo-server --lib handlers::agent::tickets::patrol::
cargo test --locked -p bamboo-server --test patrol_checkpoint_store --test patrol_ready_assembly --test patrol_runtime_observation
cargo fmt --all -- --check
```

Current-head feature CI owns the complete affected Unit/E2E acceptance. Focused
local results, hosted runtime results and a later product release are separate
evidence. Semantic patrol model/provider, Jiandu references and remaining
heartbeat/progress production sources stay under the parent trackers.
