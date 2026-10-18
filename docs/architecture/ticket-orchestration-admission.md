# Ready and Patrol integration boundaries

This finite batch combines the tested Ready and Patrol components for normal
protected `dev` admission. It is separate from both already fixed publication
batches and does not include the paused Workflow #2033 work.

## Frozen inputs and order

The integrator owns `bamboo/integration/ticket-orchestration-admission-20261018`, starting
from the freshly observed protected dev commit
`86c549b7dcc6b15d18dc1ea026aa366296a53e97`. Original feature heads remain intact.

| Slice | Exact head | Dependency |
| --- | --- | --- |
| #2051 native Ready worker | `988d4b27da02c7c25afb355058baf940073bf26b` | #2044 claims + #2045 Project/context |
| #2059 durable Ready retry | `3fe552927e68680721ef26763c63261cc974dc75` | #2051 |
| #2053 deterministic Patrol | `5e3a75897c2e5d4f11021bfc0447354a27d001d3` | #2051 |
| #2058 Host observation/checkpoint | `85b06adb4d155b96907a1eae53dbb5589d73b1f9` | #2053 |

Only the two top-level heads need normal merges: #2059 contains #2051, and
#2058 contains #2053. Both also preserve the original claims/Project commits.
The integration resolves the dependency and module-export unions while keeping
the production `ReadyWorkScheduler::with_recovery` assembly. Ready and Patrol
retain separate retry/checkpoint stores and stable identities. This batch does
not pull in later Sprint, pending-context or planning/control feature heads.

## Integration correction and new coverage

Subsequent source review found that the Ready deadline check ended before the
scheduler's queue-capacity wait. A wait could finish after expiry and then
commit a new queue entry. The correction carries a nonserialized, process-local
Host callback through launch preparation to the actual queue send. The callback
runs after capacity reservation, inside the same synchronous cancellation-gate
commit. A rejection keeps the gate pending and publishes no queue entry or
start announcement. The engine does not depend on orchestration; unchanged
callers retain their existing entry point with no additional check.

Three focused queue cases cover expiry under backpressure, rejection retaining
cancellability, and one successful commit with duplicate suppression. A separate
production-assembly test combines explicit Ready admission, canonical native
submission, the actual Patrol observation/open APIs and same-root restart.
It must prove retained retry/incident identity without a new grant, Assignment,
acceptance or provider call after restart. Written cases are not passing evidence.

Formal integration review also found that revocation could wait behind a blocked
preflight or queue admission. Each explicit installation now captures a
process-local cancellation generation before its first await. Revoke cancels and
rotates that generation, interrupts its pending waits, and drains only its old
admission calls. A new explicit installation cannot delay that completion barrier;
old cloned fences remain revoked after it. Six controlled cases cover preflight,
dispatch, ensure and installation waits, concurrent revokes and an abandoned
revoke while retaining the original durable request, attempt count and deadline.
Already queued execution keeps its canonical receipt and stop protocol;
revocation does not fabricate a stop or undo a Start.

The assembly fixture's restart boundary is a controlled child process followed
by a successful natural exit and same-root reopen in its parent. It does not
certify same-process application teardown. No lock deletion, replacement root
or fabricated epoch is allowed. The separate same-process reproduction and
ownership-drain acceptance remain open in #2062; this fixture cannot close them.

The earlier component results remain bound to their original sources: Ready's
33 orchestration and 19 file/native tests, and Patrol's 74 selected tests. They
are not summed as unique tests or relabeled as combined-source acceptance.
Run only newly affected coverage and necessary lint/metadata on the recorded
candidate after the F15 and Lotus fixture Rust units have naturally released.
Use owner-exclusive outputs and revalidate changed local crates.

## Delivery ownership and next small steps

| Missing delivery | Owner | Next bounded action |
| --- | --- | --- |
| Current candidate compile gate and exact head/base review | Integration owner, this branch | Submit the finite batch to `dev`; obtain real `Test` and Codex review, resolve findings, then use normal protection. |
| Actual authenticated policy installation control surface | #2042 Host/controller owner | Add one explicit owner-authorized finite policy command using canonical scope/Project/workspace and the existing install/revoke port. No policy is installed at startup or reconstructed from retry files. |
| Work start/heartbeat/effective progress/wait facts | Runtime producer with tickets core owner | Define verified operations bound to Work/Assignment/run/generation/epoch, reject stale updates, then expose an authorized projection. No timestamp is inferred from sequence numbers or ACKs. |
| Program-only Patrol tick and lifecycle | Patrol owner, with integrator wiring | A separate `patrol/program.rs` slice consumes observation/checkpoint ports, keeps model paths disabled, persists pending escalation, and shares one binding-level checkpoint writer. |
| Production cumulative budget and escalation delivery | #2042/#2049 policy owners and canonical ledger/runtime owners | Freeze atomic reservation and original-operation reconciliation ports before enabling any diagnostic provider or delivery attempt. |
| Jiandu versioned source consumption | Jiandu native owner plus Bamboo memory/Host owner | Publish no version implicitly. Keep the new native source contract pinned to its reviewed source; add Bamboo exact adopted-reference resolution only against an explicitly available compatible dependency. |
| Health HTTP and user controls | Tickets API and Lotus owners after verified facts exist | Preserve unsupported/unknown UI state until the corresponding producer and authorization contracts are implemented. |

The current server starts its Ready worker but installs no policy from a user
or Supervisor control surface. Tests and trusted Rust embeddings explicitly
install the policy. This batch provides that real native seam, not automatic
production dispatch configuration. Revoke remains the existing admission barrier;
already admitted execution retains its original stop/receipt protocol.

Patrol has no production start/heartbeat/effective-progress/wait timestamps;
those fields remain `None`/unknown. Actor activation heartbeats are not Work
progress. Recovery ACKs and queued escalation intents do not prove recovery or
Supervisor consumption. The checkpoint writer is exclusive per binding, so a
future loop must share one owner across Works instead of opening one per Work.

Bamboo currently pins `jiandu-memory = "=0.3.3"`. The reviewed native source
lifecycle API is not part of that pinned dependency. The consumer therefore
remains unassembled: no copied persistence, fallback to Global memory, automatic
adoption of an observed version, or new grant from memory. Ticket's immutable
DispatchSpec/context and runtime receipt remain the execution references.

## Durable Ready retry capacity

Retry metadata remains one `ReadyRetryStore` under its original stable file
lock. Format 1 stays readable with its existing bounded legacy map. Before a
new stable Work key is written, the adapter durably publishes format 2 in the
original `state.json` and syncs its directory. Original readers reject that
version rather than treating the new records as absent budgets.

The legacy map keeps every original key, revision, attempt, deadline, request
and quarantine; existing keys continue their original CAS there. New keys use
individual checksummed records of at most 64 KiB, addressed by the existing
scope/Work hash, without adding another authority or scanning all new records.
The old 1,024-entry and 8 MiB limits still bound legacy-file reads, but no longer
limit the installation's lifetime number of new Work keys.

Each individual key has a permanent empty sibling marker. Its file and parent
are synced before creating and syncing the key directory and publishing its
record. Missing/corrupt markers, key directories, records or the format-2 record
root fail closed; they never become a new retry window. Errors and cancellation
never remove initialized markers/directories or refund a published reservation.
Both the legacy and individual paths retain the original lock until blocking
file and directory synchronization ends. No expired or quarantined budget is
automatically deleted. Restoration of deliberately erased/rolled-back store data
still requires explicit Host repair; these bytes are not an execution grant.

## Admission conditions

Keep the candidate Draft until new local affected cases pass, the actual
protected `dev` compile-only `Test` passes, and Codex review for the current head
and base has no unresolved findings. Quick Checks and local read-only review do
not replace those gates. Head/base changes require fresh applicable review;
refresh dev only when a real conflict or strict protection requires it.

After protected admission, verify each component's original ancestry/postimages
against the accepted batch and close superseded PRs with that evidence. Do not
separately merge identical slices or close the broader #2042/#2046/#2050 scope.
Release acceptance and publication remain with the single publisher on a new
explicitly frozen batch; this branch never changes either existing fixed cut.
