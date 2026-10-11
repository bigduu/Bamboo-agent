# Native local Child runtime

Issue: #1794. Native Bamboo SubAgents use a Tokio task in the Host process by
default. Logical Root/Child Actor identities, canonical Session histories,
permissions, Inbox admission and durable completion remain separate.

## Selection

The default native route needs no cluster configuration, broker, Worker
executable, IPC directory or loopback connection. A stale `broker.json` or
`fabric_dir` does not activate the legacy infrastructure. Failure of the native
route is an error; it never retries through a process or remote backend.

Existing explicit `subagents.worker_bin`, `worker_args`, non-native executors,
remote/schedulable roles and Actor profiles retain their process/remote mode.
Explicit remote roles do not move unrelated native children out of the Host.
The worker binary itself explicitly selects the retained legacy nested runtime.
No cross-machine feature is added by this change.

## Ownership and publication

A Child first reserves its logical run, claims its canonical Actor and binds a
dedicated Child writer. This capability contains the exact activation, birth,
direct-parent birth, ancestry and Project identity. Root/default persistence
methods do not gain Child authority. Physical save, Inbox append/cursor/ACK,
cache and Actor-event publication revalidate this capability at the write
boundary. The existing independently authorized shared-Root Task control-plane
contract is unchanged; this does not claim to solve the broader Task/management
writer boundary tracked by #1343. A cancellation request and an expired lease are never stop proofs.

The backend retains the actual execution and coordinator handles. Its bounded
queue and running permit are held through canonical finalization. Shutdown
closes admission, drains accepted setup, cancels the owned runs and waits for
execution, resource, history-publication and coordinator completion.

A terminal local proof contains the exact resource-instance UUID, Child birth
and run, not a synthetic OS PID. Ticket stop checkpoints accept this typed
local proof while retaining the previous real-PID receipt format for the
process backend. Missing, stale or ambiguous evidence blocks finalization.

## Tools and permissions

The native path uses the Host's permission-checking tool chain with Child
context, not the Root role fence. Named/required-context/Ticket runs use a
positive native callable ceiling and disable automatic skill expansion.
Read-only and disabled-tool limits apply to direct calls as well as schemas.
Ordinary children receive the already-bound canonical SubAgent tool.
Permission review uses the existing scoped direct-parent request/decision
protocol. A reviewer boolean alone cannot authorize the operation.

Builtin Write/Edit work remains owned until its physical asynchronous work
finishes. Bash subprocess groups have an explicit per-execution owner; shutdown
kills and reaps the group and joins its output pumps. Dropping a tool waiter
does not pretend that a blocking write stopped. Unix process groups and Windows
Job Objects provide native process ownership. Unix stop evidence covers the
owned process group, not an arbitrary daemon that deliberately leaves it (for
example with `setsid`), and it does not undo external side effects. Windows keeps
the existing Git Bash/cmd shell selection, starts suspended before Job assignment, and requires
the owned Job to have zero active processes before confirming stop. Other
platforms reject scoped command execution before spawning it. Platform
acceptance is recorded separately; cross-compilation is not runtime evidence.

Configured lifecycle and portable hook commands use this same resource owner,
including their input/output capture. Their command timeout still waits for
physical termination and pipe completion. An interrupted custom hook, failed
cleanup, or escaped pipe holder makes stop evidence unconfirmed; nested builtin
tool calls cannot exempt a hook from this check. Legacy hook execution is
unchanged outside a native Child resource scope.

Custom/MCP tools preserve their inline capability scopes. Cancellation or panic
while such an invocation is outstanding makes its stop evidence unconfirmed.
Normal return relies on the tool's existing completion contract; arbitrary
third-party code that independently detaches work, or deliberately escapes a
process group, is not a sandbox guarantee. Shared Host MCP servers are not
killed as if they belonged to one Child.

## Recovery and failures

Normal completion, error and cancellation persist the real stopped instance
before publishing a durable completion source and making the runner replaceable.
An Agent panic, uncertain tool effect, failed final checkpoint/history barrier
or stale authority quarantines the run: no successful Child completion and no
successor takeover. Restart replays canonical Inbox work exactly once for a
previously stopped generation. A `running` local generation without a confirmed
stop remains blocked even after its Actor lease expires; it requires explicit
reconciliation rather than guessing that physical work ended.

Unknown stop observations are persisted by the same current Child writer,
bound to its run and birth, with a stable first-observed timestamp. They never
mint a stop receipt or release admission. If a coordinator exits before its
separate Agent task, the supervisor cancels and joins that task and drains its
owned resources before writing the final observation, so a late runtime write
cannot replace it with an older `running` marker. Failed diagnostic persistence
is reported and does not unlock the quarantined execution.

## Read-only stop diagnostics

Actor subtree snapshots keep the existing version 1 shape by default. Clients
can request `application/vnd.bamboo.actor-snapshot.v2+json` in `Accept`, with
`application/json` as their version 1 fallback. Version 2 uses a separate `as2-`
snapshot identity; both versions vary by `Accept`. The `at1` stream cursor keeps
its existing refresh semantics and grants no execution or recovery authority.

Version 2 adds only a nullable `local_task` observation to each node: a static
state/reason and observation/lease timestamps. It contains no private run,
resource instance, owner, PID, path, or error text. Only a current ordinary
native Child with matching durable birth and activation is eligible. A valid
stopped instance takes precedence over an expired lease; resource stop alone
does not imply business completion or cancellation. An explicit unconfirmed
stop remains visible even while the Host renews its Actor lease. A matching
live `running` observation whose lease expires is instead shown as
`lease_expired_without_stop`; generic or remote Actor lease expiry is not
classified as native unknown stop.

The optional `refresh_after_ms` is a one-shot transport hint for the earliest
remaining native lease deadline, not a polling mandate or part of snapshot
identity. The complete delay is bounded by the browser-safe timeout range;
stable expired evidence does not create a busy refresh loop. Query-time expiry
does not mutate storage or advance its cursor. Lotus accepts both strict wire
versions, preserves canonical business outcomes, and shows unconfirmed stops
without offering a forged success or automatic takeover.

The adjacent draft-publication, parent-wait rollback receipt and interrupted
steering journals tracked by #1328, #1330 and #1331 remain separate work. Their
current generic action paths are not presented as fixed by this backend.
