# Owned Inbox storage leases

Storage foundation for #1340, split from runtime ownership tracker #1334/#1341.
No production engine or worker uses these APIs yet. The lease controls queue
mutation; it does not authorize transcript writes, provider requests, or tools.

## Explicit opt-in

`SessionInboxPort::claim_owned(target, limit, active_run_id, request)` upgrades
that queue to format 3, preserving both coordinator and interrupt generations.
The request contains an opaque consumer identity, trusted caller time and a
positive duration of at most one hour. Each independent consumer supplies a new
identity. No timer, renewal driver or automatic activation is installed.

The existing `claim` / `claim_for_turn` / `ack` APIs reject a format 3 queue.
Current producers and coordinator releases remain supported and preserve the
format. Older readers reject its headers. Upgrade is irreversible; rolling back
the binary cannot safely resume a leased queue.

## Storage authority

Claim, renew, reclaim and ACK hold the existing lifecycle shared lock and Inbox
operation lock, including its filesystem lock. A claim stores owner, monotonically
increasing message epoch, expiry, incarnation and the item's effective activation
policy in the existing queue wrapper. Semantic ID, envelope, delivery generation
and original activation intent remain unchanged. A later immediate Interrupt
message cannot promote an earlier staged or Respect sibling.

Owned mutation APIs retain their detached Tokio transaction for caller-cancellation
compatibility. In addition, every already-started filesystem mutation reached by
owned claim/renew/ACK or inspection quarantine owns the original lifecycle shared,
process operation and Inbox FileExt guards until its complete synchronous job ends
(#1352). Owned setup also retains the already-acquired lifecycle/process scope
through Inbox mkdir/open/lock acquisition; that same FD joins the final bundle.
Guards release in reverse order. No mutation job reacquires them.

Runtime shutdown may drop the async transaction and stop later phases. It does
not guarantee whole-transaction completion: separate ACT/INT, wrapper/rotation
and receipt/removal jobs retain their existing partial-state recovery rules. A
lost response is unconfirmed, and exact retry or permanent receipt supplies the
outcome. The private writer preserves file fsync, write-error cleanup, rename
failure temp residue and no parent-directory fsync. Windows uses the existing
true replace operation, without a remove-first gap.

Compatible delivery, coordinator GEN/INT/ACT publications, guidance cancellation
and producer-visible quarantine scans use that same private holder (#1353).
The holder does not call the owned-format upgrade: legacy and format 2 queues
retain their format, while compatible producers preserve existing format 3.
Runtime admission/renewal/writer release remain #1341; no production consumer
opts in through this storage change.

Ordinary producers acquire lifecycle → process → Inbox FileExt once. Supervisor
followup admission instead transfers its complete original relationship holder:
lifecycle → Task → sorted Supervisor/target Session locks → process → Inbox FD.
Each started acquisition/publication owns these actual guards until its std job
terminates, including error cleanup. Release reverses that order. Followup never
reacquires lifecycle or substitutes a borrowed lifecycle guard for its Task and
Session authority. Its strict relationship check still precedes exact-id retry.

The narrow synchronous Maildir writer runs within this physical job. Its pretty
JSON, generation-ordered filename, file fsync and hidden-temp cleanup match the
existing writer. A supplied AdmissionGate is an atomic permission gate, not a
filesystem lock: `commit` encloses the actual rename. Cancellation can leave an
allocated GEN hole; it publishes no message or admission receipt. No nested
async writer or blocking task outlives the holder. Other Mailbox callers keep
their existing asynchronous entry points. Watermark replacement retains its
different existing error/residue and Windows true-replace semantics; Maildir
rename retains its existing platform behavior. Neither writer adds directory
fsync or a remove-first replacement.

Exact semantic/activation-intent retry remains before gate and capacity checks;
capacity remains before GEN allocation. Interrupt authority still publishes
before activation authority. Separate started jobs may complete after runtime
shutdown while later async stages never start. A completed GEN or watermark is
not proof that a message was published. Guidance cancellation retains the exact
envelope as a permanent tombstone; a claimed item is not withdrawn. Existing
partial-state recovery rules and lost-response semantics remain unchanged.

An unexpired lease belongs to its owner. The same owner's repeated claim returns
the same incarnation without extending its expiry. `renew_owned` validates the
current owner, epoch and incarnation, and extends expiry without moving it back.
At or after expiry, a new claim increments epoch and rotates incarnation, even
when the requesting owner is the previous owner. Epoch exhaustion fails closed.

`ack_owned` validates the exact current owner, epoch, expiry, path, envelope, generation
and policy. An expired claim cannot ACK. Like the legacy API, the caller must
first durably checkpoint the matching typed input; this foundation does not add
an atomic transcript/worker release protocol. A permanent receipt binds the
terminal lease identity, so exact ACK retries remain valid after expiry/reopen.
An older incarnation cannot borrow the successor's receipt.
Renewal returns a new token; a pre-renewal token or changed expiry fails both
current ACK and terminal replay, even when its other identity fields match.

## Compatibility and crash boundaries

Format 3 is written into both existing watermark files. Activation is upgraded first with a committed interrupt snapshot. While the
interrupt file still has its legacy format, new readers use only this snapshot;
a failed old Interrupt writer cannot grant authority by changing the integer.
Then interrupt is upgraded to format 3, which its old parser rejects before any
write. Missing or invalid snapshots fail closed. An interrupted header upgrade
already fences legacy consumers. No extra journal
or automatic downgrade repair is introduced.

Before path rotation, the current queue wrapper is atomically rewritten with
the leased metadata and `session_envelope_owned_v3` kind. The old v2 decoder does
not recognize this kind, including in its ACK path, which never reads headers.
Then the message moves to a path containing its generation, epoch and fresh
incarnation. An old already-held ACK cannot remove it before or after rename.
If the process stops between those writes, an owned retry finishes the same
incarnation, or an expired reclaim advances it. The wrapper remains the sole
durable lease record. Payload decoding strips lease metadata before returning
the semantic envelope.

Lease scanning and rewrites use the existing bounded physical transport limit
(`min(8 × max_payload_bytes + 4 KiB, 32 MiB)`). A wrapper that cannot fit its lease
metadata fails without publication. Inspection is bounded by the requested limit
and configured claim batch cap; it exposes only generation, epoch, expiry,
expired status and reclaim count, never consumer identity or payload.

## Verification boundary

Tests reopen real disk through independent Store/Inbox instances and coordinate
operation barriers in one OS process. They cover live exclusion, renewal versus
expiry, ACK versus reclaim, stale ACK, terminal receipts, interrupted path
rotation, frozen v2 held-claim ACK behavior, staged messages and both policy
orders. These are not process-kill experiments or evidence of exactly-once
provider execution. Runtime queued/inflight renewal and writer/worker release
fences belong to #1341.

The focused native fixture parks actual std jobs, aborts callers and separately
shuts down their runtimes. It observes the inner implementation scope Drop while
independent lifecycle/Inbox FileExt probes and same-adapter process waiters remain
blocked. Renew-versus-reclaim uses a genuinely expired successor after renewal
and requires epoch2; ACK-first requires a terminal receipt and no epoch2;
epoch2-first stale ACK makes no publication. Setup/deletion, header upgrade,
wrapper/rotation, terminal retries, quarantine and failure cleanup reuse that
bounded matrix. Executed platform/results must be reported separately from this
source contract; these are not OS crash, remote parity or provider execution
proofs.

The compatible-producer fixture reuses those real two-Store and FileExt probes.
It parks acquisition, GEN/INT/ACT replacement, Maildir rename and hidden-temp
cleanup, cancellation, and each producer-visible quarantine entry. Capacity 1
tests independent producer/owned-consumer exclusion after caller abort or inner
runtime shutdown, gate cancellation and committed-gate replay, exact semantic
retry, cold inspection, GEN holes and epoch-1 successor claims. Followup tests
also probe Task and both exact Session locks while detach/scope, Project/version
changes and deletion/recreation wait behind the started job, then require fresh
authorization on retry. These fixtures are source definitions until their
executed platform/results are separately recorded.

Scope is five production cores (Inbox routing/holder, actual Supervisor guard,
Mailbox and its writer), plus a single private `v2.rs` reexport of the existing
SupervisorFollowupGuard. There is no new grant, journal, lease schema, automatic
format upgrade, production consumer opt-in or whole-async-transaction guarantee.
Legacy claim/ACK/drain, generic Mailbox callers and runtime lifecycle integration
remain outside this holder slice.
