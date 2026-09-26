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

Owned mutation APIs finish in a detached Tokio job holding those locks. Caller
future cancellation does not cancel that job; the operation may still commit,
and an owned retry or permanent receipt supplies the outcome. The host must keep
its Tokio runtime alive until mutation jobs finish. Runtime shutdown can abort
async jobs and has not been fenced or tested here; shutdown/drain and final
writer lifetime fences are prerequisites tracked by #1341. Production consumers
must not opt in before those runtime ownership boundaries are implemented.

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
