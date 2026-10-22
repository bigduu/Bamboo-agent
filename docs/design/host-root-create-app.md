# Shared App orchestration for finite Host Root creation

This slice extends the existing `SupervisedRootCreator` flow rather than adding
another queue, operation store or Root constructor. Source dependencies are the
finite Host scope/writer sequence (#2162), fresh caller-port spawn (#2166), and
the parent's frozen canonical Root and observed legacy-absence writers. App
registration/Server and SDK builders belong to their separate assembly owner.

## Caller selection

`SupervisedRootCreator::caller_port()` returns `Arc<dyn SessionRootCreatePort>`.
`with_host_first_input(Arc<dyn HostRootFirstInputPort>)` installs the dedicated
create-purpose input adapter for that instance. The legacy creator continues to
implement only `SupervisedRootCreatePort`, avoiding ambiguous `.create()` calls
and preserving the existing Supervisor API.

The role-neutral wrapper first validates the original full caller against a
canonical execution-admission snapshot, including conversation generation. An
installed Host token takes priority for every role. Lookup errors, stale,
revoked or replaced grants and a finite grant without a creation permit never
fall back to broader Supervisor authority. Only never-installed absence plus a
genuine canonical Supervisor identity selects the old Supervisor flow. The
observed variant must retain the physical never-installed fence at each final
mutation/admission; an earlier None lookup is not a grant.

The Host scope response exposes only its finite exact operation UUID, Project
and canonical workspace creation permits and current revision. Neither caller
Project metadata nor management links create a permit. `created_target_rights`
must be empty in this slice; otherwise the result explicitly requires a separate
Host installation. Creating a Root, seeing a receipt and retaining an old link
never grants target inspection, send or management rights.

## One reservation and creation flow

Both authorities use the existing operation claim/store, workspace validator,
`build_candidate`, read-only Project prompt refresh, canonical Root writer,
repository merge, workspace publication and confirmed account event. Only the
final authority writer and Inbox adapter differ. No caller history, model,
permission posture or private prompt is copied into the independent Root.

For ordinary Roots and Children, the operation address is the Domain
`host_session_create_identity(original_caller, operation_uuid)`, bound to caller
ID and immutable birth. Genuine Supervisor operations retain the established
`supervised-root:v1` address and existing `(SupervisorReference, full_request)`
fingerprint across legacy/Host selection changes. Thus installing a finite Host
scope cannot allocate a second Root for an already pending Supervisor operation.

The ordinary/Child fingerprint is caller ID, birth and the full unchanged
request. No grant ID/revision, conversation ledger, tool-authority revision or
current caller workspace is part of that identity. The request itself, including
its original expected revision and raw workspace bytes, remains immutable. A
legacy raw path alias is resolved by the existing canonical workspace validator;
only those resulting canonical bytes are compared to the explicit Host permit.
The raw request is never rewritten to make a conflicting retry pass.

A pending reservation with deterministic target UUID and immutable birth is
saved before any Root side effect. A new Host operation passes `Some(revision)`
to the final writer. Only an exact claimed existing reservation may pass None;
that writer requires the canonical reserved Root to exist and match. Missing,
deleted, partial or uncertain Root state is a recovery blocker and never a reason
to recreate it with another birth or UUID. Terminal failure, corrupt receipt and
changed payload preserve the original truth and do not invoke a new creation.

Host retries use the existing non-pruning operation read while holding its
claim. Host terminal receipts deliberately keep `expires_at` absent, preserving
reservation identity across scope/conversation replacement. A matching retained
legacy receipt is pinned before any Host terminal return, including an expired
known receipt and a failed operation. A pin-write failure is explicit and does
not reinterpret the operation as new. Existing HTTP and legacy-only Supervisor
receipt retention remains unchanged.

## First input and receipts

`HostRootFirstInputPort` uses the existing Engine `SessionMessengerReceipt` and
`SessionMessengerError`; activation failures retain their real durable Inbox
receipt. The port must enforce create-purpose authorization under the canonical
publication/lifecycle/Task/Session/Actor/Inbox guards. General target SendPeer or
an old management link cannot substitute for it. A genuine Supervisor's
create-purpose stable message ID must preserve the old `supervisor_followup`
address so a legacy-to-Host retry cannot inject its first instruction twice.

With no concrete canonical input adapter installed, Host scope and creation
return `host_root_first_input_unavailable` before a reservation or Root write.
The never-installed genuine Supervisor path keeps its existing strict input
adapter. This slice does not silently activate a generic peer sender.

The shared flow passes the original caller/token, operation UUID, reserved Root
birth/context and unchanged instruction to the input port. It rechecks current
scope before that handoff; this preflight does not replace the port's final
physical guard. Canonical input admission is separate from activation and task
completion. A completed operation replays its retained receipt only after the
reserved Root is validated; it does not redeliver or reconstruct a lost Root.

Legacy Supervisor receipts retain `linked:true` and their established shape.
Host receipts report `linked:null`, `management_link_changed:false` and no link
revision: this route does not publish a relationship and does not guess whether
a previous legacy relationship already exists. Receipts are observations, never
scope grants or verified task completion.

## Validation and remaining delivery

Synthetic App fixtures reuse the existing TempDir Project/workspace/store and
Supervisor harness. They cover all three roles, explicit Host priority,
never-installed Supervisor compatibility, revocation without fallback, absent
input-port refusal before side effects, exact permit and workspace checks,
created-target-rights refusal, stable retry across scope replacement, old
Supervisor-to-Host reservation parity, unchanged raw requests, pending/missing
Root recovery, corrupt/failed receipt retention, and typed receipt forwarding.
The injected successful input port is explicitly a mock: it is not evidence that
a canonical create-purpose Inbox or provider run executed.

All new Rust tests, compilation, Cargo, Clippy and runtime acceptance are
**UNRUN** until the parent grants the shared Rust window. Direct rustfmt and
source/diff checks are static evidence only. Actual canonical create-purpose
Inbox installation, its observed legacy absence/generation path, builder/Base
assembly, lifecycle/restart/cancellation and controlled provider acceptance must
be frozen and tested before claiming end-to-end ordinary-agent Root creation.
No real Session, permission configuration, history or deployment was changed.
