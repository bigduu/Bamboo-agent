# Fresh calls, retained operations and scoped Session authority

Status: first bounded source slice for #2149, based on protected dev
`d694346aa42a688f204bb8e9fd50e573790361a2`. Independent of #2145/#2146.
Merged #2073/#2075/#2092 remain the Root creator, scope and versioned dispatch
implementation. This slice adds no second lifecycle, store or grant issuer.

## Delivery order and current boundary

1. Freeze replay before switching fresh `spawn_session` semantics. Capture a
   role-neutral caller and the exact original operation at the real admission
   boundary. SDK, Server and Connect share one retained-owner resolver. Introduce
   a role-neutral Root-creation Host port with the existing Supervisor adapter.
2. Install explicit scoped Host authorization for ordinary Root/Child callers,
   then switch fresh calls in supported legacy Sessions to independent Root
   creation. `SubAgent` remains the explicit bounded Child route. Complete
   catalog/final dispatch/disabled/permission coverage together. Discovery B1
   (#2148 / draft #2151) exposes an injectable port and the existing Supervisor
   adapter; send continues to reuse #2000 Inbox receipts and #1976 control.
3. Conversation reset has separate ownership (#2147 / draft #2150). C1 only
   classifies existing clear failures. Actual reset requires a fenced idle
   transaction, generation/readback and provider-payload acceptance; no
   delete/recreate or removal of the current Actor guard.

This first slice does **not** switch the legacy fresh-call alias, give ordinary
callers cross-Root grants, or complete Supervisor history reset. Those goals
remain open. No prompt-only change or alias-only fix is claimed.

## Immutable pending-operation contract

`SessionCallerReference` captures the executing Session's ID, birth, role/ancestry,
identity, tool revision, behavior version and workspace. Cosmetic title changes
are excluded. It is an observation, not authorization; Hosts must check current
Project, target, permissions and writer scope independently.

The host result writer stores `permission.operation.v1` only in message metadata:

- Original caller observation, complete original ToolCall (including raw argument
  bytes and spelling), and the selected exact registered execution owner.
- Exact result message ID and server-issued permission generation.
- Version 1, with strict decoding. Result payloads cannot supply the record.

Restoration checks caller lifetime/authority, one result occurrence, the newest
matching result, its unique preceding Assistant call and unchanged argument
bytes. It refuses duplicate result IDs, a later reused call ID, changed owner or
arguments, malformed records and changed generation before installing grants or
entering an executor. A second permission gate preserves the operation and only
rotates the generation after validating the previous approval and next request.

A valid record resolves only to its installed exact owner. A newly registered
custom alias or future fresh-call alias cannot redirect it. The exact dispatch
wrapper revalidates the canonical Session and Root gates without resolving the
original alias again. The original spelling/arguments still reach owners that
need legacy argument normalization.

Old Supervisor host bindings retain their existing validated read path, without
migration. An old spawn pending lacking a complete host binding is readable but
refused with an explicit resend instruction; its markers/history remain intact.
No Root or Child is guessed. Other legacy calls retain their established
compatibility path only when a typed retained owner agrees with the resolver.

Permissions, generation-bound once grants, Plan, disabled tools, compatibility,
canonical birth and old Supervisor proof checks remain authoritative. A receipt
never proves task completion. An executor error after dispatch is not proof that
no side effect happened, and this slice does not automatically retry it.

## Role-neutral Host seam

`SessionRootCreatePort` accepts a `SessionCallerReference`, explicit Project and
Host-bound workspace, original scope revision, stable operation UUID and first
instruction. The initial `SupervisorRootCreateAdapter` first revalidates the
caller, then delegates to `SupervisedRootCreatePort`. Existing locked birth,
Project, workspace, management CAS, Inbox and recovery checks remain in that
writer. Ordinary callers get `session_root_scope_unavailable` from this adapter;
metadata, tool arguments and names cannot manufacture a grant. A later scoped
Host adapter must provide authorization; removing singleton guards is not that
implementation.

## Validation boundary

Only direct rustfmt parsing/checking, diff checks and source/fixture inventory
are allowed in the current window. New Rust test source covers role/version
bindings, argument/owner/occurrence/generation tampering, durable serialization,
second approval gates, legacy Supervisor records, unknown legacy spawn refusal,
SDK alias shadowing, typed-answer payload injection and unbound exact gates.
These tests have not been compiled or run and are not RED/GREEN evidence.

Before admission, obtain the parent's Rust window, run the affected Domain/Core/
Engine/SDK/ServerTools/Server tests and the existing replay/permission/Root-create
regressions with controlled providers and isolated data, then original-policy
lint and the required compile/review checks. Do not merge this source-only draft.
No real-account Session, user history clear, paid provider, security change,
cache copy, release change or deployment is part of this slice.
