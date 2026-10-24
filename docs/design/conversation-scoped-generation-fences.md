# Original conversation for retained scoped operations (C3b4)

This slice stacks on C3b3 #2172 /
`7a6fba6531509f60fe88ebdeb6649926e5664663`, under #2152 / #2177. It changes
two Storage modules. It introduces no reset producer, HTTP route, Host scope,
Root idle capability, permission or lifecycle transition. #1350 remains.
Activated Supervisor reset is still unavailable.

## Implemented boundaries

`ScopedTransportCensus::read` compares the complete validated Main/runtime
conversation ledger before overlay. Malformed or mixed-generation pairs refuse
with typed `SessionAuthorityConflict` / `WouldBlock`. Existing Root, Project,
Actor witness and execution checks remain independently required.

The retained census's `check_current_live` and `require_same_snapshot` compare
their original Session against the fresh canonical census before evaluating
liveness or the narrow transport checkpoint projection. This detects a new
generation and changed receipts at the same revision. It does not refresh the
original observation from canonical data, a new lease, or a still-live logical
grant. Same-conversation unrelated message/UI changes retain existing behavior.

The wrapped delivery guard invokes that original-generation comparison before
delegating `matches` or `notify_committed_delivery`. Generation conflict therefore
cannot notify the retained logical owner through this wrapper. This read-only
census remains an expiry/refusal mechanism, not an idle grant or Root writer.
Legacy absent Actor witnesses retain their original live-router contract;
their absence is not evidence that physical execution has joined.

`ScopedLoopInputProof` captures `ConversationGenerationObservation` from the
original request Session at checkpoint entry, before canonical reads or mutable
merge. `validate_consumption` checks that frozen observation before the typed
consumption record, admitted cursor and immutable terminal-retry record. The
same proof is used by checkpoint validation, publication and the retained Inbox
filesystem check before ACK/retirement operations. Identical consumption data
and cursor cannot authorize a pre-reset operation in another conversation.
Neither successful readback nor a later failure replaces this original proof.

These comparisons do not add a new lock or change lock order. The existing
physical lifecycle/Task/Session/Inbox guards and owned filesystem jobs remain
necessary. A comparison alone cannot prevent arbitrary custom Host operations
from bypassing those boundaries.

## Synthetic fixture source

Six new test functions cover:

- Main/runtime mismatch and invalid state before overlay, with a matching
  generation-bearing pair accepted by the read predicate.
- Retained census versus advanced generation or different receipts at the same
  revision, asserting its original observation is unchanged.
- Wrapped delivery refusal before either underlying callback is invoked.
- Same-generation append/UI changes preserving existing delivery behavior.
- The actual input proof consumption predicate refusing a different original
  generation despite identical typed consumption and admitted cursor.
- Same-generation consumption acceptance while missing consumption/cursor still
  refuses through the existing predicates.

The fixtures create fresh TempDirs and selector/claim data without installing a
Supervisor grant, issuing a real Inbox claim, changing permissions, or executing
a production reset. The input proof fixtures exercise the production ACK
predicate; they do not establish a full actual ACK, activation or cancellation
run. An inherited committed-pair test remains unchanged.

All six new tests, inherited tests, typechecking, Cargo in every mode, Clippy,
physical cancellation/fault races and actual provider/cross-client acceptance
are **UNRUN** under the parent build restriction. Direct rustfmt and diff checks
are static evidence only. An independent review must bind its exact source.

## Next bounded implementation

[Root idle reset witness implementation](root-idle-reset-witness-implementation.md)
identifies the real retained JoinHandle/resource closure work and its safe
admission protocol. No Root idle witness or reset writer is supplied by this
slice. Scope intent stays stable across reset; each existing operation retains
its own original generation. A terminal status, expired lease, lost logical
registration, or local absence cannot substitute for actual task/resource
closure.
