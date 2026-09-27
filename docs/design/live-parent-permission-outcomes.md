# Live direct-parent permission outcomes

The actual Host creates a non-serializable scope for a forced typed permission
request. It binds the Child attempt, activation run, event epoch, reply ID and
one fixed 240-second deadline. Cancellation, ownership loss or an epoch change
invalidates it. Durable data cannot reconstruct that authority.

The canonical server reviewer delivers a redacted request through SessionInbox
with `InterruptSpecificWait`, commits its immutable typed transcript proof under
the existing Parent mutation lock, and then activates parent reasoning. The
original SubAgent child wait remains owned by its existing coordinator; re-arm
retains its registered and timeout timestamps. Child completion wins over a
stale finalizing runner.

A distinct immutable terminal message records one Approved or Denied winner.
Both messages are bounded to 8 KiB and protected from compression. The existing
typed-message preservation in SessionRepository full/finalized/checkpoint saves
retains them even after the bounded admission cursor evicts the request ID.
No Parent lock is held across Inbox or provider awaits.

Pending replay sends no reply and makes no additional model call. Missing,
conflicting or unknown proof denies; permanent Inbox receipts/delivery sequences
and the live scope's first-admission memo prevent record loss from creating a
fresh deadline. Storage or relay failure is unconfirmed and does not manufacture
a durable Denied. Only the exact recorded winner can be retried in the same
current scope, after fresh lineage and policy checks.

Support requires the actual AppState canonical V2 store and the same repository
and locked-store Arc wiring. Arbitrary destructive storage writers are not
supported. This does not resume a cold Child, impersonate a Human, create a broad
permission grant, or complete the full ParentRequest lifecycle in #1335/#791.
