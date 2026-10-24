# Conversation generation at recovery and control-plane boundaries (C3b2)

This bounded implementation stacks on C3b1 #2163 /
`b7a96c0f99d01c7356a7c10fb8ff2b35c72f09a0`, under #2152. It adds
recovery and original-snapshot fences. It does not expose a reset producer,
grant idle authority, change #1350, stop an Actor, or clear actual history.
The activated-idle Supervisor reset transaction remains unimplemented.

## Task journal compatibility and operation ownership

New two-Session Task undo journals use version 2. Each target carries a private
`TaskConversationWitness` containing its birth token, kind, normalized Root id,
parent id/birth, depth, Project, and complete optional conversation ledger.
It contains no transcript, provider context, Task payload or caller permission.
Normal unrelated runtime/message/metadata changes do not change this witness.
Birth, authority identity (included in the birth token), scope or any receipt
change prevents undo. The existing Task fields remain the only fields restored.

The reader accepts exactly these version/shape combinations:

| Journal | Recovery contract |
| --- | --- |
| v1, both witnesses absent | Preserve existing legacy recovery only while each canonical target has absent conversation state. No new birth evidence is invented. |
| v2, both witnesses present | Re-read each canonical target and require its original witness to match before changing either target. |
| v1 with witness, v2 missing witness, unknown version | Reject as invalid journal shape before recovery publication. |

The immediately preceding runtime only accepts Task journal v1, so it refuses
a retained v2 journal rather than interpreting its undo under the old contract.
This is a source-level compatibility boundary; no mixed-runtime test has run.
The C3a compact Main v3 and Root proof v3 boundary remains independently needed
for generation-bearing canonical Sessions.

Recovery still owns the existing cross-process Task exclusive lock. It loads
Main/runtime with their exact generation, prepares both undo targets before the
first sidecar write, and revalidates the target at each physical restore.
Witness conflict uses typed `SessionAuthorityConflict` / `WouldBlock`.
Unrecoverable journals remain present and the existing recovery-required gate
continues to deny operational access; conflict is not successful recovery.
No lock order, marker rename/commit protocol, Actor claim or cancellation
behavior changes in this slice.

The single and paired Task CAS paths separately compare the original supplied
generation with their fresh canonical control planes before any journal or
sidecar publication. The narrow Task-owned equality check alone cannot prove
that the original Session came from the same conversation. Paired updates
validate both original generations first. Within the same conversation they
continue to patch only Task-owned fields onto the latest runtime snapshot.

## Additional implemented seams

| Boundary | Change and limit |
| --- | --- |
| Task strict execution and recovery readers | Compare canonical Main/runtime generation before overlay, and prepared undo generation against Main again before physical writes. |
| Retained copy rollback marker | Before index hiding or directory removal, require an available real Main/runtime pair, compatible identity, exact pair generation and absent conversation ledger. Copies are independently born zero-generation Roots. A legacy rollback marker cannot delete a later reset Root. Existing zero-generation ownership semantics are not retroactively upgraded. |
| Supervisor missing-proof migration | Refuse any present conversation state when reconstructing an absent Supervisor identity proof, including when its migration marker is absent. The existing C3b1 nonzero Root tool-proof migration refusal remains. Missing proof is not repaired into new reset authority. |
| Supervisor management | Check the original loaded management Session against current canonical generation before proof/runtime publication. Stable management identity/scope itself is not revoked or rebound by reset. |
| Parent answer canonical pair | Compare Main/runtime ledger before overlay. Existing parent-request, Child Actor, exact raw pair and final ownership checks remain. |
| Child wait registration/CAS | Shared incarnation validation additionally requires exact original generation before status lookup, adoption, new messages, mutation or cache publication. Otherwise a fresh `latest` could carry older caller messages through a default final fence. |
| Scoped Loop committed-pair reader | Compare Main/runtime ledger before overlay in the retained-input validation path without a Root Actor proof. This pair check does not establish a fresh original-operation observation or a general idle proof. |
| Bound Root runtime/admission reads | Validate the original `RootActorRuntimeWrite` observation and identity against the materialized Session, with the existing Actor fence checked before and after the read. A newer lease or canonical read cannot refresh the original observation. No cache is published on refusal. |

Runtime sidecar startup migration already uses lifecycle -> Task -> Session
guards and refuses reconstruction of an unavailable Root through existing
Root-context validation. C3b1/this slice prevent nonzero missing-proof repair.
Generation-bearing Child state is invalid. This does not authorize a future
reset recovery transaction to use legacy migration as a replacement writer.

## Synthetic coverage and evidence limits

Eleven new test functions are supplied as source:

- Three pure Task witness tests: unrelated runtime tolerance, exact receipt/
  scope/birth comparison with no history in the witness, and legacy-zero or
  invalid-state refusal.
- Eight storage tests: stale single/paired Task CAS; both undo targets
  preflighted before writes and legacy nonzero refusal; stale copy rollback;
  missing Supervisor proof; legacy v1 zero-state recovery; stale Child wait
  without publication or caller adoption; and mismatched parent-answer and
  Scoped Loop pair overlays.

The existing Engine bound-admission fault matrix is also extended with an
otherwise valid changed-generation payload for both admission and runtime
reads. It is a controlled read-port fixture, not an actual production reset.
The negative Task preflight fixture calls the private rollback helper under its
real Task guard; it does not claim a full crash/fault recovery run. The legacy
v1 fixture supplies source for a full orphan-journal recovery in a fresh TempDir.
All fixture files and stores are synthetic; no real account/history is touched.

All eleven tests, the expanded Engine matrix, inherited tests, typechecking,
Cargo/Clippy, mixed-runtime recovery, fault/cancellation, provider payload and
cross-client acceptance remain **UNRUN** under the parent build restriction.
Direct rustfmt and diff checking provide static evidence only. This slice is a
Draft dependency, not protected admission or reset readiness.

## Remaining gates before a reset writer

The existing generation fences do not prove that all operation admissions are
closed. The next finite audit must cover custom `RuntimeSessionPersistence`
overrides and Core/SDK/Engine provider/tool admissions, broker/cold-reconciler
and scoped transport/Loop retained operations, control-plane readback/cache
callbacks, and any raw writer that can rebuild from current state after its
original caller observation was lost. A default final comparison cannot recover
that lost observation. Scope identity may stay stable, but an original operation
must not silently recapture generation from a new grant, lease or canonical read.

After that audit, the private idle reset transaction must obtain physical Host
closed-and-joined proof and canonical Actor/pending-operation evidence. Terminal
Actor status, expired lease, missing record or an absent local run is insufficient
proof of physical idleness. Reuse the existing Weak/per-invocation acquisition
and closed/shutdown-join lifecycle; do not rewrite it or delete/recreate a Root.

That transaction must first refuse retained/uncertain Task, copy and reset
journals, then hold the existing physical writer boundaries, revalidate exact
birth/generation/snapshot/operation ownership, and stage Main/runtime/proofs
with a durable recovery/quarantine contract. It must preserve identity, Project/
workspace/scope intent, management links, independent Roots, Tasks and unfinished
work. Active, pending approval and uncertain external operation states require
explicit refusal. This slice does not define such a witness or transaction.

Canonical success readback and a public history/detail/feed generation remain
separate work. A local UI fence cannot establish cross-client exclusion. The
controlled actual provider request must finally prove that pre-reset native/
model context and compression/prompt/response continuation cannot reappear;
serializer-only tests are insufficient. No current HTTP route is enabled for
activated-idle reset by these changes.
