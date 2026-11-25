# Noninterrupting resident continuation v1

This design specifies a proposed `SubAgent` action, `continue_resident`, for
continuing an existing local resident Child under its saved assignment. The
action will be opt-in and is currently unavailable. [#2498](https://github.com/bigduu/Bamboo-agent/issues/2498)

## Request and authority

The action takes `version: 1`, a caller-generated `operation_id`, a canonical
Child reference with Session ID, birth and conversation generation, the
expected immutable assignment revision and content digest, and the expected
continuation checkpoint revision. Revision zero selects an initialized scope
that has yet to commit a continuation checkpoint. An input supplies bounded
follow-up text or selects one previously accepted pending input by its exact
receipt ID. Selecting a pending input requires a fresh operation ID and
explicit idle authorization. It never appends the input a second time.

Host derives the caller from the trusted tool context and resolves its current
direct-child relationship, Session lifetime, Project/workspace binding,
assignment revision, permission policy and approval ownership from durable
authority. The Host validates those facts again at each publication or claim
boundary. A resident name, source digest, checkpoint or receipt cannot grant
access. V1 requires a local ordinary resident with exact supported authority
and respects current revocation. Missing or stale proof is a conflict or
unavailable result. A Project, workspace, assignment or permission change
requires an explicit, separately authorized adoption with its own source and
expected revisions. V1 rejects the old continuation until that transition is
available and committed.

| Identity | What it selects |
| --- | --- |
| Child Session ID, birth, conversation generation | The logical resident and exact conversation lifetime. Reset or recreation invalidates an earlier selector. |
| Assignment revision and digest, source Session birth and source revision | Current authorization to use the immutable assignment and the identity of its source content. The digest alone is a content witness. |
| Source and target Project/workspace and permission revision | Expected authority checked against current Host state. Caller parameters cannot change the Child's tools, role, credentials or approval policy. |
| Operation ID and exact Inbox receipt ID/generation | One admitted input in the caller and target lifetimes. Delivery, consumption and Run creation each require their own evidence. |
| Child launch generation and Actor activation ID, Run ID, attempt, lease owner/epoch | The scheduler generation and fenced execution that can consume the input, publish a checkpoint or finish. Recovery checks current ownership again. |
| Continuation checkpoint revision | Monotonic saved progress within one Child conversation generation. The Actor's own per-attempt checkpoint revision still fences its writes. |
| Worker placement | Recyclable execution capacity. The durable owner and receipts survive a Worker replacement. |

The Host binds the operation to canonical caller and Child lifetimes and a
digest of the full validated request, including exact input bytes or pending
receipt selection. Retries use the permanent Inbox delivery receipt and exact
activation binding. An identical authorized retry returns the original
receipt and any Run identity committed by that operation. Reusing a key with
different inputs or selectors conflicts. Replay checks current access before
disclosing a receipt and cannot turn a delivery into an activation grant.
A new lifetime or conversation generation requires fresh selectors and a new
operation.

## Transition and recovery

| State proven under the existing owner guards | Commit and response |
| --- | --- |
| Exact current Run is busy, including a durable wait | Append one continuation input with wait-respecting `QueueOnly` intent bound to that exact Run. Its current owner may consume it at the next permitted safe point. Return `queued` with the delivery receipt. Preserve the Run, tool pairs, Child and Bash waits, user and approval waits, and permission decisions. Never interrupt, resolve a wait or request an activation from this receipt. |
| Proven idle, or terminal predecessor with all waits and effects reconciled | Stage or locate the exact receipt. Claim it with compare-and-swap under the existing launch and Actor ownership gates, binding it durably to one launch generation and Run attempt. Reload and recheck the exact predecessor, receipt, checkpoint, Project and permission authority after the claim and before scheduling. Return `activation_reserved` only for a committed binding. A competing claimant can read the same binding and cannot start a second Run. |
| Restart, unresolved prior Run, unknown owner or partial publication | Retain the last confirmed checkpoint and the input. Return `pending_reconciliation` with a confirmed receipt when one is available. Recover the exact predecessor and inspect actual Inbox, checkpoint and activation state before considering a claim. Missing local Worker, lease expiry or stop ACK alone cannot prove that a Run terminated, a wait was released or its effects stopped. |

The Inbox receipt is published with the immutable input and its original Run
binding. Use the existing `SessionInboxActivationIntent::for_expected_run`
semantics for an active consumer. The continuation delivery and its replay
must identify this specific origin so that ending the Run leaves a pending
input available for reconciliation. Ordinary `QueueOnly` excludes its input
from wake scans but allows a later independently started Run to consume it.
An unrelated `TriggerTurn` must leave a pending continuation untouched.

A new explicit idle request may select the original receipt after proving
quiescence and reconciling the old claim, durable consumption cursor and ACK.
Under the same owner protections it commits one compare-and-swap from that
exact unconsumed receipt to an exclusive successor claim. Keep the original
input ID, receipt and intent. The claim alone supplies the successor's right
to consume the one selected item, with no generic Inbox wake or staged-prefix
release. Existing Inbox and activation boundaries own this future binding.
The scheduler may recover a committed idle claim under the same live
authority. Reconcile a lost reservation's exact attempt before any successor
can consume its input. Checkpoint metadata, a backlog watermark and an Inbox
ACK are insufficient to authorize a launch.

## Checkpoint publication

The proposed payload contains short facts, decisions, typed evidence references
and next steps. An evidence reference carries immutable revision and digest and
states whether the conclusion is Host-verified or writer-reported. It never
embeds referenced bytes, a full transcript, hidden reasoning, secrets, tool
credentials or approval tokens. Proposed v1 limits are 16 KiB for the complete
escaped JSON, eight evidence references, and 1 KiB of UTF-8 per text entry.
Reject an oversized or invalid checkpoint as a whole and keep the previous
revision. Evidence reads require the normal scoped, bounded resolver.

Store the payload with the target Session birth and conversation generation,
assignment/source revision and digest, expected previous checkpoint revision,
associated input receipt, and the exact Run/attempt lineage. Publish through the
canonical Session Store using its existing lifecycle and writer guards and the
current Actor fence. The Store verifies the expected previous revision and
current authority at the real replacement boundary. The same next revision
with the same complete payload and provenance is an idempotent readback. A
stale writer, different payload, expired or replaced attempt, changed source
or reset generation conflicts. A delayed save or replay from A cannot overwrite
B's checkpoint or authorize B's Run.

The checkpoint records app progress independently of input admission and
consumption. The existing Inbox claim, durable User/cursor write and ACK govern
consumption in their established order. The checkpoint records which committed
input it represents and never acknowledges a pending claim. An Inbox ACK does
not establish provider execution or successful checkpoint save. The [owned
input checkpoint](actor-owned-input-checkpoint-v1.md) started as a storage-only
primitive. The current Actor adapter uses it to publish a durable User and
cursor on supported input paths. A resident continuation needs its own
authorization, checkpoint and end-to-end consumption acceptance.

| Failure boundary | Required recovery |
| --- | --- |
| Cancellation or failure before Inbox or checkpoint replacement | Preserve the prior value. Reload and retry the same operation or expected checkpoint revision under current authority. An already published input remains pending when a later checkpoint write fails. |
| Replacement may have succeeded before error, cancellation or response loss | Treat the outcome as unconfirmed. Reopen storage and compare the exact receipt or complete checkpoint provenance, including payload digest. Honor a confirmed commit and retain its revision. Carry forward a prior revision only after a read confirms it is still current. |
| Inbox published, activation not claimed | Retry by exact receipt and recheck the intended transition. A busy receipt remains queue-only. A fresh authorized idle claim can reserve an unconsumed input once. |
| Claim committed, process fails before enqueue or reply | Recover the same binding with the existing scheduler and current authority. If a Run may have started, reconcile that exact attempt first. Preserve unresolved tool pairs, waits and inputs. |
| Consumption committed, ACK uncertain or caller replays after restart | Reload the durable input cursor and receipt under the current fence. Finish or recognize the ACK without appending another User input or starting another primary Run. |

Input deduplication bounds Bamboo admission and recovery. External tool side
effects still require their own idempotency or explicit reconciliation.

## Compatibility

| Existing path | Behavior |
| --- | --- |
| Resident `create` with `context=reset` | It stops an active Run and waits for the stop before replacing its task/context. The committed owner still completes replacement when the caller cancels after stop. |
| Resident `create` with `context=accumulate` | It stops an active Run and waits before appending the next task. Its existing history, permission inheritance and wait behavior continue. |
| `send_message` with `interrupt_running=false` | It can deliver to the active owner without physical cancellation. Current `SessionMessenger::send_with_gate` uses `InterruptSpecificWait`; this path supplies no guarantee that every wait is preserved. |
| `continue_resident` v1 | It keeps the active owner and its waits, and activates only after an independently proven idle claim. Existing callers must use the new action explicitly. |

Keep `resident_reuse_caller_cancel_after_stop_still_queues_replacement_task`
and the reset/accumulate and ordinary Inbox regression cases. Any future change
to legacy reuse needs its own migration and compatibility tests. The Worker
pool keeps its existing 300-second idle and pool-full reclamation behavior.

## Store and Host acceptance for implementation

Use real `SessionStoreV2`, Inbox files and ActorDirectory owners. Use an
assembled local Host with a held provider stream or tool barrier and a request
counter. Check saved receipts and Run attempts as well as the tool response.

| Fixture | Required observations |
| --- | --- |
| Busy ordinary Run, then Child, Bash, user and approval waits | Hold the actual original Worker. Use a matched control with no continuation to observe its normal in-flight behavior. Replay the queued request and observe one exact Inbox receipt. The same physical Worker and Run continue, existing tool call/result pairs and waits survive, and no stop, wait release or second primary request occurs. The legitimate coordinator releases its own wait. |
| Idle and terminal, with concurrent claims | Run two Host contenders against one Store and receipt. Race a competing activation and a changed Source/Project/permission at the claim boundary. Assert one committed binding and one primary request for the winner, conflict or replay for the loser, and zero launches under stale authority. |
| Restart around Inbox/claim/ACK | Cold-reopen after each real publication cut. Preserve unconsumed input when the old owner is uncertain. After proving its terminal state, send an unrelated `TriggerTurn` and confirm that its Run leaves the original receipt pending. Require a fresh explicit claim for that receipt. Replay after a lost reply or ACK and inspect one transcript input, one binding and no extra primary execution. |
| Checkpoint replacement and cancellation | Pause the actual Store before replace, after replace and before readback. Fail or cancel the caller at each cut, reopen storage, and verify the previous or exact committed revision. Replay delayed A after B claims or saves, after a reset and after source or permission drift. Require no stale write, wake or changed permission. |
| Unsupported authority and input | Exercise foreign or recreated Sessions, wrong Project, missing revision, changed replay, forged refs, overflow and unknown versions. Assert zero new receipts, checkpoint writes or provider invocations. |

## Prerequisites and implementation boundaries

[#1055](https://github.com/bigduu/Bamboo-agent/issues/1055) owns implementation
and depends on the [#1054 tracker](https://github.com/bigduu/Bamboo-agent/issues/1054).
The following contracts must be available and verified for the selected route.

- [#1342's required packet](required-child-context-packet.md) is accepted for
  the one-shot route. Reuse its immutable binding and final provider budget
  checks. A resident continuation must prove those constraints in its own
  assembled request before calling the provider.
- [#1343](https://github.com/bigduu/Bamboo-agent/issues/1343) owns scoped immutable
  reference reads using the [#1049 export](https://github.com/bigduu/Bamboo-agent/issues/1049).
  A repeated digest or path from the checkpoint does not supply read authority.
- [#1344](https://github.com/bigduu/Bamboo-agent/issues/1344) owns bounded result
  provenance and projection. The [sealed one-shot subset](explicit-child-result-inspection.md)
  does not prove resident result freshness or permit automatic parent delivery.
- [#2455](https://github.com/bigduu/Bamboo-agent/issues/2455) separately owns parent
  recovery, the committed completion witness, parent Inbox authorization and
  rollback. [#2448](https://github.com/bigduu/Bamboo-agent/issues/2448) repaired
  terminal consistency. Any future automatic parent delivery must consume an
  existing committed source and pass #2455's boundary.

Before implementation, file focused children beneath #1055. Give each a 4–8
hour estimate, explicit files and contracts, non-goals, an isolated worktree
and exact head/base review with runtime evidence. First isolate the Host Inbox
and existing activation claim in the SubAgent, engine and Store ports, using
the busy, idle and cold receipt fixtures. Leave checkpoint writes and parent
delivery out of that change. Then isolate the fenced checkpoint CAS and its
real Store fault tests, without registering a production caller. Integrate
the two through an assembled Host only when both accepted contracts and
assignment/provider proofs are available. Split a new provider rollover,
authority adoption or another independent persistence/recovery protocol into a
separate issue before implementation. This design requires no release, remote
placement, resident mass migration, new queue or changes to Worker reclamation.
