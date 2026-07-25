# SessionInbox Router

`SessionInbox` is Bamboo's internal, durable message path between logical
sessions. It is a runtime capability, not a new HTTP endpoint. User, peer,
child-completion, actor-steer, and background-Bash producers all address the
stable `Session.id`; process ids, worker mailbox ids, warm-pool slots, and
execution run ids are never durable addresses.

## Components and ownership

- `SessionMessageEnvelope` is the typed domain contract. It carries the stable
  message id, source, target `Session.id`, kind, semantic body, thread/reply
  correlation, and retry metadata.
- `FileSessionInbox` stores one ordered Maildir beside the authoritative
  session. Delivery is bounded, idempotent, and coordinated across independent
  adapters/processes by a per-session file lock.
- `SessionMessenger` validates logical-session relationships, commits the
  envelope, durably authorizes the intended activation policy, and only then
  requests activation.
- `SessionActivationRouter` tracks the current logical owner, safe-boundary
  notifications, finalization, and one successor reservation.
- The runner's safe turn boundary claims an envelope, translates it to a
  provider-valid user message, checkpoints that message and its bounded cursor
  together, verifies the durable typed transcript marker, then acknowledges the
  claim.

The transcript marker is the unbounded admission proof. The bounded cursor is
an optimization and may evict old ids; an admitted tombstone without a matching
typed transcript entry never authorizes deletion of the recoverable claim.

## Bounds and trust boundary

The default per-session limits are a **256 KiB serialized envelope**, **1,024
pending or claimed envelopes**, and **128 claims per drain batch**. Limit
failures are observable and fail before activation. Child terminal fields use a
smaller inline budget and carry the full value's length and SHA-256 identity
when the displayed value is bounded, so an oversized completion cannot strand
a satisfied parent wait.

Message ids must be canonical, non-empty, at most 256 bytes, and contain no
path separator or `..`. Maildir and admitted-receipt filenames use fixed-size
digests; neither a message id nor a target supplied by a producer becomes a
filesystem path. The target must resolve through the authoritative session
store. A session source must also exist and have the same logical root as the
target. When both sessions carry a Project id, those ids must match; a
same-root/different-Project send fails before enqueue or activation. The
same-root fallback when either Project id is absent is only a rolling-upgrade
compatibility rule for older sessions.

Envelope kind, source, and body combinations are closed and validated. Content
must be semantically non-empty. Provider-facing metadata cannot supply the
reserved `session_message` proof: Bamboo writes and verifies that marker from
the canonical typed envelope. Logs and metrics identify ids, generations, and
failure classes, never body content or secret values.

## Delivery and activation state machine

1. Validate the envelope and authorization without logging body content.
2. Under the inbox operation lock, reject same-id/different-semantic-envelope
   reuse, enforce payload/backlog limits, allocate a monotonic generation, and
   commit to `new/`.
3. Publish the producer's monotonic activation watermark. `activation_generation`
   is the authoritative upper bound of the queue prefix allowed to execute;
   delivery by itself is inert. A consumer may claim only generations less
   than or equal to that watermark, including recovered entries in `cur/`.
   Newer staged generations cannot hitchhike on an older activation.
4. If a run owns the logical session, notify that owner. Otherwise reserve one
   runner through the host's canonical runner registry. Startup and retry use
   the durable activation watermark, not the latest delivered generation.
5. At a safe reasoning boundary, recover/drain into `cur/`, checkpoint the
   provider message plus admission cursor, verify the typed transcript proof,
   write a permanent tombstone containing the semantic digest, then remove
   `cur/`.
6. Before a run becomes terminal, mark its owner finalizing. If the router has
   a newer generation than the generation actually admitted by that run,
   reserve one successor.

`interrupt_generation` is a second monotonic watermark for the authorized
prefix whose explicit user/peer/runtime steering may clear a current reasoning
gate. It is written before the corresponding `activation_generation`, so a
crash cannot expose an interrupt-authorized prefix as the stricter policy.
Child and Bash completion producers use the strict policy: they can stage
several outcomes, but publish activation only after their durable wait policy
is satisfied. An activation request can fail after enqueue and watermark
publication; retrying the same message id and body reuses the original
generation and retries activation, while reusing the id with a different body
fails closed.

Cancellation between external runner reservation and owner publication uses an
asynchronous exact-run rollback handshake. Coalesced deliveries are released
only after rollback completes, so they cannot adopt an unlaunched stale slot.
The router records the last generation for which it genuinely launched a run;
a poison claim or persistent checkpoint failure receives one in-process
successor attempt, not an unbounded provider hot loop. A newer generation or a
process restart permits another bounded attempt while the original claim
remains inspectable.

## External actor admission handshake

An actor activation uses the same host runner reservation as every other
session run. After reservation, the host binds the delivery sink and claims the
entire currently authorized prefix in generation order. Before dispatching
`Run`, it checkpoints exactly one canonical provider message per claim into the
authoritative host transcript, in that order. This pre-dispatch checkpoint is
recoverable context seeding only: it does **not** advance the admission cursor,
write an admitted tombstone, or remove the canonical `cur/` claim.

`RunSpec` carries the exact logical session identity, a fresh
`activation_run_id`, the checkpointed context, and the ordered typed initial
delivery batch. The worker validates target id, run id, strictly increasing
generations, and the local activation watermark. At its first safe boundary it
admits the batch into its local checkpoint before the first provider request,
then confirms each envelope in order with its id, generation, target, and run
id. Live deliveries use the same typed path and local watermark policy.

The host accepts only the next confirmation for the current run and canonical
claim. It then checkpoints the admission cursor, verifies the exact typed
transcript marker, writes the permanent tombstone, and acknowledges `cur/`.
Stale, reordered, or mismatched confirmations cannot delete a claim.

If confirmation is lost, the durable host transcript and still-present
canonical claim are the reconciliation facts. A warm worker reloads its local
checkpoint before reconfirming. A replacement worker, including one with an
independent local store, receives the same canonical id in the next `RunSpec`
and preserves one context entry. The host's pre-dispatch exact-marker check
also makes a retry non-duplicating. Only a matching confirmation advances the
cursor and clears the backlog, so worker replacement cannot convert a
network-level acknowledgement loss into either duplicate reasoning context or
message loss.

## Suspended sessions and completion producers

Strict activation leaves a target inert while a specific durable
`waiting_for_children` or `waiting_for_bash` ownership record remains. Explicit
interrupt-authorized steering clears only the current reasoning gate
(`status`, `suspension`, and `runtime.suspend_reason`) before runner
reservation. It deliberately preserves the durable wait owner: later terminal
events still have one coordinator, and end-of-run bookkeeping re-suspends the
session if that wait is still armed.

Background Bash completion first admits a typed envelope. If sibling waited
shells remain, it leaves the wait armed and performs no activation. The final
shell clears the Bash wait durably and then requests exactly one activation, so
multiple shell completions form one ordered backlog and one final wake.

This issue covers waits persisted and reconciled by the host. Transporting a
new `Suspended` state originating inside a nested actor worker, and propagating
that ownership end to end through the actor protocol, remains explicitly
deferred to existing **#685**. The router and actor tests here do not claim that
worker-originated nested suspension is implemented.

## Rolling-upgrade compatibility

The following legacy ingress remains temporarily readable:

- `pending_injected_messages`: deterministically converted to typed runtime
  envelopes, durably delivered, then CAS-cleared. `created_at` and `attempt`
  are retry metadata and are excluded from the idempotency-content digest.
  Target, source, kind, body, thread/reply edges, and correlation are immutable.
- `ParentFrame::Message` and broker `InboxKind::Steer {text}`: converted by the
  worker to a typed runtime-instruction envelope in its local durable inbox.
  This old path has no canonical host claim/admission confirmation and should
  be treated as active-run compatibility only; current host producers use
  canonical `ParentFrame::SessionMessage`.
- `RunSpec.logical_session = None`: accepted only for older hosts by generating
  a per-run fallback id. Current hosts must send the exact logical child,
  parent, and root ids.

Structured compatibility telemetry never contains message bodies or secret
values:

- `session_inbox.legacy_pending_ingress`
- `session_inbox.legacy_broker_steer_ingress`
- `session_inbox.legacy_actor_text_ingress`
- `session_inbox.legacy_runspec_identity_fallback`

## Observability

`SessionMessagingMetricsSnapshot` exposes delivered/rejected totals, invalid
envelopes, authorization failures, payload/backlog/storage/activation failures,
active notifications, reserved/coalesced activations, and aggregate delivery
latency. Inbox inspection reports pending, claimed, and latest generation
without exposing payloads.

Operators should alert on a growing claimed backlog, storage failures,
activation failures, repeated poison-generation suppression, or any legacy
ingress after the migration window begins.

## Legacy removal gate

Legacy readers and telemetry may be removed only when every condition below is
met:

1. The compatibility code has shipped for at least **two release trains** and
   at least **30 days**.
2. All four legacy-ingress telemetry events are zero for both **two complete
   release trains** and **30 consecutive days** in supported deployments.
3. Startup scans report zero legacy `pending_injected_messages` queues for the
   same window.
4. Rolling downgrade remains possible for the oldest supported release during
   the window; removing a writer must not make rollback lose messages.
5. CI retains old-host/new-worker, new-host/old-worker, deterministic migration
   retry, crash-before-source-clear, and transcript-checkpoint-failure tests.
6. Release notes announce the removal one train in advance and identify the
   last rollback-compatible version.

If any condition resets, restart the observation window. Removal is a separate
reviewed change, not part of routine cleanup.
