# Owned initial-input release

The builtin local owned zero-tool route requires `owned_initial_input_release_v1` before provisioning. Its provision bit requires the existing strict context, authoritative Child birth, empty actual native tool ceiling, permissions and broker route. Ordinary legacy and read-only Glob execution keep their existing contracts.

After its real local transcript/cursor and permanent receipt, the worker requests permission and waits before SDK execution. The closed 4 KiB control binds a fresh UUID nonce, logical Child birth/lineage/Project, input ID/generation, activation run and execution epoch. Explicit Project null is required for an unassigned Child. The request uses the existing Admitted Inbox kind; the link exposes its reserved tagged payload through the existing Event container, intercepted before AgentEvent processing. Replies use typed ParentFrame/Steer/RunCoord, not text or approval booleans. Unknown/partial values cannot become text steering.

The worker queues its actual permission audit first. The broker flushes the preceding ordered event batches before the request. The Host requires matching posture confirmation, current policy/denies, current Actor fence and owner, complete current input checkpoint readback, and successful exact physical Inbox ACK. It rechecks authority before returning a deadline bounded by both leases. A worker-local receipt alone is insufficient.

The same current owner may repeat the exact nonce/request after a lost reply. The Host rereads the durable admitted receipt and preserves the original deadline. Worker retries retain that nonce, time out after 60 seconds and observe cancellation/disconnection. Wrong, expired or unrelated releases fail the activation; they never become input. A release is an admission decision, not instantaneous revocation of an already entered provider call.

The existing Local placement reference records `owned-initial-release-v1:<physical mailbox>` only after the trusted capability probe and actual spawn. This is provenance for a future recovery consumer, not evidence that a process stopped. Absent/legacy placement cannot establish historical safety.

This slice covers initial typed input on fresh owned executions, including the already supported second Run/correction and Failed/new-input Run. It does not add old-claim/Already recovery, replacement activation, live-input release, renewal, remote authority, a journal or exactly-once provider effects.

Focused source fixtures use real Host Store/owned Inbox, broker and BambooRuntime with a recorded provider: ACK failure and retry, unreleased live worker past expiry, wrong nonce/epoch, cancellation, current-prefix rejection, same-receipt retransmission and cold single-input readback. The actual serve/current-exe correction/retry fixture requires the Host receipt immediately at provider entry. Compilation and runtime results belong to the shared verification lane; source fixtures are not execution evidence.

The builtin turn-boundary bridge is a legacy unowned Inbox consumer. If its
claim fails, including when an Actor has upgraded the queue to owned format 3,
it reports unresolved admission to the checked runtime path. That path stops
before prompt/provider execution and preserves the owned claim for its actual
owner. This is a fail-closed compatibility boundary, not lease renewal or
recovery of an expired Actor.
