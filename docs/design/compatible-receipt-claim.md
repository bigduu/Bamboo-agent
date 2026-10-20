# Exact receipt selection before compatible activation

The OpenAI Chat/Responses, Anthropic and Gemini HTTP surfaces currently forward
provider requests. They do not establish a real Main execution or current accepted
input. An ID in a transcript, metadata, or a provider request is not authority.

This bounded prerequisite for #1595 adds
`SessionInboxPort::claim_owned_receipt(target, receipt, active_run_id, request)`.
It matches both the canonical `message_id` and `ingress_seq` (Inbox `generation`)
under the existing owned-lease transaction. A mismatch or unavailable selected
item returns `None`; it never substitutes an earlier or later pending input.
Unsupported backends fail closed. The existing batch method keeps its behavior.

The selector creates no activation permission, run, current-input observation,
Skill grant or output subscription. This reuses the existing persistent lease model,
including irreversible owned-format opt-in even when the selected item is unavailable;
call only after taking genuine ownership of the queue. The receipt is not a Session
incarnation proof. A Host must retain its actual lifetime and execution capability.
The same lifecycle/Actor/process/physical Inbox locks protect selection and lease
CAS. Existing eligibility, successor waits, retry deadlines, dead letters and
per-item activation policy still apply. A Root-bound selection requires the
bound Actor's actual run id; its fence and lease deadline are still checked by
the existing owned filesystem capability. No permission ceiling changes.

## Lease, cancellation and retry semantics

- The same live consumer/receipt returns the same claim and expiry; this does
  not renew the lease. A different consumer cannot join that claim.
- ACK remains terminal after a durable matching transcript checkpoint. A retry
  after ACK returns `None`, including after restart; it never starts a new run.
- Explicit release and expiry preserve the existing typed envelope, retry
  evidence and increasing epoch. Stale tokens cannot ACK/release a successor.
- Dropping the caller does not cancel a started physical transaction. Existing
  owned jobs retain their actual locks through completion, including runtime
  shutdown. If the caller loses the result, the outcome is uncertain; inspect or
  recover the exact lease, never fall back to another pending input. An abandoned
  successful claim remains recoverable through existing release/expiry rules.

## Remaining activation seam

The next slice must bind one real Native reservation to the selected claim,
checkpoint/ACK, fresh current-input observation and the actual spawn. Calling
ordinary Native execute after a read-only queue inspection is insufficient: it
admits a batch and can consume another pending input. No compatible handler uses
the new primitive yet, and this prerequisite makes no HTTP activation claim.

Use one neutral Host seam for all four protocol families, with an explicit opt-in
extension. Freeze these acceptance rules before exposing it:

1. Reject an existing owner rather than joining its run. A selector is request
   data; the actual reservation, Actor fence and final callable permission
   ceiling supply authority. Preserve User currentness and cancellation through
   existing Native execution and Skills production.
2. Define output against the exact started `run_id`, through existing run-bound
   events or a protocol-specific final response. A provider-forwarding response
   is not proof of agent execution. Do not silently change ordinary forwarding.
3. Cancel only that run's retained token. A delayed cancel must not stop a newer
   owner through a session-only lookup. Disconnect, startup rejection and lost
   response outcomes need explicit tested behavior.
4. Bind HTTP idempotency to the canonical selector and request. Replay the
   original activation result, reject changed payloads, and never join or cancel
   a successor after restart or cache expiry. An ACKed receipt is terminal data,
   not authority to reconstruct a current invocation.

OpenAI Chat and Responses, Anthropic Messages and Gemini adapters must share
these rules. The core API has no provider or protocol dependency. Paid configured
models, deployed workers and full protocol activation are outside this slice.
