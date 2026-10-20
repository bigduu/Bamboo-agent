# Owner completion and confirmed process stop

Owner Work controls remain semantic Pause, Ready, Cancel and Accept commands over
the existing authenticated Ticket update/dispatch routes. There is no arbitrary
state patch and no client-side pause-then-complete workflow. A command receipt is
not a runtime termination acknowledgement.

Accept requires the current exact Submission, current generation/contract and
dependency inputs, reconciled effects, non-empty acceptance evidence, and explicit
User authority whenever the contract requires it. It additionally requires that
Submission's exact Assignment to have host-confirmed `process_stopped`. The
Assignment's Work/generation/contract/RuntimeReceipt must match the Submission.
Until confirmation, a fresh acceptance request returns `resource_blocked` (HTTP
423) and publishes no acceptance or receipt. Null `active_assignment` is
insufficient: Submit can clear it while the process is still awaiting stop.

Only trusted Runtime commands can record the stop fact. RuntimeStopped carries
the exact existing dispatch/spec/session/run receipt; ConfirmStopped uses the
existing terminal/cancelling/unknown reconciliation path. Neither an HTTP ACK,
timeout, disconnected Session, heartbeat nor a model-reported ID is a stop proof.
Late receipts can update their own historical Assignment but cannot confirm a
new generation's process. No lease, owner table or separate control-state store
is introduced.

Existing User acceptance receipts and Submission/artifact/evidence history are
retained. New acceptance does not mark Worker statements as automatically tested
or verified. Already-committed historical receipts replay their original result;
this guard does not rewrite old acceptance or invent missing historical stop
proof. Unknown effects, stale submissions and missing evidence still fail even
when the process is confirmed stopped.

An owner can explicitly request read-only current completion facts:

```json
{
  "ids": ["work-id"],
  "sections": ["controls", "assignments", "submissions"],
  "depth": 0,
  "budget_bytes": 65536,
  "fixed_commit": null
}
```

POST this body to `/api/v1/tickets/inspect`. A Work view includes:

```json
{
  "controls": {
    "accept_requires_stopped_assignment": true,
    "submission_id": "current-submission-id",
    "assignment_id": "submission-assignment-id",
    "process_stopped": false
  }
}
```

The producer marker declares the stop-guard contract, not permission or a promise
that any proposed Accept will succeed. `process_stopped: false` means unconfirmed;
it does not prove a live process. Null means there is no valid current Submission
binding from which to derive a stop fact. The IDs remain locators for preserved
history. The normal fixed-snapshot envelope and byte budget apply. Old default
inspect sections are unchanged; controls is explicit opt-in and User-only. A
missing controls field, unsupported section on an older producer, or a non-Work
Ticket must not enable manual completion.

The UI must retain the exact Submission/generation/contract binding of its User
confirmation, reconcile uncertain outcomes with the original operation identity,
and honor sequence/epoch CAS. It must not refresh CAS and blindly retry an
acceptance against a different result. Session links require the existing
permission-checked Session path. Claim timestamps, heartbeat and meaningful
progress are still unsupported where canonical history has no such facts; do
not synthesize them from update sequence or elapsed browser time.
