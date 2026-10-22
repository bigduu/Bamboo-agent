# Scoped canonical Session sender (B2a1)

Issue [#2155](https://github.com/bigduu/Bamboo-agent/issues/2155), stacked on
[B1 #2151](https://github.com/bigduu/Bamboo-agent/pull/2151). This is independent
of prompt #2146. It reuses #2000's durable Inbox and #1976's activation/wait
boundaries rather than creating another messaging lifecycle.

`ScopedSessionSendPort::send(&ToolCtx, ScopedSessionSendRequest)` is a complete
atomic operation port. The request carries only target, stable operation ID,
bounded collaboration text and optional delivery intent. The original caller
comes from the Host's `ToolCtx.executing_caller`, preserved by A2 dispatch and
retained replay; it cannot be reconstructed from a current Session with the same
ID. A caller observation is not a grant. An installed Host adapter must retain
its real scope and caller/target lifetimes through canonical durable admission.
An authorization preflight followed by an ordinary unfenced send is insufficient.

`SessionMessageTool::with_scoped_sender` explicitly installs this port. Its
failures, unsupported intents and uncertain outcomes never fall back to the
legacy sender. The default constructor still exposes the existing same-tree
route. `session_history` remains read-only; discovering a Session does not grant
permission to send to it.

## Compatibility adapter and receipts

`CanonicalSessionSendAdapter` combines existing writers without issuing grants:

- Ordinary Roots and Children use `SessionMessenger::send_peer_observed`. The
  original caller is compared with the loaded source before using the existing
  envelope, operation identity and final Inbox lifetime/tree/Project fences.
  Project membership and having the same Supervisor never authorize cross-Root
  delivery. Existing omitted/queue-only/trigger-turn intent behavior is preserved.
- The actual original Supervisor can use its existing scoped followup writer
  only with explicit `delivery_intent=trigger_turn`. That writer has no queue-only
  port. Queue-only and omitted intent return an unsupported blocker before
  admission; neither is silently reinterpreted as activation.

The canonical receipt formatter is shared with existing send. Accepted/admitted
means durable delivery, not consumption, task completion, acceptance, or a
successful activation. An activation error with a receipt preserves it. An
unknown Host outcome without a receipt reports uncertainty and never fabricates
`admitted=false`; retry must preserve the operation ID and every argument. No
message answers an approval or changes the recipient's permissions.

## Deliberately pending integration

This source slice does **not** install the sender in Server or SDK defaults.
Before those two registrations, B2b must preserve legacy `session_control
followup` Deny/Ask restrictions in the native `session_message` permission leaf.
That must be one evaluation: the actual tool owner, original args and private
`session_message send` resource retain typed approval/replay/once identity; a
second legacy context can only narrow with Deny/Ask, never grant through Allow.
Private targets must not appear in request resources. Configless unsupported
compositions fail closed. This slice alone is not end-to-end enabled behavior.

There is also no new ordinary cross-Root scope issuer. A later explicitly
authorized Host may install an atomic scoped writer; ordinary callers do not
borrow the Supervisor's identity, management links or Project authority.

## Validation status

`tests/scoped_session_messaging.rs` contains nine synthetic cases using temporary
canonical stores/Inbox plus a fake activation port, never a real account or
provider. Sources cover original-caller replacement/revision, same-tree intent
and exact retries, real Supervisor followup reuse, cross-Root rejection, missing
or denied Host with no fallback, durable activation failure receipts, Plan and
native permission classification/provider schema, uncertainty and byte bounds.

Direct `rustfmt` and source/wire consistency checks are lightweight evidence.
**Cargo, Clippy and all nine Rust tests remain unrun under the parent's Rust
window restriction.** The JSON fixture is illustrative data rather than handler
execution. The A2 caller-context source dependency, B2b permission narrowing and
default registrations, required compilation, affected runtime tests and exact
review must be satisfied before admission. Keep the PR Draft.
