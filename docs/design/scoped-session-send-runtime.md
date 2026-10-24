# Scoped Session sender runtime wiring (B2b)

Issue [#2161](https://github.com/bigduu/Bamboo-agent/issues/2161), continuing
[B2a1 #2159](https://github.com/bigduu/Bamboo-agent/pull/2159) and #2155. B2a1
`573e47ae1502bdd527d8867c77877b1ccd2624c0` supplies the unchanged canonical
sender adapter. A2 exact-owner contract query commits `b81155d3` and `58058edb`
are explicit source dependencies. This slice is independent of prompt #2146.

Server's shared Base tool surface and the SDK's defaults-backed delivery
assembly now install `CanonicalSessionSendAdapter`. Root, Child and nested
Child share that Server registration. Explicit SDK tools/default-executor
policies keep their existing precedence. `session_history` has no send action.
Assembly installs no actual grant. The integrated [finite Host sender](host-session-peer-send.md)
selects an installed SendPeer token for every caller role and requires explicit
QueueOnly or TriggerTurn intent. Only never-installed Host absence selects the
ordinary same-tree or real Supervisor followup route. The legacy Supervisor
route still rejects queue-only/omitted intent; installed, denied, stale, revoked
or unsupported Host scope never falls through to that legacy authority.

## One permission evaluation

Only the installed concrete `SessionMessageTool` with a scoped sender advertises
`bamboo.session_message.scoped_send.v1`. SDK's Builtin registry observes its exact
tool contract. Server's selected Overlay owner scopes a restriction context
around the existing base permission chain. This context binds the complete
original ToolCall, including type/name/original argument bytes, plus resolved
arguments and exact owner contract. Another nested call does not inherit it.

The native permission leaf runs its existing single evaluation, with these
additional restrictions for `send`:

1. Check legacy followup durable Deny, remembered Session Deny, whitelist Deny
   and Host hard-deny before the actual once receipt or unprompted modes.
2. Check legacy durable AlwaysAsk and configured Ask at the existing forced-ask
   stage, before remembered/new Allow, disabled-check and Bypass handling.
3. Evaluate Allow and consume a typed once receipt only for the actual
   `session_message` invocation and its private `session_message send` resource.
   Legacy Allow and legacy once receipts never grant the new sender.

The legacy `session_control followup <target>` context is private narrowing
input. The real typed request/replay owner, arguments and generation stay
unchanged; the legacy resource is never substituted into the new approval
request. Existing peer approval events expose only the action. The scope also
requires proof that the native typed leaf actually evaluated it: an opaque
base's default `Ok(None)` cannot attest this gate. There is no second permission
evaluator, additional permission overlay, or second once consumption.

Plan, native ceilings, existing disabled-tool admission, explicit hard/deny
rules and final canonical Inbox guards remain authoritative. Existing Auto
semantics remain: explicit Deny precedes Auto, while Auto suppresses approval
prompts, including configured Ask. This slice does not change that posture.

## Unsupported configurations and receipts

A missing/configless checker or opaque base that cannot establish the native
typed gate returns `scoped_session_send_permission_config_unsupported` before
sender invocation. In the SDK, the historical no-checker default and
`.bypass_permissions()` are configless. An explicitly typed `.permission_mode`
or config-backed checker supplies the narrowing contract; no empty config or
legacy sender fallback is silently installed. Other message actions retain
their existing permission paths.

Sender errors and canonical receipt semantics are inherited from B2a1. Durable
admission is not consumption/completion. Known admission plus activation failure
keeps the receipt; an uncertain outcome never permits another operation ID.
Project membership, directory visibility and management relationships do not
authorize an Ordinary caller to send across Roots.

## Validation boundary

This slice adds twelve Rust case sources: five policy tests for the Deny/Ask
matrix and exact-generation once behavior; two full-call restriction-scope
tests; three canonical Tool/Overlay/Builtin integration cases; and one source
case at each real Server/SDK assembly entry. The latter assert original-caller
requirements, actual legacy refusal and a same-tree queue-only Inbox receipt,
with no model call or receiver history append. The existing Server assembly
case also checks concrete contract provenance across every tool surface.

Direct `rustfmt`, `git diff --check` and source/illustrative-wire checks run
without compiling. **Cargo, Clippy and all Rust tests remain UNRUN** under the
parent's Rust window restriction. Source wiring and fixture definitions do not
establish runtime success. Current source compilation, affected runtime cases,
exact-head/base review and normal protected-branch checks remain admission
blockers. Keep the stacked PR Draft; do not merge or claim ordinary cross-Root
authorization is complete.
