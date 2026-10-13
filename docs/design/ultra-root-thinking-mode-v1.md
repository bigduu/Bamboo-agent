# Ultra Root thinking mode

Ultra is an independent Root product policy. It increases task-wide reasoning
through read-only planning, narrow child execution and Root verification and
synthesis. Each model call retains its separately resolved ordinary reasoning
effort. Ultra is not an alias for Max or a claimed provider-native budget.

## Authority and wire contract

The sole durable selection remains `Session.root_orchestration_only`. Public
`thinking_mode` is projected from `root_orchestration_only_enabled()`:
`true` means `ultra`, and `false` means `standard`. A Child, a Session with a
parent or a prompt-only enhancement never projects Ultra. Legacy selected
Roots read as Ultra without migration or passive writes. Index-only list rows
omit this detail-only field; GET detail loads durable authority and reports
ordinary `reasoning_effort` separately.

- First chat may select `thinking_mode`. Existing Roots must use the recoverable
  Root-mode operation before chat; even a same-mode selector requires that path.
- Select/recover accepts canonical `thinking_mode`, legacy `enabled`, or both
  when they agree. Normalize to the existing boolean request before storage.
  Missing selectors, invalid/null modes and contradictory selectors fail.
- Terminal responses expose `thinking_mode_at_completion` from the receipt.
  Recovery fenced by a successor exposes `current_thinking_mode` instead. A
  historical committed receipt does not describe the current selection.
- Generic PATCH rejects `thinking_mode` presence, including null/invalid values,
  before any write. It cannot bypass the mode operation.
- `reasoning_effort: "ultra"` remains invalid. Global/provider/model-role defaults
  cannot enable Root authority; product mode is never a provider parameter.

No second persisted field, proof version, journal or stage protocol is added.
The existing birth token, terminal epoch/receipt and dispatch fences are kept.

## Delegated execution evidence and limits

The focused acceptance uses the production server and Root runner, Plan tool,
wait-before-enqueue scheduler, actor provisioning and real `bamboo subagent-worker`
with `BambooRuntime`. Only the model endpoint is a loopback scripted SSE server.
It observes an actual worker provider request while the Root durably waits,
then permits the child to finish and observes Root resume/synthesis. It also
attempts a Root Write and checks the real dispatch authority denies it.

Root ordinary High is explicit. The planner uses an independent model and its
own ordinary provider default (no effort override); it receives neither Root
High nor product Ultra. This does not prove explicit child effort overrides
reach the worker: that pre-existing propagation gap is tracked in #1346.

Ultra guidance reuses existing tools; it does not enforce a persistent N-agent,
Plan or Review stage gate for every task. Required user goals, constraints and
acceptance criteria remain part of the assignment. Existing unsupported Plan
executor/placement routes fail closed; this slice adds no native Ultra, remote
propagation, new role, ContextPacket or lifecycle/recovery protocol.
