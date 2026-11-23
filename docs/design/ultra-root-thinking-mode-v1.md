# Ultra Root thinking mode

Ultra combines the primary model's highest reasoning effort with active
parallel SubAgent delegation. The Root looks for useful independent work,
dispatches as capacity permits, advances its own ready work, and verifies the
results. An ordinary Ultra Root can write, edit and use `exec_command` under
its workspace and permission checks. Use `SubAgent` with `intent=wait` for
non-interactive completion and `write_stdin` for input or unread output.
Other reasoning selections use their chosen level and delegate when useful.

At primary request dispatch Bamboo uses the highest supported native tier
for known limits, such as `xhigh` on GPT-5.4 and `high` on Gemini 3. Routes
without a known lower ceiling use `max`. Native models such as Claude 4.5
that reject the adapter's adaptive-effort request receive no Ultra effort
override. Provider defaults and request overrides still apply. The saved
standard effort is available when the user switches to Standard.

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
- Chat and Work each have a Host-owned default for new sessions. The provider
  receives the selected route's native reasoning tier.

Supervisor Work uses `features.work_ultra_by_default`. A managed
Work Root captures the mode in its dispatch intent and uses the tools allowed
by its Work contract. A Work Child captures the highest-tier selection at
creation and uses it for model requests under its existing Child authority.

No second persisted field, proof version, journal or stage protocol is added.
The existing birth token, terminal epoch/receipt and dispatch fences are kept.
A command approval is bound to the Session's tool authority revision. After a
mode change, send a new tool call so the current policy can authorize it.

## Delegated execution evidence and limits

The focused acceptance uses the production server and Root runner, SubAgent,
the durable Child wait, actor provisioning and a real `bamboo subagent-worker`
with `BambooRuntime`. Only the model endpoint is a loopback scripted SSE server.
It observes an actual worker provider request while the Root durably waits,
then permits the child to finish and observes Root resume/synthesis. It also
checks that an authorized Root can create and edit a file through real dispatch.

The fixture saves High as the ordinary Root setting and checks Max on
the actual Ultra Root requests. The planner uses its own model and effort
configuration.

Ultra guidance reuses existing tools; it does not enforce a persistent N-agent,
Plan or Review stage gate for every task. Required user goals, constraints and
acceptance criteria remain part of the assignment. Unsupported Child
executor and placement routes fail closed; this slice adds no native Ultra, remote
propagation, new role, ContextPacket or lifecycle/recovery protocol.
