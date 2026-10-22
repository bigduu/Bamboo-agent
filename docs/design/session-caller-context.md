# Original caller propagation

Issue #2156; stacked on #2153 (`44f6e372645b4e7e4078533a1bed7fbc409e6b1c`). This slice carries an observation, not access permission.

Real Engine dispatch snapshots the SessionCallerReference already captured at admission. A narrowly scoped task-local carries it across permission/dispatch into the owned ToolCtx. `to_tool_ctx` filters caller ID and snapshots the Arc before a detached invocation can leave the task. Detached tasks do not inherit the task-local itself. Explicit None scopes and missing/synthetic contexts retain no caller. Consumers must validate the captured lifetime and their grant under canonical final authority locks; possession alone never authorizes another Session.

The Core nested-action queue retains one original caller across its NeedMoreActions queue. Skill dynamic providers carry only the incoming same-ID observation; the repository Session used for configuration cannot replace it. This is distinct from an execution contract: every nested call will select its own concrete tool owner, rather than inherit an outer call contract.

SDK, Server resume and Connect retained replay restore the caller from the exact validated host pending-operation record before posture refresh or marker mutation. They do not capture a generic observation from the reloaded Session. A stale/malformed/injected host record fails before execution. Legacy history without a generic record remains readable and receives no newly minted generic caller; the existing validated legacy Supervisor path remains separate. Repark preserves the original record.

The next source slice must freeze concrete execution-contract provenance before universal fresh spawn routing. A canonical custom `spawn_session` can have the same name as the framework Root tool; exact spelling alone is insufficient. Ordinary cross-Root scope installation, final held-lock authority ports, generation/reset writers and actual Supervisor history reset remain separate dependencies.

## Validation

Direct rustfmt parsing/check and diff/source inventory only. Added two synthetic caller-scope test functions (roles/versions, owned detached handoff, replacement/mismatch/explicit Host scope) and a replay restoration test (tampered birth, absent legacy binding, forged payload, unchanged history). The existing SDK versioned parking/reopen fixture now requires original caller presence at actual invoke. All Rust tests, Cargo compilation and runtime/provider validation are **UNRUN** until the parent grants the shared Rust window. No actual grant, user Session or history was changed.
