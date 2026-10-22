# Fresh Session spawn and retained operation boundaries

Issue: [#2166](https://github.com/bigduu/Bamboo-agent/issues/2166). Source base:
`8f221995564803ae17ce879d22c2dbfa3cfb6d7b`, execution-contract Draft
[#2165](https://github.com/bigduu/Bamboo-agent/pull/2165).

## Delivered source contract

Fresh `spawn_session` selects an installed independent Root entry in every
supported Session version: missing, v1 and v2. A missing Root entry is
unavailable; it cannot fall back to Child creation. Explicit `SubAgent` retains
the bounded Child operation. Existing exact custom registrations, including
qualified names or a distinct `spawnSession`, keep the resolver's exact-owner
priority. The framework's canonical `spawn_session` overlay owns that canonical
name on the installed Root surface.

The domain resolver, model catalog, final dispatch policy and
`VersionedRootTools` agree on this fresh contract. The model catalog uses the
actually installed canonical schema rather than a stale configured schema.
Root thinking mode and Supervisor identity do not exclusively admit the spawn
name. Fresh canonical catalog/dispatch also require a concrete owner declaring
`bamboo.spawn_root.v1`; an opaque or uncontracted custom registration with that
name cannot gain the framework entry merely by joining the name allowlist.
Name admission is not a creation grant.

Session stamps are not migrated. `independent_root_spawn_contract()` continues
to identify the historical v2 boundary for retained-operation ambiguity and
snapshot compatibility; it no longer selects fresh aliases. A v1/v2 mismatch
between canonical and retained runtime state still fails compatibility checks.
The persistent default Supervisor singleton is reused. Older Supervisors already
had independent Root creation through `session_control(action=create)`; this
change makes their fresh spawn spelling use the Root entry too.

## Original caller and Host authority

`SpawnSessionTool::new` still installs `SupervisorRootCreateAdapter`, retaining
all existing singleton, birth, explicit Project/workspace, management revision,
Inbox and creation-idempotency checks. An ordinary Root or Child using this
default adapter receives `session_root_scope_unavailable`. Neither its Project
metadata nor a Supervisor management link can grant it creation rights.

The new `SpawnSessionTool::new_with_caller_port` accepts an explicit trusted
`SessionRootCreatePort`. Before permission admission or invocation, the tool:

1. Rejects Plan, missing original caller observation and incompatible canonical
   Session state.
2. Matches the incoming `SessionCallerReference` against the canonical caller,
   preserving the original lifetime, ancestry, workspace and authority proof.
3. Checks live disabled references against the same fresh exact-owner resolver.
4. Reads the installed port's caller scope and refuses a blocked result.
5. Rechecks canonical caller identity after that awaited scope read, retaining
   the original reference instead of refreshing it from current configuration.

The request still requires an explicit existing Project, existing bound
workspace, expected scope revision, operation UUID and first instruction. The
tool never chooses missing fields, copies scope, or initializes permissions.
The port's `create` implementation remains responsible for final atomic caller,
scope and Project/workspace validation at the canonical writer boundary. A
scope observation is not an authorization token and cannot close that final
writer race. The parent-owned finite Host scope/writer work is separate.

## Retained replay stays separate

The execution-contract selector from #2165 is unchanged. A frozen framework Root
ID selects Root; a proven legacy canonical custom owner selects the old exact
chain; frozen `SubAgent` continues Child. Original tool spelling and raw argument
bytes stay attached to that exact owner. Repark does not adopt a current ID or
upgrade a v1 record. An ambiguous old v2 canonical spawn or unknown legacy spawn
continues to refuse with history and pending markers intact.

All replay entry points still use exact execution through the full wrapper
chain. A nested new call with another ID receives the fresh Root contract,
including in a missing/v1 Session. No alias change is used to reinterpret an
old approval or consume its authorization for another operation. SDK, Server
and Connect use `refresh_retained_approval_replay_posture` after live posture
refresh: an old canonical custom owner remains blocked in orchestration-only
mode. Adding the framework name to the common allowlist cannot promote that
custom implementation after a Standard-to-Ultra transition.

## Actual surface inventory and the next bounded slices

This slice changes the installed Root surface and common fresh-call admission.
It does **not** claim all-agent runtime installation or ordinary Host grants.
Read-only source inventory on the base found:

| Actual path | Present boundary | Required follow-up |
| --- | --- | --- |
| `bamboo-server-tools/src/surface.rs`, `bamboo-server/src/app_state/tools.rs`, `bamboo-server/src/handlers/agent/execute/runtime/execution.rs` | Child uses Base; Base has same-tree message and self-only history, while Root has full history and the spawn wrapper. | Install the same explicitly scoped ports on shared Base without exposing the unrestricted Root viewer. |
| `bamboo-server/src/app_state/builder.rs` | Agent default, LocalTask Child and scheduled executor clones capture Base. | Assemble shared ports before those clones; replacing only Root overlays misses these consumers. |
| `bamboo-engine/src/external_agents/local_task/tool_policy.rs` | Local Child policy re-resolves references with the old generic resolver. | Bind fresh/retained exact selection while preserving name pinning, live Child, disabled and read-only guards. |
| `bamboo-engine/src/external_agents/local_task/skill_host.rs`, Host Native ceiling, named profiles, native protocol | Strict Child/Ticket/profile paths have smaller tool lists and concrete Builtin-owner checks. | Separate strict capability and authenticated Host bridge work; do not add names or copy Root authority to bypass these layers. |
| `src/subagent_worker.rs` | Process Worker exposes a bounded Builtin ceiling. | A separately negotiated and authenticated Host Session port is required. |
| `bamboo-sdk/src/agent/builder.rs` | Default configured SDK tools may install message; Host history APIs are not model tools. Explicit tools/no-tools/executor selections are preserved. | Add explicit Host port injection, preserving the existing SDK selection contract. |

One additional shared-file dependency was reported to the Overlay owner:
`OverlayToolExecutor::route` on this base only prevents missing-Root raw fallback
for a bound v2 Session. The same fail-closed condition is required for bound
missing/v1 callers once the fresh resolver is universal. This worker did not
edit that shared file. Actual common Base/non-strict runtime installation is
A2b2; strict Native/profile/IPC exposure remains a later bounded slice. Neither
step may infer caller scope from Project metadata or reverse management links.

## Source validation

Focused synthetic source covers all supported version/role/mode catalog and
dispatch combinations, explicit Child selection, missing Root, exact custom
priority, disabled references, legacy singleton Root creation with exact retry,
caller-bound explicit Host port forwarding and default-adapter denial. Existing
SDK fixtures now park fresh Root calls for every supported version and seed a
separate pre-cutover v1 Child record to verify old approvals after the cutover.

Only the disposable test fixtures rewrite stored stamps to represent historical
input. Production save/version guards are unchanged. The injected ordinary/Child
port fixture is a spy and creates neither a Root nor a permission grant; actual
Root/Inbox creation tests use the existing strict Supervisor writer.

All Rust tests and compilation are **UNRUN** while the parent's shared Cargo
window is closed. Direct formatting, diff checks and independent source review
are source evidence only. Local affected tests, required compilation and formal
exact-head review remain merge blockers. No real Session, history, configuration
or user permission was changed.
