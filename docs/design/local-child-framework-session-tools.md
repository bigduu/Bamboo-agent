# Local Child framework Session ports

This source slice gives an ordinary in-process Child the same installed framework
Session tool instances as Base, subject to its own Actor, permission, readonly,
disabled and Host scope checks. A Supervisor's additional role remains follow-up
and acceptance responsibility. Installing an instance grants no target, Project,
workspace, creation or Inbox authority.

The Host factory constructs the actual history/current readers, independent Root
spawn tool and optional typed message tool. Its opaque, non-serialized
`HostFrameworkSessionTool` contains the concrete `Arc<dyn Tool>`, a permission
executor and a restriction guard. The factory verifies pointer identity against
the selected exact owner's instance. A matching name or execution-contract string
does not supply this proof. An unknown wrapper supplies no proof by default.

The exact owner chain carries the proof through the normal Server Base overlays,
MCP composite and bound Skills wrappers. SDK defaults use the same factory around
their actual Builtin and optional MCP executor. Explicit SDK `tools`, `no_tools`
and executor overrides keep their existing precedence. SDK send still requires
the explicit coherent delivery installation; an absent delivery port is not
replaced with a synthetic sender.

LocalChildTools treats framework ports separately from its catalog-derived native
ceiling. At construction it rebinds each proof to the entire current outer
permission chain, preserving additional Deny/Ask restrictions. Execution performs
one native permission evaluation, checks the current restriction again, validates
the same full call and rechecks the live Child Actor before invoking the actual
instance. This does not consume a typed Once approval a second time. Server
assembly installs the typed message overlay once so its one selected-send native
evaluation scope is not nested inside another copy.

Fresh Root calls use the version-aware fresh selector for `spawn_session`,
`spawnSession` and their supported framework aliases. Exact dispatch uses that
same selection. A retained replay uses its frozen exact owner, semantic contract
and complete original call (including argument bytes). Known custom legacy spawn
may continue only under the ordinary Child's prior non-strict ceiling; it does
not acquire fresh Root semantics. Unknown historical spawn provenance asks for a
new explicit instruction and preserves the old pending operation. The shared
central replay resolver also requires an actual framework instance for framework
Root replay and orchestration/Child framework ports; a same-name custom shadow
cannot supply it. It does not upgrade old pending records or rewrite arguments.

Ordinary Ultra can expose the authorized bounded history reader as a general
Session capability. The provider catalog uses the concrete reader's schema,
including bounded `list`/`inspect`, instead of substituting a Supervisor-only
schema. A stale schema, arbitrary registry entry or injected additional schema
cannot supply missing framework provenance. Standard legacy Root embeddings keep
their explicit non-Root custom tools; an empty historical `root_session_id` is
normalized as Root, not mistaken for a Child.

Readonly/Plan Child execution installs only the two reader kinds. Strict native,
named-profile and Ticket routes do not receive these extra ports. The immutable
Builtin native eight-name ceiling is unchanged. Process/IPC workers do not carry
the in-process Arc proof: an authenticated Host bridge requires separate design
and admission. This slice does not claim all remote or strict agents are enabled.
Legacy shell disabled mappings are preserved; only independent Root references
use the fresh Session selector when resolving a disabled reference.

## Source fixtures and validation state

All Rust cases below are source only and **UNRUN** under the parent's build
window restriction. Direct rustfmt and diff checks do not substitute for Rust
compilation, execution or admission.

- `bamboo-server-tools/tests/framework_session_tools.rs`: actual common factory
  Arc/schema identity, same-name replacement rejection, fresh calls and retained
  custom dispatch in None/v1/v2 Sessions.
- `bamboo-engine/src/external_agents/local_task/tool_policy/tests/framework_session.rs`:
  concrete Host ports, fresh/exact aliases, contract-only rejection, outer
  Deny/Ask and one native gate, readonly/strict/disabled restrictions, live
  Actor/config changes during an awaited gate and central/custom replay.
- `bamboo-engine/src/runtime/runner/session_setup/tests.rs`: concrete generic
  reader schemas for Ultra/Supervisor/Child, injected schema rejection and actual
  proof absence. Existing static-catalog cases distinguish names from instances.
- `bamboo-engine/src/runtime/runner/tool_execution/per_call.rs`: missing proof
  before ToolStart and legacy Standard Root with empty root ID.
- `bamboo-engine/src/session_app/approval_replay.rs`: common SDK/Server retained
  resolver keeps frozen bytes and refuses contract-only or custom replacement.
- `bamboo-server/src/app_state/tools.rs`: actual Server surfaces expose the same
  Arc, with canonical send Deny, Ask and generation-bound Once before a synthetic
  temporary Inbox receipt. Receipt admission is not terminal completion.
- `bamboo-sdk/src/agent/builder.rs` and `versioned_spawn_replay_tests.rs`: actual
  SDK default proof, explicit tool policy precedence and typed replay with an
  explicit synthetic Host instance; contract-only replay preserves pending state.

No actual user Session, grant, history or permission configuration was modified.
Affected runtime cases, compilation and exact integrated-head admission remain
the integration owner's later build-window work.
