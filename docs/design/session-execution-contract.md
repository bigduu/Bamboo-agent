# Call-bound execution contracts

Issue: [#2160](https://github.com/bigduu/Bamboo-agent/issues/2160). Dependency
source: `605fa3f5126736586796b79361ae8da709541798` (A1 retained operations,
A2a1 original caller context, B1 authorized directory interfaces).

## Bounded change

A frozen execution name cannot distinguish an old custom `spawn_session` from
the framework Root tool with the same exact name. Capture semantic provenance
before the first awaited tool hook or permission gate, retain it with the
original caller and complete `ToolCall`, and use the same selector for replay
owner lookup, permission checks and exact execution.

This slice keeps fresh versioned aliases, installed grants, current Supervisor
requirements, Root creation, Inbox stores and Session persistence schemas as
they are. It prepares the retained-operation seam for a later change to fresh
calls. It does not enable ordinary callers or install any new caller scope.

## Observation through the existing owner chain

`Tool::execution_contract()` defaults to `None`. The framework Root tool declares
the stable ID `bamboo.spawn_root.v1`. A concrete executor exposes its tool's ID
through `local_execution_contract`; transparent wrappers continue forwarding
`exact_tool_owner`. The default `execution_contract_for_exact` consults only the
returned owner's local hook, so an owner returning itself cannot recurse.
Opaque wrappers remain unknown. A matching catalog name alone proves no
framework semantics.

The ID is a Host observation of implementation semantics, not a permission,
authorization token or executable pointer. Replay still enters the complete
executor chain. It rechecks canonical Session compatibility and existing Root
Plan, disabled-tool and live Supervisor guards.

## Retained wire contract

The existing metadata key `permission.operation.v1` remains the envelope key;
the record's numeric `version` distinguishes the following shapes. The original
caller birth, exact result occurrence, full call including argument bytes,
execution name and request generation continue to be checked before admission.

| Record | Required semantic fields | Replay interpretation |
| --- | --- | --- |
| v1 | Neither new field; no ID or known flag may be asserted | Existing exact-name compatibility. Canonical `spawn_session` in a v2 Session is ambiguous and refused. |
| v2, concrete owner | `execution_contract_known: true`; `execution_contract: null` or a bounded stable string | Null freezes a concrete uncontracted custom owner. A string freezes that semantic ID. |
| v2, opaque owner | `execution_contract_known: false`; `execution_contract: null` | No framework provenance. Canonical `spawn_session` is refused. |
| v2, missing/invalid field | Missing known flag or contract field, invalid types/ID, or unknown-owner string | Refused; no inferred semantics. |

IDs contain 1–128 ASCII letters, digits, `.`, `_`, `-` or `:`. V2 always writes
both semantic fields, including explicit JSON null. The separate known flag is
necessary because a concrete custom tool returning no ID and an opaque wrapper
returning no owner must not become the same proof.

Repark changes only the permission request generation. It retains the record
version, caller, call, exact owner, known flag and ID. Legacy v1 records retain
their old wire shape and never acquire provenance from today's registry.

## Replay selection and failure

A retained `bamboo.spawn_root.v1` record selects the independent Root chain. A
concrete, uncontracted canonical `spawn_session` selects the legacy exact custom
chain, including when the current v2 catalog shadows it with the Root overlay.
The selected concrete owner must still exist and advertise the same ID or null.
An unsupported or changed ID is unavailable; it never falls back to another tool.

Historical None/v1 Sessions with an existing v1 canonical custom owner can use
that exact legacy owner only when it is concretely observable. Historical v2
canonical spawn records lacking the ID cannot distinguish custom from Root and
must be resent explicitly. Unknown canonical spawn provenance also requires a
new instruction. SDK, Server and Connect restore and validate this evidence
before replay markers, remembered grants or tool execution can change.

Noncanonical historical exact owners, including `SubAgent` and registered
custom spellings such as `spawnSession`, keep the existing opaque-executor
compatibility when no concrete provenance is available. Their names and raw
arguments remain frozen and go through the legacy exact chain. This slice does
not claim semantic identity for an opaque third-party executor or change its
fresh alias behavior. Unknown provenance can never establish framework Root
ownership.

## Scope isolation

The asynchronous retained dispatch scope is bound to the full `ToolCall` and
exact execution name. A changed call with the same ID fails closed. A nested
new call with a different ID receives no retained selector. Tokio task-local
state is not inherited by a separately spawned task.

Owner availability uses a separate, synchronous lookup scope. This allows the
same historical branch to answer `owns_exact_tool`, `exact_tool_owner` and
contract queries without making fresh nested calls inspect the outer replay's
catalog. The returned owner pointer is never used to bypass wrappers, gates or
the complete execution path.

## Validation source and remaining gates

Focused synthetic test source covers:

- Core concrete/opaque owner observation without recursion; call/argument/owner
  binding, synchronous lookup isolation and detached task isolation.
- V2 explicit null/string and malformed/missing fields; unknown provenance;
  old v1 wire preservation and unchanged contract through repark.
- Engine legacy version/owner compatibility with ambiguous v2 canonical spawn
  rejected before state mutation.
- Real SDK fixture parking, typed approval and reopen with original owner/ID;
  damaged, unknown, unsupported and old v1 canonical Root records retaining
  history, markers and unconsumed approval.
- Server fixture exact custom canonical replay despite the current Root
  overlay, raw-argument preservation, nested fresh-call isolation, and Root
  Plan/disabled/original-Supervisor guards through permission and execute paths.

All Rust tests and compilation are **UNRUN** under the parent's shared build
window restriction. Direct formatting, whitespace checks and independent source
review are source checks only. This Draft is not merge-ready until the recorded
source passes focused local tests, required compilation and applicable review.
