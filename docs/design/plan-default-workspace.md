# Plan default workspace (#1347)

Plan loads its Root parent from durable storage before selecting a workspace.
The existing `ChildSessionPort` selection entry point then delegates to that
port's workspace validator. The server adapter keeps its existing ProjectStore
ownership checks and AppState-scoped WorkspaceResolver; this change adds no
workspace persistence or fallback authority.

## Selection and validation

1. A non-empty explicit Plan workspace wins and keeps the existing validation.
2. A `project_default` provenance marker selects the current Project path,
   rather than the old default-derived workspace metadata.
3. Otherwise, durable `workspace_path_meta()` selects the session workspace.
4. An assigned parent without that metadata selects its current Project path.
5. Only an unassigned legacy parent may use its old `Session.workspace` field.

Missing unassigned workspace fails before Child creation. An assigned parent
with neither canonical metadata nor a configured Project path also fails:
a legacy relative path or an old valid directory supplies no replacement
Project authority. Invalid Project identity, unavailable/non-directory paths,
foreign ownership and confinement errors retain the existing validator errors.
No publication cache, process cwd, global default or temporary directory is a
fallback. An explicit empty/whitespace argument keeps the existing omitted
selector behavior. Validation is also retained immediately before Child creation.

## Acceptance evidence

Focused server tests cover durable metadata versus stale publication/legacy
paths, explicit override, unassigned legacy compatibility, current Project path
CAS updates, missing/invalid/foreign paths, and assigned legacy fail-closed
behavior before any Child is persisted or parent wait armed.

`tests/plan_default_workspace.rs` uses a real first-chat route and the exact
source's `CARGO_BIN_EXE_bamboo` worker. The first chat stores only workspace
metadata; the provider emits Plan without a workspace argument. A read-only
planner calls Glob with a bounded filename pattern and no path, and its next
actual provider request must contain the canonical marker path from the Tool
result. The fixture observes the durable parent wait before releasing the
worker request, then observes Root resuming with the actual Child result.
Saved workspace fields alone do not satisfy this execution proof.

## Non-goals

No new workspace resolver, journal, lease or Project migration. SubAgent and
remote placement are unchanged. Same-logical-Child birth/reuse (#1348), required
ContextPacket (#1342), token budget and ordinary effort propagation (#1346)
are separate contracts. This slice does not add arbitrary cwd fallback.
