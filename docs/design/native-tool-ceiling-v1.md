# Native Child tool ceiling v1 (#1361)

`SubAgent(create, context_packet=...)` automatically derives an activation-time
ceiling from this AppState's actual Builtin Arc and complete Composite/Overlay
owner chain: Bash, Edit, Glob, Read, Write. Config disabled references resolve
exact owners before aliases. Foreign shadows/unknown wrappers cannot prove native
provenance. Root's nine direct orchestration tools are separate; Ultra can delegate
Write. No-packet routes keep their existing setup.

Assigned Project identity must be typed and consistent, with an exact active
manifest in this AppState's ProjectStore. Unassigned works. No Project tool-name
policy field exists: absence means no additional name restriction. Existing
workspace, permission and read-only checks still narrow execution. Existing
ProjectStore recovery is unchanged; no network/secret/workspace isolation claim.

Closed startup payload: version, Child/parent/root, canonical host birth/depth,
explicit nullable Project and sorted unique names; limits 16 KiB/five names.
Missing/null required payload, unknown/duplicate fields, unsupported version/names,
expanding capability or Run mismatch fail closed. Host and both fleet spawn paths
probe `native_tool_ceiling_v1` before provision. Worker validates before construction
and exact Run identity before seeding. Strict fresh one-shot workers omit MCP,
skill tools/automatic selection, nested spawn and composition alternatives. The isolated provider keeps its
minimal config slot without importing Host tool-search extras.

All seven async entrances and sync schema/guide/ownership/classification views
check the same concrete boundary; existing argument and permission behavior stays.
Dispatch captures a genuine native Arc before permission await and invokes that
same Arc. Replacement cannot execute; the admitted original may finish and the
next replacement capture is rejected. This is immutable per Run, not instant
revocation or durable profile freezing. #909/#1315 application remains separate.

Focused fixtures cover all entrances, owner/alias shadows, late registration,
real permission-await replacement, read-only/resource denies under three modes,
closed parsing and actual worker Run rejection before seeding/provider. Native
fixture uses same-source CLI Host/current_exe worker and only fakes the remote
model, observes durable wait before releasing the worker, and covers Ultra Root
versus Child Write, actual Read/Glob Config, Project and legacy routes. Definitions
are source-only until recorded Cargo/native gates execute.
