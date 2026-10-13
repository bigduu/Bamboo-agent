# Durable Child identity in local builtin workers (#1348)

Each supported local BambooRuntime activation carries the canonical host
Child's `LogicalSessionIdentity.creation = { created_at, spawn_depth }`, in
addition to its existing id, parent, Root and typed Project. Birth preserves
timestamp precision. This is immutable lifetime identity; it grants neither
tools nor a task change. The host directly reads durable Storage, rejects a
held/cache candidate with a different tuple, and rechecks before sending Run.
The dispatched tuple also fences the worker's event batches.

Before seeding, the worker validates the existing ActorSession identity shape,
matches the depth baked into provisioning, and directly reads its strict V2
store. An existing id with another birth, lineage or Project is rejected before
provider dispatch. It never adopts the cached birth. The original V2 final
Child writer guard remains in force across the read-to-seed boundary.

## Compatibility and cache scope

The baked `child_creation_identity` flag requires the explicit capability
`durable_child_creation_identity_v1` with the current Provision schema. Both
spawn paths probe before delivering a provision. Required-packet pre-create
checks also require this capability. The flag splits warm pool buckets, while
Child id/birth do not: compatible siblings may reuse one process. Capability
probe is trusted builtin protocol compatibility, not loaded-image attestation.
Remote/resident/custom implementations are not advertised as supported here.

The default typed cache is `<absolute host fabric_dir>/bamboo-runtime-logical-v1`.
It is constant across physical workers and initial Child identity. Thus a worker
initially provisioned for A can run B, and a cold B worker finds B's existing
transcript, cursor and exact admitted receipt. Host Run messages remain the
canonical activation snapshot; existing receipt reconciliation restores a
locally admitted typed input when a host confirmation was lost. Physical-id
sibling GC is skipped for this shared namespace. This is not a new cache
retention or exclusive writer ownership policy.

An explicit `storage_dir` stays exact. Old physical-id directories are neither
adopted nor migrated; malformed/wrong births do not get a new directory to hide
a conflict. Changing the host fabric scope or explicit directory has no local
receipt continuity guarantee. A recreated public id in an existing strict
store fails closed. New hosts refuse old workers missing capability; old pool
entries cannot satisfy the required bucket.

A legacy sender omitting creation can still perform its first activation when
that logical id is absent. A repeated legacy id in that store is unsupported
before seed/provider; the worker does not infer a birth from stored state.
Completely omitted logical identity retains the isolated random-id legacy
fallback, without same-Child continuity. Required-mode creation omission is
always an error. Required packets retain their fresh-only/no-continuation
boundary, as well as complete content/budget and read-only enforcement.

## Acceptance boundary

`tests/child_creation_identity.rs` runs the actual compiled worker with a
recorded loopback model provider and default storage. It exercises A→B→warm B
in one PID, then cold B in another PID; the same typed input, admission cursor
and receipt persist without duplication. Wrong birth/parent/Root/depth/Project
and missing required creation reject with zero additional provider requests
and unchanged durable B. Host Storage/cache conflict, atomic wire shape,
capability schema, pool segregation and legacy-first-only behavior have focused
unit coverage. The fixture's enlarged host libtest thread does not change the
native worker stack. Compiler artifact/source/profile identity is recorded
with execution evidence; a model response is not simulated runtime success.

The acceptance gate explicitly supplies `BAMBOO_1348_LEGACY_WORKER_IMAGE` to
exercise real `fleet::spawn_worker` against a fixed old image: missing birth
capability rejects before fabric/provision/Run and provider requests. Normal
CI may omit this optional old-image fixture; omitted execution is not evidence.
The recorded receipt identifies both images by hash.

The manual worker fixture supplies the same existing host read-only denylist
and permission enforcement, then verifies its actual provider callable catalog.
Existing startup currently loses the durable runtime `read_only` field (#1357);
this slice does not fix or claim acceptance of read-only field persistence.

No remote identity migration, task CAS, actor ownership, leases, new journal,
global SessionRepository cache rekeying or general GC protocol is introduced.
Dispatch checks do not replace #1341's future owner/final-mutation fences for
arbitrary lifecycle changes after an activation is already admitted.
