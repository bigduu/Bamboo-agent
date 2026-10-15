# WorkflowRun runtime topology

The public WorkflowRun snapshot is the authority for placing runtime steps in a
plan tree. Reconnect events retain their existing sequence/invalidation role.
This additive contract does not expose workflow inputs, Map values or names,
bindings, outputs, capabilities, credentials, or storage paths.

Every public plan leaf includes `step` and `node_path`. A path starts at the root
with `[]` and appends one numeric child ordinal per plan edge:

| Plan node | Child ordinal |
| --- | --- |
| Sequence or Parallel | Index in `nodes`, starting at zero |
| Choice | `then_branch` = 0, `else_branch` = 1 |
| Map | `body` = 0 |
| Retry | `node` = 0 |

New runtime step snapshots include an optional `topology` object:

```json
{
  "id": "opaque-runtime-id",
  "topology": {
    "planned_step_id": "inspect",
    "plan_node_path": [1, 0, 0],
    "map_index_path": [2, 5]
  },
  "status": "succeeded",
  "attempts": 1
}
```

Join `topology.plan_node_path` to the plan leaf's `node_path` within the same
run and pinned workflow revision. `planned_step_id` equals that leaf's `step`
and references the existing `planned_steps` entry. Paths identify topology,
not filesystem locations. Treat `id` as opaque; its internal spelling is not a
client parsing contract.

`map_index_path` contains only zero-based array indices, ordered from the
outermost Map to the innermost entered Map. Ordinary runtime instances have
one index per Map ancestor. Retry attempts retain the same leaf and index path;
they update the existing cumulative `attempts` count. Separate nested workflow
runs have their own plan root and index path.

An unexpanded skipped subtree or timeout frontier can create a zero-attempt
placeholder. Only a skipped/failed zero-attempt row with no input hash or
output may publish a shorter index path. Its index path is only the known prefix
of already-entered outer Maps. For example, `[2]` for a leaf under two Maps means the inner Map was never
expanded; it does not identify inner item zero. Previously materialized steps
retain their full index paths when later skipped or cancelled. An unexpanded
placeholder and subsequent concrete instances can coexist under the same leaf.

The mapping is persisted alongside each step before publication. File snapshot
reload, journal recovery, cancellation and same-run continuation preserve stored
metadata. Restart-as-new creates instances under a new run ID using the pinned
definition. Leaf addresses stay fixed; index paths reflect the new execution's
enumerated Map inputs.

Historical snapshots without `topology` remain readable and public steps omit
the field. Clients should retain those rows as unmapped until explicit metadata
is available. Do not infer Map positions from runtime IDs. Invalid or ambiguous
stored mappings are also omitted by the public projection. Repeated references
to one step remain unsupported by the existing compiler; they are not resolved
by choosing an arbitrary leaf.

Runtime ID collision avoidance preserves the ordinary existing spelling. When
a Map instance would occupy a declared step's ID, the engine uses a deterministic
noncolliding opaque ID. Bindings, skip and cancellation use the same explicit
step identity and Map scope, so a user-chosen step name cannot merge two leaves.
