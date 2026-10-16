# Workflow definition authoring

The explicit authoring API creates and updates schema-1 orchestration definitions
in the existing Skill catalog. It does not execute a plan or approve execution.
Markdown instruction-workflow settings and `workflow_run` remain separate.

## Endpoints

All paths below are relative to the authenticated `/api/v1` scope.

- `POST /bamboo/workflow-definitions/validate`: `{ "definition": { ... } }`.
  Checks the existing compiler's schema, budgets, plan, step references and data
  bindings without writing files or calling a model/tool. `execution_authorized`
  is always false. Syntax validation alone does not resolve nested catalog refs.
- `POST /bamboo/workflow-definitions`: `target`, optional `session_id`,
  `description`, and typed `definition`. A new definition must have revision 1.
- `GET /bamboo/workflow-definitions/{id}?target=user`: read the owned definition
  and its exact editing tokens. For Project scope add `target=project` and the
  `session_id` of a durably assigned Session.
- `PUT /bamboo/workflow-definitions/{id}`: `target`, optional `session_id`,
  `expected_revision`, `expected_digest`, `expected_skill_digest`, and the next
  `definition`. The id is unchanged and revision increments exactly once.

`target` is only `user` or `project`. User scope rejects a Session selector.
Project scope derives its directory from the durable Session's assigned Project,
never from caller-supplied paths or Project identifiers. The Session scope lock
stays held through publication, including after HTTP cancellation.

Example create body:

```json
{
  "target": "user",
  "description": "Inspect selected repository files",
  "definition": {
    "workflow_schema": 1,
    "id": "inspect-files",
    "revision": 1,
    "steps": [
      {
        "id": "inspect",
        "type": "tool",
        "tool": "Read",
        "args": { "file_path": "/absolute/path/README.md" },
        "capabilities": ["read"]
      }
    ],
    "plan": { "type": "step", "step": "inspect" }
  }
}
```

The response returns the normalized definition, description, source,
`content_digest`, `skill_digest`, `saved`, and `catalog_available`. The first
digest is SHA-256 of the exact `workflow.yaml` bytes; the second is SHA-256 of
its immutable `SKILL.md` header/body. Neither token is the catalog's separate
bundle `catalog_content_digest`. Echo both editing tokens for an update.
Stale revision, changed metadata, replacement file identity or an existing
create destination produces a conflict rather than an overwrite.

A filesystem save can succeed while a subsequent catalog refresh is unavailable;
`saved: true, catalog_available: false` records that distinction. An uncertain
publication error means the caller must GET and reconcile before retrying. It
is not evidence that the old bytes survived.

## Boundaries

- The canonical location is the existing owned Skill bundle, containing generated
  immutable `SKILL.md` and `workflow.yaml` (JSON is valid YAML). There is no new
  definition registry or run format.
- This first slice only edits bundles marked by this authoring API. It does not
  adopt arbitrary external bundles, builtins, plugins, or builtin-clone
  publications. Existing unmanaged content remains untouched. External metadata
  edits require reconciliation rather than silently losing fields.
- Definition bodies are bounded at 256 KiB, Skill metadata at 64 KiB and the
  description at 1,024 bytes. Unknown request and nested definition fields are rejected.
  `invocation_policy` and catalog policy overrides are not authoring inputs.
- New definitions are explicit/manual-only. Saving does not change Session
  automatic opt-in, grant tool permissions, make Supervisor an execution worker,
  or bypass confirmations. Current runtime Tool/Agent read-only allowlists apply
  independently even when a definition claims stronger capabilities.
- Nested Workflow references retain exact revisions and are resolved in the
  selected catalog before a save. An update cannot introduce a dependency back
  to the definition being replaced. Later catalog changes can still make a
  dependency unavailable; runtime pinning fails closed in that case.
- Start, recovery restart and completed-prefix continuation derive execution
  workspace and automatic opt-in from durable Session state, not stale cached
  identity. A Project with no source checkout can use its owned Project catalog
  and an instance-owned session execution directory. Listing runs does not
  change the execution workspace. Existing recovery eligibility and permission
  gates are unchanged.
- Project/Workspace switching rejects both workflow admission and active workers,
  including cleanup that outlives a terminal snapshot. The admission fence stays
  owned if an HTTP caller disconnects before the engine's durable-start reply.
  Read-only Agent steps use the same durable scope resolver for their child
  workspace and never overwrite the parent's runtime path from a legacy field.
- Existing runs retain their immutable definition bundles. A new run must select
  the new revision explicitly; updates never rewrite a running or persisted run.
- Writes use the shared descriptor-bound publication lock, bounded no-follow
  reads, no-replace creation and atomic file replacement. Failed/uncertain stages
  are retained outside catalog discovery instead of deleting uncertain content.

This API is the first authoring foundation. An Agent-facing mutation tool and a
minimal creation/editing UI remain separate consumer work; the API alone is not
an end-to-end natural-language authoring editor. DAG runtime visualization uses
[the topology contract](WORKFLOW-RUN-TOPOLOGY.md), not definition-file access.
