# Memory and recall

## Ownership and scope

Session memory carries continuity for the current session. Section memory holds
facts shared within a host-assigned Section. Project memory holds project-wide
facts and decisions. Global memory holds cross-project user context. The host
uses the saved assignment to authorize Project and Section memory access.
Use the `project` type with Project or an assigned Section scope for a new
write. Global writes support cross-project references and preferences.

Jiandu owns canonical persistence, derived indexes, lexical recall and stored
Dream snapshot bytes. Bamboo owns prompt selection and budgets, and the model
and cadence used to refresh Dream. Use Jiandu's canonical memory index and
embedding-free retrieval.

Jiandu's default data root is independent of Bamboo, normally `~/.jiandu`.
Bamboo's integration can receive `BAMBOO_JIANDU_DATA_DIR`. Do not assume memory
lives at `~/.bamboo/memory`, infer actual instance paths from these defaults, or
edit backing files directly. Use the supported memory tool and its permissions.

## Retrieval and prompt assembly

Look up saved memory when past decisions or preferences matter to the task.
Discover `memory` if needed, then use `action=query` with short lexical keywords
and the appropriate scope. Jiandu's compact default recall returns up to three
IDs and summaries. Use `action=get` only for selected items whose full context
is needed. Use `action=session_read` for continuity notes from the current
Session. Query before writing, then store one confirmed atomic fact with a
specific title and useful keywords, entities and tags. Keep secrets and
credentials out of memory.

Automatic prompt fields `memory.project_prompt_injection`, `memory.relevant_recall`,
`memory.relevant_recall_rerank` and `memory.project_first_dream` are deprecated.
Bamboo ignores them and their legacy environment variables.

When the task needs upcoming events, reminders, or due commitments, call
`ledger` with `{"action":"agenda"}`. If the tool is deferred, look up `ledger`
with tool discovery. For the fallback discovery tool, call
`discover_capabilities` by itself with `{"query":"ledger","limit":1}`, then call
`ledger` in a later round. Set `memory.ledger_agenda_injection` to `true` to
include the agenda automatically.

## Dream and observations

Dream is a low-trust derived orientation snapshot, not canonical evidence.
Verify consequential claims against durable Project memory and live facts.
Automatic Dream ticks default on, but configured models, eligible work and
runtime gates matter; a tick is not proof of refresh or a model call.

PromptMemoryObservability records flags, status, counts and character volumes
for assembly. Existing memory metrics can combine scope inspection and prompt
observations. They do not prove that a provider received or relied on a
particular memory, and some fields can reveal private Project identity. Do not
dump the whole object or memory bodies into a diagnostic model request.

If a lookup returns no results, check the current Session's assigned scope, the
tool's availability and the search terms. Prefer current, authorized status and
count evidence. Index rebuild, Dream refresh and storage migration have their
own operations.

Source: `AGENTS.md`; `crates/infra/bamboo-memory/src/memory_store/mod.rs`;
`crates/engine/bamboo-engine/src/runtime/runner/prompt_context/external_memory.rs`;
`crates/core/bamboo-domain/src/session/types.rs`;
`crates/infra/bamboo-config/src/config.rs`. Historical memory redesign proposals
are not a substitute for this version's implementation.
