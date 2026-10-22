# Memory and recall

## Ownership and scope

Session memory carries continuity for the current session. Project memory holds
durable project facts and decisions. Global memory holds cross-project user
context. Project identity and permission come from the host; a workspace path
or an agent-invented project key cannot grant Project memory access.

Jiandu owns canonical persistence, derived indexes, lexical recall and stored
Dream snapshot bytes. Bamboo owns prompt selection and budgets, optional
reranking, and the model and cadence used to refresh Dream. There is no Bamboo
embedding/vector store or second canonical memory index in this contract.

Jiandu's default data root is independent of Bamboo, normally `~/.jiandu`.
Bamboo's integration can receive `BAMBOO_JIANDU_DATA_DIR`. Do not assume memory
lives at `~/.bamboo/memory`, infer actual instance paths from these defaults, or
edit backing files directly. Use the supported memory tool and its permissions.

## Retrieval and prompt assembly

Use a short lexical query. Jiandu's compact default recall returns up to three
IDs and summaries; get only selected items whose full context is needed. Query
before writing, then store one confirmed atomic fact with a specific title and
useful keywords, entities and tags. Do not store secrets or credentials.

Bamboo assembles session notes, a project index, relevant durable-memory
results, Dream and ledger agenda under prompt budgets. Relevant recall builds
a deterministic lexical shortlist. `memory.relevant_recall_rerank` optionally
uses the configured memory/background model to rerank it; failure falls back
to lexical ordering. Enabling rerank does not create embeddings and does not
prove a model was called on a particular turn.

The key catalog tests defaults against typed code: relevant recall and project
index injection default on; model reranking defaults off. Eligibility, scope,
available candidates and prompt budgets can still result in no injected memory.
Configured defaults alone are not evidence of the actual prompt contents.

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

If recall seems absent, first distinguish disabled injection, absent Project
scope, no lexical matches, budget exclusion, unavailable model reranking and
stale/absent Dream. Prefer current, authorized status/count evidence. Do not
automatically rebuild indexes, refresh Dream, migrate storage or run a model to
diagnose the setting; those are separate actions.

Source: `AGENTS.md`; `crates/infra/bamboo-memory/src/memory_store/mod.rs`;
`crates/engine/bamboo-engine/src/runtime/runner/prompt_context/external_memory.rs`
and `memory_rerank.rs`; `crates/core/bamboo-domain/src/session/types.rs`;
`crates/infra/bamboo-config/src/config.rs`. Historical memory redesign proposals
are not a substitute for this version's implementation.
