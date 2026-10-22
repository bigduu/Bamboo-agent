# Conversation generation: persisted state and default-writer fences (C3a)

This slice stacks on C2 PR #2154 / `f5dcb1206f13cb8b50dc87a942a896e44c359c3c`.
It implements the canonical generation schema and the default-writer portion of
the [reset transaction design](idle-conversation-reset-transaction.md). It does
not register a reset writer or HTTP route, change the existing activated-context
guard, clear an actual conversation, or deliver activated-idle Supervisor reset.
Issue #2152 already owns this implementation sequence; no duplicate reset issue
is needed. Prompt PR #2146 is separate.

## One persisted state and shared check

`Session.conversation_reset: Option<ConversationResetState>` is authoritative
inline state in Main and runtime. Absence is the sole zero-generation spelling;
old Sessions are not migrated. A present zero ledger, future schema, malformed
receipt chain, wrong birth, or Child ledger is invalid. Roots and Children made
by ordinary constructors start absent. Independent copies explicitly clear it.

The existing C2 bounded ledger carries the generation and last eight operation
receipts. The shared Domain API is role neutral and grants no access:

| Method | Meaning |
| --- | --- |
| `conversation_revision()` | Observation; zero for absence. Callers must still validate. |
| `validate_conversation_state()` | Validate schema, Root shape, receipt chain and exact lifetime. |
| `check_conversation_generation(&canonical)` | Validate both states, then require the same lifetime and complete ledger. Never adopt a revision into stale messages. |
| `conversation_reset_sha256()` | Domain-separated SHA-256 of the bounded validated inline ledger, for proof projection. |

`ConversationGenerationError` distinguishes `InvalidState`, `LifetimeChanged`
and `StaleGeneration`. Original absence/absence Sessions retain their existing
identity checks at their storage/admission boundary. The new method does not
relax those checks. Changes to messages or other normal fields within one exact
generation remain subject to their existing authority and CAS checks.

The C2 preparation result still separates `proposed_session` and
`proposed_state`. The future transaction must attach the state to the proposed
Session and atomically publish the pair after revalidation. A prepared value,
even with that field attached in a fixture, cannot advance ordinary storage.

## Projection versioning and mixed-runtime boundary

Generation-bearing Main uses compact authority frame v3 and duplicates the
complete bounded ledger in the checked flat Main suffix. Existing zero-state
Main v1/v2 framing and absent fields remain unchanged. A v1/v2 header carrying
the new ledger, or v3 without it, is invalid. The existing 512 KiB section cap
continues to apply.

Generation-bearing Roots require Root tool proof v3. Its added projection is
`conversation_reset: { revision, sha256 }`; the full receipt list stays inline
and is not copied into the proof. The existing 4096-byte proof cap remains.
Main/runtime must agree exactly, and the committed proof must match the ledger
digest. Missing, downgraded, prepared or inconsistent proof cannot authorize a
normal write. No proof repair is added for nonzero state.

The immediately preceding C2 runtime recognizes compact frames v1/v2 and Root
proof versions derived as v1/v2; v3 is rejected by those existing checks. This
is a source-level compatibility boundary, not a runtime result or a guarantee
for custom Hosts that bypass Bamboo's checked persistence ports. Retained Actor
projection support still needs C3b before reset can produce v3 in production.

## Implemented default and merge boundaries

All checks below run before changing canonical files. Existing lifetime,
Project/workspace, mode, Supervisor proof, Actor, Task and Inbox guards remain.
No new lock protocol, authority bypass, directory claim or auto-activation is
introduced. Storage maps generation mismatch to a typed
`SessionAuthorityConflict` (`WouldBlock`); malformed incoming state at the
compatibility preflight uses the same typed error with `Unsupported`. These are
internal refusals, not new public HTTP error codes or success receipts.

| Existing final path | Added C3a fence and refusal |
| --- | --- |
| `v2::save_session_after_lock_with_title_intent` | Compare the full incoming ledger to canonical Main before serializing/publishing runtime, Main, proof, index or search. |
| `v2::save_runtime_state_after_lock` | Compare against strict canonical Session and previous runtime before runtime-only publication. Main generation cannot be silently adopted. |
| `root_context::validate_root_context_for_write` | Validate incoming state, compare canonical runtime, verify proof and (full save) Main/runtime pair; a fresh/missing-Main Root cannot introduce receipts. |
| `root_context::validate_root_tool_authority_pair` | Validate side state and require exact Main/runtime ledger equality. Partial publication remains unreadable through this checked pair. |
| `v2::write_runtime_sidecar_durable` | Existing Task final writer uses the shared Root-context fence. Task recovery inventory below is still required. |
| `RuntimeSessionPersistence::checkpoint_runtime_session` default | Refuse before appending messages, merging native groups/Inbox or invoking the custom save. Custom overrides still need an independent audit. |
| `LockedSessionStore` runtime merge, runtime-only, metadata commit | Refuse before adopting canonical metadata/model context or invoking storage/cache publication. |
| Task-conflict retry, inherited child-wait reconciliation, activation seed | Refuse before adopting the newer durable control plane into an older conversation. |
| Provider-bound checkpoint | Shared model-context checkpoint guard checks generation before append/merge. |
| Retrieval/prompt rewrite/manual archive checkpoints | Refuse both old base and staged generation before any `Rebased` branch; returning old messages with a copied new ledger is forbidden. |
| Response candidate selection | Refuse a stale cached generation before comparing timestamps or merging messages/consumed questions. A newer cache timestamp does not authorize old history. |
| Legacy `clear_session` | Nonzero state is refused: only the future fenced reset transaction may perform another reset. Existing #1350 activated/finished/retired/unknown refusal is retained. |
| Copy | Source remains unchanged; new independent Root has no reset ledger. Copy is not an in-place reset path. |

Callers must completely reload after a generation conflict; retrying the old
snapshot, copying only a new counter, or minting another Actor lease cannot make
it current. Existing typed-authority refusal suppresses cache-on-error publish.
Ordinary writers cannot publish the first generation or change an existing
ledger, even if their numeric revision increases or their new ledger is valid.

## Required C3b writer/reader inventory before any reset writer

This is the remaining concrete audit and implementation scope, not a claim that
all final writers are fenced. Most paths share the checked final writer above;
raw splicers and partial readers must be closed explicitly.

| Remaining owner/seam | Required next implementation |
| --- | --- |
| `v2/actor_transcript.rs`: `Source::read_checked`, `append_actor_transcript`, `patched_main`, final `BeforeReplace` | Carry the observed generation in the request; validate Main/runtime/proof and compare expected generation under the retained physical guard. Raw prefix/message equality alone cannot fence a reset followed by a fresh lease. |
| `v2/actor_input.rs`: claim seed and input checkpoint raw Main replacements | Bind caller generation before consuming/committing Inbox claims, verify it at final replacement/readback, and preserve the complete ledger. Inbox receipt/generation remains a different contract. |
| `v2/actor_model_context.rs`: raw ledger splice/checkpoint | Check expected Session generation before native/context merge and at final replacement; do not move old provider context into a new generation. |
| `v2/root_actor_runtime.rs`, `child_actor_runtime.rs`, `root_actor_input.rs`, `actor_checkpoint_lineage.rs`, `default_actor_context.rs` | Retain generation in the execution-private proof/binding or original expected Session and compare canonical state even after a fresh activation. Existing Running/Ordinary checks do not prove conversation freshness. Reuse the existing per-invocation physical writer guard/lifecycle; do not rewrite it. |
| `v2/actor_snapshot.rs` and retained snapshot readers | Partial `Identity`/sidecar decode must carry the inline state into checked Root pairs and snapshots. Do not make nonzero data look absent or silently copy state after selection. |
| `v2.rs` Task CAS/recovery: `load_runtime_task_recovery_control_plane`, journal identity/restore, `write_runtime_sidecar_durable` | Audit every original/updated/current comparison and recovery write for generation preservation, exact canonical pair and stale journal refusal. The common final guard is necessary but does not alone prove recovery. |
| `v2/supervisor_management.rs`, `parent_question_checkpoint.rs`, `broker_receipt.rs`, `child_wait_control_plane.rs`, `scoped_loop_control.rs` | Verify complete-load and default-save paths preserve the ledger; add missing comparisons before narrow mutation/publication. Do not widen their ownership to conversation state. |
| Startup proof/sidecar migrations, `supervisor.rs`, `root_lifetime.rs`, copy/recovery/index rebuild | Nonzero state must be preserved only for the same known lifetime; no startup backfill from a stale projection, zero-state reconstruction or reset via delete/recreate. No operational reads through prepared/unknown transaction state. |
| Core/Engine/SDK/Host admission and custom `RuntimeSessionPersistence` overrides | A-owned caller/tool context consumes this Domain API. Capture immutable generation from the original caller, then compare before context adoption/provider/tool execution. New execution authority is not conversation freshness. |

Only after those paths are source-frozen and validated may C4 introduce a
private `IdleConversationResetWrite` and recoverable transaction. C4 must reuse
the existing lifecycle/Task/Session/Actor-tree lock order, job-owned guards after
HTTP cancellation, per-invocation acquire and closed shutdown/join behavior.
It must retain actor identity/scope/managed Roots/unfinished work and reject
active, pending, unknown or recovery-required state. No reset route is added here.

## Validation and remaining acceptance

New synthetic test sources cover six Domain cases and seven storage cases:
zero-state omission, full-ledger equality, old/future generation refusal,
wrong birth/schema/Child state, default checkpoint refusal before mutation,
ordinary first-generation refusal, stale/same-revision final writes, exact
generation append/runtime preservation, v3 framing and bounded proof,
missing/downgraded/inconsistent proof, legacy-clear refusal/independent copy,
and stale merge/checkpoint/timestamp-newer cache refusal without publication.
Storage fixtures use private writes only in fresh `TempDir`s to represent a
future producer. They do not constitute a production reset or recovery path.

All 13 tests, compilation, Cargo, Clippy, actual provider payload capture,
restart/fault/cancellation recovery and cross-client acceptance are **UNRUN**.
Only direct rustfmt parsing/formatting, diff checks and source audit are allowed
until the parent hands over the Rust window. Static review cannot replace those
tests or normal protected admission. Activated-idle Supervisor reset remains
unimplemented; future canonical HTTP readback/feed generation and controlled
actual provider payload acceptance remain mandatory.
