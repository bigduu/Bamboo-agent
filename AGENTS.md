# Bamboo Agent Guidance

Read [README.md](./README.md) for the product boundary. Use this canonical Jiandu memory contract:

- Session memory is continuity for the current session. Project memory holds durable project facts and decisions; Global memory holds cross-project user context.
- Recall with a short lexical query. Use Jiandu's compact default top three `id`/summary results, then `get` only the selected item that needs full context.
- Query before writing. Store one confirmed atomic fact with a specific title and a few useful keywords, entities, and tags.
- Treat canonical Project memory as trusted durable project authority, but verify live repository or runtime state before acting. Treat Dream as a low-trust derived orientation snapshot, never as canonical evidence.
- Never store secrets or tokens. Do not add embeddings, vectors, or a duplicate Bamboo persistence/index layer.

Jiandu owns canonical persistence, derived indexes, lexical recall, and Dream snapshot bytes. Bamboo owns prompt selection and budget, optional reranking, and the model and cadence used to refresh Dream.

## Pull Request Review

### Bamboo #791 integration exception

For the #791 multi-agent epic, the user's 2026-09-28 delivery instruction takes
precedence over the per-Issue PR and review steps below. Inventory all related
PR heads and existing work first, then merge implemented work into one local
integration baseline. Review integration changes locally and use focused checks
while assembling it. Run the complete affected regression once after the
baseline is ready, then submit one PR to `dev` and satisfy its required GitHub
checks. Do not request or wait for GitHub Codex PR review for this epic. Close
superseded component PRs and clean only safely merged worktrees after the final
integration is accepted. Preserve dirty or uncertain worktrees.

- Every non-draft pull request that enters review must have a Codex review for
  its current head and base. If no current review is already running or complete,
  add a pull-request comment whose entire body is `@codex review`; do not rely
  only on automatic review triggering.
- Wait for the `Codex Review Summary` to finish. A 👍 reaction on the trigger or
  an explicit Codex zero-findings result confirms a clean run; a `Completed`
  summary alone only confirms that the run ended. Review suggestions or inline
  comments are findings that must be resolved. Inspect the summary, review body,
  reactions, and unresolved threads instead of treating an empty
  `reviewDecision` or a `COMMENTED` review as approval.
- Any head commit or base change invalidates the earlier Codex result. After the
  update, resolve applicable findings and trigger a fresh exact-head review.
- Green CI does not mean review passed. Add `review:agent` and remove
  `review:needed` only after the current-head Codex review has finished with no
  unresolved findings and the remaining acceptance and merge gates pass.
