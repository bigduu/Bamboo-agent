# Bamboo Agent Guidance

Read [README.md](./README.md) for the product boundary. Use this canonical Jiandu memory contract:

- Session memory is continuity for the current session. Project memory holds durable project facts and decisions; Global memory holds cross-project user context.
- Recall with a short lexical query. Use Jiandu's compact default top three `id`/summary results, then `get` only the selected item that needs full context.
- Query before writing. Store one confirmed atomic fact with a specific title and a few useful keywords, entities, and tags.
- Treat canonical Project memory as trusted durable project authority, but verify live repository or runtime state before acting. Treat Dream as a low-trust derived orientation snapshot, never as canonical evidence.
- Never store secrets or tokens. Do not add embeddings, vectors, or a duplicate Bamboo persistence/index layer.

Jiandu owns canonical persistence, derived indexes, lexical recall, and Dream snapshot bytes. Bamboo owns prompt selection and budget, optional reranking, and the model and cadence used to refresh Dream.

## Parallel Development

Follow [Parallel Development](./gitflow.md). Give each bounded Issue one owner,
branch and isolated worktree; only the named integrator writes a shared batch
branch. Dev CI admission requires the `Test` check to pass its compile-only mode.
Before admission, run the affected tests locally, add or update coverage for changed
behavior, and record exact source, commands and results in the PR. Auxiliary
`Quick Checks` do not replace compilation or local test evidence. Dev CI does
not run the full test suite on every PR. Freeze a recorded batch for complete
release acceptance; `release → main` must pass the full required suite and
platform matrix. Keep one release owner and one writer per build output; never
cancel or clean another worker's resources. Verify the separately approved
protection configuration and never bypass current gates. Normal direct pushes
must be fast-forward and satisfy the same evidence and checks; never force push.
Use the authoritative
[Release Process](./CONTRIBUTING.md#release-process) for cuts, repairs and publication.

## Pull Request Review

### Bamboo #791 integration exception

For the #791 multi-agent epic, the user's 2026-09-28 delivery instruction takes
precedence over the per-Issue PR and review steps below. Inventory all related
PR heads and existing work first, then merge implemented work into one local
integration baseline. Review integration changes locally and use focused checks
while assembling it. Submit one PR to `dev` with the required compilation gate
and local test/coverage evidence under the current gate policy. Run the complete affected regression once
on the frozen release snapshot before promotion to `main`. Do not request or
wait for GitHub Codex PR review for this epic. Close superseded component PRs and clean only safely merged worktrees after the final
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

## Release Branches

Follow the authoritative [Release Process](./CONTRIBUTING.md#release-process):
cut a same-repository `release/<40-character-cut-SHA>` from the freshly read
actual protected-`dev` HEAD after required changes and controls have merged.
Record the cut timestamp, SHA, tree, fresh-read receipt, and bounded scope;
review must verify the receipt and normal branch creation. Promote that frozen
branch to `main`. Keep `dev` open; never chase its moving tip in a release. Necessary
release-based fixes must pass normal review/checks into protected `dev` with
exact ancestry preserved before fast-forwarding the release. Keep frozen tags
immutable and retain all required checks, platform coverage, and protection.
