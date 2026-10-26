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
branch. Every feature PR to `dev` must pass required `Test`, which aggregates
Unit, E2E and the live SSH fixture on its actual merge source. One Linux Cargo
executor owns one target directory and preserves independent default package
configurations. Cargo runs each package's library/binary, integration and
documentation cases once. Explicit feature owners run only their catalogued
additional cases; new feature-sensitive cases require ownership review.
Compatible configurations reuse compiled artifacts, with features/profile and
actual Cargo freshness recorded. The ignored SSH case reuses its compiled
executable. Unit/E2E are logical target groups. All agents add ordinary cases to that
Cargo target catalog, instead of creating another Linux workflow for a covered
case subset. CI policy rejects unregistered Rust test producers; native OS,
containment-boundary, explicit manual diagnostics and pinned legacy routes are
scoped exceptions with named owners. Preserve
applicable native-platform acceptance before dev admission. Fix failures in the
feature branch before merging; record exact source, commands and results.
Keep the standalone default storage authority families' serial selection in that same
executor, reusing its compiled library-test executable without repeating cases.
`Quick Checks` remains auxiliary and never substitutes for runtime tests.

The intended release contract is feature tests -> accepted dev cut -> one version
reservation and release build/publication -> main promotion of the published
source. Release branches do not repair product code or rerun Unit/E2E; main
records stable code through immutable tags, Release manifests/provenance and the
promotion description, without another build or test run. Keep source manifests
at `0.0.0` and stamp only disposable formal-build checkouts. Repairs return
through feature/dev tests and produce a new frozen cut.

Cutover dependency: #2311 installs feature admission only. Existing frozen
release acceptance and main-triggered publication remain active until the
separate release-source admission change is accepted. Do not bypass those live
gates or dispatch the new release route before that cutover. Before the first
new cut, normally integrate current main ancestry into dev through a tested
feature PR. Preserve historical signed receipts and immutable tags. Use the
[Release Process](./CONTRIBUTING.md#release-process) and
[CI and publication flow](./docs/releases/ci-publication-flow.md) to distinguish
implemented feature admission from the pending release protocol.

## Pull Request Review

### Bamboo #791 integration exception

For the #791 multi-agent epic, the user's 2026-09-28 delivery instruction takes
precedence over the per-Issue PR and review steps below. Inventory all related
PR heads and existing work first, then merge implemented work into one local
integration baseline. Review integration changes locally and use focused checks
while assembling it. Submit one PR to `dev` with required Unit/E2E and local
test/coverage evidence under the current feature-admission policy. Complete
affected regression before dev admission; the target release path builds the
accepted product without repeating those tests. Do not request or
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

Follow the authoritative [Release Process](./CONTRIBUTING.md#release-process).
After release-source admission is enabled, cut a same-repository
`release/<40-character-cut-SHA>` exactly at the freshly read, tested protected-dev
commit. Record its timestamp, SHA, tree and source receipt. Keep that product
source fixed through version reservation, native builds and publication; normal
dev advancement does not change it. Promote only the exact published source to
main. A needed product repair gets a feature branch, passing tests and dev
admission, then a fresh cut. Existing frozen release/recovery contracts remain
unchanged during the explicitly documented cutover dependency.
