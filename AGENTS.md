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
The [CI and publication flow](./docs/releases/ci-publication-flow.md) records
the phase contracts: validate the frozen release before merge, authenticate
that evidence on the actual main merge, then publish that source. A successful
lightweight main admission is evidence reuse, not another full test run. Never
replace dev compilation or release acceptance with skipped/stub checks, or
substitute another commit's binary for a versioned release build.
After one authenticated version/frontend preparation, crate publication and
the source-selected native matrix run concurrently. Native workers consume
hash-bound same-run frontend transport without publication credentials. Only
the final publisher joins completed crates and all required native receipts;
parallel builds never create another allocator or publication queue.

## Pull Request Review

### Inactive frozen-source foundation (#2323)

`scripts/frozen-source-admission.cjs` produces read-only source context, with
`publicationAuthorized: false`. Its default complete-producer registry is empty.
Do not pass test fixture policy into an actual controller, treat that context as
a signed reservation/publication receipt, or wire it into the live publisher.
The current compile-only route, A339 and unexecuted C/native drafts do not earn
v2 acceptance. Preserve all active legacy gates and genuine v6/HMAC recovery.
Future activation must pin a fully accepted feature producer and its helper
closure, with complete Unit/E2E/native/quality ownership and original physical
jobs. Independently prove the exact merged PR and normal B/F/M/S identities when
GitHub drops run PR associations. Keep actual main controller C separate from
product checkout S and never rewrite GITHUB_SHA. See the
[foundation contract](docs/releases/ci-publication-flow.md#inactive-frozen-source-foundation-2323)
for the read adapter, pending owners and verification command.

### Inactive Source-dispatch authority (#2336)

`verifyAcceptedDevFacts` can inspect accepted feature evidence before a frozen
ref exists or a dispatch starts. It returns `publicationAuthorized: false`,
without a controller or frozen-ref claim. `resolveFrozenSource` adds the actual
protected-main C and frozen-S checks. Neither data object is a signing capability.
Only `crate-release.cjs`'s `makeFrozenSourceContext()` may construct the new writer;
it reads the actual dispatch event and calls the real resolver with fixed GitHub
reads, the official check-suite query and original physical logs. Never inject
fixture policy or turn an environment JSON/boolean into authority. Keep the
production registry empty until every feature owner has actual accepted evidence.

Phase A is helper-only and does not change publisher YAML. Its distinct
`sourceDispatch` receipt field uses the existing allocator, HMAC key and writer;
it cannot upgrade a signed legacy reservation. C helpers must be ordinary files
from the actual C checkout, outside S, before any candidate execution. Use S as
verified data and preserve real `GITHUB_*` identities. Every write/retried PUT
must use the same fresh source/ref/dev-protection/reservation guard. Native and
crate adapters, the sole whole-release completion and dispatch wiring need the
separate reviewed handoff; the legacy package/publish/complete path rejects this
new contract. Main promotion/protection/production activation remain separate.

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
actual protected-`dev` HEAD after the Release Process prerequisites have merged.
Its bounded early-cut route permits a known pending frontend consumer only when
recorded at the cut and later admitted through the full separate `F`/`U` procedure.
Record the cut timestamp, SHA, tree, fresh-read receipt, and bounded scope;
review must verify the receipt and normal branch creation. Promote that frozen
branch to `main`. Keep `dev` open; never chase its moving tip in a release. Necessary
release-based fixes must pass normal review/checks into protected `dev` with
exact ancestry preserved before fast-forwarding the release. Keep frozen tags
immutable and retain all required checks, platform coverage, and protection.
