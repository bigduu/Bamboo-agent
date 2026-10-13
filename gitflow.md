# Parallel Development and Release Flow

Use `feature/fix → integration/<batch> → dev` for related work. Independent,
bounded changes may go directly to `dev` through the normal gates. An integration
batch is a finite delivery scope, not another long-lived development branch.
The target release path is `dev → release/<40-character-cut-SHA> → main`, subject
to the rollout below. The
[Release Process](./CONTRIBUTING.md#release-process) remains authoritative for
currently supported release operations and publication prerequisites. Read this
file with the review requirements in [AGENTS.md](./AGENTS.md#pull-request-review).

## Ownership and parallel work

- Give each bounded Issue one owner, a unique branch and an isolated worktree.
  Record the exact starting `dev` or integration commit, affected paths,
  dependencies and acceptance criteria in its Issue/PR. Declare dependent PRs
  before implementation; coordinate overlapping paths instead of racing writes.
- Give each batch one integrator and one `integration/<batch>` branch, pinned
  initially to a recorded `dev` commit. Feature PRs target that branch. Only the
  integrator writes it, resolves cross-feature conflicts and updates its final
  PR to `dev`; other owners return changes through their own branches. Explicitly
  hand off ownership before replacing the integrator. Keep existing review rules.
- Workers run focused tests for their changes and dependencies. The integrator
  reviews the combined diff, includes only the agreed scope, then records the
  final head, tree and target-base SHA and freezes the candidate. Late work goes
  into the next batch unless it fixes an acceptance blocker.
- Run the full required suite on that actual combined candidate, then admit it
  through one normal PR to protected `dev`. Serialize final admissions: queue
  other batches and standalone PRs rather than repeatedly refreshing each other.
  A changed head/base must satisfy current review and checks again. Development
  on separate branches continues while this short admission queue is occupied.

## Evidence and resource discipline

- Record each check's source SHA/tree, actual checkout or synthetic merge,
  toolchain, lock/features, profile/flags, platform/environment, frontend artifact,
  command and result link. Report stages as not run, running, passed, failed, cancelled or inconclusive;
  compilation is not runtime acceptance and component results are not batch green.
- Before another expensive local build, inspect existing receipts. An ancestry-
  only change may reuse same-tree evidence only when all relevant build/test
  inputs and contracts match, including commit-derived build metadata. Changed
  code, dependencies, toolchain, profile, platform or acceptance contract needs
  affected validation again. Tree equality alone is insufficient. Never relabel
  another head's result or synthesize a successful GitHub status: required checks belong to the actual current head/base.
- Assign one writer per worktree, target directory and test-output directory.
  Concurrent workers must not write the same build cache/output; compatible
  cache reuse happens only after the prior writer exits. Bound concurrency by
  measured host memory, disk and workload cost; queue heavy builds rather than
  inventing thread counts or starting competing cold builds on the same host.
- Only a run's owner or explicitly designated integrator may cancel a superseded
  run. Preserve its result and reason. Never cancel another active batch/release,
  retry a real failure to get green, kill unrelated processes, or clean another
  worker's worktree/cache. Preserve dirty or uncertain work and failure evidence.

## Close a batch once

Before admission, inventory related PRs and verify their exact ancestors or
changed postimages against the final candidate. Once the batch is accepted,
close superseded component PRs with the accepted commit and equivalence evidence;
do not separately merge the same slices into `dev` and trigger another admission.
If equivalence is incomplete, keep the missing work open. Preserve PR discussions,
source references and validation receipts; cleanup is a separate owned action.

## CI deduplication rollout

The process above does not itself change workflow triggers. Today
`.github/workflows/ci.yml` runs the required `Test` on PRs to `dev`/`main`, and
dev PRs matching `frontend-acceptance.yml`'s path filters also run its full
Build/Test. Their default locked build/test commands overlap, but the frontend
workflow additionally validates the exact head and source cleanliness and has
separate artifact-policy/Clippy contracts. Preserve those contracts and record
the toolchain during consolidation.

Land one bounded CI control PR through the existing gates before enabling the
new trigger layout. Its acceptance criteria are:

1. PRs into `integration/<batch>` get scoped checks; batch PRs into `dev` and
   standalone dev PRs retain the full required gate. Missing scoped coverage
   falls back to full validation. Until these triggers are implemented, workers
   record their scoped checks explicitly; absent Actions runs are not a pass.
2. Keep one authoritative full default-workspace `Test`, retaining its required
   name, all original tests, stack settings, timeouts and failure propagation.
   Move exact-source/tree, before/after tracked/index/untracked cleanliness and
   toolchain receipts into that path before removing duplicate frontend full
   Build/Test work. Validate the actual PR merge result; if the frontend head
   tree differs, retain its source-specific validation until equivalence is proven.
   Keep frontend artifact verification and its original strict Clippy contract.
   Calling a reusable full-test workflow twice is still duplicate execution.
3. Add positive and negative routing/source/failure controls in
   `scripts/ci-policy.test.cjs` and the relevant frontend policy tests. Preserve
   required status names, review requirements, release coverage and protection.
   Do not claim a docs-only change has implemented this optimization.
4. Supersede obsolete PR runs only within that same PR; retain every required
   main-push CI/publication queue entry and the CI-success publication dependency.
   This admission queue is owner coordination, not a promise of GitHub Merge
   Queue or auto-merge support; verify repository capabilities before using either.

## Release ownership and frozen-release rollout

Name one release owner and record the exact candidate head, tree, scope and
validation receipts. Keep one Bamboo publisher and coordinate Bodhi/Zenith/
Homebrew consumers only after the actual published version/source is verified.
Do not create parallel publishers or expand permissions, paid capacity or
deployment authority.

The current `Promotion Source` job only accepts the repository's `dev` branch;
it does not yet accept `release/<40-character-cut-SHA>`. This docs-only change
neither implements that gate nor changes the current
[Release Process](./CONTRIBUTING.md#release-process). Land the frozen-release
workflow and its positive/negative policy tests through normal protected `dev`
review and checks before using the target release path. Reconcile the
CONTRIBUTING release instructions in that control PR, including required source
checks, the full Linux/macOS/Windows matrix and publication prerequisites.

Once that rollout is accepted, use these frozen-release rules:

1. Read the actual current protected `dev` head immediately before the cut.
   Record the UTC timestamp, full 40-character cut SHA, tree, fresh branch-read
   receipt and bounded release scope. Create `release/<cut-SHA>` at that exact
   commit. Keep `dev` open for new work; do not feed its later unrelated changes
   into the frozen release.
2. Inventory any moving `dev → main` PR, preserve its independent work and
   evidence, then close or replace it with the frozen release's PR. Only that
   recorded frozen release is the promotion candidate after rollout.
3. A bounded release repair branches from the frozen head. Preserve its original
   fix head `F` unchanged and validate that exact source with the complete release
   gates. Admit `F` to protected `dev` through normal review and required checks;
   if `dev` has advanced, use a separate integration head `U` containing both
   `F` and current `dev`. Record exact `F → U → accepted-dev` ancestry without
   squashing or rebasing away `F`. Keep `F` release-source and `U` integration
   evidence separate. After both pass and dev admission completes, fast-forward
   the release branch to original `F`, never to `U` or its dev merge result.
4. The promotion gate must prove cut-to-head and head-to-protected-dev ancestry.
   Review must also verify the fresh cut, bounded scope, original `F` and separate
   receipts: ancestry alone can accept an overbroad `U`. Pending or failed checks
   block promotion. Revalidate changed head/base; never relabel old results.
5. Promote only after current review, required checks, full platform coverage and
   normal protection pass. If a repair cannot preserve exact `F` ancestry or stay
   within scope, explicitly agree a new cut/scope rather than bypassing a gate.
   Frozen version/source tags stay immutable. Existing publication and deployment
   authorization requirements remain unchanged.
