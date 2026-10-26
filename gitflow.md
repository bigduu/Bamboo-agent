# Parallel Development and Release Flow

Use `feature/fix → integration/<batch> → dev` for related work; independent,
bounded PRs may target `dev` directly. An integration batch is finite delivery
scope, not another long-lived development branch. Dev requires successful CI
compilation plus passing executor-local tests with coverage for changed behavior. Validate a recorded batch once on a frozen
`release/<40-character-cut-SHA>` before promotion to `main`.

The [Release Process](./CONTRIBUTING.md#release-process) is authoritative for cuts,
release repairs and publication. The [review rules](./AGENTS.md#pull-request-review)
and actual branch protection remain mandatory. This policy accepts that `dev`
can be temporarily unstable; it does not relax release or security requirements.
Use the [CI and publication flow](./docs/releases/ci-publication-flow.md) for
phase ownership, evidence reuse, activation and recovery.

## Ownership and parallel work

- Give each bounded Issue one owner, unique branch and isolated worktree. Record
  the exact starting commit, affected paths, dependencies and acceptance criteria.
  Declare dependent PRs and coordinate overlapping paths before implementation.
- Give each integration batch one named integrator and branch pinned initially
  to a recorded `dev` commit. Related feature PRs target that branch. Only the
  integrator writes it and resolves cross-feature conflicts; others contribute
  through their own branches. Explicitly hand off ownership before replacing it.
- Workers run focused checks for changed behavior and dependencies. The integrator
  reviews the combined diff and admits only the agreed scope to protected `dev`
  through a normal PR or the verified direct-push route below. Do not present component results as batch acceptance.
- Before dev admission, each owner runs the affected tests locally in their
  isolated executor and adds or updates regression coverage for changed behavior.
  Include relevant normal and failure cases. Record the tested source SHA/tree,
  exact commands, test selection, results and coverage in the PR. Missing or
  failing relevant local tests block merge; a successful compile is not a
  substitute. Disclose unrun checks and the reason rather than claiming a pass.
- PRs targeting dev must pass required `Test`, which aggregates disjoint Unit
  and E2E runtime tests plus the live SSH fixture. One Linux executor preserves
  independent default package contexts and explicitly catalogued feature
  additions in one target directory. Ordinary library/binary, integration and
  documentation cases run once; SSH reuses its compiled executable.
  Applicable native-platform workflows also run before
  admission. Fix failures in the feature branch before merging into dev.
  `Quick Checks` remains auxiliary. Do not assume automatic coverage exists for
  arbitrary integration PR targets.
- Coordinate conflicting admissions and branch updates, not long-running heavy
  validation. Update a candidate only when actual review/protection or a real
  conflict requires it. Do not repeatedly refresh unrelated branches merely to
  rerun the same full suite. Never bypass an actual required check.
- The owner of a failing change investigates and repairs it or submits a bounded
  revert through normal dev gates. Preserve the failing evidence and identify
  affected dependents. Do not ignore a known defect relevant to the change or
  turn a failed optional full snapshot run into a green result.

## Normal direct pushes to dev

Once the actual dev branch rule permits it, a normal fast-forward direct push is
allowed. It still requires successful Unit/E2E execution, local affected tests with
coverage evidence, applicable review, and every actual required check. Force
pushes and protection overrides remain forbidden; main's rules are unchanged.

The current dev Unit/E2E `Test` workflow runs for PRs targeting dev, not
arbitrary branch pushes. A feature branch can obtain checks through the PR path before
a permitted fast-forward admission. Verify that the intended target commit has
the applicable successful checks and that tested source/tree and build inputs
match. A synthetic-merge result is not automatically proof for another checkout.
If equivalence or required status applicability is unproven, keep the normal PR
route rather than push unchecked code. The local test/coverage obligation also
applies to direct pushes; removing a PR requirement does not remove those gates.

## Temporary legacy frozen batch acceptance

This section applies only to the retained release route until release-source
admission is accepted. The new target contract is defined in Gate roles below.

After the intended changes enter `dev`, the release owner records one fresh dev
cut and finite scope, then freezes a release candidate for full acceptance.
Ongoing dev work can continue without invalidating that frozen candidate.
Manual `CI` and frontend acceptance require `snapshot_sha` to equal the dispatch
source's full lowercase 40-character SHA; a mismatch fails closed. See the
[fixed-snapshot entrypoint](./CONTRIBUTING.md#validate-a-fixed-snapshot).

Run the complete release suite and native platform-test matrix through the fixed
candidate's required main promotion checks, recording the actual tested merge SHA/tree.
Release CI preserves the native tests but does not build optimized release
executables. The automatic main publisher builds each required executable once
from the admitted main commit and reserved version/frontend identity.
Preserve the separate frontend-head contract; reuse evidence only when all source
and build/test inputs are equivalent. Do not add a second full run merely because
an entrypoint has a different name. Only this passing, reviewed candidate may
promote to `main`. Dev compilation and focused local tests cannot replace
release acceptance. A failed or incomplete release check blocks promotion.

Keep necessary repairs within the recorded scope. Record original fix `F`'s
exact SHA/tree, affected local tests and independent Code and Security review.
Preserve `F` through normal protected-dev integration `U` when needed, then
fast-forward the release to `F` and complete the final release PR's full CI.
An extra manual full CI on `F` is optional diagnosis, not a second mandatory
full run. Follow the
[release repair rules](./CONTRIBUTING.md#freeze-the-release-scope). Do not chase
unrelated dev changes or substitute the broader `U` for the frozen release fix.

## Evidence and resource discipline

- Record each check's SHA/tree, checkout or synthetic merge, toolchain,
  lock/features, profile/flags, platform/environment, frontend artifact, command
  and result link. Distinguish not run, running, passed, failed, cancelled and
  inconclusive. Compilation is not runtime acceptance.
- Before an expensive build, inspect existing receipts. Same-tree reuse requires
  all relevant build/test inputs and contracts to match, including commit-derived
  metadata. Revalidate affected changes. Never relabel another head's result or
  synthesize a successful GitHub status; required checks belong to their actual
  candidate under current protection rules.
- Assign one writer per worktree, build target and test-output directory. Reuse
  compatible caches only after the prior writer exits. Bound heavy concurrency
  by measured memory, disk and workload cost; queue competing cold builds on the
  same host instead of inventing thread counts or racing shared output.
- Only the run's owner or explicitly designated integrator may cancel a
  superseded run. Preserve its result and reason. Never cancel another active
  batch/release, retry a real failure merely to get green, kill unrelated
  processes or clean another worker's worktree/cache. Preserve uncertain work.

## Close a batch once

Inventory component PRs and verify their exact ancestors or changed postimages
against the admitted candidate. Close superseded components with the accepted
commit and equivalence evidence; do not separately merge identical slices into
`dev`. Keep missing work open. Preserve discussions, source references and
validation receipts; cleanup is a separate owned action.

## Gate roles and activation

The required `Test` gate aggregates Unit, E2E and SSH results on the actual
feature PR merge source. A missing, skipped, cancelled or failed selected job
fails admission. No standalone dev-profile workspace build precedes these tests. One physical
Linux executor preserves isolated default configurations and narrow feature
additions, reusing compatible compiled artifacts. Unit/E2E remain logical target
groups. Reuse the compiled ignored SSH executable
in that same checkout. Keep native-specific acceptance before dev admission.

The target release path freezes the exact accepted dev cut, reserves one formal
version with the existing allocator and builds/publishes each product once.
Release branches do not rerun Unit/E2E or carry product repairs. Main promotes
the exact published source and records immutable tag/manifest/provenance and
workflow receipts in the promotion description; it does not rebuild or retest.

Activation is staged. #2311 changes feature admission; the existing release
suite and main-triggered publisher remain the live route until separate
release-source admission is accepted. Preserve their required checks, queues,
source proofs and historical recovery in the meantime. Do not dispatch an
unimplemented route. Before the first new release, integrate current main
ancestry into dev through an ordinary feature PR with Unit/E2E. A release fix
returns through feature/dev and starts a new cut.

Cover successful and failed feature routing, complete/disjoint target catalogs
and aggregate failure propagation in policy tests. Never synthesize statuses,
skip a required gate, force a branch or treat different source trees as the same
tested product. Supersede iterative runs only within the same PR.

Under the retained legacy release process, manual full CI on `F` is optional diagnosis,
not a second mandatory full run. Keep that historical evidence separate from
the feature-only test ownership and pending release-source cutover.
