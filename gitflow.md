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
- PRs targeting dev must pass the real compile-only `Test` gate and applicable
  review. `Quick Checks` remains auxiliary. Dev CI does not execute the full test
  suite for each PR; complete validation belongs to the frozen batch. Workers
  may run additional scoped checks in parallel. Do not assume automatic GitHub
  coverage exists for arbitrary integration PR targets.
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
allowed. It still requires successful compilation, local affected tests with
coverage evidence, applicable review, and every actual required check. Force
pushes and protection overrides remain forbidden; main's rules are unchanged.

The current dev compile-only `Test` workflow runs for PRs targeting dev, not
arbitrary branch pushes. A feature branch can obtain checks through the PR path before
a permitted fast-forward admission. Verify that the intended target commit has
the applicable successful checks and that tested source/tree and build inputs
match. A synthetic-merge result is not automatically proof for another checkout.
If equivalence or required status applicability is unproven, keep the normal PR
route rather than push unchecked code. The local test/coverage obligation also
applies to direct pushes; removing a PR requirement does not remove those gates.

## Frozen batch acceptance

After the intended changes enter `dev`, the release owner records one fresh dev
cut and finite scope, then freezes a release candidate for full acceptance.
Ongoing dev work can continue without invalidating that frozen candidate.
Manual `CI` and frontend acceptance require `snapshot_sha` to equal the dispatch
source's full lowercase 40-character SHA; a mismatch fails closed. See the
[fixed-snapshot entrypoint](./CONTRIBUTING.md#validate-a-fixed-snapshot).

Run the complete release suite and platform matrix through the fixed candidate's
required main promotion checks, recording the actual tested merge SHA/tree.
Preserve the separate frontend-head contract; reuse evidence only when all source
and build/test inputs are equivalent. Do not add a second full run merely because
an entrypoint has a different name. Only this passing, reviewed candidate may
promote to `main`. Dev compilation and focused local tests cannot replace
release acceptance. A failed or incomplete release check blocks promotion.

Keep necessary repairs within the recorded scope. Preserve original fix `F`,
admit it to dev through integration `U` when needed, and separately validate the
release source as specified in the
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

The required `Test` gate runs `cargo build --locked` in dev compile-only mode
on the actual PR merge source and propagates compilation or source-verification
failures. A green dev `Test` reports compilation, not a full test-suite pass.
`Quick Checks` runs policy checks and formatting as an auxiliary check; it is
not a replacement for compilation. Local affected tests and regression coverage
remain a separate pre-merge obligation, documented with source-bound evidence.

Ordinary dev PRs do not run the complete CI test suite. Request full validation
on a fixed snapshot when useful. Keep its actual outcomes visible; do not
suppress failures, remove tests or make a skipped full test look like success.
Applicable separate path-scoped workflows retain their own triggers.

Frozen release/main validation retains every required check name, original test,
stack setting, timeout, failure propagation, frontend artifact policy and strict
Clippy contract. Keep exact-source/tree, toolchain and before/after source
cleanliness evidence. Consolidating equivalent heavy jobs must preserve these
contracts and must not run the same reusable full suite twice.

Keep one required check named `Test`: dev uses its compile-only mode and
release/main/manual snapshots use its full mode. Do not skip the dev check or
add another same-named skipped job. This preserves the existing required-check
configuration without a protection change. Verify the actual source and mode;
never fabricate statuses, weaken main protection, or use merge overrides.

Cover positive and negative compile-only dev/full-release routing and failure behavior
in CI policy tests. Preserve all required main-push CI/publication queue entries
and the successful-CI publication dependency. Supersede obsolete runs only within
the same PR. This coordination policy does not assume GitHub Merge Queue or
other repository capabilities that have not been verified.

## Release ownership

Name one release owner to cut, validate and promote each frozen candidate.
Inventory any moving `dev → main` PR, preserve its independent work and evidence,
then close or replace it with the recorded frozen release's PR.

Keep one Bamboo publisher. Coordinate Bodhi/Zenith/Homebrew consumption after
the actual published version/source is verified. Frozen version/source tags
stay immutable. Do not create parallel publishers or expand credentials,
permissions, paid capacity or production-deployment authority.
