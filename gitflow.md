# Parallel Development and Release Flow

Read the complete local [delivery workflow](./docs/delivery-workflow.md),
[Actions audit](./docs/ci-workflow-audit.md), [Release Process](./CONTRIBUTING.md#release-process)
and [Bamboo phase guide](./docs/releases/ci-publication-flow.md). Every agent uses
that same contract, the owning Issue and the exact source/evidence checkpoint.

The train is `feature → dev → release/<40-character-cut-SHA> → main`.
Feature branches own Unit/E2E. Fix relevant failures before normal protected-dev
admission; release is not an integration test-repair batch.
Release branches do not rerun Unit/E2E or carry product repairs. Main promotion is read-only.

## Ownership and parallel work

- Give each bounded acceptance slice one owner, branch and isolated worktree.
  Record its starting source, affected paths, dependencies and acceptance criteria.
  Serialize overlapping edits and shared persistence/authority boundaries, not an
  entire module merely because another independent owner is active.
- Related work may use a finite `integration/<batch>` with one named integrator.
  Workers submit focused PRs and affected tests; the integrator reviews the
  combined source and admits it through the normal Dev PR. This is not another
  long-lived branch or permission to defer tests to release.
- Run the smallest meaningful affected checks while iterating. Record SHA/tree,
  commands, selection, results and changed-behavior coverage before Dev admission.
  Missing or failing relevant checks block merge; compilation is not runtime proof.
- Feature PRs into Dev run selected Unit/E2E on their actual clean merge `M`.
  Preserve independent default package contexts, narrow additive selections and
  native-platform inputs. Each covered selection has one owner; reuse compatible
  compiled executables rather than copying suites into another workflow.
- The failing feature's owner investigates and repairs it or submits a bounded
  revert through normal Dev gates. Preserve the failure and inform affected
  dependents. Never skip the relevant assertion or move its repair to release.

## Protected Dev admission

Recheck the live exact PR head/base, required checks, reviews/threads, protection,
mergeability and tested/accepted trees immediately before a normal merge. A new
release source requires the confirmed accepted PR and its original pre-merge
feature evidence. The old direct-push discussion cannot supply that record.
Do not force a branch, bypass a real required check or synthesize a green status.

Keep feature head `F`, tested merge `M`, accepted Dev commit `S`, publisher
controller `C` and final Main merge distinct. A matching tree or a copied logical
success does not rename an original physical execution. Preserve job/attempt
and complete source/configuration binding when reusing successful evidence.

## Immutable release source

Select the accepted Dev cut and freshly record its exact SHA/tree, branch-read
receipt, UTC cut time, finite scope and locked artifact inputs. Freeze
`release/<S>` at that recorded commit. Dev may continue normally afterwards.

Keep the frozen source immutable. A product defect, changed upstream identity
or required source repair returns through feature/dev acceptance and gets a new
cut. Do not fast-forward a frozen release to a repair head or a broader
integration result. Pending producer/consumer gates must pass before cutting the
source that will consume them; they do not authorize a release repair exception.

Release reserves its final version before producing each formal output once for
its exact source/version/target/profile/toolchain/locked-input identity. Feature
test binaries and stamped release executables are distinct required outputs.
The release phase verifies and publishes artifacts; it adds no Unit/E2E batch.
Keep source versions at `0.0.0` and stamp disposable publishing checkouts only.

## Original outputs and Main

Retain successful original bytes and source/run/job/physical-attempt receipts
before finalization. Retry only a classified failed infrastructure target with
the same inputs; reuse successful siblings. A missing recorded-success original
is a stop for explicit recovery, not permission to compile or package it again.
A source fix requires a newly accepted cut.

A completed publication supplies the stable source, tag/version, manifest and
artifact identities for Main promotion. The trusted-base reader verifies actual
Git postimages and original publication evidence without executing candidate
product code. Main push/promotion performs no product test, compilation,
packaging or republishing. Main keeps the stable source and immutable references;
there is no post-build JSON source commit. Consumers authenticate original outputs
instead of rebuilding them.

## Evidence and resource discipline

- Record actual SHA/tree, checkout or merge identity, toolchain, locks/features,
  target, profile, frontend artifact, command and result. Keep running, passed,
  failed, cancelled, skipped, unrun and inconclusive outcomes distinct.
- Source-tree equality or a cache hit alone is not build/runtime/artifact reuse.
  Every relevant input and original byte identity must match. New changes need
  affected evidence; a different entrypoint name is not a second test owner.
- Assign one writer per worktree and output/target directory. Bound concurrency
  by actual resources, preserve other owners' caches/receipts and do not clean or
  kill unrelated work. Only the run's owner or designated integrator may cancel
  its superseded run, preserving the outcome and reason.
- Continue from the existing Issue/PR and frozen checkpoint. Close an admitted
  slice once with its actual evidence; do not merge equivalent components again.

## Gate roles and activation

The replacement entrypoints are **staged**, not established by local fixtures.
At the #2363 checkpoint the default remains `dev` and stable Main remains
`d6325a732c7669f94962f604e4ce98966e374d73`, with the legacy route still awaiting
cutover. Complete combined-source feature acceptance, producer registration,
trusted control installation and Main protection/default changes separately.
The first new Source publication and first positive Main promotion have not
been certified. Verify [Zenith #399](https://github.com/bigduu/Zenith/issues/399),
the phase guide and live remote state before operating any path. Preserve real
required gates and active historical recoveries; this policy is not a waiver or
a production dispatch instruction.

The prepared feature entrypoint owns runtime, quality and Docs; manual CI is a
same-SHA feature/dev diagnostic and rejects Main, release refs and tags. The
prepared Source dispatcher owns final artifacts, while Main and distribution
consumers read authenticated originals. Their empty registries and pending
acceptance remain blockers, not reasons to invent an alternative publisher.

The old frozen-release `F`/`U` repair procedure is historical only. Its genuine
[ce89 policy snapshot](https://github.com/bigduu/Bamboo-agent/blob/ce89b4fcf4a45014cccb3bc7f817613ddcfa7eb2/gitflow.md)
and hash-bound v8 test data preserve the original words and recovery evidence;
they cannot authorize a new repair, test run or publication on a frozen release.
