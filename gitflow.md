# Parallel Development and Release Flow

All agents follow the [shared delivery workflow](./docs/delivery-workflow.md)
and [Bamboo CI guide](./docs/releases/ci-publication-flow.md). The shared audit
includes historical snapshots; the local guide describes this repository's path.

## Feature and dev

- Use an isolated worktree and a feature branch for each bounded change. Agree
  on file ownership when agents work in parallel.
- Run and fix Unit/E2E on the feature branch before its normal PR to `dev`.
  Preserve the relevant platform and failure cases, and record actual results.
- Give each test selection one automatic owner. Reuse compatible outputs rather
  than running the same suite or compiling the same output again.
- Review the current head/base independently and satisfy the actual branch
  protection before merging. Do not replace a failed or skipped check with a
  successful result from another source.

## Release

Cut `release/<SHA>` from the accepted `dev` commit. Reserve one version, build
and publish each required platform artifact once, and tag that exact source with
the same version. Do not run another Unit/E2E batch on the release branch.

A product failure goes back to a feature branch and a new `dev` acceptance; cut a
new release afterwards. For an infrastructure retry, retain the successful
original artifacts and retry only the failed work with the same inputs.

## Main

After the release succeeds, merge its stable source into `main` and record the
version, tag and artifact references. Main does not repeat tests, compilation or
publication. Review the promotion and retain the actual release evidence.

## Parallel work and historical releases

Coordinate overlapping files and shared output directories. Do not cancel or
clean another agent's work. Continue from the same branch and evidence when
handing a task to another agent.

Historical releases keep their original tags and recovery procedures. During
rollout, check the installed workflow before using the new release path. The
previous release instructions remain in the [historical source](https://github.com/bigduu/Bamboo-agent/blob/ce89b4fcf4a45014cccb3bc7f817613ddcfa7eb2/gitflow.md).
