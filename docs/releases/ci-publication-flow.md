# CI and publication flow

Use the same [delivery workflow](../delivery-workflow.md) and
[Actions audit](../ci-workflow-audit.md) regardless of which agent owns the work.
The current workflow files and branch protection are authoritative during rollout.

## Feature: test before dev

Create a feature branch and PR to `dev`. Its CI runs Unit and E2E, including the
applicable native-platform cases. Fix failures on that feature branch before
merging. Run focused local tests for changed behavior; retain the actual CI
source and results for review.

Run each selection once for the same source, platform and configuration. Reuse
compatible compiled test executables and frontend artifacts; do not add a second
workflow that runs an already-owned selection. Different platform or feature
configurations can require distinct outputs. A cache hit alone is not a test
result.

`frontend-acceptance.yml` checks the embedded frontend package, build scripts and
server resources. The relevant checks belong in feature CI; a standalone manual
run is diagnosis, not another automatic release or Main test. Lotus browser E2E
is a separate frontend test.

The shared `observer-api-lint.yml` owns the five-library API-only Clippy check.
`trusted-observer.yml` runs both Observer runtime selections in one feature job,
reusing compatible compiled output and running the shared serde case once. The
original subscription workflow remains available for manual diagnosis.

The canonical feature driver owns Planner's 12 Plan and 10 artifact library
cases. It excludes only those ordinary library cases from the parallel run and
executes each once with one test thread from the same package's already compiled
binary. Each library keeps its own default dependency graph; no second Cargo
compile is introduced. The independent Delegated Planner workflow still owns
the distinct API-only real CLI pair and its original Clippy graphs.

## Release: build the final artifacts

Create `release/<SHA>` at the accepted `dev` commit and keep that source fixed.
The release workflow reserves an unused version, stamps it into a disposable
publishing checkout, and builds each required platform binary once. Package the
locked frontend and managed crates, then publish the original assets with the
same version and tag. Do not run Unit/E2E or rebuild the frontend here.

Once the new workflow is installed, dispatch it from protected `dev` with the
frozen source and the feature PR and CI run that accepted that source:

```bash
gh workflow run publish-crate.yml -R bigduu/Bamboo-agent --ref dev \
  -f "expected_source_sha=$CUT_SHA" -f "feature_pr=$FEATURE_PR" \
  -f "accepted_ci_run=$FEATURE_CI_RUN"
```

The controller checks `release/<cut-sha>` and builds that source, even if `dev`
has advanced. It does not build or test the controller revision. The allocator
chooses the release version; use the resulting tag and original artifacts for
promotion.

Use the existing release allocator, credentials and recovery mechanisms. Keep
publishing credentials out of test and build children that do not need them.
If infrastructure fails, retain successful outputs and retry only the missing
work with the same source/version. If a crate build may have started but its
original archive and signed checksum cannot be confirmed, stop for recovery;
do not rebuild it. A product fix goes through a feature branch and a new `dev`
acceptance before a new release cut.

## Main and consumers

Promote the release only after its required artifacts are published and checked.
Main stores the stable source and version/tag references without another product
build, Unit/E2E run or publication. Documentation and Docker consumers use the
original release or feature artifacts where applicable; they must not rebuild a
successful output simply to consume it.

After publication completes, Docker consumes the original Linux x64 binary.
Linux ARM64 remains a separate image architecture and is built once from the
same frozen source. Retried image jobs reuse their verified candidate digests.

## Rollout and historical releases

Land the feature and release workflow changes through the normal reviewed PR
flow. During the first cutover, Main may still contain the legacy Docker workflow:
GitHub selects a `workflow_run` listener from the default branch, so the new
listener cannot run until promotion. Once the first release is verified, invoke
the new Docker consumer once from protected `dev`, using that release's version
and source, then promote the frozen source to Main:

```bash
gh workflow run docker-publish.yml -R bigduu/Bamboo-agent --ref dev \
  -f "version=$RELEASE_VERSION" -f "source=$CUT_SHA"
```

After promotion, the Main listener starts this consumer automatically for later
completed publishers. Do not assume a staged branch has changed Main's triggers.

Keep old release tags, source identities, signed receipts and recovery inputs.
Their original instructions remain in the [historical guide](https://github.com/bigduu/Bamboo-agent/blob/ce89b4fcf4a45014cccb3bc7f817613ddcfa7eb2/docs/releases/ci-publication-flow.md).

### Recorded baseline

For `v2026.10.15`, source
`d4e79947805957418b0ed18d4b05ac7bcbb1dc2d`, the October 21, 2026 run spent about
75 minutes in [post-merge main CI](https://github.com/bigduu/Bamboo-agent/actions/runs/39602293449).
The [publisher](https://github.com/bigduu/Bamboo-agent/actions/runs/39613148725)
then spent about 22 minutes on the crate stage, followed by native builds whose
slowest job took about 42 minutes, and about 2 minutes on final asset publication.
From the main merge to complete assets, elapsed time was about 2 hours 24 minutes,
excluding pre-merge acceptance.

That historical source required all four v1 targets; its longest native job was
macOS Intel. New v2 releases have three targets. Compare each source contract
separately and retain exact source/run/attempt/platform details when measuring
the new workflow. These timings explain the optimization; they do not claim a
measured speedup for an unpublished candidate.

Parallel execution changes the post-preparation critical path from approximately
`crate duration + slowest native duration` to their maximum, plus preparation,
runner waiting and finalization. Measure the real run; this is the scheduling
model, not a promised duration or percentage improvement.
