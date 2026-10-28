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

### Fixed v2026.10.19 crate recovery

Issue #2371 handles only publisher [40613036533/attempt 1](https://github.com/bigduu/Bamboo-agent/actions/runs/40613036533), frozen source
`4786b7db862cabbebce49e288da62b284ccf5b72`, version `2026.10.19` and draft
Release `426156779`. The server upload failed with an indeterminate transport or
response error after its archive and signed checksum were retained. Preserve all
27 crate originals/checksums, the sealed `@bigduu/lotus-next@2026.10.24` frontend
and all three successful native attempt-1 artifacts. A missing registry response
does not authorize repackaging an existing original.

After normal protected-Dev review and CI admission, select the exact reviewed
controller commit and use the single-input recovery workflow:

```bash
gh workflow run recover-frozen-crate-release.yml -R bigduu/Bamboo-agent --ref dev \
  -f "expected_controller_sha=$REVIEWED_CONTROLLER_SHA"
```

The workflow checks out controller and product separately, preserving its real
runner/run/attempt identity. Read-only authorization rechecks the original source,
failure, reservation, registry and artifacts. Authenticated restoration writes
only the original sealed frontend locally; it does not download npm or repack it.
The existing Cargo sandbox then owns metadata and verification compilation after
temporary version stamping and the original external-lock check. Release and
registry write credentials are mapped only to the final publish step.

The controller reuses the retained server archive, or verifies its existing
registry version. Only `bamboo-client-core`, `bamboo-tui` and `bamboo-agent` may be
compiled for the first time, after proving they never began in the original run.
Each verified original is persisted before its signed checksum and registry
upload. Unknown, changed, pending or missing prior originals stop recovery.

Success requires authenticated remote readback of `recovery2371.completed=true`
with no pending write, while the Release remains draft, `receipt.completed=false`
and `publisherRunId=40613036533`. This workflow does not allocate a version,
retag, publish native assets or finalize the Release. Once that readback passes,
retry the original publisher's failed crate job and its dependent finalizer;
they reuse the three successful native attempt-1 outputs. Do not rerun all jobs
or treat a local/policy check as completed production recovery.

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
flow. GitHub selects a `workflow_run` listener from the default branch. At the
time of this guide, the default is protected `dev`, so merging the listener into
`dev` enables automatic Docker consumption even before Main is promoted. After
the first publisher succeeds, check its corresponding Docker run before doing
anything else. Do not manually dispatch a duplicate consumer. If a consumer run
failed, recover its failed jobs in the same run and reuse its verified outputs.

If the expected automatic run is confirmed missing, recheck the live default
branch and installed listener. Only when no consumer run for this release exists,
use the existing recovery flow for its exact version and source:

```bash
gh workflow run docker-publish.yml -R bigduu/Bamboo-agent --ref dev \
  -f "version=$RELEASE_VERSION" -f "source=$CUT_SHA"
```

Recheck the live default branch, installed listener and actual runs before
recovery; a staged branch alone does not activate a listener.

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
