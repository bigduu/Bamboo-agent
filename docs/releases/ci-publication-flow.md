# CI and publication flow

Use the same [delivery workflow](../delivery-workflow.md) and
[Actions audit](../ci-workflow-audit.md) regardless of which agent owns the work.
The current workflow files and branch protection are authoritative during rollout.

## Feature: test before dev

Create a feature branch and PR to `dev`. Code CI runs Linux Unit and E2E in
independently rerunnable test shards; native-platform cases are available by
manual dispatch. Root and docs Markdown-only changes run Quick Checks. Fix failures on that feature branch before
merging. Run focused local tests for changed behavior; retain the actual CI
source and results for review.

Run each selection once for the same source, platform and configuration. Reuse
compatible compiled test executables and frontend artifacts; do not add a second
workflow that runs an already-owned selection. Different platform or feature
configurations can require distinct outputs. A cache hit alone is not a test
result.

The four Linux `agent-tests-*` jobs run Bamboo Agent integration targets.
`agent-tests-0` retains both default Ticket runtime targets and their additive
feature context. The slower Actor and Broker targets have explicit owners in
`agent-tests-1` through `agent-tests-3`. Other integration targets are assigned
by name hash, and the routing test checks complete, disjoint ownership.

The Linux `packages-1` shard owns the default LiteLLM wire-types suite. Its
additive `schema` context runs the three schema-only cases with the same target
directory and checks that the test names match the compiled catalog.

Ubuntu Quick Checks run the complete Supervisor, Lotus artifact and release
frontend policy Node suites once. Manual Windows and macOS diagnostics run nine system cases:
bounded file reading, canonical artifact paths, four process/listener binding
cases, actual archive staging/preservation and both LF/CRLF Git/Bash guards.
macOS checks real process ownership with `ps/lsof`; Windows checks the explicit
unsupported-process-binding refusal instead of claiming native Host acceptance.
The native runner selects their exact names with Node's built-in test runner and
requires all nine successful executions with no failed, cancelled, skipped or
todo cases. A renamed, missing or duplicated case fails the job. Platform-free
protocol, manifest/hash and release-selection policies stay in Ubuntu Quick;
the existing native Rust acceptance still runs separately.

`frontend-acceptance.yml` checks the embedded frontend package, build scripts and
server resources. The relevant checks belong in feature CI; a standalone manual
run is diagnosis, not another automatic release or Main test. Lotus browser E2E
is a separate frontend test.

The shared `observer-api-lint.yml` owns the five-library API-only Clippy check.
`trusted-observer.yml` runs both Observer runtime selections in one feature job,
reusing compatible compiled output and running the shared serde case once. The
original subscription workflow remains available for manual diagnosis.

The canonical feature driver runs the nine retained Planner artifact and
recovery cases serially from its compiled default engine test binary. Retained
Planner Analysis runs the two original API-only Clippy configurations. The
CLI cases that invoked the retired `Plan` tool remain historical evidence.

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

The frozen publisher fills the GitHub Release body with change subjects and
linked commit SHAs. It compares the accepted source with the previous published
stable ancestor and links the full Git history. The existing signed provenance
block continues to supply the machine-readable receipt.

Use the existing release allocator, credentials and recovery mechanisms. Keep
publishing credentials out of test and build children that do not need them.
The frozen-source guard retries temporary GitHub metadata read failures using
fresh GET requests. Each read permits three retries after 1, 2, and 4 seconds.
Access errors and failed source checks still stop publication, and writes
remain outside the retry.
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
runner/run/attempt identity. Authorization uses the read-only `github.token`.
Restoration uses the existing release token to read the private draft because
draft visibility with `contents: read` is not established. It authenticates the
original state and writes only the sealed frontend locally, with no external
write, npm download or repack.
The existing Cargo sandbox then owns metadata and verification compilation after
temporary version stamping and the original external-lock check. Only the final
publish step executes external writes and receives the Cargo registry token.

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

### Fixed v2026.11.22 crate recovery

Issue #2762 binds draft 446034086 and source
`13336567999504a8c7deb89fba94f7907b04c3ee` to [publisher 43712198357](https://github.com/bigduu/Bamboo-agent/actions/runs/43712198357).
Attempt 1 received HTTP 503 during the readback of the eighth crate,
`bamboo-infrastructure`, after retaining its original archive and checksum.
The fixed controller verifies the failed job log, eight published checksums,
the sealed frontend and all five native transports from attempt 1. It
applies bounded backoff to transient registry reads. The signed checkpoint
tracks first verification and archive retention for the remaining 22 crates.

Dispatch this controller after its reviewed PR merges into protected Dev:

```bash
gh workflow run recover-crate-2026-11-22.yml -R bigduu/Bamboo-agent --ref dev \
  -f "expected_controller_sha=$REVIEWED_CONTROLLER_SHA"
```

The recovery succeeds when authenticated readback shows
`recovery20261122.completed=true`, no pending compilation, `receipt.completed=false`,
the original draft and `publisherRunId=43712198357`. Retry only the failed jobs
of that publisher after the recovery completes. Its finalizer uses the
attempt-1 native outputs and publishes the v3 manifest for the same tag and
source. The procedure requires the original retained transports and their
unexpired identities.

### Fixed v2026.11.31 crate recovery

Issue #2840 binds draft 450766353 and source
`1387680f8ce96d808ba48b7b570fba5dded4e021` to [publisher 44437436231](https://github.com/bigduu/Bamboo-agent/actions/runs/44437436231).
The attempt-1 registry read terminated with an incomplete HTTP response body.
The original publisher retains the crate archive and signs its checksum before
it reads the registry again. The signed prefix ends at `bamboo-sdk`. The fixed
controller binds that failure, the 26 original crate checksums, the sealed
frontend and five original native transports. It retries bounded registry reads
through full body consumption and verifies the exact Cargo download path.
The four crates starting with `bamboo-server` use the existing signed
first-compilation checkpoint.

Dispatch after the reviewed controller merges into protected Dev:

```bash
gh workflow run recover-crate-2026-11-31.yml -R bigduu/Bamboo-agent --ref dev \
  -f "expected_controller_sha=$REVIEWED_CONTROLLER_SHA"
```

Verify signed readback of `recovery20261131.completed=true` with no pending
compilation, `receipt.completed=false` and `publisherRunId=44437436231`. Retry
only the original run's failed jobs. The original pipeline publishes its
retained v3 executables with the existing draft, source and tag.

### Fixed v2026.11.43 crate recovery

Issue #3053 binds draft 457147104 and source
`d9a04783cc9eff82e406a78e291136537eff10c6` to [publisher 45451853467](https://github.com/bigduu/Bamboo-agent/actions/runs/45451853467).
The first 24 crates were verified in the registry. The publisher retained and
recorded the `bamboo-projects` archive before its upload returned HTTP 500.
Recovery checks the registry before uploading that original again. It preserves
the signed crate prefix, sealed frontend and all five original native transports.
The six crates from `bamboo-server-tools` through `bamboo-agent` use the existing
signed first-compilation checkpoint.

Dispatch after the reviewed controller merges into protected Dev.

```bash
gh workflow run recover-crate-2026-11-43.yml -R bigduu/Bamboo-agent --ref dev \
  -f "expected_controller_sha=$REVIEWED_CONTROLLER_SHA"
```

Require authenticated remote readback with `recovery20261143.completed=true`,
no pending compilation, `receipt.completed=false` and
`publisherRunId=45451853467`. Retry the original failed jobs to finalize the
same version with the retained native outputs.

## Main and consumers

### Desktop browser runtime

The desktop runtime consumer listens for the successful original `Publish Crate` workflow on protected Dev. A frozen v3 source with `browser-runtime/desktop.lock.json` opts into native desktop runtime packaging. The consumer authenticates the original executable manifest, packs the runtime for each of the five targets, and retains each archive before publication. The protected `bamboo-release` job verifies the original build outputs and uploads `bamboo-desktop-browser-runtimes-v1.json` after all five archives have been verified at the exact Bamboo release tag. The manifest binds the source and executable manifest SHA-256.

Recover a publication failure by retrying the failed jobs of the same browser runtime workflow. Archives already verified by a successful pack step must be recovered from the original transport. This workflow uses the published executable release as its input and leaves its original asset digests and receipts intact.

The frozen source selects the native executable contract. The five-target v3
contract requires Linux ARM64 and Windows ARM64 feature evidence through the
existing feature aggregate and requires both binaries in the completed Release.
Verify each architecture and the original output identity. The v2 contract
keeps its existing three targets. See [shared executable assets](shared-executable-assets.md)
for the complete target and recovery rules.

Promote the release only after its required artifacts are published and checked.
Main stores the stable source and version/tag references without another product
build, Unit/E2E run or publication. Documentation and Docker consumers use the
original release or feature artifacts where applicable; they must not rebuild a
successful output simply to consume it.

Docker uses the executable contract recorded in the frozen source. For v3 it
binds the downloaded manifest hash to the original successful verification step
of the exact protected Dev Publish Crate run and attempt. Both Linux images
package their exact original release executables in the Bookworm runtime. The
consumer verifies both binary hashes, runs their version and loader checks,
and starts each image to check health
and the embedded frontend before accepting its platform digest. The versioned
index binds the verified digest for each architecture and the scan uses the
verified index digest. Retried image jobs verify and reuse the original candidate
by digest, including any digest already retained by that run. Historical v2 releases
supply the original Linux x64 executable and keep their frozen ARM64 image
build path. Run `node --test scripts/docker-publish.test.cjs` for the focused
release-selection and Docker packaging contracts.

## npm CLI package

[Publish Bamboo CLI to npm](../../.github/workflows/publish-npm.yml) runs by manual
dispatch on protected Dev. Supply a completed v3 release version and frozen
source, plus the original successful Publish Crate run and attempt.
The preparation job checks the publisher's authenticated manifest and the
asset digests for all five executables. It packages the launcher, the exact
manifest, and the verified Browser Host. On first invocation the launcher
downloads and verifies the executable selected for the client's platform.

The `verify` operation exercises the live Release proof and package file
selection. The `stage` and `publish` operations retain one npm tarball from
the preparation job. Both upload jobs verify its SHA-256 before sending it to
npm. The publish job checks that the exact public version reports the
prepared archive's integrity. If a registry request fails with an uncertain
outcome, inspect the public version or the npm staging area before retrying
only the failed job with the retained archive.

### Authorize the first npm version

1. In [npm Access Tokens](https://www.npmjs.com/settings/~/tokens), sign in as
   an account with publish access to `@bigduu`. Generate a granular access
   token limited to that package scope with **Read and write (stage only)**.
   [npm token instructions](https://docs.npmjs.com/creating-and-viewing-access-tokens/)
2. Add the token as the `BAMBOO_NPM_STAGE_TOKEN` secret in Bamboo-agent's
   [`bamboo-release` environment](https://github.com/bigduu/Bamboo-agent/settings/environments).
3. Run the workflow with `operation=stage`. npm creates a public
   `0.0.0-stage` placeholder for a new package while the real version is staged.
   In the npm website's **Staged Packages** tab, review
   `@bigduu/bamboo-agent` at the exact version and approve it with 2FA.
   [npm staged publishing](https://docs.npmjs.com/staged-publishing/)

Run the following command to stage Bamboo v2026.11.22.

~~~bash
gh workflow run publish-npm.yml -R bigduu/Bamboo-agent --ref dev \
  -f operation=stage -f version=2026.11.22 \
  -f source=13336567999504a8c7deb89fba94f7907b04c3ee \
  -f publisher_run_id=43712198357 -f publisher_attempt=2
~~~

### Authorize subsequent npm versions

On npmjs.com, open `@bigduu/bamboo-agent` → **Settings** → **Trusted Publishing**
and choose **GitHub Actions**. Enter owner `bigduu`, repository `Bamboo-agent`,
workflow filename `publish-npm.yml`, and environment `bamboo-release`. Enable
**Allow npm publish**. Dispatch `operation=publish` against each subsequent
completed v3 release. npm supplies the workflow with short-lived OIDC
credentials and a provenance attestation.
[npm Trusted Publishing](https://docs.npmjs.com/trusted-publishers/)

## Rollout and historical releases

Land the feature and release workflow changes through the normal reviewed PR
flow. GitHub selects a `workflow_run` listener from the default branch. At the
time of this guide, the default is protected `dev`, so merging the listener into
`dev` enables automatic Docker consumption even before Main is promoted. After
the first publisher succeeds, check its corresponding Docker run before doing
anything else. Do not manually dispatch a duplicate consumer. If a consumer run
failed, recover its failed jobs in the same run and reuse its verified outputs
when the installed controller can safely do so.

Automatic `workflow_run` events use read-only GitHub Actions cache access: they
restore cached layers but do not export them. A protected Dev manual dispatch may
export an optional cache; an export failure does not bypass image verification.
If an older automatic controller failed because it attempted a cache export,
wait for all its jobs to finish, preserve the failure and inspect each target's
candidate and final image. After the reviewed controller fix reaches Dev, an
exact-version/source manual recovery may reuse verified candidates and build
only a failed target whose candidate is confirmed absent. Unknown registry state
or a missing candidate for an already completed image stops recovery.

If the expected automatic run is confirmed missing, or the failed-controller
recovery above applies, recheck the live default branch and installed listener.
For v3, provide the successful Publish Crate run and exact attempt whose
verification step completed for that version and source. The original Actions
check and step log must still be available.

```bash
gh workflow run docker-publish.yml -R bigduu/Bamboo-agent --ref dev \
  -f "version=$RELEASE_VERSION" -f "source=$CUT_SHA" \
  -f "publisher_run_id=$PUBLISHER_RUN_ID" -f "publisher_attempt=$PUBLISHER_ATTEMPT"
```

For historical v2, provide only the exact version and source:

```bash
gh workflow run docker-publish.yml -R bigduu/Bamboo-agent --ref dev \
  -f "version=$RELEASE_VERSION" -f "source=$CUT_SHA"
```

Recheck the live default branch, installed listener and actual runs before
recovery; a staged branch alone does not activate a listener.

Keep old release tags, source identities, signed receipts and recovery inputs.
Their original instructions remain in the [historical guide](https://github.com/bigduu/Bamboo-agent/blob/ce89b4fcf4a45014cccb3bc7f817613ddcfa7eb2/docs/releases/ci-publication-flow.md).

Run `node --test scripts/release-source.test.cjs` for the eight retained historical
dispatch/preflight regressions. Their `workflow_run` checks read the hash-checked
publisher fixture from `f0eb67c378e87e203fe490c85c54a89683608624`, rather than the
active `publish-crate.yml`; the original v2/v4 producer and rejection cases stay.
This file is not directly selected by feature CI. Current frozen-Dev positive and
negative admission coverage remains in `crate-release.test.cjs` and
`ci-routing.test.cjs`, both selected by `Quick Checks`. Historical compatibility
does not authorize publication through the current frozen-Dev entrypoint.

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
macOS Intel. The v2 contract has three targets and v3 has five. Compare each source contract
separately and retain exact source/run/attempt/platform details when measuring
the new workflow. These timings explain the optimization; they do not claim a
measured speedup for an unpublished candidate.

Parallel execution changes the post-preparation critical path from approximately
`crate duration + slowest native duration` to their maximum, plus preparation,
runner waiting and finalization. Measure the real run; this is the scheduling
model, not a promised duration or percentage improvement.
