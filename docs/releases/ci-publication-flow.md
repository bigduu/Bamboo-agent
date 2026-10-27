# CI and publication flow

This is the operating guide for Bamboo's CI and automatic release path. The
[Release Process](../../CONTRIBUTING.md#release-process) remains authoritative
for cuts, original-source repairs, protected admission and credentials.
[Parallel Development](../../gitflow.md) defines ownership and local evidence.

## Phase contracts and activation

Feature admission is implemented by #2311. Required `Test` aggregates Unit, E2E
and SSH runtime results on every dev PR. One physical Linux executor preserves
independent default package configurations plus the recorder in one owned target
directory. Ordinary library/binary, integration and documentation cases run once;
explicit feature owners run only catalogued additions. The controller records
each configuration's features/profile and actual compiled-artifact freshness.
Serial storage and ignored SSH reuse their default context executables.
Unit/E2E remain logical reporting groups. Applicable native acceptance runs before dev
admission; the recorder's Linux lifecycle is already in the canonical executor.
Ordinary Linux orchestration/context tests do not get separate compilation jobs.
Fix failures in feature branches.

## Feature quality owners prepared by #2331

All rows below use CI's actual dev-PR merge M with clean checkout checks before
and after execution. Both required summaries consume every selected result;
missing, skipped, cancelled and failed owners block admission. This candidate
combines A95 with C2330's native/browser WIP; it requires hosted acceptance on
its own final source and does not establish success for the earlier candidates.

| Owner | Configuration and real command | Output and boundary |
| --- | --- | --- |
| Lint | Ubuntu/stable/dev: `cargo clippy --locked --all-features`, `cargo clippy --locked --workspace --all-features`, `CARGO_BUILD_JOBS=1 cargo clippy --locked --all-features --lib -p bamboo-storage` | Three distinct feature graphs, one owned analysis target; original strict `-D warnings` and reviewed exceptions. JSON diagnostics retain actual compatible-unit freshness and each failure, per attempt. |
| MSRV (Rust 1.95) | Ubuntu/Rust 1.95.0: Python check-msrv unit tests, `python3 scripts/check-msrv.py`, `cargo check --locked --workspace --all-targets --all-features` | Full resolved locked metadata plus workspace/dependency MSRV checks; no substitution with stable/default compilation. |
| Security Audit | Ubuntu/stable: install pinned cargo-audit 0.22.2, `cargo audit --deny warnings` | Original `.cargo/audit.toml`; no new exemptions. |
| cargo-deny | Pinned action `3c634983`, cargo-deny 0.20.2: `check` | The action defaults to `--all-features` in its Rust 1.85/Alpine container; original targets/licenses/bans/advisories/sources remain. This is distinct from audit and MSRV. |
| Feature CodeQL | Ubuntu, actions/javascript-typescript/python/rust, init/analyze v4, `build-mode: none` | Four analyses with no product compilation, browser execution or repeated Node tests. |
| Documentation (#2321 staged producer) | Ubuntu/stable: `cargo doc --locked --no-deps --all-features --document-private-items` | One supported site configuration on the 30 default members; both required summaries consume the real job result. Original tar and complete integrity receipt retained per physical attempt. |
| Frontend Package Inventory | Ubuntu: committed bytes verification; stable Cargo only for `cargo package --locked -p bamboo-server --allow-dirty --list` | Inventory requires build.rs, frontend_build.rs, manifest and zip. No additional product compilation, runtime suite, build target/cache, version stamping or final archive. |

Ordinary embedded compilation/runtime, including the real `CARGO_BIN_EXE_bamboo`
frontend case, belongs to A's canonical default-package executor. Inventory adds
no extra dev opt-1/server opt-z or combined-member build configuration. MSRV
check artifacts, all-feature graphs, API-only Actor and npm-staged release
consumers remain distinct valid inputs. Share compiled outputs only within a
compatible compiler/features/profile/frontend identity; a restored cache is not
execution evidence. Quick Checks owns feature formatting once. All three
focused frontend runtime targets remain in A's canonical default-package tests.

Before converting Root Birth Strict Lint to a manual exact-snapshot diagnostic,
the A95 and C2330 definitions were checked: blob
`02f8bb8f7b40a238cc77bc11be71f4e6ef256451` contains zero runtime commands.
Its only command is standalone storage strict Clippy on Ubuntu/stable,
all-features/lib/dev, with one build job. Feature Lint retains that selection
on M. The four exact Root-birth cases and all other serial authority families
still execute once using A's default-storage test executable; the old workflow
comment about all-feature Unit execution was not execution evidence.

The existing main/manual CodeQL, original release frontend-head contract and
full legacy quality routes remain active until #2318 cutover. Thus this slice
fills feature ownership without claiming release/main repetition is already
removed. Rustdoc is staged below; Docker/ABI, native release outputs and final
crate archives belong to their separate slices; #2331 neither creates nor activates source
authority, version allocation, publication or promotion protocols.

## Documentation originals and pending Main consumer

#2321 stages the sole dev-PR site producer in CI's `Documentation` job. It checks
the actual merge M and clean source before/after generation, clears only its
owned `target/doc`, and preserves the `bamboo_agent/index.html` redirect. The
supported configuration uses all features and private items on Cargo's 30
default members, excluding Analytics and the recorder example. Doctests remain
with their runtime owners; CodeQL remains a separate analysis without a build.

`feature-docs-<run>-<physicalAttempt>` retains `artifact.tar` and `receipt.json`
for 90 days. The receipt includes every site file's path/size/SHA-256, the raw
tar hash, actual M/tree, distinct workflow controller SHA/ref, repository/run/
attempt, toolchain, selected manifests, configuration and lock/workflow hashes.
Bounded regular-file reads and real tar verification reject links, unsafe paths,
missing/changed files and source drift. Receipt integrity does not establish
publication authority: the future consumer must separately authenticate the
official workflow/run/jobs, original artifact API ID/digest and promoted source
through the shared v2 Main admission interface. Equal trees are insufficient.

Only the producer is implemented here, pending its own hosted acceptance. The
read-only Main consumer is not connected; legacy `docs.yml` still builds
main/master PRs and pushes and retains its existing main-only Pages permissions
and environment. Full legacy CI keeps its old `cargo doc --no-deps` route until
the coordinated authority cutover. These repetitions are pending, not removed.
The future consumer must copy the original tar rather than rebuild or repackage;
its Pages transfer has a separate one-day retention boundary. Missing, expired
or changed originals stop deployment. There is no new store or allocator, and
bootstrap never authorizes Pages deployment.

## Native and browser feature owners

#2319 consolidates feature native acceptance into `Native Feature
(windows-2025)` and `Native Feature (macos-latest)`. Each owns one workspace and
`target/native-feature`; both required summaries reject a failed, skipped,
cancelled or missing matrix result. Actor/Ticket standalone workflows no longer
start another dev-PR producer. Explicit manual and full legacy release checks
remain available; this change does not activate release-source publication.

The three portable Node suites register 46 Supervisor, 6 Lotus artifact and
9 frontend policy cases. Ubuntu `Quick Checks` runs these 61 registrations once;
each Windows/macOS native feature job runs them once before Cargo. Windows
retains the explicit unsupported-platform refusal assertions. These counts
describe registration, not hosted execution or the separate `browser-runtime`
Node suites.

#2330 prepares one Ubuntu Node 24 `Browser Runtime Unit and E2E` feature job.
Both required summaries consume that same job result. It installs the locked
`playwright-core` 1.62.1 with package scripts disabled, installs only its matching
headless shell once, then runs `npm --prefix browser-runtime run test:runtime`.
That serial command lists all thirteen files: original host/profile/checkpoint/
writer-close/shared-host/shared-failclosed/storage-runtime/native-root-tracker,
plus busy-response, ephemeral-clean, launch-admission, storage-preparation and
working-directory. The final TAP summary must contain real passing cases and
zero failures, cancellation, skips or TODOs. There is no Cargo/Vite or portable
61-case repetition, and release/main/legacy full paths skip the new suite.

The Node child receives only runtime paths and actual M identity, without
workflow credentials/write channels; Git source/clean checks bracket execution.
Per-attempt TAP evidence preserves failures. #2329's unknown writer scratch is
retained for isolated-runner teardown. Its diagnostic `40422761407` passed one
bounded download case plus eight close fixtures; it is not thirteen-file or
combined feature acceptance. C7c552 + F772468 are explicit WIP prerequisites;
refresh only through normal accepted A/C integration and require final-source
CI before claiming this owner accepted. This does not activate publication.

| Native configuration | Cases currently selected from source | Runtime owner |
| --- | --- | --- |
| Windows Rust 1.99/MSVC/API-only, independent storage/server/tools defaults | 41: 9 snapshot + 3 gateway + 19 directory + 5 scoped + 5 initial processes | Existing Actor immutable-executable controller inside the Windows owner |
| Windows stable/default server, embedded frontend, test opt-0 | 7 TLS + 7 frontend contract | One server Cargo invocation, two enumerated test targets |
| Recorder default package on both OSes | 2 Unit + 2 native lifecycle | Actual Cargo binary plus original `CARGO_BIN_EXE` and cwd |
| Windows default Ticket | 1 unsupported-filesystem case | Windows owner, exact selection |
| Portable Skill source/read/catalog | Windows 22/17/46; macOS 24/17/47 | Independent default skills/server-tools contexts |
| macOS TLS dev/all-features, server opt-z | 8 including real backtrace | macOS owner with oversized-unwind diagnostic rejection |
| macOS frontend default/test opt-0 | 7 | macOS owner; distinct from dev-profile server |

These are source/cfg expectations, not a claim of hosted runtime success. The
driver enumerates actual compiled names and requires the selected runtime count
with zero failures/ignores, recording source/tree, compiler, features/profile,
frontend mode, executable hashes and compatible-unit freshness. Expected totals
are 145 Cargo runtime cases on Windows and 107 on macOS. The original 41 Actor names, real junction
and process evidence remain guarded. Default storage cannot acquire server
test-utils; different frontend modes and macOS dev/test profiles do not share
incompatible executables. Compatible dependencies share the owned target.

Run `node --test scripts/native-test-owner.test.cjs scripts/ci-routing.test.cjs`
for controller fixtures. Native execution is
`node scripts/native-test-owner.cjs` on the matching disposable runner, with
`CARGO_TARGET_DIR` set to its own `target/native-feature`; it deliberately has no
cross-platform or compilation fallback. Per-attempt evidence includes partial
command logs and `native-owner.json`. Never publish raw host process/env logs.

New source authority #2318 must wait for accepted #2311 plus complete #2319
native receipts. Its release path must not repeat these cases. A339/v7 was not
published or present in the audited frozen sources; retain its commit/run as
runtime evidence without treating it as complete v8 native acceptance. Actual
v6 and earlier frozen workflow/helper contracts remain unchanged.

| Phase | Target contract | Activation |
| --- | --- | --- |
| Feature -> dev | Disjoint Unit/E2E, native acceptance and current review pass on the feature merge source. | Prepared by #2311/#2319; activation requires their exact-source hosted runs and protected dev admission. |
| Release source | Freeze the exact tested dev cut and reserve one formal version with the existing allocator. | Pending release-source admission. |
| Release artifacts | Stamp disposable checkouts; build/package/publish each native product once. No Unit/E2E repetition or product repairs. | Pending release-source admission. |
| Main promotion | Verify the exact published source and retain tag, manifest/provenance hashes and publisher run in the promotion description. No build/test. | Pending release-source admission. |

Keep source manifests at `0.0.0`. Immutable tags and Release manifests/provenance
are the version/artifact record; do not move release HEAD to add a post-build
JSON record. Before the first new cut, normally integrate current main ancestry
into dev through a feature PR passing Unit/E2E. A repair returns through
feature/dev and creates a fresh cut.

The existing release suite and main-triggered publisher remain active until the
separate source-admission cutover is accepted. The remaining sections document
that temporary legacy route and historical recovery, not permission to skip its
live gates. In that route release PRs still run complete acceptance, main CI
reuses it, and the canonical publisher reserves the version and builds native
assets after main. Preserve original proof/source/run identities throughout the
migration. Do not trigger an unimplemented release route or add another publisher.

Keep required check names stable. A missing, skipped, cancelled or failed
selected dependency fails required `Test`. Native Cargo matrices keep
`fail-fast: false` and independent failed-job reruns. More jobs or a cache hit
alone do not prove a speedup; report compile/execution/queue timing separately.

## Temporary legacy release and recovery procedure

## Freeze once and preserve source identities

1. Finish the bounded changes through protected `dev` admission. Freshly read
   actual dev HEAD, then record its UTC time, full SHA, tree and finite scope.
2. Create `release/<that-40-character-SHA>` normally at that exact commit. Keep
   dev open while the release owner validates this frozen candidate.
3. Complete current-head/base review, the full promotion suite and the separate
   original frontend-head contract: strict Clippy, the full locked build and the
   three frontend packaging/build/runtime test targets. Backend test suites
   belong to canonical CI. Record run URLs, actual tested source/tree, toolchain,
   lockfile, features, profile and results.
4. Use the normal two-parent release merge. Main admission authenticates that
   exact merge and its completed pre-merge evidence. Do not update the release
   with unrelated dev changes merely because dev advanced.
5. For a necessary repair, record original fix `F`'s exact SHA/tree, affected local
   tests and independent Code and Security review. Preserve `F` through normal
   protected-dev integration `U`, then fast-forward the release to `F`. Complete
   the mandatory full CI on the final release PR; an extra manual full CI on `F`
   is optional diagnosis, not a second mandatory full run. Keep each source's
   evidence separate and preserve the cut and original failure evidence.

The release head, PR synthetic merge, final main merge and workflow controller
are separate Git identities. Equal trees alone do not make their binaries or
runtime metadata interchangeable. In particular, version stamping and
commit-derived generated metadata remain bound to the actual formal build.
This process reuses acceptance evidence; it does not publish an unversioned CI
binary or a binary produced from another commit.

## Authenticate acceptance before publication

The controller for `workflow_run` comes from protected default `dev`; the
product source comes from the successful main-push CI event. Never assume the
controller SHA is the product SHA. Establish authority before checking out or
executing product code and before exposing publication credentials.

Main admission and the automatic publisher must independently verify:

- The same repository's normal merged release PR, exact base/head, ordered
  two-parent main merge, unchanged reviewed release tree and cut ancestry.
- The release head's normal admission into protected dev and current main
  freshness. A direct main push or stale queued source cannot publish.
- The actual latest applicable full release CI result, all required platform
  jobs and `Promotion Source`, with completion strictly before the merge.
  Never search past a newer failed exact-input run to find an older green one.
- Original job/run attempts when successful jobs are retained by a failed-only
  rerun. A partial rerun does not make earlier jobs execute again.
- Historical empty-PR-association evidence only through the reviewed producer,
  official check suite, executed workflow and checkout-free source-job proof.
  A conflicting association, arbitrary log or self-described receipt cannot
  grant publication authority.

API errors and unknown, missing, ambiguous or changed evidence fail closed.
Historical full main CI keeps its original evidence contract. A skipped heavy
job on the new main route cannot be accepted as an executed full test.

## One version and one publisher

Keep source manifests at `0.0.0`. The existing allocator reserves an unused
`YYYY.M.N` version for one accepted source and frontend identity. Stamp only
disposable publication/build checkouts and preserve external dependency locks.
The locked frontend package, version, archive and manifest hashes remain fixed;
registry fetch failures must not select a moving `latest` or another bundle.

All production attempts share `bamboo-crate-publication`, with queued entries
retained and cancellation disabled. Parallel platform jobs are build workers,
not independent publishers or version allocators. Native workers receive no
GitHub publication token, Cargo registry token or signing key.

The current v2 executable contract requires macOS ARM, Linux x64 and Windows
x64. Historical v1 sources retain their four-platform contract, including macOS
Intel. The accepted source selects the contract; a dispatch or retry cannot
remove a target. See [Shared Bamboo executables](shared-executable-assets.md).

### Parallel execution and cache boundaries

`automatic` authenticates the main source, reserves the signed identity, and
uploads `bamboo-prepared-frontend-attempt-<preparation-attempt>`. The independent
`prepared_release` job output binds the original attempt, same run/controller,
Release ID, source/tree, version, frontend hashes and selected platform contract.
The transport itself grants no authority.

After preparation succeeds, `publish-crates` and `executables` both depend only
on `automatic` and run concurrently. The crate worker authenticates fresh signed
progress for that exact reservation without calling the allocator again. It
cannot replace sealed frontend assets once workers have started. Native workers
validate exactly two bounded regular frontend files, reject extras or symlinks,
and verify the independent hashes before building. They need no draft-Release
API access, signing key or publication token.

`publish-executables` joins `automatic`, `publish-crates` and `executables`.
It still authenticates the completed public crate receipt and all required
native outputs before publishing the final manifest. Failure in either branch
cannot advertise a complete binary release.

Native caches map `RUNNER_TEMP/bamboo-executable-target`. Cache compatibility
includes the source profile, producer contract and target, together with the
action's Rust toolchain, environment and Cargo input keys. Restore dependencies,
then evict workspace executables, fingerprints and generated build outputs so
the actual source/version regenerates its embedded frontend evidence. Preserve
the source's compiler/profile/wrapper restrictions. Historical v1 and frozen
restore-only sources do not write these caches.

Crate readiness and binary readiness are separate. A public crate Release is
insufficient for desktop consumption until every asset required by its source
contract and the final manifest have been uploaded and verified. Downstream
consumers pin the actual version, source and hashes after that point.

### Crate worker disk lifecycle

The credential-free Cargo worker measures free space and its private debug
scratch before and after packaging. It reclaims only that worker's
`data/target/debug` tree when free space drops below 2 GiB or debug scratch
exceeds 8 GiB. It preserves source, staged `.crate` archives, package output,
registry cache and signed publication receipts. Startup measurements describe
the same private target filesystem; this is separate from GitHub's persistent
Actions cache storage.

A package command that exits normally with Cargo status 101 and an explicit
ENOSPC diagnostic may reclaim debug scratch and retry the same package once.
Other errors, interrupted or unknown container outcomes, and a second ENOSPC
remain failures. This recovery does not repeat an upload, reserve a version or
change the archive checksum. Worker permission cleanup covers only its own
non-symlink files and preserves Cargo's exit status.

`Release Cargo Isolation` exercises this path with a tiny Cargo package and a
private 128 MiB Linux tmpfs. It fills that mount to real ENOSPC, checks one
successful recovery and exact reserved bytes, and replaces only the final
network uploader with a fixture. Ordinary sandbox tests also cover bounded
retry, non-space failures and symlink rejection. Release timing improvements
must be measured on a later accepted production run.

## Failure and recovery

- Preserve the original failed run, source and reservation. Retry the same
  identity; do not allocate a new version to hide a partial failure.
- Prefer failed-job reruns when supported, retaining successful native outputs
  and their actual attempt numbers. A full rerun must respect existing signed
  checkpoints and immutable asset identities.
- Select frontend transport using the successful preparation's original attempt,
  not the retrying job's current attempt. Native transport retention is bounded;
  missing expired inputs require the normal verified rebuild route.
- If the same main CI run is retried, fresh admission may advance only that
  run's attempt while preserving all source and pre-merge bindings. Keep original
  signed reservation/checkpoint attempts intact; never relabel them as new work.
- After an uncertain upload, read back the exact remote identity before retrying.
  Existing identical bytes may be reused; conflicting bytes stop publication.
- Complete only missing authorized assets. Never replace an uploaded asset,
  move a frozen tag, re-sign foreign evidence or substitute a locally rebuilt
  binary for the original accepted one.
- Cancel only runs owned by this workstream or explicitly handed to its owner.
  Do not borrow another release's runner window, output directory or cache writer.

## Activate and measure

Land workflow and verifier changes through a reviewed PR to protected dev,
including executable positive/negative policy and authority tests. Preserve
current full acceptance on the next normally scoped frozen release; do not
advance an active frozen branch just to activate tooling.

Before promotion, verify the actual current required checks and workflow
definitions. Review changes to pinned historical producers as authority changes.
After promotion, record the lightweight main run, canonical publisher run,
reserved source/version/frontend, platform receipts and final manifest identity.

Compare actual critical-path and step timings against a recorded baseline.
Cache restoration alone is not acceleration evidence, and synthetic policy tests
do not establish native builds or production publication. This document records
the workflow contract; each release still needs its own real acceptance evidence.

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
