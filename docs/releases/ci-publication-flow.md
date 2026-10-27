# CI and publication flow

Read the complete local [delivery-flow/v1](../delivery-workflow.md) and
[Actions audit](../ci-workflow-audit.md) first. These shared copies come from
Zenith `28b6e0e`; this guide adds Bamboo's commands, configurations and test map.
Use the same source checkpoint and original evidence when another agent takes
over. Check current activation and actual runs; a prepared owner is not an
accepted or published artifact.

This is the operating guide for Bamboo's CI and automatic release path. The
[Release Process](../../CONTRIBUTING.md#release-process) remains authoritative
for cuts, original-source repairs, protected admission and credentials.
[Parallel Development](../../gitflow.md) defines ownership and local evidence.

## Inactive frozen-source foundation (#2323)

`scripts/frozen-source-admission.cjs` is an inactive, read-only library. The live
phase contracts below remain active. This foundation does not change Actions,
publishers, signed reservations, release recovery, protection or dispatches.
Its default accepted-producer registry is empty: deployed compile-only/v6,
A339 without complete native owners, C0f38 without portable feature Node owners,
and the unexecuted C7c552 correction cannot acquire v2 source acceptance.

The target order is feature Unit/E2E and quality acceptance -> protected dev ->
exact frozen release source -> the existing single version reservation -> one
formal build/publication per native target -> normal main promotion. Product
repairs return through feature/dev and a new cut. Main push/promotion must not
repeat build/tests; a release dispatch using trusted main controller code is the
release phase. Immutable tags, Release manifests/provenance and promotion refs
record publication; do not mutate the release head or add a post-build source
JSON commit. This library does not activate that order.

The result is `bamboo-frozen-source-context/v2`, with
`publicationAuthorized: false`. It records:

- `source`: exact frozen ref, S, accepted dev merge D, tree, trusted main-history
  anchor and observed dev heads. S must equal D; the frozen ref must remain S.
- `feature`: original PR, immutable B/F/M, original CI run, latest logical
  attempt, official workflow/check-suite identity and definition revision Q,
  producer digest and each original job ID/physical attempt/steps fingerprint.
- `controller`: actual trusted main workflow revision C, event/ref and observed
  main heads. Actual GITHUB_SHA and GITHUB_WORKFLOW_SHA remain C; product S is
  a separate source selection. No source checkout or candidate code executes.
- `inputs`: original Git blob IDs and SHA-256 for Cargo.lock, committed frontend
  lock/wrapper/ZIP and native contract, plus the fixed three native targets.
  Raw ZIP identity is independent of the wrapper's resource-directory hash.
  Wrapper/archive semantics and universal-resource verification belong to C's
  existing artifact/preparation verifier.

The independent PR API must report a closed, merged dev PR with exact F and D.
Both the original physical source-job log's M and D must have ordered parents
[B,F] and the same tree; matching trees alone cannot identify a PR. B comes
from these immutable merge objects, rather than a mutable PR `base.sha` view.
Empty `run.pull_requests` is allowed only with that independent proof, the
official GitHub Actions check-suite and original jobs. Conflicting associations,
an OPEN PR despite a Git merge, missing logs or ambiguous identity fail closed.
The selected CI run must remain the latest matching feature run. Each required
physical job must have completed before dev admission with its required steps
successful. Logical copies in a later attempt retain their original physical
attempt and may have a different job ID. Select the earliest matching complete
job/step execution projection, excluding ID, attempt and runner metadata;
validate every step's time bounds and original attempt membership before
reading that original job's log. A copied logical entry is not another execution.
Paginated lists require stable, complete `total_count` and unique IDs, so a
truncated later run/job cannot disappear from the proof. Re-read PR, run,
check-suite, latest jobs, original jobs and refs before returning. The result is
an observed source context, not permission for a later publication write.

The trusted caller supplies `json(relativeRoute)` for fixed-repository REST
GETs, `checkSuite(nodeId)` using the existing read-only check-suite query and
`jobLog(jobId)` for original logs. Reuse the controller's existing transport;
the library adds no HTTP/credential/polling or artifact-execution mechanism.
`policy` belongs to reviewed controller code, never dispatch inputs or a PR.
It pins a complete workflow digest, workflow ID, critical jobs/steps, helper
closure and main-history anchor. Dev protection must enforce the reviewed
required contexts for everyone with GitHub Actions app 15368; a deletion-only
`protected: true` flag is insufficient. The inactive default requires `Test`
and `E2E Tests`; a future reviewed producer policy may pin its actual complete
gate names. This check changes no protection settings. Fixture policy is inert unit-test data; it is
not evidence that a real complete producer has run. The source-selected native
contract is read as bytes, without importing publication/signing helpers.

Run the focused contract fixtures through their existing Quick policy owner:

```bash
node --test --test-name-pattern='frozen source foundation' scripts/ci-policy.test.cjs
```

Before a real producer can be installed, finish #2322 Linux acceptance and
#2319 native/portable Node acceptance, including the actual matching hosted
platforms. The native drafts preserve 41 Actor cases and distinct compiler,
features, frontend and profile contexts; source context cannot substitute for
their runtime evidence. Legacy Platform's 61 portable Node registrations per
OS migrate to Ubuntu Quick and each native feature job in #2319, not this
foundation. #2330 separately owns all 13 browser-runtime Node files missing an
automatic owner; #2329 corrects the old retirement fixture independently.

Other pending owners cannot disappear at cutover: full strict all-feature
Clippy; Rust 1.95 MSRV/Python/locked all-target checks; source-sensitive Root
Birth strict Clippy; cargo-audit/cargo-deny/CodeQL; and the original frontend
build/lint contracts. #2321 owns unique documentation production/deployment.
All of these need explicit feature/analysis ownership before release tests are
retired. Keep the Release Cargo Isolation boundary owner. Normal main history
must enter the accepted dev cut through real tested ancestry, not tree equality.

The publisher/controller activation remains separate from #2324 original native
output durability, #2325 original crate archive reuse and #2320 Docker consumers.
Public main-promotion completion proof is another authority and is outside this
foundation. Preserve genuine v6 and the frozen 2026.10.11 HMAC recovery bytes;
never reinterpret those signed formats as the new read-only source context.

## Inactive Source-dispatch authority (#2336)

Phase A adds helpers and contract tests; publisher YAML is unchanged. No complete
production producer is registered, so real new-contract calls stop before an API
read or reservation. Local mocked-module positives and tiny Git merge fixtures
prove component contracts, not actual hosted feature acceptance or publication.

`verifyAcceptedDevFacts({sourceRevision, featurePr, ciRun}, reads, policy)` returns
`bamboo-accepted-dev-facts/v2`, with `publicationAuthorized: false`. It needs no
dispatch environment and reads no main/frozen ref. All original normal B/F/M/S,
execution-before-merge, official workflow/check-suite, helper/input closure,
pagination, protection and fresh-evidence checks remain. `resolveFrozenSource`
wraps those facts with actual protected-main controller C, required stable-main
ancestry and frozen `release/<S>` checks before and after the proof.

`readFrozenSourceContext()` reads only the actual dispatch event and fixed-origin
GitHub REST GETs, the existing read-only check-suite query and original job logs.
It accepts no proof/policy argument. The event selects `release_source_contract:
frozen-source-v2`, exact `expected_source_sha`, `release_ref`, `feature_pr` and
`accepted_ci_run`; the allocator owns the version, and the locked Lotus Next
input remains exact. The current publisher run must really be an in-progress
main `workflow_dispatch` at C. Actual GITHUB_SHA and GITHUB_WORKFLOW_SHA stay C.

Only `makeFrozenSourceContext()` calls this real verifier to construct a new
guarded writer. It checks the literal C helper closure against ordinary Git
blobs in an independent C checkout, and hashes immutable S frontend/contract data
before parsing it. C's publish-order helper is the only metadata entrypoint;
candidate scripts never become privileged helpers. Existing temporary Cargo
manifest/lock stamping remains a separate checked preparation step.

B0 fixes a pre-existing #2323 semantic gap: `frontend-manifest.json.bundle_hash`
is the extracted resource-directory hash (excluding that wrapper), not the raw
ZIP SHA-256. M/S Git blob equality and the raw ZIP SHA remain independent source
constraints. Facts validate data/format without granting archive acceptance.
The real factory first checks C helpers and immutable S bytes, then runs C's
`frontend-package.cjs verify --source-root <S>` before metadata or writer
construction. This verify-only option requires the committed Lotus Next input;
it cannot select an alternate root for stage/info or enable local/npm builds.
The child receives only PATH/HOME, owned temporary space and fixed committed
mode/package values, with no GitHub, registry or signing credentials. The
existing verifier actually checks canonical inner/outer wrapper equality,
resource-directory hash and the complete universal resource lock. It reads the
original committed wrapper/ZIP and never restages, changes `built_at` or rebuilds
the archive. Temporary extraction uses the verifier's existing owned scratch.
Real committed-data regressions are separate from the factory's inert transport
fixtures; those fixtures cannot establish semantic artifact or producer admission.

The existing signed receipt gains one `bamboo-frozen-source-dispatch/v1`
discriminator in `sourceDispatch`; its nested source context remains read-only
data. Identity, version allocator, HMAC domain/key and reservation store stay
unchanged. Historical receipts are neither upgraded nor adopted by this route.
Branch observations may advance normally, while original source, feature,
controller and run identities stay fixed. The same guard refreshes source/ref,
protected dev ancestry, actual controller run and the authenticated Release/tag
reservation before every existing write or retried registry PUT. A retained tag
that disappears stops; only a just-created reservation in the current process
may create its initial direct S tag. An interrupted untagged reservation stops
conservatively rather than allocating a replacement.

Phase B must hand off publisher ownership and consume this same factory/guard
from the reviewed Native and Crate adapters. It must load C before S, retain the
original-output contracts and use a fresh authenticated receipt for the sole
whole-release completion. This Phase A factory refuses the legacy
package/publish/complete operations. It cannot silently rebuild/repackage by
falling back to them. Do not activate the empty producer registry, add another
allocator/store/signer, import Main promotion authority, or dispatch production
while those dependencies remain pending. Genuine v6 and frozen recovery bytes
and the active legacy route remain unchanged.

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

## Read-only completed-release proof prepared by #2345

The public verifier is `scripts/published-source-admission.cjs`. It calls the
real accepted-dev facts verifier and independently authenticates the original
main-ref release run, Actions app/check suite, executed workflow C and all 16
trusted helper digests. It reads the unique canonical whole-release completion
frame from the earliest physical successful finalizer, then checks fresh
Release/tag, receipt, original frontend/crate/native/Host asset IDs and bytes,
and registry checksums. Main has no HMAC key: the independently authenticated C
finalizer is the proof that those signed original checkpoints were verified.
The completion JSON itself is data with `publicationAuthorized:false`.

Latest logical owners must succeed; copied successes retain their earliest
physical job and immutable old completion fields. A sealed crate may belong to
an earlier failed worker; preserve that origin and require the latest whole
crate closure to succeed separately. Ignore download/comment counters during
fresh identity checks, while retaining exact source/ref/ID/size/digest/body and
original bytes. Native Host manifest identity must match its retained asset.

Main promotion additionally verifies exact live PR head/base and tested merge,
normal S ancestry, every product blob/mode/gitlink and one complete trusted base
control closure. It rejects a same-tree copy without S ancestry, partial control
mixes and an old release tree that would remove the installed Main gate. Exact
S passes only if it already contains the whole trusted base closure.

These are staged read-only modules/tests, not an activated release or Main gate.
Default registries remain empty. There is no CLI, candidate code execution,
workflow/protection cutover, production dispatch or compiler in this child.
The existing Quick Checks imports the new focused tests exactly once; while
iterating, use `node --test scripts/published-source-admission.test.cjs` or its
name filters. Do not rerun product suites or historical Source checks to change
an original execution's identity. Actual trusted-base loader/bootstrap and the
first complete release/promotion remain separate acceptance work.

## Main loader and merged promotion prepared by #2351

The separate `main-admission.yml` prepares a single real `Main Admission` check
for Main PR Target events. It has read-only contents/actions/checks/pull-request
permissions, no Environment or publication credentials, and runs no candidate
code, Cargo, npm, browser, Unit/E2E, package or publication command. An initial
guard stops before checkout while default remains dev. The helper independently
rechecks actual repository default, enforced Main context/app, event, protected
Main B, clean checkout and the entire fixed 25-file ordinary B closure, including
both complete linked local shared documents.

The CLI accepts no mode, policy or approval JSON. Its one bounded locator in the
PR body is data, never authority:

```text
<!-- bamboo-main-locator/v1
{"mode":"promotion","sourceRevision":"<S>","featurePr":123,"ciRun":456,"releaseRun":789}
-->
```

The real publication verifier still needs the accepted complete feature/release
producers. Neither registry is activated here. The standalone Ops export
`verifyBootstrap` accepts a bootstrap locator with the same source/feature fields
and no releaseRun. It proves only legacy Main `d6325a73` / tree `9557a389` plus
all controls from actual accepted dev D; product/lock/gitlink/mode changes fail.
The normal CLI refuses that mode. First bootstrap is a separate guarded Ops
operation, not an invented Actions success or downstream deployment permission.

PRT's default-branch execution semantics and API metadata differ. Bodhi's real
negative PR135/run40432117302 checked out B=`19f11149`, but REST run/job/check-run,
CheckSuite.commit and GraphQL `file.repositoryFileUrl` all named F=`a0cc5cfd`.
The loader preserves actual GITHUB identities and validates that explicit F
metadata shape. It separately verifies B's pinned workflow bytes and closure;
it never infers executed B from that file URL. Promotion M comes from the fresh
PR/Git API, independently of GITHUB_SHA. Feature M remains in its original facts.

After all checks and fresh runtime/PR/source reads, the real job emits exactly
one canonical `Bamboo Main admission:` frame. The consumer accepts only that
original physical successful job/attempt and completed required step in the
specific Target workflow. Cloned rerun metadata does not create another physical
execution. `verifyMergedPromotion({pullRequest, admissionRun}, reads)` requires
an actually merged PR, P's ordered [B,F] parents, original admission before merge,
S ancestry, unchanged product plus complete B controls, and P.tree equal to the
checked promotion M.tree. It rereads mutable refs/checks/locators and permits
only normal subsequent Main advancement. Bootstrap cannot produce this proof.

The result is read-only facts (`publicationAuthorized:false`). Its `featureDocs`
field identifies the authenticated original Documentation execution, tested M,
source tree and expected artifact name. A separate Docs consumer must still
validate official artifact ID/digest, receipt and original tar bytes. Docker
transport/publication likewise remains its own lifecycle; this loader neither
builds nor deploys those outputs.

`main-admission.cjs` exports zero-argument `createPublicReads()` for these later
public consumers. It shares the fixed-origin repository GET, fixed read-only
check-suite query, original job-log/asset reads and credential-free crates.io
checksum read. It accepts no caller policy, authority or credential override;
requiring the module does not run its event-only CLI. Consumers reuse these
ports rather than copying a second GitHub transport.

Before calling this active, Ops must install truly accepted controls, retire
legacy automatic Main compilers/publishers, switch default to Main, configure
and read back the real protection, and exercise same-name candidate success
stubs with both completion orders. Current context+Actions-app protection and
PR135's second-bootstrap rejection do not prove collision prevention. If the
personal repository cannot bind the required workflow, record that boundary;
do not widen credentials, introduce an App/key or claim mock acceptance as live.

Focused local validation, without historical/product-suite repetition:

```bash
node --test scripts/main-admission.test.cjs
node --test --test-name-pattern='^(Main2351|Main promotion)' scripts/published-source-admission.test.cjs
node --test --test-name-pattern='^Main2351 workflow contract$' scripts/ci-policy.test.cjs
actionlint .github/workflows/main-admission.yml
```

Tiny real Git, synthetic API/frame and valid thirty-crate receipt fixtures prove
component behavior only. The public pretty receipt has a 1 MiB bound aligned
with its producer; both completion and Main admission log frames remain 64 KiB.
Original wrapper assets have a 1 MiB role. New Source intentionally accepts ZIP
blobs only up to 16 MiB (and bounded JSON/base64 reads), while the legacy carrier
supports up to 256 MiB. This child does not expand Source or claim larger ZIP
admission. Hashes, Git blobs, asset IDs/size/digest and fresh rechecks remain strict.

## Staged completed-release Docker consumer (#2320)

`docker-publish.yml` now selects a completed `Publish Crate` run, or an explicit
same-repository `release_run` on manual dispatch. Tag creation and main pushes
have no Docker producer. This is staged source: the default accepted feature and
release registries are empty, current default dev fails the main-controller
check, and no new-contract image/publication has been exercised. Activation also
requires the separate Ops control-closure update; #2320 does not change
`MAIN_CONTROLS`, default branch, protection, signing or version allocation.

`docker-from-release.cjs` uses the inherited #2351 zero-argument public read port
and calls the real `verifyPublishedSource` without policy injection. The original
physical finalizer frame supplies locators only. Official source/controller,
run/check suite, complete helper closure, raw original ZIP/asset bytes and crate
registry proof remain the existing public verifier's responsibility. Failed,
missing, expired, foreign or incomplete release evidence stops consumption.
Keep Docker D, publisher C, tested M and compiled S identities unchanged.

For x64, stage the original `x86_64-unknown-linux-gnu` executable and Host tar
from that completed release. The Host manifest must enumerate exactly its three
regular files with original byte hashes, and the lock must select Playwright
1.62.1. Links, traversal, extra/duplicate members and hash mismatches fail. Never
download a moving frontend, execute candidate scripts or copy the source tree.
The isolated context contains only those verified files, D's fixed Dockerfile
and a data-only source/version/asset/ABI descriptor.

The runtime is pinned to the official multi-arch Node24 Bookworm index
`sha256:55fb5f5a527ea2c5c4a95f963e48ef90aa38c7b346054fdc8ee9957a2b55d205`.
Official registry index and amd64/arm64 config metadata were read and hashed on
2026-10-27; both configs report Node24.22.1. This was a metadata-only read with
no image layers pulled. It is not compatibility evidence. The current original
Linux producer is Ubuntu22.04; inspect the actual ELF64 architecture, DT_NEEDED
and GLIBC/GLIBCXX/CXXABI requirements. Reject a newer-than-Bookworm GLIBC demand;
the actual loader and smoke decide the remaining ABI/runtime requirements.

The Dockerfile copies the binary and Host bytes unchanged, installs only the
original locked runtime dependencies with `npm ci --omit=dev --ignore-scripts`,
then installs their matching Chromium headless shell and required OS/FFmpeg
dependencies once. There is no Rust toolchain, Cargo, frontend npm/Vite build,
Unit/E2E or full browser suite. Preserve user10001, `/data`, port9562 and the
existing server entrypoint and broker-agent override. Compose requires an
explicit `BAMBOO_IMAGE` version/digest and never builds an unavailable image from
the checkout; its loopback binding, capabilities and named data volume remain.

Build the runtime image locally, before any push. Artifact smoke checks the
exact Bamboo version, actual loader/no missing libraries, API health, embedded
HTML/manifest, Node24/Host module and one headless dependency launch/normal close.
Children inherit only runtime paths/locale, not publisher tokens or workflow
write channels. No candidate repo, credentials or host volume enters a probe
container. Uncertain writer scratch stays in that isolated runner/container
lifecycle; a Node exit does not authorize host-directory deletion.

After smoke, a fresh real completed-source check immediately precedes standard
Docker push. Per-architecture candidate tags are temporary distribution outputs;
they are not a complete multi-arch release. Retain source/C/run/version-bound
digest records for 90days, keyed by consumer run, physical attempt and arch.
Failed-job reruns may reuse the other original carrier; missing, changed or
duplicate carriers stop finalization. A whole-workflow rerun is not a recovery
protocol. Never compensate by compiling a missing upstream original.

Both matrix entries remain mandatory. Additional Linux ARM64 originals and ABI
acceptance are the hard dependency
[#2353](https://github.com/bigduu/Bamboo-agent/issues/2353), outside the existing
three-native-target manifest. Until supplied, the ARM route explicitly fails:
no emulation, x64 substitution, source fallback, matrix deletion or partial
multi-arch/version/latest push. The finalizer authenticates fresh source and two
exact same-release digests, creates their manifest with standard Buildx, and
checks both descriptors and the returned registry digest before scanning that
exact image. The existing pinned Trivy warning-only policy is unchanged.

Focused local commands are `node --test scripts/docker-from-release.test.cjs`,
the single affected `crate resolver and Docker original consumer keep separate
owners` test in `release-frontend-policy.test.cjs`, and
`actionlint .github/workflows/docker-publish.yml`. Quick Checks imports the new
consumer fixtures once. These use real tiny tar data, mocked read ports and
static routing only; they do not pull/build/run/publish an image or certify ABI.
The approved scope is nine core files (including that necessary existing owner
assertion adjustment). Preserve historical fixtures and prior RED evidence.
One real compatibility smoke and fully accepted upstream/ARM/authority cutover
remain pending before calling this path active.
