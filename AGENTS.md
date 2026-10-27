# Bamboo Agent Guidance

Read [README.md](./README.md) for the product boundary. Use this canonical Jiandu memory contract:

Before CI or release work, read the complete local
[shared delivery workflow](docs/delivery-workflow.md),
[Actions ownership audit](docs/ci-workflow-audit.md) and
[Bamboo commands and test map](docs/releases/ci-publication-flow.md).
Every agent follows the same feature/dev/release/main ownership and checkpoint
procedure. The full shared documents are present in this repository; consult
their recorded source snapshots and verify live evidence before using a proposed
path. The local entrypoint table below records the later #2363 composition; the
shared audit is historical evidence, not a live trigger inventory.

- Session memory is continuity for the current session. Project memory holds durable project facts and decisions; Global memory holds cross-project user context.
- Recall with a short lexical query. Use Jiandu's compact default top three `id`/summary results, then `get` only the selected item that needs full context.
- Query before writing. Store one confirmed atomic fact with a specific title and a few useful keywords, entities, and tags.
- Treat canonical Project memory as trusted durable project authority, but verify live repository or runtime state before acting. Treat Dream as a low-trust derived orientation snapshot, never as canonical evidence.
- Never store secrets or tokens. Do not add embeddings, vectors, or a duplicate Bamboo persistence/index layer.

Jiandu owns canonical persistence, derived indexes, lexical recall, and Dream snapshot bytes. Bamboo owns prompt selection and budget, optional reranking, and the model and cadence used to refresh Dream.

## CI entrypoints staged by #2363

The composed source retires automatic release/main CI and the main-CI publisher.
It has not passed combined-source dev admission or activated production. The
complete feature/release producer registries remain empty; default/protection,
38-control bootstrap and real positive release/promotion/Pages/Docker acceptance
are separate gates. Native feature acceptance still has the server writer-lock
lifecycle failure tracked in [#2364](https://github.com/bigduu/Bamboo-agent/issues/2364).
Fix it through its feature owner; a local policy pass cannot waive that failure.

| Entrypoint | Sole responsibility in this source |
| --- | --- |
| CI dev PR | Feature Unit/E2E, native/browser runtime, quality and Docs on the actual clean merge M. Both required summaries fail on incomplete selected owners. |
| CI manual | Same-SHA feature/dev diagnostic only. Reject Main, `release/*`, tags and unknown refs. It cannot authorize a formal release. |
| Publish Crate | Only `workflow_dispatch` using protected Main C, `frozen-source-v2` and frozen accepted-dev S. One allocator, original outputs and one finalizer; the empty registry stops new cuts. |
| Main Admission | Read-only trusted-base source/release or controller maintenance proof. No product execution, test or compilation. |
| Docs / Docker | Reuse authenticated originals through their staged consumers. Docs does not regenerate the site; Docker adds its one distinct ARM output and assembles/smokes the two runtime images. |
| Standalone Actor / Ticket / frontend acceptance / CodeQL | Explicit manual diagnostics. Their automatic Main/PR/push routes are retired; feature owners retain the corresponding configurations. |
| Release Cargo Isolation | Dev-PR publication-boundary acceptance and explicit manual diagnosis, using its own dummy-credential/ENOSPC inputs. No Main-PR route. |

`frontend-acceptance.yml` checks embedded packaging/build-script/server-resource
contracts; its automatic copy is retired because the feature executor and
inventory own those selections. Its diagnostic is not the Lotus browser suite.
Quick Checks is the sole Ubuntu Node policy owner. CI policy imports Main,
public-release, Docs, Docker and ARM fixtures once; sidecar policy registers
Native originals once, and crate policy retains its inline Source assertions.
Do not add those modules as second top-level commands or repeat them in the
Linux Cargo feature job. Keep the portable native-OS registrations distinct.
The v8 CI/old publisher archive is fixed test data, never current acceptance or
a dispatch fallback. Historical signed receipts/CURRENT/helper pins are unchanged.

## Parallel Development

Follow [Parallel Development](./gitflow.md). Give each bounded Issue one owner,
branch and isolated worktree; only the named integrator writes a shared batch
branch. Every feature PR to `dev` must pass required `Test`, which aggregates
Unit, E2E and the live SSH fixture on its actual merge source. One Linux Cargo
executor owns one target directory and preserves independent default package
configurations. Cargo runs each package's library/binary, integration and
documentation cases once. Explicit feature owners run only their catalogued
additional cases; new feature-sensitive cases require ownership review.
Compatible configurations reuse compiled artifacts, with features/profile and
actual Cargo freshness recorded. The ignored SSH case reuses its compiled
executable. Unit/E2E are logical target groups. All agents add ordinary cases to that
Cargo target catalog, instead of creating another Linux workflow for a covered
case subset. CI policy rejects unregistered Rust test producers; native OS,
containment-boundary, explicit manual diagnostics and pinned legacy routes are
scoped exceptions with named owners. Preserve
applicable native-platform acceptance before dev admission. Fix failures in the
feature branch before merging; record exact source, commands and results.
Ubuntu `Quick Checks` owns the 46 Supervisor component/protocol registrations
and 15 frontend artifact/policy registrations once. Each native feature job
runs the same 61 portable Node registrations once on its own OS; retain the
Windows unsupported-platform assertions. These do not execute the separate
`browser-runtime` Node suites. #2330 prepares their sole Ubuntu Node 24 feature
owner: `Browser Runtime Unit and E2E`, required by both Test/E2E summaries. Its
locked Playwright 1.62.1 headless shell runs all thirteen files serially once;
never copy that suite into portable Node, native Cargo or release/main jobs.
Keep real M/clean input binding and failure propagation. Native writer scratch
remains with the isolated runner; a Node exit never grants deletion. The WIP
ownership/retirement changes require combined-source CI before dev admission.
Keep the standalone default storage authority families' serial selection in that same
executor, reusing its compiled library-test executable without repeating cases.
`Quick Checks` remains auxiliary and never substitutes for runtime tests.

#2331 prepares required feature quality on the same actual merge source M.
Both `Test` and `E2E Tests` require Lint, MSRV, audit, deny, all four CodeQL
languages and Frontend Package Inventory. Keep all three strict
Clippy selections: default members/all-features, workspace/all-features and
standalone storage/all-features/lib (one build job). They share one analysis
target, but their feature graphs remain distinct. Preserve Rust 1.95 MSRV and
the existing warning/advisory policies; a real Clippy or compiler failure blocks
admission and must not become an allowlist, skip or release repair.
CodeQL uses `build-mode: none`; frontend inventory verifies committed bytes and
lists the server package without another product build. Ordinary embedded
compilation/runtime belongs to the canonical Linux executor, including its real
`CARGO_BIN_EXE_bamboo` frontend case. Inventory adds no separate dev optimization
or combined-member configuration, runtime suite or formal archive. Preserve API-only and
npm-staged consumer configurations. Root Birth Strict Lint is manual diagnosis
only; its exact analysis lane is owned by feature Lint and its runtime cases
remain with the canonical default-storage executable. See the guide's quality
owner table before adding another producer. A95/C2330/#2331 and the composed
retirement source require exact-source hosted acceptance before dev admission.
Retiring triggers in this source does not activate the empty producer registry
or change the installed controllers and protection.

## Documentation producer staged by #2321

CI's `Documentation` job owns dev-PR documentation on the actual clean merge M;
both required summaries consume its real result. Its supported command is
`cargo doc --locked --no-deps --all-features --document-private-items`, using
the 30 workspace default members rather than `--workspace`. Analytics and the
recorder example remain outside that selection. Keep this distinct from doctests
and CodeQL's `build-mode: none` analysis.

Retain the original site tar plus complete file/hash manifest and source,
controller, run/physical-attempt, configuration and toolchain receipt for 90 days
under `feature-docs-<run>-<attempt>`. The receipt checks integrity, not Main
authorization or external artifact API identity. Never overwrite an attempt or
silently follow input/site links. Run the targeted `feature Docs` policy fixtures
and affected CI routing checks; they use tiny real tar files, not Rustdoc.

#2355 stages the `docs.yml` Main-push consumer. Its zero-argument entrypoint reads
actual protected-main C and uses the real shared `verifyMergedPromotion` only
for `mode=promotion`. The metadata job may identify a shared-parser-recognized
bootstrap/controller event and skip Pages; unknown modes stop. Never treat a
locator, receipt or a green metadata job as a published promotion.

Read the original physical Docs upload log and artifact API identity, then
verify the original ZIP, source/configuration-bound receipt and every TAR file.
Copy `artifact.tar` through the existing Pages Actions transfer without Rustdoc,
retarring, runtime tests or source execution. Only that verified transfer gets
Pages/OIDC permissions; recheck the real Main promotion after large local reads
and require Main still equals this push. The consumer bounds ZIP and TAR to
1 GiB. Missing, expired, changed or unsupported originals stop without rebuilding
or a new durable-store fallback. Retries preserve the feature's physical attempt.

The reviewed 38-path Main closure includes the three literal Docs controls;
actual Ops installation and Pages positive acceptance remain pending. Missing
installed controls stop the consumer. Current CI has no Main/release site
producer. Keep the d6a producer and its original evidence. `ci-policy.test.cjs`
registers the new consumer Node contracts once in the existing Quick Checks
owner; never add a
Main test job or another Rust documentation producer.

## Native feature ownership

#2319 gives Windows and macOS one feature job, workspace and owned target each.
`native-feature-tests` runs `node scripts/native-test-owner.cjs`; both required
`Test` and `E2E Tests` require that whole matrix to succeed. Never start a second
dev-PR Actor, Ticket, TLS or recorder producer. The separate Actor/Ticket
workflows are manual diagnostics; full CI is an explicit feature/dev diagnostic.
There is no automatic release/Main native test route in this source.

Preserve these configurations; a shared directory is not permission to unify
their features or reuse an incompatible executable:

- Windows Actor keeps Rust 1.99.0/MSVC, API-only frontend and all 41 selected
  cases (9 snapshot, 3 gateway, 19 directory, 5 scoped and 5 initial processes).
  Standalone default storage remains separate from server storage/test-utils.
  Its actual junction/host evidence and frozen lib-test executable checks stay.
- Ordinary Windows server/TLS/frontend, recorder, Ticket and portable Skill use
  independent default package contexts, the stable toolchain and test opt-0.
  Actor API-only and ordinary embedded server outputs are distinct.
- macOS TLS/backtrace keeps the existing all-feature dev-profile opt-z server
  unwind guard; ordinary recorder/Skill/frontend tests keep test opt-0.
- Recorder lifecycle uses its actual Cargo-built binary and package working
  directory. Preserve `CARGO_BIN_EXE_tool-event-recorder`; never replace it with
  an invented path or stub executable.

The driver records compiled names, runtime counts, executable hashes and actual
Cargo freshness. A repeated compatible unit must be fresh; different feature,
profile, frontend or compiler identities remain distinct. Zero/ignored/missing
cases, source drift or missing native output fail admission. Native children
receive no publication/provider/tunnel credentials or workflow write channels.
Failed evidence is retained under the original source/run/attempt; do not clean
another owner or call a cache hit runtime evidence.

For this ownership change, run Node driver/policy/routing and the frozen authority
fixtures locally. Real Windows PowerShell/process/junction and macOS unwind
acceptance must pass on the matching native runners before dev admission.
The future #2318 source controller must depend on accepted #2311 and this complete
native map, with real unskipped job/case receipts. A339/v7 runtime evidence does
not establish this v8 native contract. Preserve the actual v6 historical bytes.

The intended release contract is feature tests -> accepted dev cut -> one version
reservation and release build/publication -> main promotion of the published
source. Release branches do not repair product code or rerun Unit/E2E; main
records stable code through immutable tags, Release manifests/provenance and the
promotion description, without another build or test run. Keep source manifests
at `0.0.0` and stamp only disposable formal-build checkouts. Repairs return
through feature/dev tests and produce a new frozen cut.

Before the first new cut, normally integrate current main ancestry into dev
through a tested feature PR. Preserve historical signed receipts and immutable
tags. Use the [CI and publication flow](./docs/releases/ci-publication-flow.md)
for current entrypoints and pending activation. The older
[Release Process](./CONTRIBUTING.md#release-process) is historical reference;
its release-repair and main-CI publication procedure is superseded here. Do not
dispatch a new cut before complete producer acceptance and the guarded Ops
cutover; do not reactivate the retired path to bypass either gate.

## Pull Request Review

### Inactive frozen-source foundation (#2323)

`scripts/frozen-source-admission.cjs` produces read-only source context, with
`publicationAuthorized: false`. Its default complete-producer registry is empty.
Do not pass test fixture policy into an actual controller, treat that context as
a signed reservation/publication receipt, or wire it into the live publisher.
Historical compile-only evidence, A339 and unaccepted C/native drafts do not
earn v2 acceptance. Preserve genuine v6/HMAC recovery within its frozen scope.
Future activation must pin a fully accepted feature producer and its helper
closure, with complete Unit/E2E/native/quality ownership and original physical
jobs. Independently prove the exact merged PR and normal B/F/M/S identities when
GitHub drops run PR associations. Keep actual main controller C separate from
product checkout S and never rewrite GITHUB_SHA. See the
[foundation contract](docs/releases/ci-publication-flow.md#inactive-frozen-source-foundation-2323)
for the read adapter, pending owners and verification command.

### Inactive Source-dispatch authority (#2336)

`verifyAcceptedDevFacts` can inspect accepted feature evidence before a frozen
ref exists or a dispatch starts. It returns `publicationAuthorized: false`,
without a controller or frozen-ref claim. `resolveFrozenSource` adds the actual
protected-main C and frozen-S checks. Neither data object is a signing capability.
Only `crate-release.cjs`'s `makeFrozenSourceContext()` may construct the new writer;
it reads the actual dispatch event and calls the real resolver with fixed GitHub
reads, the official check-suite query and original physical logs. Never inject
fixture policy or turn an environment JSON/boolean into authority. Keep the
production registry empty until every feature owner has actual accepted evidence.

Phase A was helper-only. Its distinct
`sourceDispatch` receipt field uses the existing allocator, HMAC key and writer;
it cannot upgrade a signed legacy reservation. C helpers must be ordinary files
from the actual C checkout, outside S, before any candidate execution. Use S as
verified data and preserve real `GITHUB_*` identities. Every write/retried PUT
must use the same fresh source/ref/dev-protection/reservation guard. Native and
crate adapters and dispatch wiring are staged separately by #2344 below; the
legacy package/publish/complete path rejects this new contract. Main
promotion/protection/production activation remain separate.

B0 corrects the #2323 frontend hash semantics: wrapper `bundle_hash` describes
extracted resources, while the raw ZIP has its own Git blob and SHA-256. Facts
remain read-only data. Before constructing the new writer, the factory runs C's
existing `frontend-package.cjs verify --source-root <S>` with no publisher,
registry or GitHub credentials. Only verify may select this committed Lotus Next
data root; stage and legacy roots stay unchanged. Reuse S's original wrapper/ZIP
bytes without restaging or recompression. The C verifier must check inner/outer
wrapper bytes, the resource-directory hash and the complete universal lock.

### Inactive original-output Source wiring (#2344)

`publish-crate.yml` has an explicit `frozen-source-v2` dispatch branch. It loads
actual protected-main C before product S and uses the real Source factory;
the production producer registry stays empty, so new cuts still stop. This
does not activate Main promotion, protection settings or production dispatch.
The old `workflow_run` trigger is retired. Its shared aliases remain unreachable
from every declared event; keep historical v1/v2 receipts, fixed v8 test data
and frozen recovery contracts without granting them a new dispatch route.

Source preparation reuses the existing frontend artifact: two exact S files plus
canonical `prepared-release.json`, bounded to 192 KiB as a file. Environment/job
outputs carry a small typed locator. Every reader checks regular files, real
paths, size, canonical JSON and hash. Preserve the original physical preparation
attempt on failed-only retries; the Native selector also binds its hash to the
official original preparation job/log. A copied plan cannot start a new native
build or cache/toolchain/stamp step: rerun the full workflow to produce a fresh
plan, which verifies every successful original before authorizing failed targets.
Legacy carriers remain exactly two files with their existing 64 KiB JSON bound.

Crate verification seals the first archive before its one verification compile;
uploads/resume reuse that ZIP. Native collection runs even when a sibling fails
and preserves exact original transport ZIPs. Missing successful originals stop.
Only the finalizer may mark the whole Release public after fresh authenticated
crate/registry closure, all native/host originals and latest required jobs pass.
Crate `receipt.completed` alone is not whole completion. The sole public proof
is the physical finalizer's canonical 64 KiB completion frame, immutable assets
and independently verified official execution; JSON alone never authorizes Main.
Preserve each crate's original physical origin even if that whole job later
failed. The finalizer performs no Cargo compilation or host packaging.

### Bamboo #791 integration exception

For the #791 multi-agent epic, the user's 2026-09-28 delivery instruction takes
precedence over the per-Issue PR and review steps below. Inventory all related
PR heads and existing work first, then merge implemented work into one local
integration baseline. Review integration changes locally and use focused checks
while assembling it. Submit one PR to `dev` with required Unit/E2E and local
test/coverage evidence under the current feature-admission policy. Complete
affected regression before dev admission; the target release path builds the
accepted product without repeating those tests. Do not request or
wait for GitHub Codex PR review for this epic. Close superseded component PRs and clean only safely merged worktrees after the final
integration is accepted. Preserve dirty or uncertain worktrees.

- Every non-draft pull request that enters review must have a Codex review for
  its current head and base. If no current review is already running or complete,
  add a pull-request comment whose entire body is `@codex review`; do not rely
  only on automatic review triggering.
- Wait for the `Codex Review Summary` to finish. A 👍 reaction on the trigger or
  an explicit Codex zero-findings result confirms a clean run; a `Completed`
  summary alone only confirms that the run ended. Review suggestions or inline
  comments are findings that must be resolved. Inspect the summary, review body,
  reactions, and unresolved threads instead of treating an empty
  `reviewDecision` or a `COMMENTED` review as approval.
- Any head commit or base change invalidates the earlier Codex result. After the
  update, resolve applicable findings and trigger a fresh exact-head review.
- Green CI does not mean review passed. Add `review:agent` and remove
  `review:needed` only after the current-head Codex review has finished with no
  unresolved findings and the remaining acceptance and merge gates pass.

## Release Branches

Follow the current [CI and publication flow](./docs/releases/ci-publication-flow.md).
After release-source admission is enabled, cut a same-repository
`release/<40-character-cut-SHA>` exactly at the freshly read, tested protected-dev
commit. Record its timestamp, SHA, tree and source receipt. Keep that product
source fixed through version reservation, native builds and publication; normal
dev advancement does not change it. Promote only the exact published source to
main. A needed product repair gets a feature branch, passing tests and dev
admission, then a fresh cut. Existing frozen release/recovery contracts remain
unchanged during the explicitly documented cutover dependency.

## Read-only Main proof prepared by #2345

`scripts/published-source-admission.cjs` verifies the accepted feature facts,
trusted main controller and its whole helper closure, original physical
finalizer log, retained asset bytes and registry checksums. It grants no writer
and receives no signing key. Both producer registries remain empty by default;
there is no Main workflow/protection activation in this slice.

Promotion must keep the released S product tree, its normal ancestry and the
entire currently trusted base-control closure. Exact S is acceptable only when
it already contains that same closure. Never replace the Main gate with an old
feature-only workflow or mix individual control files. Keep original physical
log identities when GitHub copies successes on rerun; an old frame is immutable.
The existing Quick Checks owner imports the focused Main proof tests once.

## Main loader staged by #2351

`main-admission.yml` and `scripts/main-admission.cjs` prepare a read-only
`Main Admission` job. The current default branch is dev: the workflow stops
before checkout until the separate Ops cutover selects default Main. Never
change default/protection, dispatch or register a producer merely to make this
staged code green. Source/release registries remain inactive. #2363 removes
the automatic Main compilers/publisher from this composed source; installation
on the protected controllers remains separate.

Before initial bootstrap, #2359 fixes the planned closure at 38 ordinary files:
the original 25 plus Docs three, Docker four, the sole ARM helper and five legacy
Main workflow retirements. This is a pre-bootstrap prerequisite, not an upgrade
of an installed 25-path controller. The anchor remains legacy Main `d6325a73`
and tree `9557a389`; no Main installation or producer registration occurs here.

After activation, only actual trusted B is checked out. These 38 ordinary
B controls include the full release-helper and Main-reader closure plus both
complete linked local shared documents; candidate
paths/policy/approval JSON cannot select a partial closure. Do not run product
S code, package managers, compilers or tests in this workflow. The normal CLI
always rejects bootstrap. Its separate exported Ops component can prove only
the fixed legacy Main product tree plus actual accepted-D controls; that result
cannot authorize a release, Docs deployment or Docker publication.

The #2359 prerequisite fixed the closure without activating its consumers. The
#2363 staged composition now includes Source2344, Docs2355, Docker2320, ARM2353
and the retired automatic routes. Missing consumer/helper files still fail a
real checkout/overlay; never add stubs or drop paths. Synthetic `Closure2359`
paths are inert data. Run only affected selected cases for a closure change.
Before live Ops, complete feature/registry acceptance and a separately reviewed
whole-control postimage. Every other stable product/version/lock/gitlink byte
and mode stays legacy B. Preserve the genuine v6 and fixed2026.10.11 recovery contracts.

Keep PRT metadata F separate from executed B: actual run/job/check-suite and
GraphQL file URL name F, while GITHUB_SHA/GITHUB_WORKFLOW_SHA and checkout are
B on default Main. A file URL naming F is not the executed definition. The
physical canonical admission frame binds B/F/promotion M, published S and the
original job; `verifyMergedPromotion` subsequently proves real normal P and its
exact postimage, without relabeling the feature's tested M or release C/S.

Required name plus Actions app15368 is not proven workflow-definition binding.
Ops must test candidate same-name stubs in both completion orders and record any
personal-repository platform limitation. Do not add an App/key or fabricate a
status to fill that gap. Future consumers verify original successful execution
and the merged-P proof, not merely a green name. The returned Docs execution
locator does not yet verify artifact API ID/digest or site tar integrity.

Public pretty receipt assets use the producer's 1 MiB role; completion/admission
frames stay 64 KiB. Wrapper assets stay 1 MiB. New Source accepts ZIP blobs only
up to 16 MiB, despite the broader legacy frontend carrier. Keep all original
hash/ID/size and fresh-read checks. Use the focused #2351 commands in the local
guide; do not repeat historical Source or product suites for this loader.

## Controller maintenance prepared by #2354

Normal Main promotion retains B controls; the separate `controller` locator
maintains those controls without moving unreleased product code to Main. It
requires real accepted-dev D facts under a known complete producer, actual
trusted default-Main B execution and the same fresh PR/Git identity checks.
Only the entire fixed ordinary D closure replaces B controls. Every other B
product, version, lock, frontend byte, mode and gitlink stays exact. A Main
workflow/record version change or additional control/helper path needs its own
explicit compatibility/closure review; do not infer a path list from D.

Controller maintenance emits `Bamboo Main controller maintenance:` with kind
`bamboo-main-controller-maintenance/v1`, independently of publication frames.
It has no release/version/tag fields and grants no publication, Docs or Docker
permission. Consumers still require a normally merged completed-release
promotion. Ordinary CLI bootstrap remains forbidden; the fixed legacy Ops
bootstrap is a separate one-time source-only operation.

For a future recipe P1, first accept D1 under already known P0 while keeping P0
definition/helper pins unchanged; D1 may carry reviewed P1 registration in the
existing Source registry. Maintain those complete controls to Main before D2
switches to P1 and earns its own real acceptance. If D1 changes a helper pinned
by P0, that first step cannot pass: stop and split the registration/compatibility
work. Never accept unknown definitions, omit helper pins, install fixture policy
or create another registry/store/key as a fallback. Current registries are
still empty, so real controller maintenance remains inactive.

Preserve the shared zero-argument public read factory and the inherited bounded
artifact ZIP port. This child changes no Main YAML/hash, protection/default,
publisher, Docs/Docker worker or production state. Only focused `Controller2354`
mock/tiny-Git contracts run locally; their fixtures are not accepted producers.

## Raw official job-log transport (#2358)

The Main public reader, Source-factory reader and Native selector/collector use
`--allow-escape-sequences` only on their fixed raw Actions job-log GETs. Preserve
original ANSI and line-ending bytes for hashes and frame verification. Keep the
existing bounds, child environments and JSON/binary policy; never strip escapes,
enable the flag globally or treat readable logs as source/publication authority.
Use the three existing test owners' `Raw2358` inert-port cases for this boundary,
without repeating product, browser or historical acceptance suites.

## Staged Docker consumer (#2320)

Docker consumes an authenticated completed release. Its only automatic event is
completed `Publish Crate`; a manual selector names the exact original run. Tags
and main pushes do not compile Bamboo or stage another frontend. Keep Docker
controller D, publisher C and compiled source S separate. Reuse zero-argument
`main-admission.cjs.createPublicReads()`. #2361 makes the real Source job the
sole full `verifyPublishedSource` caller. Its bounded canonical handoff is
accepted only from the same R/D's original successful physical Source job,
official attempt and step/log; JSON or an approval boolean grants no authority.
Workers never fall back to the full verifier. Production registries remain
empty until the coordinated cutover.

Every public zero-argument `authenticated()` call re-authenticates that Source
and refreshes Main/Dev protection, normal ancestry, frozen ref, exact accepted
feature context, publisher jobs/completion, Release/tag/assets and registry
checks. The completed original job fingerprints were independently proven once;
fresh complete latest/all/original-attempt projections must still match them.
Keep API metadata X distinct from actual workflow/checkout D. Equal current
Main/Dev SHAs use the Source-proven anchor's reflexive ancestry; a changed head
requires the official forward comparison. Main's own full verifier is unchanged.

Only the real Source role may retry a failed first read-only proof, after
complete old attempt evidence proves every Source ended without success and
every declared downstream node was skipped with empty steps. The exact static
image-name template may stand for its skipped two-platform matrix only after
the bound D workflow's name/needs/matrix bytes match the fixed definition.
Unknown, mixed or duplicate placeholders and started workers stop. An existing successful
Source always uses its original proof; lost successful evidence cannot trigger
another full proof. A whole rerun records guarded reuse, preserving the first
physical full-proof identity; latest Source failure blocks workers.

`arm-originals` is the sole Docker-only aarch64-unknown-linux-gnu producer.
It runs on a real ARM runner, pins official Rust 1.99.1 Bookworm by ARM manifest
digest, stamps disposable S using C's external stamper and compiles only the
default embedded bamboo-agent/bamboo release binary once. Resolve/fetch public
locked dependencies in isolation, then compile offline without container network
or credentials. Do not call the standard three-target CLI, alter its six assets
or completed receipt, run Unit/E2E, fetch moving npm or regenerate frontend.
Consume standard R's exact authenticated frontend ZIP/wrapper, equal to S,
within new Source limits of 16 MiB / 1 MiB. Keep real GITHUB identities unchanged.

Retain exactly bare `bamboo` and `receipt.json` for 90days. The official physical
upload log, original attempt/job, artifact ID/digest and completion frame bind
the receipt; JSON is data. Raw ZIP is capped at 1 GiB, binary at 1 GiB-8 MiB and
receipt/frame at 64 KiB. A started compilation without complete authenticated
originals stops, including failed/cancelled/unknown outcomes; never rebuild it
on retry. Copied successes and full reruns reuse the original physical output.
Cross-run/D recovery is unsupported. ELF/symbol checks are static; actual final
Bookworm loading and dependency smoke still own compatibility. Main closure,
registrations, official event metadata roles and genuine ARM execution remain
activation dependencies. See the guide's ARM producer contract before changes.

Image assembly copies the published Linux x64 binary or authenticated Actions
ARM original, exact three-file Host package and fixed runtime resources into
the isolated context. Install the locked Host runtime
dependencies with scripts disabled and its matching headless shell once. Do not
run Cargo, frontend installation, Unit/E2E or the thirteen-file browser suite.
Real ELF/loader, version, API/frontend and Host dependency smoke must pass before
candidate push. A checksum or Ubuntu 22.04 label does not prove ABI compatibility.
Keep UID 10001, `/data`, entrypoint, broker override and loopback compose defaults.

Retain both architecture entries and require both before formal multi-arch,
version or latest publication. ARM64 uses the separate original producer
[#2353](https://github.com/bigduu/Bamboo-agent/issues/2353); consumers never
substitute x64, compile source or delete the entry. It does not silently extend
the native three-target release manifest. Missing/ambiguous digest evidence stops
finalization; the original release assets must remain available and verified.
Before assembly, the same-run selector verifies official jobs/steps and original
Actions ZIP/digest carriers, including the original upload log's unique ID,
size and download URL inside its physical step. A retained success takes the
real read-only reuse path, skipping assembly, smoke, push and another upload.
A successful assembly without a complete authenticated carrier stops. Cross-consumer-run/D recovery is
unsupported in code: the same Source run or an unknown prior producer points
back to its original consumer instead of assembling again.
After carrier/history reads, re-authenticate the original Source and all live
guards, comparing held source/release/controller/completion before each write.
Image stage may only authenticate and copy immutable originals into owned
scratch initially; its complete live guard and actual consumer-step check must
finish before it emits any build-context output. On drift, retain scratch but
emit no context and start no build. Do not add an argv/policy bypass or repeat
the successful full Source download sweep in a worker.
The Docker consumer is staged: Node/tiny archive fixtures are not image evidence.
Real compatibility smoke and Docker control-closure/Ops activation remain pending.
The Host parser's decoded eight-MiB role limit is unchanged; only its bounded
base64 JSON pipe includes expansion overhead. Historical recovery writes its
version tag; `latest` additionally requires the exact official latest Release
ID/tag/source before and after the write. Keep all Docker actions immutable.
