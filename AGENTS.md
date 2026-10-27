# Bamboo Agent Guidance

Read [README.md](./README.md) for the product boundary. Use this canonical Jiandu memory contract:

Before CI or release work, read the complete local
[shared delivery workflow](docs/delivery-workflow.md),
[Actions ownership audit](docs/ci-workflow-audit.md) and
[Bamboo commands and test map](docs/releases/ci-publication-flow.md).
Every agent follows the same feature/dev/release/main ownership and checkpoint
procedure. The full shared documents are present in this repository; consult
their activation table and verify live evidence before using a proposed path.

- Session memory is continuity for the current session. Project memory holds durable project facts and decisions; Global memory holds cross-project user context.
- Recall with a short lexical query. Use Jiandu's compact default top three `id`/summary results, then `get` only the selected item that needs full context.
- Query before writing. Store one confirmed atomic fact with a specific title and a few useful keywords, entities, and tags.
- Treat canonical Project memory as trusted durable project authority, but verify live repository or runtime state before acting. Treat Dream as a low-trust derived orientation snapshot, never as canonical evidence.
- Never store secrets or tokens. Do not add embeddings, vectors, or a duplicate Bamboo persistence/index layer.

Jiandu owns canonical persistence, derived indexes, lexical recall, and Dream snapshot bytes. Bamboo owns prompt selection and budget, optional reranking, and the model and cadence used to refresh Dream.

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
owner table before adding another producer. A95/C2330/#2331 are combined WIP
until exact-source hosted acceptance; legacy main/release gates stay active
until the separate source-controller cutover. Do not claim that cutover here.

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

This producer is staged until exact-source hosted acceptance. The Main consumer
depends on shared v2 promotion authority and is not wired: legacy `docs.yml`
still builds main/master PRs and pushes, and full legacy CI retains its old docs
command. Preserve those live routes until cutover. The future authenticated
consumer must copy the successful original tar bytes; missing/expired evidence
stops deployment without recompilation or repackaging. Bootstrap cannot deploy.

## Native feature ownership

#2319 gives Windows and macOS one feature job, workspace and owned target each.
`native-feature-tests` runs `node scripts/native-test-owner.cjs`; both required
`Test` and `E2E Tests` require that whole matrix to succeed. Never start a second
dev-PR Actor, Ticket, TLS or recorder producer. Their separate workflows/full
jobs remain explicit manual or historical release routes until #2318 activation.

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

Cutover dependency: #2311 installs feature admission only. Existing frozen
release acceptance and main-triggered publication remain active until the
separate release-source admission change is accepted. Do not bypass those live
gates or dispatch the new release route before that cutover. Before the first
new cut, normally integrate current main ancestry into dev through a tested
feature PR. Preserve historical signed receipts and immutable tags. Use the
[Release Process](./CONTRIBUTING.md#release-process) and
[CI and publication flow](./docs/releases/ci-publication-flow.md) to distinguish
implemented feature admission from the pending release protocol.

## Pull Request Review

### Inactive frozen-source foundation (#2323)

`scripts/frozen-source-admission.cjs` produces read-only source context, with
`publicationAuthorized: false`. Its default complete-producer registry is empty.
Do not pass test fixture policy into an actual controller, treat that context as
a signed reservation/publication receipt, or wire it into the live publisher.
The current compile-only route, A339 and unexecuted C/native drafts do not earn
v2 acceptance. Preserve all active legacy gates and genuine v6/HMAC recovery.
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

Phase A is helper-only and does not change publisher YAML. Its distinct
`sourceDispatch` receipt field uses the existing allocator, HMAC key and writer;
it cannot upgrade a signed legacy reservation. C helpers must be ordinary files
from the actual C checkout, outside S, before any candidate execution. Use S as
verified data and preserve real `GITHUB_*` identities. Every write/retried PUT
must use the same fresh source/ref/dev-protection/reservation guard. Native and
crate adapters, the sole whole-release completion and dispatch wiring need the
separate reviewed handoff; the legacy package/publish/complete path rejects this
new contract. Main promotion/protection/production activation remain separate.

B0 corrects the #2323 frontend hash semantics: wrapper `bundle_hash` describes
extracted resources, while the raw ZIP has its own Git blob and SHA-256. Facts
remain read-only data. Before constructing the new writer, the factory runs C's
existing `frontend-package.cjs verify --source-root <S>` with no publisher,
registry or GitHub credentials. Only verify may select this committed Lotus Next
data root; stage and legacy roots stay unchanged. Reuse S's original wrapper/ZIP
bytes without restaging or recompression. The C verifier must check inner/outer
wrapper bytes, the resource-directory hash and the complete universal lock.

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

Follow the authoritative [Release Process](./CONTRIBUTING.md#release-process).
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
staged code green. Source/release registries remain inactive. Legacy automatic
Main compilers/publishers are still live until their separate cutover.

After activation, only actual trusted B is checked out. The fixed 25 ordinary
B controls include the full release-helper and Main-reader closure plus both
complete linked local shared documents; candidate
paths/policy/approval JSON cannot select a partial closure. Do not run product
S code, package managers, compilers or tests in this workflow. The normal CLI
always rejects bootstrap. Its separate exported Ops component can prove only
the fixed legacy Main product tree plus actual accepted-D controls; that result
cannot authorize a release, Docs deployment or Docker publication.

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
