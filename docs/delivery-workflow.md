# Shared delivery workflow

Contract: `delivery-flow/v1`. This is the common policy for Bamboo, Lotus Next,
Bodhi and their Homebrew consumer, regardless of which agent owns a change.
Implementation is tracked in [Zenith #399](https://github.com/bigduu/Zenith/issues/399).
The [Actions file audit](ci-workflow-audit.md) maps every reviewed workflow to
its owner, deduplication decision and retained historical or analysis boundary.

This document defines the reviewed target. The activation table below records
which executable gates are still pending; a written policy is not evidence that
the corresponding Actions have shipped.

## One responsibility per phase

The product branch sequence applies to Bamboo, Lotus Next and Bodhi. Zenith
and the tap do not produce those products: their feature/update PRs target main
and run their own source-policy or installation checks before admission. Their
main pushes do not repeat those checks. Do not add product dev/release branches
or publication dispatchers to these metadata consumers.

| Phase | Owner and allowed work | Completion gate |
| --- | --- | --- |
| Feature PR into `dev` | The feature owner fixes its Unit and E2E failures. Test binaries and browser fixtures are compiled here, once for each distinct configuration. | Required Unit/E2E outcomes, complete coverage, current review and exact head/base/source evidence. |
| `dev` | Integrates accepted features. Normal pushes do not repeat the feature suites. | Accepted merge/tree linked to the original feature checks; no unchecked direct admission. |
| Frozen `release/<cut-SHA>` | Select an accepted dev cut, reserve one final version, build/package each formal target once and publish the original assets. | Source admission, version/architecture/package/signature/resource/hash checks and completed publication receipts. |
| Promotion to `main` | Preserve stable source and its immutable version, tag and artifact records. | Read-only proof of the built source, completed release, normal promotion and unchanged product tree. |
| Main push/promotion events, recovery and consumers | Read, verify, distribute or recover the accepted assets. | Original source/version/run/attempt/artifact identities. No product rebuild or full test rerun. |

Release is not a test integration or repair branch. A failing feature remains
with its feature owner. A runtime defect discovered during packaging returns to
a feature branch, passes Unit/E2E into dev, and gets a new frozen cut. Do not
repair a frozen product in place, chase moving dev, or assemble release-only
fix/integration batches.

Release may reject a malformed or incorrectly stamped artifact. Inspecting its
version, architecture, package members, signature, embedded resources and hashes
is artifact verification; it must not invoke Unit, E2E or another compiler.
Registry readback and checksum verification are also required consumer checks.

## What counts as duplicate work

Identify a test by source, target, case, features and native environment. Give
each selection one automatic owner. Unit and E2E selectors must not both run
the same ordinary test harness. An all-features integration selector must not
silently select the library Unit harness again. Keep deliberately different
feature configurations, native OS coverage and negative-control source variants
explicit in the repository's test map.

Identify a formal build by product source, final version, target, profile,
toolchain and locked inputs. It has one producer. Publish, recovery, Docker and
desktop consumers verify and reuse those bytes. A cache hit is not an artifact
receipt. Uploading an identity file and rebuilding the product in the next job
is still a duplicate build.

Feature test compilation and the final version-stamped release build have
different outputs. Do not replace the latter with a `0.0.0` CI bundle. Within
feature acceptance, reuse compiled test outputs and browser dist between phases
instead of adding a standalone workspace build before Cargo test or rebuilding
dist for every browser consumer. Rerun failed jobs only when their exact inputs
and successful retained outputs still match; source changes require fresh
affected evidence.

Each successful formal target must persist its output and receipt before the
next publication/finalization step. Key the checkpoint by frozen source, final
version, target, locked inputs and original run/attempt; verify its bytes before
reuse. Infrastructure failures may retry only the failed target with unchanged
admitted inputs. A finalizer retry uses successful checkpoints and performs no
compilation or packaging. If a previously successful original output is missing, stop for
explicit recovery rather than quietly rebuilding it. Product/source repairs
return through feature/dev and require a new frozen cut.

Bamboo's current per-crate/per-server-block runners still compile overlapping
test outputs. Disjoint test names alone do not resolve this. Before activation,
use one physical Linux executor and one owned target directory for the canonical
Unit/E2E configuration, then reuse the compiled ignored SSH fixture as well.
Native checks on other operating systems remain distinct. A future restoration
of parallel case workers must transport the actual test outputs and required
resources; dependency caches alone cannot establish compilation reuse.

Apply the same ownership rule within each native OS: compatible test selections
share one compilation and its outputs. An OS label alone does not justify two
producers for overlapping targets. Record any required feature/profile/toolchain
difference in the test map. Runtime acceptance, lint analysis and formal release
outputs have distinct purposes; none may be copied into another workflow merely
to repeat its existing gate.

Lotus retains supported Node Unit compatibility lanes, but its ordinary browser
suite has one canonical Node24 dist producer and four case shards. Real Bamboo
consumes that same dist. Do not build another dist or repeat the full browser
suite solely because the test-runner Node version differs. Preserve complete
case coverage and explicit native negative-control source variants.
The selected publication/verification browser still needs its OS dependencies
and FFmpeg for video evidence. Removing an unused default Chromium download must
not remove those real runtime prerequisites.

## Product ownership

| Product | Feature acceptance | Sole release producer | Downstream consumption |
| --- | --- | --- | --- |
| Lotus Next | Unit, browser E2E, necessary Real Bamboo behavior and native Actor controls. | Build the final-version dist and pack its npm tgz once; publish that exact tgz. No release-time browser/Unit rerun. | Bamboo and Bodhi verify the package/version/source/manifest and every resource; no consumer Vite build. |
| Bamboo | Disjoint Unit and integration E2E, frontend consumption contracts, native filesystem/process/Host cases and required feature-specific coverage. | One existing allocator; temporary stamping; one native binary per supported target; one verified crate archive per crate. Upload verified original archives without another package compilation. | Bodhi downloads the locked executable. Docker repackages supported published targets after the final manifest exists. |
| Bodhi | Shell Unit and real actor/static-dir/browser assembly E2E using the same debug/test output and locked published dependencies. | Stamp the final version and build each desktop installer once. No feature CI installers and no release-time Unit/E2E. | Publication and recovery retain original installer bytes and receipts; Homebrew verifies the published DMG. |
| Homebrew tap | Updater Unit plus one necessary candidate installation/architecture/signing E2E. | No upstream product producer. | Main stores accepted URL/version/hash. Automatic updates own their prepared-candidate audit; ordinary main pushes do not repeat it. |

Current native Bamboo v2 and Bodhi targets are macOS ARM64, Linux x64 and Windows
x64. Historical receipts retain their original target set, including Intel
macOS where required. Docker Linux ARM64 is an additional target, not a duplicate
of the existing Linux x64 executable; producer support and runtime ABI checks are
a separate bounded change.

Jiandu's current Homebrew formula compiles from a source archive. Eliminating
that downstream compilation requires a real prebuilt Jiandu producer and a
verified formula migration. Do not point the formula at invented assets or claim
the tap is a zero-compilation consumer until that dependency is delivered.

## Source, version and authority

Keep these identities separate:

1. The actual feature test snapshot, feature head/base and CI run/attempt.
2. The normally accepted dev merge and its tree.
3. The frozen release source actually compiled and its reserved final version.
4. The trusted workflow controller and original build run/job/attempt.
5. The later main promotion commit.

Admission must prove the feature result belongs to the accepted inputs and
that the frozen source preserves the tested product tree. Preserve the actual
tested snapshot even when GitHub's eventual merge SHA differs. Equal trees do
not make the two runtime identities interchangeable. Never relabel a release
binary as built on a later main merge.

Use the existing trusted default-branch controller to dispatch a selected frozen
release source. Feature code gets no publication credential. Controller and
product checkouts remain separate. The phase is selected by the event and its
validated inputs, not by the controller ref: a release `workflow_dispatch` may
execute reviewed controls from main while building the frozen release source.
This is the sole release producer, not a second main-push/promotion producer.
A new release-source admission contract must
reject incomplete, skipped, failed, stale or foreign feature evidence. Historical
compile-only or runtime-skipped receipts cannot authorize a new-contract release.
Retain old receipt parsers only for their explicitly frozen recovery routes.

Use the one existing version allocator or explicit reviewed unused version.
Reserve it before compiling, stamp only disposable build checkouts, and preserve
source manifests' `0.0.0` policy where used. Do not add a second allocator.
The immutable version tag, completed Release manifest/provenance and promotion
PR/merge references are the stable version record. Do not add a post-build
record commit that silently moves the frozen release head.

Immediately before irreversible publication, recheck the exact frozen ref,
source, version reservation and admitted dev ancestry. API failures are unknown
state: read back the targeted remote identity before retrying a write. A final
manifest means every required asset is complete. Tag creation or a public crate
Release alone does not establish binary availability.

## Agent procedure

Every repository must provide a root `AGENTS.md` and link its local delivery
guide. Use the existing filename; do not create a second conflicting `AGENT.md`.
The local guide maps the shared phases to exact commands, check names, supported
configurations, test selections, artifact names and ownership.

1. Read `AGENTS.md`, the local guide and the activation state. Inspect the Issue,
   existing owner/PR, remote base and required checks before creating work.
2. Claim one bounded slice. Use its isolated worktree and unique branch. Record
   scope, file/contract ownership, non-goals and a 4-8 hour delivery estimate.
3. Fix and test in feature. Coordinate overlapping manifests and authority
   helpers; do not cancel another owner's jobs or mutate their checkout.
4. Submit to dev with current review and real Unit/E2E. Failures stay in feature;
   do not waive them into a release. An integration update refreshes affected
   evidence for its actual head/base.
5. Freeze only an accepted dev cut. Release builds and verifies formal artifacts
   once. Consumers retain exact source/version/hash identities.
6. Promote only a completed release to main through a read-only metadata/source
   gate. Main push/promotion events perform no product test, build, pack or
   publication; this does not prohibit the trusted main-ref release controller
   described above.
7. Report exact source/check/run/attempt/artifact evidence and remaining work.
   Do not call a proposed, skipped, locally mocked or pending phase delivered.

Before handing work to another agent, save the reviewable source as a commit
and record its exact SHA, worktree, owner, validation and remaining steps. Use
the same branch and evidence for a continuation; do not start duplicate suites
because the agent changed. Preserve active worktrees and uncommitted evidence;
only clean them after ownership and recovery have been checked.

Repository branch protection must require its real feature aggregate, and main
promotion must require its read-only source/release check. Changing a required
check name and activating the matching rule are one coordinated cutover; do not
create a window for unchecked admission or replace a test with a success stub.

## Migration and activation

The baseline review used Bamboo dev `7e36677a`, Lotus Next main `64f3617e`, Bodhi
main `c556e0fc` and tap main `e8fa95f8`. This is an audit baseline, not a moving
release selector.

| Slice | Tracking | Activation state |
| --- | --- | --- |
| Shared plan, root instructions and ownership | [Zenith #400](https://github.com/bigduu/Zenith/pull/400), [#403](https://github.com/bigduu/Zenith/pull/403) | Merged. The executable source-repository cutovers below remain separate. |
| Bamboo Linux feature Unit/E2E and instructions | [Bamboo #2322](https://github.com/bigduu/Bamboo-agent/pull/2322), Issue #2311 | Draft. Preserve the cancelled first run. At head `95ea3498`, original [run40425939556/1](https://github.com/bigduu/Bamboo-agent/actions/runs/40425939556) passed all 31 default commands, the serial/SSH selections and 14 additive cases, then failed the agent-feature catalog before running that context. This is partial runtime evidence, not complete admission. The next fix must preserve per-target case ownership and include newer accepted dev changes. |
| Bamboo compatible native compilation ownership | [Bamboo #2319](https://github.com/bigduu/Bamboo-agent/issues/2319) | In implementation after the Linux ownership slice. Native OS, feature, profile and toolchain differences remain explicit; a green Linux result cannot substitute for these checks. |
| Lotus dev feature Unit/E2E and PR runtime identity | [Lotus Next #450](https://github.com/bigduu/lotus-next/pull/450), Issue #446 | Draft. Unit, all four browser shards and Native Actor controls pass. [#453](https://github.com/bigduu/lotus-next/issues/453) preserves the original network-change failure and inconclusive diagnostic. One failed-job retry reused the same dist and passed packed local acceptance, then failed HTTPS browser control. Independent product repair [Bamboo #2326](https://github.com/bigduu/Bamboo-agent/issues/2326) needs a deterministic regression and a genuinely published replacement input before fresh complete Lotus acceptance. |
| Bodhi feature tests and debug assembly reuse | [Bodhi #118](https://github.com/bigduu/Bodhi-AI/pull/118), Issue #117 | Confirmed merged at `1ff40d39`; six actual feature checks passed. The earlier inconsistent GitHub record resolved normally and remains in the history. Dev requires real Unit Tests and E2E Tests from GitHub Actions. |
| Bodhi frozen-source admission foundation | [Bodhi #123](https://github.com/bigduu/Bodhi-AI/pull/123), Issue #121, [#129](https://github.com/bigduu/Bodhi-AI/pull/129), Issue #127, [#133](https://github.com/bigduu/Bodhi-AI/pull/133), Issue #130 | Merged through protected dev with six actual checks for each head. Preserve physical original jobs, literal feature context, actual tested merge, accepted merge/tree and completion before merge. Accepted dev is `93ba85d0`; live read-only proof uses the original logs when GitHub's closed-PR association is empty. |
| Homebrew instructions and one candidate audit | [Homebrew tap #25](https://github.com/bigduu/homebrew-tap/pull/25), Issue #24 | Merged with five hosted checks passing. Main-push repetition is removed. Authenticated updater handoff has positive/negative policy coverage; the next real automatic candidate still supplies its own native evidence. |
| Bamboo frozen-source release controller and original outputs | [Bamboo #2318](https://github.com/bigduu/Bamboo-agent/issues/2318), children [#2323](https://github.com/bigduu/Bamboo-agent/issues/2323), [#2324](https://github.com/bigduu/Bamboo-agent/issues/2324), [#2325](https://github.com/bigduu/Bamboo-agent/issues/2325) | Pending complete feature/native evidence, source/controller admission, original binary checkpoints and verified crate archive reuse. |
| Lotus one final-version npm archive and main promotion | [Lotus Next #451](https://github.com/bigduu/lotus-next/issues/451), children [#454](https://github.com/bigduu/lotus-next/issues/454), [#455](https://github.com/bigduu/lotus-next/issues/455), [#456](https://github.com/bigduu/lotus-next/issues/456) | Source proof, actual archive durability and main/controller activation are separate bounded slices. Pending complete feature evidence; no new-contract producer is active. |
| Bodhi original installer checkpoints and main promotion | [Bodhi #119](https://github.com/bigduu/Bodhi-AI/issues/119), [#124](https://github.com/bigduu/Bodhi-AI/pull/124), [#131](https://github.com/bigduu/Bodhi-AI/pull/131), [#134](https://github.com/bigduu/Bodhi-AI/pull/134) | Checkpoint lifecycle and read-only admission merged. Dedicated bootstrap PR134 put exactly 11 accepted controls on main `19f11149`, preserving the prior product/dependency tree. Main now strictly requires the real Main Admission check from GitHub Actions. [Negative PR135](https://github.com/bigduu/Bodhi-AI/pull/135) was actually blocked by that check and closed without merge; no product test/build/publish ran. No new-contract production release or positive completed-release promotion has been exercised. |
| Bamboo Docker completed-asset consumption | [Bamboo #2320](https://github.com/bigduu/Bamboo-agent/issues/2320) | Pending. Await the final manifest and reuse published Linux x64 bytes; additional Linux ARM64 support has its own producer/ABI acceptance. |
| Bamboo Rustdoc producer and retained site deployment | [Bamboo #2321](https://github.com/bigduu/Bamboo-agent/issues/2321) | Pending. Distinct Rustdoc selections and main-push recompilation still need a single producer plus retained site artifact; do not describe the current main path as compiler-free. |
| Jiandu prebuilt formula dependency | Separate producer/consumer slice under #399 | Pending; the current source formula still compiles. Do not claim zero downstream compilation. |

Before cutover:

- Run one fixed migration baseline and route existing failures to focused feature
  owners. Resolve them before accepting new release cuts. Do not move that debt
  to a release suite or silently ignore it.
- Bootstrap missing dev branches from an explicitly recorded stable main. Keep
  existing PRs and their owners intact; do not silently retarget open work.
- Bamboo PR2322 restores main `d6325a73` ancestry by normal merge without a
  product-tree change. The PR is still unaccepted; require its actual feature
  acceptance before the first new cut. Do not weaken version-ordering proofs.
- Lotus/Bodhi keep trusted main controllers. Bootstrap reviewed workflow/docs
  control changes through a dedicated source-only PR, with feature validation and
  exact postimage proof, without promoting unrelated runtime code or publishing.
- Preserve active frozen sources, immutable tags and historical recovery. Lotus
  #445/version28 and the completed #449/version29 publication use the previous
  receipt contract. Preserve both original identities and recovery evidence;
  do not treat an older skipped Real Bamboo result as new-contract success.
- Only after the new producer and promotion proofs pass may the old automatic
  release test/build routes be retired. No new release cut uses the old
  release-repair-batch policy after activation.

Each implementation PR updates its `AGENTS.md`, local guide, YAML, routing/source
tests and activation evidence together. Local policy tests must execute positive
and negative event cases and reject failed/skipped/missing selected outcomes.
Verify that release contains one formal build/pack per output and no test
commands; main and downstream distributors contain no product compiler/build.
Production success and speed improvements require real original run evidence;
the migration itself does not authorize a production release dispatch.
