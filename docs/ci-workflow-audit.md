# GitHub Actions ownership audit

This is the file index for [delivery-flow/v1](delivery-workflow.md), tracked by
[Zenith #399](https://github.com/bigduu/Zenith/issues/399). Read that guide's
activation table before using a proposed path. Review of a file does not mean
its replacement is deployed or its production run has passed.

The audit covers every `.github/workflows/` file in these source snapshots:

| Repository | Reviewed source |
| --- | --- |
| Bamboo | Main `d6325a732c7669f94962f604e4ce98966e374d73` and feature PR2322 `339c997918a05789fb06fc407106728e64b36a41`; removals compared with dev `bf2db02f07625b8638706e16c075f35a2cf3d1e7`. |
| Lotus Next | Feature PR450 `55d1b7dc9d49eea09a79ea7667b121d7c4aca202`, with the existing historical publisher retained. |
| Bodhi | Feature `2d8bdb3ca5f008e7cd6cd5671643248c9e1da06a` and source-foundation PR123 `60c06ac580058ec15cbab45230715b4e57bd2eb6`; release/recovery compared with stable main `c556e0fc96e63fd7ede7b061e7d9117c1e2fc7d1`. |
| Homebrew tap | PR25 `05bd845f6a7d77fc8af975825db7f9ce9685e3f6`, merged with the same tree as main `c7be602e335321efd126277a9e5fdcd9bebb3c4b`. |

Source revisions identify the definitions reviewed here. Actual test snapshots,
controller revisions, accepted merges and original build attempts remain separate
identities in each repository's evidence. A later edit adds a fresh audit entry;
it does not retroactively change one of these snapshots.

Subsequent audit additions use Bamboo dev `fda1ec05abb6efa720c8af58176878cbfb46b2d8`
for `trusted-observer.yml` and `a64a852810b7104954e9fd1f60230df09b48eef2`
for `delegated-planner-acceptance.yml`, plus Bodhi accepted dev
`93ba85d0dad4c76cf2c12d099a7b3f12050bf3f6` and trusted main
`19f11149f5f1c19d07b80faec5b0ca88de74ba90`. These additions preserve the
earlier snapshots above; they do not substitute a new source for an old run.
The next addition uses Bamboo dev
`6cfe65e4624d6c0e86651187221b9fad861709a9` for
`observer-subscriptions.yml`. Its original feature-head recipe is reviewed
separately from its future canonical-merge owner.

## Bamboo

| File | Responsibility and review decision |
| --- | --- |
| `ci.yml` | Feature Unit/E2E owns one Linux target directory with separate default-package and narrow additive feature contexts. Keep actual default-only coverage. Legacy full validation/main admission remain until the release-controller cutover; they are not the new release-only path. PR2322 requires hosted runtime acceptance. |
| `frontend-acceptance.yml` | Tests embedded frontend packaging, build-script and server resource contracts, which feature CI also owns. The retained main/manual route still compiles under the old contract and must be retired at source-controller cutover; it must not become a second new-contract test gate. |
| `actor-snapshot-windows.yml` | Real Windows junction, process and executable behavior on Rust1.99/MSVC/API-only inputs. Issue2319 moves compatible automatic dev selections to one native owner while preserving default feature contexts and explicit historical/manual routes. |
| `ticket-platform-adapter.yml` | Windows fail-closed filesystem behavior and an explicit manual snapshot budget/analysis route. The Windows dev case joins Issue2319's native owner; the frozen diagnostic route is retained separately. |
| `root-birth-acceptance.yml` | Root-birth runtime selections are consolidated in feature CI. The remaining stricter Clippy analysis is a separate output, not another runtime suite. |
| `jiandu-project-source.yml` | Read-only source/registry identity. Its former runtime selections are consolidated in feature CI. |
| `orchestration-native-recovery.yml` | Removed by PR2322 after moving its runtime selections into the canonical feature owner. |
| `scoped-context-view.yml` | Removed by PR2322 after moving its runtime selections into the canonical feature owner. |
| `publish-crate.yml` | Existing single version allocator, native binaries and verified crate publication. Issues2323/2324/2325 separately cover accepted frozen-source admission, original native output checkpoints and original `.crate` reuse. The current happy path uses cheap `cargo package --no-verify` followed by one sandbox verification compile; whole-worker retries can repeat that verification. Do not describe it as two initial verification builds. |
| `recover-crate-release.yml` | Fixed historical crate source/reservation recovery. It may package and verify missing crates under its original sandbox contract; original archive reuse is still Issue2325. Keep old recovery parsers confined to their original frozen sources; they cannot admit a new-contract cut. |
| `docker-publish.yml` | Still recompiles Linux x64 and starts before a completed binary manifest. Issue2320 changes it to final-asset consumption. Linux ARM64 is an additional target requiring a real producer and ABI acceptance. The observed missing DuckDB source failure does not prove a bad image was published. |
| `docs.yml` | Rustdoc site producer and deployment. Its flags differ from CI Rustdoc, and main PR/push can compile again. Issue2321 must unify the documented output and retain the site bytes for deployment. |
| `codeql.yml` | Distinct security analysis for Actions, JS/TS, Python and Rust, using `build-mode: none`. It invokes no Rust product compiler. Retain the analysis result. |
| `release-cargo-isolation.yml` | Path-scoped dummy-credential/Cargo isolation and real ENOSPC acceptance, plus package-compatibility verification. These exercise the publication boundary, not the full product Unit/E2E suite; keep their distinct inputs explicit. |
| `release-credentials-check.yml` | Explicit operator credential check; no ordinary feature/release test producer. Retain without adding automatic credential access. |
| `claude.yml` | Explicitly mentioned issue/PR assistant. Not an automatic product test or release stage. Retain its existing authorization boundary. |
| `pr-cache-cleanup.yml` | Closed-PR cache lifecycle. No product build/test. Retain fork restrictions. |
| `trusted-observer.yml` | Added by accepted PR2313 after the original audit. Path-scoped Linux stable API-only runtime of domain/storage/server libraries plus stricter all-features library Clippy. It checks feature-head source rather than the canonical tested merge. Its API-only configuration and package graph are distinct from embedded default CI; migrate the owner explicitly before retiring this route. Do not claim that matching case names already establish deduplication or new-contract source evidence. |
| `delegated-planner-acceptance.yml` | Added by accepted PR2304 after the original audit. Linux stable API-only Plan library, immutable-artifact library and real CLI/restart cases, followed by all-features library/integration Clippy. It also checks feature-head source. Runtime/compiler overlap must be compared with the canonical runner using actual target/features/configuration; its required-child integration lint is an additional selection. It remains active pending a bounded ownership/source-context migration. |
| `observer-subscriptions.yml` | Added by accepted PR2316. Linux stable API-only joint domain/engine/server libraries, 19 serial cases, plus the same five-library all-features strict Clippy configuration as Trusted Observer. The joint engine/dev-dependency graph differs from standalone default CI. Issue2338 preserves both original joint graphs, consolidates the identical lint, and reuses the one overlapping serde case only with a current-M identical compiler unit/features/profile/path/hash and prior real PASS. Its original feature-head checks remain historical evidence until that migration passes hosted acceptance. |

Native TLS, Recorder and portable Skill selections are also embedded in
`ci.yml`. Issue2319 must compare actual target/features/profile/toolchain within
each OS, rather than assuming separate job names justify repeated compilation.
macOS dev-profile TLS unwind acceptance is distinct from test-profile runtime;
Windows API-only Rust1.99 Actor inputs are distinct from stable embedded inputs.
Coverage catalog counts are not native execution evidence.

Original PR2322 head `8a1de1ff` completed real canonical Linux Unit/E2E acceptance in
[run40441830591/1](https://github.com/bigduu/Bamboo-agent/actions/runs/40441830591):
actual tested merge `c5007d4c`, all nine selected jobs green, 31 default contexts,
serial/SSH and all 19 additive cases complete. This accepts that source only.
The refreshed `95e3b5d5` includes accepted subscription source `6cfe65e4` and
complete local documentation; its independent
[run40452299792/1](https://github.com/bigduu/Bamboo-agent/actions/runs/40452299792)
is still required. Dev now strictly requires Test, E2E Tests and the real
Feature Unit and E2E job from Actions app15368 during this cutover. Preserve the
earlier cancelled and partial RED runs; no runtime speedup is claimed.

Native ownership #2319, browser runtime #2330, quality #2331 and retained
Rustdoc #2321 have reviewed local candidates. Source foundation #2323, original
native/crate outputs #2324/#2325 and dispatch factory #2336 also have reviewed
local checkpoints. Whole-release wiring #2344 and the read-only public Main
proof #2345 remain separate; default complete-producer registries are empty.
No local fixture or prepared owner establishes new-contract publication, Main
promotion, Docker consumption or Pages deployment acceptance.

## Lotus Next

| File | Responsibility and review decision |
| --- | --- |
| `ci.yml` | Feature PRs into dev own supported Node Unit lanes, one Node24 dist, four complete browser shards and Real Bamboo using that dist. The feature aggregate rejects missing/skipped/failed selected jobs. No dev/main push repeat after feature admission. The packed-runtime network failure remains tracked in Issue453. |
| `actor-native.yml` | Called once by feature CI. Positive controls consume the same canonical dist; deliberately mutated frontend and backend negative controls require different source outputs. Keep those controls and their failure assertions. |
| `publish-npm.yml` | Old publication still builds final-version dist twice and transfers an identity instead of the actual archive. Issue451 must produce one final-version dist/tgz, retain it, publish those bytes and move all runtime suites to accepted feature evidence. Preserve the failed version28 candidate/run and the published version29 identity separately. |
| `verify-published-npm.yml` | Explicit read-only recovery for fixed published version2026.10.12. Verifies registry bytes and original evidence without building or republishing. Retain its narrow version/source scope. |

The fixed verification Chrome still needs Linux libraries and FFmpeg. Removing
an unused default browser download must preserve those prerequisites. A passing
instrumented diagnostic is not a replacement for the failed original acceptance
run or a deterministic fix.

## Bodhi

| File | Responsibility and review decision |
| --- | --- |
| `ci.yml` | Feature PRs into dev run Node contracts and real Unit/E2E on Linux x64, macOS ARM64 and Windows x64. Reuse the debug/test assembly and staged resources; no CI installers or standalone release shell build. Aggregate outcomes remain fail-closed. Main PRs execute the trusted-base read-only Main Admission check; main pushes do not repeat the product suites. Dev and main now require their respective real checks. |
| `release.yml` | Sole final-version desktop installer producer. Consume locked published Bamboo and Lotus, not their source builds. Merged Issue122 adds durable per-target original output checkpoints before finalization; source foundation121 and original feature context127/130 are merged. The accepted controller is installed on main through PR134. No new-contract production run has yet proved the positive publication/promotion path. |
| `recover-release.yml` | Reuses original installers/evidence and performs no compiler run. Keep the v1 frozen recovery contract; a new original-output contract must not fall back to it for newly admitted releases. |

`web:build` in package mode stages and verifies an existing Lotus package; its
name does not establish another Vite compilation. PR134's source-only bootstrap
preserved the prior main product/dependency tree and copied exactly 11 accepted
controls. The actual negative PR135 was rejected by the required Main Admission
check, then closed without merge. This is live negative admission evidence,
not a completed v2 release or a positive published-release promotion. Preserve
concurrent release/dependency PRs and their locks.

## Homebrew tap

| File | Responsibility and review decision |
| --- | --- |
| `check.yml` | Ordinary PR owns updater Unit plus native candidate checks. An automatic PR may reuse only a trusted-base verifier's authenticated original updater job/attempt, exact source/tree and successful required steps. No bot-name/prefix skip or success stub. Main pushes do not repeat the audit. |
| `check-tap.yml` | Shared real ARM64/Intel candidate installation, architecture, signing and runtime checks. Reuse mode verifies the original successful evidence; ordinary mode executes the checks. Do not wait on the updater's whole run when its merge job itself depends on acceptance. |
| `update-tap.yml` | Scheduled/manual prepared-candidate owner. Its original Unit and native results can authorize its generated PR; successful existing candidates are not audited again by PR/main. Existing no-change schedules still execute fast updater contract tests, a separate minor follow-up from product-build ownership. |

Jiandu's source formula still compiles during actual installation acceptance.
Zero downstream compilation requires a real prebuilt producer and a separately
verified formula migration; this review does not invent such artifacts.

## Historical Lotus

The additional read-only review of `bigduu/Lotus` uses exact source
`0478930c26b24e36c1cdddf49a9c4a0bfe709d3e`. Its canonical successor is Lotus Next;
the fixed legacy npm rollback identity does not authorize a new legacy release.
These older definitions are recorded as historical debt, not described as
converted or disabled by this migration.

| File | Remaining legacy behavior |
| --- | --- |
| `ci.yml` | Main PR/push both run Unit, type/lint checks and a frontend build. |
| `e2e-tests.yml` | Main PR/push build frontend again and compile a moving Bamboo dev backend for browser E2E. These outputs are not the locked artifacts used by the canonical delivery path. |
| `publish-npm.yml` | Explicit old publisher can repeat Unit; `npm pack --dry-run` and source `npm publish` each invoke `prepack: npm run build`. Its skip-tests/typecheck/error-string behavior is not the new source/artifact admission contract. |

Do not dispatch these legacy publishers or import their skip rules into Lotus
Next. Retiring legacy entrypoints is separate from preserving an already
published fixed rollback artifact.

## Continuation by another agent

Use the existing Issue/PR, exact checkpoint and original evidence. Read the
owning `AGENTS.md` and local delivery/test map, then check the shared activation
table. Do not create a second runner, repeat a successful suite, turn a failed
runtime into a skipped result, relabel a matching tree as a different tested
commit, or repair product code on a frozen release branch.
