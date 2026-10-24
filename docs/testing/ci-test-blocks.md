# CI test blocks

`CI` retains its existing dev, frozen snapshot, promotion and main-admission
routes. `Test` still compiles ordinary dev PRs. Full `Test` and `E2E Tests` are
aggregates over independently rerunnable blocks. Main authenticates completed
pre-merge evidence through the existing checkout-free admission; it does not run
another full suite. This change sets no new automatic trigger frequency.

## Build and runtime boundaries

| Lane | Build job | Exact original selection | Runtime |
| --- | --- | --- | --- |
| Test | `Test Build` | default-members, default features, `cargo test --locked --no-run` | one crate/target/topic/name shard per `Test Block` |
| Doctests | the same `Test Build` | actual Cargo rustdoc invocations and dependency graph | one crate per `Test Doctest` |
| E2E | `E2E Build` | default-members, `--all-features --lib --tests` | one crate/target/topic/name shard per `E2E Block` |
| Real SSH | `Test SSH Build` | standalone `-p bamboo-broker --test russh_live` | `Test SSH`, original Docker/SSH/SFTP fixture and explicit ignored test |

Default and all-feature evidence is independent. In particular, the server's
required-feature `env_cache_isolation` target is enabled only under the correct
input. Standalone storage and macOS optimized server/TLS retain their separate
local/native input profiles; a server-unified storage artifact does not replace
standalone storage evidence. Existing native Windows, TLS, recorder, portable
Skill, MSRV, lint, security, documentation and release-build checks remain.
Analytics remains outside default-members; its existing workspace MSRV check is
retained. Recorder runtime tests retain their separate three-platform jobs.

The build controller compares Cargo metadata with every emitted test artifact,
then inventories all test names and ignored names from the actual executables.
It checks that blocks cover every name exactly once. Every block contains one
crate and one Cargo target. Library topics split into sorted inventories of at
most 128 names; integration targets split at 16 names. The ten thematic routes
remain useful inside a crate; they do not combine multiple crates into a job.
Each Test/E2E lane spreads its complete runtime inventory evenly across two
matrix job families. Each matrix contains at most 256 independently timed jobs;
the lane supports up to 512 runtime blocks, without combining crates or targets.
Both families run at most one block concurrently, retaining two active blocks
per lane. Doctests retain their separate matrix with a 256-job limit. Crates with
zero matched tests, ambiguous routes and capacity overflow fail the build with
the complete diagnostic inventory. Nothing is silently omitted.

The second family may be skipped only when the authenticated plan contains zero
blocks for it. That structural skip contributes no coverage or passing count;
the aggregate and publisher require every planned block in both families to
finish successfully. The original Test/E2E aggregate names remain unchanged.

Doctest inventory uses a `RUSTDOC` wrapper only in the build job. Cargo supplies
the original argv, working directory, package environment, features and extern
graph; the wrapper adds `--list`, then inventories ignored cases separately.
Runtime invokes those captured rustdoc commands directly, retaining the same
dependency files and generated build outputs. It compiles doctest snippets as
rustdoc requires, without asking Cargo to rebuild the workspace. Empty doctest
targets remain recorded in the plan and are not counted as executed tests.

## Ten-minute execution limit

Every block job has `timeout-minutes: 10` and starts its cumulative clock before
checkout/setup. The controller uses the remaining budget and reserves 45 seconds
for evidence upload. Each actual child process receives a hard timer bounded by
that same deadline, including preflight lists and every test command. Splitting
commands inside a block never resets the clock. Timeout is a failed result.
Matrices use `fail-fast: false`; one failure does not cancel other blocks.

Results report build preparation, test preparation and test execution times
separately. A passing child exit alone is insufficient: names, executed/ignored
counts, process exits/signals and the 600-second ceiling must agree. Normal
ignored tests retain their original skip semantics and are counted separately.
The real SSH test alone retains its explicit ignored selection and prerequisite
fixture. There is no automatic retry, new ignore, or weakened assertion.

## Exact-input transport

The local JavaScript action uses the same official `@actions/artifact` client as
`actions/upload-artifact@v7`, pinned by npm lockfile. JavaScript action runtime
credentials stay in that action; they are not exported into shell steps. No
additional service, paid runner class, protection change or credential is added.

One build writes an immutable plan bound to the event SHA/tree, checkout layout,
profile, actual artifact features, toolchain hashes, environment/config digest,
OS/architecture/runner image and workflow run. It uploads support files once and
each test executable once, with separate doctest dependency transport. A block
downloads only its own target and required support. Archives preserve modes;
their names, IDs, member paths and hashes are verified before restoration.
Application binaries referenced by `CARGO_BIN_EXE_*`, dynamic libraries, package
cwd/env and generated `OUT_DIR` resources remain available at their original
absolute paths. Runtime uses no Cargo compile/cache lookup.

Compiled transport expires after one day; plans and diagnostic results retain
seven days. An expired or changed artifact fails explicitly. There is no paid
storage upgrade or implicit rebuild in a block. Recompile on changed inputs or
expired transport, then validate the new plan. No cross-SHA, cross-profile,
cross-platform or cross-run equivalence is assumed.

A failure before executable transport is uploaded leaves compiler/list logs and
inventory diagnostics, rather than reusable executable bytes. Replaying those
inventories needs no compilation, but cannot run the tests. GitHub reruns retain
the original workflow/source definition, so a workflow repair cannot be injected
into an old run. Preserve its independent results and caches; validate a repaired
source with its exact-input artifacts without relabeling earlier results.

## Reruns, aggregates and publication

Use GitHub's **Re-run failed jobs** or rerun one failed block. Successful blocks
and their actual earlier attempts remain valid under the same immutable plan.
The build does not rerun merely because another block failed. Aggregates read
GitHub `jobs?filter=latest`, find each exact planned job, and require the matching
attempt's result artifact. Missing, skipped, cancelled, failed, duplicated,
unexpected or wrong-plan results fail the aggregate. Old green artifacts cannot
hide a newer failure. Re-running a build creates a new plan; old aggregate or
block receipts cannot certify it.

The publisher independently authenticates each build plan, block result and
aggregate record in the real job logs, along with job status, source, actual
attempt membership and completion before merge. Its normalized producer hash,
helper digest and both loader pins change together. The reviewed historical
producer contracts remain explicit; they cannot claim the new shard contract.
Main's exact skipped-job set includes the new build/matrix jobs, while full
pre-merge acceptance requires their real successful execution.

The implementation baseline is protected-dev snapshot
`ff6866f8c378b1a1ee9dc66efb2e513e6d51d85b`. Release integration must preserve the
actual frozen candidate's product source and F/U ancestry. Review the infra-only
patch and recompute producer/helper pins for the final workflow definition;
do not substitute the broader dev tree or copy another candidate's test results.

Light validation:

```sh
node --test scripts/test-groups.test.cjs scripts/test-groups-ci.test.cjs
node --test scripts/ci-policy.test.cjs scripts/ci-routing.test.cjs
node --test scripts/crate-release.test.cjs scripts/release-source.test.cjs
```

Real Cargo inventory, artifact handoff and runtime evidence belongs to the first
grouped full CI run on the final selected source. Controller fixtures do not
claim that run already happened.

The compressed fixture `scripts/tests/fixtures/grouped-inventory-46b52ce8.json.gz`
captures the actual all-feature inventory from run `40048666734`, attempt 1,
artifact `12542483784`: 159 targets, 30 crates, 12,397 listed cases (38 ignored),
and 274 blocks. Its metadata file records hashes and provenance. Controller
fixtures replay all names into two 137-job matrices; publisher fixtures verify
the exact three-page job set and reject missing, repeated or skipped planned
blocks. This fixture proves inventory routing and evidence checks, not runtime
success for that source or any later source.
