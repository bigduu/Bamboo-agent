# Local grouped Rust tests

This local controller reduces the scope of a retry and supplies the shared
routing/list/count checks for [CI test blocks](ci-test-blocks.md). CI subdivides
each crate and target independently with a ten-minute cap; local thematic groups
are not the CI matrix. Follow the current owner of a shared build window.

`scripts/test-groups.cjs` uses Node 18+ standard library and existing Cargo/libtest.
Its Cargo JSON, executable hash, `--list` and runtime count checks follow the
existing `scripts/actor-windows-native.ps1`. That script keeps its own exact
41-case Windows acceptance contract; a generic platform group does not replace it.
No nextest installation or paid service is required.

## Inputs and compile boundaries

Choose one explicit build profile per evidence directory. Compile once for that
profile, then run groups directly from its recorded executables. A group run,
list, summary or retry never invokes Cargo. Another profile needs its own compile
receipt, while Cargo can reuse compatible artifacts in the owner's target/cache.
Do not copy `target` or race another writer of that target.

| Profile | Existing input preserved | Limits |
| --- | --- | --- |
| `default` | `cargo test --locked --no-run`, default-members and default features | Does not produce doctest artifacts or enable server `env_cache_isolation` |
| `all-features` | `cargo test --locked --all-features --lib --tests --no-run` | Separate E2E feature selection, includes required `test-utils` targets |
| `storage-standalone` | `cargo test --locked -p bamboo-storage --lib --no-run` | Retains standalone features; server-unified `storage/test-utils` is different evidence |
| `macos-server-dev` | `cargo test --locked --profile dev -p bamboo-server --all-features --lib --no-run` | macOS only; rejects `__eh_frame section too large` in compilation diagnostics |
| `ssh-live` | `cargo test --locked -p bamboo-broker --test russh_live --no-run` | Original standalone real SSH input, separately selected in its owned Docker fixture |

The ordinary default-member set excludes analytics and tool-event-recorder.
Profiles never silently replace that set with `--workspace`. The compile step
compares emitted test artifacts against `cargo metadata --no-deps` targets,
records disabled required-feature targets, and fails on a missing, duplicate or
unexpected artifact. An artifact containing zero tests is explicitly reported,
for example when its platform `cfg` disables all its tests. An entirely empty
profile fails.

Each receipt binds the exact clean SHA/tree and checkout, build command, actual
Cargo artifact features/profile, toolchain bytes/version, Node version, host OS
and architecture, runtime flags, runner/config, Cargo configuration files in the
checkout's ancestors and Cargo home, and a digest of the environment.
Environment values are not written to the receipt. Changed inputs require a new
receipt. There is no cross-SHA or cross-platform equivalence claim, even when
trees match. Review comments without input changes do not invalidate evidence.

The receipt hashes **all emitted executables**, including application binaries
referenced by `CARGO_BIN_EXE_*`, and discovered runtime shared libraries. It runs
each test in its Cargo package directory, supplies the package/manifest and
build-script `OUT_DIR` context, and reconstructs Cargo's dynamic-library search
directories. Original absolute executable paths remain valid and are not moved.
It rejects changed/missing bytes. Existing host system libraries, services and
external tools still belong to the original environment; this is not a hermetic
archive or a release-installation test.

## Groups and selection

The first thematic groups are `config-credentials`, `llm-retry`,
`engine-compression`, `session-permissions`, and `frontend-resources`.
`restart-takeover`, `cross-process`, `browser`, and `platform` are independent
expensive groups. `other` retains every remaining test. The configuration uses
actual package/target identities, with namespace routing inside shared lib-test
binaries; it does not assume each topic has its own Cargo target.

More specific routes precede broad package routes. Equal-priority conflicting
routes fail. Every enumerated test must appear in exactly one group, and the
combined group set must equal the full inventory. `other` is visible in the
final summary and must pass for completeness. A selected unknown group, a
duplicate group or a group with zero matches fails before runtime. Groups that
cannot exist in the chosen profile/platform are listed as unavailable.

Full and ignored-only `--list` inventories freeze the actual test names. Before
a group runs, its command filters are listed again and compared with the exact
frozen name set. Pure namespaces are batched; a substring collision uses an exact
test selection. This avoids both giant argument lists and one process per test
for normal namespace groups.

Default ignored behavior is unchanged: ignored tests are inventoried, remain
skipped, and are counted separately. `--ignored only` runs only ignored tests;
`--ignored include` runs them alongside regular tests. Both need an explicit
invocation and have separate result identities. Some ignored cases are child
fixture entrypoints or require real credentials/services; only select them with
their existing prerequisites and fixture contract. An ignored count is never
reported as an executed/passed test.

## Commands

List configuration or run the lightweight controller tests now; neither compiles Rust:

```sh
node scripts/test-groups.cjs groups
node --test scripts/test-groups.test.cjs
```

After the Rust window is available, use an **owned, new** evidence directory
outside tracked source, or inside the owner's ignored `target` directory:

```sh
node scripts/test-groups.cjs compile --profile default --evidence /tmp/bamboo-default-EVIDENCE_ID
node scripts/test-groups.cjs --list --evidence /tmp/bamboo-default-EVIDENCE_ID --all
node scripts/test-groups.cjs run --evidence /tmp/bamboo-default-EVIDENCE_ID --group config-credentials
node scripts/test-groups.cjs run --evidence /tmp/bamboo-default-EVIDENCE_ID --group llm-retry
node scripts/test-groups.cjs retry-failed --evidence /tmp/bamboo-default-EVIDENCE_ID
node scripts/test-groups.cjs summary --evidence /tmp/bamboo-default-EVIDENCE_ID
```

`run --all` explicitly selects every available group, including expensive groups.
Multiple `--group NAME` options select affected groups. A successful scoped `run`
exits zero for its requested groups while its summary still names all unrun or
failed groups. `summary` exits nonzero until **all** available groups have valid
passing results for that profile and ignored mode. `--list` validates selections
without creating runtime passes.

`retry-failed` reruns only groups whose latest runtime result failed under the
same receipt/mode, keeping every previous attempt and valid passing result. It
does not turn missing groups green. No automatic retry hides a real failure.
After a source/lock/feature/environment change, compile a new receipt and run the
affected groups. This first version deliberately does not import unaffected
results from a different SHA; proving that equivalence is a separate follow-up.

## Evidence, exits and coverage

The controller locks its evidence directory against a second controller writer.
It never replaces a compile directory, group attempt, process log or result.
`artifacts.json` contains input identity, hashes and both test inventories.
`runs/NNNNNN/GROUP/` retains preflight/runtime stdout, stderr and `result.json`.
Each result includes its selection key, timeout, counts, controller exit, actual
child exits/signals and timings. `runs/NNNNNN/summary.json` records the combined
state using the latest result for every group. Earlier failures remain readable.

Every group has a cumulative timeout, including its preflight lists, and runs
serially with `--test-threads=1` and `RUST_MIN_STACK=8388608`. These are local
runner settings, not a CI concurrency or scheduling policy. Nonzero process
exit, missing/incorrect counts, unexpected ignored counts, signal, timeout or
changed artifacts fails the group even if stdout claims success. Other selected
groups continue after ordinary failures. Cancellation stops new groups, kills
only the controller's own child process tree and retains received output.
Existing fixture-retained paths remain in their original logs; the runner does
not collect arbitrary config/session stores or upload artifacts.

`completeWithinSelection` covers the chosen ignored mode; `coverage` names the
full inventory, selected count, omitted regular tests in ignored-only mode, and
tests without accounted results. Ignored-only success cannot set
`completeWithinProfile` while regular tests are omitted. `completeWithinProfile`
requires the whole inventoried profile under its explicit ignored semantics;
`fullAcceptance` is always false. The summary lists coverage outside this controller:
doctests from bare `cargo test`, excluded workspace members, other feature or
platform profiles, examples, lint, external Node suites, real SSH, browser UI
acceptance, the native Windows contract, and packaging/install/publication tests.
Retain those existing validation paths and all actual promotion checks. These
local receipts do not create or transfer a required GitHub status.
