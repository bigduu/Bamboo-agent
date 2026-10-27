# Bamboo Agent Guidance

Read [README.md](./README.md) for the product boundary. Use this canonical Jiandu memory contract:

Before CI or release work, read the complete local
[shared delivery workflow](docs/delivery-workflow.md),
[Actions ownership audit](docs/ci-workflow-audit.md) and
[Bamboo commands and test map](docs/releases/ci-publication-flow.md).
Every agent follows the same feature/dev/release/main workflow. The shared
review documents include historical snapshots; use the local guide and actual
workflow files for the current implementation and rollout status.

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
Windows unsupported-platform assertions. The `Browser Runtime Unit and E2E`
feature job owns all `browser-runtime` Node test files on Node 24; both feature
aggregates require it to pass. Do not add a second automatic owner for those
tests.
Keep the standalone default storage authority families' serial selection in that same
executor, reusing its compiled library-test executable without repeating cases.
`Quick Checks` remains auxiliary and never substitutes for runtime tests.

`trusted-observer.yml` is the single automatic owner for the two original
API-only Observer joint recipes, run sequentially in one target directory on the
actual PR head. Keep both package selections and their full behavior assertions.
The first recipe owns the shared supervisor-management serde case; the second
skips only that full case name and still selects `bamboo-domain` for its six
subscription windows. `observer-subscriptions.yml` retains the complete original
recipe as an explicit manual diagnostic on the selected workflow SHA. The shared
five-library Clippy check remains solely in `observer-api-lint.yml`; these API-only
contexts remain distinct from canonical embedded feature tests.

## Native feature ownership

#2319 gives Windows and macOS one feature job, workspace and owned target each.
`native-feature-tests` runs `node scripts/native-test-owner.cjs`; both required
`Test` and `E2E Tests` require that whole matrix to succeed. Never start a second
dev-PR Actor, Ticket, TLS or recorder producer. Their separate workflows/full
jobs are retained for explicit diagnosis or historical recovery.

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
Keep the results from the actual feature source and retain historical release
receipts. Skipped jobs and a cache hit do not establish test acceptance.

The intended release contract is feature tests -> accepted dev cut -> one version
reservation and release build/publication -> main promotion of the published
source. Release branches do not repair product code or rerun Unit/E2E; main
records stable code through immutable tags, Release manifests/provenance and the
promotion description, without another build or test run. Keep source manifests
at `0.0.0` and stamp only disposable formal-build checkouts. Repairs return
through feature/dev tests and produce a new frozen cut.

Use the [Release Process](./CONTRIBUTING.md#release-process) and
[CI and publication flow](./docs/releases/ci-publication-flow.md) for the same
workflow. During rollout, check the installed workflow before releasing and
preserve existing signed receipts and immutable tags.

## Pull Request Review

### Bamboo #791 integration exception

For the #791 multi-agent epic, the user's 2026-09-28 delivery instruction takes
precedence over the per-Issue PR and review steps below. Inventory all related
PR heads and existing work first, then merge implemented work into one local
integration baseline. Review integration changes locally and use focused checks
while assembling it. Submit one PR to `dev` with required Unit/E2E and local
test/coverage evidence under the current feature-admission policy. Complete
affected regression before dev admission; the target release path builds the
accepted product without repeating those tests. Do not request or wait for GitHub
Codex PR review for this epic. Close superseded component PRs and clean only
safely merged worktrees after final integration. Preserve dirty or uncertain
worktrees.

### General review

- Every PR needs an independent review of its current head and base. Any agent
  may perform that review using the same acceptance criteria and record its
  findings. Resolve applicable findings and review affected changes again after
  the head or base changes.
- Merge only after the required checks pass and unresolved review threads are
  addressed. Human approval is required when the user or branch protection asks
  for it. Use `review:agent` only after the independent review is complete.

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
