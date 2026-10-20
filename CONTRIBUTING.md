# Contributing to Bamboo

First off, thank you for considering contributing to Bamboo! It's people like you that make Bamboo such a great tool.

## Code of Conduct

This project and everyone participating in it is governed by the [Bamboo Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the issue list as you might find out that you don't need to create one. When you are creating a bug report, please include as many details as possible:

- **Use a clear and descriptive title**
- **Describe the exact steps to reproduce the problem**
- **Provide specific examples to demonstrate the steps**
- **Describe the behavior you observed and what behavior you expected**
- **Include logs and screenshots if helpful**
- **Specify your environment** (OS, Rust version, bamboo version)

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, include:

- **Use a clear and descriptive title**
- **Provide a detailed description of the suggested enhancement**
- **Explain why this enhancement would be useful**
- **List some examples of how it would be used**
- **Specify which module/component it affects**

### Pull Requests

- Fill in the required template
- Do not include issue numbers in the PR title
- Include screenshots and animated GIFs in your pull request whenever possible
- Follow the Rust code style guidelines
- Include tests for new functionality
- Update documentation for changed functionality
- End all files with a newline

## Development Setup

### Prerequisites

- Rust 1.95 or later
- Cargo
- Git

### Setting Up Your Development Environment

1. Fork and clone the repository:
   ```bash
   git clone https://github.com/YOUR_USERNAME/bamboo.git
   cd bamboo
   ```

2. Create a branch from the integration branch for your changes:
   ```bash
   git checkout dev
   git pull --ff-only
   git checkout -b feature/my-new-feature
   ```

3. Build the project:
   ```bash
   cargo build
   ```

4. Run tests:
   ```bash
   cargo test
   ```

5. Run the server:
   ```bash
   cargo run -- serve
   ```

### Running Tests

```bash
# Verify the complete workspace on the minimum supported Rust version
cargo +1.95.0 check --locked --workspace --all-targets --all-features

# Run all tests
cargo test

# Run specific test suite
cargo test --test server_integration

# Run tests with verbose output
cargo test -- --nocapture

# Run specific test
cargo test test_bamboo_config_default
```

### Code Style

We follow standard Rust conventions:

- Use `cargo fmt` to format your code
- Use `cargo clippy` to catch common mistakes
- Write documentation comments for public APIs
- Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)

### Commit Messages

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters or less
- Reference issues and pull requests liberally after the first line
- Consider starting the commit message with an applicable emoji:
  - 🎨 `:art:` when improving the format/structure of the code
  - 🐎 `:racehorse:` when improving performance
  - 🚱 `:non-potable_water:` when plugging memory leaks
  - 📝 `:memo:` when writing docs
  - 🐛 `:bug:` when fixing a bug
  - 🔥 `:fire:` when removing code or files
  - 💚 `:green_heart:` when fixing the CI build
  - ✅ `:white_check_mark:` when adding tests
  - 🔒 `:lock:` when dealing with security
  - ⬆️ `:arrow_up:` when upgrading dependencies
  - ⬇️ `:arrow_down:` when downgrading dependencies

### Project Structure

Bamboo uses a Cargo workspace with the following crates under `crates/`:

```
bamboo/
├── src/                    # Main crate (bamboo-agent root)
│   └── bin/bamboo.rs       # CLI binary entry point
├── crates/
│   ├── bamboo-agent-core/  # Agent runtime core, composition, storage, tools
│   ├── bamboo-compression/ # Context compression and summarization
│   ├── bamboo-domain/      # Domain types: sessions, tools, workflows, schedules, MCP
│   ├── bamboo-engine/      # Agent engine: MCP, metrics, runtime, skills
│   ├── bamboo-infrastructure/ # Config, LLM providers, process management, storage
│   ├── bamboo-memory/      # Memory system: durable memory, budget, Dream notebook
│   ├── bamboo-server/      # HTTP server, handlers, routes, app state
│   └── bamboo-tools/       # Tool registry, executor, orchestrator, built-in tools
├── tests/                  # Integration tests
├── Cargo.toml              # Workspace manifest
└── README.md
```

### Workspace Crate Responsibilities

| Crate | Responsibility |
|---|---|
| `bamboo-agent-core` | Agent system composition, workspace state, core agent types |
| `bamboo-compression` | Context compression, summarization, token limits |
| `bamboo-domain` | Domain types for sessions, tools, workflows, schedules, MCP config |
| `bamboo-engine` | Agent engine: MCP integration, metrics, runtime, skill execution |
| `bamboo-infrastructure` | Configuration management, LLM providers, process management, SQLite storage |
| `bamboo-memory` | Memory system, token budget management, Dream notebook |
| `bamboo-server` | HTTP server, request handlers, routes, session app state |
| `bamboo-tools` | Tool registry, executor, orchestrator, built-in tools, permission system |

### Module Guidelines

- Each module should have a clear responsibility
- Use `mod.rs` to re-export public APIs
- Document all public items
- Include unit tests within modules
- Keep module dependencies minimal

### Testing Guidelines

- Write tests for all new functionality
- Before merging to dev, run affected tests locally in the assigned executor
- Add or update regression coverage for changed behavior, including relevant
  normal and failure cases; record source SHA/tree, commands, selection and results
- Missing or failing relevant local tests block merge; compilation is not test evidence
- Dev CI requires successful compilation and normal review; complete build/test
  acceptance is mandatory through the frozen candidate's main promotion checks
- Use descriptive test names
- Include edge cases in tests
- Use `#[tokio::test]` for async tests
- Use `tempfile` for tests that need file system access
- On macOS, use `scripts/run-macos-server-lib-tests.sh` for the monolithic
  `bamboo-server` lib-test. The library package's directly linked test/example
  artifacts disable Apple's compact-unwind table because the crate's DWARF
  unwind records exceed that format's 16 MiB offset limit. DWARF unwinding and
  line tables remain enabled for panic backtraces and debugging. The flag has
  no final-link effect while creating the rlib and is not propagated to
  downstream dev/release binaries.

### Documentation Guidelines

- Update README.md if you change functionality
- Update API documentation with `///` comments
- Include examples in documentation
- Keep CHANGELOG.md updated
- Add inline comments for complex logic

## Release Process

The automatic publisher also provides the standard complete Bamboo executables
for standalone and desktop reuse. See [Shared Bamboo executables](docs/releases/shared-executable-assets.md)
for the four-platform assets, source/version provenance, browser host archive,
and consumer hash verification contract. These assets retain all release gates
described below.

Follow [Parallel Development](./gitflow.md) for branch ownership, bounded batches
and evidence. Batch integration and standalone PRs target `dev`. Its required
`Test` gate runs `cargo build --locked` in compile-only mode; `Quick Checks`
remains auxiliary.
Before merge, owners must run affected tests locally and add or update coverage
for changed behavior, recording exact source, commands and results. Dev CI does
not execute the full test suite for every PR. Complete validation runs on a
fixed batch for release; its failures remain visible and owned. Existing review
and security requirements still apply.

Keep the existing required `Test` check name. Its dev mode must actually compile
and propagate source-verification or Cargo failures; it must not be skipped.
A green dev `Test` means compilation passed, not that the full suite ran. The
same check retains full validation for release/main and explicit snapshots.
Main's required checks are unchanged.

A normal fast-forward direct push may be used once dev's actual branch rule
permits it, with the same compilation, local test/coverage and applicable review
requirements. The current compile job is PR-triggered; obtain valid candidate
checks through that existing path before direct admission, or keep the normal
PR route when exact-source equivalence is unproven. Do not force push or treat
removal of the PR requirement as permission to bypass required checks. See
[direct-push evidence](./gitflow.md#normal-direct-pushes-to-dev).

Each release follows
`dev → release/<40-character-cut-SHA> → main` in this repository. Never promote
the moving `dev` branch directly to `main`.

### Validate a fixed snapshot

For manual `CI` or `Frontend consumer acceptance` runs, select the intended
branch/tag and supply `snapshot_sha` as the full lowercase 40-character SHA.
It must equal that dispatch event's actual source SHA; mismatches fail closed.
Record the ref, exact SHA/tree, workflow run and result. Do not silently replace
it with a later moving `dev` head. Manual validation does not grant publication
authority.

CI on a main PR tests its actual synthetic merge result. The frontend workflow
also runs on main PRs and validates the exact PR head with its original
source-cleanliness, artifact and strict Clippy contracts. Preserve the two source
identities; evidence is reusable only if all relevant source and build/test
inputs match. An initial cut does not require an additional manual full suite
merely to repeat equivalent promotion validation. Repair `F` still requires the
separate exact-source acceptance below.
Ordinary dev PRs compile in CI but do not automatically run these full test suites.
Existing path-scoped Windows/adapter checks retain their own triggers.

### Freeze the release scope

1. After all changes and controls required for this release have normally merged
   into protected `dev`, freshly read its actual current HEAD immediately before
   creating the release branch. This is the cut; do not select an arbitrary
   historical ancestor. Record the cut's UTC timestamp, full lowercase
   40-character SHA, tree SHA, fresh branch-read receipt, and bounded release
   scope in the release PR. Create `release/<cut-SHA>` at that exact recorded SHA.
   Keep `dev` open for ongoing development after the cut.

   A bounded exception allows an early cut for an accepted backend checkpoint
   while a known pending frontend consumer awaits its immutable producer. All
   required backend changes and release controls must already have normally
   merged into protected `dev`. Use the same fresh actual-HEAD read and normal
   branch creation above; an old checkpoint is not a substitute for that read.
   At cut time, explicitly record the known pending consumer: its immutable
   producer ref, source revision, version and existing acceptance evidence,
   exact allowed consumer paths, remaining producer/consumer gates, and bounded
   release scope. Do not describe this known dependency as an unexpected fix.

   Keep one release owner and use an isolated release worktree. Other owners
   may normally advance `dev` immediately after the branch and cut receipt exist;
   they need not wait for the consumer or publication. After the producer's
   accepted publication identity and complete artifact hashes are verified,
   create the consumer's original head `F` from the frozen release. Apply the
   full separate `F` release-source and `U` protected-dev integration procedure
   in step 3, including exact-source review, Linux/macOS/Windows validation,
   preserved ancestry and normal admission before fast-forwarding only to `F`.
   Record the verified artifact hashes in `F`'s receipt. A pending or failed
   producer/consumer gate blocks release promotion and publication, not ongoing
   development. Never pull unrelated `dev` work into the release; a changed
   producer identity or expanded consumer scope requires explicit scope review
   through the normal process. This exception changes ordering only, not gates,
   protection, publication authority or the `Promotion Source` contract.
2. Open the release branch's PR into `main`. Freeze its scope: do not repeatedly
   merge the latest `dev` tip or unrelated features into the release to chase
   ongoing development. If a broader release is needed, explicitly choose and
   record a new cut and scope on a new release branch.
3. For a necessary release fix, branch from the frozen release head and keep
   the original fix head `F` within the recorded release scope. Record `F`'s
   exact SHA/tree and bounded diff as the intended release target; independently
   review and run the complete release-source gates on that exact `F`, including
   full Linux/macOS/Windows coverage. Preserve `F` unchanged while admitting it
   to protected `dev` (do not squash or rebase it into a different commit).
   If `dev` has advanced, use a separate integration head `U` containing both
   `F` and the current protected `dev` head. Keep `U` up to date as strict branch
   protection requires, and submit it through normal current-head/base review
   and required checks. Record `U`'s exact SHA/tree, review/check receipts, and
   normal admission into `dev`, with exact `F` → `U` → accepted-`dev` ancestry.
   Keep release-source and integration validation separate: `U`'s checks cannot
   replace `F`'s complete release-source checks, and `F`'s checks cannot replace
   `U`'s normal integration checks. Bind both receipts to their respective
   SHA/tree, even if the two heads coincide when no dev update is needed.
   Only after both pass and `F` is admitted through `U`, fast-forward the release
   branch to the recorded original fix head `F`; never substitute `U` or its
   dev merge result for recorded `F`. Review must reject any other repair target,
   even if its ancestry gate passes. Do not merge unrelated `dev` work into the
   release. A changed `F` requires fresh source evidence and normal admission.
   Pending or failing release-source or required integration checks block this
   route; fix and rerun them, never change
   the cut to evade a gate. Only if existing branch rules cannot preserve exact
   `F` ancestry, or the necessary repair cannot stay within the recorded scope,
   stop and explicitly agree a new cut/scope through the normal process.
   Never bypass protection.
4. The required `Promotion Source` gate verifies the actual same-repository
   release branch against the PR's exact head, proves cut → release-head
   ancestry, and proves release-head → protected-`dev` ancestry. Missing proof,
   source drift, or an unbackmerged fix fails closed. Later `dev` commits do not
   invalidate a frozen release. Static CI cannot prove the historical timing of
   branch creation or judge release scope. In particular, an admitted `U` can
   pass ancestry while containing unrelated `dev` work; this gate alone cannot
   authorize `U` as the release target. Review must verify the fresh cut receipt
   and actual normal branch creation, the recorded scope, and the separate `F`
   release-source and `U` integration receipts before accepting a repair.
5. Merge the release PR into `main` only after its current-head review, all
   required checks, the full Linux/macOS/Windows matrix, and normal branch
   protection pass. Main PR checks continue to test the synthetic merge result;
   manual CI can additionally validate the exact release source. A new head or
   changed base requires fresh review and checks, never relabeled old results.

Release source and version tags are immutable: never move or replace a frozen
tag. This process adds no publication or deployment authorization, credentials,
permissions, or protection exceptions. Existing publication prerequisites still
apply.

After the normal protected release-branch promotion, successful same-repository
main push CI queues publication of its exact tested commit to crates.io and a
GitHub Release. The workflow must first reach main through that normal promotion;
do not promote unrelated development solely to activate it. Source manifests
keep `0.0.0`; temporary publication stamps an unused `YYYY.M.N` UTC version and
embeds the committed frontend lock. Keep this successful-main-CI path as the
single automatic Bamboo publisher. Once Bamboo is published, downstream Zenith
runs should use `targets=bodhi`, verify the already-published Bamboo version and
source, and consume that release instead of dispatching a duplicate Bamboo
publication.

Main pushes also trigger documentation publication to public GitHub Pages.
GHCR image publication is triggered separately by `v*`/`bamboo-v*` tag pushes or
an explicit Docker workflow dispatch, not by a main push. Account for these
side effects under the existing authorization and release prerequisites; the
branch policy does not change their workflows.

Automatic runs and manual/Zenith dispatches using this workflow share one
`bamboo-crate-publication` queue without canceling queued uploads. Previously
frozen tags retain their own workflow definitions; this change does not
retroactively add the queue or new guards to those tags.

Before main activation, coordinate outstanding frozen-tag trains' explicit
versions and same-source resume through [Zenith #322](https://github.com/bigduu/Zenith/issues/322)
and [#370](https://github.com/bigduu/Zenith/issues/370), alongside the normal
promotion and Environment configuration. Their historical workflows cannot join
the new queue retroactively.

### Automatic main publication

Both production jobs use the `bamboo-release` Environment; only the automatic
job receives the GitHub/HMAC authority. Before
checkout, a fixed bootstrap verifies the successful same-repository main push CI
source and protected-dev controller history. It binds the server-recorded merged
release PR's exact head/base, two-parent merge and unchanged release-head tree,
cut → release-head → protected-dev ancestry, successful full pre-merge release CI
(including all three platform builds and Promotion Source), and successful main
push CI. Main push routing may skip platform builds already required before
merge. Jobs must complete strictly before the merge; failed-only reruns may
retain earlier successful job attempts. Missing, ambiguous, stale or failed
evidence stops before checkout or publication credentials. The main branch's
protection flag is not publication evidence. The existing exact-head review
remains responsible for cut scope and original F versus integration U; ancestry
alone cannot distinguish their roles. The
checkout uses the tested CI SHA, which may differ from the workflow's SHA.
Pre-policy main commits cannot acquire the new publication authority. Queued
sources must still equal fresh main before authorization, reservation, tag or
receipt changes, each crate publish attempt, release completion and each native
asset upload. API failures stop the write. Manual and historical recovery entry
contracts, signing authority and allocator remain unchanged.

An authenticated draft body reserves source, frontend and version before any
crate upload. Original frontend assets and each package checksum are preserved
for recovery. Every downloaded crate must match its reserved checksum, Git source
SHA and, for bamboo-server, original embedded frontend bytes. Only the complete
verified closure becomes a public GitHub Release. Reruns reuse the matching
source/frontend reservation; completed reruns verify without republishing.

Authenticated reservations and registry versions advance the full-u64 allocator;
unsigned names only occupy candidates. Verified registry/source ancestry prevents
stale sources overtaking newer versions or GitHub latest, even after Release
metadata deletion. Public target claims and copied receipts grant no recovery
authority; transport failures stop publication.

In that Environment, set `BAMBOO_RELEASE_TOKEN` with repository Contents/Workflows
write, `BAMBOO_RELEASE_SIGNING_KEY` to 32 random bytes as 64 lowercase hex digits,
and variable `BAMBOO_RELEASE_SIGNING_KEY_SHA256` to the SHA-256 of decoded key bytes.
Keep these names absent from repository/org configuration and the key stable
across token rotation. Missing/mismatched configuration stops publication; lost
keys require manual evidence verification, never accepting or re-signing unsigned
history. Tag authority is checked before upload.

### Manual and Zenith publication

Production manual dispatch retains the Cargo-only publication path on protected
dev/main or a protected annotated `bamboo-release-source-<40-character-source-SHA>`
tag. The tag suffix, required `expected_source_sha`, dispatched `GITHUB_SHA`,
workflow SHA and clean checkout HEAD must match. Before checkout, an inline
bootstrap verifies the exact workflow ref, protection status, a single annotated
tag object peeling directly to that commit, protected dev/main ancestry, and the
committed trusted-publication policy marker. Historical branch runs remain pinned
to their dispatched SHA even if the protected branch advances while queued.
Manual runs have Contents read and no GitHub release token or signing key. Only
the final publish step receives the Environment `CARGO_REGISTRY_TOKEN`.

Configure that Environment with exactly branch rules dev/main and a tag rule
`bamboo-release-source-*`. The tag namespace needs two active repository rulesets:
creation restricted with only RepositoryRole admin (actor_id 5) always-bypass,
and update/deletion restricted with no bypass actors. A protected-ref flag alone
does not prove these restrictions; verify their live configuration before use.
Create accepted tags only after the hardened publisher is merged into protected
history. Zenith separately accepts the new tag object, peeled source and matching
root pointer through its normal PR flow; old `bamboo-bodhi-source-*` tags cannot
be granted production authority by this workflow.

Before activation, provision the existing owner-controlled crates.io credential
in this Environment, verify its intended managed-crate publish scope, drain old
queued/running frozen publishers, and remove repository/org Cargo-token copies.
Old workflows must then fail for lack of credentials. This migration does not
revoke a potentially shared token globally. Secret-free dry runs do not prove
production token scope or real upload permission for the complete crate closure.

After merging the credential-check workflow into a protected branch, dispatch
`release-credentials-check.yml` there to verify the Environment GitHub and Cargo
authentication and signing-key fingerprint without checking out source or
uploading artifacts. This read-only check does not prove GitHub write scopes or
publish permissions for every managed crate; the actual authorized publication
must still verify those permissions and the resulting release identity.

Pass a real Cargo SemVer `version`; empty/`latest` uses the source manifest and
rejects `0.0.0`. Build metadata is unsupported. Stable versions cannot exceed the
current UTC year/month; historical versions, u64 counters and prereleases remain
valid. Inputs are parsed as environment data. Locked frontend and fixed legacy
rollback inputs remain available.

`dry_run=true` uses a separate job without an Environment and with publication
credential variables explicitly empty, including on feature refs. It validates
the exact dispatched source and stamped workspace buildability without uploads.
Manual publication retains the existing dependency-order, propagation,
rate-limit and skip-existing loop. It does not recover signed drafts, create
GitHub Releases, change latest or add provenance to historical manual skips.

Both publication paths use the isolated Cargo entrypoint delivered in
[#1752](https://github.com/bigduu/Bamboo-agent/issues/1752), described below.
The protected-ref credential migration is tracked in
[#1699](https://github.com/bigduu/Bamboo-agent/issues/1699); its operational gates
must finish before independent main publication is activated.

## Additional Notes

### Issue and Pull Request Labels

- `bug` - Something isn't working
- `enhancement` - New feature or request
- `documentation` - Improvements or additions to documentation
- `good first issue` - Good for newcomers
- `help wanted` - Extra attention is needed
- `wontfix` - This will not be worked on

## CI/CD Setup

### Existing Workflows

Bamboo uses GitHub Actions for continuous integration and publishing:

- **CI** (`.github/workflows/ci.yml`) -- Pull requests into `dev` run the required
  `Test` check in compile-only mode (`cargo build --locked`) and auxiliary
  `Quick Checks` (Node policy tests and Rust formatting). A compile pass does not
  replace the owner's
  pre-merge local affected-test and regression-coverage evidence. The same `Test`
  check runs the complete build/test job for fixed-snapshot manual validation,
  frozen release PRs into `main`, and main pushes; dev mode does not execute
  the full suite. Release/main and manual full-validation routes
  retain the comprehensive checks and all-feature `E2E Tests`. Same-repository
  frozen release PRs and manual full validation run the Linux/macOS/Windows
  release-build matrix. Main pushes retain their complete CI/publication queue.
  Required-check configuration is separate from these workflow triggers; verify
  the intended dev compilation gate and preserve all main requirements before declaring
  the policy active. PR cache cleanup remains scoped to that same-repository PR.
- **CodeQL** (`.github/workflows/codeql.yml`) -- Runs the Actions, JavaScript/TypeScript, Python, and Rust analyses for pull requests into `main`, pushes to `main`, and explicit manual dispatches. Routine `dev` activity does not run CodeQL.
- **Publish Crate** (`.github/workflows/publish-crate.yml`) -- Publishes successful main CI source commits to crates.io and GitHub Releases with verified same-source recovery. Manual/Zenith dispatches retain explicit versions, the exact locked frontend and `dry_run`.
  Its Cargo PATH entrypoint, including metadata invoked from Python, runs the
  digest-pinned official Rust 1.99.0 Bookworm image. Docker failure is fatal;
  publication never falls back to host Cargo. The entrypoint needs Node,
  Python 3.11+ and a local Unix Docker daemon, using an empty Docker client
  config so host proxy configuration cannot inject worker variables.
  Workers use a separate UID,
  private PID namespace, no capabilities, no-new-privileges, and a read-only
  root filesystem. Only disposable source and isolated Cargo/target data are
  mounted. The exact Git SHA, stamped manifests and staged frontend are copied
  without checkout credentials, host Git pointers or hooks. Workers cannot
  modify the original source/controller; only regular Cargo.lock and selected
  package archive bytes return after the container exits. Metadata paths map
  back to the original workspace and its target/package directory.
  GitHub/HMAC credentials never enter Cargo. Only `cargo publish` receives the
  registry token; build scripts during that operation can still read that
  intentional Cargo authority. Production token access is restricted through
  the #1699 Environment migration. The Release Cargo Isolation workflow proves the Linux ancestor
  boundary using dummy credentials and a real dependency build script, plus
  metadata/check/package/publish-dry-run; a macOS Docker smoke alone is weaker.
- **Publish Docker image** (`.github/workflows/docker-publish.yml`) -- Builds the multi-arch container image and pushes it to GHCR.
- **Documentation** (`.github/workflows/docs.yml`) -- Builds documentation on every push to main. Deploys to GitHub Pages.

### Badge URLs

After workflows run, badges resolve to:

- CI: `https://github.com/bigduu/Bamboo-agent/actions/workflows/ci.yml`
- Documentation: `https://github.com/bigduu/Bamboo-agent/actions/workflows/docs.yml`
- GitHub Pages: `https://bigduu.github.io/Bamboo-agent/`
- docs.rs: `https://docs.rs/bamboo-agent` (built automatically after publishing to crates.io)

### Setup Checklist

1. Push changes to GitHub to trigger CI.
2. Enable GitHub Pages: **Settings > Pages > Source** set to **GitHub Actions**.
3. Configure the protected `bamboo-release` Environment and its publication secrets as described above; keep Cargo-token copies absent from repository/org secrets.
4. Verify badge status in README after pushing.

## E2E Testing

```bash
# Run all e2e tests
cargo test --test e2e

# Run specific test
cargo test --test e2e test_health_endpoint
```

Tests cover all API endpoints (chat, execute, events, sessions, tasks, respond, metrics, MCP, health). Each test is isolated using actix-web's in-memory test framework.

### Adding E2E tests

1. Create `tests/e2e/new_endpoint.rs`
2. Use `create_test_app()` helper from `common`
3. Add the module to `tests/e2e/mod.rs`

## Questions?

Feel free to open an issue with the question label or start a discussion on GitHub.

---

Thank you for contributing!
