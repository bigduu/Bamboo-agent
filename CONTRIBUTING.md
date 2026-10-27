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

Feature GitHub CI runs [one shared Unit/E2E executor](docs/testing/ci-test-blocks.md).
Native-platform and retained historical release jobs can be rerun independently.

For opt-in local groups, compile once per explicit input profile and retain
independent group evidence with the [grouped test controller](docs/testing/test-groups.md).
Its lightweight fixtures need only Node; it changes no CI or release gate.

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
- Dev CI requires disjoint Unit/E2E runtime success and normal review; fix
  failures in the feature branch before merging into dev
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

Read the complete local [delivery workflow](docs/delivery-workflow.md),
[Actions audit](docs/ci-workflow-audit.md) and
[Bamboo phase guide](docs/releases/ci-publication-flow.md), then follow
[Parallel Development](./gitflow.md) and [AGENTS.md](./AGENTS.md).

Feature branches own Unit/E2E. Every feature PR into `dev` runs required `Test`,
which aggregates its selected runtime and supporting owners on the actual merge
source. Before merge, owners must run affected tests locally and add or update coverage
for changed behavior, recording exact source, commands and results. Repair
relevant failures on the feature branch before normal protected-dev admission.
An integration branch is a finite acceptance slice, not a place to defer tests.

The train is `feature → dev → release/<40-character-cut-SHA> → main`.
Release branches do not rerun Unit/E2E or carry product repairs. Main promotion is read-only.
The release phase reserves the final version and produces each supported formal
output once. Main records stable code and immutable version/artifact references;
its push and promotion events perform no product tests, compilation or packaging.
A trusted controller dispatched from Main can build a separate frozen release
source; that is the release phase, not a second Main build.

### Activation checkpoint

The replacement entrypoints and Source/Main consumers are **staged**. At the
#2363 checkpoint, Bamboo's default branch is `dev`; stable Main is still
`d6325a732c7669f94962f604e4ce98966e374d73`. Its live legacy route has not been
replaced merely because a prepared branch or policy fixture passed. Combined
feature admission, complete producer registration, trusted controller installation,
Main protection/default cutover and real release/consumer acceptance remain
separate gates. The first new Source publication and first positive Main
promotion have not been established by these documentation changes.

Check [Zenith #399](https://github.com/bigduu/Zenith/issues/399), the phase guide
and fresh remote state before operating a release. Preserve currently required
checks, active queues and original evidence. Do not dispatch an inactive producer
or use this target policy to waive an existing gate.

### Freeze the release scope

1. Finish the feature's affected local checks, selected Unit/E2E and review before
   its normal PR enters protected `dev`. Keep the actual tested merge `M`, feature
   head/base, accepted Dev merge `S` and their exact trees distinct. New Source
   admission requires a confirmed accepted PR; a direct push or matching tree
   does not manufacture that record.
2. Select the accepted Dev cut, freshly read and record its exact SHA/tree, branch
   receipt and UTC cut time, and create `release/<S>` at that recorded commit.
   State the finite release scope and exact locked frontend identity. Ongoing
   development may advance Dev without changing this frozen source.
3. Keep `release/<S>` immutable. A product defect or failed relevant test returns
   to its feature owner, passes the affected checks into Dev, and requires a new
   recorded cut. Do not fix a frozen release, merge a broader integration head
   into it or collect a release test-repair batch. A pending producer/consumer
   identity must be accepted through feature/dev before selecting its new cut.

### Produce and retain formal outputs once

The Source controller `C` and frozen product `S` are separate checkouts and
receipt fields. Keep actual GitHub SHA/ref identities unchanged. Use only the
admitted producer and its existing version allocator; reserve the version before
formal compilation. Source manifests retain `0.0.0`; stamp the final version in
disposable publishing checkouts, without a post-build source JSON commit.

The release workflow builds/packages the standard three-platform binaries and
managed crate outputs with their explicit target/profile/toolchain/locked inputs.
See [Shared Bamboo executables](docs/releases/shared-executable-assets.md) for the
fixed platform and Host asset contracts.
A feature test executable and a final-version release binary are different
outputs; a cache hit is not original-output evidence. Do not add another Unit,
E2E, Rustdoc or frontend compiler path to release. Preserve distinct required
artifact verification and publication-isolation inputs.

Checkpoint each successful output's original run/job/physical attempt, artifact
ID, byte size and digest before finalization. Reuse successful siblings and exact
retained bytes on recovery. Classify a failed target before an infrastructure
retry; source repairs require feature/dev and a new cut. Missing or changed
previously successful originals stop recovery; they do not authorize rebuilding
or repackaging that output. Finalization joins and publishes verified originals.

### Promote the published source

Only a completed publication can supply a normal Main promotion.
The trusted Main-base reader checks the real source, controller, original jobs,
version/tag, manifests/assets and exact Git postimage. A locator, local fixture,
same-name green check or unconfirmed merge is not that proof. Record actual
Main merge identity separately from the source that was built. Main never
repeats product tests, compilation, packaging or publication after promotion.
Docs and distribution consumers use authenticated originals; they cannot
reconstruct missing bytes by running the source producer again.

### Validate a fixed snapshot

In the prepared entrypoints, manual CI is an explicit same-SHA feature/dev
diagnostic. It rejects Main, release refs, tags and unknown refs; it is not a
formal-release or publication gate. Record the actual source, configuration,
command and outcome. Standalone native/frontend/CodeQL diagnostics retain their
own explicit inputs and cannot replace selected feature evidence.

Historical workflows and recovery continue to use their original immutable
controller, source, receipts and permissions. Keep failed, cancelled, skipped
and unknown outcomes distinct. Do not rerun a failure merely to obtain green.

### Automatic main publication

This heading preserves old links. The new normal flow has no Main-CI publisher:
formal outputs belong to frozen-source release, and Main promotion reads their
completed evidence. The historical v8 policy is preserved in an
[immutable source snapshot](https://github.com/bigduu/Bamboo-agent/blob/ce89b4fcf4a45014cccb3bc7f817613ddcfa7eb2/CONTRIBUTING.md#release-process)
and hash-bound fixture data, not as a new release entrypoint.

### Manual and Zenith publication

Zenith is a policy/pointer wrapper, not a new cross-repository release producer.
The retained fixed historical recovery routes keep their own version/source and
credential boundaries. Never reinterpret an old manual skip, signed receipt or
unpublished v8 fixture as new Source authority. Use the phase guide for current
operator prerequisites and exact active entrypoints; this document grants no
credential, protection, publication or deployment change.

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

The following describes the prepared #2363 ownership. The activation checkpoint
above and phase guide distinguish it from still-installed legacy Main definitions.

- **CI**: Dev PRs own Unit/E2E, native/browser runtime, quality and retained Docs
  on the actual clean merge. Required summaries reject incomplete selected
  owners. Quick Checks owns Ubuntu Node policy once; Linux Cargo does not repeat
  that suite. Preserve distinct native OS, target and feature configurations.
- **Publish Crate**: the frozen-source release dispatcher uses trusted Main `C`,
  accepted `S`, one allocator, original outputs and one finalizer. It does not
  rerun product tests. Empty producer registries stop new cuts; local fixtures
  are not production publication evidence.
- **Main Admission**: the trusted-base reader verifies completed release or
  supported controller metadata. No candidate product code, test or compiler runs.
- **CodeQL**: feature quality owns all four language analyses with `build-mode:
  none`. The standalone entrypoint is explicit diagnosis, not an automatic Main
  copy or a Rust product build.
- **Documentation**: feature Docs owns the retained site. The Main consumer
  verifies and transfers original site bytes without Rustdoc or retarring.
- **Docker**: the consumer verifies completed binary/Host originals; its distinct
  ARM executable output and two runtime-image assembly/smoke configurations keep
  their named owners. No consumer rebuild fallback replaces lost originals.
- **Publication isolation and recovery**: keep their exact dummy-credential,
  ENOSPC or historical-original contracts. They are neither a second ordinary
  runtime suite nor permission to repair a frozen product source.

See [CI and publication flow](docs/releases/ci-publication-flow.md) for the full
entrypoint inventory, transport, cache, retry and credential prerequisites.

### Badge URLs

After workflows run, badges resolve to:

- CI: `https://github.com/bigduu/Bamboo-agent/actions/workflows/ci.yml`
- Documentation: `https://github.com/bigduu/Bamboo-agent/actions/workflows/docs.yml`
- GitHub Pages: `https://bigduu.github.io/Bamboo-agent/`
- docs.rs: `https://docs.rs/bamboo-agent` (built automatically after publishing to crates.io)

### Setup Checklist

1. Submit the feature PR to Dev and inspect its actual selected checks.
2. Before Pages or publication setup, verify the phase guide's accepted controls,
   original outputs and separate activation prerequisites.
3. Follow the separately reviewed controller/protection cutover; this document
   does not grant credential or production activation authority.
4. Verify original run/artifact evidence; a badge alone does not prove publication.

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
