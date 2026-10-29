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

Follow the same [delivery workflow](docs/delivery-workflow.md) and
[CI and publication guide](docs/releases/ci-publication-flow.md) as every agent.

### Feature and dev

Open a feature PR to `dev`. Run and fix its Unit and E2E tests before merging.
Keep each test selection with one CI owner and reuse compatible build output;
separate operating systems or feature configurations may need separate output.
Record the actual source and results, and resolve review findings before merging.

### Freeze the release scope

After the relevant features are accepted into `dev`, create `release/<SHA>` at
that exact accepted commit. Keep the source fixed while the release is built.
Reserve an unused version and use the same version and source for the tag and
all published artifacts. Source manifests may keep `0.0.0`; stamp the final
version in the disposable publishing checkout.

### Build and publish

Build each required platform binary once, package the existing frontend and
publish the release artifacts. The release workflow does not rerun Unit or E2E.
Reuse successful original artifacts when retrying an infrastructure failure;
never rebuild a successful output merely because a different job needs it.
If product code or a test needs a fix, return to a feature branch, merge the fix
into `dev`, and cut a new release.

### Promote to main

After the release succeeds, review and merge its stable source into `main`.
Record the version, tag and published artifacts with the promotion. Main does
not run another product build or test suite.

### Validate a fixed snapshot

A manual test run is for diagnosis on the chosen feature source. It does not
replace the required feature PR checks or authorize a release.

### Automatic main publication

The previous Main-CI publisher is a historical release path. The new release
workflow owns publication; preserve existing tags and signed receipts for their
original recovery procedures. During the changeover, verify which workflow is
installed before dispatching a release.

### Manual and Zenith publication

Use Bamboo's existing release environment and publishing credentials only in
the release workflow. Zenith records the chosen revisions and versions; it does
not start a second Bamboo build. Do not put publishing credentials in feature
jobs or replace an immutable published artifact.

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

- Feature CI runs Unit/E2E and the applicable native-platform checks on a PR to
  `dev`, with one automatic owner for each test selection.
- The release workflow reserves the version, builds the required binaries and
  publishes their original artifacts once.
- Main keeps stable code and version information without repeating product
  tests or compilation. Documentation and distribution jobs reuse published or
  retained artifacts where available.

Read [CI and publication flow](docs/releases/ci-publication-flow.md) for commands,
frontend acceptance and recovery. Existing workflows stay in force until their
replacement has been merged and installed.

### Badge URLs

After workflows run, badges resolve to:

- CI: `https://github.com/bigduu/Bamboo-agent/actions/workflows/ci.yml`
- Documentation: `https://github.com/bigduu/Bamboo-agent/actions/workflows/docs.yml`
- GitHub Pages: `https://bigduu.github.io/Bamboo-agent/`
- docs.rs: `https://docs.rs/bamboo-agent` (built automatically after publishing to crates.io)

### Setup Checklist

1. Submit the feature PR to `dev` and check its Unit/E2E results.
2. Confirm the release workflow and existing `bamboo-release` environment are
   installed before cutting a release.
3. Verify the published version, tag and assets before promoting to `main`.

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
