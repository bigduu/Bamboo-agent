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
- Ensure all tests pass before submitting PRs
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

Feature pull requests target `dev`. A normal protected `dev → main` promotion
starts comprehensive CI for its merge commit. After that exact `main` push CI
succeeds, Publish Crate automatically publishes the workspace dependency closure
to crates.io and creates a GitHub Release at the same commit. It selects an unused
`YYYY.M.N` UTC sequence after all existing crate versions, tags, and draft
reservations. Source manifests keep their `0.0.0` placeholders; the temporary
publishing checkout stamps the selected version and embeds the exact committed
frontend package. The workflow and release policy must first reach `main` through
a normal promotion; this change does not itself promote the existing dev backlog.

Automatic releases and manual/Zenith dispatches share one publication queue.
A draft Release reserves the version and stores canonical source/frontend
provenance atomically in its body before any crate upload. Its frontend assets
preserve the initial staged bytes for retries; restored manifest semantics and
ZIP payloads must still match the independently verified fixed npm package.
Each expected crate checksum is recorded before publishing, and the downloaded
crate must match that checksum,
its `.cargo_vcs_info.json` source SHA, and (for bamboo-server) the preserved
embedded frontend bytes. The GitHub Release becomes public only after every
crate is verified. Rerun a failed Publish Crate run to continue the same source
and version; a completed automatic CI rerun verifies existing artifacts without
publishing again or changing the latest release.
An older main CI recovery remains public without replacing a completed newer
main source as GitHub's latest; source ancestry takes precedence over the date
sequence allocated when each run first starts.
Before any crate upload, an older or unproven main source at or above an already
completed newer source version is rejected, preserving crates.io version order.

The existing manual workflow inputs remain available, including `dev` dispatches
from Zenith and the explicit fixed legacy frontend rollback. Pass an unused real
`version`; an occupied version can resume only with matching source/frontend
provenance. Historical releases without this provenance are rejected rather than
blindly skipped. `dry_run=true` performs local validation without creating any
draft, tag, or release asset. Manual releases do not replace the automatic main
release as GitHub's latest release.

Set `CARGO_REGISTRY_TOKEN` for crates.io and, for historical queued source with
workflow files different from the default `dev` branch, `BAMBOO_RELEASE_TOKEN`
with repository Contents write and Workflows write permissions. The workflow
otherwise uses `github.token`. Tag creation checks that authority before any
crate upload and rejects a tag pointing to different source. A token permission
failure leaves the candidate unpublished and requires configuration before rerun.

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

- **CI** (`.github/workflows/ci.yml`) -- Pull requests into `dev` run locked Rust build/test, formatting, and CI workflow policy checks in the required `Test` gate. Pull requests into `main`, pushes to `main`, and manual dispatches retain comprehensive validation, with the all-feature library and integration suite in the required `E2E Tests` job. Only promotion pull requests from this repository's `dev` branch into `main` add release builds on Linux, macOS, and Windows; manual dispatches also run that platform matrix. Linux TLS and frontend contract tests run in `Test`, while macOS and Windows run their platform-specific checks. Successful dev PR builds can reuse their own Rust cache until closure; `.github/workflows/pr-cache-cleanup.yml` then removes only that same-repository PR's merge-ref caches.
- **CodeQL** (`.github/workflows/codeql.yml`) -- Runs the Actions, JavaScript/TypeScript, Python, and Rust analyses for pull requests into `main`, pushes to `main`, and explicit manual dispatches. Routine `dev` activity does not run CodeQL.
- **Publish Crate** (`.github/workflows/publish-crate.yml`) -- Publishes successful main CI source commits to crates.io and GitHub Releases. Manual/Zenith dispatches retain explicit versions and the exact locked frontend; supports `dry_run` and verified same-source recovery.
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
3. Add `CARGO_REGISTRY_TOKEN` secret under **Settings > Secrets and variables > Actions** for crates.io publishing.
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
