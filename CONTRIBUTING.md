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

### Documentation Guidelines

- Update README.md if you change functionality
- Update API documentation with `///` comments
- Include examples in documentation
- Keep CHANGELOG.md updated
- Add inline comments for complex logic

## Release Process

1. Update CHANGELOG.md with new version
2. Update version in Cargo.toml
3. Create a git tag: `git tag v0.x.0`
4. Push tag: `git push origin v0.x.0`
5. CI will automatically publish to crates.io

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

- **CI** (`.github/workflows/ci.yml`) -- Pull requests into `dev` run the Linux test suites plus `rustfmt` and Clippy gates. Only promotion pull requests from this repository's `dev` branch into `main` add release builds on Linux, macOS, and Windows. Manual dispatches also run the platform matrix. Documentation, `cargo-audit`, and `cargo-deny` remain part of CI.
- **Publish Crate** (`.github/workflows/publish-crate.yml`) -- Publishes the workspace crates to crates.io in dependency order. Normally dispatched by the Zenith release train with the unified date version and the `@bigduu/lotus` frontend version to embed; supports `dry_run`.
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
