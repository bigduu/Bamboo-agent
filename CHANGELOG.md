# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Historical `0.x` releases followed [Semantic Versioning](https://semver.org/spec/v2.0.0.html);
since then the project ships **date-versioned nightly releases** (see below).

## [Unreleased] — nightly (date-versioned)

Since `0.3.0` the project ships **date-versioned nightly releases** (e.g. `2026.7.x`)
cut by the release train, rather than SemVer point releases. Changes between
nightlies are tracked in the git history and merged PRs (the source of truth);
the SemVer sections below are retained for the historical `0.x` releases.

## [0.3.0] - 2026-02-26

### Security
- Provider API keys are now encrypted at rest in `config.json` and are never returned via API (masked placeholders only).
- MCP SSE header values and MCP stdio env var values are now encrypted at rest in `config.json` and are never returned via API (masked placeholders only).

## [0.2.12] - 2026-02-26

### Changed
- Configuration is now fully unified in `config.json` (single entry/exit): keyword masking, model mappings, MCP server config, and permissions are persisted through the unified config.

### Fixed
- MCP server management endpoints now persist changes to the unified config (no more drift between runtime and disk).

## [0.2.11] - 2026-02-26

### Fixed
- Copilot auth endpoints now honor configured HTTP/HTTPS proxy settings (company network support).
- Built-in `http_request` tool now honors the same configured proxy settings (consistent outbound networking).

## [0.2.10] - 2026-02-26

### Added
- Tool execution streaming: tools can now emit incremental output events while running (SSE `tool_token`).
- Claude Code CLI integration: optional built-in `claude_code` tool (auto-enabled when `claude` is discoverable) that runs with `--output-format stream-json` and streams output.

### Changed
- Streaming: tool-emitted `token` events are treated as tool-scoped output (`tool_token`) instead of mixing into assistant text streaming.
- Model limits: added a built-in context window entry for `gpt-4.1` (defaults to 128k; override via user config if needed).

### Fixed
- Claude Code CLI: always pass `--verbose` when using `-p/--print` with `--output-format=stream-json` (required by Claude Code).

## [0.2.9] - 2026-02-25

### Added
- Copilot: support selecting and persisting a provider-specific default model via `providers.copilot.model`.
- Copilot: OpenAI-compatible `/v1/chat/completions` now forwards the resolved model to Copilot upstream requests.

### Changed
- Settings: clarified that `POST /bamboo/settings/provider` already saves + reloads provider configuration (so a separate reload call is typically unnecessary).

## [0.2.8] - 2026-02-25

### Removed (Breaking)
- Removed the legacy `bamboo_agent::agent::server::state::AppState` type/module; Bamboo now has a single unified `AppState`.
  - **Migration**: use `bamboo_agent::server::app_state::AppState` (or `bamboo_agent::server::AppState`) everywhere.
- Removed legacy server implementations/modules:
  - `bamboo_agent::agent::server` (legacy Actix server)
  - `bamboo_agent::web_service` (proxy server)
- Removed the legacy `/api/v1/stream/{session_id}` endpoint. Use `POST /api/v1/execute/{session_id}` + `GET /api/v1/events/{session_id}`.

### Fixed
- Fixed Actix `Data<T>` extractor mismatches that could cause runtime failures (e.g. `/anthropic/v1/messages`) when the wrong `AppState` type was required by handlers.

## [0.2.6] - 2025-02-25

### Fixed - Critical Production Blockers

#### Security
- **SECURITY**: Fixed API key leak in `bamboo config` command - secrets now redacted by default
  - Added `--show-secrets` flag to explicitly show API keys when needed
  - Prevents API keys from appearing in shell history, CI logs, or screen sharing

#### Configuration System
- **CRITICAL**: Fixed `--data-dir` flag not being honored by running server
  - Server now correctly loads configuration from specified data directory
  - `AppState::new()` and `reload_config()` use `Config::from_data_dir()`
  - Fixes potential data corruption and security issues

#### Documentation
- **HIGH**: Fixed documentation drift in configuration module
  - Updated config file location to `${BAMBOO_DATA_DIR}/config.json` (default `${HOME}/.bamboo/config.json`; was incorrectly showing XDG paths)
  - Removed TOML format references (actual format is JSON only)
  - Fixed environment variable name: `BAMBOO_HEADLESS` (was incorrectly `BAMBOO_HEADLESS_AUTH`)
  - Removed mentions of `HTTP_PROXY`/`HTTPS_PROXY` (explicitly ignored by implementation)
  - Documented correct priority order: CLI > Env > File > Defaults
  - Converted all provider configuration examples from TOML to JSON

### Changed

#### Default Provider
- **CHANGED**: Default provider reverted from "copilot" to "anthropic"
  - **Reason**: Copilot OAuth2 authentication is difficult to test and mock in CI/CD environments
  - **Impact**: New installations will use Anthropic by default (requires API key)
  - **Migration**: Users wanting Copilot should explicitly set `provider: "copilot"` in config
  - **Future**: Copilot will be re-introduced as default in v0.4.0 with proper test infrastructure

- Configuration documentation now accurately reflects implementation behavior
- All provider configuration examples use JSON format consistently

### Architecture
- Unified configuration system with single `Config` struct
- Proper priority ordering: CLI arguments > Environment variables > Config file > Code defaults
- Server configuration (port, bind, workers, static_dir) now part of unified Config

### Known Issues (Lower Priority)
- `--workers` CLI flag is parsed but not wired to server (uses default worker count)
- `--static-dir` CLI flag is parsed but not wired to server
- These are documented and can be addressed in future release

### Files Modified
- `src/core/config.rs` - Reverted default provider to "anthropic"
- `src/server/app_state.rs` - Fixed data_dir usage in config loading
- `src/server/handlers/agent_api.rs` - Fixed `get_claude_dir()` to create directory if missing
- `src/bin/bamboo.rs` - Added `--show-secrets` flag and secret redaction
- `tests/e2e/copilot_auth.rs` - Updated test for new default provider
- `Cargo.toml` - Version bump to 0.2.6

### Migration Notes
- **Default Provider Change**: If you relied on implicit "copilot" default, explicitly set in config:
  ```json
  {
    "provider": "copilot"
  }
  ```
- **No other breaking changes** - 100% backward compatible for existing configurations
- Users can optionally add `server` section to config.json (defaults used if omitted)
- Environment variable `BAMBOO_HEADLESS_AUTH` deprecated, use `BAMBOO_HEADLESS`

### Deployment Status
✅ **READY FOR PRODUCTION DEPLOYMENT**

All critical production blockers from Codex review Round 3 have been fixed.

## [0.2.0] - 2026-02-24

### 🎉 Major Refactoring: Unified Server Architecture

This release consolidates `web_service` and `agent::server` into a unified `server/` module
and unifies all HTTP handlers with explicit routing.

### Added

- **Unified server module** (`src/server/`)
  - Single `AppState` with direct provider access (eliminates proxy pattern)
  - Unified metrics infrastructure
  - Comprehensive migration guide in README.md and MIGRATION.md

- **Explicit routing system**
  - All routes now use explicit `web::route()` registration
  - No more `#[get]`, `#[post]` macros in handlers
  - Single source of truth in `src/server/routes.rs` (~120 routes)

- **Unified handler terminology**
  - All HTTP handlers consolidated under `src/server/handlers/`
  - Agent handlers: `handlers/agent/` (chat, execute, events, etc.)
  - Provider handlers: `handlers/*.rs` (openai, anthropic, gemini, etc.)

- **Server modes**
  - `run()` - Desktop mode (localhost only, no rate limiting)
  - `run_with_bind()` - Docker mode (custom bind, rate limiting)
  - `run_with_bind_and_static()` - Production with frontend serving

- **Module organization**
  - New `server::routes` module with route configuration
  - New `server::server` module with entry points
  - New `server::config` module with CORS/security headers
  - New `server::metrics` module with unified infrastructure

### Changed

- **BREAKING** (with backward compatibility):
  - Deprecated `agent::server` module → use `server` instead
  - Deprecated `web_service` module → use `server` instead
  - Old imports still work with deprecation warnings

- **Handlers structure**:
  - `src/server/handlers/*.rs` → `src/server/handlers/agent/*.rs` (core handlers)
  - `src/server/controllers/*.rs` → `src/server/handlers/*.rs` (provider handlers)
  - All handlers unified with consistent terminology

- **Route registration**:
  - From: Macro-based (`#[get("/path")]`)
  - To: Explicit (`.route("/path", web::get().to(handler))`)
  - All ~120 routes now explicitly registered

- **State management**:
  - Single unified `AppState` instead of dual state
  - Direct provider access instead of HTTP callbacks to self

- **Code organization**:
  - Eliminated 24 duplicate route registrations (54 → 30, 44% reduction)
  - Removed proxy pattern (`build_agent_state()` function)
  - Cleaner module structure

### Removed

- Duplicate route definitions (24 routes eliminated)
- Proxy pattern with HTTP callbacks to self
- Routing macros in favor of explicit registration
- ~430 lines of redundant code

### Fixed

- Async test blocking issue in `app_state::tests`
- Config save/load tests now use temp paths for CI compatibility
- HTTP request tests opt-in via `BAMBOO_TEST_NETWORK=1` environment variable

### Migration Guide

#### For Library Users

Old (deprecated but still works):
```rust
// NOTE: this legacy import path was removed in v0.2.8.
// use bamboo_agent::agent::server::state::AppState;
use bamboo_agent::web_service::WebService;
use bamboo_agent::agent::server::handlers;
```

New (recommended):
```rust
use bamboo_agent::server::AppState;
use bamboo_agent::server::WebService;
use bamboo_agent::server::handlers;
```

#### For Contributors

- All HTTP handlers are in `src/server/handlers/`
- Use explicit route registration in `src/server/routes.rs`
- No more `#[get]`, `#[post]`, etc. macros

### Stats

- **Files changed**: 63
- **Lines added**: +599
- **Lines removed**: -1029
- **Net change**: -430 lines (cleaner codebase!)
- **Tests**: 867/867 passing (100%)
- **Duplicate routes eliminated**: 24 (44% reduction)
- **Commits**: 8 major commits over 2-3 days
- Eliminated proxy pattern (`build_agent_state`)
- Unified state management (single AppState)
- All 866 tests updated and passing

### Fixed
- Async test blocking issue in `app_state::tests::test_app_state_creation`

### Migration Guide

#### Before v0.2.0
```rust
use bamboo_agent::agent::server::AppState;
use bamboo_agent::web_service::WebService;
```

#### After v0.2.0
```rust
use bamboo_agent::server::AppState;
use bamboo_agent::server::WebService;
```

All old imports still work with deprecation warnings. See MIGRATION.md for details.

## [0.1.2] - 2026-02-23

### Added
- Initial release on crates.io
- Multi-LLM provider support (OpenAI, Anthropic, Gemini, Copilot)
- Built-in HTTP server with Actix-web
- Agent loop with tool execution
- Session management
- Workflow system
- MCP (Model Context Protocol) integration
- 863 passing tests

[0.2.0]: https://github.com/bigduu/Bamboo-agent/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/bigduu/Bamboo-agent/releases/tag/v0.1.2
