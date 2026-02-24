# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-02-24

### Added
- Unified `server/` module consolidating `web_service` and `agent::server`
- Comprehensive migration guide in README.md and MIGRATION.md
- New `server::routes` module with single source of truth for all 100+ routes
- New `server::server` module with unified entry points (run, run_with_bind, WebService)
- Unified `AppState` with direct provider access (eliminates proxy pattern)

### Changed
- **BREAKING**: Deprecated `agent::server` module (use `server` instead)
- **BREAKING**: Deprecated `web_service` module (use `server` instead)
- Consolidated 54 route registrations → 30 (44% reduction)
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
