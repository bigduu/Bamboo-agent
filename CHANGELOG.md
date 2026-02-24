# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-02-24

### Added
- **Comprehensive API Documentation** - 366/412 items documented (89% coverage)
  - All HTTP API endpoints with detailed documentation
  - Core type system with examples
  - Agent event system with SSE streaming
  - Tool registry and execution framework
  - Session management and conversation flow
  - LLM provider integration (OpenAI, Anthropic, Gemini, Copilot)
  - Agentic tool framework for autonomous execution
  - 20+ built-in tools with usage guides
  - Tool guide system with multi-language support
  - DSL composition builder for workflows
  - JSONL storage backend
  - Complete module-level documentation

### Documentation
- **API.md** - Complete API reference with all endpoints
- **DOCUMENTATION_SUMMARY.md** - Implementation overview
- **FINAL_DOCUMENTATION_REPORT.md** - Complete documentation report
- **100+ code examples** across all modules
- **~5,000 lines of documentation** added
- **843 HTML documentation files** generated

### Changed
- Updated to version 0.1.2
- All public APIs now have comprehensive documentation
- Improved code examples and usage patterns
- Enhanced module organization with clear overviews

### Quality
- 100% documentation coverage for P0-P4 priority items
- Production-ready documentation quality
- Ready for crates.io publication

## [0.1.1] - 2026-02-24

### Added
- **Auto-publish workflow** - Automatic version detection and crates.io publishing
- **Comprehensive documentation structure** - Organized docs/ directory with guides
- **CODE_OF_CONDUCT.md** - Contributor Covenant code of conduct
- **SECURITY.md** - Security policy and vulnerability reporting guide
- **Auto-publish guide** - Complete documentation for automated releases

### Changed
- **Documentation reorganization** - Moved development docs to archive, guides to dedicated folder
- **Improved code quality** - Fixed all clippy warnings across the codebase
- **Better path handling** - Changed `&PathBuf` to `&Path` for better idiomatic Rust

### Fixed
- **Windows compilation** - Fixed Windows-specific compilation errors
  - Removed winapi dependency, using std library instead
  - Platform-specific XDG_RUNTIME_DIR implementation
- **CI/CD workflows** - Updated GitHub Actions to use latest artifact versions (v4)
- **Code formatting** - Applied consistent formatting with cargo fmt
- **Lint warnings** - Resolved all clippy warnings for strict `-D warnings` mode

### Removed
- **Unused src/main.rs** - Project uses src/bin/bamboo.rs as binary entry point

### Security
- No security changes in this release

## [0.1.0] - 2026-02-23

### Added

#### Core Infrastructure
- XDG Base Directory specification support
- JSON-based configuration system
- Environment variable overrides
- BambooConfig with builder pattern
- Process registry for external process management

#### Agent System
- **Agent Core** (32 files)
  - Agent abstractions and types
  - Budget management
  - Composition engine
  - Memory system
  - Session storage
  - Tool execution framework
  - 23 unit tests passing

- **Agent LLM** (30 files)
  - OpenAI provider with streaming
  - Anthropic provider with tool support
  - Google Gemini provider
  - GitHub Copilot provider with OAuth
  - Protocol implementations (OpenAI, Anthropic, Gemini)
  - Provider factory pattern
  - Retry logic with middleware

- **Agent Tools** (36 files)
  - 24 built-in tool implementations
  - Tool registry with dynamic loading
  - Permission system
  - Guide system for tool documentation
  - Output manager for artifacts

- **Agent Metrics** (8 files)
  - Metrics collection
  - SQLite storage backend
  - Event bus
  - Worker threads
  - Aggregation support

- **Agent MCP** (13 files)
  - MCP protocol implementation
  - SSE and stdio transports
  - Server management

- **Agent Loop** (7 files)
  - Agent execution loop
  - Todo context
  - Stream handling
  - Budget management

- **Agent Server** (22 files)
  - 13 HTTP handlers
  - Workflow management
  - Server state
  - Metrics service

- **Agent Skill** (7 files)
  - Skill management
  - Built-in skills
  - Skill store

- **Agent CLI** (1 file)
  - CLI interface

#### Web Service Layer
- **Controllers** (11 files)
  - OpenAI-compatible API
  - Anthropic-compatible API
  - Gemini-compatible API
  - GitHub Copilot authentication
  - Tool execution endpoints
  - Settings management
  - Workflow CRUD

- **Services** (4 files)
  - Model mapping services
  - Skill loading service

- **Core** (5 files)
  - Actix-web server with CORS
  - Rate limiting (actix-governor)
  - Error handling
  - Configuration helpers
  - Provider hot-reload

#### Claude Integration
- **Binary Discovery** (4 files)
  - System-wide Claude binary discovery
  - Version comparison
  - Installation management
  - Command creation with environment

- **Commands** (5 files)
  - Slash command loading and parsing
  - Workflow save/delete
  - Keyword masking configuration
  - Clipboard utilities
  - Markdown with YAML frontmatter support

#### Testing
- 806 tests total
- 774 library unit tests
- 31 integration tests across 5 suites
- 1 documentation test
- 100% pass rate
- ~6 second execution time

#### Documentation
- Comprehensive README
- API documentation
- Module-level documentation
- Test documentation

### Changed
- N/A (Initial release)

### Deprecated
- N/A (Initial release)

### Removed
- N/A (Initial release)

### Fixed
- N/A (Initial release)

### Security
- Secure API key storage
- CORS configuration
- Rate limiting
- Input validation
- Path traversal protection

## [Unreleased]

### Added
- OpenTelemetry integration (planned)
- Plugin system (planned)
- WebSocket support (planned)
- GraphQL API (planned)

---

## Version History

- **0.1.0** - Initial release with complete agent system, web service, and Claude integration
