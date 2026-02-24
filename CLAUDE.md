# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Bamboo Agent is a fully self-contained AI agent backend framework built in Rust. It provides a complete backend system for AI agents with built-in HTTP server, multi-LLM provider support, and comprehensive tool execution capabilities.

**Key Characteristics:**
- Dual mode: Binary (standalone server) or library (embedded in applications)
- Production-ready with CORS, rate limiting, and security headers
- XDG-compliant configuration management
- Zero external runtime dependencies - everything runs locally

## Development Commands

### Building

```bash
# Development build (fast compile, slower runtime)
cargo build

# Release build (slower compile, fastest runtime)
cargo build --release

# Watch mode - auto-rebuild on changes
cargo watch -x build
```

### Running the Server

```bash
# Run in development mode with defaults (port 8080)
cargo run -- serve

# Custom port and bind address
cargo run -- serve --port 9000 --bind 0.0.0.0

# Custom data directory
cargo run -- serve --data-dir /path/to/data

# With debug logging
RUST_LOG=debug cargo run -- serve

# With trace logging (very verbose)
RUST_LOG=trace cargo run -- serve
```

### Testing

```bash
# Run all unit and integration tests
cargo test

# Run tests with verbose output
cargo test -- --nocapture

# Run specific test by name
cargo test test_name_here

# Run specific test file
cargo test --test server_integration

# Run E2E tests
cargo test --test e2e_tests --all-features

# Run integration tests only
cargo test --tests
```

### Code Quality

```bash
# Check formatting
cargo fmt --check

# Auto-fix formatting
cargo fmt

# Run linter (clippy)
cargo clippy

# Run clippy with all features and strict warnings
cargo clippy --all-features -- -D warnings

# Security audit
cargo audit
```

### Documentation

```bash
# Build documentation
cargo doc --no-deps

# Open documentation in browser
cargo doc --open
```

## Architecture

### Core Modules

The codebase is organized into these major modules:

**`src/core/`** - Core types and utilities
- Encryption, paths, todo tracking, keyword masking
- Foundation types used across the entire system

**`src/agent/`** - Complete AI agent framework
- **`core/`** - Agent implementation, conversation management, storage, memory, budget
- **`llm/`** - Multi-provider LLM abstraction (OpenAI, Anthropic, Gemini, Copilot)
- **`tools/`** - Built-in tool execution (20+ tools for file ops, git, commands, etc.)
- **`loop_module/`** - Agent execution loop
- **`mcp/`** - Model Context Protocol client for external tools
- **`skill/`** - Skill/prompt template management
- **`metrics/`** - Token usage and performance metrics collection

**`src/server/`** - Unified HTTP server and API layer
- **`app_state/`** - Unified state management with direct provider access
- **`handlers/`** - Agent API handlers (chat, execute, events, metrics)
- **`controllers/`** - Multi-provider API controllers (OpenAI, Anthropic, Gemini)
- **`services/`** - Business logic services
- **`workflow/`** - YAML/JSON workflow definition and execution
- **`routes/`** - Route configuration for all 100+ API endpoints

**`src/config/`** - XDG-compliant configuration management
- Loads from `~/.config/bamboo/config.json` (or TOML)
- Supports environment variable overrides

**`src/process/`** - Process lifecycle management
- Registration and tracking of running processes
- Graceful and forceful termination
- Live output capture for agent runs

**`src/claude/`** - Claude Code integration
- Binary discovery and version management

**`src/commands/`** - Command system
- Workflows, slash commands, keyword masking

**`src/bin/bamboo.rs`** - Binary entry point
- CLI using clap with `serve` and `config` subcommands

### Important Architecture Decisions

**Unified Server Module (v0.2.0+):**
- Version 0.2.0 consolidated `web_service` and `agent::server` into a single `server` module
- Eliminated proxy pattern - direct provider access without HTTP callbacks to self
- Unified state management with single `AppState`
- All imports should use `bamboo_agent::server::*` paths (old paths work with deprecation warnings)

**Provider System:**
- All LLM providers implement the `LLMProvider` trait in `src/agent/llm/provider.rs`
- Protocol adapters handle differences between provider APIs (Anthropic, OpenAI, Gemini)
- Factory function `create_provider()` instantiates providers from configuration

**Tool System:**
- Plugin-based architecture using `ToolRegistry` pattern
- All tools implement the `Tool` trait
- Permission system controls what actions tools can perform
- Output manager handles tool results and artifact references

**Session Management:**
- Conversations stored in JSONL format via `Storage` trait
- External memory system for conversation summarization
- Todo list tracking for task management

**Workflow Engine:**
- Workflows defined in YAML/JSON files in `~/.local/share/bamboo/workflows/`
- Supports sequential, parallel, and conditional execution
- Tool composition allows complex agent behaviors

**XDG Compliance:**
- Config: `$XDG_CONFIG_HOME/bamboo/config.json` (default: `~/.config/bamboo/`)
- Data: `$XDG_DATA_HOME/bamboo/` (default: `~/.local/share/bamboo/`)
- Cache: `$XDG_CACHE_HOME/bamboo/` (default: `~/.cache/bamboo/`)
- Runtime: `$XDG_RUNTIME_DIR/bamboo/` (default: `/tmp/bamboo-$UID/`)

## Configuration

### Configuration File Format

Configuration is stored in `~/.config/bamboo/config.json`:

```json
{
  "http_proxy": "",
  "https_proxy": "",
  "provider": "anthropic",
  "providers": {
    "anthropic": {
      "api_key": "sk-ant-...",
      "model": "claude-3-5-sonnet-20241022",
      "max_tokens": 4096
    },
    "openai": {
      "api_key": "sk-...",
      "model": "gpt-4"
    },
    "gemini": {
      "api_key": "...",
      "model": "gemini-2.0-flash-exp"
    },
    "copilot": {
      "model": "gpt-4"
    }
  }
}
```

Legacy TOML format is automatically migrated to JSON.

### Environment Variables

Higher priority than config file:
- `BAMBOO_PORT` - Server port (default: 8080)
- `BAMBOO_BIND` - Bind address (default: 127.0.0.1)
- `BAMBOO_DATA_DIR` - Data directory
- `BAMBOO_PROVIDER` - Default LLM provider
- `RUST_LOG` - Log level (debug, info, warn, trace)

## Testing Strategy

### Test Organization

- **Unit tests**: Located within modules using `#[cfg(test)]` and `#[test]`
- **Integration tests**: In `tests/` directory
  - `server_integration.rs` - Server lifecycle tests
  - `api_integration.rs` - API endpoint tests
  - `provider_integration.rs` - LLM provider tests
  - `workflow_integration.rs` - Workflow engine tests
  - `command_integration.rs` - Command system tests
  - `e2e/` - End-to-end tests
- **Doc tests**: Code examples in documentation comments

### Running Specific Test Suites

```bash
# Unit tests only
cargo test --lib

# Integration tests only
cargo test --tests

# E2E tests
cargo test --test e2e_tests

# Specific integration test file
cargo test --test server_integration
```

### Test Patterns

- Use `tempfile` crate for tests requiring file system access
- Use `wiremock` for HTTP mocking in provider tests
- Use `tokio::test` for async tests
- Tests are co-located with code in `#[cfg(test)]` modules

## API Structure

### Route Organization

**Agent Routes (`/api/v1/*`):**
- `/api/v1/health` - Health check
- `/api/v1/chat/completions` - Chat endpoint
- `/api/v1/agent/run` - Execute agent with tools
- `/api/v1/agent/events` - SSE event stream
- `/api/v1/sessions` - Session management
- `/api/v1/workflows` - Workflow CRUD
- `/api/v1/metrics/*` - Usage metrics
- `/api/v1/mcp/*` - MCP server management

**OpenAI-Compatible Routes (`/v1/*`):**
- `/v1/chat/completions` - OpenAI-compatible chat
- `/v1/models` - Model listing

**Anthropic Routes (`/anthropic/v1/*`):**
- `/anthropic/v1/messages` - Anthropic-compatible messages

**Gemini Routes (`/gemini/v1beta/*`):**
- `/gemini/v1beta/models/*` - Gemini-compatible endpoints

## Common Development Patterns

### Adding a New Tool

1. Create tool struct in `src/agent/tools/tools/`
2. Implement the `Tool` trait
3. Register in `src/agent/tools/tools/registry.rs`
4. Add to `BUILTIN_TOOL_NAMES` in `src/agent/tools/executor.rs`
5. Add unit tests in the same file
6. Add integration test in `tests/`

### Adding a New LLM Provider

1. Create provider module in `src/agent/llm/providers/`
2. Implement `LLMProvider` trait
3. Add protocol adapter in `src/agent/llm/protocol/` if needed
4. Register in `src/agent/llm/provider_factory.rs`
5. Add configuration support in `src/config/bamboo_config.rs`
6. Add tests using `wiremock`

### Adding a New API Endpoint

1. Create handler in appropriate `src/server/handlers/` subdirectory
2. Add route definition in `src/server/routes.rs`
3. Update API documentation in `docs/guides/API.md`
4. Add integration test in `tests/api_integration.rs`

## Migration Notes (v0.1.x → v0.2.0)

Version 0.2.0 consolidated `web_service` and `agent::server` into unified `server` module.

**Old imports (still work with deprecation warnings):**
```rust
use bamboo_agent::agent::server::state::AppState;
use bamboo_agent::web_service::WebService;
use bamboo_agent::web_service::controllers::*;
```

**New imports:**
```rust
use bamboo_agent::server::AppState;
use bamboo_agent::server::WebService;
use bamboo_agent::server::controllers::*;
```

Key changes:
- Eliminated 24 duplicate routes
- Removed HTTP callback proxy pattern
- Direct provider access through unified `AppState`
- All functionality preserved, cleaner architecture

## Performance Characteristics

- Startup time: < 100ms
- Memory usage: ~10-30MB base, scales with workload
- Supports 1000+ concurrent connections
- 10,000+ requests/second throughput (workload-dependent)
- < 10ms latency for local operations

## Deployment Modes

### Desktop Mode (Default)
```bash
bamboo serve
```
Binds to localhost only, no rate limiting. Perfect for local development.

### Docker Mode
```bash
bamboo serve --bind 0.0.0.0
```
Custom bind address with rate limiting enabled.

### Production Mode with Frontend
```bash
bamboo serve --bind 0.0.0.0 --static-dir ./dist
```
Serves static files alongside API.

## Commit Message Guidelines

Follow conventional commits with emoji prefixes:
- 🎨 `:art:` - Code format/structure improvements
- 🐎 `:racehorse:` - Performance improvements
- 📝 `:memo:` - Documentation
- 🐛 `:bug:` - Bug fixes
- 🔥 `:fire:` - Code/file removal
- 💚 `:green_heart:` - CI build fixes
- ✅ `:white_check_mark:` - Adding tests
- 🔒 `:lock:` - Security changes
- ⬆️ `:arrow_up:` - Dependency upgrades
- ⬇️ `:arrow_down:` - Dependency downgrades

## Key Dependencies

- **actix-web** - HTTP server framework
- **tokio** - Async runtime
- **reqwest** - HTTP client for LLM providers
- **serde/serde_json** - Serialization
- **rusqlite** - Metrics storage (SQLite)
- **tracing** - Logging and diagnostics
- **anyhow/thiserror** - Error handling
- **clap** - CLI argument parsing

## Important Notes

- The project uses Rust 2021 edition
- Minimum Rust version: 1.70+
- All code must pass `cargo clippy --all-features -- -D warnings`
- All code must be formatted with `cargo fmt`
- All public APIs must have documentation comments
- Tests are required for all new functionality
- The project has 867+ tests with 100% pass rate requirement
