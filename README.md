# Bamboo Agent 🎋

[![Crates.io](https://img.shields.io/crates/v/bamboo-agent.svg)](https://crates.io/crates/bamboo-agent)
[![Documentation](https://docs.rs/bamboo-agent/badge.svg)](https://docs.rs/bamboo-agent)
[![License](https://img.shields.io/crates/l/bamboo-agent.svg)](https://crates.io/crates/bamboo-agent)
[![Build Status](https://github.com/bigduu/Bamboo-agent/workflows/CI/badge.svg)](https://github.com/bigduu/Bamboo-agent/actions/workflows/ci.yml)
[![Documentation](https://github.com/bigduu/Bamboo-agent/workflows/Documentation/badge.svg)](https://github.com/bigduu/Bamboo-agent/actions/workflows/docs.yml)
[![Test Coverage](https://img.shields.io/badge/tests-867%20passing-brightgreen)](https://github.com/bigduu/Bamboo-agent)

**A fully self-contained AI agent backend framework with built-in web services.**

Bamboo provides everything you need to build, deploy, and scale AI-powered applications with support for multiple LLM providers, comprehensive tool execution, and production-ready infrastructure.

📖 **[Full Documentation](docs/README.md)** | 🚀 **[Getting Started](#quick-start)** | 📚 **[API Docs](https://docs.rs/bamboo-agent)**

## ✨ Features

- 🤖 **Complete Agent System**: Agent loop, tool execution, skill management
- 🌐 **Built-in HTTP Server**: Actix-web based API server with REST and streaming endpoints
- 🧠 **Multi-LLM Support**: OpenAI, Anthropic, Google Gemini, GitHub Copilot
- 📁 **XDG-Compliant**: Follows XDG Base Directory specification
- 🔧 **Dual Mode**: Binary (standalone server) or library (embedded)
- 🔐 **Production-Ready**: CORS, rate limiting, security headers built-in
- 🔄 **Session Management**: Persistent conversation history
- ⚡ **Workflow System**: Automate complex tasks with YAML workflows
- 🤝 **Claude Integration**: Seamless Claude Code binary discovery and management
- 🧪 **Well Tested**: 867 tests with 100% pass rate

## Installation

### From crates.io

```bash
cargo install bamboo-agent
```

### From source

```bash
git clone https://github.com/bigduu/Bamboo-agent
cd bamboo
cargo install --path .
```

## Quick Start

### Binary Mode

```bash
# Start server with default settings
bamboo serve

# Custom configuration
bamboo serve --port 9000 --bind 0.0.0.0 --data-dir /var/lib/bamboo
```

### Library Mode

```rust
use bamboo_agent::{BambooBuilder, BambooConfig};

#[tokio::main]
async fn main() {
    let server = BambooBuilder::new()
        .port(3000)
        .bind("0.0.0.0")
        .data_dir(std::path::PathBuf::from("/var/lib/myapp"))
        .build()
        .unwrap();

    server.start().await.unwrap();
}
```

## Configuration

Bamboo follows the [XDG Base Directory specification](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html).

### Default Paths

- **Config**: `$XDG_CONFIG_HOME/bamboo/config.json` (default: `~/.config/bamboo/`)
- **Data**: `$XDG_DATA_HOME/bamboo/` (default: `~/.local/share/bamboo/`)
- **Cache**: `$XDG_CACHE_HOME/bamboo/` (default: `~/.cache/bamboo/`)

### Configuration File

Edit `~/.config/bamboo/config.json`:

```json
{
  "server": {
    "port": 8080,
    "bind": "127.0.0.1",
    "workers": 10
  },
  "data_dir": "~/.local/share/bamboo"
}
```

### Environment Variables

Override configuration with environment variables:

- `BAMBOO_PORT`: Server port
- `BAMBOO_BIND`: Bind address
- `BAMBOO_DATA_DIR`: Data directory

## Migration Guide

### Migrating from v0.1.x to v0.2.0

Version 0.2.0 consolidates `web_service` and `agent::server` into a unified `server` module. If you were using the library API:

#### Before v0.2.0
```rust
use bamboo_agent::agent::server::state::AppState;
use bamboo_agent::agent::server::handlers;
use bamboo_agent::web_service::WebService;
use bamboo_agent::web_service::controllers::*;
```

#### After v0.2.0
```rust
use bamboo_agent::server::AppState;
use bamboo_agent::server::handlers;
use bamboo_agent::server::WebService;
use bamboo_agent::server::controllers::*;
```

All other code works without changes. The old import paths still work but will show deprecation warnings.

### What Changed

**Consolidated Modules:**
- `agent::server::handlers` → `server::handlers`
- `agent::server::state` → `server::app_state`
- `agent::server::workflow` → `server::workflow`
- `web_service::controllers` → `server::controllers`
- `web_service::services` → `server::services`

**Unified State Management:**
- Single `AppState` with direct provider access (no more proxy pattern)
- Consolidated route definitions (eliminated 24 duplicate routes)
- Unified metrics infrastructure

**Breaking Changes:**
- None (all old import paths still work with deprecation warnings)

### Benefits

- ✅ **No route duplication**: Single source of truth for 100+ routes
- ✅ **Direct provider access**: No HTTP callbacks to self
- ✅ **Clearer architecture**: One server module instead of two
- ✅ **Better performance**: Eliminated proxy pattern

## API Endpoints

Once running, Bamboo exposes the following endpoints:

### Health Check
```
GET /api/v1/health
```

### Chat Completions
```
POST /api/v1/chat/completions
```

### Agent Execution
```
POST /api/v1/agent/run
```

### Workflows
```
GET    /v1/workflows
POST   /v1/workflows
DELETE /v1/workflows/{name}
```

### Sessions
```
GET  /api/v1/sessions
POST /api/v1/sessions
```

## Development

### Build

```bash
cargo build
```

### Test

```bash
cargo test
```

### Run

```bash
cargo run -- serve
```

## Architecture

Bamboo is organized into the following modules:

- **`config`**: Configuration management with XDG support
- **`core`**: Core types and utilities
- **`agent`**: Agent system (loop, tools, skills, LLM providers)
- **`server`**: HTTP server and controllers
- **`process`**: Process management
- **`claude`**: Claude Code integration
- **`commands`**: Workflow, slash commands, keyword masking

## Documentation

- **[Full Documentation](docs/README.md)** - Comprehensive guides and references
- **[API Documentation](https://docs.rs/bamboo-agent)** - Auto-generated API docs
- **[Migration Guide](MIGRATION_GUIDE.md)** - Migrating from monorepo structure
- **[Contributing](CONTRIBUTING.md)** - How to contribute to Bamboo
- **[Changelog](CHANGELOG.md)** - Version history and release notes
- **[Security Policy](SECURITY.md)** - Security information and reporting

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please read our [Contributing Guidelines](CONTRIBUTING.md) before submitting PRs.

## Support

- **Issues**: https://github.com/bigduu/Bamboo-agent/issues
- **Discussions**: https://github.com/bigduu/Bamboo-agent/discussions
- **Security**: See [SECURITY.md](SECURITY.md) for reporting security issues

## Roadmap

- [ ] Complete agent system migration
- [ ] Full OpenAI/Anthropic API compatibility
- [ ] Webhook support
- [ ] Plugin system
- [ ] gRPC API
- [ ] Kubernetes deployment guides

---

**Made with ❤️ by the Bamboo Contributors**
