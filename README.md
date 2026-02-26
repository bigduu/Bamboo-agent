# Bamboo Agent 🎋

[![Crates.io](https://img.shields.io/crates/v/bamboo-agent.svg)](https://crates.io/crates/bamboo-agent)
[![Documentation](https://docs.rs/bamboo-agent/badge.svg)](https://docs.rs/bamboo-agent)
[![License](https://img.shields.io/crates/l/bamboo-agent.svg)](https://crates.io/crates/bamboo-agent)
[![Build Status](https://github.com/bigduu/Bamboo-agent/workflows/CI/badge.svg)](https://github.com/bigduu/Bamboo-agent/actions/workflows/ci.yml)
[![Documentation](https://github.com/bigduu/Bamboo-agent/workflows/Documentation/badge.svg)](https://github.com/bigduu/Bamboo-agent/actions/workflows/docs.yml)
[![Test Coverage](https://img.shields.io/badge/tests-959%20passing-brightgreen)](https://github.com/bigduu/Bamboo-agent)
[![E2E Coverage](https://img.shields.io/badge/e2e%20coverage-100%25-brightgreen)](https://github.com/bigduu/Bamboo-agent)

**🚀 A Complete, Self-Contained AI Agent Backend Framework Built with Rust** 🦀

Bamboo is a **production-ready**, **high-performance** AI agent framework that runs **entirely locally** with zero external dependencies. Built from the ground up in **Rust** for maximum efficiency and safety, it provides everything you need to build, deploy, and scale AI-powered applications.

## 🎯 Why Bamboo?

- ⚡ **Blazingly Fast** - Native Rust performance with zero-cost abstractions
- 🔒 **Privacy-First** - Runs 100% locally, your data never leaves your machine
- 🎯 **All-in-One** - Complete agent system with built-in HTTP server, no microservices needed
- 🦀 **Rust-Native** - Leverages Rust's safety guarantees and async ecosystem
- 📦 **Zero Config** - Works out of the box with sensible defaults
- 🏭 **Production-Ready** - Battle-tested with 959+ tests, CORS, rate limiting, security headers

📖 **[Full Documentation](docs/README.md)** | 🚀 **[Getting Started](#quick-start)** | 📚 **[API Docs](https://docs.rs/bamboo-agent)**

## ✨ Key Features

### 🤖 Complete Agent System
- **Tool Execution** 🔧 - Execute shell commands, read/write files, search code
- **Skill Management** 🎯 - Create and manage reusable prompt templates
- **Workflow Engine** 🔄 - Automate complex tasks with YAML-defined workflows
- **Todo Tracking** ✅ - Built-in task management and progress tracking
- **External Memory** 🧠 - Automatic conversation summarization for long sessions

### 🌐 Built-in HTTP Server
- **Actix-web Powered** ⚡ - High-performance async HTTP server
- **REST API** 🔌 - Complete RESTful API for all operations
- **Streaming Support** 📡 - Real-time event streaming via Server-Sent Events
- **CORS & Security** 🔐 - Production-ready security headers and CORS configuration
- **Rate Limiting** 🛡️ - Built-in protection against abuse

### 🧠 Multi-LLM Provider Support
- **OpenAI** 💬 - Full support for GPT-4, GPT-3.5, and custom models
- **Anthropic** 🎭 - Claude 3.5 Sonnet, Claude 3 Opus, and more
- **Google Gemini** ✨ - Gemini 2.0 Flash and Pro models
- **GitHub Copilot** 👨‍💻 - OAuth device flow authentication with token caching

### ⚡ Performance & Efficiency
- **Native Rust** 🦀 - Zero-cost abstractions, no GC pauses
- **Async/Await** 🚀 - Tokio-based async runtime for maximum concurrency
- **Connection Pooling** 🔗 - Efficient HTTP connection reuse
- **Streaming** 📊 - Stream large responses without buffering
- **Memory Efficient** 💾 - Minimal allocations, efficient data structures

### 🏗️ Architecture
- **Modular Design** 🧩 - Clean separation of concerns
- **Dual Mode** 🎭 - Use as standalone binary or embedded library
- **XDG-Compliant** 📁 - Standard Linux directory layout
- **Hot Reload** 🔄 - Reload configuration without restart
- **Plugin System** 🔌 - MCP (Model Context Protocol) for external tools

### 🧪 Quality & Testing
- **959 Tests** ✅ - Comprehensive test coverage with 100% pass rate
- **175 E2E Tests** 🎯 - Complete API endpoint coverage (100%)
- **784 Unit Tests** 🧪 - Every module thoroughly tested
- **Integration Tests** 🔗 - End-to-end API testing
- **Documentation Tests** 📚 - Code examples verified in docs

### 🔒 Security & Privacy
- **Local-First** 🏠 - Everything runs on your machine
- **No Cloud Dependencies** ☁️ - Works offline, no API keys stored externally
- **Encrypted Storage** 🔐 - Sensitive data encrypted at rest
- **Keyword Masking** 🎭 - Automatically mask sensitive information

## 🚀 Installation & Quick Start

### 📦 Installation

#### Option 1: Install from crates.io (Recommended)

```bash
cargo install bamboo-agent
```

#### Option 2: Build from source

```bash
# Clone the repository
git clone https://github.com/bigduu/Bamboo-agent.git
cd Bamboo-agent

# Build in release mode for best performance
cargo build --release

# Install locally
cargo install --path .
```

### 🎯 Quick Start Guide

#### 🖥️ Binary Mode (Standalone Server)

```bash
# Start server with default settings on port 8080
bamboo serve

# Custom configuration
bamboo serve --port 9000 --bind 0.0.0.0 --data-dir /var/lib/bamboo

# Enable debug logging
RUST_LOG=debug bamboo serve
```

#### 📦 Library Mode (Embedded in Your App)

```rust
use bamboo_agent::{BambooBuilder, BambooConfig};

#[tokio::main]
async fn main() {
    // Build your custom server with fluent API
    let server = BambooBuilder::new()
        .port(3000)
        .bind("0.0.0.0")
        .data_dir(std::path::PathBuf::from("/var/lib/myapp"))
        .build()
        .unwrap();

    // Start the server (blocking)
    server.start().await.unwrap();
}
```

## ⚙️ Configuration

Bamboo uses a simple, unified directory structure for all configuration and data.

### 📁 Default Paths

All Bamboo data is stored under `~/.bamboo/`:
- **Config**: `~/.bamboo/config.json`
- **Data**: `~/.bamboo/` (sessions, skills, workflows, etc.)
- **Cache**: `~/.bamboo/cache/`
- **Runtime**: `~/.bamboo/runtime/`

You can override the data directory with the `BAMBOO_DATA_DIR` environment variable.

### 📝 Configuration File

Edit `~/.bamboo/config.json` (JSON format):

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
    }
  }
}
```

> **Note**: Legacy `config.toml` is no longer loaded. Please migrate to `config.json`.

### 🔧 Environment Variables

Override configuration with environment variables (higher priority than config file):

| Variable | Description | Example |
|----------|-------------|---------|
| `BAMBOO_PORT` | Server port | `9000` |
| `BAMBOO_BIND` | Bind address | `0.0.0.0` |
| `BAMBOO_DATA_DIR` | Data directory | `/var/lib/bamboo` |
| `BAMBOO_PROVIDER` | Default LLM provider | `anthropic` |
| `RUST_LOG` | Log level | `debug`, `info`, `warn` |

## Migration Guide

### Migrating from v0.1.x to v0.2.0

Version 0.2.0 consolidates `web_service` and `agent::server` into a unified `server` module. If you were using the library API:

#### Before v0.2.0
```rust
// NOTE: this legacy import path was removed in v0.2.8.
// use bamboo_agent::agent::server::state::AppState;
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

All other code works without changes. The legacy import paths were deprecated in v0.2.0 and removed in v0.2.8.

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

## 🔌 API Endpoints

Once running, Bamboo exposes a comprehensive REST API:

### 🏥 Health & Status

```bash
# Check server health
GET /api/v1/health
```

### 💬 Chat Completions

```bash
# OpenAI-compatible chat endpoint
POST /api/v1/chat/completions
Content-Type: application/json

{
  "model": "claude-3-5-sonnet-20241022",
  "messages": [
    {"role": "user", "content": "Hello, world!"}
  ],
  "stream": true
}
```

### 🤖 Agent Execution

```bash
# Execute agent with tools
POST /api/v1/agent/run
Content-Type: application/json

{
  "session_id": "my-session",
  "message": "Read the README.md file and summarize it"
}
```

### 🔄 Workflows

```bash
# List all workflows
GET /api/v1/workflows

# Create new workflow
POST /api/v1/workflows
Content-Type: application/json

{
  "name": "my-workflow",
  "description": "Automated task",
  "composition": {
    "type": "sequence",
    "steps": [...]
  }
}

# Delete workflow
DELETE /api/v1/workflows/{name}
```

### 📚 Sessions

```bash
# List all sessions
GET /api/v1/sessions

# Create new session
POST /api/v1/sessions
Content-Type: application/json

{
  "model": "claude-3-5-sonnet-20241022"
}
```

### 📊 Metrics & Monitoring

```bash
# Get usage metrics
GET /api/v1/metrics/summary

# Get session details
GET /api/v1/metrics/sessions/{session_id}
```

> 📖 **Full API Documentation**: See [API.md](docs/API.md) for complete endpoint reference

## 🛠️ Development

### 🔨 Build & Run

```bash
# Development build (fast compile, slower runtime)
cargo build

# Release build (slower compile, fastest runtime)
cargo build --release

# Run with auto-reload on code changes
cargo watch -x run -- serve

# Run tests
cargo test

# Run tests with coverage
cargo tarpaulin

# Check code formatting
cargo fmt --check

# Fix code formatting
cargo fmt

# Run linter
cargo clippy
```

### 🐛 Debugging

```bash
# Enable debug logging
RUST_LOG=debug cargo run -- serve

# Enable trace logging (very verbose)
RUST_LOG=trace cargo run -- serve

# Run specific test
cargo test test_agent_loop

# Run tests with output
cargo test -- --nocapture
```

## 🏗️ Architecture

Bamboo is built with a clean, modular architecture optimized for performance and maintainability:

```
┌─────────────────────────────────────────────────────────────┐
│                     Bamboo Agent 🎋                          │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   🌐 HTTP    │  │  🤖 Agent    │  │  🧠 LLM      │     │
│  │   Server     │  │   Loop       │  │  Providers   │     │
│  │              │  │              │  │              │     │
│  │  - Actix-web │  │  - Tools     │  │  - OpenAI    │     │
│  │  - REST API  │  │  - Skills    │  │  - Anthropic │     │
│  │  - SSE       │  │  - Workflow  │  │  - Gemini    │     │
│  └──────────────┘  └──────────────┘  │  - Copilot   │     │
│                                      └──────────────┘     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │  📊 Metrics  │  │  💾 Storage  │  │  🔌 MCP      │     │
│  │              │  │              │  │  Protocol    │     │
│  │  - SQLite    │  │  - JSONL     │  │              │     │
│  │  - Events    │  │  - Sessions  │  │  - Tools     │     │
│  │  - Analytics │  │  - History   │  │  - Servers   │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Core Modules

| Module | Description | Key Features |
|--------|-------------|--------------|
| **`config`** 📝 | Configuration management | XDG-compliant, hot-reload, multi-format |
| **`core`** 🎯 | Core types and utilities | Encryption, paths, todo tracking |
| **`agent`** 🤖 | Agent system | Loop execution, tools, skills, LLM providers |
| **`server`** 🌐 | HTTP server & controllers | REST API, streaming, handlers |
| **`process`** ⚙️ | Process management | Lifecycle tracking, output buffering |
| **`claude`** 🎭 | Claude Code integration | Binary discovery, version management |
| **`commands`** 📋 | Command system | Workflows, slash commands, keyword masking |

### Design Principles

- **🦀 Zero-Cost Abstractions** - Rust's performance guarantees
- **⚡ Async-First** - Tokio-based async runtime
- **🔒 Memory Safe** - No data races or buffer overflows
- **📦 Self-Contained** - No external runtime dependencies
- **🎯 Single Responsibility** - Each module has a clear purpose

## 📚 Documentation

| Resource | Description |
|----------|-------------|
| 📖 **[Full Documentation](docs/README.md)** | Comprehensive guides and tutorials |
| 📚 **[API Documentation](https://docs.rs/bamboo-agent)** | Auto-generated API docs (docs.rs) |
| 🔄 **[Migration Guide](MIGRATION_GUIDE.md)** | Upgrading from v0.1.x to v0.2.x |
| 🤝 **[Contributing](CONTRIBUTING.md)** | How to contribute to Bamboo |
| 📝 **[Changelog](CHANGELOG.md)** | Version history and release notes |
| 🔒 **[Security Policy](SECURITY.md)** | Security information and reporting |

## 📈 Performance

Bamboo is designed for maximum performance:

- **⚡ Startup Time**: < 100ms to fully operational
- **💾 Memory Usage**: ~10-30MB base, scales with workload
- **🔄 Concurrent Requests**: 1000+ concurrent connections
- **📊 Throughput**: 10,000+ requests/second (depends on workload)
- **🚀 Latency**: < 10ms for local operations

## 🗺️ Roadmap

### Current Version (v0.2.x) ✅
- [x] Complete agent system
- [x] Multi-LLM provider support
- [x] Workflow automation
- [x] MCP (Model Context Protocol) integration
- [x] Comprehensive metrics & monitoring

### Upcoming Features (v0.3.x) 🚧
- [ ] Webhook support for external integrations
- [ ] Plugin system for custom tool extensions
- [ ] gRPC API for high-performance clients
- [ ] WebSocket support for bidirectional streaming
- [ ] Built-in web UI dashboard

### Future Plans (v1.0+) 🌟
- [ ] Kubernetes deployment guides & Helm charts
- [ ] Distributed agent execution
- [ ] Advanced workflow visualizer
- [ ] Multi-tenant support
- [ ] Cloud deployment templates

---

## 📄 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

We love contributions! Whether you're fixing bugs, improving documentation, or proposing new features, your help is welcome.

**Getting Started:**
1. Read our [Contributing Guidelines](CONTRIBUTING.md)
2. Check out [Good First Issues](https://github.com/bigduu/Bamboo-agent/issues?q=is%3Aopen+is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
3. Fork the repo and create a feature branch
4. Submit a Pull Request

**Code of Conduct:** Be respectful, inclusive, and constructive. We're all here to build something great together!

## 💬 Support & Community

### 🐛 Bug Reports
Found a bug? [Open an issue](https://github.com/bigduu/Bamboo-agent/issues/new?template=bug_report.md)

### 💡 Feature Requests
Have an idea? [Start a discussion](https://github.com/bigduu/Bamboo-agent/discussions/new?category=ideas)

### 🔒 Security Issues
Found a security vulnerability? Please see [SECURITY.md](SECURITY.md) for responsible disclosure.

### 💬 Get Help
- **GitHub Discussions**: Ask questions and share knowledge
- **Documentation**: Check the [full docs](docs/README.md) first
- **Issues**: Search existing issues or create a new one

## 🌟 Star History

If you find Bamboo useful, please consider giving it a ⭐ star on GitHub! It helps the project grow and lets others discover it.

[![Star History Chart](https://api.star-history.com/svg?repos=bigduu/Bamboo-agent&type=Date)](https://star-history.com/#bigduu/Bamboo-agent&Date)

---

<p align="center">
  <strong>Made with ❤️ by the Bamboo Contributors</strong>
</p>

<p align="center">
  <sub>Built with 🦀 Rust • Powered by ☕ Coffee and 🎋 Bamboo</sub>
</p>
