# Bamboo Agent 🎋

[![Crates.io](https://img.shields.io/crates/v/bamboo-agent.svg)](https://crates.io/crates/bamboo-agent)
[![Documentation](https://docs.rs/bamboo-agent/badge.svg)](https://docs.rs/bamboo-agent)
[![License](https://img.shields.io/crates/l/bamboo-agent.svg)](https://crates.io/crates/bamboo-agent)
[![Build Status](https://github.com/bigduu/Bamboo-agent/workflows/CI/badge.svg)](https://github.com/bigduu/Bamboo-agent/actions/workflows/ci.yml)
[![Documentation](https://github.com/bigduu/Bamboo-agent/workflows/Documentation/badge.svg)](https://github.com/bigduu/Bamboo-agent/actions/workflows/docs.yml)
[![Test Coverage](https://img.shields.io/badge/tests-806%20passing-brightgreen)](https://github.com/bigduu/Bamboo-agent)

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
- 🧪 **Well Tested**: 806 tests with 100% pass rate

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
