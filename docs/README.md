# Bamboo Documentation

Welcome to the Bamboo documentation! This directory contains comprehensive guides, development documentation, and archived materials.

## 📚 Documentation Structure

### For Users

- **[README.md](../README.md)** - Project overview and quick start guide
- **[MIGRATION_GUIDE.md](../MIGRATION_GUIDE.md)** - Guide for migrating from the monorepo structure
- **[CHANGELOG.md](../CHANGELOG.md)** - Version history and release notes
- **[CONTRIBUTING.md](../CONTRIBUTING.md)** - How to contribute to Bamboo

### Guides

- **[GitHub Actions Setup](guides/GITHUB_ACTIONS_SETUP.md)** - Setting up CI/CD with GitHub Actions

### Development

The `development/` directory is reserved for active development documentation and design documents.

### Archive

The `archive/` directory contains historical documentation from the project development process:

- **[archive/development/](archive/development/)** - Phase completion reports and progress tracking from the crate migration project

## 🚀 Quick Links

### Getting Started

1. [Installation](../README.md#installation)
2. [Quick Start](../README.md#quick-start)
3. [Configuration](../README.md#configuration)

### API Reference

- **API Documentation**: https://docs.rs/bamboo-agent
- **Development Docs**: https://bigduu.github.io/Bamboo-agent/

### Project Information

- **Repository**: https://github.com/bigduu/Bamboo-agent
- **crates.io**: https://crates.io/crates/bamboo-agent
- **Issues**: https://github.com/bigduu/Bamboo-agent/issues

## 📖 Additional Resources

### Architecture

Bamboo is organized into the following modules:

- **config**: Configuration management with XDG support
- **core**: Core types and utilities
- **agent**: Agent system (loop, tools, skills, LLM providers)
- **server**: HTTP server and controllers
- **process**: Process management
- **claude**: Claude Code integration
- **commands**: Workflow, slash commands, keyword masking

See the [main README](../README.md#architecture) for more details.

### Testing

Bamboo has comprehensive test coverage with 806 tests:

```bash
# Run all tests
cargo test

# Run tests with verbose output
cargo test -- --nocapture

# Run specific test
cargo test test_name
```

### Building

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# Build documentation
cargo doc --no-deps --open
```

## 🤝 Contributing

We welcome contributions! Please see:

- [Contributing Guide](../CONTRIBUTING.md) - How to contribute
- [Code of Conduct](CODE_OF_CONDUCT.md) - Community standards
- [License](../LICENSE) - MIT License

## 📝 Document Organization Guidelines

When adding new documentation:

1. **User-facing guides** → `docs/guides/`
2. **Development notes** → `docs/development/`
3. **Historical records** → `docs/archive/`
4. **Project metadata** → Root directory (README, CHANGELOG, LICENSE, etc.)

## 🔍 Finding Information

- **Looking for setup instructions?** → [README.md](../README.md)
- **Need help contributing?** → [CONTRIBUTING.md](../CONTRIBUTING.md)
- **Want to see what changed?** → [CHANGELOG.md](../CHANGELOG.md)
- **Migrating from old version?** → [MIGRATION_GUIDE.md](../MIGRATION_GUIDE.md)
- **Setting up CI/CD?** → [GitHub Actions Guide](guides/GITHUB_ACTIONS_SETUP.md)

---

**Last Updated**: 2026-02-23
**Maintainer**: Bamboo Contributors
