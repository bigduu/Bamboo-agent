# Bamboo Documentation

Welcome to the Bamboo documentation! This directory contains comprehensive guides, development documentation, and archived materials.

## 📚 Documentation Structure

### For Users

- **[README.md](../README.md)** - Project overview and quick start guide
- **[MIGRATION_GUIDE.md](guides/MIGRATION_GUIDE.md)** - Guide for migrating from the monorepo structure
- **[CHANGELOG.md](../CHANGELOG.md)** - Version history and release notes
- **[CONTRIBUTING.md](../CONTRIBUTING.md)** - How to contribute to Bamboo
- **[CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md)** - Community standards
- **[SECURITY.md](../SECURITY.md)** - Security policy

### Guides

- **[API Documentation](guides/API.md)** - Complete API reference and usage examples
- **[Migration Guide](guides/MIGRATION_GUIDE.md)** - Guide for migrating from the monorepo structure
- **[Windows Troubleshooting](guides/WINDOWS_FIX.md)** - Platform-specific setup and troubleshooting for Windows
- **[GitHub Actions Setup](guides/GITHUB_ACTIONS_SETUP.md)** - Setting up CI/CD with GitHub Actions
- **[Auto-Publish Workflow](guides/AUTO_PUBLISH.md)** - Automatic version detection and publishing to crates.io

### Development

The `development/` directory is reserved for active development documentation and design documents.

### Archive

The `archive/` directory contains historical documentation from the project development process:

- **[archive/development/](archive/development/)** - Phase completion reports, progress tracking, test summaries, and milestone celebrations from the crate migration project

#### Archived Documents

**Development Progress & Milestones:**
- [Phase Completion Reports](archive/development/) - PHASE2 through PHASE6 completion documentation
- [Project Completion](archive/development/PROJECT_COMPLETE.md) - Final project completion report
- [Progress Reports](archive/development/PROGRESS_REPORT.md) - Overall progress tracking
- [Milestone Report](archive/development/MILESTONE_REPORT.md) - Key milestones achieved

**Documentation & Testing:**
- [Documentation Summary](archive/development/DOCUMENTATION_SUMMARY.md) - Documentation completeness tracking
- [Documentation Tasks](archive/development/DOCUMENTATION_TASKS.md) - Documentation task checklist
- [Documentation Reorganization](archive/development/DOCUMENTATION_REORG.md) - Docs restructuring notes
- [Test Coverage Report](archive/development/TEST_COVERAGE_REPORT.md) - Comprehensive test coverage analysis
- [E2E Test Summary](archive/development/E2E_TEST_SUMMARY.md) - End-to-end testing results

**Release & Celebration:**
- [Release 0.1.2 Summary](archive/development/RELEASE_0.1.2_SUMMARY.md) - Release notes for v0.1.2
- [Publication Success](archive/development/PUBLICATION_SUCCESS.md) - crates.io publication celebration
- [P0 Complete Celebration](archive/development/P0_COMPLETE_CELEBRATION.md) - Priority 0 milestone achievement

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

Bamboo has comprehensive test coverage with 867 tests:

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
- **Migrating from old version?** → [Migration Guide](guides/MIGRATION_GUIDE.md)
- **Need API reference?** → [API Documentation](guides/API.md)
- **Windows installation issues?** → [Windows Troubleshooting](guides/WINDOWS_FIX.md)
- **Setting up CI/CD?** → [GitHub Actions Guide](guides/GITHUB_ACTIONS_SETUP.md)
- **Viewing project history?** → [Development Archive](archive/development/)

---

**Last Updated**: 2026-02-24
**Maintainer**: Bamboo Contributors
