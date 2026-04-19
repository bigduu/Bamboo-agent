# Bamboo Agent 🎋

[![Crates.io](https://img.shields.io/crates/v/bamboo-agent.svg)](https://crates.io/crates/bamboo-agent)
[![Documentation](https://docs.rs/bamboo-agent/badge.svg)](https://docs.rs/bamboo-agent)
[![License](https://img.shields.io/crates/l/bamboo-agent.svg)](https://crates.io/crates/bamboo-agent)
[![CI](https://github.com/bigduu/Bamboo-agent/actions/workflows/ci.yml/badge.svg)](https://github.com/bigduu/Bamboo-agent/actions/workflows/ci.yml)

<p align="center">
  <img src="./docs/assets/bamboo-agent-hero.svg" alt="Bamboo agent runtime overview" width="100%" />
</p>

**Bamboo is a local-first Rust runtime for agent systems.**  
It provides the execution core behind products that need structured context, tool orchestration, memory, task tracking, scheduling, MCP extensibility, and HTTP/SSE integration.

## Positioning

Bamboo is not primarily the end-user product layer.
It is the **runtime and backend foundation** behind products like Bodhi and other agent-enabled applications.

Use Bamboo when you need a system that is:

- **backend-first** rather than UI-first
- **embeddable or serviceable** rather than tied to one interface
- **structured and governable** rather than prompt-only
- **capable of longer-running work** with context, memory, and task state
- **fit for integration** into desktop, web, or internal tools

## What Bamboo provides

### Structured execution runtime
- multi-step agent loop
- built-in task list and execution state
- approval and clarification checkpoints
- final confirmation / handoff flow

### Tooling foundation
- file, search, edit, shell, and workspace tools
- tool usage guidance injected into runtime context
- policy-aware and approval-aware execution paths
- safe parallel execution support where applicable

### Context and memory system
- prompt section layering
- session-scoped note memory
- Dream notebook and durable memory support
- long-session compaction and oversized-output handling

### Integration surface
- HTTP APIs
- Server-Sent Events streaming
- standalone runtime mode
- embeddable Rust integration path
- MCP, skills, workflows, and schedules

## Why teams use Bamboo

### 1. Clear runtime ownership
Bamboo gives you an execution core you can understand, host, extend, and evolve.

### 2. Better structure for serious agent behavior
Instead of relying on one growing transcript, Bamboo organizes context, tasks, tools, memory, and runtime state more explicitly.

### 3. Safer execution boundaries
Approvals, explicit questions, task state, and guided tool usage make the runtime more governable.

### 4. Better fit for product integration
Bamboo can sit behind a desktop app, a web client, or an internal product surface without being tightly coupled to one UX.

## Compared with common agent stacks

| Common pattern | Bamboo approach |
|---|---|
| Product surface and runtime tightly coupled | **Backend-first runtime boundary** |
| Prompt-heavy orchestration | **Runtime-level tools, tasks, memory, and scheduling** |
| Hard to reuse across multiple clients | **Embeddable / serviceable integration model** |
| Long work collapses into transcript growth | **More structured context and memory handling** |
| Tool behavior is present but lightly governed | **Guide + policy + approval-aware execution model** |

## Quick start

### Install

```bash
cargo install bamboo-agent
```

Or build from source:

```bash
git clone https://github.com/bigduu/Bamboo-agent.git
cd Bamboo-agent
cargo build --release
cargo install --path .
```

### Run the server

```bash
bamboo serve
```

Default local paths:

- REST API: `http://localhost:9562/api/v1`
- Config path: `${HOME}/.bamboo/config.json`
- Data dir: `${HOME}/.bamboo/`

### Example configuration

```json
{
  "provider": "anthropic",
  "providers": {
    "anthropic": {
      "api_key": "sk-ant-...",
      "model": "claude-sonnet-4-20250514"
    }
  }
}
```

## Integration modes

### Run Bamboo as a backend
Use Bamboo behind a web app, desktop product, or internal agent platform.

### Embed Bamboo into your app
Use Bamboo as a Rust-native runtime inside your own application.

### Extend Bamboo
Add skills, MCP integrations, workflows, and schedules on top of the runtime core.

## Documentation

- [API reference](./docs/guides/API.md)
- [Migration guide](./docs/guides/MIGRATION_GUIDE.md)
- [CONTRIBUTING](./CONTRIBUTING.md)
- [CHANGELOG](./CHANGELOG.md)
- [SECURITY](./SECURITY.md)

## Development

### Auto-publish

Bamboo auto-publishes to crates.io when the version in `Cargo.toml` changes on `main`. To release:

1. Update version in `Cargo.toml`
2. Update `CHANGELOG.md`
3. Commit and push — CI handles tagging, GitHub Release, and crates.io publish

Requires `CARGO_REGISTRY_TOKEN` secret in GitHub repo settings.

### Docker

```bash
cd docker && docker compose up -d --build
curl http://localhost:9562/api/v1/health
```

Key env vars: `BAMBOO_DATA_DIR`, `BAMBOO_PORT`, `BAMBOO_BIND`, `BAMBOO_WORKERS`, `BAMBOO_CORS_ALLOW_ORIGINS`.

## When Bamboo is the right choice

Choose Bamboo if you want a runtime that is:

- **more structured than a single CLI loop**
- **more reusable than a one-off local workflow**
- **more governable than pure prompt orchestration**
- **better suited to power products behind the scenes**

If Bodhi is the AI product surface, Bamboo is the execution engine underneath.

## License

MIT
