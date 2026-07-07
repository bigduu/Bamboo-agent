# Migration Guide

This guide helps you migrate from the old `agent-*` crates to the unified `bamboo-agent` crate.

## Overview

Bamboo is now organized as a Cargo workspace with the following crates under `crates/`:

- `bamboo-agent-core` -- Agent runtime core, composition, storage, tools
- `bamboo-compression` -- Context compression and summarization
- `bamboo-domain` -- Domain types: sessions, tools, workflows, schedules, MCP
- `bamboo-engine` -- Agent engine: MCP, metrics, runtime, skills
- `bamboo-infrastructure` -- Config, LLM providers, process management, storage
- `bamboo-memory` -- Memory system: durable memory, budget, Dream notebook
- `bamboo-server` -- HTTP server, handlers, routes, app state
- `bamboo-tools` -- Tool registry, executor, orchestrator, built-in tools

These replace the earlier monolithic `bamboo-agent` crate with internal modules such as `chat_core`, `agent-core`, `agent-llm`, `agent-tools`, `agent-metrics`, `agent-mcp`, `agent-loop`, `agent-server`, `agent-skill`, `agent-cli`, and `web_service`.

## Migration Steps

### 1. Update Cargo.toml

**Before:**
```toml
[dependencies]
chat_core = { path = "../chat_core" }
agent-core = { path = "../agent-core" }
agent-llm = { path = "../agent-llm" }
agent-tools = { path = "../agent-tools" }
web_service = { path = "../web_service" }
```

**After:**
```toml
[dependencies]
bamboo-agent = "2026.4"
# Or use individual workspace crates:
# bamboo-domain = { path = "../crates/bamboo-domain" }
# bamboo-server = { path = "../crates/bamboo-server" }
# bamboo-tools = { path = "../crates/bamboo-tools" }
```

### 2. Update Imports

#### Core Types

**Before:**
```rust
use chat_core::Config;
use chat_core::paths::bamboo_dir;
use chat_core::keyword_masking::KeywordMaskingConfig;
```

**After:**
```rust
use bamboo_infrastructure::config::Config;
use bamboo_domain::paths::bamboo_dir;
use bamboo_domain::keyword_masking::KeywordMaskingConfig;
```

#### Agent Types

**Before:**
```rust
use agent_core::{AgentError, Session, Message};
use agent_core::tools::{ToolCall, ToolResult, ToolExecutor};
```

**After:**
```rust
use bamboo_agent_core::agent::{AgentError, Session, Message};
use bamboo_tools::{ToolCall, ToolResult, ToolExecutor};
```

#### LLM Providers

**Before:**
```rust
use agent_llm::{LLMProvider, LLMError};
use agent_llm::providers::{OpenAIProvider, AnthropicProvider};
use agent_llm::create_provider;
```

**After:**
```rust
use bamboo_infrastructure::llm::{LLMProvider, LLMError};
use bamboo_infrastructure::llm::providers::{OpenAIProvider, AnthropicProvider};
use bamboo_infrastructure::llm::create_provider;
```

#### Tools

**Before:**
```rust
use agent_tools::{BuiltinToolExecutor, ToolRegistry};
use agent_tools::tools::ReadFileTool;
```

**After:**
```rust
use bamboo_tools::{BuiltinToolExecutor, ToolRegistry};
use bamboo_tools::tools::ReadFileTool;
```

#### Metrics

**Before:**
```rust
use agent_metrics::{MetricsBus, MetricsWorker};
```

**After:**
```rust
use bamboo_engine::metrics::{MetricsBus, MetricsWorker};
```

#### Web Service

**Before (v0.1.x):**
```rust
use bamboo::web_service::WebService;
use bamboo::web_service::controllers::agent_controller;
```

**After (v0.1.x):**
```rust
use bamboo::web_service::WebService;
use bamboo::web_service::controllers::agent_controller;
```

**Latest (v0.2.0+ / workspace):**
```rust
use bamboo_server::WebService;
use bamboo_server::handlers;
// Handlers are under crates/bamboo-server/src/handlers/
```

#### Claude Integration

**Before:**
```rust
// In src-tauri
use crate::claude::find_claude_binary;
use crate::command::slash_commands::SlashCommand;
use crate::command::workflows::save_workflow;
```

**After:**
```rust
use bamboo_server::claude_runner::find_claude_binary;
use bamboo_tools::slash_commands::SlashCommand;
use bamboo_server::workflow::save_workflow;
```

### 3. Update Function Calls

Most function calls remain the same, but some paths have changed:

#### Creating Providers

**Before:**
```rust
let provider = agent_llm::create_provider(&config)?;
```

**After:**
```rust
let provider = bamboo_infrastructure::llm::create_provider(&config)?;
```

#### Tool Execution

**Before:**
```rust
let executor = agent_tools::BuiltinToolExecutor::new();
let result = executor.execute(&tool_call).await;
```

**After:**
```rust
let executor = bamboo_tools::BuiltinToolExecutor::new();
let result = executor.execute(&tool_call).await;
```

### 4. Update Configuration

Bamboo now uses a unified data directory for all configuration and data:
- `BAMBOO_DATA_DIR` (default `${HOME}/.bamboo`)

**Before:**
```rust
let config_dir = dirs::home_dir().unwrap().join(".bamboo");
```

**After:**
```rust
let config_dir = bamboo_infrastructure::config::paths::bamboo_home();
let data_dir = bamboo_infrastructure::config::paths::bamboo_home();
```

You can also use the provided helper functions:

```rust
use bamboo_infrastructure::config::paths;

let config_path = paths::config_json_path();
let sessions_dir = paths::sessions_dir();
let workflows_dir = paths::workflows_dir();
```

### 5. Update Server Configuration

**Before:**
```rust
use web_service::WebService;

let server = WebService::new(
    data_dir,
    provider,
    config,
    metrics_bus,
);
```

**After:**
```rust
use bamboo_server::WebService;

let server = WebService::new(
    data_dir,
    provider,
    config,
    metrics_bus,
);
```

**Latest (workspace):**
```rust
use bamboo_server::app_state::AppState;
use bamboo_server::routes;

let app = bamboo_server::build_app(data_dir, config);
```

## API Changes

### ToolSchema Structure

**Before:**
```rust
let schema = ToolSchema {
    name: "read_file".to_string(),
    description: "...".to_string(),
    parameters: json!({}),
};
```

**After:**
```rust
let schema = ToolSchema {
    schema_type: "function".to_string(),
    function: FunctionSchema {
        name: "read_file".to_string(),
        description: "...".to_string(),
        parameters: json!({}),
    },
};
```

### KeywordEntry Fields

**Before:**
```rust
let entry = KeywordEntry {
    pattern: "secret".to_string(),
    mask_type: MaskType::Exact,
    replacement: "***".to_string(),
    case_sensitive: false,
};
```

**After:**
```rust
let entry = KeywordEntry {
    pattern: "secret".to_string(),
    match_type: MatchType::Exact,
    enabled: true,
};
```

## Common Migration Patterns

### Pattern 1: Using Prelude

Create a prelude module to simplify imports:

```rust
// src/prelude.rs
pub use bamboo_domain::session::{Session, Message, Role};
pub use bamboo_infrastructure::llm::LLMProvider;
pub use bamboo_tools::BuiltinToolExecutor;
pub use bamboo_infrastructure::config::Config;

// In your code
mod prelude;
use prelude::*;
```

### Pattern 2: Type Aliases

If you have many type references, create aliases:

```rust
type Provider = bamboo_infrastructure::llm::LLMProvider;
type Executor = bamboo_tools::BuiltinToolExecutor;
type Result<T> = std::result::Result<T, bamboo_agent_core::AgentError>;
```

### Pattern 3: Re-export Common Types

In your lib.rs:

```rust
pub use bamboo_agent_core::{
    Session, Message, AgentError,
};
pub use bamboo_infrastructure::config::Config;
```

## Testing Your Migration

1. **Run cargo check**: `cargo check`
2. **Run tests**: `cargo test`
3. **Check imports**: Look for any remaining old crate references
4. **Test functionality**: Ensure all features work as expected

## Troubleshooting

### Error: "cannot find type `Session` in crate `bamboo`"

**Solution**: Update your import paths. `Session` is now at `bamboo_domain::session::Session`.

### Error: "no field `name` on type `ToolSchema`"

**Solution**: `ToolSchema` now has a nested structure. Access the name via `schema.function.name`.

### Error: "unresolved import `chat_core`"

**Solution**: Replace all `chat_core` imports with `bamboo_domain` (domain types) or `bamboo_infrastructure` (config, llm).

### Error: "no module named `agent_loop`"

**Solution**: The loop module is now `bamboo_engine::runtime` (the engine crate handles agent runtime and execution).

## Additional Resources

- [API Documentation](https://docs.rs/bamboo-agent)
- [Repository](https://github.com/bigduu/Bamboo-agent)
- [GitHub Issues](https://github.com/bigduu/Bamboo-agent/issues)

## Getting Help

If you encounter issues during migration:

1. Check the [API documentation](https://docs.rs/bamboo-agent)
2. Search [existing issues](https://github.com/bigduu/Bamboo-agent/issues)
3. Open a new issue with the "migration" label
4. Start a [discussion](https://github.com/bigduu/Bamboo-agent/discussions)

## Changelog

See [CHANGELOG.md](../../CHANGELOG.md) for a complete list of changes.

## v0.2.0 Server Consolidation

Version 0.2.0 introduces a major refactoring that consolidates the dual server architecture into a unified module.

### Key Changes

1. **Workspace crates**: Monolithic `bamboo-agent` crate split into `crates/bamboo-server`, `crates/bamboo-domain`, etc.
2. **Explicit routing**: All routes registered in `crates/bamboo-server/src/routes/`
3. **Unified handlers**: Controllers and handlers merged into `bamboo_server::handlers`
4. **Direct provider access**: Eliminated proxy pattern with HTTP callbacks

### Migration from v0.1.x to v0.2.0

#### Server Imports

**Before (v0.1.x):**
```rust
// NOTE: this legacy import path was removed in v0.2.8.
// use bamboo::agent::server::state::AppState;
use bamboo::agent::server::handlers;
use bamboo::web_service::WebService;
use bamboo::web_service::controllers::*;
```

**After (v0.2.0+ / workspace):**
```rust
use bamboo_server::app_state::AppState;
use bamboo_server::handlers;
use bamboo_server::WebService;
// Note: controllers::* → handlers::*
```

#### Handler Organization

**Agent handlers** (under `crates/bamboo-server/src/handlers/agent/`):
- `chat`, `execute`, `events`, `stream`, `stop`, `history`, `respond`, `delete`, `health`, `metrics`, `todo`, `mcp`

**Provider handlers** (under `crates/bamboo-server/src/handlers/`):
- `openai/`, `anthropic/`, `gemini/`, `copilot_auth/`, `agent_api.rs`, `command/`, `settings/`, `skill/`, `tools/`, `workspace/`

### Backward Compatibility
Legacy import paths were deprecated in v0.2.0 and removed in v0.2.8.

```rust
// Old (removed in v0.2.8)
// use bamboo::agent::server::state::AppState;
// use bamboo::web_service::WebService;
// use bamboo::server::controllers::agent_api;

// Current (workspace crates)
use bamboo_server::app_state::AppState;
use bamboo_server::WebService;
use bamboo_server::handlers::agent_api;
```

### Benefits

- ✅ **No route duplication**: Single source of truth for all routes
- ✅ **Clearer architecture**: Workspace crate separation with clear boundaries
- ✅ **Better performance**: Direct provider access (no HTTP callbacks)
- **Easier maintenance**: All routes visible in `crates/bamboo-server/src/routes/`
- ✅ **-430 lines of code**: Cleaner, more maintainable codebase

### For More Details

See [CHANGELOG.md](../../CHANGELOG.md) for the full change history.

---

Need help? Open an issue or start a discussion on GitHub!
