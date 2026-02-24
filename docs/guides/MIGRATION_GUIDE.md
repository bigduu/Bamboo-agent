# Migration Guide

This guide helps you migrate from the old `agent-*` crates to the unified `bamboo-agent` crate.

## Overview

Bamboo consolidates the following crates into a single, cohesive package:

- `chat_core`
- `agent-core`
- `agent-llm`
- `agent-tools`
- `agent-metrics`
- `agent-mcp`
- `agent-loop`
- `agent-server`
- `agent-skill`
- `agent-cli`
- `web_service`

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
bamboo-agent = "0.1"
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
use bamboo::core::Config;
use bamboo::core::paths::bamboo_dir;
use bamboo::core::keyword_masking::KeywordMaskingConfig;
```

#### Agent Types

**Before:**
```rust
use agent_core::{AgentError, Session, Message};
use agent_core::tools::{ToolCall, ToolResult, ToolExecutor};
```

**After:**
```rust
use bamboo::agent::{AgentError, Session, Message};
use bamboo::agent::core::tools::{ToolCall, ToolResult, ToolExecutor};
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
use bamboo::agent::llm::{LLMProvider, LLMError};
use bamboo::agent::llm::providers::{OpenAIProvider, AnthropicProvider};
use bamboo::agent::llm::create_provider;
```

#### Tools

**Before:**
```rust
use agent_tools::{BuiltinToolExecutor, ToolRegistry};
use agent_tools::tools::ReadFileTool;
```

**After:**
```rust
use bamboo::agent::tools::{BuiltinToolExecutor, ToolRegistry};
use bamboo::agent::tools::tools::ReadFileTool;
```

#### Metrics

**Before:**
```rust
use agent_metrics::{MetricsBus, MetricsWorker};
```

**After:**
```rust
use bamboo::agent::metrics::{MetricsBus, MetricsWorker};
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

**Latest (v0.2.0+):**
```rust
use bamboo::server::WebService;
use bamboo::server::handlers;  // Unified handlers
// Note: controllers are now part of handlers module
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
use bamboo::claude::find_claude_binary;
use bamboo::commands::slash_commands::SlashCommand;
use bamboo::commands::workflows::save_workflow;
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
let provider = bamboo::agent::llm::create_provider(&config)?;
```

#### Tool Execution

**Before:**
```rust
let executor = agent_tools::BuiltinToolExecutor::new();
let result = executor.execute(&tool_call).await;
```

**After:**
```rust
let executor = bamboo::agent::tools::BuiltinToolExecutor::new();
let result = executor.execute(&tool_call).await;
```

### 4. Update Configuration

Bamboo now uses XDG Base Directory specification by default:

**Before:**
```rust
let config_dir = dirs::home_dir().unwrap().join(".bamboo");
```

**After:**
```rust
let config_dir = bamboo::config::xdg_paths::bamboo_config_dir();
let data_dir = bamboo::config::xdg_paths::bamboo_data_dir();
```

You can also use the provided helper functions:

```rust
use bamboo::core::paths;

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
use bamboo::{BambooBuilder, BambooConfig};

let config = BambooConfig::default();
let server = BambooBuilder::new()
    .port(8080)
    .bind("127.0.0.1")
    .data_dir(data_dir)
    .build()?;
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
pub use bamboo::agent::{
    AgentError, Session, Message, Role,
};
pub use bamboo::agent::llm::LLMProvider;
pub use bamboo::agent::tools::BuiltinToolExecutor;
pub use bamboo::core::Config;

// In your code
mod prelude;
use prelude::*;
```

### Pattern 2: Type Aliases

If you have many type references, create aliases:

```rust
type Provider = bamboo::agent::llm::LLMProvider;
type Executor = bamboo::agent::tools::BuiltinToolExecutor;
type Result<T> = std::result::Result<T, bamboo::agent::AgentError>;
```

### Pattern 3: Re-export Common Types

In your lib.rs:

```rust
pub use bamboo::{
    // Re-export commonly used types
    BambooConfig,
    BambooBuilder,
    agent::{Session, Message, AgentError},
    core::Config,
};
```

## Testing Your Migration

1. **Run cargo check**: `cargo check`
2. **Run tests**: `cargo test`
3. **Check imports**: Look for any remaining old crate references
4. **Test functionality**: Ensure all features work as expected

## Troubleshooting

### Error: "cannot find type `Session` in crate `bamboo`"

**Solution**: Update your import paths. `Session` is now at `bamboo::agent::Session`.

### Error: "no field `name` on type `ToolSchema`"

**Solution**: `ToolSchema` now has a nested structure. Access the name via `schema.function.name`.

### Error: "unresolved import `chat_core`"

**Solution**: Replace all `chat_core` imports with `bamboo::core`.

### Error: "no module named `agent_loop`"

**Solution**: The loop module is now `bamboo::agent::loop_module` (renamed to avoid keyword conflict).

## Additional Resources

- [API Documentation](https://docs.rs/bamboo)
- [Examples Repository](https://github.com/bamboo-ai/bamboo/tree/main/examples)
- [GitHub Issues](https://github.com/bamboo-ai/bamboo/issues)

## Getting Help

If you encounter issues during migration:

1. Check the [API documentation](https://docs.rs/bamboo)
2. Search [existing issues](https://github.com/bamboo-ai/bamboo/issues)
3. Open a new issue with the "migration" label
4. Start a [discussion](https://github.com/bamboo-ai/bamboo/discussions)

## Changelog

See [CHANGELOG.md](../../CHANGELOG.md) for a complete list of changes.

## v0.2.0 Server Consolidation

Version 0.2.0 introduces a major refactoring that consolidates the dual server architecture into a unified module.

### Key Changes

1. **Unified server module**: `web_service` and `agent::server` → `server`
2. **Explicit routing**: All routes now explicitly registered in `routes.rs` (~120 routes)
3. **Unified handlers**: Controllers and handlers merged into `server::handlers`
4. **Direct provider access**: Eliminated proxy pattern with HTTP callbacks

### Migration from v0.1.x to v0.2.0

#### Server Imports

**Before (v0.1.x):**
```rust
use bamboo::agent::server::state::AppState;
use bamboo::agent::server::handlers;
use bamboo::web_service::WebService;
use bamboo::web_service::controllers::*;
```

**After (v0.2.0+):**
```rust
use bamboo::server::AppState;
use bamboo::server::handlers;
use bamboo::server::WebService;
// Note: controllers::* → handlers::*
```

#### Handler Organization

**Agent handlers** (moved to `handlers/agent/`):
- `chat`, `execute`, `events`, `stream`, `stop`, `history`, `respond`, `delete`, `health`, `metrics`, `todo`, `mcp`

**Provider handlers** (moved to `handlers/`):
- `openai`, `anthropic`, `gemini`, `copilot_auth`, `agent_api`, `command`, `settings`, `skill`, `tools`, `workspace`

### Backward Compatibility

All old import paths still work with deprecation warnings:

```rust
// Old (deprecated but functional)
use bamboo::agent::server::AppState;
use bamboo::web_service::WebService;
use bamboo::server::controllers::agent_api;

// New (recommended)
use bamboo::server::AppState;
use bamboo::server::WebService;
use bamboo::server::handlers::agent_api;
```

### Benefits

- ✅ **No route duplication**: Single source of truth for all routes
- ✅ **Clearer architecture**: One unified server module
- ✅ **Better performance**: Direct provider access (no HTTP callbacks)
- ✅ **Easier maintenance**: All routes visible in `routes.rs`
- ✅ **-430 lines of code**: Cleaner, more maintainable codebase

### For More Details

See [MIGRATION.md](../../MIGRATION.md) for comprehensive migration documentation.

---

Need help? Open an issue or start a discussion on GitHub!
