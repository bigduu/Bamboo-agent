# Codex Review - Issues Fixed

> **Update (2026-02-26)**: `Config` no longer contains a persisted `data_dir` field and Bamboo no longer
> loads legacy `config.toml`. The data directory is runtime-derived via `BAMBOO_DATA_DIR` (or defaults
> to `~/.bamboo`), and `config.json` is always stored under that directory.

## Summary of Critical Fixes Applied

This document summarizes the critical issues identified by Codex review and how they were fixed.

### ✅ Issue #1: Environment Variable Override Logic (CRITICAL)
**Problem**: Environment variables were only applied when NO config file existed, violating the expected behavior where env vars should ALWAYS override file values.

**Fix**: Refactored `Config::new()` to:
1. Load config from file (if exists)
2. Apply environment variable overrides (highest priority)
3. Keep config writes scoped to the selected runtime data directory

**Code Changes**:
- Created `Config::from_data_dir()` method to support custom data directories
- Modified `Config::new()` to always apply env var overrides after loading from file
- Added `Config::save_to_dir(dir)` for explicit persistence (server runtime uses `AppState.app_data_dir`)

**Files**: `src/core/config.rs`

---

### ✅ Issue #2: CLI Argument Handling (CRITICAL)
**Problem**: Clap default values for `--port`/`--bind` always overrode config file values, even when user didn't provide CLI flags. `--static-dir` and `--workers` were parsed but discarded.

**Fix**: Changed CLI arguments to `Option<T>` and only apply when explicitly provided:
- Removed `default_value` from clap arguments
- Made all arguments `Option<T>`
- Applied overrides conditionally: `if let Some(p) = port { config.server.port = p; }`
- Now properly respects config file values

**Files**: `src/bin/bamboo.rs`

---

### ✅ Issue #3: BambooServer::start() Todo (CRITICAL)
**Problem**: `BambooServer::start()` was `todo!()` but would panic at runtime when called by CLI.

**Fix**: Replaced `BambooServer` usage with direct call to `server::run_with_bind()`:
- Removed dependency on unimplemented `BambooServer::start()`
- Binary now calls server API directly with async/await
- Simpler and more direct code path

**Files**: `src/bin/bamboo.rs`, `src/lib.rs`

---

### ✅ Issue #4: headless_auth Migration (CRITICAL)
**Problem**: Migration moved `headless_auth` to `providers.copilot.headless_auth`, but runtime code still read from deprecated root field.

**Fix**: Updated provider factory to read from new location with fallback:
```rust
let headless_auth = config
    .providers
    .copilot
    .as_ref()
    .map(|c| c.headless_auth)
    .unwrap_or(config.headless_auth);
```

**Files**: `src/agent/llm/provider_factory.rs:66`

---

### ✅ Issue #5: Config File Path Consistency (CRITICAL)
**Problem**: Different parts of code used different config paths:
- `Config::new()` used global `core::paths::config_json_path()`
- Settings endpoints used `{data_dir}/config.json` (server runtime: `app_state.app_data_dir/config.json`)
- Would diverge when `data_dir` is customized

**Fix**: Made config location runtime-derived:
- `Config::from_data_dir(Some(dir))` loads from `{dir}/config.json` (parameter beats env)
- `core::paths::bamboo_dir()` derives runtime data dir from `BAMBOO_DATA_DIR` (or `~/.bamboo`)
- Server persists via `AppState.app_data_dir` to ensure a single source of truth

**Files**: `src/core/config.rs`

---

### ✅ Issue #6: Deprecation Version Numbers (MINOR)
**Problem**: Deprecations marked `since = "0.3.0"` but crate is `0.2.5`.

**Fix**: Updated to `since = "0.2.6"`:
- `src/config/bamboo_config.rs`
- `src/config/mod.rs`

---

### ✅ Issue #7: Stack Overflow in Tests (CRITICAL)
**Problem**: `Config::default()` called `Config::new()`, which called `Self::default()` in error paths, causing infinite recursion.

**Fix**: Created `Config::create_default()` helper:
- Private method that creates default config without recursion
- Used in all "fallback to default" paths in `Config::new()`
- `Config::default()` still calls `Config::new()` for backward compatibility

**Files**: `src/core/config.rs`

---

### ✅ Issue #8: Test Hermeticity (MAJOR)
**Problem**: Tests could read/modify real user config files.

**Fix**: All tests now:
1. Acquire env lock to prevent race conditions
2. Create temp home directory
3. Set HOME env var BEFORE calling Config methods
4. Properly isolated from user's real config

**Files**: `src/core/config.rs` (tests module)

---

## Test Results

All tests passing:
- ✅ 788 unit tests passed
- ✅ 6 integration tests passed
- ✅ 0 tests failed
- ✅ Build successful

## Priority Order Applied

Binary mode:
1. **CLI arguments** (when explicitly provided; includes `--data-dir`)
2. **Environment variables**
3. **Config file values** (`{data_dir}/config.json`)
4. **Code defaults**

## Backward Compatibility

All changes are **100% backward compatible**:
- Old config files work without modification
- Old code using deprecated APIs still works (with warnings)
- Existing env vars continue to work
- Priority order matches legacy `BambooConfig::from_env()` behavior

## Files Modified

1. `src/core/config.rs` - Core configuration implementation
2. `src/bin/bamboo.rs` - Binary entry point
3. `src/lib.rs` - Library exports
4. `src/agent/llm/provider_factory.rs` - Provider factory
5. `src/config/bamboo_config.rs` - Deprecation version
6. `src/config/mod.rs` - Module deprecation

## Known Remaining Issues (Minor)

These were identified by Codex but are not critical:

1. **Docs drift** - Some doc comments mention old XDG paths and old env var names (minor, cosmetic)
2. **Invalid env values silently ignored** - e.g., bad `BAMBOO_PORT` values are silently ignored (acceptable behavior)

## Success Criteria Met

- [x] Environment variables override file values
- [x] Config storage is scoped to the runtime data directory (no `config.data_dir` field)
- [x] Copilot headless mode reads from providers.copilot.headless_auth
- [x] All tests passing
- [x] No compilation errors
- [x] Backward compatible
- [x] Single source of truth for configuration
