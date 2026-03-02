# Critical Issues Found in Second Codex Review

## Summary

After implementing the unified config system, Codex review found **4 CRITICAL** and **4 MAJOR** issues that need immediate attention.

## Status Update (2026-02-26)

- ✅ **Fixed**: `--data-dir` now loads config from the specified directory (and runtime paths are aligned via `BAMBOO_DATA_DIR`).
- ✅ **Fixed**: `AppState::new(data_dir)` and `reload_config()` now load from the provided directory.
- ✅ **Fixed**: Legacy `config.toml` fallback has been removed.
- ✅ **Fixed**: Unknown root keys are preserved via `Config.extra` (and unknown keys under `providers` / `server` are also preserved).

## Critical Issues

### 1. 🔴 CLI --data-dir Doesn't Actually Load Config from Specified Directory

**Status**: ✅ Fixed.

### 2. 🔴 AppState Ignores data_dir for Config Loading

**Status**: ✅ Fixed.

### 3. 🔴 Hot-Reload Doesn't Update Provider (llm is Stale)

**Problem**: `reload_provider()` updates `self.provider` but not `self.llm`
- Code: `src/server/app_state.rs:235-239` caches `llm: Arc<dyn LLMProvider>`
- Code: `src/server/app_state.rs:540-560` doesn't update the cached `llm`
- Impact: After config reload, handlers still use old provider

**Fix Needed**: Remove cached `llm` field or update it in `reload_provider()`

### 4. 🔴 Provider-Specific Env Override Doesn't Work

**Problem**: `BAMBOO_HEADLESS` can't override file's `providers.copilot.headless_auth`
- Code reads `providers.copilot.headless_auth` before falling back to root
- But env only writes to root `headless_auth`
- Impact: Can't override provider-specific settings via env

**Fix Needed**: Apply env overrides to provider configs too

## Major Issues

### 5. 🟡 Config::default() Does I/O

**Problem**: `Config::default()` calls `Config::new()` which does file I/O and env reads
- Impact: Surprising behavior, makes tests non-hermetic
- Best Practice: `Default` trait should be pure (no I/O)

**Fix Needed**: Create separate `Config::new()` for loading and make `Default` return pure defaults

### 6. 🟡 TOML Support Confusing

**Status**: ✅ Resolved by removing legacy `config.toml` fallback entirely.

### 7. 🟡 Config.save() Drops Unknown Fields

**Status**: ✅ Fixed (unknown fields preserved via `#[serde(flatten)]`).

### 8. 🟡 --static-dir and --workers CLI Args Ignored

**Problem**: These args are parsed but never used
- Code: `src/server/server.rs` hardcodes `DEFAULT_WORKER_COUNT`
- Impact: Users expect these to work but they don't

**Fix Needed**: Wire these through to actual server startup

## Minor Issues

- Docs mention `BAMBOO_HEADLESS_AUTH` but code uses `BAMBOO_HEADLESS`
- Builder example uses `~/.bamboo` which doesn't expand `~`
- OldConfig-first parsing can misclassify configs with mixed fields

## Test Coverage Gaps

Missing tests for:
1. `--data-dir` loads config from specified directory
2. `--static-dir` and `--workers` actually affect server
3. `AppState` with custom data_dir loads from that dir
4. Hot-reload updates all cached fields (provider, llm, etc.)
5. Env override of provider-specific values
6. (Removed) TOML fallback behavior

## Recommended Fix Priority

**Phase 1 (Critical - Do Now):**
1. Fix `--data-dir` to actually load from that directory
2. Fix AppState to use passed data_dir for config loading
3. Fix hot-reload to update cached llm provider
4. Fix provider-specific env overrides

**Phase 2 (Major - Soon):**
5. Make `Config::default()` pure (no I/O)
6. Decide on TOML support (keep or remove)
7. Fix unknown field preservation in `Config::save()`
8. Wire through `--static-dir` and `--workers`

**Phase 3 (Minor - Later):**
9. Fix docs to match actual env var names
10. Add missing test coverage

## Questions for Discussion

1. Should `Config::default()` remain I/O-based or become pure defaults (with a separate loader)?
3. Should we remove the cached `llm` field from AppState or update it on reload?
4. Do we need a `try_from_data_dir()` that returns Result for better error handling?
