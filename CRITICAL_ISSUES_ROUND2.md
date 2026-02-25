# Critical Issues Found in Second Codex Review

## Summary

After implementing the unified config system, Codex review found **4 CRITICAL** and **4 MAJOR** issues that need immediate attention.

## Critical Issues

### 1. 🔴 CLI --data-dir Doesn't Actually Load Config from Specified Directory

**Problem**: `bamboo serve --data-dir /path` doesn't load config from `/path/config.json`
- Code: `src/bin/bamboo.rs:65-82` loads `Config::new()` first, then just sets `config.data_dir`
- Impact: Users expect config to be loaded from the specified directory, but it's not

**Fix Needed**:
```rust
// Instead of:
let mut config = Config::new();
if let Some(d) = data_dir {
    config.data_dir = d;
}

// Should be:
let config = if let Some(ref d) = data_dir {
    Config::from_data_dir(Some(d.clone()))
} else {
    Config::new()
};
```

### 2. 🔴 AppState Ignores data_dir for Config Loading

**Problem**: `AppState::new(bamboo_home_dir)` doesn't load config from that directory
- Code: `src/server/app_state.rs:327-348` uses `Config::new()` (global)
- Code: `src/server/app_state.rs:590-595` `reload_config()` also uses `Config::new()`
- Impact: Server can run with one data_dir but config from another (inconsistent state)

**Fix Needed**: Use `Config::from_data_dir(Some(bamboo_home_dir))` in AppState

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

**Problem**: Docs say "config.toml preferred" but code uses `config.json` in data_dir
- TOML fallback only checks `./config.toml` in current directory (surprising)
- Impact: Confusing for users, potential security issue (stray config files)

**Fix Needed**: Remove TOML support or clarify it's legacy-only

### 7. 🟡 Config.save() Drops Unknown Fields

**Problem**: `Config::save()` serializes only known fields
- Settings endpoints write `proxy_auth_encrypted` which Config ignores
- Impact: Can erase fields written by other parts of code
- This re-introduces "two writers, incompatible schema" problem

**Fix Needed**: Preserve unknown fields using `#[serde(flatten)]` or read-modify-write pattern

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
6. TOML fallback behavior (or test that it's removed)

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

1. Should we remove TOML support entirely or keep as legacy fallback?
2. Should `Config::save()` preserve unknown fields (more complex) or is dropping them acceptable?
3. Should we remove the cached `llm` field from AppState or update it on reload?
4. Do we need a `try_from_data_dir()` that returns Result for better error handling?
