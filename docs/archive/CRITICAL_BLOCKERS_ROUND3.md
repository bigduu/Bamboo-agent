# 🚨 Critical Production Blockers Found (Round 3)

## Summary

Codex review found **3 critical production blockers** that must be fixed before deployment.

## 🔴 CRITICAL: Production Blockers

### 1. `--data-dir` Not Honored by Running Server

**Problem**: `bamboo serve --data-dir /custom` loads config from `/custom` but `AppState` ignores it!
- **Location**: `src/server/app_state.rs:327` - Uses `Config::new()` instead of passed data_dir
- **Location**: `src/server/app_state.rs:590-595` - `reload_config()` also uses `Config::new()`
- **Impact**: Server runs with wrong config, can cause data corruption or security issues

**Fix Required**:
```rust
// AppState::new() should receive and use the data_dir
impl AppState {
    pub async fn new(data_dir: PathBuf) -> Self {
        let config = Config::from_data_dir(Some(data_dir.clone()));
        // ...
    }
}

// reload_config() should use self's data_dir
fn reload_config(&self) {
    let config = Config::from_data_dir(Some(self.app_data_dir.clone()));
    // ...
}
```

### 2. CLI Flags `--static-dir` and `--workers` Are No-Ops

**Problem**: CLI parses these flags but they're never used!
- **--workers**: Hardcoded to `DEFAULT_WORKER_COUNT` in `src/server/server.rs:88`
- **--static-dir**: Static serving code exists but not wired to CLI flag
- **Impact**: Users think they're configuring server but settings are ignored

**Fix Required**:
```rust
// Pass config to server startup
pub async fn run_with_bind(
    data_dir: PathBuf,
    port: u16,
    bind: &str,
    config: Config,  // Pass entire config
) -> Result<(), String> {
    let workers = config.server.workers;
    // ...
}
```

### 3. Security: `bamboo config` Leaks API Keys

**Problem**: `bamboo config` prints ALL secrets to stdout!
- **Location**: `src/bin/bamboo.rs:98-105`
- **Impact**: API keys leak into shell history, CI logs, screen sharing

**Fix Required**:
```rust
Commands::Config { path, show_secrets } => {
    let config = Config::new();
    let mut config_json = serde_json::to_value(&config).unwrap();

    if !show_secrets {
        // Redact sensitive fields
        if let Some(providers) = config_json.get_mut("providers") {
            // Redact all api_key fields
            redact_secrets(providers);
        }
    }

    println!("{}", serde_json::to_string_pretty(&config_json).unwrap());
}
```

## 🟡 HIGH: Documentation Drift

### 4. Wrong Environment Variable Name in Docs

**Problem**: Docs say `BAMBOO_HEADLESS_AUTH` but code uses `BAMBOO_HEADLESS`
- **Location**: `src/core/config.rs:29` (docs) vs `src/core/config.rs:361-363` (code)
- **Impact**: Users set wrong env var, feature doesn't work

### 5. Proxy Env Vars Documented but Ignored

**Problem**: Docs mention `HTTP_PROXY`/`HTTPS_PROXY` but code explicitly ignores them
- **Impact**: Users set proxy vars but they don't work

### 6. TOML/XDG Docs Don't Match Reality

**Status (2026-02-26)**: Resolved.
- Legacy `config.toml` fallback removed.
- Runtime data dir is derived from `BAMBOO_DATA_DIR` (or defaults to `~/.bamboo`).
- Configuration is always `{data_dir}/config.json`.

## 🟠 MEDIUM: Error Handling Issues

### 7. Silent Fallback on Config Errors

**Problem**: `Config::new()` silently swallows IO/parse errors and returns defaults
- **Impact**: User has malformed config, but server starts with defaults silently
- **Better**: At least log warnings, or return Result

### 8. Non-Atomic Config Saves

**Problem**: `Config::save()` is not atomic and doesn't set file permissions
- **Impact**: Crash during write corrupts config; API keys readable by other users
- **Fix**: Use atomic write + `0600` permissions

## 📊 Test Coverage Gaps

**Missing critical tests:**
1. `bamboo serve --data-dir /custom` loads from /custom (would catch issue #1)
2. `--workers` and `--static-dir` actually affect running server (would catch issue #2)
3. AppState with custom data_dir loads from that dir
4. Hot-reload uses correct data_dir

## 🎯 Priority Fix Order

**MUST FIX BEFORE DEPLOYMENT:**
1. ✅ Fix AppState to use passed data_dir for config loading
2. ✅ Wire `--workers` through to server or remove the flag
3. ✅ Redact secrets in `bamboo config` output

**SHOULD FIX SOON:**
4. Fix documentation drift
5. Add config load/save error logging
6. Make config saves atomic with proper permissions

**CAN FIX LATER:**
7. Add missing integration tests
8. Refactor Config::default() to be pure

## 📝 Code Locations

### Critical Files to Modify:
- `src/server/app_state.rs` - Fix data_dir usage
- `src/server/server.rs` - Wire workers config
- `src/bin/bamboo.rs` - Add show-secrets flag, fix config command
- `src/core/config.rs` - Add save_with_permissions, error logging

### Tests to Add:
- `tests/integration_tests.rs` - Server with custom data_dir
- `tests/cli_tests.rs` - Workers and static_dir flags

## 🚫 Deployment Status

**Status: ❌ NOT READY FOR PRODUCTION**

**Blockers:**
1. Server uses wrong config (--data-dir broken)
2. CLI flags are no-ops (--workers, --static-dir)
3. Security issue (secrets in stdout)

**Must complete Round 3 fixes before deployment!**
