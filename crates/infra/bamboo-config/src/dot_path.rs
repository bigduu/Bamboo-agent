//! Generic, validated dot-path `config set` support (offline writer).
//!
//! Backs `bamboo config set <key> <value>` for keys beyond the historical
//! hardcoded trio (`provider`, `providers.<p>.api_key`, `providers.<p>.model`).
//! The flow is deliberately conservative — it never writes a config file it
//! cannot fully re-read:
//!
//! 1. Project the current (hydrated, in-memory) [`Config`] into its explicit
//!    compatibility JSON view, including independently persisted modules.
//! 2. Apply the dot-path assignment onto that JSON tree.
//! 3. Deserialize the patched JSON back into the typed [`Config`] — a type
//!    mismatch fails here with the offending key in the error.
//! 4. Reject typos: a key that serde silently *dropped* (strict struct) or
//!    that landed in one of the forward-compat `extra` flatten maps is
//!    reported as an unknown key instead of being written.
//! 5. Hand the validated [`Config`] back to the caller, which persists it via
//!    [`Config::save_to_dir`] — the single writer that re-encrypts secrets and
//!    writes atomically (with `.bak` rotation).
//!
//! # Secrets
//!
//! Plaintext secret fields (`providers.<p>.api_key`,
//! `provider_instances.<id>.api_key`, `notifications.ntfy.token`,
//! `notifications.bark.device_key`, broker/proxy credentials) are
//! `#[serde(skip_serializing)]` — they cannot round-trip through JSON, so this
//! generic setter REFUSES them with [`DotPathError::SecretPath`] /
//! [`DotPathError::Unsupported`]; the CLI routes them through the dedicated
//! typed setters instead. `*_encrypted` fields are derived state and are
//! always refused (writing ciphertext directly is how cipher divergence — and
//! real data loss — happens).
//!
//! `proxy_auth` needs one extra guard: unlike every other refresh,
//! [`Config::refresh_proxy_auth_encrypted`] CLEARS the at-rest ciphertext when
//! the in-memory plaintext is `None`. Since the JSON round-trip drops the
//! (skip-serializing) plaintext, [`apply_dot_path_set`] carries `proxy_auth`
//! over from the current config so an unrelated `config set` can never wipe
//! stored proxy credentials.

use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::Config;

/// Errors surfaced by [`apply_dot_path_set`]. Messages are user-facing (the
/// CLI prints them verbatim), so each names the failing key.
#[derive(Debug, thiserror::Error)]
pub enum DotPathError {
    /// The key does not correspond to a recognized config field.
    #[error("unknown config key '{key}'{hint}")]
    UnknownKey { key: String, hint: String },

    /// The value cannot be stored at this key (type mismatch, bad shape, …).
    #[error("invalid value for '{key}': {message}")]
    InvalidValue { key: String, message: String },

    /// The key holds a secret that must go through the dedicated
    /// encrypt-at-rest writer, not the generic JSON patch.
    #[error("'{key}' is a secret: {guidance}")]
    SecretPath { key: String, guidance: String },

    /// The key exists but cannot be written by this setter.
    #[error("cannot set '{key}': {reason}")]
    Unsupported { key: String, reason: String },
}

/// Result of a validated dot-path assignment (not yet persisted).
#[derive(Debug)]
pub struct DotPathSetOutcome {
    /// The updated, fully validated config — ready for `save_to_dir`.
    pub config: Config,
    /// The previous JSON value at the key (None when the key was absent).
    pub old_value: Option<Value>,
    /// The JSON value that was applied.
    pub new_value: Value,
}

/// Parse a CLI value: anything that parses as JSON is taken as JSON
/// (numbers, bools, null, arrays, objects); everything else is a string.
/// Pass a quoted value (`'"true"'`) to force a literal string.
pub fn parse_cli_value(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_string()))
}

/// Apply `key = value` onto `current` with full validation (see module docs).
/// Returns the new [`Config`] to persist; never touches the disk itself.
pub fn apply_dot_path_set(
    current: &Config,
    key: &str,
    value: Value,
) -> Result<DotPathSetOutcome, DotPathError> {
    let segments: Vec<&str> = key.split('.').collect();
    if key.trim().is_empty() || segments.iter().any(|s| s.is_empty()) {
        return Err(DotPathError::InvalidValue {
            key: key.to_string(),
            message: "empty path segment (keys are dot-separated, e.g. `server.port`)".to_string(),
        });
    }
    guard_reserved_paths(&segments, key)?;

    let before = current
        .to_compatibility_value()
        .map_err(|e| DotPathError::InvalidValue {
            key: key.to_string(),
            message: format!("failed to serialize current config: {e}"),
        })?;
    let old_value = resolve_path(&before, &segments).cloned();

    let mut patched = before;
    apply_path(&mut patched, &segments, key, value.clone())?;

    let mut config: Config =
        serde_json::from_value(patched).map_err(|e| DotPathError::InvalidValue {
            key: key.to_string(),
            message: e.to_string(),
        })?;

    // Typo guard 1: a key the typed config only "kept" by capturing it into a
    // forward-compat `extra` flatten map is not a real field — refuse it
    // rather than persist a key every reader will ignore.
    if let Some(extra_key) = first_new_extra_key(current, &config) {
        return Err(DotPathError::UnknownKey {
            key: extra_key,
            hint: " (not a recognized config field)".to_string(),
        });
    }

    // Typo guard 2: strict structs silently DROP unknown fields on
    // deserialize — detect that by checking the key survived a round-trip.
    // The `mcpServers` subtree is exempt: its custom (de)serializer converts
    // between the internal shape and the mainstream on-disk shape, so paths
    // legitimately move (`enabled` ⇄ `disabled`, transport flattening).
    let mcp_subtree = matches!(segments[0], "mcpServers" | "mcp");
    if !mcp_subtree {
        let after = config
            .to_compatibility_value()
            .map_err(|e| DotPathError::InvalidValue {
                key: key.to_string(),
                message: format!("failed to serialize updated config: {e}"),
            })?;
        match resolve_path(&after, &segments) {
            Some(round_tripped) if values_equivalent(round_tripped, &value) => {}
            Some(round_tripped) => {
                return Err(DotPathError::InvalidValue {
                    key: key.to_string(),
                    message: format!(
                        "value did not survive a config round-trip (stored form would be \
                         {round_tripped}); refusing to write"
                    ),
                });
            }
            // Setting a field to an empty/default value can legitimately make
            // it disappear from the serialized form (skip_serializing_if).
            None if is_vanishing_default(&value) => {}
            None => {
                return Err(DotPathError::UnknownKey {
                    key: key.to_string(),
                    hint: " (the config schema has no such field; nothing would be stored)"
                        .to_string(),
                });
            }
        }
    }

    // Round-trip restore: `proxy_auth` is in-memory-only and its refresh
    // CLEARS the ciphertext when None (see module docs). Reserved-path guards
    // above ensure the patch never targets it, so carrying it over is safe.
    config.proxy_auth = current.proxy_auth.clone();

    Ok(DotPathSetOutcome {
        config,
        old_value,
        new_value: value,
    })
}

/// Compute the changed leaf paths between two JSON trees, as
/// `(dotted_path, old, new)` tuples (`None` = absent on that side).
/// Used for `config set --dry-run` previews.
pub fn diff_json(before: &Value, after: &Value) -> Vec<(String, Option<Value>, Option<Value>)> {
    let mut out = Vec::new();
    diff_json_inner(before, after, String::new(), &mut out);
    out
}

fn diff_json_inner(
    before: &Value,
    after: &Value,
    path: String,
    out: &mut Vec<(String, Option<Value>, Option<Value>)>,
) {
    if before == after {
        return;
    }
    match (before, after) {
        (Value::Object(b), Value::Object(a)) => {
            let keys: BTreeSet<&String> = b.keys().chain(a.keys()).collect();
            for k in keys {
                let child = if path.is_empty() {
                    k.to_string()
                } else {
                    format!("{path}.{k}")
                };
                match (b.get(k), a.get(k)) {
                    (Some(bv), Some(av)) => diff_json_inner(bv, av, child, out),
                    (Some(bv), None) => out.push((child, Some(bv.clone()), None)),
                    (None, Some(av)) => out.push((child, None, Some(av.clone()))),
                    (None, None) => unreachable!("key came from the union of both maps"),
                }
            }
        }
        (Value::Array(b), Value::Array(a)) if b.len() == a.len() => {
            for (i, (bv, av)) in b.iter().zip(a.iter()).enumerate() {
                diff_json_inner(bv, av, format!("{path}.{i}"), out);
            }
        }
        _ => out.push((path, Some(before.clone()), Some(after.clone()))),
    }
}

/// Refuse secret-bearing and derived paths (see module docs).
fn guard_reserved_paths(segments: &[&str], key: &str) -> Result<(), DotPathError> {
    if segments.iter().any(|s| s.ends_with("_encrypted")) {
        return Err(DotPathError::Unsupported {
            key: key.to_string(),
            reason: "encrypted fields are derived automatically from their plaintext \
                     counterpart on save; set the plaintext key instead"
                .to_string(),
        });
    }

    match segments {
        ["proxy_auth", ..] => Err(DotPathError::Unsupported {
            key: key.to_string(),
            reason: "proxy credentials are managed via the web UI settings (stored \
                     encrypted as `proxy_auth_encrypted`)"
                .to_string(),
        }),
        ["providers", p, "api_key"] => Err(DotPathError::SecretPath {
            key: key.to_string(),
            guidance: format!(
                "API keys are encrypted at rest; use the dedicated setter \
                 (`bamboo config set providers.{p}.api_key <key>` routes there automatically)"
            ),
        }),
        ["provider_instances", id, "api_key"] => Err(DotPathError::SecretPath {
            key: key.to_string(),
            guidance: format!(
                "instance API keys are encrypted at rest; use the dedicated setter \
                 (`bamboo config set provider_instances.{id}.api_key <key>` routes there \
                 automatically)"
            ),
        }),
        ["notifications", "ntfy", "token"] | ["notifications", "bark", "device_key"] => {
            Err(DotPathError::SecretPath {
                key: key.to_string(),
                guidance: "notification-channel secrets are encrypted at rest; the CLI \
                           routes this key through the dedicated setter automatically"
                    .to_string(),
            })
        }
        ["subagents", "broker", ..] => Err(DotPathError::Unsupported {
            key: key.to_string(),
            reason: "the broker client config is runtime-only and not persisted in \
                     config.json"
                .to_string(),
        }),
        _ => Ok(()),
    }
}

/// Resolve a dot-path inside a JSON tree (numeric segments index arrays).
fn resolve_path<'a>(root: &'a Value, segments: &[&str]) -> Option<&'a Value> {
    let mut node = root;
    for seg in segments {
        node = match node {
            Value::Object(map) => map.get(*seg)?,
            Value::Array(items) => items.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(node)
}

/// Apply `value` at the dot-path, creating intermediate objects as needed.
/// Numeric segments index arrays (final segment may append at `len`).
fn apply_path(
    root: &mut Value,
    segments: &[&str],
    key: &str,
    value: Value,
) -> Result<(), DotPathError> {
    let mut node = root;
    for (i, seg) in segments.iter().enumerate() {
        let last = i + 1 == segments.len();
        match node {
            Value::Object(map) => {
                if last {
                    map.insert(seg.to_string(), value);
                    return Ok(());
                }
                node = map
                    .entry(seg.to_string())
                    .or_insert_with(|| Value::Object(Map::new()));
            }
            Value::Array(items) => {
                let idx = seg
                    .parse::<usize>()
                    .map_err(|_| DotPathError::InvalidValue {
                        key: key.to_string(),
                        message: format!(
                            "'{}' is an array; segment '{seg}' must be a numeric index",
                            segments[..i].join(".")
                        ),
                    })?;
                if last {
                    if idx < items.len() {
                        items[idx] = value;
                    } else if idx == items.len() {
                        items.push(value);
                    } else {
                        return Err(DotPathError::InvalidValue {
                            key: key.to_string(),
                            message: format!(
                                "array index {idx} out of range for '{}' (length {})",
                                segments[..i].join("."),
                                items.len()
                            ),
                        });
                    }
                    return Ok(());
                }
                node = items
                    .get_mut(idx)
                    .ok_or_else(|| DotPathError::InvalidValue {
                        key: key.to_string(),
                        message: format!(
                            "array index {idx} out of range for '{}'",
                            segments[..i].join(".")
                        ),
                    })?;
            }
            other => {
                return Err(DotPathError::InvalidValue {
                    key: key.to_string(),
                    message: format!(
                        "cannot descend into '{}': it holds {} — not an object/array",
                        segments[..i].join("."),
                        json_type_name(other)
                    ),
                });
            }
        }
    }
    unreachable!("segments is non-empty; the loop always returns on the last segment");
}

fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// Values whose disappearance from the serialized form is expected
/// (`skip_serializing_if` on empty/None fields).
fn is_vanishing_default(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
        _ => false,
    }
}

/// Loose equality: exact match, or numerically-equal numbers (so an f64
/// round-trip through a float field doesn't false-negative).
fn values_equivalent(a: &Value, b: &Value) -> bool {
    if a == b {
        return true;
    }
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => (x - y).abs() <= f64::EPSILON * x.abs().max(y.abs()).max(1.0) * 4.0,
        _ => false,
    }
}

/// Dotted paths of every key currently held by a forward-compat `extra`
/// flatten map. Keep in sync with the `#[serde(flatten)] extra` fields on
/// [`Config`] and its sub-structs (root, `server`, `providers` container,
/// the per-provider configs, `provider_instances.<id>`).
fn extra_key_paths(config: &Config) -> BTreeSet<String> {
    let mut keys: BTreeSet<String> = config.extra.keys().cloned().collect();
    keys.extend(config.server.extra.keys().map(|k| format!("server.{k}")));
    keys.extend(
        config
            .providers
            .extra
            .keys()
            .map(|k| format!("providers.{k}")),
    );

    macro_rules! provider_extra {
        ($field:ident) => {
            if let Some(p) = &config.providers.$field {
                keys.extend(
                    p.extra
                        .keys()
                        .map(|k| format!("providers.{}.{k}", stringify!($field))),
                );
            }
        };
    }
    provider_extra!(openai);
    provider_extra!(anthropic);
    provider_extra!(gemini);
    provider_extra!(copilot);
    provider_extra!(bodhi);

    for (id, instance) in &config.provider_instances {
        keys.extend(
            instance
                .extra
                .keys()
                .map(|k| format!("provider_instances.{id}.{k}")),
        );
    }
    keys
}

/// First `extra`-captured key present in `candidate` but not in `current`
/// (i.e. introduced by the patch → an unknown field).
fn first_new_extra_key(current: &Config, candidate: &Config) -> Option<String> {
    let before = extra_key_paths(current);
    extra_key_paths(candidate)
        .into_iter()
        .find(|k| !before.contains(k))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProxyAuth;
    use serde_json::json;
    use std::path::PathBuf;

    /// A default in-memory config that never touches a real data dir and —
    /// unlike `from_data_dir_without_publish` — applies NO `BAMBOO_*` env
    /// overrides, so parallel tests that mutate the env can't leak in.
    fn default_config() -> Config {
        Config::from_data_dir_without_env(Some(PathBuf::from(
            "/nonexistent-bamboo-dot-path-test-dir",
        )))
    }

    #[test]
    fn parse_cli_value_json_first_then_string() {
        assert_eq!(parse_cli_value("9999"), json!(9999));
        assert_eq!(parse_cli_value("true"), json!(true));
        assert_eq!(parse_cli_value("null"), Value::Null);
        assert_eq!(parse_cli_value(r#"{"a":1}"#), json!({"a":1}));
        assert_eq!(parse_cli_value("[1,2]"), json!([1, 2]));
        assert_eq!(parse_cli_value("anthropic"), json!("anthropic"));
        // Quoting forces a literal string.
        assert_eq!(parse_cli_value(r#""true""#), json!("true"));
    }

    #[test]
    fn valid_set_updates_typed_field() {
        let config = default_config();
        let outcome = apply_dot_path_set(&config, "server.port", json!(19999)).unwrap();
        assert_eq!(outcome.config.server.port, 19999);
        assert_eq!(outcome.old_value, Some(json!(9562)));
        assert_eq!(outcome.new_value, json!(19999));
    }

    #[test]
    fn valid_set_creates_missing_intermediate_objects() {
        let config = default_config();
        assert!(config.providers.anthropic.is_none());
        let outcome =
            apply_dot_path_set(&config, "providers.anthropic.model", json!("claude-x")).unwrap();
        assert_eq!(
            outcome
                .config
                .providers
                .anthropic
                .as_ref()
                .unwrap()
                .model
                .as_deref(),
            Some("claude-x")
        );
    }

    #[test]
    fn type_mismatch_is_rejected_with_key_in_error() {
        let config = default_config();
        let err = apply_dot_path_set(&config, "server.port", json!("not-a-port")).unwrap_err();
        match err {
            DotPathError::InvalidValue { key, .. } => assert_eq!(key, "server.port"),
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn unknown_root_key_is_rejected_not_written_to_extra() {
        let config = default_config();
        let err = apply_dot_path_set(&config, "serverr", json!({"port": 1})).unwrap_err();
        assert!(matches!(err, DotPathError::UnknownKey { ref key, .. } if key == "serverr"));
    }

    #[test]
    fn unknown_nested_key_captured_by_extra_is_rejected() {
        let config = default_config();
        // `server` has a forward-compat extra map — a typo lands there.
        let err = apply_dot_path_set(&config, "server.prot", json!(8080)).unwrap_err();
        assert!(matches!(err, DotPathError::UnknownKey { ref key, .. } if key == "server.prot"));

        // Unknown provider names are captured by the providers container map.
        let err = apply_dot_path_set(&config, "providers.grok.model", json!("g1")).unwrap_err();
        assert!(matches!(err, DotPathError::UnknownKey { ref key, .. } if key == "providers.grok"));
    }

    #[test]
    fn unknown_key_dropped_by_strict_struct_is_rejected() {
        let config = default_config();
        // KeywordMaskingConfig has no extra map: serde drops unknown fields.
        let err = apply_dot_path_set(&config, "keyword_masking.nope", json!(true)).unwrap_err();
        assert!(
            matches!(err, DotPathError::UnknownKey { ref key, .. } if key == "keyword_masking.nope")
        );
    }

    #[test]
    fn existing_extension_keys_remain_editable() {
        let mut config = default_config();
        config
            .extra
            .insert("setup".to_string(), json!({"completed": false}));
        let outcome = apply_dot_path_set(&config, "setup.completed", json!(true)).unwrap();
        assert_eq!(outcome.config.extra["setup"], json!({"completed": true}));
    }

    #[test]
    fn secret_paths_are_refused_by_the_generic_setter() {
        let config = default_config();
        for key in [
            "providers.anthropic.api_key",
            "providers.bodhi.api_key",
            "provider_instances.work.api_key",
            "notifications.ntfy.token",
            "notifications.bark.device_key",
        ] {
            let err = apply_dot_path_set(&config, key, json!("sk-secret")).unwrap_err();
            assert!(
                matches!(err, DotPathError::SecretPath { .. }),
                "expected SecretPath for {key}, got {err:?}"
            );
        }
        for key in [
            "proxy_auth.username",
            "providers.anthropic.api_key_encrypted",
            "proxy_auth_encrypted",
            "subagents.broker.token",
        ] {
            let err = apply_dot_path_set(&config, key, json!("x")).unwrap_err();
            assert!(
                matches!(err, DotPathError::Unsupported { .. }),
                "expected Unsupported for {key}, got {err:?}"
            );
        }
    }

    #[test]
    fn vanishing_default_values_are_accepted() {
        let config = default_config();
        // `server.tls = null` disappears from the serialized form
        // (skip_serializing_if) — that must not be treated as unknown.
        let outcome = apply_dot_path_set(&config, "server.tls", Value::Null).unwrap();
        assert!(outcome.config.server.tls.is_none());
    }

    #[test]
    fn mcp_subtree_is_type_checked_but_shape_exempt() {
        let config = default_config();
        let outcome = apply_dot_path_set(
            &config,
            "mcpServers.everything",
            json!({"command": "npx", "args": ["mcp-everything"]}),
        )
        .unwrap();
        let server = outcome
            .config
            .mcp
            .servers
            .iter()
            .find(|s| s.id == "everything")
            .expect("server added");
        assert!(server.enabled);

        // Garbage transport combos still fail typed validation.
        let err = apply_dot_path_set(
            &config,
            "mcpServers.bad",
            json!({"command": "npx", "url": "http://x"}),
        )
        .unwrap_err();
        assert!(matches!(err, DotPathError::InvalidValue { .. }));
    }

    #[test]
    fn proxy_auth_survives_an_unrelated_generic_set() {
        let mut config = default_config();
        config.proxy_auth = Some(ProxyAuth {
            username: "alice".to_string(),
            password: "hunter2".to_string(),
        });
        let outcome =
            apply_dot_path_set(&config, "http_proxy", json!("http://proxy:8080")).unwrap();
        // Without the carry-over, save_to_dir would CLEAR proxy_auth_encrypted
        // (refresh_proxy_auth_encrypted nulls it when proxy_auth is None).
        assert_eq!(
            outcome
                .config
                .proxy_auth
                .as_ref()
                .map(|a| a.username.as_str()),
            Some("alice")
        );
        assert_eq!(outcome.config.http_proxy, "http://proxy:8080");
    }

    #[test]
    fn generic_set_preserves_provider_credential_reference_at_rest() {
        let _guard = crate::test_support::env_cache_lock_acquire();
        let dir = tempfile::tempdir().expect("tempdir");
        let data_dir = dir.path().to_path_buf();

        let reference = crate::credential_ref("provider", "anthropic", "api_key").unwrap();
        crate::CredentialStore::open(&data_dir)
            .replace(
                reference.clone(),
                "sk-ant-super-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        // Seed a config with a credential-store-backed anthropic key.
        let mut config = Config::from_data_dir_without_env(Some(data_dir.clone()));
        config.providers.anthropic = Some(crate::AnthropicConfig {
            api_key: "sk-ant-super-secret".to_string(),
            credential_ref: Some(reference.clone()),
            ..Default::default()
        });
        config.save_to_dir(data_dir.clone()).expect("seed save");

        let on_disk = std::fs::read_to_string(data_dir.join("providers.json")).unwrap();
        assert!(
            !on_disk.contains("sk-ant-super-secret"),
            "plaintext key must never be written to disk"
        );
        let before: Value = serde_json::from_str(&on_disk).unwrap();
        let before_data = before.get("data").unwrap_or(&before);
        let reference_before = before_data["anthropic"]["credential_ref"]
            .as_str()
            .expect("credential reference present")
            .to_string();

        // Now perform an UNRELATED generic set and save again.
        let loaded = Config::from_data_dir_without_env(Some(data_dir.clone()));
        assert_eq!(
            loaded.providers.anthropic.as_ref().unwrap().api_key,
            "sk-ant-super-secret",
            "hydration sanity check"
        );
        let outcome =
            apply_dot_path_set(&loaded, "providers.anthropic.model", json!("claude-x")).unwrap();
        outcome.config.save_to_dir(data_dir.clone()).expect("save");

        let on_disk = std::fs::read_to_string(data_dir.join("providers.json")).unwrap();
        assert!(!on_disk.contains("sk-ant-super-secret"));
        let root: Value = serde_json::from_str(&on_disk).unwrap();
        let data = root.get("data").unwrap_or(&root);
        assert_eq!(data["anthropic"]["model"], json!("claude-x"));
        assert_eq!(
            data["anthropic"]["credential_ref"]
                .as_str()
                .expect("credential reference still present"),
            reference_before,
            "an unrelated generic set must not churn or drop the credential reference"
        );
        assert!(!on_disk.contains("api_key_encrypted"));

        let config_json: Value = serde_json::from_slice(
            &std::fs::read(data_dir.join("config.json")).expect("root config exists"),
        )
        .expect("root config is valid JSON");
        for sidecar_key in ["memory", "subagents", "providers"] {
            assert!(
                config_json.get(sidecar_key).is_none(),
                "compatibility projection must not persist {sidecar_key} in config.json"
            );
        }

        // And the key still decrypts after the round-trip.
        let reloaded = Config::from_data_dir_without_env(Some(data_dir));
        assert_eq!(
            reloaded.providers.anthropic.as_ref().unwrap().api_key,
            "sk-ant-super-secret"
        );
    }

    #[test]
    fn diff_json_reports_changed_paths() {
        let before = json!({"a": {"b": 1, "keep": true}, "gone": 1});
        let after = json!({"a": {"b": 2, "keep": true}, "new": [1]});
        let diff = diff_json(&before, &after);
        assert!(diff.contains(&("a.b".to_string(), Some(json!(1)), Some(json!(2)))));
        assert!(diff.contains(&("gone".to_string(), Some(json!(1)), None)));
        assert!(diff.contains(&("new".to_string(), None, Some(json!([1])))));
        assert_eq!(diff.len(), 3);
    }
}
