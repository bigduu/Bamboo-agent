//! `bamboo init` / `bamboo doctor` / `bamboo config set` — first-run onboarding.
//!
//! These are local, server-less commands: they read and write `config.json`
//! under the data dir directly (via [`bamboo_config::Config`]), so a fresh
//! install can configure a provider + API key and self-diagnose **without** the
//! web UI or hand-editing JSON. Provider keys go through the isolated
//! credential store; configuration metadata is written via `Config::save_to_dir`.

use std::collections::BTreeSet;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use bamboo_config::Config;

/// Providers that `init` / `config set` can configure with a static API key.
///
/// Copilot uses an interactive OAuth device flow (not a static key), so it is
/// intentionally excluded here — configure it via the web UI / `bamboo actor`.
const KEYED_PROVIDERS: [&str; 3] = ["anthropic", "openai", "gemini"];

/// All providers the config recognizes, for selecting the *default* provider.
/// Broader than [`KEYED_PROVIDERS`]: `copilot` (OAuth) and `bodhi` (proxy) are
/// valid default providers even though this CLI does not manage their keys.
const KNOWN_PROVIDERS: [&str; 5] = ["anthropic", "openai", "gemini", "copilot", "bodhi"];

/// A reasonable default chat model per provider, matching the ids referenced
/// elsewhere in the repo (README / `docker/config.example.json`). Used only when
/// the user does not pass an explicit model.
fn default_model_for(provider: &str) -> Option<&'static str> {
    match provider {
        "anthropic" => Some("claude-sonnet-4-6"),
        "openai" => Some("gpt-4o-mini"),
        "gemini" => Some("gemini-2.0-flash-exp"),
        _ => None,
    }
}

/// Arguments for [`run_init`], mirrored from the `bamboo init` clap variant.
pub struct InitArgs {
    pub data_dir: Option<PathBuf>,
    pub provider: Option<String>,
    pub api_key: Option<String>,
    pub model: Option<String>,
    pub force: bool,
    pub non_interactive: bool,
}

/// `bamboo init` — scaffold/populate `config.json` with a provider + API key.
///
/// Interactive by default (prompts for anything not passed as a flag); with
/// `--non-interactive` (or when stdin is not a TTY) it requires the values as
/// flags instead of prompting, so it is CI-safe.
pub fn run_init(args: InitArgs) -> Result<()> {
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(bamboo_config::paths::resolve_bamboo_dir);
    let interactive = !args.non_interactive && std::io::stdin().is_terminal();

    // Load existing (or default) config from the target dir. No env overrides
    // (this one-shot writer immediately re-saves; applying `BAMBOO_*` here would
    // bake transient env values into config.json) and no cache publish (#40).
    let mut config = Config::from_data_dir_without_env(Some(data_dir.clone()));

    // 1. Provider.
    let provider = match args.provider {
        Some(p) => normalize_provider(&p)?,
        None if interactive => prompt_provider()?,
        None => bail!(
            "--provider is required in non-interactive mode (one of: {})",
            KEYED_PROVIDERS.join(", ")
        ),
    };

    // 2. Overwrite guard — don't silently clobber an existing key.
    if provider_has_key(&config, &provider) && !args.force {
        if interactive {
            if !prompt_yes_no(
                &format!("Provider '{provider}' already has an API key. Overwrite?"),
                false,
            )? {
                println!("Aborted; existing configuration left unchanged.");
                return Ok(());
            }
        } else {
            bail!("provider '{provider}' already has an API key; pass --force to overwrite");
        }
    }

    // 3. API key.
    let api_key = match args.api_key {
        Some(k) => k,
        None if interactive => prompt_api_key(&provider)?,
        None => bail!("--api-key is required in non-interactive mode"),
    };
    let api_key = api_key.trim().to_string();
    if api_key.is_empty() {
        bail!("API key must not be empty");
    }

    // 4. Model (explicit, else a sensible provider default).
    let model = args
        .model
        .or_else(|| default_model_for(&provider).map(String::from));

    // 5. Apply + persist. Credential storage is committed before the metadata
    // sidecar so no ordinary config document ever contains the plaintext.
    config.provider = provider.clone();
    configure_provider_credential(&mut config, &provider, &api_key)?;
    if let Some(m) = &model {
        set_provider_model(&mut config, &provider, m)?;
    }
    let provider_intents = BTreeSet::from([provider.clone()]);
    bamboo_config::persist_provider_credential_transaction(
        &data_dir,
        &mut config,
        &provider_intents,
    )?;

    // 6. Report + next steps.
    let path = data_dir.join("config.json");
    println!("✓ Wrote {}", path.display());
    println!("  provider = {provider}");
    if let Some(m) = &model {
        println!("  model    = {m}");
    }
    println!("  api_key  = {} (stored encrypted)", mask_secret(&api_key));
    println!();
    println!("Next steps:");
    println!("  bamboo doctor        # verify the setup");
    println!("  bamboo -p \"hello\"    # run a one-shot agent");
    println!("  bamboo serve         # start the HTTP server");
    Ok(())
}

/// `bamboo doctor` — diagnose the local install. Returns `Ok(true)` when healthy,
/// `Ok(false)` when a blocking problem was found (caller should exit non-zero).
pub async fn run_doctor(data_dir: Option<PathBuf>) -> Result<bool> {
    let data_dir = data_dir.unwrap_or_else(bamboo_config::paths::resolve_bamboo_dir);
    let config_path = data_dir.join("config.json");
    let mut errors = 0u32;
    let mut warns = 0u32;

    println!("Bamboo doctor — data dir: {}", data_dir.display());
    println!();

    // 1. config.json presence.
    if config_path.exists() {
        ok(&format!("config.json found ({})", config_path.display()));
    } else {
        err(&format!(
            "config.json not found ({}) — run `bamboo init`",
            config_path.display()
        ));
        errors += 1;
    }

    let config = Config::from_data_dir_without_publish(Some(data_dir.clone()));

    // 2. Active provider + credential.
    let provider = config.provider.clone();
    if provider.trim().is_empty() {
        err("no default provider set — run `bamboo init`");
        errors += 1;
    } else {
        info(&format!("default provider = {provider}"));
        match provider_credential_status(&config, &provider) {
            CredentialStatus::Present => ok(&format!("provider '{provider}' has an API key")),
            CredentialStatus::OauthProvider => ok(&format!(
                "provider '{provider}' uses interactive login (no static key required)"
            )),
            CredentialStatus::Missing => {
                err(&format!(
                    "provider '{provider}' has no API key — run `bamboo init` or \
                     `bamboo config set providers.{provider}.api_key <key>`"
                ));
                errors += 1;
            }
            CredentialStatus::Unknown => {
                warn(&format!(
                    "provider '{provider}' is not a recognized keyed provider; cannot verify a credential"
                ));
                warns += 1;
            }
        }
    }

    // 3. Server reachability (informational — not running is normal). Probe the
    // configured bind; a wildcard/empty bind is reached via loopback.
    let port = config.server.port;
    let bind = config.server.bind.trim();
    let host = if bind.is_empty() || bind == "0.0.0.0" || bind == "::" {
        "127.0.0.1"
    } else {
        bind
    };
    let url = format!("http://{host}:{port}/api/v1/health");
    if check_health(&url).await {
        ok(&format!("server reachable at {host}:{port}"));
    } else {
        info(&format!(
            "server not running on {host}:{port} (start it with `bamboo serve`)"
        ));
    }

    println!();
    if errors > 0 {
        println!("✗ {errors} problem(s) found.");
        Ok(false)
    } else if warns > 0 {
        println!("✓ Ready ({warns} warning(s)).");
        Ok(true)
    } else {
        println!("✓ All checks passed.");
        Ok(true)
    }
}

/// `bamboo config set <key> <value>` — write a single config value.
///
/// Two classes of keys:
///
/// **Secret-aware keys** (provider keys use the isolated credential store;
/// remaining legacy domains are encrypted by `Config::save_to_dir`):
///   - `provider` (default provider selection; not a secret, kept for
///     backward compatibility)
///   - `providers.<anthropic|openai|gemini>.api_key` (unchanged legacy path)
///   - `providers.bodhi.api_key`
///   - `provider_instances.<id>.api_key` (the instance must already exist)
///   - `notifications.ntfy.token`, `notifications.bark.device_key`
///   - `providers.<anthropic|openai|gemini>.model` (legacy path, not a secret)
///
/// **Everything else** goes through the generic validated dot-path setter
/// ([`bamboo_config::dot_path::apply_dot_path_set`]): the value is parsed as
/// JSON when it parses (numbers/bools/arrays/objects), else taken as a
/// string; the result must deserialize back into the typed `Config` (type
/// mismatches and unknown fields are rejected before anything is written).
/// `proxy_auth.*`, `subagents.broker.*` and all `*_encrypted` keys are
/// refused. With `dry_run` the resulting diff is printed and nothing is
/// saved.
pub fn run_config_set(
    key: &str,
    value: &str,
    data_dir: Option<PathBuf>,
    dry_run: bool,
) -> Result<()> {
    let data_dir = data_dir.unwrap_or_else(bamboo_config::paths::resolve_bamboo_dir);
    let mut config = Config::from_data_dir_without_env(Some(data_dir.clone()));
    let mut provider_credential_intents = BTreeSet::new();
    let mut provider_instance_credential_intents = BTreeSet::new();
    let mut notification_credential_intents = BTreeSet::new();

    let parts: Vec<&str> = key.split('.').collect();
    // `Some(outcome)` = generic dot-path set (validated new config inside);
    // `None` = a dedicated arm mutated `config` in place.
    let outcome = match parts.as_slice() {
        // Selecting the default provider accepts any known provider (incl.
        // copilot/bodhi), not just the keyed ones.
        ["provider"] => {
            config.provider = normalize_known_provider(value)?;
            None
        }
        // Bodhi's static key is secret-aware too, but is not a keyed provider
        // for `init` — handled before the legacy keyed-provider arm.
        ["providers", "bodhi", "api_key"] => {
            let v = value.trim();
            if v.is_empty() {
                bail!("api_key must not be empty");
            }
            configure_provider_credential(&mut config, "bodhi", v)?;
            provider_credential_intents.insert("bodhi".to_string());
            None
        }
        ["providers", p, "api_key"] => {
            let p = normalize_provider(p)?;
            let v = value.trim();
            if v.is_empty() {
                bail!("api_key must not be empty");
            }
            configure_provider_credential(&mut config, &p, v)?;
            provider_credential_intents.insert(p);
            None
        }
        ["providers", p, "model"] => {
            let p = normalize_provider(p)?;
            let v = value.trim();
            if v.is_empty() {
                bail!("model must not be empty");
            }
            set_provider_model(&mut config, &p, v)?;
            None
        }
        ["provider_instances", id, "api_key"] => {
            let v = value.trim();
            if v.is_empty() {
                bail!("api_key must not be empty");
            }
            let Some(instance) = config.provider_instances.get_mut(*id) else {
                bail!(
                    "provider instance '{id}' not found; create the instance first \
                     (its api_key is then stored encrypted at rest)"
                );
            };
            instance.api_key = v.to_string();
            provider_instance_credential_intents.insert((*id).to_string());
            None
        }
        ["notifications", "ntfy", "token"] => {
            let v = value.trim();
            if v.is_empty() {
                bail!("token must not be empty");
            }
            config.notifications.ntfy.token = Some(v.to_string());
            config.notifications.ntfy.configured = true;
            notification_credential_intents.insert("ntfy".to_string());
            None
        }
        ["notifications", "bark", "device_key"] => {
            let v = value.trim();
            if v.is_empty() {
                bail!("device_key must not be empty");
            }
            config.notifications.bark.device_key = Some(v.to_string());
            config.notifications.bark.configured = true;
            notification_credential_intents.insert("bark".to_string());
            None
        }
        // Generic, validated dot-path set for every other key. Secret paths
        // not routed above are refused inside (defense in depth), so a
        // plaintext secret can never be silently dropped or mis-stored.
        _ => {
            let parsed = bamboo_config::dot_path::parse_cli_value(value);
            Some(bamboo_config::dot_path::apply_dot_path_set(
                &config, key, parsed,
            )?)
        }
    };

    let is_secret_key =
        key.ends_with("api_key") || key.ends_with(".token") || key.ends_with(".device_key");
    let shown = if is_secret_key {
        mask_secret(value)
    } else {
        value.to_string()
    };

    if dry_run {
        match &outcome {
            Some(out) => print_dry_run_diff(&config, &out.config),
            None => println!(
                "--dry-run: would set {key} = {shown} via the dedicated setter \
                 (secrets are stored encrypted at rest). No changes written."
            ),
        }
        return Ok(());
    }

    let mut to_save = match outcome {
        Some(out) => out.config,
        None => config,
    };
    if !notification_credential_intents.is_empty() {
        let revision = bamboo_config::CredentialStore::open(&data_dir).revision()?;
        bamboo_config::persist_notification_credential_transaction_at_revision(
            &data_dir,
            &mut to_save,
            &notification_credential_intents,
            revision,
        )?;
    } else if !provider_credential_intents.is_empty()
        || !provider_instance_credential_intents.is_empty()
    {
        bamboo_config::persist_provider_instance_credential_transaction(
            &data_dir,
            &mut to_save,
            &provider_credential_intents,
            &provider_instance_credential_intents,
        )?;
    } else {
        to_save
            .save_to_dir(data_dir.clone())
            .context("failed to write config.json")?;
    }
    println!("✓ set {key} = {shown}");
    Ok(())
}

/// Print the prospective `config set` change as a path-level diff.
/// Both sides are serialized with plaintext secrets sanitized (the same
/// clearing `save_to_dir` performs), so the preview never leaks a secret.
fn print_dry_run_diff(current: &Config, updated: &Config) {
    let before = sanitized_config_value(current);
    let after = sanitized_config_value(updated);
    let diff = bamboo_config::dot_path::diff_json(&before, &after);
    if diff.is_empty() {
        println!("--dry-run: no effective change. Nothing would be written.");
        return;
    }
    println!(
        "--dry-run: would apply {} change(s) (secrets omitted):",
        diff.len()
    );
    for (path, old, new) in diff {
        let old = old
            .map(|v| v.to_string())
            .unwrap_or_else(|| "(absent)".to_string());
        let new = new
            .map(|v| v.to_string())
            .unwrap_or_else(|| "(absent)".to_string());
        println!("  {path}: {old} -> {new}");
    }
    println!("No changes written.");
}

/// In-memory serialization with the same plaintext-secret clearing that
/// `save_to_dir` applies before writing (env vars, cluster-fabric SSH
/// secrets; `api_key`/token plaintexts are `skip_serializing` already).
fn sanitized_config_value(config: &Config) -> serde_json::Value {
    let mut c = config.clone();
    c.sanitize_env_vars_for_disk();
    c.sanitize_cluster_fabric_for_disk();
    serde_json::to_value(&c).unwrap_or(serde_json::Value::Null)
}

// ---- provider helpers ----

/// Validate + canonicalize a provider name against the keyed-provider set
/// (providers this CLI can set an API key for).
fn normalize_provider(provider: &str) -> Result<String> {
    let p = provider.trim().to_ascii_lowercase();
    if KEYED_PROVIDERS.contains(&p.as_str()) {
        Ok(p)
    } else {
        bail!(
            "unsupported provider '{provider}' (supported: {})",
            KEYED_PROVIDERS.join(", ")
        )
    }
}

/// Validate + canonicalize a provider name against the full known-provider set
/// (for selecting the default provider, which may be copilot/bodhi).
fn normalize_known_provider(provider: &str) -> Result<String> {
    let p = provider.trim().to_ascii_lowercase();
    if KNOWN_PROVIDERS.contains(&p.as_str()) {
        Ok(p)
    } else {
        bail!(
            "unknown provider '{provider}' (known: {})",
            KNOWN_PROVIDERS.join(", ")
        )
    }
}

/// Construct an empty provider-config struct via serde (these structs don't
/// derive `Default`; every field is either serde-`default` or `Option`, so an
/// empty JSON object deserializes cleanly).
fn empty_provider<T: serde::de::DeserializeOwned>() -> T {
    serde_json::from_value(serde_json::json!({}))
        .expect("an empty provider config always deserializes")
}

fn set_provider_api_key(config: &mut Config, provider: &str, key: &str) -> Result<()> {
    match provider {
        "anthropic" => {
            config
                .providers_mut()
                .anthropic
                .get_or_insert_with(empty_provider)
                .api_key = key.to_string()
        }
        "openai" => {
            config
                .providers_mut()
                .openai
                .get_or_insert_with(empty_provider)
                .api_key = key.to_string()
        }
        "gemini" => {
            config
                .providers_mut()
                .gemini
                .get_or_insert_with(empty_provider)
                .api_key = key.to_string()
        }
        _ => bail!("unsupported provider '{provider}'"),
    }
    Ok(())
}

fn configure_provider_credential(config: &mut Config, provider: &str, key: &str) -> Result<()> {
    if provider == "bodhi" {
        config
            .providers_mut()
            .bodhi
            .get_or_insert_with(empty_provider)
            .api_key = key.to_string();
    } else {
        set_provider_api_key(config, provider, key)?;
    }
    Ok(())
}

fn set_provider_model(config: &mut Config, provider: &str, model: &str) -> Result<()> {
    let model = Some(model.to_string());
    match provider {
        "anthropic" => {
            config
                .providers_mut()
                .anthropic
                .get_or_insert_with(empty_provider)
                .model = model
        }
        "openai" => {
            config
                .providers_mut()
                .openai
                .get_or_insert_with(empty_provider)
                .model = model
        }
        "gemini" => {
            config
                .providers_mut()
                .gemini
                .get_or_insert_with(empty_provider)
                .model = model
        }
        _ => bail!("unsupported provider '{provider}'"),
    }
    Ok(())
}

enum CredentialStatus {
    Present,
    Missing,
    OauthProvider,
    Unknown,
}

fn provider_credential_status(config: &Config, provider: &str) -> CredentialStatus {
    match provider {
        "anthropic" => key_status(
            config
                .providers()
                .anthropic
                .as_ref()
                .map(|c| (c.api_key.as_str(), c.api_key_encrypted.as_deref())),
        ),
        "openai" => key_status(
            config
                .providers()
                .openai
                .as_ref()
                .map(|c| (c.api_key.as_str(), c.api_key_encrypted.as_deref())),
        ),
        "gemini" => key_status(
            config
                .providers()
                .gemini
                .as_ref()
                .map(|c| (c.api_key.as_str(), c.api_key_encrypted.as_deref())),
        ),
        "bodhi" => key_status(
            config
                .providers()
                .bodhi
                .as_ref()
                .map(|c| (c.api_key.as_str(), c.api_key_encrypted.as_deref())),
        ),
        "copilot" => CredentialStatus::OauthProvider,
        _ => CredentialStatus::Unknown,
    }
}

/// A credential is present if either the in-memory plaintext key or the on-disk
/// encrypted key is non-empty.
fn key_status(pair: Option<(&str, Option<&str>)>) -> CredentialStatus {
    match pair {
        Some((plain, enc))
            if !plain.trim().is_empty() || enc.is_some_and(|e| !e.trim().is_empty()) =>
        {
            CredentialStatus::Present
        }
        _ => CredentialStatus::Missing,
    }
}

fn provider_has_key(config: &Config, provider: &str) -> bool {
    matches!(
        provider_credential_status(config, provider),
        CredentialStatus::Present
    )
}

// ---- health probe ----

async fn check_health(url: &str) -> bool {
    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
    else {
        return false;
    };
    matches!(client.get(url).send().await, Ok(r) if r.status().is_success())
}

// ---- prompts / formatting ----

fn prompt_provider() -> Result<String> {
    println!("Select a provider:");
    for (i, p) in KEYED_PROVIDERS.iter().enumerate() {
        println!("  {}) {}", i + 1, p);
    }
    let line = prompt_line("Provider [number or name] (default: anthropic): ")?;
    let line = line.trim();
    if line.is_empty() {
        return Ok("anthropic".to_string());
    }
    if let Ok(n) = line.parse::<usize>() {
        if (1..=KEYED_PROVIDERS.len()).contains(&n) {
            return Ok(KEYED_PROVIDERS[n - 1].to_string());
        }
    }
    normalize_provider(line)
}

fn prompt_api_key(provider: &str) -> Result<String> {
    eprintln!("Note: the key is visible as you type; it is stored encrypted at rest.");
    prompt_line(&format!("Enter the {provider} API key: "))
}

fn prompt_line(prompt: &str) -> Result<String> {
    print!("{prompt}");
    std::io::stdout().flush().ok();
    let mut s = String::new();
    let n = std::io::stdin()
        .read_line(&mut s)
        .context("failed to read from stdin")?;
    if n == 0 {
        bail!("unexpected end of input");
    }
    Ok(s.trim_end_matches(['\n', '\r']).to_string())
}

fn prompt_yes_no(question: &str, default_yes: bool) -> Result<bool> {
    let hint = if default_yes { "[Y/n]" } else { "[y/N]" };
    let ans = prompt_line(&format!("{question} {hint} "))?;
    Ok(match ans.trim().to_ascii_lowercase().as_str() {
        "" => default_yes,
        "y" | "yes" => true,
        _ => false,
    })
}

/// Show a secret as `head…tail (N chars)` so a confirmation line never prints
/// the full key.
fn mask_secret(secret: &str) -> String {
    let s = secret.trim();
    let n = s.chars().count();
    if n <= 6 {
        return "*".repeat(n.max(1));
    }
    let head: String = s.chars().take(3).collect();
    let tail: String = {
        let mut t: Vec<char> = s.chars().rev().take(2).collect();
        t.reverse();
        t.into_iter().collect()
    };
    format!("{head}…{tail} ({n} chars)")
}

fn ok(msg: &str) {
    println!("  ✓ {msg}");
}
fn err(msg: &str) {
    println!("  ✗ {msg}");
}
fn warn(msg: &str) {
    println!("  ! {msg}");
}
fn info(msg: &str) {
    println!("  · {msg}");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A default in-memory config that never touches a real data dir: loading
    /// from a non-existent directory returns defaults without reading or writing
    /// anything on disk.
    fn default_config() -> Config {
        Config::from_data_dir_without_publish(Some(PathBuf::from(
            "/nonexistent-bamboo-setup-cli-test-dir",
        )))
    }

    #[test]
    fn normalize_provider_accepts_keyed_and_rejects_unknown() {
        assert_eq!(normalize_provider("Anthropic").unwrap(), "anthropic");
        assert_eq!(normalize_provider("  openai ").unwrap(), "openai");
        // copilot has no static key → not a keyed provider.
        assert!(normalize_provider("copilot").is_err());
        assert!(normalize_provider("nope").is_err());
    }

    #[test]
    fn normalize_known_provider_accepts_oauth_and_proxy_providers() {
        // Selecting the default provider accepts copilot/bodhi too.
        assert_eq!(normalize_known_provider("Copilot").unwrap(), "copilot");
        assert_eq!(normalize_known_provider("bodhi").unwrap(), "bodhi");
        assert_eq!(normalize_known_provider("anthropic").unwrap(), "anthropic");
        assert!(normalize_known_provider("nope").is_err());
    }

    #[test]
    fn set_key_and_model_preserve_the_other_field() {
        let mut config = default_config();
        set_provider_api_key(&mut config, "anthropic", "sk-ant-xyz").unwrap();
        set_provider_model(&mut config, "anthropic", "claude-x").unwrap();
        let a = config.providers().anthropic.as_ref().unwrap();
        assert_eq!(a.api_key, "sk-ant-xyz");
        assert_eq!(a.model.as_deref(), Some("claude-x"));

        // Setting the model again must not wipe the key.
        set_provider_model(&mut config, "anthropic", "claude-y").unwrap();
        let a = config.providers().anthropic.as_ref().unwrap();
        assert_eq!(a.api_key, "sk-ant-xyz");
        assert_eq!(a.model.as_deref(), Some("claude-y"));
    }

    #[test]
    fn credential_status_reflects_plaintext_and_encrypted() {
        let mut config = default_config();
        assert!(matches!(
            provider_credential_status(&config, "openai"),
            CredentialStatus::Missing
        ));
        set_provider_api_key(&mut config, "openai", "sk-openai").unwrap();
        assert!(matches!(
            provider_credential_status(&config, "openai"),
            CredentialStatus::Present
        ));
        assert!(matches!(
            provider_credential_status(&config, "copilot"),
            CredentialStatus::OauthProvider
        ));
    }

    #[test]
    fn mask_secret_hides_the_middle() {
        assert_eq!(mask_secret("abc"), "***");
        let m = mask_secret("sk-ant-1234567890");
        assert!(m.starts_with("sk-"));
        assert!(m.ends_with("chars)"));
        assert!(!m.contains("1234567890"));
    }

    #[test]
    fn config_set_generic_key_round_trips_to_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_dir = dir.path().to_path_buf();

        run_config_set("server.port", "19999", Some(data_dir.clone()), false).unwrap();
        let raw = std::fs::read_to_string(data_dir.join("config.json")).unwrap();
        let root: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(root["server"]["port"], serde_json::json!(19999));

        // Unknown / mistyped keys are rejected without writing.
        assert!(run_config_set("server.prot", "1", Some(data_dir.clone()), false).is_err());
        assert!(
            run_config_set("server.port", "not-a-port", Some(data_dir.clone()), false).is_err()
        );
        let raw_after = std::fs::read_to_string(data_dir.join("config.json")).unwrap();
        let root: serde_json::Value = serde_json::from_str(&raw_after).unwrap();
        assert_eq!(root["server"]["port"], serde_json::json!(19999));
    }

    #[test]
    fn config_set_dry_run_writes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_dir = dir.path().to_path_buf();

        run_config_set("server.port", "12001", Some(data_dir.clone()), true).unwrap();
        assert!(
            !data_dir.join("config.json").exists(),
            "--dry-run must not create/modify config.json"
        );
    }

    #[test]
    fn config_set_api_key_still_encrypted_at_rest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_dir = dir.path().to_path_buf();

        // Secret-aware arm writes through the isolated credential store.
        run_config_set(
            "providers.anthropic.api_key",
            "sk-ant-setupcli-secret",
            Some(data_dir.clone()),
            false,
        )
        .unwrap();

        let raw = std::fs::read_to_string(data_dir.join("providers.json")).unwrap();
        assert!(
            !raw.contains("sk-ant-setupcli-secret"),
            "plaintext api_key must never reach disk"
        );
        let root: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            root["anthropic"]["credential_ref"],
            "provider.anthropic.api_key"
        );
        assert!(root["anthropic"].get("api_key_encrypted").is_none());
        let credentials = std::fs::read_to_string(data_dir.join("credentials.json")).unwrap();
        assert!(!credentials.contains("sk-ant-setupcli-secret"));

        // A follow-up generic set must keep the stored ciphertext intact.
        run_config_set(
            "providers.anthropic.model",
            "claude-x",
            Some(data_dir.clone()),
            false,
        )
        .unwrap();
        let raw = std::fs::read_to_string(data_dir.join("providers.json")).unwrap();
        assert!(!raw.contains("sk-ant-setupcli-secret"));
        let root: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(root["anthropic"]["model"], "claude-x");
        assert_eq!(
            root["anthropic"]["credential_ref"],
            "provider.anthropic.api_key"
        );
        assert!(root["anthropic"].get("api_key_encrypted").is_none());

        let reloaded = Config::from_data_dir_without_env(Some(data_dir));
        assert_eq!(
            reloaded.providers().anthropic.as_ref().unwrap().api_key,
            "sk-ant-setupcli-secret",
            "the stored key must still decrypt after a generic set"
        );
    }

    #[test]
    fn config_set_notification_secret_uses_isolated_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_dir = dir.path().to_path_buf();

        run_config_set(
            "notifications.ntfy.token",
            "ntfy-setupcli-secret",
            Some(data_dir.clone()),
            false,
        )
        .unwrap();

        let raw = std::fs::read_to_string(data_dir.join("config.json")).unwrap();
        assert!(!raw.contains("ntfy-setupcli-secret"));
        assert!(!raw.contains("token_encrypted"));
        let root: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            root["notifications"]["ntfy"]["credential_ref"],
            "notification.ntfy.token"
        );
        assert_eq!(root["notifications"]["ntfy"]["configured"], true);
        let credentials = std::fs::read_to_string(data_dir.join("credentials.json")).unwrap();
        assert!(!credentials.contains("ntfy-setupcli-secret"));

        run_config_set(
            "notifications.ntfy.topic",
            "alerts",
            Some(data_dir.clone()),
            false,
        )
        .unwrap();
        let reloaded = Config::from_data_dir_without_env(Some(data_dir));
        assert_eq!(reloaded.notifications.ntfy.topic, "alerts");
        assert_eq!(
            reloaded.notifications.ntfy.token.as_deref(),
            Some("ntfy-setupcli-secret")
        );
    }

    #[test]
    fn config_set_rejects_secret_paths_it_cannot_protect() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_dir = dir.path().to_path_buf();
        for (key, value) in [
            ("proxy_auth.username", "alice"),
            ("providers.anthropic.api_key_encrypted", "cipher"),
            ("provider_instances.nope.api_key", "sk-x"),
        ] {
            assert!(
                run_config_set(key, value, Some(data_dir.clone()), false).is_err(),
                "{key} must be refused"
            );
        }
        assert!(!data_dir.join("config.json").exists());
    }
}
