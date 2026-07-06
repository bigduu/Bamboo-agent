//! `bamboo init` / `bamboo doctor` / `bamboo config set` — first-run onboarding.
//!
//! These are local, server-less commands: they read and write `config.json`
//! under the data dir directly (via [`bamboo_config::Config`]), so a fresh
//! install can configure a provider + API key and self-diagnose **without** the
//! web UI or hand-editing JSON. Writes go through `Config::save_to_dir`, which
//! encrypts provider keys at rest and writes atomically (with a rotating `.bak`).

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

    // 5. Apply + persist (encrypts the key, atomic write, rotates .bak).
    config.provider = provider.clone();
    set_provider_api_key(&mut config, &provider, &api_key)?;
    if let Some(m) = &model {
        set_provider_model(&mut config, &provider, m)?;
    }
    config
        .save_to_dir(data_dir.clone())
        .context("failed to write config.json")?;

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
/// Supported keys: `provider`, `providers.<provider>.api_key`,
/// `providers.<provider>.model` (provider ∈ anthropic|openai|gemini). Existing
/// fields on the provider are preserved.
pub fn run_config_set(key: &str, value: &str, data_dir: Option<PathBuf>) -> Result<()> {
    let data_dir = data_dir.unwrap_or_else(bamboo_config::paths::resolve_bamboo_dir);
    let mut config = Config::from_data_dir_without_env(Some(data_dir.clone()));

    let parts: Vec<&str> = key.split('.').collect();
    match parts.as_slice() {
        // Selecting the default provider accepts any known provider (incl.
        // copilot/bodhi), not just the keyed ones.
        ["provider"] => config.provider = normalize_known_provider(value)?,
        ["providers", p, "api_key"] => {
            let p = normalize_provider(p)?;
            let v = value.trim();
            if v.is_empty() {
                bail!("api_key must not be empty");
            }
            set_provider_api_key(&mut config, &p, v)?;
        }
        ["providers", p, "model"] => {
            let p = normalize_provider(p)?;
            let v = value.trim();
            if v.is_empty() {
                bail!("model must not be empty");
            }
            set_provider_model(&mut config, &p, v)?;
        }
        _ => bail!(
            "unsupported config key '{key}'. Supported keys:\n  \
             provider\n  \
             providers.<anthropic|openai|gemini>.api_key\n  \
             providers.<anthropic|openai|gemini>.model"
        ),
    }

    config
        .save_to_dir(data_dir.clone())
        .context("failed to write config.json")?;
    let shown = if key.ends_with("api_key") {
        mask_secret(value)
    } else {
        value.to_string()
    };
    println!("✓ set {key} = {shown}");
    Ok(())
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
                .providers
                .anthropic
                .get_or_insert_with(empty_provider)
                .api_key = key.to_string()
        }
        "openai" => {
            config
                .providers
                .openai
                .get_or_insert_with(empty_provider)
                .api_key = key.to_string()
        }
        "gemini" => {
            config
                .providers
                .gemini
                .get_or_insert_with(empty_provider)
                .api_key = key.to_string()
        }
        _ => bail!("unsupported provider '{provider}'"),
    }
    Ok(())
}

fn set_provider_model(config: &mut Config, provider: &str, model: &str) -> Result<()> {
    let model = Some(model.to_string());
    match provider {
        "anthropic" => {
            config
                .providers
                .anthropic
                .get_or_insert_with(empty_provider)
                .model = model
        }
        "openai" => {
            config
                .providers
                .openai
                .get_or_insert_with(empty_provider)
                .model = model
        }
        "gemini" => {
            config
                .providers
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
                .providers
                .anthropic
                .as_ref()
                .map(|c| (c.api_key.as_str(), c.api_key_encrypted.as_deref())),
        ),
        "openai" => key_status(
            config
                .providers
                .openai
                .as_ref()
                .map(|c| (c.api_key.as_str(), c.api_key_encrypted.as_deref())),
        ),
        "gemini" => key_status(
            config
                .providers
                .gemini
                .as_ref()
                .map(|c| (c.api_key.as_str(), c.api_key_encrypted.as_deref())),
        ),
        "bodhi" => key_status(
            config
                .providers
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
        let a = config.providers.anthropic.as_ref().unwrap();
        assert_eq!(a.api_key, "sk-ant-xyz");
        assert_eq!(a.model.as_deref(), Some("claude-x"));

        // Setting the model again must not wipe the key.
        set_provider_model(&mut config, "anthropic", "claude-y").unwrap();
        let a = config.providers.anthropic.as_ref().unwrap();
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
}
