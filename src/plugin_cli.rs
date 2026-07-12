//! The `bamboo plugin install|list|remove|update` CLI — a thin HTTP client
//! over a running `bamboo serve` instance's `/api/v1/plugins` routes.
//!
//! Mirrors the `bamboo mcp ...` verb pattern in [`crate::admin_cli`]: this
//! module only builds request bodies, resolves the base URL (via the shared
//! [`ConnArgs`]) and pretty-prints responses. The server (built in parallel
//! against the same frozen contract) is the single source of truth for
//! whether an install/update/remove actually succeeds.
//!
//! Wire contract (frozen — see `PLUGIN_PLAN.md` §"2. CLI agent" / §"3. HTTP
//! agent"):
//! - `GET /api/v1/plugins` -> `{ "plugins": [ { id, name?, version, source,
//!   status, registered: { mcp_server_ids, preset_ids, skill_dirs,
//!   workflow_filenames } } ] }`
//! - `POST /api/v1/plugins/install` -> body `{ "source": <SourceSpec> }`
//!   (`InstallDisposition::FailIfInstalled`); `SourceSpec` is one of
//!   `{"type":"local_dir","path":"..."}` / `{"type":"local_archive","path":"..."}`
//!   / `{"type":"url","url":"...","sha256":"..."?}` — the same tagged shape as
//!   `bamboo_plugin::registry::PluginSource`'s `#[serde(tag = "type")]` wire
//!   form, reproduced here by hand (this crate does not depend on
//!   `bamboo-plugin`, to stay decoupled from the parallel installer-core
//!   branch).
//! - `POST /api/v1/plugins/{id}/update` -> same body shape (`Upgrade`).
//! - `DELETE /api/v1/plugins/{id}` -> uninstall.
//! - Errors: 409 (Conflict / AlreadyInstalled), 422 (UnsupportedPlatform), 404
//!   (NotFound), 400 (bad manifest/artifact); body `{"error": "..."}`.

use std::path::Path;
use std::time::Duration;

use colored::Colorize;

use crate::admin_cli::{
    confirm, guard_id_segment, server_error_message, truncate, unreachable, ConnArgs,
};

/// Plain reads (`list`) get the ordinary admin-CLI budget.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Install/update can copy a local archive, unpack a `.tar.gz`/`.zip`, or
/// download over the network — give it a generous budget vs. the plain reads,
/// matching the MCP mutate verbs' posture (stdio child spawn, etc.).
const PLUGIN_MUTATE_TIMEOUT: Duration = Duration::from_secs(120);

/// Auto-detect the `SourceSpec` JSON for a `<path-or-url>` CLI argument:
/// - an existing directory -> `{"type":"local_dir","path":<absolute path>}`
/// - an existing file ending `.tar.gz`/`.tgz`/`.zip` ->
///   `{"type":"local_archive","path":<absolute path>}`
/// - something starting `http://`/`https://` -> `{"type":"url","url":<as-is>}`
///   (+ `"sha256"` when `--sha256` was given)
///
/// Local paths are canonicalized to absolute so the source resolves correctly
/// even if `bamboo serve` runs with a different working directory than the
/// CLI invocation (e.g. a long-running sidecar). `--sha256` is rejected for
/// local sources — it only pins a network download.
pub(crate) fn detect_source(spec: &str, sha256: Option<&str>) -> anyhow::Result<serde_json::Value> {
    if spec.starts_with("http://") || spec.starts_with("https://") {
        let mut v = serde_json::json!({ "type": "url", "url": spec });
        if let Some(sha) = sha256 {
            v["sha256"] = serde_json::Value::String(sha.to_string());
        }
        return Ok(v);
    }

    let path = Path::new(spec);
    let metadata = std::fs::metadata(path)
        .map_err(|e| anyhow::anyhow!("cannot read '{spec}': {e} (expected a directory, a .tar.gz/.tgz/.zip archive, or an http(s):// URL)"))?;
    let abs = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());

    if metadata.is_dir() {
        if sha256.is_some() {
            anyhow::bail!("--sha256 only applies to a URL source, not a local directory");
        }
        return Ok(serde_json::json!({ "type": "local_dir", "path": abs }));
    }

    let lower = spec.to_ascii_lowercase();
    if metadata.is_file()
        && (lower.ends_with(".tar.gz") || lower.ends_with(".tgz") || lower.ends_with(".zip"))
    {
        if sha256.is_some() {
            anyhow::bail!("--sha256 only applies to a URL source, not a local archive");
        }
        return Ok(serde_json::json!({ "type": "local_archive", "path": abs }));
    }

    anyhow::bail!(
        "'{spec}' is neither a directory, a recognized archive (.tar.gz/.tgz/.zip), nor an http(s):// URL"
    )
}

/// `bamboo plugin install <path-or-url> [--sha256 <hex>]` —
/// `POST /api/v1/plugins/install`. On a 409 (already installed) prints a
/// pointer to `bamboo plugin update` and returns an error (non-zero exit).
pub async fn install(
    conn: ConnArgs,
    source_spec: &str,
    sha256: Option<&str>,
) -> anyhow::Result<()> {
    let source = detect_source(source_spec, sha256)?;
    let base = conn.api_base();
    let url = format!("{base}/plugins/install");
    let resp = reqwest::Client::new()
        .post(&url)
        .timeout(PLUGIN_MUTATE_TIMEOUT)
        .json(&serde_json::json!({ "source": source }))
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if status.is_success() {
        let id_suffix = body
            .get("id")
            .and_then(|s| s.as_str())
            .or_else(|| {
                body.get("plugin")
                    .and_then(|p| p.get("id"))
                    .and_then(|s| s.as_str())
            })
            .map(|id| format!(" '{id}'"))
            .unwrap_or_default();
        println!(
            "{} plugin{id_suffix} installed from '{source_spec}'",
            "✓".green()
        );
        Ok(())
    } else if status.as_u16() == 409 {
        anyhow::bail!(
            "plugin already installed {} — use `bamboo plugin update <id> <path-or-url>` to reinstall/upgrade it",
            server_error_message(&body)
        );
    } else if status.as_u16() == 422 {
        anyhow::bail!("unsupported platform {}", server_error_message(&body));
    } else {
        anyhow::bail!(
            "install failed: HTTP {status} {}",
            server_error_message(&body)
        );
    }
}

/// `bamboo plugin list [--json]` — `GET /api/v1/plugins`.
pub async fn list(conn: ConnArgs, json: bool) -> anyhow::Result<()> {
    let base = conn.api_base();
    let url = format!("{base}/plugins");
    let resp = reqwest::Client::new()
        .get(&url)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    if !resp.status().is_success() {
        anyhow::bail!("GET {url} -> HTTP {}", resp.status());
    }
    let v: serde_json::Value = resp.json().await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&v)?);
        return Ok(());
    }

    let plugins = v.get("plugins").and_then(|p| p.as_array());
    let plugins = match plugins {
        Some(p) if !p.is_empty() => p,
        _ => {
            println!("(no plugins installed)");
            return Ok(());
        }
    };

    println!(
        "{:<20} {:<10} {:<12} {:>4} {:>4} {:>4} {:>4}  SOURCE",
        "ID", "VERSION", "STATUS", "MCP", "SKL", "PST", "WFL"
    );
    for p in plugins {
        let id = p.get("id").and_then(|x| x.as_str()).unwrap_or("?");
        let version = p.get("version").and_then(|x| x.as_str()).unwrap_or("-");
        let status = p.get("status").and_then(|x| x.as_str()).unwrap_or("?");
        let registered = p.get("registered");
        let count = |key: &str| {
            registered
                .and_then(|r| r.get(key))
                .and_then(|a| a.as_array())
                .map(|a| a.len())
                .unwrap_or(0)
        };
        println!(
            "{:<20} {:<10} {:<12} {:>4} {:>4} {:>4} {:>4}  {}",
            truncate(id, 20),
            truncate(version, 10),
            truncate(status, 12),
            count("mcp_server_ids"),
            count("skill_dirs"),
            count("preset_ids"),
            count("workflow_filenames"),
            truncate(&format_source(p.get("source")), 50)
        );
    }
    println!("\n{} plugin(s).", plugins.len());
    Ok(())
}

/// One-line rendering of a `PluginSource` JSON value for the list table.
fn format_source(source: Option<&serde_json::Value>) -> String {
    let Some(source) = source else {
        return "-".to_string();
    };
    match source.get("type").and_then(|t| t.as_str()) {
        Some("local_dir") => format!(
            "local_dir:{}",
            source.get("path").and_then(|p| p.as_str()).unwrap_or("?")
        ),
        Some("local_archive") => format!(
            "local_archive:{}",
            source.get("path").and_then(|p| p.as_str()).unwrap_or("?")
        ),
        Some("url") => format!(
            "url:{}",
            source.get("url").and_then(|u| u.as_str()).unwrap_or("?")
        ),
        _ => source.to_string(),
    }
}

/// `bamboo plugin remove <id> [--yes]` — `DELETE /api/v1/plugins/{id}`.
/// Destructive (stops/removes its registered MCP servers, prompt presets and
/// workflow files, then deletes the plugin directory), so it confirms like
/// `mcp remove` / `session delete` unless `--yes`.
pub async fn remove(conn: ConnArgs, id: &str, yes: bool) -> anyhow::Result<()> {
    guard_id_segment("plugin id", id)?;
    if !yes
        && !confirm(&format!(
            "Remove plugin '{id}'? This uninstalls it and deletes its registered capabilities."
        ))?
    {
        println!("aborted (nothing removed).");
        return Ok(());
    }
    let base = conn.api_base();
    let url = format!("{base}/plugins/{id}");
    let resp = reqwest::Client::new()
        .delete(&url)
        .timeout(PLUGIN_MUTATE_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if status.is_success() {
        println!("{} plugin '{id}' removed", "✓".green());
        Ok(())
    } else if status.as_u16() == 404 {
        anyhow::bail!("plugin '{id}' not found (check `bamboo plugin list`)");
    } else {
        anyhow::bail!(
            "remove failed: HTTP {status} {}",
            server_error_message(&body)
        );
    }
}

/// `bamboo plugin update <id> <path-or-url> [--sha256]` —
/// `POST /api/v1/plugins/{id}/update` (`InstallDisposition::Upgrade`).
pub async fn update(
    conn: ConnArgs,
    id: &str,
    source_spec: &str,
    sha256: Option<&str>,
) -> anyhow::Result<()> {
    guard_id_segment("plugin id", id)?;
    let source = detect_source(source_spec, sha256)?;
    let base = conn.api_base();
    let url = format!("{base}/plugins/{id}/update");
    let resp = reqwest::Client::new()
        .post(&url)
        .timeout(PLUGIN_MUTATE_TIMEOUT)
        .json(&serde_json::json!({ "source": source }))
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if status.is_success() {
        println!("{} plugin '{id}' updated from '{source_spec}'", "✓".green());
        Ok(())
    } else if status.as_u16() == 404 {
        anyhow::bail!("plugin '{id}' not found (check `bamboo plugin list`)");
    } else if status.as_u16() == 422 {
        anyhow::bail!("unsupported platform {}", server_error_message(&body));
    } else {
        anyhow::bail!(
            "update failed: HTTP {status} {}",
            server_error_message(&body)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_source_recognizes_http_and_https_urls() {
        let v = detect_source("https://example.com/plugin.tar.gz", None).unwrap();
        assert_eq!(v["type"], "url");
        assert_eq!(v["url"], "https://example.com/plugin.tar.gz");
        assert!(v.get("sha256").is_none());

        let v = detect_source("http://example.com/plugin.tar.gz", Some("deadbeef")).unwrap();
        assert_eq!(v["type"], "url");
        assert_eq!(v["sha256"], "deadbeef");
    }

    #[test]
    fn detect_source_recognizes_local_dir() {
        let dir = tempfile::tempdir().unwrap();
        let v = detect_source(dir.path().to_str().unwrap(), None).unwrap();
        assert_eq!(v["type"], "local_dir");
        assert_eq!(
            v["path"].as_str().unwrap(),
            dir.path().canonicalize().unwrap().to_str().unwrap()
        );
    }

    #[test]
    fn detect_source_rejects_sha256_for_local_dir() {
        let dir = tempfile::tempdir().unwrap();
        let err = detect_source(dir.path().to_str().unwrap(), Some("deadbeef")).unwrap_err();
        assert!(err.to_string().contains("--sha256"));
    }

    #[test]
    fn detect_source_recognizes_archives_by_extension() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["plugin.tar.gz", "plugin.tgz", "plugin.zip"] {
            let path = dir.path().join(name);
            std::fs::write(&path, b"fake archive bytes").unwrap();
            let v = detect_source(path.to_str().unwrap(), None).unwrap();
            assert_eq!(v["type"], "local_archive", "{name}");
        }
    }

    #[test]
    fn detect_source_rejects_sha256_for_local_archive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plugin.tar.gz");
        std::fs::write(&path, b"fake archive bytes").unwrap();
        let err = detect_source(path.to_str().unwrap(), Some("deadbeef")).unwrap_err();
        assert!(err.to_string().contains("--sha256"));
    }

    #[test]
    fn detect_source_rejects_unrecognized_file_extension() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plugin.txt");
        std::fs::write(&path, b"not an archive").unwrap();
        let err = detect_source(path.to_str().unwrap(), None).unwrap_err();
        assert!(err.to_string().contains("neither a directory"));
    }

    #[test]
    fn detect_source_rejects_missing_path() {
        let err = detect_source("/no/such/path/should/exist/anywhere", None).unwrap_err();
        assert!(err.to_string().contains("cannot read"));
    }

    #[test]
    fn format_source_renders_each_kind() {
        assert_eq!(
            format_source(Some(
                &serde_json::json!({"type":"local_dir","path":"/tmp/x"})
            )),
            "local_dir:/tmp/x"
        );
        assert_eq!(
            format_source(Some(
                &serde_json::json!({"type":"local_archive","path":"/tmp/x.tar.gz"})
            )),
            "local_archive:/tmp/x.tar.gz"
        );
        assert_eq!(
            format_source(Some(
                &serde_json::json!({"type":"url","url":"https://example.com/x.tar.gz"})
            )),
            "url:https://example.com/x.tar.gz"
        );
        assert_eq!(format_source(None), "-");
    }
}
