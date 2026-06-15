//! The `bamboo health | status | sessions | stop` admin CLI.
//!
//! A thin HTTP client over a running `bamboo serve` instance. Each command wraps
//! an endpoint the server already exposes — `/api/v1/health`,
//! `/api/v1/sessions`, `/api/v1/stop/{id}` — so an operator can probe and steer
//! the backend without hand-writing `curl`. The server is the single source of
//! truth; this module only resolves the base URL and pretty-prints responses.

use std::path::PathBuf;
use std::time::Duration;

use colored::Colorize;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Connection options shared by every admin subcommand.
#[derive(Debug, Clone, Default)]
pub struct ConnArgs {
    /// Full base URL override (e.g. `http://127.0.0.1:9562`). Wins over the rest.
    pub server_url: Option<String>,
    /// Port override (else read from the resolved config).
    pub port: Option<u16>,
    /// Data dir holding `config.json` (else `~/.bamboo`).
    pub data_dir: Option<PathBuf>,
}

impl ConnArgs {
    /// Resolve the API base, e.g. `http://127.0.0.1:9562/api/v1`.
    fn api_base(&self) -> String {
        if let Some(url) = &self.server_url {
            let url = url.trim_end_matches('/');
            // Tolerate a scheme-less override like `localhost:9562`.
            let url = if url.contains("://") {
                url.to_string()
            } else {
                format!("http://{url}")
            };
            return format!("{url}/api/v1");
        }
        let config = bamboo_llm::Config::from_data_dir(self.data_dir.clone());
        let port = self.port.unwrap_or(config.server.port);
        let host = match config.server.bind.trim() {
            // Listen-on-all addresses: a client must dial a concrete host.
            "" | "0.0.0.0" | "::" | "[::]" => "127.0.0.1".to_string(),
            // Bracket a bare IPv6 literal so the URL is well-formed.
            h if h.contains(':') && !h.starts_with('[') => format!("[{h}]"),
            h => h.to_string(),
        };
        format!("http://{host}:{port}/api/v1")
    }
}

fn unreachable(base: &str, e: reqwest::Error) -> anyhow::Error {
    anyhow::anyhow!("could not reach the server at {base} ({e}). Is `bamboo serve` running?")
}

/// `bamboo health` — liveness probe. Exits non-zero (via the returned `Err`)
/// when the server is unreachable or reports an unhealthy status.
pub async fn health(conn: ConnArgs) -> anyhow::Result<()> {
    let base = conn.api_base();
    let url = format!("{base}/health");
    let resp = reqwest::Client::new()
        .get(&url)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    if resp.status().is_success() {
        println!("{}  {base}", "● healthy".green().bold());
        Ok(())
    } else {
        anyhow::bail!("unhealthy: HTTP {} from {url}", resp.status());
    }
}

/// `bamboo status` — one-screen overview: address, health, session counts.
pub async fn status(conn: ConnArgs) -> anyhow::Result<()> {
    let base = conn.api_base();
    let server = base.trim_end_matches("/api/v1");
    println!("{:<10}{server}", "server:".bold());

    let client = reqwest::Client::new();
    let health = client
        .get(format!("{base}/health"))
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await;
    match health {
        Ok(r) if r.status().is_success() => println!("{:<10}{}", "health:".bold(), "ok".green()),
        Ok(r) => {
            println!("{:<10}{} (HTTP {})", "health:".bold(), "down".red(), r.status());
            return Ok(());
        }
        Err(e) => {
            println!("{:<10}{} ({e})", "health:".bold(), "unreachable".red());
            return Ok(());
        }
    }

    if let Ok(r) = client
        .get(format!("{base}/sessions"))
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
    {
        if let Ok(v) = r.json::<serde_json::Value>().await {
            let sessions = v.get("sessions").and_then(|s| s.as_array());
            let total = sessions.map(|s| s.len()).unwrap_or(0);
            let running = sessions.map(|s| count_running(s)).unwrap_or(0);
            println!(
                "{:<10}{total} total, {} running",
                "sessions:".bold(),
                running.to_string().cyan()
            );
        }
    }
    Ok(())
}

/// `bamboo sessions` — tabulate sessions on a running server.
pub async fn sessions_list(conn: ConnArgs) -> anyhow::Result<()> {
    let base = conn.api_base();
    let url = format!("{base}/sessions");
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
    let sessions = v.get("sessions").and_then(|s| s.as_array());
    let sessions = match sessions {
        Some(s) if !s.is_empty() => s,
        _ => {
            println!("(no sessions)");
            return Ok(());
        }
    };

    // Plain (un-colored) cells so the column widths line up — ANSI escapes would
    // otherwise be counted against the padding.
    println!(
        "{:<38} {:<5} {:<26} {:>5}  TITLE",
        "SESSION ID", "RUN", "MODEL", "MSGS"
    );
    for s in sessions {
        let id = s.get("id").and_then(|x| x.as_str()).unwrap_or("?");
        let running = s
            .get("is_running")
            .and_then(|b| b.as_bool())
            .unwrap_or(false);
        let model = s.get("model").and_then(|x| x.as_str()).unwrap_or("");
        let msgs = s.get("message_count").and_then(|x| x.as_u64()).unwrap_or(0);
        let title = s.get("title").and_then(|x| x.as_str()).unwrap_or("");
        println!(
            "{:<38} {:<5} {:<26} {:>5}  {}",
            id,
            if running { "run" } else { "-" },
            truncate(model, 26),
            msgs,
            truncate(title, 60)
        );
    }
    let running = count_running(sessions);
    println!(
        "\n{running} running. Stop one with: {}",
        "bamboo stop <session-id>".cyan()
    );
    Ok(())
}

/// `bamboo stop <id>` — cancel a running session's loop.
pub async fn stop(conn: ConnArgs, session_id: &str) -> anyhow::Result<()> {
    // Guard the path segment: real session ids are opaque tokens (UUIDs), so
    // reject anything that could traverse or malform the URL rather than encode it.
    if session_id.is_empty()
        || session_id == "."
        || session_id == ".."
        || session_id.contains(['/', '\\', '?', '#', '%'])
        || session_id.chars().any(char::is_whitespace)
    {
        anyhow::bail!("invalid session id: '{session_id}'");
    }
    let base = conn.api_base();
    let url = format!("{base}/stop/{session_id}");
    let resp = reqwest::Client::new()
        .post(&url)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    let message = body
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_string();
    if status.is_success() {
        let msg = if message.is_empty() { "stopped" } else { &message };
        println!("{} {msg}", "✓".green());
        Ok(())
    } else if status.as_u16() == 404 {
        anyhow::bail!(
            "no active run for session '{session_id}'{}",
            if message.is_empty() {
                String::new()
            } else {
                format!(" ({message})")
            }
        );
    } else {
        anyhow::bail!("stop failed: HTTP {status} {message}");
    }
}

/// Count array entries whose `is_running` is true.
fn count_running(sessions: &[serde_json::Value]) -> usize {
    sessions
        .iter()
        .filter(|x| {
            x.get("is_running")
                .and_then(|b| b.as_bool())
                .unwrap_or(false)
        })
        .count()
}

/// Truncate to `max` chars with a trailing ellipsis.
fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let head: String = s.chars().take(max.saturating_sub(1)).collect();
        format!("{head}…")
    }
}
