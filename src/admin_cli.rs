//! The `bamboo health | status | sessions | session | stop | respond |
//! schedules` admin CLI.
//!
//! A thin HTTP client over a running `bamboo serve` instance. Each command wraps
//! an endpoint the server already exposes — `/api/v1/health`,
//! `/api/v1/sessions`, `/api/v1/stop/{id}`, `/api/v1/respond/{id}`,
//! `/api/v1/schedules` — so an operator can probe and steer the backend without
//! hand-writing `curl`. The server is the single source of truth; this module
//! only resolves the base URL and pretty-prints responses.

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

/// Guard an id used as a URL path segment: real ids (session ids, schedule
/// ids) are opaque tokens (UUIDs), so reject anything that could traverse or
/// malform the URL rather than encode it. `kind` names the id in the error,
/// e.g. "session id".
fn guard_id_segment(kind: &str, id: &str) -> anyhow::Result<()> {
    if id.is_empty()
        || id == "."
        || id == ".."
        || id.contains(['/', '\\', '?', '#', '%'])
        || id.chars().any(char::is_whitespace)
    {
        anyhow::bail!("invalid {kind}: '{id}'");
    }
    Ok(())
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
            println!(
                "{:<10}{} (HTTP {})",
                "health:".bold(),
                "down".red(),
                r.status()
            );
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
    guard_id_segment("session id", session_id)?;
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
        let msg = if message.is_empty() {
            "stopped"
        } else {
            &message
        };
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

/// `bamboo history <session-id>` — print a session's UI-visible message
/// transcript (a thin read over `GET /api/v1/history/{id}`). Handy for reviewing
/// what a headless `-p` run actually did, or an interactive session's log,
/// without the web UI. Folded in from the retired `bamboo-cli history`.
pub async fn history(conn: ConnArgs, session_id: &str) -> anyhow::Result<()> {
    guard_id_segment("session id", session_id)?;
    let base = conn.api_base();
    let url = format!("{base}/history/{session_id}");
    let resp = reqwest::Client::new()
        .get(&url)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    if resp.status().as_u16() == 404 {
        anyhow::bail!("session '{session_id}' not found");
    }
    if !resp.status().is_success() {
        anyhow::bail!("GET {url} -> HTTP {}", resp.status());
    }
    let v: serde_json::Value = resp.json().await?;
    let messages = v.get("messages").and_then(|m| m.as_array());
    let messages = match messages {
        Some(m) if !m.is_empty() => m,
        _ => {
            println!("(no messages)");
            return Ok(());
        }
    };
    for m in messages {
        let role = m.get("role").and_then(|r| r.as_str()).unwrap_or("?");
        let content = m.get("content").and_then(|c| c.as_str()).unwrap_or("");
        let label = match role {
            "user" => "user".cyan(),
            "assistant" => "assistant".green(),
            "system" => "system".dimmed(),
            "tool" => "tool".yellow(),
            other => other.normal(),
        };
        println!("{label}: {content}");
    }
    // The server caps a cold (non-delta) history fetch and reports the pre-cap
    // count in `total_message_count` (+ `truncated`), so report the TRUE total —
    // `messages.len()` would silently under-count a capped session (#423).
    let total = v
        .get("total_message_count")
        .and_then(|x| x.as_u64())
        .unwrap_or(messages.len() as u64);
    let truncated = v
        .get("truncated")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);
    println!(
        "\n{}",
        history_summary(session_id, messages.len(), total, truncated)
    );
    Ok(())
}

/// The trailing summary line for `bamboo history`: the true total (pre-cap
/// `total_message_count`), plus a truncation note when the server dropped
/// older messages to stay under its cold-fetch cap (#423).
fn history_summary(session_id: &str, shown: usize, total: u64, truncated: bool) -> String {
    if truncated {
        format!(
            "{total} message(s) in session {session_id} (showing the newest {shown}; older messages omitted by the server's history cap)."
        )
    } else {
        format!("{total} message(s) in session {session_id}.")
    }
}

/// `bamboo respond <id> <answer>` — answer a session's pending question
/// (permission gate / clarification) out-of-band via
/// `POST /api/v1/respond/{id}`. Answering resumes the blocked run server-side.
pub async fn respond(conn: ConnArgs, session_id: &str, answer: &str) -> anyhow::Result<()> {
    guard_id_segment("session id", session_id)?;
    let base = conn.api_base();
    let url = format!("{base}/respond/{session_id}");
    let resp = reqwest::Client::new()
        .post(&url)
        .timeout(REQUEST_TIMEOUT)
        // The same shape the frontends send; the server also accepts optional
        // model/provider/reasoning_effort overrides which the CLI leaves unset.
        .json(&serde_json::json!({ "response": answer }))
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if status.is_success() {
        let auto_resume = body
            .get("auto_resume_status")
            .and_then(|s| s.as_str())
            .unwrap_or("unknown");
        println!(
            "{} response recorded; the run resumes server-side (auto-resume: {auto_resume}).",
            "✓".green()
        );
        if let Some(run_id) = body.get("run_id").and_then(|r| r.as_str()) {
            println!("run id: {run_id}");
        }
        return Ok(());
    }
    if status.as_u16() == 404 {
        anyhow::bail!("session '{session_id}' not found");
    }
    let error = body.get("error").and_then(|e| e.as_str()).unwrap_or("");
    if status.as_u16() == 400 && error.contains("No pending question") {
        anyhow::bail!(
            "session '{session_id}' has no pending question — nothing to answer \
             (check with: bamboo respond {session_id} --pending)"
        );
    }
    let detail = body.get("message").and_then(|m| m.as_str()).unwrap_or("");
    anyhow::bail!(
        "respond failed: HTTP {status}{}{}",
        if error.is_empty() {
            String::new()
        } else {
            format!(" {error}")
        },
        if detail.is_empty() {
            String::new()
        } else {
            format!(" ({detail})")
        }
    );
}

/// `bamboo respond <id> --pending` — show the question a session is blocked on
/// (`GET /api/v1/respond/{id}/pending`), pretty or as raw JSON.
pub async fn respond_pending(conn: ConnArgs, session_id: &str, json: bool) -> anyhow::Result<()> {
    guard_id_segment("session id", session_id)?;
    let base = conn.api_base();
    let url = format!("{base}/respond/{session_id}/pending");
    let resp = reqwest::Client::new()
        .get(&url)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    if resp.status().as_u16() == 404 {
        anyhow::bail!("session '{session_id}' not found");
    }
    if !resp.status().is_success() {
        anyhow::bail!("GET {url} -> HTTP {}", resp.status());
    }
    let v: serde_json::Value = resp.json().await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&v)?);
        return Ok(());
    }
    match format_pending_question(session_id, &v) {
        Some(text) => println!("{text}"),
        None => println!("no pending question for session '{session_id}'."),
    }
    Ok(())
}

/// Pretty-print the `GET /respond/{id}/pending` payload, or `None` when the
/// session has no pending question.
fn format_pending_question(session_id: &str, v: &serde_json::Value) -> Option<String> {
    if !v
        .get("has_pending_question")
        .and_then(|b| b.as_bool())
        .unwrap_or(false)
    {
        return None;
    }
    let question = v.get("question").and_then(|q| q.as_str()).unwrap_or("");
    let mut out = format!("session:  {session_id}\nquestion: {question}\n");
    if let Some(options) = v.get("options").and_then(|o| o.as_array()) {
        if !options.is_empty() {
            out.push_str("options:\n");
            for (i, opt) in options.iter().enumerate() {
                let opt = opt
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| opt.to_string());
                out.push_str(&format!("  {}. {opt}\n", i + 1));
            }
        }
    }
    if v.get("allow_custom")
        .and_then(|b| b.as_bool())
        .unwrap_or(false)
    {
        out.push_str("(custom free-text answers are allowed)\n");
    }
    if let Some(tool) = v
        .get("tool_name")
        .and_then(|t| t.as_str())
        .filter(|t| !t.is_empty())
    {
        out.push_str(&format!("tool:     {tool}\n"));
    }
    out.push_str(&format!(
        "\nAnswer with: bamboo respond {session_id} \"<answer>\" — answering resumes the run server-side."
    ));
    Some(out)
}

/// `bamboo session show <id>` — one session's detail
/// (`GET /api/v1/sessions/{id}`), pretty or as raw JSON.
pub async fn session_show(conn: ConnArgs, session_id: &str, json: bool) -> anyhow::Result<()> {
    guard_id_segment("session id", session_id)?;
    let base = conn.api_base();
    let url = format!("{base}/sessions/{session_id}");
    let resp = reqwest::Client::new()
        .get(&url)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    if resp.status().as_u16() == 404 {
        anyhow::bail!("session '{session_id}' not found");
    }
    if !resp.status().is_success() {
        anyhow::bail!("GET {url} -> HTTP {}", resp.status());
    }
    let v: serde_json::Value = resp.json().await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&v)?);
        return Ok(());
    }
    let session = v.get("session").unwrap_or(&v);
    println!("{}", format_session_detail(session));
    Ok(())
}

/// Pretty-print the `session` object of `GET /sessions/{id}` as label/value
/// lines (same visual style as `bamboo status`). Optional fields are only
/// printed when present so the output stays one screen.
fn format_session_detail(s: &serde_json::Value) -> String {
    let str_field = |key: &str| s.get(key).and_then(|x| x.as_str()).unwrap_or("");
    let mut lines: Vec<String> = Vec::new();
    let mut push = |label: &str, value: String| {
        // Pad BEFORE colorizing: ANSI escapes would otherwise count against the
        // column width (same caveat as the `sessions` table above).
        let label = format!("{:<16}", format!("{label}:"));
        lines.push(format!("{}{value}", label.bold()));
    };

    push("id", str_field("id").to_string());
    push("title", str_field("title").to_string());
    push("kind", str_field("kind").to_string());
    let model = str_field("model").to_string();
    let model = match s.get("provider").and_then(|p| p.as_str()) {
        Some(provider) if !provider.is_empty() => format!("{provider}:{model}"),
        _ => model,
    };
    push("model", model);
    let running = s
        .get("is_running")
        .and_then(|b| b.as_bool())
        .unwrap_or(false);
    push(
        "running",
        if running {
            "yes".green().to_string()
        } else {
            "no".to_string()
        },
    );
    if let Some(status) = s.get("last_run_status").and_then(|x| x.as_str()) {
        let mut line = status.to_string();
        if let Some(err) = s.get("last_run_error").and_then(|x| x.as_str()) {
            line.push_str(&format!(" ({err})"));
        }
        push("last run", line);
    }
    let pending = s
        .get("has_pending_question")
        .and_then(|b| b.as_bool())
        .unwrap_or(false);
    push(
        "pending q",
        if pending {
            "yes (see: bamboo respond <id> --pending)"
                .yellow()
                .to_string()
        } else {
            "no".to_string()
        },
    );
    push(
        "messages",
        s.get("message_count")
            .and_then(|x| x.as_u64())
            .unwrap_or(0)
            .to_string(),
    );
    if s.get("pinned").and_then(|b| b.as_bool()).unwrap_or(false) {
        push("pinned", "yes".to_string());
    }
    if let Some(parent) = s.get("parent_session_id").and_then(|x| x.as_str()) {
        push("parent", parent.to_string());
    }
    let child_count = s
        .get("running_child_count")
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    if child_count > 0 {
        push("children", format!("{child_count} running"));
    }
    push("created", str_field("created_at").to_string());
    push("last activity", str_field("last_activity_at").to_string());
    if let Some(placement) = s.get("placement") {
        let kind = placement.get("kind").and_then(|x| x.as_str()).unwrap_or("");
        let host = placement.get("host").and_then(|x| x.as_str()).unwrap_or("");
        if !kind.is_empty() || !host.is_empty() {
            push("placement", format!("{kind} @ {host}"));
        }
    }
    lines.join("\n")
}

/// `bamboo session delete <id>` — delete a session
/// (`DELETE /api/v1/sessions/{id}`), cancelling any running execution.
/// Prompts for confirmation unless `yes` is set.
pub async fn session_delete(conn: ConnArgs, session_id: &str, yes: bool) -> anyhow::Result<()> {
    guard_id_segment("session id", session_id)?;
    if !yes && !confirm(&format!(
        "Delete session '{session_id}'? This cancels any running execution and removes it permanently."
    ))? {
        println!("aborted (nothing deleted).");
        return Ok(());
    }
    let base = conn.api_base();
    let url = format!("{base}/sessions/{session_id}");
    let resp = reqwest::Client::new()
        .delete(&url)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    let status = resp.status();
    if status.is_success() {
        println!("{} session '{session_id}' deleted", "✓".green());
        return Ok(());
    }
    if status.as_u16() == 404 {
        anyhow::bail!("session '{session_id}' not found");
    }
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    let error = body.get("error").and_then(|e| e.as_str()).unwrap_or("");
    anyhow::bail!(
        "delete failed: HTTP {status}{}",
        if error.is_empty() {
            String::new()
        } else {
            format!(" ({error})")
        }
    );
}

/// Ask a yes/no question on stdout and read the answer from stdin. Defaults to
/// "no" on anything but an explicit y/yes — including EOF (non-TTY pipe), so a
/// script that forgets `--yes` aborts instead of deleting.
fn confirm(prompt: &str) -> anyhow::Result<bool> {
    use std::io::Write as _;
    print!("{prompt} [y/N] ");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    let answer = line.trim().to_ascii_lowercase();
    Ok(answer == "y" || answer == "yes")
}

// ---------------------------------------------------------------------------
// `bamboo schedules ...` — timed tasks on a running server (/api/v1/schedules).
// The scheduler creates a fresh session per fire and (when `auto_execute` is
// set with a task message) runs it unattended.
// ---------------------------------------------------------------------------

/// Flag-based inputs for `bamboo schedules create` (the common case). The
/// clap layer guarantees exactly one trigger source (`cron` / `every` /
/// `daily` / `json`) and that `name` + `prompt` accompany the flag form.
#[derive(Debug, Clone, Default)]
pub struct ScheduleCreateArgs {
    /// Schedule name (required unless `json`).
    pub name: Option<String>,
    /// Cron trigger expression (seconds-first, as the trigger engine parses it).
    pub cron: Option<String>,
    /// Interval trigger: fire every N seconds.
    pub every: Option<u64>,
    /// Daily trigger at `HH:MM[:SS]`.
    pub daily: Option<String>,
    /// Task prompt: the fired session's user message, auto-executed.
    pub prompt: Option<String>,
    /// Model override `provider:model` for the fired session.
    pub model: Option<String>,
    /// Workspace directory for the fired session's file tools.
    pub workspace: Option<String>,
    /// IANA timezone for wall-clock triggers (daily/cron).
    pub timezone: Option<String>,
    /// Create the schedule disabled (it will not fire until enabled).
    pub disabled: bool,
    /// Raw `CreateScheduleRequest` JSON: a file path or `-` for stdin. Full
    /// fidelity escape hatch — posted verbatim.
    pub json: Option<String>,
}

/// `bamboo schedules list` — tabulate schedules on a running server.
pub async fn schedules_list(conn: ConnArgs, json: bool) -> anyhow::Result<()> {
    let base = conn.api_base();
    let body = get_json(&base, &format!("{base}/schedules")).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&body)?);
        return Ok(());
    }
    let schedules = body.get("schedules").and_then(|s| s.as_array());
    let schedules = match schedules {
        Some(s) if !s.is_empty() => s,
        _ => {
            println!("(no schedules)");
            return Ok(());
        }
    };

    // Plain (un-colored) cells so the column widths line up.
    println!(
        "{:<38} {:<4} {:<24} {:<20} {:<20} NAME",
        "SCHEDULE ID", "ON", "TRIGGER", "NEXT RUN", "LAST RUN"
    );
    for s in schedules {
        let id = s.get("id").and_then(|x| x.as_str()).unwrap_or("?");
        let enabled = s.get("enabled").and_then(|b| b.as_bool()).unwrap_or(false);
        let trigger = s.get("trigger").map(trigger_summary).unwrap_or_default();
        let state = s.get("state");
        let next = state.and_then(|st| st.get("next_fire_at"));
        let last = state.and_then(|st| st.get("last_started_at"));
        let name = s.get("name").and_then(|x| x.as_str()).unwrap_or("");
        println!(
            "{:<38} {:<4} {:<24} {:<20} {:<20} {}",
            id,
            if enabled { "on" } else { "off" },
            truncate(&trigger, 24),
            fmt_ts(next),
            fmt_ts(last),
            truncate(name, 40)
        );
    }
    println!(
        "\n{} schedule(s). Inspect one with: {}",
        schedules.len(),
        "bamboo schedules show <id>".cyan()
    );
    Ok(())
}

/// `bamboo schedules show <id>` — one schedule in detail. The server exposes
/// no single-schedule GET, so this filters `GET /api/v1/schedules` by id.
pub async fn schedules_show(conn: ConnArgs, schedule_id: &str, json: bool) -> anyhow::Result<()> {
    guard_id_segment("schedule id", schedule_id)?;
    let base = conn.api_base();
    let schedule = find_schedule(&base, schedule_id).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&schedule)?);
        return Ok(());
    }

    let str_of = |v: &serde_json::Value| v.as_str().map(str::to_string);
    let field = |key: &str| schedule.get(key).and_then(str_of).unwrap_or_default();
    let enabled = schedule
        .get("enabled")
        .and_then(|b| b.as_bool())
        .unwrap_or(false);
    println!("{:<16}{}", "id:".bold(), field("id"));
    println!("{:<16}{}", "name:".bold(), field("name"));
    println!(
        "{:<16}{}",
        "enabled:".bold(),
        if enabled {
            "true".green()
        } else {
            "false".red()
        }
    );
    if let Some(trigger) = schedule.get("trigger") {
        println!("{:<16}{}", "trigger:".bold(), trigger_summary(trigger));
    }
    for key in ["timezone", "start_at", "end_at"] {
        if let Some(value) = schedule.get(key).and_then(|v| v.as_str()) {
            println!("{:<16}{value}", format!("{key}:").bold());
        }
    }
    for key in ["misfire_policy", "overlap_policy"] {
        if let Some(value) = schedule.get(key) {
            let rendered = value
                .get("type")
                .and_then(|t| t.as_str())
                .map(str::to_string)
                .or_else(|| str_of(value))
                .unwrap_or_else(|| value.to_string());
            println!("{:<16}{rendered}", format!("{key}:").bold());
        }
    }
    if let Some(state) = schedule.get("state") {
        println!(
            "{:<16}{}",
            "next fire:".bold(),
            fmt_ts(state.get("next_fire_at"))
        );
        println!(
            "{:<16}{}",
            "last started:".bold(),
            fmt_ts(state.get("last_started_at"))
        );
        println!(
            "{:<16}{}",
            "last success:".bold(),
            fmt_ts(state.get("last_success_at"))
        );
        let count = |key: &str| state.get(key).and_then(|v| v.as_u64()).unwrap_or(0);
        println!(
            "{:<16}{} total, {} ok, {} failed, {} missed ({} queued, {} running now)",
            "runs:".bold(),
            count("total_run_count"),
            count("total_success_count"),
            count("total_failure_count"),
            count("total_missed_count"),
            count("queued_run_count"),
            count("running_run_count"),
        );
    }
    if let Some(rc) = schedule.get("run_config") {
        let rc_str = |key: &str| rc.get(key).and_then(|v| v.as_str());
        if let Some(task) = rc_str("task_message") {
            println!("{:<16}{}", "prompt:".bold(), truncate(task, 120));
        }
        for (label, key) in [
            ("model:", "model"),
            ("workspace:", "workspace_path"),
            ("reasoning:", "reasoning_effort"),
        ] {
            if let Some(value) = rc_str(key) {
                println!("{:<16}{value}", label.bold());
            }
        }
        let auto = rc
            .get("auto_execute")
            .and_then(|b| b.as_bool())
            .unwrap_or(false);
        println!("{:<16}{auto}", "auto-execute:".bold());
    }
    println!(
        "{:<16}{}   {:<10}{}",
        "created:".bold(),
        fmt_ts(schedule.get("created_at")),
        "updated:".bold(),
        fmt_ts(schedule.get("updated_at"))
    );
    Ok(())
}

/// `bamboo schedules create` — POST /api/v1/schedules. Either assembles the
/// request from the common-case flags or posts a raw JSON payload verbatim
/// (`--json <file|->`) for full schema fidelity.
pub async fn schedules_create(conn: ConnArgs, args: ScheduleCreateArgs) -> anyhow::Result<()> {
    let payload = match &args.json {
        Some(source) => read_json_payload(source)?,
        None => build_create_payload(&args)?,
    };

    let base = conn.api_base();
    let url = format!("{base}/schedules");
    let resp = reqwest::Client::new()
        .post(&url)
        .timeout(REQUEST_TIMEOUT)
        .json(&payload)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if !status.is_success() {
        anyhow::bail!(
            "create failed: HTTP {status} {}",
            server_error_message(&body)
        );
    }
    let id = body.get("id").and_then(|x| x.as_str()).unwrap_or("?");
    let name = body.get("name").and_then(|x| x.as_str()).unwrap_or("");
    let enabled = body
        .get("enabled")
        .and_then(|b| b.as_bool())
        .unwrap_or(false);
    let trigger = body.get("trigger").map(trigger_summary).unwrap_or_default();
    println!(
        "{} created schedule {id} ('{name}', {trigger}, {})",
        "✓".green(),
        if enabled { "enabled" } else { "disabled" }
    );
    if let Some(next) = body.get("state").and_then(|st| st.get("next_fire_at")) {
        println!("  next fire: {}", fmt_ts(Some(next)));
    }
    Ok(())
}

/// `bamboo schedules delete <id>` — DELETE /api/v1/schedules/{id}. Confirms
/// interactively (after resolving the schedule's name) unless `yes` is set.
pub async fn schedules_delete(conn: ConnArgs, schedule_id: &str, yes: bool) -> anyhow::Result<()> {
    guard_id_segment("schedule id", schedule_id)?;
    let base = conn.api_base();
    if !yes {
        // Resolve the name first so the operator confirms the right thing
        // (this also fails fast on an unknown id before prompting).
        let schedule = find_schedule(&base, schedule_id).await?;
        let name = schedule.get("name").and_then(|x| x.as_str()).unwrap_or("?");
        if !confirm(&format!("Delete schedule '{name}' ({schedule_id})?"))? {
            println!("aborted (nothing deleted).");
            return Ok(());
        }
    }
    let url = format!("{base}/schedules/{schedule_id}");
    let resp = reqwest::Client::new()
        .delete(&url)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if status.as_u16() == 404 {
        anyhow::bail!("schedule '{schedule_id}' not found");
    }
    if !status.is_success() {
        anyhow::bail!(
            "delete failed: HTTP {status} {}",
            server_error_message(&body)
        );
    }
    println!("{} deleted schedule {schedule_id}", "✓".green());
    Ok(())
}

/// `bamboo schedules run <id>` — POST /api/v1/schedules/{id}/run (trigger now).
pub async fn schedules_run(conn: ConnArgs, schedule_id: &str) -> anyhow::Result<()> {
    guard_id_segment("schedule id", schedule_id)?;
    let base = conn.api_base();
    let url = format!("{base}/schedules/{schedule_id}/run");
    let resp = reqwest::Client::new()
        .post(&url)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if status.as_u16() == 404 {
        anyhow::bail!("schedule '{schedule_id}' not found");
    }
    if !status.is_success() {
        anyhow::bail!("run failed: HTTP {status} {}", server_error_message(&body));
    }
    let run_id = body.get("run_id").and_then(|x| x.as_str()).unwrap_or("?");
    println!(
        "{} run {run_id} enqueued (watch it with: {})",
        "✓".green(),
        format!("bamboo schedules runs {schedule_id}").cyan()
    );
    Ok(())
}

/// `bamboo schedules runs <id>` — run history (GET /api/v1/schedules/{id}/runs).
pub async fn schedules_runs(conn: ConnArgs, schedule_id: &str, json: bool) -> anyhow::Result<()> {
    guard_id_segment("schedule id", schedule_id)?;
    let base = conn.api_base();
    let body = get_json(&base, &format!("{base}/schedules/{schedule_id}/runs")).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&body)?);
        return Ok(());
    }
    let runs = body.get("runs").and_then(|r| r.as_array());
    let runs = match runs {
        Some(r) if !r.is_empty() => r,
        _ => {
            println!("(no runs)");
            return Ok(());
        }
    };
    println!(
        "{:<38} {:<9} {:<20} {:<20} {:>9}  SESSION",
        "RUN ID", "STATUS", "SCHEDULED FOR", "STARTED", "DURATION"
    );
    for r in runs {
        let run_id = r.get("run_id").and_then(|x| x.as_str()).unwrap_or("?");
        let run_status = r.get("status").and_then(|x| x.as_str()).unwrap_or("?");
        let duration = r
            .get("execution_duration_ms")
            .and_then(|x| x.as_u64())
            .map(|ms| format!("{ms}ms"))
            .unwrap_or_else(|| "-".to_string());
        let session = r.get("session_id").and_then(|x| x.as_str()).unwrap_or("-");
        println!(
            "{:<38} {:<9} {:<20} {:<20} {:>9}  {}",
            run_id,
            run_status,
            fmt_ts(r.get("scheduled_for")),
            fmt_ts(r.get("started_at")),
            duration,
            session
        );
    }
    println!("\n{} run(s) for schedule {schedule_id}.", runs.len());
    Ok(())
}

/// GET `url` and parse the JSON body, with the shared unreachable/HTTP-status
/// error surface.
async fn get_json(base: &str, url: &str) -> anyhow::Result<serde_json::Value> {
    let resp = reqwest::Client::new()
        .get(url)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(base, e))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if !status.is_success() {
        anyhow::bail!("GET {url} -> HTTP {status} {}", server_error_message(&body));
    }
    Ok(body)
}

/// Resolve one schedule by exact id from the list endpoint (the server has no
/// single-schedule GET).
async fn find_schedule(base: &str, schedule_id: &str) -> anyhow::Result<serde_json::Value> {
    let body = get_json(base, &format!("{base}/schedules")).await?;
    body.get("schedules")
        .and_then(|s| s.as_array())
        .and_then(|schedules| {
            schedules
                .iter()
                .find(|s| s.get("id").and_then(|x| x.as_str()) == Some(schedule_id))
        })
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "schedule '{schedule_id}' not found (list them with: bamboo schedules list)"
            )
        })
}

/// Assemble a `CreateScheduleRequest` body from the flag-based inputs.
fn build_create_payload(args: &ScheduleCreateArgs) -> anyhow::Result<serde_json::Value> {
    let name = args
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--name is required (or pass --json)"))?;
    let prompt = args
        .prompt
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--prompt is required (or pass --json)"))?;

    let trigger = if let Some(expr) = &args.cron {
        serde_json::json!({ "type": "cron", "expr": expr })
    } else if let Some(every_seconds) = args.every {
        serde_json::json!({ "type": "interval", "every_seconds": every_seconds })
    } else if let Some(hms) = &args.daily {
        let (hour, minute, second) = parse_daily_time(hms)?;
        serde_json::json!({ "type": "daily", "hour": hour, "minute": minute, "second": second })
    } else {
        anyhow::bail!("a trigger is required: --cron <expr>, --every <seconds>, or --daily <HH:MM[:SS]> (or pass --json)");
    };

    // The fired session's task message auto-executes: a schedule created from
    // the CLI's flag form exists to run an agent, not just to park a session.
    let mut run_config = serde_json::json!({
        "task_message": prompt,
        "auto_execute": true,
    });
    if let Some(model) = &args.model {
        run_config["model"] = serde_json::json!(model);
    }
    if let Some(workspace) = &args.workspace {
        run_config["workspace_path"] = serde_json::json!(workspace);
    }

    let mut payload = serde_json::json!({
        "name": name,
        "trigger": trigger,
        "enabled": !args.disabled,
        "run_config": run_config,
    });
    if let Some(timezone) = &args.timezone {
        payload["timezone"] = serde_json::json!(timezone);
    }
    Ok(payload)
}

/// Parse `HH:MM` / `HH:MM:SS` for the `--daily` trigger.
fn parse_daily_time(value: &str) -> anyhow::Result<(u8, u8, u8)> {
    let bad = || anyhow::anyhow!("invalid --daily time '{value}' (expected HH:MM or HH:MM:SS)");
    let parts: Vec<&str> = value.split(':').collect();
    if parts.len() != 2 && parts.len() != 3 {
        return Err(bad());
    }
    let hour: u8 = parts[0].parse().map_err(|_| bad())?;
    let minute: u8 = parts[1].parse().map_err(|_| bad())?;
    let second: u8 = if parts.len() == 3 {
        parts[2].parse().map_err(|_| bad())?
    } else {
        0
    };
    if hour > 23 || minute > 59 || second > 59 {
        return Err(bad());
    }
    Ok((hour, minute, second))
}

/// Read a raw JSON payload from a file path or stdin (`-`).
fn read_json_payload(source: &str) -> anyhow::Result<serde_json::Value> {
    let text = if source == "-" {
        use std::io::Read as _;
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        buf
    } else {
        std::fs::read_to_string(source)
            .map_err(|e| anyhow::anyhow!("failed to read '{source}': {e}"))?
    };
    serde_json::from_str(text.trim()).map_err(|e| anyhow::anyhow!("payload is not valid JSON: {e}"))
}

/// Pull the server's `{"error": "..."}` detail out of an error body, if any.
fn server_error_message(body: &serde_json::Value) -> String {
    body.get("error")
        .and_then(|e| e.as_str())
        .map(|e| format!("({e})"))
        .unwrap_or_default()
}

/// One-line human summary of a `ScheduleTrigger` JSON value.
fn trigger_summary(trigger: &serde_json::Value) -> String {
    let joined = |key: &str| {
        trigger
            .get(key)
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .map(|d| match d {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default()
    };
    let hm = || {
        format!(
            "{:02}:{:02}",
            trigger.get("hour").and_then(|v| v.as_u64()).unwrap_or(0),
            trigger.get("minute").and_then(|v| v.as_u64()).unwrap_or(0)
        )
    };
    match trigger.get("type").and_then(|t| t.as_str()) {
        Some("interval") => format!(
            "every {}s",
            trigger
                .get("every_seconds")
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
        ),
        Some("daily") => format!(
            "daily {}:{:02}",
            hm(),
            trigger.get("second").and_then(|v| v.as_u64()).unwrap_or(0)
        ),
        Some("weekly") => format!("weekly {} {}", joined("weekdays"), hm()),
        Some("monthly") => format!("monthly {} {}", joined("days"), hm()),
        Some("cron") => format!(
            "cron '{}'",
            trigger.get("expr").and_then(|v| v.as_str()).unwrap_or("?")
        ),
        _ => trigger.to_string(),
    }
}

/// Render an RFC3339 timestamp value as `YYYY-MM-DD HH:MM:SS` (UTC); `-` when
/// absent. Keeps table columns narrow without a chrono dependency here.
fn fmt_ts(value: Option<&serde_json::Value>) -> String {
    let Some(s) = value.and_then(|v| v.as_str()) else {
        return "-".to_string();
    };
    // "2026-07-10T12:34:56.789Z" -> "2026-07-10 12:34:56"
    if s.len() >= 19 && s.is_char_boundary(19) && s.as_bytes().get(10) == Some(&b'T') {
        format!("{} {}", &s[..10], &s[11..19])
    } else {
        s.to_string()
    }
}

// ---------------------------------------------------------------------------
// `bamboo mcp status|connect|disconnect|refresh|tools|add|remove` — MCP server
// management on a running instance (/api/v1/mcp). The offline config view
// stays `bamboo mcp list` (read_cli).
// ---------------------------------------------------------------------------

/// Startup/refresh of an MCP server can take a while (stdio child spawn +
/// initialize handshake, default startup timeout 20s), so the mutating MCP
/// verbs get a more generous budget than the plain reads.
const MCP_MUTATE_TIMEOUT: Duration = Duration::from_secs(60);

/// `bamboo mcp status` — live view over `GET /api/v1/mcp/servers`: connection
/// state + tool counts per configured server. `--json` prints the raw response.
pub async fn mcp_status(conn: ConnArgs, json: bool) -> anyhow::Result<()> {
    let base = conn.api_base();
    let url = format!("{base}/mcp/servers");
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

    let servers = v.get("servers").and_then(|s| s.as_array());
    let servers = match servers {
        Some(s) if !s.is_empty() => s,
        _ => {
            println!("(no MCP servers configured)");
            return Ok(());
        }
    };

    // Plain (un-colored) cells so the column widths line up.
    println!(
        "{:<24} {:<8} {:<14} {:>5}  NAME",
        "ID", "ENABLED", "STATUS", "TOOLS"
    );
    for s in servers {
        let id = s.get("id").and_then(|x| x.as_str()).unwrap_or("?");
        let enabled = s.get("enabled").and_then(|b| b.as_bool()).unwrap_or(false);
        let status = s.get("status").and_then(|x| x.as_str()).unwrap_or("?");
        let tools = s.get("tool_count").and_then(|x| x.as_u64()).unwrap_or(0);
        let name = s.get("name").and_then(|x| x.as_str()).unwrap_or("");
        println!(
            "{:<24} {:<8} {:<14} {:>5}  {}",
            truncate(id, 24),
            if enabled { "yes" } else { "no" },
            truncate(status, 14),
            tools,
            truncate(name, 40)
        );
    }
    for s in servers {
        if let Some(err) = s.get("last_error").and_then(|e| e.as_str()) {
            if !err.trim().is_empty() {
                let id = s.get("id").and_then(|x| x.as_str()).unwrap_or("?");
                println!("{} {id}: {}", "!".yellow(), truncate(err, 100));
            }
        }
    }
    let connected = servers
        .iter()
        .filter(|s| s.get("status").and_then(|x| x.as_str()) == Some("connected"))
        .count();
    println!("\n{connected} connected of {}.", servers.len());
    Ok(())
}

/// `bamboo mcp connect <id>` — enable + (re)connect a configured server
/// (`POST /api/v1/mcp/servers/{id}/connect`).
pub async fn mcp_connect(conn: ConnArgs, server_id: &str) -> anyhow::Result<()> {
    guard_id_segment("MCP server id", server_id)?;
    let base = conn.api_base();
    let url = format!("{base}/mcp/servers/{server_id}/connect");
    let resp = reqwest::Client::new()
        .post(&url)
        .timeout(MCP_MUTATE_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if status.is_success() {
        println!("{} server '{server_id}' connected", "✓".green());
        Ok(())
    } else if status.as_u16() == 404 {
        anyhow::bail!("MCP server '{server_id}' not found (check `bamboo mcp status`)");
    } else {
        anyhow::bail!(
            "connect failed: HTTP {status} {}",
            server_error_message(&body)
        );
    }
}

/// `bamboo mcp disconnect <id>` — disable + disconnect a server
/// (`POST /api/v1/mcp/servers/{id}/disconnect`).
pub async fn mcp_disconnect(conn: ConnArgs, server_id: &str) -> anyhow::Result<()> {
    guard_id_segment("MCP server id", server_id)?;
    let base = conn.api_base();
    let url = format!("{base}/mcp/servers/{server_id}/disconnect");
    let resp = reqwest::Client::new()
        .post(&url)
        .timeout(MCP_MUTATE_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if status.is_success() {
        println!("{} server '{server_id}' disconnected", "✓".green());
        Ok(())
    } else if status.as_u16() == 404 {
        anyhow::bail!("MCP server '{server_id}' not found (check `bamboo mcp status`)");
    } else {
        anyhow::bail!(
            "disconnect failed: HTTP {status} {}",
            server_error_message(&body)
        );
    }
}

/// `bamboo mcp refresh [<id>]` — re-list tools from one server, or from every
/// enabled server when no id is given (`POST /api/v1/mcp/servers/{id}/refresh`).
pub async fn mcp_refresh(conn: ConnArgs, server_id: Option<&str>) -> anyhow::Result<()> {
    let base = conn.api_base();
    let client = reqwest::Client::new();

    let targets: Vec<String> = match server_id {
        Some(id) => {
            guard_id_segment("MCP server id", id)?;
            vec![id.to_string()]
        }
        None => {
            // Refresh every ENABLED server (a disabled one has nothing to list).
            let url = format!("{base}/mcp/servers");
            let resp = client
                .get(&url)
                .timeout(REQUEST_TIMEOUT)
                .send()
                .await
                .map_err(|e| unreachable(&base, e))?;
            if !resp.status().is_success() {
                anyhow::bail!("GET {url} -> HTTP {}", resp.status());
            }
            let v: serde_json::Value = resp.json().await?;
            let ids: Vec<String> = v
                .get("servers")
                .and_then(|s| s.as_array())
                .map(|servers| {
                    servers
                        .iter()
                        .filter(|s| s.get("enabled").and_then(|b| b.as_bool()).unwrap_or(false))
                        .filter_map(|s| s.get("id").and_then(|x| x.as_str()))
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            if ids.is_empty() {
                println!("(no enabled MCP servers to refresh)");
                return Ok(());
            }
            ids
        }
    };

    let mut failures = 0usize;
    for id in &targets {
        let url = format!("{base}/mcp/servers/{id}/refresh");
        let result = client.post(&url).timeout(MCP_MUTATE_TIMEOUT).send().await;
        match result {
            Ok(resp) if resp.status().is_success() => {
                let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
                let tools = body.get("tool_count").and_then(|t| t.as_u64()).unwrap_or(0);
                println!("{} {id}: {tools} tool(s)", "✓".green());
            }
            Ok(resp) => {
                let status = resp.status();
                let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
                println!(
                    "{} {id}: HTTP {status} {}",
                    "✗".red(),
                    server_error_message(&body)
                );
                failures += 1;
            }
            Err(e) => {
                println!("{} {id}: {e}", "✗".red());
                failures += 1;
            }
        }
    }
    if failures > 0 {
        anyhow::bail!("{failures} of {} refresh(es) failed", targets.len());
    }
    Ok(())
}

/// `bamboo mcp tools [<id>]` — list the tools one server exposes
/// (`GET /api/v1/mcp/servers/{id}/tools`), or every server's tools
/// (`GET /api/v1/mcp/tools`) when no id is given. `--json` prints raw JSON.
pub async fn mcp_tools(conn: ConnArgs, server_id: Option<&str>, json: bool) -> anyhow::Result<()> {
    let base = conn.api_base();
    let url = match server_id {
        Some(id) => {
            guard_id_segment("MCP server id", id)?;
            format!("{base}/mcp/servers/{id}/tools")
        }
        None => format!("{base}/mcp/tools"),
    };
    let resp = reqwest::Client::new()
        .get(&url)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    if resp.status().as_u16() == 404 {
        anyhow::bail!(
            "MCP server '{}' not found (check `bamboo mcp status`)",
            server_id.unwrap_or("?")
        );
    }
    if !resp.status().is_success() {
        anyhow::bail!("GET {url} -> HTTP {}", resp.status());
    }
    let v: serde_json::Value = resp.json().await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&v)?);
        return Ok(());
    }

    let tools = v.get("tools").and_then(|t| t.as_array());
    let tools = match tools {
        Some(t) if !t.is_empty() => t,
        _ => {
            println!("(no tools)");
            return Ok(());
        }
    };
    println!("{:<32} {:<20} DESCRIPTION", "ALIAS", "SERVER");
    for t in tools {
        let alias = t.get("alias").and_then(|x| x.as_str()).unwrap_or("?");
        let server = t.get("server_id").and_then(|x| x.as_str()).unwrap_or("");
        let desc = t.get("description").and_then(|x| x.as_str()).unwrap_or("");
        println!(
            "{:<32} {:<20} {}",
            truncate(alias, 32),
            truncate(server, 20),
            truncate(desc, 70)
        );
    }
    println!("\n{} tool(s).", tools.len());
    Ok(())
}

/// `bamboo mcp add --json <file|->` — add (or overwrite) a server from a raw
/// JSON payload passed through to `POST /api/v1/mcp/servers`. The payload may
/// be the internal shape or the mainstream flat shape (`command`/`url`), same
/// as the HTTP API.
pub async fn mcp_add(conn: ConnArgs, payload_source: &str) -> anyhow::Result<()> {
    // Fail fast on malformed JSON instead of bouncing off the server.
    let payload = read_json_payload(payload_source)?;

    let base = conn.api_base();
    let url = format!("{base}/mcp/servers");
    let resp = reqwest::Client::new()
        .post(&url)
        .timeout(MCP_MUTATE_TIMEOUT)
        .json(&payload)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if status.is_success() {
        let id = body
            .get("server_id")
            .and_then(|s| s.as_str())
            .unwrap_or("?");
        println!("{} server '{id}' saved", "✓".green());
        Ok(())
    } else {
        anyhow::bail!("add failed: HTTP {status} {}", server_error_message(&body));
    }
}

/// `bamboo mcp remove <id>` — stop + delete a server
/// (`DELETE /api/v1/mcp/servers/{id}`). Destructive (the stored config is
/// gone), though a removed server can be re-added with `bamboo mcp add` — so
/// it confirms like `sessions delete` / `schedules delete` unless `--yes`.
pub async fn mcp_remove(conn: ConnArgs, server_id: &str, yes: bool) -> anyhow::Result<()> {
    guard_id_segment("MCP server id", server_id)?;
    if !yes
        && !confirm(&format!(
            "Remove MCP server '{server_id}'? This stops it and deletes its stored config."
        ))?
    {
        println!("aborted (nothing removed).");
        return Ok(());
    }
    let base = conn.api_base();
    let url = format!("{base}/mcp/servers/{server_id}");
    let resp = reqwest::Client::new()
        .delete(&url)
        .timeout(MCP_MUTATE_TIMEOUT)
        .send()
        .await
        .map_err(|e| unreachable(&base, e))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if status.is_success() {
        println!("{} server '{server_id}' removed", "✓".green());
        Ok(())
    } else {
        anyhow::bail!(
            "remove failed: HTTP {status} {}",
            server_error_message(&body)
        );
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

#[cfg(test)]
mod tests {
    use super::{
        build_create_payload, fmt_ts, format_pending_question, format_session_detail,
        guard_id_segment, history_summary, parse_daily_time, server_error_message, trigger_summary,
        ScheduleCreateArgs,
    };

    #[test]
    fn guard_id_segment_rejects_path_hazards() {
        for bad in [
            "", ".", "..", "a/b", "a\\b", "a?b", "a#b", "a%b", "a b", "a\tb",
        ] {
            assert!(
                guard_id_segment("session id", bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
        assert!(guard_id_segment("session id", "0195fd1e-abc4-7def-8123-456789abcdef").is_ok());
        // The error names the id kind so schedule/session messages stay distinct.
        let err = guard_id_segment("schedule id", "a/b").unwrap_err();
        assert!(err.to_string().contains("invalid schedule id"));
    }

    #[test]
    fn history_summary_reports_true_total_and_truncation() {
        // Un-truncated: the plain total (#423 item 2 — the total, not page len).
        assert_eq!(
            history_summary("s1", 3, 3, false),
            "3 message(s) in session s1."
        );
        // Truncated cold fetch: true total + an explicit truncation note.
        let line = history_summary("s1", 2000, 5000, true);
        assert!(line.starts_with("5000 message(s) in session s1"));
        assert!(line.contains("newest 2000"));
        assert!(line.contains("history cap"));
    }

    #[test]
    fn format_pending_question_pretty_prints_question_and_options() {
        colored::control::set_override(false);
        let v = serde_json::json!({
            "has_pending_question": true,
            "question": "Proceed with the deploy?",
            "options": ["Yes", "No"],
            "allow_custom": true,
            "tool_call_id": "tc-1",
            "tool_name": "conclusion_with_options",
            "source": "tool",
        });
        let text = format_pending_question("sess-1", &v).expect("pending question");
        assert!(text.contains("question: Proceed with the deploy?"));
        assert!(text.contains("1. Yes"));
        assert!(text.contains("2. No"));
        assert!(text.contains("custom free-text answers are allowed"));
        assert!(text.contains("tool:     conclusion_with_options"));
        assert!(text.contains("bamboo respond sess-1"));
        assert!(text.contains("resumes the run server-side"));
    }

    #[test]
    fn format_pending_question_none_when_no_question() {
        let v = serde_json::json!({ "has_pending_question": false });
        assert!(format_pending_question("sess-1", &v).is_none());
        // Defensive: an empty/odd body also reads as "no pending question".
        assert!(format_pending_question("sess-1", &serde_json::json!({})).is_none());
    }

    #[test]
    fn format_session_detail_shows_core_and_optional_fields() {
        colored::control::set_override(false);
        let s = serde_json::json!({
            "id": "sess-9",
            "title": "Fix the bug",
            "kind": "root",
            "model": "claude-sonnet-5",
            "provider": "anthropic",
            "is_running": true,
            "last_run_status": "failed",
            "last_run_error": "boom",
            "has_pending_question": true,
            "message_count": 12,
            "pinned": true,
            "running_child_count": 2,
            "created_at": "2026-07-10T00:00:00Z",
            "last_activity_at": "2026-07-10T01:00:00Z",
            "placement": { "kind": "local", "host": "mac.local" },
        });
        let text = format_session_detail(&s);
        assert!(text.contains("sess-9"));
        assert!(text.contains("Fix the bug"));
        assert!(text.contains("anthropic:claude-sonnet-5"));
        assert!(text.contains("failed (boom)"));
        assert!(text.contains("2 running"));
        assert!(text.contains("local @ mac.local"));
        assert!(text.contains("bamboo respond <id> --pending"));

        // Optional fields absent → their lines are omitted entirely.
        let minimal = serde_json::json!({
            "id": "sess-min",
            "title": "",
            "kind": "root",
            "model": "m",
            "is_running": false,
            "message_count": 0,
        });
        let text = format_session_detail(&minimal);
        assert!(!text.contains("last run"));
        assert!(!text.contains("parent"));
        assert!(!text.contains("children"));
        assert!(!text.contains("placement"));
    }
    #[test]
    fn parse_daily_time_accepts_hm_and_hms() {
        assert_eq!(parse_daily_time("09:30").unwrap(), (9, 30, 0));
        assert_eq!(parse_daily_time("23:59:59").unwrap(), (23, 59, 59));
    }

    #[test]
    fn parse_daily_time_rejects_malformed_and_out_of_range() {
        for bad in ["", "9", "24:00", "09:60", "09:30:60", "a:b", "09:30:15:00"] {
            assert!(parse_daily_time(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn build_create_payload_maps_flags_to_request_shape() {
        let payload = build_create_payload(&ScheduleCreateArgs {
            name: Some("nightly".to_string()),
            cron: Some("0 0 2 * * *".to_string()),
            prompt: Some("run the suite".to_string()),
            model: Some("anthropic:claude-sonnet-4".to_string()),
            workspace: Some("/tmp/repo".to_string()),
            timezone: Some("Asia/Shanghai".to_string()),
            ..Default::default()
        })
        .unwrap();

        assert_eq!(payload["name"], "nightly");
        assert_eq!(payload["enabled"], true);
        assert_eq!(payload["trigger"]["type"], "cron");
        assert_eq!(payload["trigger"]["expr"], "0 0 2 * * *");
        assert_eq!(payload["timezone"], "Asia/Shanghai");
        assert_eq!(payload["run_config"]["task_message"], "run the suite");
        assert_eq!(payload["run_config"]["auto_execute"], true);
        assert_eq!(payload["run_config"]["model"], "anthropic:claude-sonnet-4");
        assert_eq!(payload["run_config"]["workspace_path"], "/tmp/repo");
    }

    #[test]
    fn build_create_payload_daily_and_disabled() {
        let payload = build_create_payload(&ScheduleCreateArgs {
            name: Some("standup".to_string()),
            daily: Some("09:30".to_string()),
            prompt: Some("summarize".to_string()),
            disabled: true,
            ..Default::default()
        })
        .unwrap();

        assert_eq!(payload["enabled"], false);
        assert_eq!(payload["trigger"]["type"], "daily");
        assert_eq!(payload["trigger"]["hour"], 9);
        assert_eq!(payload["trigger"]["minute"], 30);
        assert_eq!(payload["trigger"]["second"], 0);
        // Optional fields stay absent so server defaults apply.
        assert!(payload.get("timezone").is_none());
        assert!(payload["run_config"].get("model").is_none());
    }

    #[test]
    fn build_create_payload_interval_trigger() {
        let payload = build_create_payload(&ScheduleCreateArgs {
            name: Some("tick".to_string()),
            every: Some(3600),
            prompt: Some("check".to_string()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(payload["trigger"]["type"], "interval");
        assert_eq!(payload["trigger"]["every_seconds"], 3600);
    }

    #[test]
    fn build_create_payload_requires_name_prompt_and_trigger() {
        assert!(build_create_payload(&ScheduleCreateArgs::default()).is_err());
        assert!(build_create_payload(&ScheduleCreateArgs {
            name: Some("x".to_string()),
            prompt: Some("y".to_string()),
            ..Default::default()
        })
        .is_err());
    }

    #[test]
    fn trigger_summary_renders_each_kind() {
        let case = |json: serde_json::Value| trigger_summary(&json);
        assert_eq!(
            case(serde_json::json!({"type":"interval","every_seconds":60})),
            "every 60s"
        );
        assert_eq!(
            case(serde_json::json!({"type":"daily","hour":9,"minute":30,"second":0})),
            "daily 09:30:00"
        );
        assert_eq!(
            case(serde_json::json!({"type":"weekly","weekdays":["mon","fri"],"hour":9,"minute":0})),
            "weekly mon,fri 09:00"
        );
        assert_eq!(
            case(serde_json::json!({"type":"monthly","days":[1,15],"hour":8,"minute":5})),
            "monthly 1,15 08:05"
        );
        assert_eq!(
            case(serde_json::json!({"type":"cron","expr":"0 0 2 * * *"})),
            "cron '0 0 2 * * *'"
        );
    }

    #[test]
    fn fmt_ts_shortens_rfc3339_and_defaults_to_dash() {
        let value = serde_json::json!("2026-07-10T12:34:56.789012Z");
        assert_eq!(fmt_ts(Some(&value)), "2026-07-10 12:34:56");
        assert_eq!(fmt_ts(None), "-");
        let null = serde_json::Value::Null;
        assert_eq!(fmt_ts(Some(&null)), "-");
    }

    #[test]
    fn server_error_message_extracts_error_field() {
        assert_eq!(
            server_error_message(&serde_json::json!({"error":"name is required"})),
            "(name is required)"
        );
        assert_eq!(server_error_message(&serde_json::Value::Null), "");
    }
}
