//! The `bamboo health | status | sessions | session | stop | respond` admin CLI.
//!
//! A thin HTTP client over a running `bamboo serve` instance. Each command wraps
//! an endpoint the server already exposes — `/api/v1/health`,
//! `/api/v1/sessions`, `/api/v1/stop/{id}`, `/api/v1/respond/{id}` — so an
//! operator can probe and steer the backend without hand-writing `curl`. The
//! server is the single source of truth; this module only resolves the base URL
//! and pretty-prints responses.

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

/// Guard a session id used as a URL path segment: real session ids are opaque
/// tokens (UUIDs), so reject anything that could traverse or malform the URL
/// rather than encode it.
fn guard_session_id(session_id: &str) -> anyhow::Result<()> {
    if session_id.is_empty()
        || session_id == "."
        || session_id == ".."
        || session_id.contains(['/', '\\', '?', '#', '%'])
        || session_id.chars().any(char::is_whitespace)
    {
        anyhow::bail!("invalid session id: '{session_id}'");
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
    guard_session_id(session_id)?;
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
    guard_session_id(session_id)?;
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
    guard_session_id(session_id)?;
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
    guard_session_id(session_id)?;
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
    guard_session_id(session_id)?;
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
    guard_session_id(session_id)?;
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
        format_pending_question, format_session_detail, guard_session_id, history_summary,
    };

    #[test]
    fn guard_session_id_rejects_path_hazards() {
        for bad in [
            "", ".", "..", "a/b", "a\\b", "a?b", "a#b", "a%b", "a b", "a\tb",
        ] {
            assert!(guard_session_id(bad).is_err(), "{bad:?} must be rejected");
        }
        assert!(guard_session_id("0195fd1e-abc4-7def-8123-456789abcdef").is_ok());
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
}
