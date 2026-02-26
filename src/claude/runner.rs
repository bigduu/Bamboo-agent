//! Claude Code CLI runner (streaming).
//!
//! This module spawns `claude` with `--output-format stream-json`, converts the
//! output stream into Bamboo `AgentEvent`s, and registers the process in
//! `ProcessRegistry` for cancellation and monitoring.

use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::agent::core::{AgentEvent, TokenUsage};
use crate::process::ProcessRegistry;

use super::command::create_tokio_command_with_env;
use super::stream_json::ClaudeStreamJsonParser;

#[derive(Debug, Clone)]
pub struct ClaudeCodeCliConfig {
    pub claude_path: String,
    pub project_path: PathBuf,
    pub prompt: String,
    /// A UUID string. Claude Code requires session ids to be valid UUIDs.
    pub session_id: String,
    /// e.g. `http://localhost:8080/anthropic` (Claude will append `/v1/...`)
    pub anthropic_base_url: String,
    /// Optional structured output schema (passed as `--json-schema`).
    pub json_schema: Option<String>,
    /// If true, pass `--dangerously-skip-permissions`.
    pub skip_permissions: bool,
    /// If true, pass `--include-partial-messages` to improve streaming UX.
    pub include_partial_messages: bool,
}

/// Spawn Claude Code CLI and stream its output as `AgentEvent`s.
///
/// Returns the registered `run_id` from the `ProcessRegistry`.
pub async fn spawn_claude_code_cli(
    registry: Arc<ProcessRegistry>,
    event_sender: broadcast::Sender<AgentEvent>,
    cancel_token: CancellationToken,
    config: ClaudeCodeCliConfig,
) -> Result<i64, String> {
    let mut cmd = create_tokio_command_with_env(&config.claude_path);

    cmd.current_dir(&config.project_path);
    cmd.env("ANTHROPIC_BASE_URL", &config.anthropic_base_url);

    // Non-interactive prompt mode.
    cmd.arg("-p").arg(&config.prompt);
    cmd.arg("--output-format").arg("stream-json");
    // Claude Code requires `--verbose` when using `--print/-p` with `--output-format=stream-json`.
    cmd.arg("--verbose");

    if config.include_partial_messages {
        cmd.arg("--include-partial-messages");
    }
    if config.skip_permissions {
        cmd.arg("--dangerously-skip-permissions");
    }
    cmd.arg("--session-id").arg(&config.session_id);

    if let Some(schema) = &config.json_schema {
        cmd.arg("--json-schema").arg(schema);
    }

    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("Failed to spawn Claude Code CLI: {e}"))?;

    let pid = child.id().unwrap_or(0);
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Failed to capture Claude stdout".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "Failed to capture Claude stderr".to_string())?;

    let child_arc = Arc::new(Mutex::new(Some(child)));

    let run_id = registry
        .register_claude_session(
            config.session_id.clone(),
            pid,
            config.project_path.to_string_lossy().to_string(),
            config.prompt.clone(),
            "claude-code-cli".to_string(),
            child_arc.clone(),
        )
        .await?;

    let terminal_sent = Arc::new(AtomicBool::new(false));

    // Stream stdout (stream-json) -> AgentEvents.
    {
        let registry = registry.clone();
        let event_sender = event_sender.clone();
        let terminal_sent = terminal_sent.clone();
        tokio::spawn(async move {
            let mut parser = ClaudeStreamJsonParser::default();
            let mut reader = BufReader::new(stdout).lines();

            while let Ok(Some(line)) = reader.next_line().await {
                let _ = registry.append_live_output(run_id, &line).await;

                for event in parser.parse_line(&line) {
                    if matches!(event, AgentEvent::Complete { .. } | AgentEvent::Error { .. }) {
                        terminal_sent.store(true, Ordering::SeqCst);
                    }
                    let _ = event_sender.send(event);
                }
            }
        });
    }

    // Stream stderr as plain text into the live output buffer for debugging.
    {
        let registry = registry.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                let _ = registry.append_live_output(run_id, &format!("[stderr] {line}")).await;
            }
        });
    }

    // Wait for process completion or cancellation, then emit a terminal event if needed.
    {
        let registry = registry.clone();
        let event_sender = event_sender.clone();
        let terminal_sent = terminal_sent.clone();
        let child_arc = child_arc.clone();
        tokio::spawn(async move {
            let wait_fut = async {
                // Take ownership of the child for waiting.
                let child = {
                    let mut guard = child_arc.lock().map_err(|e| e.to_string())?;
                    guard.take()
                };

                let mut child = match child {
                    Some(c) => c,
                    None => return Err::<std::process::ExitStatus, String>("Child already taken".to_string()),
                };

                child
                    .wait()
                    .await
                    .map_err(|e| format!("Failed to wait for Claude process: {e}"))
            };

            let status = tokio::select! {
                _ = cancel_token.cancelled() => {
                    let _ = registry.kill_process(run_id).await;
                    Err("cancelled".to_string())
                }
                s = wait_fut => s.map_err(|e| e),
            };

            // Ensure we emit a terminal event if Claude didn't provide one.
            if !terminal_sent.load(Ordering::SeqCst) {
                match status {
                    Ok(exit_status) if exit_status.success() => {
                        let _ = event_sender.send(AgentEvent::Complete {
                            usage: TokenUsage {
                                prompt_tokens: 0,
                                completion_tokens: 0,
                                total_tokens: 0,
                            },
                        });
                    }
                    Ok(exit_status) => {
                        let _ = event_sender.send(AgentEvent::Error {
                            message: format!("Claude Code CLI exited with status: {exit_status}"),
                        });
                    }
                    Err(e) if e == "cancelled" => {
                        let _ = event_sender.send(AgentEvent::Error {
                            message: "Claude Code execution cancelled".to_string(),
                        });
                    }
                    Err(e) => {
                        let _ = event_sender.send(AgentEvent::Error { message: e });
                    }
                }
            }

            let _ = registry.unregister_process(run_id).await;
        });
    }

    Ok(run_id)
}
