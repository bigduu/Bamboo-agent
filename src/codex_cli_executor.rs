//! `CodexExecutor`: a [`ChildExecutor`] that drives the official OpenAI
//! Codex CLI through `codex exec --json`.
//!
//! The CLI is one process per activation. Prompts are written on stdin (never
//! argv), stdout is consumed as bounded JSONL, and the process owns a process
//! group so cancellation tears down any descendants as well as the leader.
//! Session resume, provider/auth selection, and bamboo permission-profile
//! mapping are intentionally handled by the dependent issues in epic #568.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::{AgentEvent, TokenUsage, ToolResult};
use bamboo_subagent::executor::{ChildExecutor, ChildOutcome, EventSink, SteerInbox};
use bamboo_subagent::proto::RunSpec;

/// The oldest Codex CLI schema this executor intentionally supports. The
/// executor additionally capability-checks `exec --help` and `exec resume
/// --help`, so a backported or vendor build must still expose the required
/// flags. Version 0.144 is the schema verified by issue #569.
pub const MIN_CODEX_VERSION: (u64, u64, u64) = (0, 144, 0);

const MAX_STDOUT_LINE_BYTES: usize = 10 * 1024 * 1024;
const STDERR_TAIL_BYTES: usize = 16 * 1024;
const TOOL_RESULT_TRUNCATE_CHARS: usize = 20_000;
const SIGTERM_WAIT: Duration = Duration::from_secs(5);
const PROCESS_EXIT_WAIT: Duration = Duration::from_secs(5);

const ENV_ALLOWLIST: &[&str] = &[
    "HOME", "PATH", "SHELL", "TERM", "LANG", "TMPDIR", "USER", "LOGNAME",
];

/// Resolve the per-child directory used for `--output-last-message`.
pub fn resolve_codex_state_dir(storage_dir: &Option<String>, child_id: &str) -> PathBuf {
    storage_dir
        .clone()
        .map(PathBuf::from)
        .unwrap_or_else(|| bamboo_config::paths::subagents_dir().join(child_id))
}

/// One-process-per-activation Codex CLI executor.
pub struct CodexExecutor {
    binary: PathBuf,
    version: String,
    model: Option<String>,
    sandbox: Option<String>,
    workspace: Option<String>,
    state_dir: Option<PathBuf>,
    inherit_user_config: bool,
    forward_env: Vec<String>,
}

impl CodexExecutor {
    /// Resolve and capability-check the configured Codex binary before the
    /// worker begins serving runs. This keeps install/version failures at
    /// provisioning time instead of surfacing halfway through a child turn.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        binary: Option<String>,
        model: Option<String>,
        sandbox: Option<String>,
        workspace: Option<String>,
        state_dir: Option<PathBuf>,
        inherit_user_config: bool,
        forward_env: Vec<String>,
    ) -> Result<Self, String> {
        let requested = binary.unwrap_or_else(|| "codex".to_string());
        let resolved =
            resolve_binary(&requested).ok_or_else(|| missing_binary_error(&requested))?;
        ensure_executable(&resolved)?;

        let version_output = Command::new(&resolved)
            .arg("--version")
            .output()
            .await
            .map_err(|error| format!("run '{} --version': {error}", resolved.display()))?;
        if !version_output.status.success() {
            return Err(format!(
                "'{} --version' failed with status {}; reinstall or upgrade Codex CLI",
                resolved.display(),
                version_output.status
            ));
        }
        let version_text = String::from_utf8_lossy(&version_output.stdout)
            .trim()
            .to_string();
        let parsed_version = parse_codex_version(&version_text).ok_or_else(|| {
            format!(
                "could not parse Codex CLI version from {version_text:?}; expected `codex-cli X.Y.Z`"
            )
        })?;
        if parsed_version < MIN_CODEX_VERSION {
            return Err(format!(
                "Codex CLI {version_text} is too old; Bamboo requires >= {}.{}.{} with `exec --json` and `exec resume`",
                MIN_CODEX_VERSION.0, MIN_CODEX_VERSION.1, MIN_CODEX_VERSION.2
            ));
        }

        verify_help_surface(
            &resolved,
            &["exec", "--help"],
            &["--json", "--output-last-message", "stdin"],
        )
        .await?;
        verify_help_surface(&resolved, &["exec", "resume", "--help"], &["--json"]).await?;

        Ok(Self {
            binary: resolved,
            version: version_text,
            model,
            sandbox,
            workspace,
            state_dir,
            inherit_user_config,
            forward_env,
        })
    }

    fn last_message_path(&self) -> Option<PathBuf> {
        self.state_dir
            .as_ref()
            .map(|directory| directory.join("codex-last-message.txt"))
    }

    fn build_command(&self) -> Command {
        let mut command = Command::new(&self.binary);
        command
            .arg("exec")
            .arg("--json")
            .arg("--color")
            .arg("never");

        if let Some(workspace) = &self.workspace {
            command.arg("--cd").arg(workspace);
            if !has_git_metadata(Path::new(workspace)) {
                command.arg("--skip-git-repo-check");
            }
        }
        if let Some(model) = &self.model {
            command.arg("--model").arg(model);
        }
        // Safe foundation default until #571 maps Bamboo permission profiles
        // onto Codex's sandbox and approval-policy pair.
        command
            .arg("--sandbox")
            .arg(self.sandbox.as_deref().unwrap_or("read-only"));
        if !self.inherit_user_config {
            // Auth still resolves from HOME/.codex/auth.json. Only mutable user
            // defaults and exec-policy rules are ignored in the core issue;
            // full CODEX_HOME isolation belongs to #570.
            command.arg("--ignore-user-config").arg("--ignore-rules");
        }
        if let Some(path) = self.last_message_path() {
            command.arg("--output-last-message").arg(path);
        }

        // `-` is the documented stdin prompt sentinel. It avoids both argv
        // length limits and leaking the assignment through process listings.
        command.arg("-");

        command.env_clear();
        for (key, value) in std::env::vars() {
            if ENV_ALLOWLIST.contains(&key.as_str()) || key.starts_with("LC_") {
                command.env(key, value);
            }
        }
        for name in &self.forward_env {
            if let Ok(value) = std::env::var(name) {
                command.env(name, value);
            }
        }
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
        command.kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        command
    }

    async fn prepare_output_file(&self) -> Result<(), String> {
        let Some(path) = self.last_message_path() else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|error| {
                format!("create Codex state dir '{}': {error}", parent.display())
            })?;
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!(
                "remove stale Codex last-message file '{}': {error}",
                path.display()
            )),
        }
    }

    fn handle_event(&self, value: Value, events: &EventSink, state: &mut RunState) {
        let event_type = value.get("type").and_then(Value::as_str).unwrap_or("");
        match event_type {
            "thread.started" => {
                state.thread_id = value
                    .get("thread_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                events.emit(json!({
                    "type": "runner_progress",
                    "session_id": state.thread_id,
                    "round_count": 0,
                    "executor": "codex",
                    "binary": self.binary,
                    "version": self.version,
                    "sandbox": self.sandbox.as_deref().unwrap_or("read-only"),
                }));
            }
            "turn.started" => {
                state.turn_started = true;
                events.emit(event_json(AgentEvent::RunnerProgress {
                    session_id: state.session_id(),
                    round_count: 1,
                }));
            }
            "item.started" | "item.updated" | "item.completed" => {
                let phase = event_type.trim_start_matches("item.");
                if let Some(item) = value.get("item") {
                    handle_item(phase, item, events, state);
                }
            }
            "turn.completed" => {
                state.completed = true;
                state.usage = parse_usage(value.get("usage"));
                if let Some(text) = final_text_from_terminal(&value) {
                    state.last_agent_message = text;
                }
                events.emit(event_json(AgentEvent::Complete { usage: state.usage }));
            }
            "turn.failed" => {
                let message = error_message(&value, "Codex turn failed");
                state.failure = Some(message.clone());
                events.emit(event_json(AgentEvent::Error { message }));
            }
            "error" => {
                let message = error_message(&value, "Codex CLI error");
                state.failure = Some(message.clone());
                events.emit(event_json(AgentEvent::Error { message }));
            }
            other => {
                tracing::debug!(event_type = other, "codex: unrecognized JSONL event");
            }
        }
    }

    async fn read_last_message(&self) -> Option<String> {
        let path = self.last_message_path()?;
        let text = tokio::fs::read_to_string(path).await.ok()?;
        let trimmed = text.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    }

    async fn run_process(
        &self,
        prompt: &str,
        events: &EventSink,
        cancel: &CancellationToken,
    ) -> ChildOutcome {
        if let Err(error) = self.prepare_output_file().await {
            return ChildOutcome::error(error);
        }

        let mut child = match spawn_with_etxtbsy_retry(|| self.build_command()).await {
            Ok(child) => child,
            Err(error) => {
                return ChildOutcome::error(format!(
                    "spawn Codex CLI '{}': {error}; install with `npm i -g @openai/codex`, `brew install codex`, or an official GitHub release",
                    self.binary.display()
                ));
            }
        };
        let Some(mut stdin) = child.stdin.take() else {
            terminate_child(&mut child).await;
            return ChildOutcome::error("Codex child has no stdin pipe");
        };
        let Some(stdout) = child.stdout.take() else {
            terminate_child(&mut child).await;
            return ChildOutcome::error("Codex child has no stdout pipe");
        };
        let stderr = child.stderr.take();

        let write_result = tokio::select! {
            result = async {
                stdin.write_all(prompt.as_bytes()).await?;
                stdin.write_all(b"\n").await?;
                stdin.shutdown().await
            } => result,
            _ = cancel.cancelled() => {
                terminate_child(&mut child).await;
                return ChildOutcome::cancelled();
            }
        };
        if let Err(error) = write_result {
            terminate_child(&mut child).await;
            return ChildOutcome::error(format!("write Codex prompt to stdin: {error}"));
        }
        drop(stdin);

        let stderr_tail = Arc::new(Mutex::new(String::new()));
        let stderr_task = stderr.map(|stderr| {
            let tail = stderr_tail.clone();
            tokio::spawn(async move { drain_stderr_tail(stderr, tail).await })
        });

        let mut reader = BufReader::with_capacity(64 * 1024, stdout);
        let mut state = RunState::default();
        let mut read_error = None;
        let mut cancelled = false;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    cancelled = true;
                    terminate_child(&mut child).await;
                    break;
                }
                line = read_bounded_line(&mut reader, MAX_STDOUT_LINE_BYTES) => {
                    match line {
                        Ok(Some(bytes)) => {
                            if bytes.iter().all(u8::is_ascii_whitespace) {
                                continue;
                            }
                            match serde_json::from_slice::<Value>(&bytes) {
                                Ok(value) => self.handle_event(value, events, &mut state),
                                Err(error) => tracing::debug!(%error, "codex: skipping unparsable stdout line"),
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            read_error = Some(format!("Codex stdout read error: {error}"));
                            terminate_child(&mut child).await;
                            break;
                        }
                    }
                }
            }
        }

        let status = if cancelled || read_error.is_some() {
            child.try_wait().ok().flatten()
        } else {
            match tokio::time::timeout(PROCESS_EXIT_WAIT, child.wait()).await {
                Ok(result) => result.ok(),
                Err(_) => {
                    terminate_child(&mut child).await;
                    child.try_wait().ok().flatten()
                }
            }
        };
        if let Some(task) = stderr_task {
            let _ = task.await;
        }
        let stderr = stderr_tail.lock().await.clone();

        if cancelled {
            events.emit(event_json(AgentEvent::Cancelled {
                message: Some("Codex child cancelled".to_string()),
            }));
            return ChildOutcome::cancelled();
        }
        if let Some(error) = read_error {
            return ChildOutcome::error(error);
        }
        if status.as_ref().is_some_and(|status| !status.success()) {
            return ChildOutcome::error(format!(
                "Codex CLI exited with status {}; stderr tail: {}",
                status
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "<unknown>".to_string()),
                display_stderr_tail(&stderr)
            ));
        }
        if let Some(error) = state.failure {
            return ChildOutcome::error(format!(
                "{error}; stderr tail: {}",
                display_stderr_tail(&stderr)
            ));
        }
        if !state.completed {
            return ChildOutcome::error(format!(
                "Codex CLI exited without a turn.completed event; stderr tail: {}",
                display_stderr_tail(&stderr)
            ));
        }

        let final_text = if state.last_agent_message.trim().is_empty() {
            self.read_last_message().await
        } else {
            Some(state.last_agent_message)
        };
        match final_text {
            Some(text) => ChildOutcome::completed(text),
            None => ChildOutcome::error(
                "Codex CLI completed without a final agent message or output-last-message file",
            ),
        }
    }
}

#[async_trait]
impl ChildExecutor for CodexExecutor {
    async fn run(
        &self,
        spec: RunSpec,
        events: EventSink,
        mut steer: SteerInbox,
        cancel: CancellationToken,
    ) -> ChildOutcome {
        // `codex exec` v1 has no mid-turn steering channel. Drain the inbox so
        // transport senders cannot build an unbounded backlog.
        let steer_drain = tokio::spawn(async move { while steer.recv().await.is_some() {} });
        let outcome = self.run_process(&spec.assignment, &events, &cancel).await;
        steer_drain.abort();
        outcome
    }
}

#[derive(Default)]
struct RunState {
    thread_id: String,
    turn_started: bool,
    completed: bool,
    failure: Option<String>,
    last_agent_message: String,
    usage: TokenUsage,
    started_items: HashSet<String>,
    item_text: HashMap<String, String>,
    item_output: HashMap<String, String>,
}

impl RunState {
    fn session_id(&self) -> String {
        if self.thread_id.is_empty() {
            "codex".to_string()
        } else {
            self.thread_id.clone()
        }
    }
}

fn handle_item(phase: &str, item: &Value, events: &EventSink, state: &mut RunState) {
    let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
    let item_id = item
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("codex-{item_type}"));

    match item_type {
        "agent_message" => {
            let text = item.get("text").and_then(Value::as_str).unwrap_or("");
            emit_text_delta(
                &item_id,
                text,
                &mut state.item_text,
                |delta| AgentEvent::Token { content: delta },
                events,
            );
            if phase == "completed" && !text.is_empty() {
                state.last_agent_message = text.to_string();
            }
        }
        "reasoning" => {
            let text = reasoning_text(item);
            emit_text_delta(
                &item_id,
                &text,
                &mut state.item_text,
                |delta| AgentEvent::ReasoningToken { content: delta },
                events,
            );
        }
        "command_execution" => {
            ensure_tool_started(
                &item_id,
                "Bash",
                json!({ "command": item.get("command").cloned().unwrap_or(Value::Null) }),
                events,
                state,
            );
            let output = item
                .get("aggregated_output")
                .and_then(Value::as_str)
                .unwrap_or("");
            emit_tool_output_delta(&item_id, output, events, state);
            if phase == "completed" {
                let exit_code = item.get("exit_code").and_then(Value::as_i64);
                let successful = exit_code == Some(0)
                    && item.get("status").and_then(Value::as_str) != Some("failed");
                if successful {
                    events.emit(event_json(AgentEvent::ToolComplete {
                        tool_call_id: item_id,
                        result: ToolResult::text(
                            true,
                            truncate_chars(output, TOOL_RESULT_TRUNCATE_CHARS),
                        ),
                    }));
                } else {
                    let error = if output.trim().is_empty() {
                        format!("command failed with exit code {exit_code:?}")
                    } else {
                        truncate_chars(output, TOOL_RESULT_TRUNCATE_CHARS)
                    };
                    events.emit(event_json(AgentEvent::ToolError {
                        tool_call_id: item_id,
                        error,
                    }));
                }
            }
        }
        "file_change" => {
            let detail = item
                .get("changes")
                .or_else(|| item.get("patch"))
                .cloned()
                .unwrap_or_else(|| item.clone());
            ensure_tool_started(
                &item_id,
                "ApplyPatch",
                json!({ "changes": detail }),
                events,
                state,
            );
            if phase == "completed" {
                complete_structured_tool(&item_id, item, events);
            }
        }
        "mcp_tool_call" => {
            let server = item.get("server").and_then(Value::as_str).unwrap_or("mcp");
            let tool = item
                .get("tool")
                .or_else(|| item.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("tool");
            let tool_name = format!("{server}::{tool}");
            let arguments = item
                .get("arguments")
                .or_else(|| item.get("input"))
                .cloned()
                .unwrap_or_else(|| json!({}));
            ensure_tool_started(&item_id, &tool_name, arguments, events, state);
            if phase == "completed" {
                complete_structured_tool(&item_id, item, events);
            }
        }
        "web_search" => {
            let query = item.get("query").cloned().unwrap_or(Value::Null);
            ensure_tool_started(
                &item_id,
                "WebSearch",
                json!({ "query": query }),
                events,
                state,
            );
            if phase == "completed" {
                complete_structured_tool(&item_id, item, events);
            }
        }
        "todo_list" => {
            events.emit(json!({
                "type": "runner_progress",
                "session_id": state.session_id(),
                "round_count": 1,
                "codex_item_type": "todo_list",
                "item": item,
            }));
        }
        other => {
            tracing::debug!(item_type = other, phase, "codex: unrecognized item type");
        }
    }
}

fn ensure_tool_started(
    item_id: &str,
    tool_name: &str,
    arguments: Value,
    events: &EventSink,
    state: &mut RunState,
) {
    if state.started_items.insert(item_id.to_string()) {
        events.emit(event_json(AgentEvent::ToolStart {
            tool_call_id: item_id.to_string(),
            tool_name: tool_name.to_string(),
            arguments,
        }));
    }
}

fn emit_tool_output_delta(item_id: &str, output: &str, events: &EventSink, state: &mut RunState) {
    let previous = state.item_output.entry(item_id.to_string()).or_default();
    let delta = if output.starts_with(previous.as_str()) {
        &output[previous.len()..]
    } else {
        output
    };
    if !delta.is_empty() {
        events.emit(event_json(AgentEvent::ToolToken {
            tool_call_id: item_id.to_string(),
            content: delta.to_string(),
        }));
    }
    *previous = output.to_string();
}

fn complete_structured_tool(item_id: &str, item: &Value, events: &EventSink) {
    if let Some(error) = item.get("error").filter(|value| !value.is_null()) {
        events.emit(event_json(AgentEvent::ToolError {
            tool_call_id: item_id.to_string(),
            error: value_text(error),
        }));
        return;
    }
    let result = item
        .get("result")
        .or_else(|| item.get("output"))
        .cloned()
        .unwrap_or_else(|| item.clone());
    events.emit(event_json(AgentEvent::ToolComplete {
        tool_call_id: item_id.to_string(),
        result: ToolResult::text(
            true,
            truncate_chars(&value_text(&result), TOOL_RESULT_TRUNCATE_CHARS),
        ),
    }));
}

fn emit_text_delta<F>(
    item_id: &str,
    text: &str,
    seen: &mut HashMap<String, String>,
    build: F,
    events: &EventSink,
) where
    F: FnOnce(String) -> AgentEvent,
{
    let previous = seen.entry(item_id.to_string()).or_default();
    let delta = if text.starts_with(previous.as_str()) {
        &text[previous.len()..]
    } else {
        text
    };
    if !delta.is_empty() {
        events.emit(event_json(build(delta.to_string())));
    }
    *previous = text.to_string();
}

fn reasoning_text(item: &Value) -> String {
    match item.get("text").or_else(|| item.get("summary")) {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| {
                part.as_str()
                    .or_else(|| part.get("text").and_then(Value::as_str))
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Some(value) => value.to_string(),
        None => String::new(),
    }
}

fn parse_usage(value: Option<&Value>) -> TokenUsage {
    let input = value
        .and_then(|usage| usage.get("input_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output = value
        .and_then(|usage| usage.get("output_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    TokenUsage {
        prompt_tokens: input,
        completion_tokens: output,
        total_tokens: input.saturating_add(output),
    }
}

fn final_text_from_terminal(value: &Value) -> Option<String> {
    ["final_output", "output_text", "result"]
        .into_iter()
        .find_map(|key| value.get(key).and_then(Value::as_str))
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

fn error_message(value: &Value, fallback: &str) -> String {
    value
        .pointer("/error/message")
        .or_else(|| value.get("message"))
        .or_else(|| value.get("error"))
        .map(value_text)
        .filter(|message| !message.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn value_text(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

fn display_stderr_tail(stderr: &str) -> &str {
    let trimmed = stderr.trim();
    if trimmed.is_empty() {
        "<empty>"
    } else {
        trimmed
    }
}

fn event_json(event: AgentEvent) -> Value {
    serde_json::to_value(event).unwrap_or_else(|_| json!({}))
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let head: String = text.chars().take(max_chars).collect();
    let dropped = text.chars().count().saturating_sub(max_chars);
    format!("{head}\n… [truncated, {dropped} more chars]")
}

fn missing_binary_error(requested: &str) -> String {
    format!(
        "Codex CLI binary {requested:?} was not found or is not executable; install it with `npm i -g @openai/codex`, `brew install codex`, or an official GitHub release, or set codex_binary"
    )
}

fn resolve_binary(requested: &str) -> Option<PathBuf> {
    let requested_path = Path::new(requested);
    if requested_path.components().count() > 1 || requested_path.is_absolute() {
        return requested_path
            .exists()
            .then(|| requested_path.to_path_buf());
    }
    let path = std::env::var_os("PATH")?;
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join(requested);
        if candidate.is_file() {
            return Some(candidate);
        }
        #[cfg(windows)]
        for extension in ["exe", "cmd", "bat"] {
            let candidate = directory.join(format!("{requested}.{extension}"));
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

fn ensure_executable(path: &Path) -> Result<(), String> {
    let metadata = std::fs::metadata(path)
        .map_err(|error| format!("inspect Codex binary '{}': {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(missing_binary_error(&path.display().to_string()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            return Err(missing_binary_error(&path.display().to_string()));
        }
    }
    Ok(())
}

fn parse_codex_version(text: &str) -> Option<(u64, u64, u64)> {
    let token = text
        .split_whitespace()
        .find(|token| token.chars().next().is_some_and(|ch| ch.is_ascii_digit()))?;
    let clean = token.trim_start_matches('v').split(['-', '+']).next()?;
    let mut parts = clean.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next().unwrap_or("0").parse().ok()?;
    Some((major, minor, patch))
}

async fn verify_help_surface(
    binary: &Path,
    args: &[&str],
    required: &[&str],
) -> Result<(), String> {
    let output = Command::new(binary)
        .args(args)
        .output()
        .await
        .map_err(|error| format!("run '{} {}': {error}", binary.display(), args.join(" ")))?;
    if !output.status.success() {
        return Err(format!(
            "'{} {}' failed with status {}; upgrade Codex CLI",
            binary.display(),
            args.join(" "),
            output.status
        ));
    }
    let help = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let missing: Vec<_> = required
        .iter()
        .copied()
        .filter(|flag| !help.contains(flag))
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "Codex CLI '{}' lacks required `{}` capability flag(s): {}; upgrade to >= {}.{}.{}",
            binary.display(),
            args.join(" "),
            missing.join(", "),
            MIN_CODEX_VERSION.0,
            MIN_CODEX_VERSION.1,
            MIN_CODEX_VERSION.2
        ))
    }
}

fn has_git_metadata(path: &Path) -> bool {
    path.ancestors()
        .any(|ancestor| ancestor.join(".git").exists())
}

async fn spawn_with_etxtbsy_retry(mut build: impl FnMut() -> Command) -> std::io::Result<Child> {
    let mut last_error = None;
    for _ in 0..5 {
        match build().spawn() {
            Ok(child) => return Ok(child),
            Err(error) if error.raw_os_error() == Some(26) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.expect("retry loop records ETXTBSY before exhausting"))
}

enum ProcessSignal {
    Term,
    Kill,
}

#[cfg(unix)]
fn signal_process_group(child: &Child, signal: ProcessSignal) {
    if let Some(pid) = child.id() {
        let signal = match signal {
            ProcessSignal::Term => libc::SIGTERM,
            ProcessSignal::Kill => libc::SIGKILL,
        };
        // SAFETY: the pid is from our child and build_command created a new
        // process group with that child as leader. ESRCH is harmless.
        unsafe {
            libc::kill(-(pid as libc::pid_t), signal);
        }
    }
}

#[cfg(not(unix))]
fn signal_process_group(_child: &Child, _signal: ProcessSignal) {}

async fn terminate_child(child: &mut Child) {
    signal_process_group(child, ProcessSignal::Term);
    if tokio::time::timeout(SIGTERM_WAIT, child.wait())
        .await
        .is_ok()
    {
        return;
    }
    signal_process_group(child, ProcessSignal::Kill);
    let _ = child.start_kill();
    let _ = child.wait().await;
}

async fn read_bounded_line<R>(reader: &mut R, max_bytes: usize) -> std::io::Result<Option<Vec<u8>>>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut output = Vec::new();
    loop {
        let (found_newline, consumed) = {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                return Ok(if output.is_empty() {
                    None
                } else {
                    Some(output)
                });
            }
            match available.iter().position(|byte| *byte == b'\n') {
                Some(position) => {
                    output.extend_from_slice(&available[..position]);
                    (true, position + 1)
                }
                None => {
                    output.extend_from_slice(available);
                    (false, available.len())
                }
            }
        };
        reader.consume(consumed);
        if output.len() > max_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("stdout line exceeded {max_bytes} bytes"),
            ));
        }
        if found_newline {
            return Ok(Some(output));
        }
    }
}

async fn drain_stderr_tail(stderr: tokio::process::ChildStderr, tail: Arc<Mutex<String>>) {
    let mut reader = BufReader::new(stderr);
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        match reader.read_until(b'\n', &mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {
                let mut tail = tail.lock().await;
                tail.push_str(&String::from_utf8_lossy(&buffer));
                if tail.len() > STDERR_TAIL_BYTES {
                    let excess = tail.len() - STDERR_TAIL_BYTES;
                    let cut = tail
                        .char_indices()
                        .map(|(index, _)| index)
                        .find(|index| *index >= excess)
                        .unwrap_or(tail.len());
                    tail.drain(..cut);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_subagent::proto::TerminalStatus;

    fn fixture_executor() -> CodexExecutor {
        CodexExecutor {
            binary: PathBuf::from("/usr/local/bin/codex"),
            version: "codex-cli 0.144.5".to_string(),
            model: None,
            sandbox: None,
            workspace: None,
            state_dir: None,
            inherit_user_config: false,
            forward_env: Vec::new(),
        }
    }

    fn map_fixture(input: &str) -> (RunState, Vec<Value>) {
        let executor = fixture_executor();
        let (sink, mut rx) = EventSink::channel();
        let mut state = RunState::default();
        for line in input.lines().filter(|line| !line.trim().is_empty()) {
            executor.handle_event(serde_json::from_str(line).unwrap(), &sink, &mut state);
        }
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        (state, events)
    }

    #[test]
    fn version_parser_accepts_current_and_rejects_noise() {
        assert_eq!(parse_codex_version("codex-cli 0.144.5"), Some((0, 144, 5)));
        assert_eq!(parse_codex_version("codex 1.2"), Some((1, 2, 0)));
        assert_eq!(parse_codex_version("not-a-version"), None);
    }

    #[tokio::test]
    async fn bounded_reader_accepts_ten_megabytes_and_rejects_more() {
        let allowed = vec![b'x'; MAX_STDOUT_LINE_BYTES];
        let mut bytes = allowed.clone();
        bytes.push(b'\n');
        let mut reader = BufReader::new(bytes.as_slice());
        assert_eq!(
            read_bounded_line(&mut reader, MAX_STDOUT_LINE_BYTES)
                .await
                .unwrap()
                .unwrap()
                .len(),
            MAX_STDOUT_LINE_BYTES
        );

        let mut too_large = vec![b'x'; MAX_STDOUT_LINE_BYTES + 1];
        too_large.push(b'\n');
        let mut reader = BufReader::new(too_large.as_slice());
        assert_eq!(
            read_bounded_line(&mut reader, MAX_STDOUT_LINE_BYTES)
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn usage_matches_real_codex_schema() {
        let usage = parse_usage(Some(&json!({
            "input_tokens": 13_460,
            "cached_input_tokens": 9_984,
            "output_tokens": 6,
            "reasoning_output_tokens": 0
        })));
        assert_eq!(usage.prompt_tokens, 13_460);
        assert_eq!(usage.completion_tokens, 6);
        assert_eq!(usage.total_tokens, 13_466);
    }

    #[test]
    fn recorded_jsonl_fixtures_map_agent_and_command_events() {
        let cases = [
            (
                include_str!("../tests/fixtures/codex-cli/0.144.5-simple.jsonl"),
                "PONG",
                false,
                13_466,
            ),
            (
                include_str!("../tests/fixtures/codex-cli/0.144.5-command.jsonl"),
                "The current working directory is `/private/tmp/zenith-bamboo-569-codex-cli`.",
                true,
                27_108,
            ),
        ];

        for (fixture, expected_final, expects_tool, total_tokens) in cases {
            let (state, events) = map_fixture(fixture);
            assert!(state.completed);
            assert_eq!(state.last_agent_message, expected_final);
            assert_eq!(state.usage.total_tokens, total_tokens);
            assert!(events.iter().any(|event| event["type"] == "token"));
            assert!(events.iter().any(|event| event["type"] == "complete"));
            assert_eq!(
                events.iter().any(|event| event["type"] == "tool_start"),
                expects_tool
            );
            if expects_tool {
                assert!(events.iter().any(|event| {
                    event["type"] == "tool_start" && event["tool_name"] == "Bash"
                }));
                assert!(events.iter().any(|event| event["type"] == "tool_token"));
                assert!(events.iter().any(|event| event["type"] == "tool_complete"));
            }
        }
    }

    #[test]
    fn item_mapping_table_covers_non_command_codex_items() {
        let cases = [
            (
                json!({"id":"r1","type":"reasoning","text":"thinking"}),
                "reasoning_token",
                None,
            ),
            (
                json!({"id":"f1","type":"file_change","changes":[{"path":"a.txt","kind":"add"}]}),
                "tool_start",
                Some("ApplyPatch"),
            ),
            (
                json!({"id":"m1","type":"mcp_tool_call","server":"files","tool":"read","arguments":{"path":"a.txt"},"result":"ok"}),
                "tool_start",
                Some("files::read"),
            ),
            (
                json!({"id":"w1","type":"web_search","query":"Bamboo","result":[]}),
                "tool_start",
                Some("WebSearch"),
            ),
            (
                json!({"id":"t1","type":"todo_list","items":[]}),
                "runner_progress",
                None,
            ),
        ];

        for (item, expected_type, expected_tool) in cases {
            let (sink, mut rx) = EventSink::channel();
            let mut state = RunState::default();
            handle_item("completed", &item, &sink, &mut state);
            let first = rx.try_recv().expect("mapping emitted an event");
            assert_eq!(first["type"], expected_type, "item: {item}");
            if let Some(tool) = expected_tool {
                assert_eq!(first["tool_name"], tool, "item: {item}");
                assert!(rx
                    .try_recv()
                    .is_ok_and(|event| event["type"] == "tool_complete"));
            }
        }
    }

    #[test]
    fn unknown_event_and_item_types_are_tolerated() {
        let executor = fixture_executor();
        let (sink, mut rx) = EventSink::channel();
        let mut state = RunState::default();
        executor.handle_event(
            json!({"type":"future.event","payload":{"schema":2}}),
            &sink,
            &mut state,
        );
        executor.handle_event(
            json!({"type":"item.completed","item":{"id":"x","type":"future_item"}}),
            &sink,
            &mut state,
        );
        assert!(rx.try_recv().is_err());
        assert!(!state.completed);
        assert!(state.failure.is_none());
    }

    #[cfg(unix)]
    mod unix_process_tests {
        use super::*;
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt;

        fn write_stub(dir: &Path, body: &str) -> PathBuf {
            let path = dir.join("codex");
            let mut file = std::fs::File::create(&path).unwrap();
            writeln!(file, "#!/bin/sh").unwrap();
            file.write_all(body.as_bytes()).unwrap();
            let mut permissions = std::fs::metadata(&path).unwrap().permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&path, permissions).unwrap();
            path
        }

        fn executor(binary: PathBuf, workspace: &Path) -> CodexExecutor {
            CodexExecutor {
                binary,
                version: "codex-cli 0.144.5".to_string(),
                model: None,
                sandbox: None,
                workspace: Some(workspace.to_string_lossy().into_owned()),
                state_dir: None,
                inherit_user_config: false,
                forward_env: Vec::new(),
            }
        }

        fn run_spec(assignment: &str) -> RunSpec {
            RunSpec {
                assignment: assignment.to_string(),
                reasoning_effort: None,
                permission_policy: None,
                messages: Vec::new(),
            }
        }

        #[tokio::test]
        async fn preflight_accepts_current_surface_and_rejects_old_or_missing_binary() {
            let dir = tempfile::tempdir().unwrap();
            let current = write_stub(
                dir.path(),
                r#"
case "$*" in
  "--version") echo 'codex-cli 0.144.5' ;;
  "exec --help") echo '--json --output-last-message prompt from stdin' ;;
  "exec resume --help") echo '--json' ;;
  *) exit 2 ;;
esac
"#,
            );
            let checked = CodexExecutor::new(
                Some(current.to_string_lossy().into_owned()),
                None,
                None,
                None,
                None,
                false,
                Vec::new(),
            )
            .await
            .unwrap();
            assert_eq!(checked.version, "codex-cli 0.144.5");

            let old_dir = tempfile::tempdir().unwrap();
            let old = write_stub(
                old_dir.path(),
                r#"
if [ "$1" = "--version" ]; then echo 'codex-cli 0.143.9'; else exit 2; fi
"#,
            );
            let old_error = CodexExecutor::new(
                Some(old.to_string_lossy().into_owned()),
                None,
                None,
                None,
                None,
                false,
                Vec::new(),
            )
            .await
            .err()
            .expect("old version rejected");
            assert!(old_error.contains("too old"));
            assert!(old_error.contains(">= 0.144.0"));

            let missing_error = CodexExecutor::new(
                Some("/definitely/missing/codex".to_string()),
                None,
                None,
                None,
                None,
                false,
                Vec::new(),
            )
            .await
            .err()
            .expect("missing binary rejected");
            assert!(missing_error.contains("npm i -g @openai/codex"));
            assert!(missing_error.contains("brew install codex"));
            assert!(missing_error.contains("codex_binary"));
        }

        #[tokio::test]
        async fn clean_completion_and_nonzero_exit_have_distinct_outcomes() {
            let workspace = tempfile::tempdir().unwrap();
            let ok_dir = tempfile::tempdir().unwrap();
            let ok = write_stub(
                ok_dir.path(),
                r#"
read -r prompt
echo '{"type":"thread.started","thread_id":"stub-thread"}'
echo '{"type":"turn.started"}'
echo '{"type":"item.completed","item":{"id":"answer","type":"agent_message","text":"PONG"}}'
echo '{"type":"turn.completed","usage":{"input_tokens":3,"output_tokens":1}}'
"#,
            );
            let (sink, _rx) = EventSink::channel();
            let outcome = executor(ok, workspace.path())
                .run(
                    run_spec("reply PONG"),
                    sink,
                    SteerInbox::disconnected(),
                    CancellationToken::new(),
                )
                .await;
            assert_eq!(outcome.status, TerminalStatus::Completed);
            assert_eq!(outcome.result.as_deref(), Some("PONG"));

            let fail_dir = tempfile::tempdir().unwrap();
            let fail = write_stub(
                fail_dir.path(),
                r#"
read -r prompt
echo 'credential lookup failed' >&2
exit 7
"#,
            );
            let (sink, _rx) = EventSink::channel();
            let outcome = executor(fail, workspace.path())
                .run(
                    run_spec("fail"),
                    sink,
                    SteerInbox::disconnected(),
                    CancellationToken::new(),
                )
                .await;
            assert_eq!(outcome.status, TerminalStatus::Error);
            let error = outcome.error.unwrap();
            assert!(error.contains("status exit status: 7"), "{error}");
            assert!(error.contains("credential lookup failed"), "{error}");
        }

        #[tokio::test]
        async fn spawn_uses_stdin_safe_defaults_and_last_message_fallback() {
            let workspace = tempfile::tempdir().unwrap();
            let bin_dir = tempfile::tempdir().unwrap();
            let state_dir = tempfile::tempdir().unwrap();
            let bin = write_stub(
                bin_dir.path(),
                r#"
DIR=$(cd "$(dirname "$0")" && pwd)
: > "$DIR/argv.txt"
OUT=''
while [ "$#" -gt 0 ]; do
  printf '%s\n' "$1" >> "$DIR/argv.txt"
  if [ "$1" = '--output-last-message' ]; then
    shift
    OUT="$1"
    printf '%s\n' "$1" >> "$DIR/argv.txt"
  fi
  shift
done
IFS= read -r prompt
printf '%s\n' "$prompt" > "$DIR/stdin.txt"
printf 'PONG\n' > "$OUT"
echo '{"type":"thread.started","thread_id":"fallback-thread"}'
echo '{"type":"turn.started"}'
echo '{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1}}'
"#,
            );
            let mut exec = executor(bin, workspace.path());
            exec.state_dir = Some(state_dir.path().to_path_buf());
            let assignment = "a private prompt that must not appear in argv";
            let (sink, _rx) = EventSink::channel();
            let outcome = exec
                .run(
                    run_spec(assignment),
                    sink,
                    SteerInbox::disconnected(),
                    CancellationToken::new(),
                )
                .await;
            assert_eq!(outcome.status, TerminalStatus::Completed);
            assert_eq!(outcome.result.as_deref(), Some("PONG"));
            assert_eq!(
                std::fs::read_to_string(bin_dir.path().join("stdin.txt"))
                    .unwrap()
                    .trim(),
                assignment
            );
            let argv = std::fs::read_to_string(bin_dir.path().join("argv.txt")).unwrap();
            assert!(
                !argv.contains(assignment),
                "prompt leaked into argv: {argv}"
            );
            for required in [
                "exec",
                "--json",
                "--cd",
                "--skip-git-repo-check",
                "--sandbox",
                "read-only",
                "--ignore-user-config",
                "--ignore-rules",
                "--output-last-message",
                "-",
            ] {
                assert!(
                    argv.lines().any(|arg| arg == required),
                    "missing {required}: {argv}"
                );
            }
        }

        #[tokio::test]
        async fn cancellation_kills_the_entire_process_group() {
            let workspace = tempfile::tempdir().unwrap();
            let bin_dir = tempfile::tempdir().unwrap();
            let bin = write_stub(
                bin_dir.path(),
                r#"
DIR=$(cd "$(dirname "$0")" && pwd)
read -r prompt
echo '{"type":"thread.started","thread_id":"cancel-thread"}'
sleep 30 &
echo $! > "$DIR/grandchild.pid"
wait
"#,
            );
            let pid_path = bin_dir.path().join("grandchild.pid");
            let (sink, _rx) = EventSink::channel();
            let cancel = CancellationToken::new();
            let cancel_for_run = cancel.clone();
            let run = tokio::spawn(async move {
                executor(bin, workspace.path())
                    .run(
                        run_spec("wait"),
                        sink,
                        SteerInbox::disconnected(),
                        cancel_for_run,
                    )
                    .await
            });

            for _ in 0..500 {
                if pid_path.exists() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(pid_path.exists(), "grandchild pid was recorded");
            let grandchild_pid: libc::pid_t = std::fs::read_to_string(&pid_path)
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            cancel.cancel();
            let outcome = tokio::time::timeout(Duration::from_secs(10), run)
                .await
                .expect("cancel completed within TERM/KILL bound")
                .unwrap();
            assert_eq!(outcome.status, TerminalStatus::Cancelled);

            for _ in 0..100 {
                // SAFETY: signal 0 only probes existence and does not signal.
                if unsafe { libc::kill(grandchild_pid, 0) } == -1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            // SAFETY: signal 0 only probes existence and does not signal.
            assert_eq!(unsafe { libc::kill(grandchild_pid, 0) }, -1);
        }
    }
}
