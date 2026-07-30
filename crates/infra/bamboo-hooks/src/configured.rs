use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use bamboo_agent_core::{AgentHook, Session};
use bamboo_config::{LifecycleHookGroup, LifecycleHookHandler, LifecycleHooksConfig};
use bamboo_domain::{
    AgentHookPoint, HookPayload, HookResult, SessionEndStatus, SessionStartSource,
};
use bamboo_infrastructure::{
    build_command_environment, hide_window_for_tokio_command, preferred_bash_shell,
};
use chrono::Utc;
use regex::Regex;
use rquickjs::{CatchResultExt, Context, Promise, Runtime};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tracing::warn;

use crate::HookDispatcher;

const HOOK_OUTPUT_LIMIT_BYTES: usize = 64 * 1024;

/// User-facing lifecycle events that currently map to engine hook seams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleHookEvent {
    SessionStart,
    UserPromptSubmit,
    PreToolUse,
    PostToolUse,
    Stop,
    SessionEnd,
    PreCompact,
    Notification,
}

/// Raw handler result returned by the settings dry-run endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LifecycleHookTestOutput {
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

/// Backward-compatible name for the command-only dry-run response.
pub type ShellHookTestOutput = LifecycleHookTestOutput;
/// Backward-compatible name retained for SDK consumers.
pub type ShellHookEvent = LifecycleHookEvent;

impl LifecycleHookEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SessionStart => "SessionStart",
            Self::UserPromptSubmit => "UserPromptSubmit",
            Self::PreToolUse => "PreToolUse",
            Self::PostToolUse => "PostToolUse",
            Self::Stop => "Stop",
            Self::SessionEnd => "SessionEnd",
            Self::PreCompact => "PreCompact",
            Self::Notification => "Notification",
        }
    }

    fn point(self) -> AgentHookPoint {
        match self {
            Self::SessionStart => AgentHookPoint::AfterSessionSetup,
            Self::UserPromptSubmit => AgentHookPoint::BeforeSessionSetup,
            Self::PreToolUse => AgentHookPoint::BeforeToolExecution,
            Self::PostToolUse => AgentHookPoint::AfterToolExecution,
            Self::Stop => AgentHookPoint::BeforeFinalize,
            Self::SessionEnd => AgentHookPoint::AfterSessionEnd,
            Self::PreCompact => AgentHookPoint::BeforeCompression,
            Self::Notification => AgentHookPoint::AfterNotification,
        }
    }

    fn supports_tool_matcher(self) -> bool {
        matches!(self, Self::PreToolUse | Self::PostToolUse)
    }
}

/// One config-driven lifecycle shell command.
pub struct ShellCommandHook {
    event: LifecycleHookEvent,
    event_name_override: Option<&'static str>,
    command: String,
    timeout: Duration,
    matcher: Option<Regex>,
    fallback_cwd: Option<PathBuf>,
    name: String,
}

impl ShellCommandHook {
    pub fn new(
        event: LifecycleHookEvent,
        command: impl Into<String>,
        timeout_ms: u64,
        matcher: Option<&str>,
        fallback_cwd: Option<PathBuf>,
        sequence: usize,
    ) -> Result<Self, regex::Error> {
        let matcher = matcher.map(Regex::new).transpose()?;
        Ok(Self {
            event,
            event_name_override: None,
            command: command.into(),
            timeout: Duration::from_millis(timeout_ms.max(1)),
            matcher,
            fallback_cwd,
            name: format!("lifecycle_shell:{}:{sequence}", event.as_str()),
        })
    }

    fn event_name(&self) -> &'static str {
        self.event_name_override
            .unwrap_or_else(|| self.event.as_str())
    }

    fn effective_cwd(&self, session: &Session) -> Option<PathBuf> {
        session
            .workspace
            .as_deref()
            .filter(|workspace| !workspace.trim().is_empty())
            .map(PathBuf::from)
            .or_else(|| self.fallback_cwd.clone())
            .or_else(|| std::env::current_dir().ok())
    }

    fn envelope(
        &self,
        payload: &HookPayload,
        session: &Session,
        cwd: Option<&PathBuf>,
    ) -> HookEnvelope {
        build_hook_envelope(self.event_name(), payload, session, cwd)
    }

    async fn execute(
        &self,
        input: Vec<u8>,
        cwd: Option<&PathBuf>,
        session: &Session,
    ) -> Result<CommandOutput, String> {
        let shell = preferred_bash_shell();
        let overrides = bamboo_llm::Config::current_env_vars();
        let prepared_env = build_command_environment(&overrides).await;
        let mut command = Command::new(&shell.program);
        hide_window_for_tokio_command(&mut command);
        prepared_env.apply_to_tokio_command(&mut command);
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        command
            .arg(shell.arg)
            .arg(&self.command)
            .env("BAMBOO_SESSION_ID", &session.id)
            .env("BAMBOO_HOOK_EVENT", self.event_name())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            // Put every hook in its own process group so a timeout kills the
            // complete command tree. Killing only the shell can leave a child
            // holding stdout/stderr open and make the nominal timeout wait for
            // that child to exit.
            command.process_group(0);
        }

        let mut child = command
            .spawn()
            .map_err(|error| format!("failed to spawn lifecycle hook: {error}"))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| "failed to open lifecycle hook stdin".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "failed to capture lifecycle hook stdout".to_string())?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| "failed to capture lifecycle hook stderr".to_string())?;

        let input_task = tokio::spawn(async move {
            let result = stdin.write_all(&input).await;
            let _ = stdin.shutdown().await;
            result
        });
        let stdout_task = tokio::spawn(read_capped(stdout));
        let stderr_task = tokio::spawn(read_capped(stderr));

        let (status, timed_out) = match tokio::time::timeout(self.timeout, child.wait()).await {
            Ok(Ok(status)) => (Some(status), false),
            Ok(Err(error)) => return Err(format!("failed waiting for lifecycle hook: {error}")),
            Err(_) => {
                kill_hook_process_tree(&mut child).await;
                (None, true)
            }
        };

        if let Ok(Err(error)) = input_task.await {
            warn!(hook = %self.name, error = %error, "failed writing lifecycle hook stdin");
        }
        let stdout = stdout_task
            .await
            .map_err(|error| format!("lifecycle hook stdout task failed: {error}"))?
            .map_err(|error| format!("failed reading lifecycle hook stdout: {error}"))?;
        let stderr = stderr_task
            .await
            .map_err(|error| format!("lifecycle hook stderr task failed: {error}"))?
            .map_err(|error| format!("failed reading lifecycle hook stderr: {error}"))?;

        Ok(CommandOutput {
            exit_code: status.and_then(|status| status.code()),
            stdout,
            stderr,
            timed_out,
        })
    }

    fn interpret(&self, output: CommandOutput) -> HookResult {
        if output.stdout.truncated || output.stderr.truncated {
            warn!(
                hook = %self.name,
                stdout_truncated = output.stdout.truncated,
                stderr_truncated = output.stderr.truncated,
                "lifecycle hook output exceeded capture limit"
            );
        }
        if output.timed_out {
            warn!(hook = %self.name, "lifecycle hook timed out and was killed");
            return HookResult::Continue;
        }

        match output.exit_code {
            Some(0) => self.interpret_success(&output.stdout.bytes),
            Some(2) => {
                let reason = String::from_utf8_lossy(&output.stderr.bytes)
                    .trim()
                    .to_string();
                HookResult::Deny {
                    reason: if reason.is_empty() {
                        "lifecycle hook exited with blocking status 2".to_string()
                    } else {
                        reason
                    },
                }
            }
            exit_code => {
                warn!(hook = %self.name, ?exit_code, "lifecycle hook failed non-blocking");
                HookResult::Continue
            }
        }
    }

    fn interpret_success(&self, stdout: &[u8]) -> HookResult {
        let stdout = String::from_utf8_lossy(stdout);
        let stdout = stdout.trim();
        if stdout.is_empty() {
            return HookResult::Continue;
        }

        let response: HookResponse = match serde_json::from_str(stdout) {
            Ok(response) => response,
            Err(error) => {
                warn!(hook = %self.name, error = %error, "ignoring malformed lifecycle hook response");
                return HookResult::Continue;
            }
        };
        interpret_response(response)
    }

    async fn test(
        &self,
        payload: &HookPayload,
        session: &Session,
    ) -> Result<LifecycleHookTestOutput, String> {
        let cwd = self.effective_cwd(session);
        let input = serde_json::to_vec(&self.envelope(payload, session, cwd.as_ref()))
            .map_err(|error| format!("failed serializing lifecycle hook test payload: {error}"))?;
        let output = self.execute(input, cwd.as_ref(), session).await?;
        Ok(LifecycleHookTestOutput {
            exit_code: output.exit_code,
            stdout: String::from_utf8_lossy(&output.stdout.bytes).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr.bytes).into_owned(),
            timed_out: output.timed_out,
            stdout_truncated: output.stdout.truncated,
            stderr_truncated: output.stderr.truncated,
        })
    }
}

/// Execute one configured handler against a deterministic synthetic payload.
/// Production runtime limits and output caps are preserved, while result
/// interpretation is skipped so callers receive raw diagnostics.
pub async fn test_lifecycle_handler(
    event_name: &str,
    handler: &LifecycleHookHandler,
    fallback_cwd: Option<PathBuf>,
) -> Result<LifecycleHookTestOutput, String> {
    let (event, payload) = synthetic_test_payload(event_name)?;
    let session = Session::new("lifecycle-hook-test", "hook-test");

    match handler {
        LifecycleHookHandler::Command {
            command,
            timeout_ms,
        } => {
            let mut hook =
                ShellCommandHook::new(event, command, *timeout_ms, None, fallback_cwd, 0)
                    .map_err(|error| format!("invalid lifecycle hook matcher: {error}"))?;
            hook.name = format!("lifecycle_shell_test:{event_name}");
            hook.test(&payload, &session).await
        }
        LifecycleHookHandler::JavaScript {
            source,
            timeout_ms,
            memory_limit_bytes,
        } => {
            let mut hook = JavaScriptHook::new(
                event,
                source,
                *timeout_ms,
                *memory_limit_bytes,
                None,
                fallback_cwd,
                0,
            )
            .map_err(|error| format!("invalid lifecycle hook matcher: {error}"))?;
            hook.name = format!("lifecycle_javascript_test:{event_name}");
            hook.test(&payload, &session).await
        }
    }
}

/// Backward-compatible command-only dry-run entry point.
pub async fn test_lifecycle_shell_command(
    event_name: &str,
    command: &str,
    timeout_ms: u64,
    fallback_cwd: Option<PathBuf>,
) -> Result<LifecycleHookTestOutput, String> {
    test_lifecycle_handler(
        event_name,
        &LifecycleHookHandler::command(command, timeout_ms),
        fallback_cwd,
    )
    .await
}

fn synthetic_test_payload(event_name: &str) -> Result<(LifecycleHookEvent, HookPayload), String> {
    let value = match event_name {
        "SessionStart" => (
            LifecycleHookEvent::SessionStart,
            HookPayload::SessionSetup {
                initial_message: "Lifecycle hook test".to_string(),
                source: SessionStartSource::Startup,
            },
        ),
        "UserPromptSubmit" => (
            LifecycleHookEvent::UserPromptSubmit,
            HookPayload::Prompt {
                prompt: "Lifecycle hook test".to_string(),
            },
        ),
        "PreToolUse" => (
            LifecycleHookEvent::PreToolUse,
            HookPayload::ToolExecution {
                tool_name: "Bash".to_string(),
                tool_call_id: "hook-test-call".to_string(),
                parsed_args: serde_json::json!({"command": "echo lifecycle-hook-test"}),
            },
        ),
        "PostToolUse" => (
            LifecycleHookEvent::PostToolUse,
            HookPayload::ToolResult {
                tool_name: "Bash".to_string(),
                tool_call_id: "hook-test-call".to_string(),
                outcome: bamboo_domain::HookToolOutcome {
                    success: true,
                    result: Some("lifecycle-hook-test".to_string()),
                    error: None,
                    needs_human: false,
                    duration_ms: 1,
                },
            },
        ),
        "Stop" => (
            LifecycleHookEvent::Stop,
            HookPayload::Finalize {
                stop_hook_active: false,
            },
        ),
        "SessionEnd" => (
            LifecycleHookEvent::SessionEnd,
            HookPayload::SessionEnd {
                status: SessionEndStatus::Completed,
                completion_reason: Some("lifecycle hook test".to_string()),
            },
        ),
        "PreCompact" => (
            LifecycleHookEvent::PreCompact,
            HookPayload::Compression {
                estimated_tokens: 1_000,
                usage_percent: 50.0,
                max_context_tokens: 2_000,
                trigger_context_tokens: 1_600,
                trigger: "threshold".to_string(),
                phase: "test".to_string(),
            },
        ),
        "Notification" => (
            LifecycleHookEvent::Notification,
            HookPayload::Notification {
                id: Some("notification-test".to_string()),
                category: "custom".to_string(),
                priority: "normal".to_string(),
                title: "Lifecycle hook test".to_string(),
                body: "Synthetic notification delivery".to_string(),
                dedup_key: Some("lifecycle-hook-test".to_string()),
                created_at: Some(Utc::now().to_rfc3339()),
                click_url: None,
            },
        ),
        other => return Err(format!("unknown lifecycle hook event '{other}'")),
    };
    Ok(value)
}

#[cfg(unix)]
async fn kill_hook_process_tree(child: &mut Child) {
    if let Some(pid) = child.id() {
        // SAFETY: the child was spawned as the leader of its own process group,
        // so the negative pid targets only that hook and its descendants.
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
    }
    let _ = child.wait().await;
}

#[cfg(windows)]
async fn kill_hook_process_tree(child: &mut Child) {
    if let Some(pid) = child.id() {
        let pid = pid.to_string();
        let mut kill = Command::new("taskkill");
        hide_window_for_tokio_command(&mut kill);
        let _ = kill.args(["/F", "/T", "/PID", &pid]).status().await;
    }
    let _ = child.start_kill();
    let _ = child.wait().await;
}

#[cfg(not(any(unix, windows)))]
async fn kill_hook_process_tree(child: &mut Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[async_trait]
impl AgentHook for ShellCommandHook {
    fn point(&self) -> AgentHookPoint {
        self.event.point()
    }

    async fn run(
        &self,
        _point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
    ) -> HookResult {
        let cwd = self.effective_cwd(session);
        let input = match serde_json::to_vec(&self.envelope(payload, session, cwd.as_ref())) {
            Ok(input) => input,
            Err(error) => {
                warn!(hook = %self.name, error = %error, "failed serializing lifecycle hook payload");
                return HookResult::Continue;
            }
        };
        match self.execute(input, cwd.as_ref(), session).await {
            Ok(output) => self.interpret(output),
            Err(error) => {
                warn!(hook = %self.name, error = %error, "lifecycle hook execution failed non-blocking");
                HookResult::Continue
            }
        }
    }

    fn matches(&self, payload: &HookPayload) -> bool {
        let Some(matcher) = &self.matcher else {
            return true;
        };
        match payload {
            HookPayload::ToolExecution { tool_name, .. }
            | HookPayload::ToolResult { tool_name, .. } => matcher.is_match(tool_name),
            _ => false,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

const JAVASCRIPT_HOOK_MAX_STACK_BYTES: usize = 512 * 1024;
const JAVASCRIPT_INVOKE_SOURCE: &str = r#"
(async () => {
    "use strict";
    const hookFunction = globalThis.hook;
    if (typeof hookFunction !== "function") {
        throw new TypeError("JavaScript lifecycle hook must define globalThis.hook(input)");
    }
    const input = Object.freeze(JSON.parse(globalThis.__bamboo_hook_input_json));
    const output = await hookFunction(input);
    if (output === undefined || output === null) {
        return "{}";
    }
    const encoded = JSON.stringify(output);
    if (encoded === undefined) {
        throw new TypeError("JavaScript lifecycle hook result must be JSON-serializable");
    }
    return encoded;
})()
"#;

/// One config-driven JavaScript lifecycle handler.
///
/// Every invocation gets a fresh QuickJS runtime and context. No module loader
/// or host functions are installed, so scripts cannot access Bamboo's
/// filesystem, network, process, or environment through this runtime.
pub struct JavaScriptHook {
    event: LifecycleHookEvent,
    source: String,
    timeout: Duration,
    memory_limit_bytes: usize,
    matcher: Option<Regex>,
    fallback_cwd: Option<PathBuf>,
    name: String,
}

impl JavaScriptHook {
    pub fn new(
        event: LifecycleHookEvent,
        source: impl Into<String>,
        timeout_ms: u64,
        memory_limit_bytes: usize,
        matcher: Option<&str>,
        fallback_cwd: Option<PathBuf>,
        sequence: usize,
    ) -> Result<Self, regex::Error> {
        Ok(Self {
            event,
            source: source.into(),
            timeout: Duration::from_millis(timeout_ms.max(1)),
            memory_limit_bytes: memory_limit_bytes.max(1),
            matcher: matcher.map(Regex::new).transpose()?,
            fallback_cwd,
            name: format!("lifecycle_javascript:{}:{sequence}", event.as_str()),
        })
    }

    fn effective_cwd(&self, session: &Session) -> Option<PathBuf> {
        session
            .workspace
            .as_deref()
            .filter(|workspace| !workspace.trim().is_empty())
            .map(PathBuf::from)
            .or_else(|| self.fallback_cwd.clone())
            .or_else(|| std::env::current_dir().ok())
    }

    fn envelope(
        &self,
        payload: &HookPayload,
        session: &Session,
        cwd: Option<&PathBuf>,
    ) -> HookEnvelope {
        build_hook_envelope(self.event.as_str(), payload, session, cwd)
    }

    async fn execute(&self, input_json: String) -> Result<JavaScriptExecutionOutput, String> {
        let source = self.source.clone();
        let timeout = self.timeout;
        let memory_limit_bytes = self.memory_limit_bytes;
        tokio::task::spawn_blocking(move || {
            execute_javascript(source, input_json, timeout, memory_limit_bytes)
        })
        .await
        .map_err(|error| format!("JavaScript lifecycle hook task failed: {error}"))
    }

    fn interpret(&self, output: &JavaScriptExecutionOutput) -> HookResult {
        if output.timed_out {
            warn!(hook = %self.name, "JavaScript lifecycle hook exceeded its deadline");
            return HookResult::Continue;
        }
        if !output.stderr.is_empty() {
            warn!(
                hook = %self.name,
                error = %output.stderr,
                "JavaScript lifecycle hook failed non-blocking"
            );
            return HookResult::Continue;
        }
        if output.stdout_truncated {
            warn!(
                hook = %self.name,
                "JavaScript lifecycle hook result exceeded the output limit"
            );
            return HookResult::Continue;
        }
        match serde_json::from_str::<HookResponse>(&output.stdout) {
            Ok(response) => interpret_response(response),
            Err(error) => {
                warn!(
                    hook = %self.name,
                    error = %error,
                    "ignoring malformed JavaScript lifecycle hook response"
                );
                HookResult::Continue
            }
        }
    }

    async fn test(
        &self,
        payload: &HookPayload,
        session: &Session,
    ) -> Result<LifecycleHookTestOutput, String> {
        let cwd = self.effective_cwd(session);
        let input_json = serde_json::to_string(&self.envelope(payload, session, cwd.as_ref()))
            .map_err(|error| format!("failed serializing lifecycle hook test payload: {error}"))?;
        let output = self.execute(input_json).await?;
        Ok(LifecycleHookTestOutput {
            exit_code: None,
            stdout: output.stdout,
            stderr: output.stderr,
            timed_out: output.timed_out,
            stdout_truncated: output.stdout_truncated,
            stderr_truncated: output.stderr_truncated,
        })
    }
}

#[async_trait]
impl AgentHook for JavaScriptHook {
    fn point(&self) -> AgentHookPoint {
        self.event.point()
    }

    async fn run(
        &self,
        _point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
    ) -> HookResult {
        let cwd = self.effective_cwd(session);
        let input_json = match serde_json::to_string(&self.envelope(payload, session, cwd.as_ref()))
        {
            Ok(input) => input,
            Err(error) => {
                warn!(
                    hook = %self.name,
                    error = %error,
                    "failed serializing JavaScript lifecycle hook payload"
                );
                return HookResult::Continue;
            }
        };
        match self.execute(input_json).await {
            Ok(output) => self.interpret(&output),
            Err(error) => {
                warn!(
                    hook = %self.name,
                    error = %error,
                    "JavaScript lifecycle hook execution failed non-blocking"
                );
                HookResult::Continue
            }
        }
    }

    fn matches(&self, payload: &HookPayload) -> bool {
        let Some(matcher) = &self.matcher else {
            return true;
        };
        match payload {
            HookPayload::ToolExecution { tool_name, .. }
            | HookPayload::ToolResult { tool_name, .. } => matcher.is_match(tool_name),
            _ => false,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Default)]
struct JavaScriptExecutionOutput {
    stdout: String,
    stderr: String,
    timed_out: bool,
    stdout_truncated: bool,
    stderr_truncated: bool,
}

fn execute_javascript(
    source: String,
    input_json: String,
    timeout: Duration,
    memory_limit_bytes: usize,
) -> JavaScriptExecutionOutput {
    let timed_out = Arc::new(AtomicBool::new(false));
    let runtime = match Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            return javascript_error_output(format!("failed creating QuickJS runtime: {error}"));
        }
    };
    runtime.set_memory_limit(memory_limit_bytes);
    runtime.set_max_stack_size(JAVASCRIPT_HOOK_MAX_STACK_BYTES);
    let deadline = Instant::now() + timeout;
    let interrupt_timed_out = timed_out.clone();
    runtime.set_interrupt_handler(Some(Box::new(move || {
        let expired = Instant::now() >= deadline;
        if expired {
            interrupt_timed_out.store(true, Ordering::Relaxed);
        }
        expired
    })));

    let context = match Context::full(&runtime) {
        Ok(context) => context,
        Err(error) => {
            return javascript_error_output(format!("failed creating QuickJS context: {error}"));
        }
    };
    let result = context.with(|ctx| -> Result<String, String> {
        ctx.eval::<(), _>(source.as_str())
            .catch(&ctx)
            .map_err(|error| error.to_string())?;
        ctx.globals()
            .set("__bamboo_hook_input_json", input_json)
            .catch(&ctx)
            .map_err(|error| error.to_string())?;
        let promise: Promise = ctx
            .eval(JAVASCRIPT_INVOKE_SOURCE)
            .catch(&ctx)
            .map_err(|error| error.to_string())?;
        promise
            .finish::<String>()
            .catch(&ctx)
            .map_err(|error| error.to_string())
    });

    match result {
        Ok(stdout) => {
            let (stdout, stdout_truncated) = cap_text(stdout);
            JavaScriptExecutionOutput {
                stdout,
                stdout_truncated,
                ..JavaScriptExecutionOutput::default()
            }
        }
        Err(error) => {
            let (stderr, stderr_truncated) = cap_text(error);
            JavaScriptExecutionOutput {
                stderr,
                timed_out: timed_out.load(Ordering::Relaxed),
                stderr_truncated,
                ..JavaScriptExecutionOutput::default()
            }
        }
    }
}

fn javascript_error_output(error: String) -> JavaScriptExecutionOutput {
    let (stderr, stderr_truncated) = cap_text(error);
    JavaScriptExecutionOutput {
        stderr,
        stderr_truncated,
        ..JavaScriptExecutionOutput::default()
    }
}

fn cap_text(mut text: String) -> (String, bool) {
    if text.len() <= HOOK_OUTPUT_LIMIT_BYTES {
        return (text, false);
    }
    let mut boundary = HOOK_OUTPUT_LIMIT_BYTES;
    while !text.is_char_boundary(boundary) {
        boundary -= 1;
    }
    text.truncate(boundary);
    (text, true)
}

fn build_hook_envelope(
    event_name: &str,
    payload: &HookPayload,
    session: &Session,
    cwd: Option<&PathBuf>,
) -> HookEnvelope {
    let (
        tool_name,
        tool_input,
        tool_response,
        prompt,
        source,
        stop_hook_active,
        terminal_status,
        completion_reason,
    ) = match payload {
        HookPayload::SessionSetup {
            initial_message,
            source,
        } => (
            None,
            None,
            None,
            Some(initial_message.clone()),
            Some(*source),
            None,
            None,
            None,
        ),
        HookPayload::Prompt { prompt } => (
            None,
            None,
            None,
            Some(prompt.clone()),
            None,
            None,
            None,
            None,
        ),
        HookPayload::ToolExecution {
            tool_name,
            parsed_args,
            ..
        } => (
            Some(tool_name.clone()),
            Some(parsed_args.clone()),
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        HookPayload::ToolResult {
            tool_name, outcome, ..
        } => (
            Some(tool_name.clone()),
            None,
            serde_json::to_value(outcome).ok(),
            None,
            None,
            None,
            None,
            None,
        ),
        HookPayload::Finalize { stop_hook_active } => (
            None,
            None,
            None,
            None,
            None,
            Some(*stop_hook_active),
            None,
            None,
        ),
        HookPayload::SessionEnd {
            status,
            completion_reason,
        } => (
            None,
            None,
            None,
            None,
            None,
            None,
            Some(*status),
            completion_reason.clone(),
        ),
        HookPayload::None
        | HookPayload::Round { .. }
        | HookPayload::Compression { .. }
        | HookPayload::Notification { .. } => (None, None, None, None, None, None, None, None),
    };

    HookEnvelope {
        schema_version: 1,
        hook_event_name: event_name.to_string(),
        session_id: session.id.clone(),
        workspace_path: cwd
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_default(),
        model: session.model.clone(),
        payload: payload.clone(),
        tool_name,
        tool_input,
        tool_response,
        prompt,
        source,
        stop_hook_active,
        terminal_status,
        completion_reason,
        timestamp: Utc::now().to_rfc3339(),
    }
}

#[derive(Debug, Serialize)]
struct HookEnvelope {
    schema_version: u8,
    hook_event_name: String,
    session_id: String,
    workspace_path: String,
    model: String,
    /// Complete, event-specific payload. Legacy convenience fields below stay
    /// populated for existing tool/prompt/session hook commands.
    payload: HookPayload,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_input: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_response: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<SessionStartSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop_hook_active: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    terminal_status: Option<SessionEndStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completion_reason: Option<String>,
    timestamp: String,
}

#[derive(Debug, Deserialize)]
struct HookResponse {
    #[serde(default)]
    decision: Option<HookDecision>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    additional_context: Option<String>,
    #[serde(default)]
    suppress_output: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum HookDecision {
    Block,
    Allow,
    Ask,
}

fn interpret_response(response: HookResponse) -> HookResult {
    let HookResponse {
        decision,
        reason,
        additional_context,
        suppress_output: _suppress_output,
    } = response;
    let result = match decision {
        Some(HookDecision::Block) => HookResult::Deny {
            reason: reason
                .filter(|reason| !reason.trim().is_empty())
                .unwrap_or_else(|| "blocked by lifecycle hook".to_string()),
        },
        Some(HookDecision::Allow) => HookResult::Allow,
        Some(HookDecision::Ask) => HookResult::Ask,
        None => HookResult::Continue,
    };
    match additional_context.filter(|context| !context.trim().is_empty()) {
        Some(text) if matches!(result, HookResult::Continue) => HookResult::InjectContext { text },
        Some(text) => HookResult::WithContext {
            result: Box::new(result),
            text,
        },
        None => result,
    }
}

#[derive(Debug)]
struct CommandOutput {
    exit_code: Option<i32>,
    stdout: CapturedOutput,
    stderr: CapturedOutput,
    timed_out: bool,
}

#[derive(Debug, Default)]
struct CapturedOutput {
    bytes: Vec<u8>,
    truncated: bool,
}

async fn read_capped(mut reader: impl AsyncRead + Unpin) -> Result<CapturedOutput, std::io::Error> {
    let mut captured = CapturedOutput::default();
    let mut chunk = [0_u8; 8192];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        let remaining = HOOK_OUTPUT_LIMIT_BYTES.saturating_sub(captured.bytes.len());
        let keep = remaining.min(read);
        captured.bytes.extend_from_slice(&chunk[..keep]);
        captured.truncated |= keep < read;
    }
    Ok(captured)
}

pub(super) fn register_configured_hooks(
    dispatcher: &mut HookDispatcher,
    config: &LifecycleHooksConfig,
    fallback_cwd: Option<PathBuf>,
) {
    if !config.enabled {
        return;
    }

    let events: [(LifecycleHookEvent, &[LifecycleHookGroup]); 8] = [
        (LifecycleHookEvent::SessionStart, &config.session_start),
        (
            LifecycleHookEvent::UserPromptSubmit,
            &config.user_prompt_submit,
        ),
        (LifecycleHookEvent::PreToolUse, &config.pre_tool_use),
        (LifecycleHookEvent::PostToolUse, &config.post_tool_use),
        (LifecycleHookEvent::Stop, &config.stop),
        (LifecycleHookEvent::SessionEnd, &config.session_end),
        (LifecycleHookEvent::PreCompact, &config.pre_compact),
        (LifecycleHookEvent::Notification, &config.notification),
    ];
    let mut sequence = 0_usize;
    for (event, groups) in events {
        for group in groups {
            if !group.enabled {
                sequence += group.hooks.len();
                continue;
            }
            let matcher = if event.supports_tool_matcher() {
                group.matcher.as_deref()
            } else {
                if group.matcher.is_some() {
                    warn!(
                        event = event.as_str(),
                        "ignoring matcher on non-tool lifecycle hook"
                    );
                }
                None
            };
            for handler in &group.hooks {
                let hook: Result<Arc<dyn AgentHook>, regex::Error> = match handler {
                    LifecycleHookHandler::Command {
                        command,
                        timeout_ms,
                    } => ShellCommandHook::new(
                        event,
                        command,
                        *timeout_ms,
                        matcher,
                        fallback_cwd.clone(),
                        sequence,
                    )
                    .map(|hook| Arc::new(hook) as Arc<dyn AgentHook>),
                    LifecycleHookHandler::JavaScript {
                        source,
                        timeout_ms,
                        memory_limit_bytes,
                    } => JavaScriptHook::new(
                        event,
                        source,
                        *timeout_ms,
                        *memory_limit_bytes,
                        matcher,
                        fallback_cwd.clone(),
                        sequence,
                    )
                    .map(|hook| Arc::new(hook) as Arc<dyn AgentHook>),
                };
                match hook {
                    Ok(hook) => dispatcher.register(hook),
                    Err(error) => {
                        warn!(
                            event = event.as_str(),
                            error = %error,
                            "skipping lifecycle hook with invalid matcher"
                        );
                    }
                }
                sequence += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_config::{
        DEFAULT_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES, DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
    };
    use serde_json::json;
    use std::time::Instant;

    fn command(command: impl Into<String>, timeout_ms: u64) -> LifecycleHookHandler {
        LifecycleHookHandler::command(command, timeout_ms)
    }

    fn javascript(source: impl Into<String>, timeout_ms: u64) -> LifecycleHookHandler {
        LifecycleHookHandler::javascript(
            source,
            timeout_ms,
            DEFAULT_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES,
        )
    }

    fn shell_hook(
        event: LifecycleHookEvent,
        command: impl Into<String>,
        timeout_ms: u64,
        matcher: Option<&str>,
        sequence: usize,
    ) -> ShellCommandHook {
        ShellCommandHook::new(event, command, timeout_ms, matcher, None, sequence).unwrap()
    }

    fn javascript_hook(
        event: LifecycleHookEvent,
        source: impl Into<String>,
        timeout_ms: u64,
        memory_limit_bytes: usize,
        matcher: Option<&str>,
    ) -> JavaScriptHook {
        JavaScriptHook::new(
            event,
            source,
            timeout_ms,
            memory_limit_bytes,
            matcher,
            None,
            0,
        )
        .unwrap()
    }

    fn session(workspace: &std::path::Path) -> Session {
        let mut session = Session::new("session-1", "test-model");
        session.workspace = Some(workspace.to_string_lossy().into_owned());
        session
    }

    #[tokio::test]
    async fn exit_zero_without_output_passes_through() {
        let dir = tempfile::tempdir().unwrap();
        let hook = shell_hook(
            ShellHookEvent::SessionStart,
            "printf ''",
            DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
            None,
            0,
        );
        let result = hook
            .run(
                AgentHookPoint::AfterSessionSetup,
                &HookPayload::SessionSetup {
                    initial_message: "hello".to_string(),
                    source: SessionStartSource::Startup,
                },
                &session(dir.path()),
            )
            .await;
        assert_eq!(result, HookResult::Continue);
    }

    #[tokio::test]
    async fn exit_two_blocks_with_stderr_reason() {
        let dir = tempfile::tempdir().unwrap();
        let hook = shell_hook(
            ShellHookEvent::PreToolUse,
            "printf 'blocked by policy' >&2; exit 2",
            1_000,
            None,
            0,
        );
        let result = hook
            .run(
                AgentHookPoint::BeforeToolExecution,
                &HookPayload::ToolExecution {
                    tool_name: "bash".to_string(),
                    tool_call_id: "call-1".to_string(),
                    parsed_args: json!({"command": "pwd"}),
                },
                &session(dir.path()),
            )
            .await;
        assert_eq!(
            result,
            HookResult::Deny {
                reason: "blocked by policy".to_string()
            }
        );
    }

    #[tokio::test]
    async fn versioned_json_protocol_is_delivered_on_stdin() {
        let dir = tempfile::tempdir().unwrap();
        let shell = r#"payload=$(cat); case "$payload" in *'"hook_event_name":"PreToolUse"'*'"tool_name":"bash"'*) printf '%s' '{"decision":"allow"}' ;; *) printf 'bad payload' >&2; exit 2 ;; esac"#;
        let hook = shell_hook(ShellHookEvent::PreToolUse, shell, 1_000, None, 0);
        let result = hook
            .run(
                AgentHookPoint::BeforeToolExecution,
                &HookPayload::ToolExecution {
                    tool_name: "bash".to_string(),
                    tool_call_id: "call-1".to_string(),
                    parsed_args: json!({"command": "pwd"}),
                },
                &session(dir.path()),
            )
            .await;
        assert_eq!(result, HookResult::Allow);
    }

    #[tokio::test]
    async fn stdout_decisions_and_context_are_parsed() {
        let dir = tempfile::tempdir().unwrap();
        for (body, expected) in [
            (r#"{"decision":"allow"}"#, HookResult::Allow),
            (r#"{"decision":"ask"}"#, HookResult::Ask),
            (
                r#"{"decision":"block","reason":"nope"}"#,
                HookResult::Deny {
                    reason: "nope".to_string(),
                },
            ),
            (
                r#"{"additional_context":"remember this"}"#,
                HookResult::InjectContext {
                    text: "remember this".to_string(),
                },
            ),
            (
                r#"{"decision":"allow","additional_context":"allowed context"}"#,
                HookResult::WithContext {
                    result: Box::new(HookResult::Allow),
                    text: "allowed context".to_string(),
                },
            ),
        ] {
            let shell = format!("printf '%s' '{body}'");
            let hook = shell_hook(ShellHookEvent::SessionStart, shell, 1_000, None, 0);
            assert_eq!(
                hook.run(
                    AgentHookPoint::AfterSessionSetup,
                    &HookPayload::None,
                    &session(dir.path()),
                )
                .await,
                expected
            );
        }
    }

    #[tokio::test]
    async fn runner_preserves_decision_and_context_from_one_response() {
        let dir = tempfile::tempdir().unwrap();
        let mut runner = HookDispatcher::new();
        runner.register(Arc::new(shell_hook(
            ShellHookEvent::SessionStart,
            r#"printf '%s' '{"decision":"allow","additional_context":"keep me"}'"#,
            1_000,
            None,
            0,
        )));
        let session = session(dir.path());
        let report = runner
            .run_hooks(
                AgentHookPoint::AfterSessionSetup,
                &HookPayload::None,
                &session,
            )
            .await;
        assert_eq!(report.outcome.decision, HookResult::Allow);
        assert_eq!(report.outcome.injected_contexts, vec!["keep me"]);
    }

    #[tokio::test]
    async fn malformed_json_is_non_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let hook = shell_hook(
            ShellHookEvent::SessionStart,
            "printf 'not-json'",
            1_000,
            None,
            0,
        );
        assert_eq!(
            hook.run(
                AgentHookPoint::AfterSessionSetup,
                &HookPayload::None,
                &session(dir.path()),
            )
            .await,
            HookResult::Continue
        );
    }

    #[tokio::test]
    async fn timeout_kills_hook_and_continues() {
        let dir = tempfile::tempdir().unwrap();
        let hook = shell_hook(ShellHookEvent::SessionStart, "sleep 5", 25, None, 0);
        let started = Instant::now();
        let result = hook
            .run(
                AgentHookPoint::AfterSessionSetup,
                &HookPayload::None,
                &session(dir.path()),
            )
            .await;
        assert_eq!(result, HookResult::Continue);
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn javascript_hook_reads_envelope_and_awaits_promises() {
        let dir = tempfile::tempdir().unwrap();
        let hook = javascript_hook(
            LifecycleHookEvent::PreToolUse,
            r#"
                async function hook(input) {
                    await Promise.resolve();
                    return {
                        decision: "allow",
                        additional_context: `${input.hook_event_name}:${input.tool_name}:${input.tool_input.command}`
                    };
                }
            "#,
            1_000,
            DEFAULT_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES,
            Some("^Bash$"),
        );
        let result = hook
            .run(
                AgentHookPoint::BeforeToolExecution,
                &HookPayload::ToolExecution {
                    tool_name: "Bash".to_string(),
                    tool_call_id: "call-js-1".to_string(),
                    parsed_args: json!({"command": "pwd"}),
                },
                &session(dir.path()),
            )
            .await;

        assert_eq!(
            result,
            HookResult::WithContext {
                result: Box::new(HookResult::Allow),
                text: "PreToolUse:Bash:pwd".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn javascript_errors_are_diagnostic_and_non_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let hook = javascript_hook(
            LifecycleHookEvent::SessionStart,
            "function hook() { throw new Error('policy exploded'); }",
            1_000,
            DEFAULT_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES,
            None,
        );

        assert_eq!(
            hook.run(
                AgentHookPoint::AfterSessionSetup,
                &HookPayload::None,
                &session(dir.path()),
            )
            .await,
            HookResult::Continue
        );
        let output = hook
            .test(&HookPayload::None, &session(dir.path()))
            .await
            .unwrap();
        assert!(output.stderr.contains("policy exploded"), "{output:?}");
        assert!(!output.timed_out);
    }

    #[tokio::test]
    async fn javascript_infinite_loop_is_interrupted_by_wall_clock_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let hook = javascript_hook(
            LifecycleHookEvent::SessionStart,
            "function hook() { while (true) {} }",
            25,
            DEFAULT_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES,
            None,
        );
        let started = Instant::now();
        let output = hook
            .test(&HookPayload::None, &session(dir.path()))
            .await
            .unwrap();

        assert!(output.timed_out, "{output:?}");
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn javascript_memory_limit_stops_unbounded_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let hook = javascript_hook(
            LifecycleHookEvent::SessionStart,
            r#"
                function hook() {
                    const values = [];
                    while (true) {
                        values.push("x".repeat(4096));
                    }
                }
            "#,
            2_000,
            4 * 1024 * 1024,
            None,
        );
        let output = hook
            .test(&HookPayload::None, &session(dir.path()))
            .await
            .unwrap();

        assert!(!output.stderr.is_empty(), "{output:?}");
        assert!(!output.timed_out, "{output:?}");
    }

    #[tokio::test]
    async fn javascript_result_is_capped_before_interpretation() {
        let dir = tempfile::tempdir().unwrap();
        let hook = javascript_hook(
            LifecycleHookEvent::SessionStart,
            r#"
                function hook() {
                    return { additional_context: "x".repeat(128 * 1024) };
                }
            "#,
            1_000,
            DEFAULT_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES,
            None,
        );
        let output = hook
            .test(&HookPayload::None, &session(dir.path()))
            .await
            .unwrap();

        assert_eq!(output.stdout.len(), HOOK_OUTPUT_LIMIT_BYTES);
        assert!(output.stdout_truncated, "{output:?}");
        assert!(output.stderr.is_empty(), "{output:?}");
        assert_eq!(
            hook.run(
                AgentHookPoint::AfterSessionSetup,
                &HookPayload::None,
                &session(dir.path()),
            )
            .await,
            HookResult::Continue,
            "a truncated response must fail open instead of applying partial JSON"
        );
    }

    #[tokio::test]
    async fn javascript_runtime_exposes_no_host_io_globals() {
        let dir = tempfile::tempdir().unwrap();
        let hook = javascript_hook(
            LifecycleHookEvent::SessionStart,
            r#"
                function hook() {
                    return {
                        additional_context: [
                            typeof process,
                            typeof require,
                            typeof fetch,
                            typeof Deno,
                            typeof Bun
                        ].join(",")
                    };
                }
            "#,
            1_000,
            DEFAULT_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES,
            None,
        );

        assert_eq!(
            hook.run(
                AgentHookPoint::AfterSessionSetup,
                &HookPayload::None,
                &session(dir.path()),
            )
            .await,
            HookResult::InjectContext {
                text: "undefined,undefined,undefined,undefined,undefined".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn configured_javascript_handler_matches_tools_and_returns_decision() {
        let dir = tempfile::tempdir().unwrap();
        let config = LifecycleHooksConfig {
            enabled: true,
            pre_tool_use: vec![LifecycleHookGroup {
                enabled: true,
                matcher: Some("^Bash$".to_string()),
                hooks: vec![javascript(
                    "function hook() { return { decision: 'block', reason: 'blocked in JS' }; }",
                    1_000,
                )],
            }],
            ..LifecycleHooksConfig::default()
        };
        let dispatcher = HookDispatcher::new().with_lifecycle_config(&config, None);

        let report = dispatcher
            .run_hooks(
                AgentHookPoint::BeforeToolExecution,
                &HookPayload::ToolExecution {
                    tool_name: "Bash".to_string(),
                    tool_call_id: "call-js-2".to_string(),
                    parsed_args: json!({}),
                },
                &session(dir.path()),
            )
            .await;
        assert_eq!(
            report.outcome.decision,
            HookResult::Deny {
                reason: "blocked in JS".to_string(),
            }
        );
    }

    #[test]
    fn matcher_filters_tool_names() {
        let hook = shell_hook(
            ShellHookEvent::PreToolUse,
            "exit 2",
            1_000,
            Some("^(bash|write_file)$"),
            0,
        );
        let payload = |tool_name: &str| HookPayload::ToolExecution {
            tool_name: tool_name.to_string(),
            tool_call_id: "call-1".to_string(),
            parsed_args: json!({}),
        };
        assert!(hook.matches(&payload("bash")));
        assert!(!hook.matches(&payload("read_file")));
    }

    #[test]
    fn envelope_contains_versioned_protocol_fields() {
        let dir = tempfile::tempdir().unwrap();
        let hook = shell_hook(ShellHookEvent::PreToolUse, "true", 1_000, None, 0);
        let session = session(dir.path());
        let payload = HookPayload::ToolExecution {
            tool_name: "bash".to_string(),
            tool_call_id: "call-1".to_string(),
            parsed_args: json!({"command": "pwd"}),
        };
        let value = serde_json::to_value(hook.envelope(
            &payload,
            &session,
            hook.effective_cwd(&session).as_ref(),
        ))
        .unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["hook_event_name"], "PreToolUse");
        assert_eq!(value["session_id"], "session-1");
        assert_eq!(value["model"], "test-model");
        assert_eq!(value["tool_name"], "bash");
        assert_eq!(value["tool_input"]["command"], "pwd");
        assert_eq!(value["payload"]["type"], "tool_execution");
        assert_eq!(value["payload"]["tool_call_id"], "call-1");
        assert!(value["timestamp"].as_str().is_some());
    }

    #[test]
    fn session_envelopes_include_source_stop_and_terminal_fields() {
        let dir = tempfile::tempdir().unwrap();
        let session = session(dir.path());
        for (event, payload, expected) in [
            (
                ShellHookEvent::SessionStart,
                HookPayload::SessionSetup {
                    initial_message: "hello".to_string(),
                    source: SessionStartSource::Resume,
                },
                ("source", json!("resume")),
            ),
            (
                ShellHookEvent::Stop,
                HookPayload::Finalize {
                    stop_hook_active: true,
                },
                ("stop_hook_active", json!(true)),
            ),
            (
                ShellHookEvent::SessionEnd,
                HookPayload::SessionEnd {
                    status: SessionEndStatus::Cancelled,
                    completion_reason: Some("cancelled by user".to_string()),
                },
                ("terminal_status", json!("cancelled")),
            ),
        ] {
            let hook = shell_hook(event, "true", 1_000, None, 0);
            let value = serde_json::to_value(hook.envelope(
                &payload,
                &session,
                hook.effective_cwd(&session).as_ref(),
            ))
            .unwrap();
            assert_eq!(value["hook_event_name"], event.as_str());
            assert_eq!(value[expected.0], expected.1);
            if matches!(event, ShellHookEvent::SessionEnd) {
                assert_eq!(value["completion_reason"], "cancelled by user");
            }
        }
    }

    #[test]
    fn configured_runner_registers_all_enabled_events() {
        let hook = command("true", 1_000);
        let config = LifecycleHooksConfig {
            enabled: true,
            pre_tool_use: vec![LifecycleHookGroup {
                enabled: true,
                matcher: Some("bash".to_string()),
                hooks: vec![hook.clone()],
            }],
            session_start: vec![LifecycleHookGroup {
                enabled: true,
                matcher: None,
                hooks: vec![hook.clone()],
            }],
            user_prompt_submit: vec![LifecycleHookGroup {
                enabled: true,
                matcher: None,
                hooks: vec![hook.clone()],
            }],
            session_end: vec![LifecycleHookGroup {
                enabled: true,
                matcher: None,
                hooks: vec![hook.clone()],
            }],
            notification: vec![LifecycleHookGroup {
                enabled: true,
                matcher: None,
                hooks: vec![hook],
            }],
            ..LifecycleHooksConfig::default()
        };

        let base = HookDispatcher::new();
        let configured = base.with_lifecycle_config(&config, None);
        assert!(
            base.is_empty(),
            "per-run registration must not mutate the base"
        );
        assert_eq!(configured.len(), 5);
        assert!(configured.has_hooks_for(AgentHookPoint::AfterSessionSetup));
        assert!(configured.has_hooks_for(AgentHookPoint::BeforeSessionSetup));
        assert!(configured.has_hooks_for(AgentHookPoint::AfterSessionEnd));
        assert!(configured.has_hooks_for(AgentHookPoint::BeforeToolExecution));
        assert!(configured.has_hooks_for(AgentHookPoint::AfterNotification));
    }

    #[test]
    fn disabled_group_is_preserved_but_not_registered() {
        let config = LifecycleHooksConfig {
            enabled: true,
            pre_tool_use: vec![LifecycleHookGroup {
                enabled: false,
                matcher: None,
                hooks: vec![command("exit 2", 1_000)],
            }],
            ..LifecycleHooksConfig::default()
        };

        let configured = HookDispatcher::new().with_lifecycle_config(&config, None);

        assert!(configured.is_empty());
    }

    #[tokio::test]
    async fn configured_commands_run_in_order_and_first_block_wins() {
        let dir = tempfile::tempdir().unwrap();
        let config = LifecycleHooksConfig {
            enabled: true,
            pre_tool_use: vec![LifecycleHookGroup {
                enabled: true,
                matcher: None,
                hooks: vec![
                    command("printf 'first' >&2; exit 2", 1_000),
                    command("printf 'second' >&2; exit 2", 1_000),
                ],
            }],
            ..LifecycleHooksConfig::default()
        };
        let runner = HookDispatcher::new().with_lifecycle_config(&config, None);
        let report = runner
            .run_hooks(
                AgentHookPoint::BeforeToolExecution,
                &HookPayload::ToolExecution {
                    tool_name: "bash".to_string(),
                    tool_call_id: "call-1".to_string(),
                    parsed_args: json!({}),
                },
                &session(dir.path()),
            )
            .await;
        assert_eq!(
            report.outcome.decision,
            HookResult::Deny {
                reason: "first".to_string()
            }
        );
        assert_eq!(report.executions.len(), 1);
    }

    #[tokio::test]
    async fn precompact_observer_ignores_block_and_collects_later_instructions() {
        let dir = tempfile::tempdir().unwrap();
        let config = LifecycleHooksConfig {
            enabled: true,
            pre_compact: vec![LifecycleHookGroup {
                enabled: true,
                matcher: None,
                hooks: vec![
                    command("printf 'cannot block compaction' >&2; exit 2", 1_000),
                    command(
                        r#"printf '%s' '{"additional_context":"preserve the failing assertion"}'"#,
                        1_000,
                    ),
                ],
            }],
            ..LifecycleHooksConfig::default()
        };
        let runner = HookDispatcher::new().with_lifecycle_config(&config, None);
        let report = runner
            .run_observer_hooks(
                AgentHookPoint::BeforeCompression,
                &HookPayload::Compression {
                    estimated_tokens: 1_700,
                    usage_percent: 85.0,
                    max_context_tokens: 2_000,
                    trigger_context_tokens: 1_600,
                    trigger: "threshold".to_string(),
                    phase: "pre-turn".to_string(),
                },
                &session(dir.path()),
            )
            .await;

        assert_eq!(report.executions.len(), 2);
        assert_eq!(
            report.outcome.injected_contexts,
            vec!["preserve the failing assertion".to_string()]
        );
    }

    #[test]
    fn invalid_matcher_is_skipped_without_failing_registration() {
        let config = LifecycleHooksConfig {
            enabled: true,
            pre_tool_use: vec![LifecycleHookGroup {
                enabled: true,
                matcher: Some("[".to_string()),
                hooks: vec![command("true", 1_000)],
            }],
            ..LifecycleHooksConfig::default()
        };
        let configured = HookDispatcher::new().with_lifecycle_config(&config, None);
        assert!(configured.is_empty());
    }

    #[tokio::test]
    async fn output_capture_is_capped_but_fully_drained() {
        let (mut writer, reader) = tokio::io::duplex(1024);
        let input = vec![b'x'; HOOK_OUTPUT_LIMIT_BYTES + 17];
        let write_task = tokio::spawn(async move {
            writer.write_all(&input).await.unwrap();
        });
        let captured = read_capped(reader).await.unwrap();
        write_task.await.unwrap();
        assert_eq!(captured.bytes.len(), HOOK_OUTPUT_LIMIT_BYTES);
        assert!(captured.truncated);
    }
}
