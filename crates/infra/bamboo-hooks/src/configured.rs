use std::ffi::OsString;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bamboo_agent_core::{AgentHook, Session};
use bamboo_config::{
    lifecycle_script_extension, LifecycleHookGroup, LifecycleHookHandler, LifecycleHooksConfig,
    LifecycleScriptRunner,
};
use bamboo_domain::{
    AgentHookPoint, HookPayload, HookResult, SessionEndStatus, SessionStartSource,
};
use bamboo_infrastructure::{build_command_environment, preferred_bash_shell};
use chrono::Utc;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
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
            .stderr(Stdio::piped());
        configure_hook_process(&mut command);

        let child = command
            .spawn()
            .map_err(|error| format!("failed to spawn lifecycle hook: {error}"))?;
        let child = HookChild::new(child)?;
        capture_hook_child(child, input, self.timeout, &self.name).await
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
            Some(0) => {
                if output.stdout.truncated {
                    return HookResult::Continue;
                }
                self.interpret_success(&output.stdout.bytes)
            }
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
        LifecycleHookHandler::Script {
            path,
            runner,
            timeout_ms,
        } => {
            let mut hook =
                ScriptHook::new(event, path, *runner, *timeout_ms, None, fallback_cwd, 0)
                    .map_err(|error| format!("invalid lifecycle hook matcher: {error}"))?;
            hook.name = format!("lifecycle_script_test:{event_name}");
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

async fn capture_hook_child(
    mut child: HookChild,
    input: Vec<u8>,
    timeout: Duration,
    hook_name: &str,
) -> Result<CommandOutput, String> {
    let mut stdin = child
        .child
        .stdin
        .take()
        .ok_or_else(|| "failed to open lifecycle hook stdin".to_string())?;
    let stdout = child
        .child
        .stdout
        .take()
        .ok_or_else(|| "failed to capture lifecycle hook stdout".to_string())?;
    let stderr = child
        .child
        .stderr
        .take()
        .ok_or_else(|| "failed to capture lifecycle hook stderr".to_string())?;

    let (io_tx, mut io_rx) = mpsc::channel(3);
    let input_tx = io_tx.clone();
    tokio::spawn(async move {
        let result = stdin.write_all(&input).await;
        let _ = stdin.shutdown().await;
        let _ = input_tx.send(HookIoEvent::Input(result)).await;
    });
    let stdout_tx = io_tx.clone();
    tokio::spawn(async move {
        let _ = stdout_tx
            .send(HookIoEvent::Stdout(read_capped(stdout).await))
            .await;
    });
    tokio::spawn(async move {
        let _ = io_tx
            .send(HookIoEvent::Stderr(read_capped(stderr).await))
            .await;
    });

    let mut io_results = HookIoResults::default();
    let completion = async {
        let (status, io_result) = tokio::join!(
            child.child.wait(),
            receive_hook_io(&mut io_rx, &mut io_results)
        );
        io_result?;
        status.map_err(|error| format!("failed waiting for lifecycle hook: {error}"))
    };
    let (status, timed_out) = match tokio::time::timeout(timeout, completion).await {
        Ok(Ok(status)) => (Some(status), false),
        Ok(Err(error)) => return Err(error),
        Err(_) => {
            kill_hook_process_tree(&mut child).await;
            receive_hook_io(&mut io_rx, &mut io_results).await?;
            (None, true)
        }
    };

    let (stdout, stderr) = io_results.finish(hook_name)?;
    child.finished = true;

    Ok(CommandOutput {
        exit_code: status.and_then(|status| status.code()),
        stdout,
        stderr,
        timed_out,
    })
}

fn configure_hook_process(command: &mut Command) {
    command.kill_on_drop(true);
    #[cfg(unix)]
    {
        // The saved process-group id remains usable after the direct child
        // exits, so descendants that keep captured pipes open can still be
        // terminated when the full wall-clock deadline expires.
        command.process_group(0);
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Threading::{CREATE_NO_WINDOW, CREATE_SUSPENDED};

        // Suspend before first instruction so the process can be assigned to a
        // Job Object before it has any opportunity to spawn descendants.
        command.creation_flags(CREATE_NO_WINDOW | CREATE_SUSPENDED);
    }
}

struct HookChild {
    child: Child,
    #[cfg(unix)]
    process_group_id: u32,
    finished: bool,
    #[cfg(windows)]
    job: WindowsJob,
}

impl HookChild {
    fn new(child: Child) -> Result<Self, String> {
        let process_id = child
            .id()
            .ok_or_else(|| "lifecycle hook exited before process isolation".to_string())?;
        #[cfg(windows)]
        let job = WindowsJob::assign_and_resume(&child, process_id)
            .map_err(|error| format!("failed isolating lifecycle hook process tree: {error}"))?;
        Ok(Self {
            child,
            #[cfg(unix)]
            process_group_id: process_id,
            finished: false,
            #[cfg(windows)]
            job,
        })
    }
}

impl Drop for HookChild {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        terminate_hook_process_tree(self);
        let _ = self.child.start_kill();
    }
}

async fn kill_hook_process_tree(child: &mut HookChild) {
    terminate_hook_process_tree(child);
    let _ = child.child.start_kill();
    let _ = child.child.wait().await;
}

fn terminate_hook_process_tree(child: &HookChild) {
    #[cfg(unix)]
    {
        // SAFETY: every hook is spawned as the leader of its own process group,
        // and the group id is captured before the direct child can exit.
        unsafe {
            libc::kill(-(child.process_group_id as libc::pid_t), libc::SIGKILL);
        }
    }
    #[cfg(windows)]
    {
        let _ = child.job.terminate();
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = child;
    }
}

#[cfg(windows)]
struct WindowsJob {
    handle: std::os::windows::io::OwnedHandle,
}

#[cfg(windows)]
impl WindowsJob {
    fn assign_and_resume(child: &Child, process_id: u32) -> std::io::Result<Self> {
        use std::os::windows::io::{FromRawHandle, RawHandle};
        use std::ptr;
        use windows_sys::Win32::System::JobObjects::{AssignProcessToJobObject, CreateJobObjectW};

        let raw_job = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
        if raw_job.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        let job = Self {
            handle: unsafe {
                std::os::windows::io::OwnedHandle::from_raw_handle(raw_job as RawHandle)
            },
        };
        let process_handle = child.raw_handle().ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::NotFound,
                "lifecycle hook exited before Job Object assignment",
            )
        })?;
        if unsafe { AssignProcessToJobObject(job.raw_handle(), process_handle.cast()) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        resume_suspended_process(process_id)?;
        Ok(job)
    }

    fn raw_handle(&self) -> windows_sys::Win32::Foundation::HANDLE {
        use std::os::windows::io::AsRawHandle;

        self.handle.as_raw_handle().cast()
    }

    fn terminate(&self) -> std::io::Result<()> {
        use windows_sys::Win32::System::JobObjects::TerminateJobObject;

        if unsafe { TerminateJobObject(self.raw_handle(), 1) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(windows)]
fn resume_suspended_process(process_id: u32) -> std::io::Result<()> {
    use std::mem::size_of;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
    };
    use windows_sys::Win32::System::Threading::{OpenThread, ResumeThread, THREAD_SUSPEND_RESUME};

    let raw_snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if raw_snapshot == INVALID_HANDLE_VALUE {
        return Err(std::io::Error::last_os_error());
    }
    let snapshot = unsafe { OwnedHandle::from_raw_handle(raw_snapshot as RawHandle) };
    let mut entry = THREADENTRY32 {
        dwSize: size_of::<THREADENTRY32>() as u32,
        ..unsafe { std::mem::zeroed() }
    };
    let mut has_entry = unsafe { Thread32First(snapshot.as_raw_handle().cast(), &mut entry) } != 0;
    while has_entry {
        if entry.th32OwnerProcessID == process_id {
            let raw_thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
            if raw_thread.is_null() {
                return Err(std::io::Error::last_os_error());
            }
            let thread = unsafe { OwnedHandle::from_raw_handle(raw_thread as RawHandle) };
            if unsafe { ResumeThread(thread.as_raw_handle().cast()) } == u32::MAX {
                return Err(std::io::Error::last_os_error());
            }
            return Ok(());
        }
        has_entry = unsafe { Thread32Next(snapshot.as_raw_handle().cast(), &mut entry) } != 0;
    }
    Err(std::io::Error::new(
        ErrorKind::NotFound,
        "suspended lifecycle hook primary thread was not found",
    ))
}

enum HookIoEvent {
    Input(std::io::Result<()>),
    Stdout(std::io::Result<CapturedOutput>),
    Stderr(std::io::Result<CapturedOutput>),
}

#[derive(Default)]
struct HookIoResults {
    input: Option<std::io::Result<()>>,
    stdout: Option<std::io::Result<CapturedOutput>>,
    stderr: Option<std::io::Result<CapturedOutput>>,
}

impl HookIoResults {
    fn is_complete(&self) -> bool {
        self.input.is_some() && self.stdout.is_some() && self.stderr.is_some()
    }

    fn record(&mut self, event: HookIoEvent) -> Result<(), String> {
        match event {
            HookIoEvent::Input(result) if self.input.is_none() => self.input = Some(result),
            HookIoEvent::Stdout(result) if self.stdout.is_none() => self.stdout = Some(result),
            HookIoEvent::Stderr(result) if self.stderr.is_none() => self.stderr = Some(result),
            HookIoEvent::Input(_) => {
                return Err("duplicate lifecycle hook stdin result".to_string())
            }
            HookIoEvent::Stdout(_) => {
                return Err("duplicate lifecycle hook stdout result".to_string())
            }
            HookIoEvent::Stderr(_) => {
                return Err("duplicate lifecycle hook stderr result".to_string())
            }
        }
        Ok(())
    }

    fn finish(self, hook_name: &str) -> Result<(CapturedOutput, CapturedOutput), String> {
        let Self {
            input,
            stdout,
            stderr,
        } = self;
        match input.ok_or_else(|| "lifecycle hook stdin task ended unexpectedly".to_string())? {
            Ok(()) => {}
            Err(error) => {
                warn!(hook = hook_name, error = %error, "failed writing lifecycle hook stdin");
            }
        }
        let stdout = stdout
            .ok_or_else(|| "lifecycle hook stdout task ended unexpectedly".to_string())?
            .map_err(|error| format!("failed reading lifecycle hook stdout: {error}"))?;
        let stderr = stderr
            .ok_or_else(|| "lifecycle hook stderr task ended unexpectedly".to_string())?
            .map_err(|error| format!("failed reading lifecycle hook stderr: {error}"))?;
        Ok((stdout, stderr))
    }
}

async fn receive_hook_io(
    receiver: &mut mpsc::Receiver<HookIoEvent>,
    results: &mut HookIoResults,
) -> Result<(), String> {
    while !results.is_complete() {
        let event = receiver
            .recv()
            .await
            .ok_or_else(|| "lifecycle hook I/O task ended unexpectedly".to_string())?;
        results.record(event)?;
    }
    Ok(())
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

/// One config-driven lifecycle script executed by an installed system runtime.
///
/// Scripts receive Bamboo's versioned hook envelope on stdin and emit the same
/// JSON response protocol as command hooks on stdout.
pub struct ScriptHook {
    event: LifecycleHookEvent,
    path: String,
    runner: LifecycleScriptRunner,
    timeout: Duration,
    matcher: Option<Regex>,
    fallback_cwd: Option<PathBuf>,
    name: String,
}

impl ScriptHook {
    pub fn new(
        event: LifecycleHookEvent,
        path: impl Into<String>,
        runner: LifecycleScriptRunner,
        timeout_ms: u64,
        matcher: Option<&str>,
        fallback_cwd: Option<PathBuf>,
        sequence: usize,
    ) -> Result<Self, regex::Error> {
        Ok(Self {
            event,
            path: path.into(),
            runner,
            timeout: Duration::from_millis(timeout_ms.max(1)),
            matcher: matcher.map(Regex::new).transpose()?,
            fallback_cwd,
            name: format!("lifecycle_script:{}:{sequence}", event.as_str()),
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

    async fn resolve_path(&self, cwd: Option<&PathBuf>) -> Result<PathBuf, String> {
        let configured = PathBuf::from(self.path.trim());
        let resolved = if configured.is_absolute() {
            configured
        } else {
            let cwd = cwd.ok_or_else(|| {
                format!(
                    "cannot resolve relative lifecycle script path '{}' without a working directory",
                    self.path
                )
            })?;
            cwd.join(configured)
        };
        let metadata = tokio::fs::metadata(&resolved).await.map_err(|error| {
            format!(
                "lifecycle script '{}' is not accessible: {error}",
                resolved.display()
            )
        })?;
        if !metadata.is_file() {
            return Err(format!(
                "lifecycle script '{}' is not a file",
                resolved.display()
            ));
        }
        Ok(resolved)
    }

    async fn execute(
        &self,
        input: Vec<u8>,
        cwd: Option<&PathBuf>,
        session: &Session,
    ) -> Result<CommandOutput, String> {
        let path = self.resolve_path(cwd).await?;
        let configured_path = self.path.trim();
        let invocations = script_invocations(configured_path, &path, self.runner)?;
        let overrides = bamboo_llm::Config::current_env_vars();
        let prepared_env = build_command_environment(&overrides).await;
        let mut missing_runtimes = Vec::new();

        for invocation in invocations {
            let mut command = Command::new(&invocation.program);
            prepared_env.apply_to_tokio_command(&mut command);
            if let Some(cwd) = cwd {
                command.current_dir(cwd);
            }
            command
                .args(&invocation.args)
                .env("BAMBOO_SESSION_ID", &session.id)
                .env("BAMBOO_HOOK_EVENT", self.event.as_str())
                .env("BAMBOO_HOOK_SCRIPT", &path)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            configure_hook_process(&mut command);

            match command.spawn() {
                Ok(child) => {
                    let child = HookChild::new(child)?;
                    return capture_hook_child(child, input, self.timeout, &self.name).await;
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    missing_runtimes.push(invocation.program.to_string_lossy().into_owned());
                }
                Err(error) => {
                    return Err(format!(
                        "failed to spawn lifecycle script runtime '{}': {error}",
                        invocation.program.to_string_lossy()
                    ));
                }
            }
        }

        Err(format!(
            "no lifecycle script runtime was found for '{}'; tried {}",
            self.path,
            missing_runtimes.join(", ")
        ))
    }

    fn interpret(&self, output: CommandOutput) -> HookResult {
        if output.stdout.truncated || output.stderr.truncated {
            warn!(
                hook = %self.name,
                stdout_truncated = output.stdout.truncated,
                stderr_truncated = output.stderr.truncated,
                "lifecycle script output exceeded capture limit"
            );
        }
        if output.timed_out {
            warn!(hook = %self.name, "lifecycle script timed out and was killed");
            return HookResult::Continue;
        }

        match output.exit_code {
            Some(0) => {
                if output.stdout.truncated {
                    return HookResult::Continue;
                }
                let stdout = String::from_utf8_lossy(&output.stdout.bytes);
                let stdout = stdout.trim();
                if stdout.is_empty() {
                    return HookResult::Continue;
                }
                match serde_json::from_str::<HookResponse>(stdout) {
                    Ok(response) => interpret_response(response),
                    Err(error) => {
                        warn!(
                            hook = %self.name,
                            error = %error,
                            "ignoring malformed lifecycle script response"
                        );
                        HookResult::Continue
                    }
                }
            }
            Some(2) => {
                let reason = String::from_utf8_lossy(&output.stderr.bytes)
                    .trim()
                    .to_string();
                HookResult::Deny {
                    reason: if reason.is_empty() {
                        "lifecycle script exited with blocking status 2".to_string()
                    } else {
                        reason
                    },
                }
            }
            exit_code => {
                warn!(hook = %self.name, ?exit_code, "lifecycle script failed non-blocking");
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
        let input = serde_json::to_vec(&self.envelope(payload, session, cwd.as_ref()))
            .map_err(|error| format!("failed serializing lifecycle hook test payload: {error}"))?;
        match self.execute(input, cwd.as_ref(), session).await {
            Ok(output) => Ok(LifecycleHookTestOutput {
                exit_code: output.exit_code,
                stdout: String::from_utf8_lossy(&output.stdout.bytes).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr.bytes).into_owned(),
                timed_out: output.timed_out,
                stdout_truncated: output.stdout.truncated,
                stderr_truncated: output.stderr.truncated,
            }),
            Err(error) => {
                let (stderr, stderr_truncated) = cap_text(error);
                Ok(LifecycleHookTestOutput {
                    exit_code: None,
                    stdout: String::new(),
                    stderr,
                    timed_out: false,
                    stdout_truncated: false,
                    stderr_truncated,
                })
            }
        }
    }
}

#[async_trait]
impl AgentHook for ScriptHook {
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
                warn!(
                    hook = %self.name,
                    error = %error,
                    "failed serializing lifecycle script payload"
                );
                return HookResult::Continue;
            }
        };
        match self.execute(input, cwd.as_ref(), session).await {
            Ok(output) => self.interpret(output),
            Err(error) => {
                warn!(
                    hook = %self.name,
                    error = %error,
                    "lifecycle script execution failed non-blocking"
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScriptInvocation {
    program: OsString,
    args: Vec<OsString>,
}

impl ScriptInvocation {
    fn with_path(program: impl Into<OsString>, leading_args: &[&str], path: &Path) -> Self {
        let mut args = leading_args.iter().map(OsString::from).collect::<Vec<_>>();
        args.push(path.as_os_str().to_owned());
        Self {
            program: program.into(),
            args,
        }
    }
}

fn script_invocations(
    configured_path: &str,
    resolved_path: &Path,
    runner: LifecycleScriptRunner,
) -> Result<Vec<ScriptInvocation>, String> {
    let extension = lifecycle_script_extension(configured_path).ok_or_else(|| {
        format!(
            "unsupported lifecycle script extension for '{configured_path}'; expected .js, .mjs, .cjs, .py, .sh, .ps1, .bat, or .cmd"
        )
    })?;
    if !runner.supports_path(configured_path) {
        return Err(format!(
            "lifecycle script runner '{}' cannot execute '.{extension}' files",
            runner.as_str()
        ));
    }

    let selected = match runner {
        LifecycleScriptRunner::Auto => match extension.as_str() {
            "js" | "mjs" | "cjs" => vec![
                ScriptInvocation::with_path("node", &[], resolved_path),
                ScriptInvocation::with_path("bun", &["run"], resolved_path),
            ],
            "py" => python_invocations(resolved_path),
            "sh" => system_shell_invocations(resolved_path),
            "ps1" => powershell_invocations(resolved_path),
            "bat" | "cmd" => cmd_invocations(resolved_path)?,
            _ => unreachable!("supported extensions are matched exhaustively"),
        },
        LifecycleScriptRunner::Node => {
            vec![ScriptInvocation::with_path("node", &[], resolved_path)]
        }
        LifecycleScriptRunner::Bun => {
            vec![ScriptInvocation::with_path("bun", &["run"], resolved_path)]
        }
        LifecycleScriptRunner::Python => python_invocations(resolved_path),
        LifecycleScriptRunner::Bash => bash_invocations(resolved_path),
        LifecycleScriptRunner::PowerShell => powershell_invocations(resolved_path),
        LifecycleScriptRunner::Cmd => cmd_invocations(resolved_path)?,
    };
    Ok(selected)
}

fn python_invocations(path: &Path) -> Vec<ScriptInvocation> {
    let invocations = vec![
        ScriptInvocation::with_path("python3", &[], path),
        ScriptInvocation::with_path("python", &[], path),
    ];
    #[cfg(windows)]
    let invocations = {
        let mut invocations = invocations;
        invocations.push(ScriptInvocation::with_path("py", &["-3"], path));
        invocations
    };
    invocations
}

fn system_shell_invocations(path: &Path) -> Vec<ScriptInvocation> {
    let shell = preferred_bash_shell();
    #[cfg(windows)]
    let program = if shell.arg == "-lc" {
        shell.program
    } else {
        "bash".to_string()
    };
    #[cfg(not(windows))]
    let program = shell.program;
    vec![ScriptInvocation::with_path(program, &[], path)]
}

fn bash_invocations(path: &Path) -> Vec<ScriptInvocation> {
    #[cfg(windows)]
    {
        return system_shell_invocations(path);
    }
    #[cfg(not(windows))]
    {
        vec![ScriptInvocation::with_path("bash", &[], path)]
    }
}

fn powershell_invocations(path: &Path) -> Vec<ScriptInvocation> {
    vec![
        ScriptInvocation::with_path(
            "pwsh",
            &["-NoLogo", "-NoProfile", "-NonInteractive", "-File"],
            path,
        ),
        ScriptInvocation::with_path(
            "powershell",
            &["-NoLogo", "-NoProfile", "-NonInteractive", "-File"],
            path,
        ),
    ]
}

#[cfg(windows)]
fn cmd_invocations(path: &Path) -> Result<Vec<ScriptInvocation>, String> {
    Ok(vec![ScriptInvocation::with_path(
        "cmd.exe",
        &["/D", "/C", "call"],
        path,
    )])
}

#[cfg(not(windows))]
fn cmd_invocations(_path: &Path) -> Result<Vec<ScriptInvocation>, String> {
    Err("batch lifecycle scripts require Windows and cmd.exe".to_string())
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
                    LifecycleHookHandler::Script {
                        path,
                        runner,
                        timeout_ms,
                    } => ScriptHook::new(
                        event,
                        path,
                        *runner,
                        *timeout_ms,
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
    use bamboo_config::DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS;
    use serde_json::json;
    use std::time::Instant;

    fn command(command: impl Into<String>, timeout_ms: u64) -> LifecycleHookHandler {
        LifecycleHookHandler::command(command, timeout_ms)
    }

    fn script(
        path: impl Into<String>,
        runner: LifecycleScriptRunner,
        timeout_ms: u64,
    ) -> LifecycleHookHandler {
        LifecycleHookHandler::script(path, runner, timeout_ms)
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

    fn script_hook(
        event: LifecycleHookEvent,
        path: impl Into<String>,
        runner: LifecycleScriptRunner,
        timeout_ms: u64,
        matcher: Option<&str>,
    ) -> ScriptHook {
        ScriptHook::new(event, path, runner, timeout_ms, matcher, None, 0).unwrap()
    }

    fn write_script(dir: &tempfile::TempDir, name: &str, body: &str) -> String {
        std::fs::write(dir.path().join(name), body).unwrap();
        name.to_string()
    }

    fn session(workspace: &std::path::Path) -> Session {
        let mut session = Session::new("session-1", "test-model");
        session.workspace = Some(workspace.to_string_lossy().into_owned());
        session
    }

    #[cfg(unix)]
    fn process_is_alive(process_id: u32) -> bool {
        let result = unsafe { libc::kill(process_id as libc::pid_t, 0) };
        result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }

    #[cfg(windows)]
    fn process_is_alive(process_id: u32) -> bool {
        use windows_sys::Win32::Foundation::{CloseHandle, WAIT_TIMEOUT};
        use windows_sys::Win32::System::Threading::{
            OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE,
        };

        let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, process_id) };
        if handle.is_null() {
            return false;
        }
        let wait_result = unsafe { WaitForSingleObject(handle, 0) };
        unsafe {
            CloseHandle(handle);
        }
        wait_result == WAIT_TIMEOUT
    }

    #[cfg(not(any(unix, windows)))]
    fn process_is_alive(_process_id: u32) -> bool {
        false
    }

    async fn assert_process_exits(process_id: u32) {
        for _ in 0..100 {
            if !process_is_alive(process_id) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !process_is_alive(process_id),
            "hook descendant process {process_id} survived timeout cleanup"
        );
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

    #[test]
    fn truncated_command_response_is_not_interpreted() {
        let hook = shell_hook(ShellHookEvent::SessionStart, "true", 1_000, None, 0);
        let output = CommandOutput {
            exit_code: Some(0),
            stdout: CapturedOutput {
                bytes: br#"{"decision":"block","reason":"must not apply"}"#.to_vec(),
                truncated: true,
            },
            stderr: CapturedOutput::default(),
            timed_out: false,
        };

        assert_eq!(hook.interpret(output), HookResult::Continue);
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

    #[test]
    fn script_invocations_cover_supported_system_runtimes() {
        let js = script_invocations(
            "guard.js",
            Path::new("guard.js"),
            LifecycleScriptRunner::Auto,
        )
        .unwrap();
        assert_eq!(js[0].program, OsString::from("node"));
        assert_eq!(js[1].program, OsString::from("bun"));
        assert_eq!(js[1].args[0], OsString::from("run"));

        let bun = script_invocations(
            "guard.mjs",
            Path::new("guard.mjs"),
            LifecycleScriptRunner::Bun,
        )
        .unwrap();
        assert_eq!(bun[0].program, OsString::from("bun"));

        let python = script_invocations(
            "guard.py",
            Path::new("guard.py"),
            LifecycleScriptRunner::Auto,
        )
        .unwrap();
        assert_eq!(python[0].program, OsString::from("python3"));
        assert_eq!(python[1].program, OsString::from("python"));

        let shell = script_invocations(
            "guard.sh",
            Path::new("guard.sh"),
            LifecycleScriptRunner::Auto,
        )
        .unwrap();
        assert!(!shell[0].program.is_empty());

        let powershell = script_invocations(
            "guard.ps1",
            Path::new("guard.ps1"),
            LifecycleScriptRunner::Auto,
        )
        .unwrap();
        assert_eq!(powershell[0].program, OsString::from("pwsh"));
        assert_eq!(powershell[1].program, OsString::from("powershell"));

        assert!(script_invocations(
            "guard.py",
            Path::new("guard.py"),
            LifecycleScriptRunner::Node
        )
        .unwrap_err()
        .contains("cannot execute"));
    }

    #[cfg(unix)]
    #[test]
    fn explicit_bash_runner_does_not_fall_back_to_sh() {
        let bash = script_invocations(
            "guard.sh",
            Path::new("guard.sh"),
            LifecycleScriptRunner::Bash,
        )
        .unwrap();

        assert_eq!(bash[0].program, OsString::from("bash"));
    }

    #[cfg(windows)]
    #[test]
    fn batch_scripts_use_cmd_on_windows() {
        let batch = script_invocations(
            "guard.bat",
            Path::new("guard.bat"),
            LifecycleScriptRunner::Auto,
        )
        .unwrap();
        assert_eq!(batch[0].program, OsString::from("cmd.exe"));
        assert_eq!(batch[0].args[0], OsString::from("/D"));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn batch_script_executes_through_cmd() {
        let dir = tempfile::tempdir().unwrap();
        let script_dir = dir.path().join("hook scripts");
        std::fs::create_dir(&script_dir).unwrap();
        std::fs::write(
            script_dir.join("guard.bat"),
            "@echo off\r\necho {\"additional_context\":\"executed by bat\"}\r\n",
        )
        .unwrap();
        let hook = script_hook(
            LifecycleHookEvent::SessionStart,
            "hook scripts/guard.bat",
            LifecycleScriptRunner::Auto,
            2_000,
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
                text: "executed by bat".to_string(),
            }
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn batch_scripts_report_the_platform_requirement() {
        let error = script_invocations(
            "guard.bat",
            Path::new("guard.bat"),
            LifecycleScriptRunner::Auto,
        )
        .unwrap_err();
        assert!(error.contains("require Windows"), "{error}");
    }

    #[tokio::test]
    async fn javascript_script_uses_system_runtime_and_reads_stdin_envelope() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_script(
            &dir,
            "guard.js",
            r#"
let raw = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", chunk => raw += chunk);
process.stdin.on("end", async () => {
  await Promise.resolve();
  const input = JSON.parse(raw);
  process.stdout.write(JSON.stringify({
    decision: "allow",
    additional_context: `${input.hook_event_name}:${input.tool_name}:${input.tool_input.command}:${process.env.BAMBOO_HOOK_EVENT}`
  }));
});
"#,
        );
        let hook = script_hook(
            LifecycleHookEvent::PreToolUse,
            path,
            LifecycleScriptRunner::Auto,
            2_000,
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
                text: "PreToolUse:Bash:pwd:PreToolUse".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn bun_can_be_selected_explicitly_when_installed() {
        if Command::new("bun").arg("--version").output().await.is_err() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = write_script(
            &dir,
            "bun-hook.js",
            r#"
await Bun.stdin.text();
process.stdout.write(JSON.stringify({additional_context: "executed by bun"}));
"#,
        );
        let hook = script_hook(
            LifecycleHookEvent::SessionStart,
            path,
            LifecycleScriptRunner::Bun,
            2_000,
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
                text: "executed by bun".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn python_script_uses_system_python() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_script(
            &dir,
            "guard.py",
            r#"
import json
import sys

payload = json.load(sys.stdin)
print(json.dumps({"additional_context": payload["hook_event_name"] + ":python"}))
"#,
        );
        let hook = script_hook(
            LifecycleHookEvent::SessionStart,
            path,
            LifecycleScriptRunner::Auto,
            2_000,
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
                text: "SessionStart:python".to_string(),
            }
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shell_script_uses_system_shell() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_script(
            &dir,
            "guard.sh",
            r#"
payload=$(cat)
case "$payload" in
  *'"hook_event_name":"SessionStart"'*) printf '%s' '{"additional_context":"executed by sh"}' ;;
  *) printf '%s' 'unexpected payload' >&2; exit 1 ;;
esac
"#,
        );
        let hook = script_hook(
            LifecycleHookEvent::SessionStart,
            path,
            LifecycleScriptRunner::Auto,
            2_000,
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
                text: "executed by sh".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn powershell_script_executes_when_a_system_runtime_is_installed() {
        let pwsh_available = Command::new("pwsh").arg("--version").output().await.is_ok();
        let windows_powershell_available = Command::new("powershell")
            .arg("-NoLogo")
            .arg("-Command")
            .arg("$PSVersionTable.PSVersion")
            .output()
            .await
            .is_ok();
        if !pwsh_available && !windows_powershell_available {
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let path = write_script(
            &dir,
            "guard.ps1",
            r#"
$null = [Console]::In.ReadToEnd()
[Console]::Out.Write('{"additional_context":"executed by powershell"}')
"#,
        );
        let hook = script_hook(
            LifecycleHookEvent::SessionStart,
            path,
            LifecycleScriptRunner::Auto,
            3_000,
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
                text: "executed by powershell".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn script_errors_are_diagnostic_and_non_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_script(&dir, "error.js", "throw new Error('policy exploded');");
        let hook = script_hook(
            LifecycleHookEvent::SessionStart,
            path,
            LifecycleScriptRunner::Node,
            2_000,
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
    async fn script_timeout_kills_the_runtime_process() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_script(&dir, "loop.js", "while (true) {}");
        let hook = script_hook(
            LifecycleHookEvent::SessionStart,
            path,
            LifecycleScriptRunner::Node,
            25,
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
    async fn script_timeout_covers_descendant_pipe_drain_and_kills_tree() {
        if Command::new("node")
            .arg("--version")
            .output()
            .await
            .is_err()
        {
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let process_id_path = dir.path().join("descendant.pid");
        let process_id_literal = serde_json::to_string(&process_id_path.to_string_lossy()).unwrap();
        let body = r#"
const fs = require("node:fs");
const childProcess = require("node:child_process");
const child = childProcess.spawn(
  process.execPath,
  ["-e", "setTimeout(() => {}, 10000)"],
  {stdio: ["ignore", "inherit", "inherit"]}
);
fs.writeFileSync(__PROCESS_ID_PATH__, String(child.pid));
child.unref();
"#
        .replace("__PROCESS_ID_PATH__", &process_id_literal);
        let path = write_script(&dir, "descendant.js", &body);
        let hook = script_hook(
            LifecycleHookEvent::SessionStart,
            path,
            LifecycleScriptRunner::Node,
            1_000,
            None,
        );
        let started = Instant::now();
        let output = hook
            .test(&HookPayload::None, &session(dir.path()))
            .await
            .unwrap();

        assert!(output.timed_out, "{output:?}");
        assert!(started.elapsed() < Duration::from_secs(3));
        let process_id = std::fs::read_to_string(&process_id_path)
            .unwrap()
            .parse::<u32>()
            .unwrap();
        assert_process_exits(process_id).await;
    }

    #[tokio::test]
    async fn script_result_is_capped_before_interpretation() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_script(
            &dir,
            "large.js",
            r#"process.stdout.write(JSON.stringify({additional_context: "x".repeat(128 * 1024)}));"#,
        );
        let hook = script_hook(
            LifecycleHookEvent::SessionStart,
            path,
            LifecycleScriptRunner::Node,
            2_000,
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
    async fn missing_script_is_reported_by_dry_run_and_fails_open() {
        let dir = tempfile::tempdir().unwrap();
        let hook = script_hook(
            LifecycleHookEvent::SessionStart,
            "missing.js",
            LifecycleScriptRunner::Auto,
            1_000,
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
        assert!(output.stderr.contains("not accessible"), "{output:?}");
        assert_eq!(output.exit_code, None);
    }

    #[tokio::test]
    async fn configured_script_handler_matches_tools_and_returns_decision() {
        let dir = tempfile::tempdir().unwrap();
        write_script(
            &dir,
            "block.js",
            r#"
process.stdin.resume();
process.stdin.on("end", () => {
  process.stderr.write("blocked in JS");
  process.exit(2);
});
"#,
        );
        let config = LifecycleHooksConfig {
            enabled: true,
            pre_tool_use: vec![LifecycleHookGroup {
                enabled: true,
                matcher: Some("^Bash$".to_string()),
                hooks: vec![script("block.js", LifecycleScriptRunner::Node, 2_000)],
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
