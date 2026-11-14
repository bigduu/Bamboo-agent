//! Foreground Bash with the model-facing contract from Pi v1.2.0.
//!
//! Adapted from Pi's bash.ts, output-accumulator.ts, and truncate.ts.
//! https://github.com/earendil-works/pi/tree/v1.2.0/packages/coding-agent/src/core/tools

// MIT License
//
// Copyright (c) 2025 Mario Zechner
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::borrow::Cow;
use std::future::pending;
use std::io;
use std::process::{ExitStatus, Stdio};

use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_infrastructure::process::{
    build_command_environment, hide_window_for_tokio_command, preferred_bash_shell,
    trace_windows_command, PreparedCommandEnvironment, ShellCommand,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tempfile::TempPath;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{ChildStderr, ChildStdout, Command};
use tokio::time::{sleep_until, Duration, Instant};

use super::workspace_state;
use crate::local_task_resources::{InitialWait, ManagedBashChild};

const MAX_LINES: usize = 2_000;
const MAX_BYTES: usize = 50 * 1024;
const ROLLING_TAIL_BYTES: usize = 2 * MAX_BYTES;
const MAX_TIMEOUT_SECONDS: f64 = 2_147_483.647;
const PIPE_IDLE_GRACE: Duration = Duration::from_millis(100);
const PIPE_MAX_GRACE: Duration = Duration::from_secs(2);

pub const FOREGROUND_BASH_EXECUTION_CONTRACT: &str = "bamboo.foreground-bash.v1";

#[derive(Deserialize)]
struct BashArgs {
    command: String,
    timeout: Option<f64>,
}

pub struct ForegroundBashTool;

#[async_trait]
impl Tool for ForegroundBashTool {
    fn name(&self) -> &str {
        "bash"
    }

    fn execution_contract(&self) -> Option<&'static str> {
        Some(FOREGROUND_BASH_EXECUTION_CONTRACT)
    }

    fn description(&self) -> &str {
        "Execute a bash command in the current working directory. Returns stdout and stderr. Output is truncated to last 2000 lines or 50KB (whichever is hit first). If truncated, full output is saved to a temp file. Optionally provide a timeout in seconds."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": {"type": "string", "description": "Shell command to execute"},
                "timeout": {"type": "number", "description": "Timeout in seconds (optional, no default timeout)"}
            }
        })
    }

    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        let args: BashArgs = serde_json::from_value(args)
            .map_err(|error| ToolError::InvalidArguments(format!("Invalid bash args: {error}")))?;
        let timeout = args.timeout.map(resolve_timeout).transpose()?;
        let cwd = workspace_state::workspace_or_process_cwd(ctx.session_id());
        let environment = build_command_environment(&bamboo_llm::Config::current_env_vars()).await;
        let shell = foreground_shell(&environment)?;
        trace_windows_command(
            "agent.bash.foreground",
            &shell.program,
            [shell.arg, &args.command],
        );
        let mut command = Command::new(&shell.program);
        hide_window_for_tokio_command(&mut command);
        environment.apply_to_tokio_command(&mut command);
        command
            .current_dir(cwd)
            .arg(shell.arg)
            .arg(&args.command)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        // Keep the initial process owner armed through the last await. A
        // dropped invocation cancels the process group or Windows Job even
        // if the leader has exited and inherited output pipes are still open.
        let mut child = ManagedBashChild::spawn_polled(&mut command)
            .map_err(|error| ToolError::Execution(format!("Failed to execute command: {error}")))?;
        let deadline = timeout.map(|duration| Instant::now() + duration);
        let mut pipes = OutputPipes::take(&mut child)?;
        let mut output = OutputAccumulator::default();
        let prepared = {
            let wait = child.prepare_initial_wait();
            tokio::pin!(wait);
            let deadline = wait_for_deadline(deadline);
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut deadline => break None,
                    result = &mut wait => break Some(result),
                    bytes = pipes.read(), if pipes.open() => output.append(bytes?).await?,
                }
            }
        };
        let prepared = prepared.transpose().map_err(wait_error)?;
        let timed_out = prepared.is_none() || !pipes.drain(&mut output, deadline).await?;
        if timed_out {
            child.kill().await.map_err(|error| {
                ToolError::Execution(format!("Failed to stop timed-out command: {error}"))
            })?;
            pipes.drain(&mut output, None).await?;
            output.finish().await?;
            let text = append_status(
                output.into_text("")?,
                &format!(
                    "Command timed out after {} seconds",
                    args.timeout.expect("timeout elapsed")
                ),
            );
            return Ok(tool_result(false, text));
        }

        output.finish().await?;
        let status = match prepared.expect("completed command has a terminal observation") {
            InitialWait::NaturalPending => {
                let owner = child.initial_handoff().expect("initial process owner");
                child.mark_initial_capture_ready();
                owner.deliver().map_err(wait_error)?.ok_or_else(|| {
                    ToolError::Execution("Command terminated without an exit code".into())
                })?
            }
            InitialWait::NeedWait => child.wait().await.map_err(wait_error)?,
            InitialWait::Reaped(result) => result.map_err(wait_error)?,
        };
        let text = output.into_text("(no output)")?;
        let result = match exit_code(status) {
            Some(0) => tool_result(true, text),
            Some(code) => tool_result(
                false,
                append_status(text, &format!("Command exited with code {code}")),
            ),
            None => tool_result(
                false,
                append_status(text, "Command terminated without an exit code"),
            ),
        };
        Ok(result)
    }
}

fn foreground_shell(environment: &PreparedCommandEnvironment) -> Result<ShellCommand, ToolError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        use std::path::PathBuf;
        let bash = std::iter::once(PathBuf::from("/bin/bash"))
            .chain(
                environment
                    .env
                    .get("PATH")
                    .into_iter()
                    .flat_map(std::env::split_paths)
                    .map(|dir| dir.join("bash")),
            )
            .find(|path| {
                path.metadata().is_ok_and(|metadata| {
                    metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
                })
            });
        if let Some(bash) = bash {
            return Ok(ShellCommand {
                program: bash.to_string_lossy().into_owned(),
                arg: "-c",
            });
        }
        Ok(preferred_bash_shell())
    }
    #[cfg(windows)]
    {
        use std::path::Path;
        let preferred = preferred_bash_shell();
        let candidate = (preferred.arg == "-lc")
            .then_some(preferred.program.into())
            .into_iter();
        let path = environment
            .env
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("PATH"));
        let bash = candidate
            .chain(
                path.into_iter()
                    .flat_map(|(_, value)| std::env::split_paths(value))
                    .map(|dir| dir.join("bash.exe")),
            )
            .find(|path| path.is_file() && !is_wsl_bash(path));
        let bash = bash.ok_or_else(|| {
            ToolError::Execution("No native Bash shell found. Install Git for Windows or add a native bash.exe to PATH.".into())
        })?;
        Ok(ShellCommand {
            program: bash.to_string_lossy().into_owned(),
            arg: "-c",
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = environment;
        Ok(preferred_bash_shell())
    }
}

#[cfg(windows)]
fn is_wsl_bash(path: &std::path::Path) -> bool {
    // The WSL launcher requires a different command transport and cannot
    // retain the resulting Linux process inside the native Windows Job.
    path.parent()
        .and_then(std::path::Path::file_name)
        .is_some_and(|name| {
            let name = name.to_string_lossy();
            name.eq_ignore_ascii_case("system32") || name.eq_ignore_ascii_case("sysnative")
        })
}

fn resolve_timeout(seconds: f64) -> Result<Duration, ToolError> {
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(ToolError::InvalidArguments(
            "Invalid timeout: must be a finite number of seconds".into(),
        ));
    }
    if seconds > MAX_TIMEOUT_SECONDS {
        return Err(ToolError::InvalidArguments(format!(
            "Invalid timeout: maximum is {MAX_TIMEOUT_SECONDS} seconds"
        )));
    }
    Ok(Duration::from_secs_f64(seconds).max(Duration::from_millis(1)))
}

fn wait_error(error: io::Error) -> ToolError {
    ToolError::Execution(format!("Failed waiting for command: {error}"))
}

fn exit_code(status: ExitStatus) -> Option<i32> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        status
            .code()
            .or_else(|| status.signal().map(|signal| 128 + signal))
    }
    #[cfg(not(unix))]
    {
        status.code()
    }
}

fn append_status(mut text: String, status: &str) -> String {
    if !text.is_empty() {
        text.push_str("\n\n");
    }
    text.push_str(status);
    text
}

fn tool_result(success: bool, result: String) -> ToolOutcome {
    ToolOutcome::Completed(ToolResult {
        success,
        result,
        display_preference: Some("Collapsible".into()),
        images: Vec::new(),
    })
}

async fn wait_for_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => pending().await,
    }
}

struct OutputPipes {
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
    stdout_buffer: [u8; 8192],
    stderr_buffer: [u8; 8192],
}

impl OutputPipes {
    fn take(child: &mut ManagedBashChild) -> Result<Self, ToolError> {
        let stdout = child
            .take_stdout()
            .ok_or_else(|| ToolError::Execution("Failed to capture stdout".into()))?;
        let stderr = child
            .take_stderr()
            .ok_or_else(|| ToolError::Execution("Failed to capture stderr".into()))?;
        Ok(Self {
            stdout: Some(stdout),
            stderr: Some(stderr),
            stdout_buffer: [0; 8192],
            stderr_buffer: [0; 8192],
        })
    }

    fn open(&self) -> bool {
        self.stdout.is_some() || self.stderr.is_some()
    }

    async fn read(&mut self) -> Result<&[u8], ToolError> {
        tokio::select! {
            result = Self::read_pipe(&mut self.stdout, &mut self.stdout_buffer) => result,
            result = Self::read_pipe(&mut self.stderr, &mut self.stderr_buffer) => result,
        }
    }

    async fn read_pipe<'a>(
        pipe: &mut Option<impl tokio::io::AsyncRead + Unpin>,
        buffer: &'a mut [u8],
    ) -> Result<&'a [u8], ToolError> {
        let Some(reader) = pipe else {
            return pending().await;
        };
        let count = reader.read(buffer).await.map_err(|error| {
            ToolError::Execution(format!("Failed reading command output: {error}"))
        })?;
        if count == 0 {
            *pipe = None;
        }
        Ok(&buffer[..count])
    }

    /// Drain while output is arriving, within a bound even if a descendant
    /// holds a pipe open. False means the user-provided command timeout won.
    async fn drain(
        &mut self,
        output: &mut OutputAccumulator,
        deadline: Option<Instant>,
    ) -> Result<bool, ToolError> {
        let limit = Instant::now() + PIPE_MAX_GRACE;
        let deadline = wait_for_deadline(deadline);
        tokio::pin!(deadline);
        while self.open() {
            let idle = limit.min(Instant::now() + PIPE_IDLE_GRACE);
            tokio::select! {
                biased;
                _ = &mut deadline => return Ok(false),
                _ = sleep_until(idle) => break,
                bytes = self.read() => output.append(bytes?).await?,
            }
        }
        self.stdout = None;
        self.stderr = None;
        Ok(true)
    }
}

struct Spill {
    file: File,
    path: TempPath,
}

#[derive(Default)]
struct OutputAccumulator {
    raw: Vec<u8>,
    incomplete_utf8: Vec<u8>,
    has_decoded: bool,
    tail: String,
    tail_starts_midline: bool,
    decoded_bytes: usize,
    completed_lines: usize,
    last_line_bytes: usize,
    spill: Option<Spill>,
}

impl OutputAccumulator {
    async fn append(&mut self, bytes: &[u8]) -> Result<(), ToolError> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.decode(bytes);
        if self.spill.is_some() || self.raw.len() + bytes.len() > MAX_BYTES || self.truncated() {
            self.start_spill().await?;
            self.spill
                .as_mut()
                .expect("spill is ready")
                .file
                .write_all(bytes)
                .await
                .map_err(spill_error)?;
        } else {
            self.raw.extend_from_slice(bytes);
        }
        Ok(())
    }

    fn decode(&mut self, bytes: &[u8]) {
        let bytes = if self.incomplete_utf8.is_empty() {
            Cow::Borrowed(bytes)
        } else {
            let mut combined = std::mem::take(&mut self.incomplete_utf8);
            combined.extend_from_slice(bytes);
            Cow::Owned(combined)
        };
        let mut consumed = 0;
        for chunk in bytes.utf8_chunks() {
            self.append_text(chunk.valid());
            let invalid = chunk.invalid();
            consumed += chunk.valid().len() + invalid.len();
            if invalid.is_empty() {
                continue;
            }
            if consumed == bytes.len()
                && std::str::from_utf8(invalid).is_err_and(|error| error.error_len().is_none())
            {
                self.incomplete_utf8.extend_from_slice(invalid);
            } else {
                self.append_text("\u{fffd}");
            }
        }
    }

    fn append_text(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        let text = if self.has_decoded {
            text
        } else {
            self.has_decoded = true;
            text.strip_prefix('\u{feff}').unwrap_or(text)
        };
        self.decoded_bytes = self.decoded_bytes.saturating_add(text.len());
        self.completed_lines = self
            .completed_lines
            .saturating_add(text.bytes().filter(|byte| *byte == b'\n').count());
        self.last_line_bytes = match text.rsplit_once('\n') {
            Some((_, line)) => line.len(),
            None => self.last_line_bytes.saturating_add(text.len()),
        };
        self.tail.push_str(text);
        if self.tail.len() > 2 * ROLLING_TAIL_BYTES {
            let start = self
                .tail
                .ceil_char_boundary(self.tail.len() - ROLLING_TAIL_BYTES);
            self.tail_starts_midline = self.tail.as_bytes()[start - 1] != b'\n';
            self.tail.drain(..start);
        }
    }

    fn total_lines(&self) -> usize {
        self.completed_lines
            .saturating_add(usize::from(self.last_line_bytes > 0))
    }

    fn truncated(&self) -> bool {
        self.decoded_bytes > MAX_BYTES || self.total_lines() > MAX_LINES
    }

    async fn start_spill(&mut self) -> Result<(), ToolError> {
        if self.spill.is_none() {
            let (file, path) = tempfile::Builder::new()
                .prefix("bamboo-bash-")
                .suffix(".log")
                .tempfile()
                .map_err(spill_error)?
                .into_parts();
            let mut file = File::from_std(file);
            file.write_all(&std::mem::take(&mut self.raw))
                .await
                .map_err(spill_error)?;
            self.spill = Some(Spill { file, path });
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), ToolError> {
        if !self.incomplete_utf8.is_empty() {
            self.incomplete_utf8.clear();
            self.append_text("\u{fffd}");
        }
        if self.truncated() {
            self.start_spill().await?;
        }
        if let Some(spill) = &mut self.spill {
            spill.file.flush().await.map_err(spill_error)?;
        }
        Ok(())
    }

    fn into_text(self, empty: &str) -> Result<String, ToolError> {
        if !self.truncated() {
            return Ok(if self.tail.is_empty() {
                empty.to_owned()
            } else {
                self.tail
            });
        }
        let total = self.total_lines();
        let snapshot = if self.tail_starts_midline {
            self.tail
                .split_once('\n')
                .map_or(self.tail.as_str(), |(_, rest)| rest)
        } else {
            self.tail.as_str()
        };
        let (mut visible, lines, limit) = truncate_tail(snapshot, self.decoded_bytes);
        let start = total.saturating_sub(lines).saturating_add(1).min(total);
        let displayed = match limit {
            TailLimit::PartialLastLine => format!(
                "last {} of line {total} (line is {})",
                format_size(visible.len()),
                format_size(self.last_line_bytes),
            ),
            TailLimit::Bytes => format!(
                "lines {start}-{total} of {total} ({} limit)",
                format_size(MAX_BYTES)
            ),
            TailLimit::Lines => format!("lines {start}-{total} of {total}"),
        };
        let spill = self.spill.expect("truncated output was spilled");
        let path = spill
            .path
            .keep()
            .map_err(|error| spill_error(error.error))?;
        if visible.is_empty() {
            visible.push_str(empty);
        }
        visible.push_str(&format!(
            "\n\n[Showing {displayed}. Full output: {}]",
            path.display()
        ));
        Ok(visible)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum TailLimit {
    Lines,
    Bytes,
    PartialLastLine,
}

fn truncate_tail(snapshot: &str, decoded_bytes: usize) -> (String, usize, TailLimit) {
    let lines: Vec<_> = snapshot.split_terminator('\n').collect();
    if snapshot.len() <= MAX_BYTES && lines.len() <= MAX_LINES {
        let limit = if lines.len() < MAX_LINES && decoded_bytes > MAX_BYTES {
            TailLimit::Bytes
        } else {
            TailLimit::Lines
        };
        return (snapshot.to_owned(), lines.len(), limit);
    }
    let mut first = lines.len();
    let mut bytes = 0;
    let mut limit = TailLimit::Lines;
    for line in lines.iter().rev().take(MAX_LINES) {
        let needed = line.len() + usize::from(first < lines.len());
        if bytes + needed > MAX_BYTES {
            if first == lines.len() {
                let start = line.ceil_char_boundary(line.len() - MAX_BYTES);
                return (line[start..].to_owned(), 1, TailLimit::PartialLastLine);
            }
            limit = TailLimit::Bytes;
            break;
        }
        bytes += needed;
        first -= 1;
    }
    (lines[first..].join("\n"), lines.len() - first, limit)
}

fn format_size(bytes: usize) -> String {
    match bytes {
        0..=1023 => format!("{bytes}B"),
        1024..=1_048_575 => format!("{:.1}KB", bytes as f64 / 1024.0),
        _ => format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0)),
    }
}

fn spill_error(error: io::Error) -> ToolError {
    ToolError::Execution(format!("Failed to save command output: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_timeout_accepts_pi_fractional_seconds_and_bounds() {
        assert_eq!(resolve_timeout(1.25).unwrap(), Duration::from_millis(1250));
        assert_eq!(resolve_timeout(0.0001).unwrap(), Duration::from_millis(1));
        assert!(resolve_timeout(MAX_TIMEOUT_SECONDS).is_ok());
        for invalid in [
            0.0,
            -1.0,
            f64::NAN,
            f64::INFINITY,
            MAX_TIMEOUT_SECONDS + 0.001,
        ] {
            assert!(resolve_timeout(invalid).is_err());
        }
    }

    fn extract_output_path(text: &str) -> &std::path::Path {
        let path = text.rsplit_once(". Full output: ").unwrap().1;
        std::path::Path::new(path.strip_suffix(']').unwrap())
    }

    #[tokio::test]
    async fn line_truncation_preserves_tail_and_complete_private_output() {
        let complete = (1..=2002)
            .map(|i| format!("line {i}\n"))
            .collect::<String>();
        assert!(complete.len() < MAX_BYTES);
        let mut output = OutputAccumulator::default();
        for chunk in complete.as_bytes().chunks(467) {
            output.append(chunk).await.unwrap();
        }
        output.finish().await.unwrap();
        let result = output.into_text("(no output)").unwrap();
        let path = extract_output_path(&result);
        let saved = std::fs::read(path).unwrap();
        #[cfg(unix)]
        let permissions = {
            use std::os::unix::fs::PermissionsExt;
            std::fs::metadata(path).unwrap().permissions().mode()
        };
        std::fs::remove_file(path).unwrap();
        assert_eq!(saved, complete.as_bytes());
        #[cfg(unix)]
        assert_eq!(permissions & 0o077, 0);
        assert!(result.starts_with("line 3\n"));
        assert!(result.contains("line 2002\n\n[Showing lines 3-2002 of 2002. Full output: "));
        assert_eq!(
            result
                .split_once("\n\n[Showing ")
                .unwrap()
                .0
                .lines()
                .count(),
            MAX_LINES
        );
    }

    #[tokio::test]
    async fn partial_last_line_stays_within_byte_limit_at_utf8_boundary() {
        let mut output = OutputAccumulator::default();
        let complete = format!("first\n{}", "汉".repeat(MAX_BYTES));
        for chunk in complete.as_bytes().chunks(8192) {
            output.append(chunk).await.unwrap();
        }
        output.finish().await.unwrap();
        let result = output.into_text("(no output)").unwrap();
        let path = extract_output_path(&result);
        let saved = std::fs::read(path).unwrap();
        std::fs::remove_file(path).unwrap();
        let (visible, notice) = result.split_once("\n\n[Showing ").unwrap();
        assert!(notice.starts_with("last 50.0KB of line 2 (line is 150.0KB)."));
        assert!(visible.len() <= MAX_BYTES);
        assert!(visible.chars().all(|c| c == '汉'));
        assert_eq!(saved, complete.as_bytes());
    }

    #[tokio::test]
    async fn cancelled_accumulator_removes_unpublished_spill() {
        let mut output = OutputAccumulator::default();
        output.append(&vec![b'x'; MAX_BYTES + 1]).await.unwrap();
        let path = output.spill.as_ref().unwrap().path.to_path_buf();
        assert!(path.exists());
        drop(output);
        assert!(!path.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn plain_result_uses_session_directory_and_null_stdin() {
        async fn run(args: Value, ctx: ToolCtx) -> ToolResult {
            let ToolOutcome::Completed(result) =
                ForegroundBashTool.invoke(args, ctx).await.unwrap()
            else {
                panic!("bash must return one foreground result");
            };
            result
        }
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("fixture"), "here").unwrap();
        let session_id: std::sync::Arc<str> = format!("bash-{}", uuid::Uuid::new_v4()).into();
        workspace_state::publish_resolved_workspace(&session_id, directory.path().to_path_buf());
        let mut ctx = ToolCtx::none("bash-cwd");
        ctx.session_id = Some(session_id);
        let result = run(
            json!({"command": "if read -r input; then exit 99; fi; printf 'stdout '; cat fixture; printf ' stderr' >&2; exit 7"}),
            ctx.clone(),
        ).await;
        assert!(!result.success);
        assert!(
            result.result.contains("stdout here")
                || (result.result.contains("stdout ") && result.result.contains("here"))
        );
        assert!(result.result.contains(" stderr"));
        assert!(result.result.ends_with("\n\nCommand exited with code 7"));
        let result = run(
            json!({"command": "values=(bash command); [[ ${values[1]} == command ]]"}),
            ctx,
        )
        .await;
        assert!(result.success);
        assert_eq!(result.result, "(no output)");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_reaps_owned_command_and_retains_partial_output() {
        let started = Instant::now();
        let result = ForegroundBashTool
            .invoke(
                json!({"command": "printf partial; sleep 20 & wait", "timeout": 0.35}),
                ToolCtx::none("bash-timeout"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = result else {
            panic!("bash must return one foreground result");
        };
        assert!(!result.success);
        assert_eq!(
            result.result,
            "partial\n\nCommand timed out after 0.35 seconds"
        );
        assert!(started.elapsed() < Duration::from_secs(10));
    }
}
