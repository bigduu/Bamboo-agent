use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use bamboo_agent_core::{AgentHook, Session};
use bamboo_config::{
    LifecycleHookCommand, LifecycleHookGroup, LifecycleHookType, LifecycleHooksConfig,
};
use bamboo_domain::{AgentHookPoint, HookPayload, HookResult};
use bamboo_infrastructure::{
    build_command_environment, hide_window_for_tokio_command, preferred_bash_shell,
};
use chrono::Utc;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tracing::warn;

use super::HookRunner;

const HOOK_OUTPUT_LIMIT_BYTES: usize = 64 * 1024;

/// User-facing lifecycle events that currently map to engine hook seams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellHookEvent {
    SessionStart,
    PreToolUse,
    PostToolUse,
    Stop,
    PreCompact,
}

impl ShellHookEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SessionStart => "SessionStart",
            Self::PreToolUse => "PreToolUse",
            Self::PostToolUse => "PostToolUse",
            Self::Stop => "Stop",
            Self::PreCompact => "PreCompact",
        }
    }

    fn point(self) -> AgentHookPoint {
        match self {
            Self::SessionStart => AgentHookPoint::AfterSessionSetup,
            Self::PreToolUse => AgentHookPoint::BeforeToolExecution,
            Self::PostToolUse => AgentHookPoint::AfterToolExecution,
            Self::Stop => AgentHookPoint::BeforeFinalize,
            Self::PreCompact => AgentHookPoint::BeforeCompression,
        }
    }

    fn supports_tool_matcher(self) -> bool {
        matches!(self, Self::PreToolUse | Self::PostToolUse)
    }
}

/// One config-driven lifecycle shell command.
pub struct ShellCommandHook {
    event: ShellHookEvent,
    command: String,
    timeout: Duration,
    matcher: Option<Regex>,
    fallback_cwd: Option<PathBuf>,
    name: String,
}

impl ShellCommandHook {
    pub fn new(
        event: ShellHookEvent,
        config: &LifecycleHookCommand,
        matcher: Option<&str>,
        fallback_cwd: Option<PathBuf>,
        sequence: usize,
    ) -> Result<Self, regex::Error> {
        let matcher = matcher.map(Regex::new).transpose()?;
        Ok(Self {
            event,
            command: config.command.clone(),
            timeout: Duration::from_millis(config.timeout_ms.max(1)),
            matcher,
            fallback_cwd,
            name: format!("lifecycle_shell:{}:{sequence}", event.as_str()),
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
        let (tool_name, tool_input, tool_response, prompt) = match payload {
            HookPayload::SessionSetup { initial_message } => {
                (None, None, None, Some(initial_message.clone()))
            }
            HookPayload::Prompt { prompt } => (None, None, None, Some(prompt.clone())),
            HookPayload::ToolExecution {
                tool_name,
                parsed_args,
                ..
            } => (
                Some(tool_name.clone()),
                Some(parsed_args.clone()),
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
            ),
            HookPayload::None
            | HookPayload::Round { .. }
            | HookPayload::Compression { .. }
            | HookPayload::Finalize => (None, None, None, None),
        };

        HookEnvelope {
            schema_version: 1,
            hook_event_name: self.event.as_str(),
            session_id: session.id.clone(),
            workspace_path: cwd
                .map(|path| path.to_string_lossy().into_owned())
                .unwrap_or_default(),
            model: session.model.clone(),
            tool_name,
            tool_input,
            tool_response,
            prompt,
            timestamp: Utc::now().to_rfc3339(),
        }
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
            .env("BAMBOO_HOOK_EVENT", self.event.as_str())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

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
                let _ = child.kill().await;
                let _ = child.wait().await;
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
            Some(text) if matches!(result, HookResult::Continue) => {
                HookResult::InjectContext { text }
            }
            Some(text) => HookResult::WithContext {
                result: Box::new(result),
                text,
            },
            None => result,
        }
    }
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

#[derive(Debug, Serialize)]
struct HookEnvelope {
    schema_version: u8,
    hook_event_name: &'static str,
    session_id: String,
    workspace_path: String,
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_input: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_response: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt: Option<String>,
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

pub(super) fn register_configured_shell_hooks(
    runner: &mut HookRunner,
    config: &LifecycleHooksConfig,
    fallback_cwd: Option<PathBuf>,
) {
    if !config.enabled {
        return;
    }

    let events: [(ShellHookEvent, &[LifecycleHookGroup]); 5] = [
        (ShellHookEvent::SessionStart, &config.session_start),
        (ShellHookEvent::PreToolUse, &config.pre_tool_use),
        (ShellHookEvent::PostToolUse, &config.post_tool_use),
        (ShellHookEvent::Stop, &config.stop),
        (ShellHookEvent::PreCompact, &config.pre_compact),
    ];
    let mut sequence = 0_usize;
    for (event, groups) in events {
        for group in groups {
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
            for command in &group.hooks {
                match command.hook_type {
                    LifecycleHookType::Command => {
                        match ShellCommandHook::new(
                            event,
                            command,
                            matcher,
                            fallback_cwd.clone(),
                            sequence,
                        ) {
                            Ok(hook) => runner.register(std::sync::Arc::new(hook)),
                            Err(error) => warn!(
                                event = event.as_str(),
                                error = %error,
                                "skipping lifecycle hook with invalid matcher"
                            ),
                        }
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

    fn command(command: impl Into<String>, timeout_ms: u64) -> LifecycleHookCommand {
        LifecycleHookCommand {
            hook_type: LifecycleHookType::Command,
            command: command.into(),
            timeout_ms,
        }
    }

    fn session(workspace: &std::path::Path) -> Session {
        let mut session = Session::new("session-1", "test-model");
        session.workspace = Some(workspace.to_string_lossy().into_owned());
        session
    }

    #[tokio::test]
    async fn exit_zero_without_output_passes_through() {
        let dir = tempfile::tempdir().unwrap();
        let hook = ShellCommandHook::new(
            ShellHookEvent::SessionStart,
            &command("printf ''", DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS),
            None,
            None,
            0,
        )
        .unwrap();
        let result = hook
            .run(
                AgentHookPoint::AfterSessionSetup,
                &HookPayload::SessionSetup {
                    initial_message: "hello".to_string(),
                },
                &session(dir.path()),
            )
            .await;
        assert_eq!(result, HookResult::Continue);
    }

    #[tokio::test]
    async fn exit_two_blocks_with_stderr_reason() {
        let dir = tempfile::tempdir().unwrap();
        let hook = ShellCommandHook::new(
            ShellHookEvent::PreToolUse,
            &command("printf 'blocked by policy' >&2; exit 2", 1_000),
            None,
            None,
            0,
        )
        .unwrap();
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
        let hook = ShellCommandHook::new(
            ShellHookEvent::PreToolUse,
            &command(shell, 1_000),
            None,
            None,
            0,
        )
        .unwrap();
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
            let hook = ShellCommandHook::new(
                ShellHookEvent::SessionStart,
                &command(shell, 1_000),
                None,
                None,
                0,
            )
            .unwrap();
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
        let mut runner = HookRunner::new();
        runner.register(std::sync::Arc::new(
            ShellCommandHook::new(
                ShellHookEvent::SessionStart,
                &command(
                    r#"printf '%s' '{"decision":"allow","additional_context":"keep me"}'"#,
                    1_000,
                ),
                None,
                None,
                0,
            )
            .unwrap(),
        ));
        let session = session(dir.path());
        let mut state = bamboo_domain::AgentRuntimeState::new("run-1");
        let outcome = runner
            .run_hooks(
                AgentHookPoint::AfterSessionSetup,
                &HookPayload::None,
                &session,
                &mut state,
                None,
            )
            .await;
        assert_eq!(outcome.decision, HookResult::Allow);
        assert_eq!(outcome.injected_contexts, vec!["keep me"]);
    }

    #[tokio::test]
    async fn malformed_json_is_non_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let hook = ShellCommandHook::new(
            ShellHookEvent::SessionStart,
            &command("printf 'not-json'", 1_000),
            None,
            None,
            0,
        )
        .unwrap();
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
        let hook = ShellCommandHook::new(
            ShellHookEvent::SessionStart,
            &command("sleep 5", 25),
            None,
            None,
            0,
        )
        .unwrap();
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
    fn matcher_filters_tool_names() {
        let hook = ShellCommandHook::new(
            ShellHookEvent::PreToolUse,
            &command("exit 2", 1_000),
            Some("^(bash|write_file)$"),
            None,
            0,
        )
        .unwrap();
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
        let hook = ShellCommandHook::new(
            ShellHookEvent::PreToolUse,
            &command("true", 1_000),
            None,
            None,
            0,
        )
        .unwrap();
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
        assert!(value["timestamp"].as_str().is_some());
    }

    #[test]
    fn configured_runner_registers_only_enabled_engine_events() {
        let hook = command("true", 1_000);
        let config = LifecycleHooksConfig {
            enabled: true,
            pre_tool_use: vec![LifecycleHookGroup {
                matcher: Some("bash".to_string()),
                hooks: vec![hook.clone()],
            }],
            session_start: vec![LifecycleHookGroup {
                matcher: None,
                hooks: vec![hook.clone()],
            }],
            notification: vec![LifecycleHookGroup {
                matcher: None,
                hooks: vec![hook],
            }],
            ..LifecycleHooksConfig::default()
        };

        let base = HookRunner::new();
        let configured = base.with_lifecycle_config(&config, None);
        assert!(
            base.is_empty(),
            "per-run registration must not mutate the base"
        );
        assert_eq!(configured.len(), 2);
        assert!(configured.has_hooks_for(AgentHookPoint::AfterSessionSetup));
        assert!(configured.has_hooks_for(AgentHookPoint::BeforeToolExecution));
    }

    #[tokio::test]
    async fn configured_commands_run_in_order_and_first_block_wins() {
        let dir = tempfile::tempdir().unwrap();
        let config = LifecycleHooksConfig {
            enabled: true,
            pre_tool_use: vec![LifecycleHookGroup {
                matcher: None,
                hooks: vec![
                    command("printf 'first' >&2; exit 2", 1_000),
                    command("printf 'second' >&2; exit 2", 1_000),
                ],
            }],
            ..LifecycleHooksConfig::default()
        };
        let runner = HookRunner::new().with_lifecycle_config(&config, None);
        let mut state = bamboo_domain::AgentRuntimeState::new("run-ordered");
        let outcome = runner
            .run_hooks(
                AgentHookPoint::BeforeToolExecution,
                &HookPayload::ToolExecution {
                    tool_name: "bash".to_string(),
                    tool_call_id: "call-1".to_string(),
                    parsed_args: json!({}),
                },
                &session(dir.path()),
                &mut state,
                None,
            )
            .await;
        assert_eq!(
            outcome.decision,
            HookResult::Deny {
                reason: "first".to_string()
            }
        );
        assert_eq!(state.checkpoints.len(), 1);
    }

    #[test]
    fn invalid_matcher_is_skipped_without_failing_registration() {
        let config = LifecycleHooksConfig {
            enabled: true,
            pre_tool_use: vec![LifecycleHookGroup {
                matcher: Some("[".to_string()),
                hooks: vec![command("true", 1_000)],
            }],
            ..LifecycleHooksConfig::default()
        };
        let configured = HookRunner::new().with_lifecycle_config(&config, None);
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
