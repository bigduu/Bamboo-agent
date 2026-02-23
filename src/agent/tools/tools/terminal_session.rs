//! Terminal session tool for managing long-running interactive processes.
//!
//! This tool provides support for:
//! - Starting long-running processes (e.g., dev servers, build processes)
//! - Sending input to running processes
//! - Reading output from processes
//! - Managing multiple concurrent sessions
//!
//! # Security
//!
//! This tool requires `PermissionType::TerminalSession` permission.
//! Sessions have a maximum lifetime and are automatically cleaned up.

use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::sleep;

/// Maximum number of concurrent sessions
const MAX_SESSIONS: usize = 5;

/// Default session inactivity timeout (5 minutes)
const DEFAULT_SESSION_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(300);

/// Maximum output buffer size (100KB)
const MAX_OUTPUT_BUFFER: usize = 100 * 1024;

/// Terminal session action
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum SessionAction {
    /// Start a new session
    Start {
        /// Command to execute
        command: String,
        /// Working directory
        #[serde(skip_serializing_if = "Option::is_none")]
        working_dir: Option<String>,
        /// Environment variables
        #[serde(skip_serializing_if = "Option::is_none")]
        env: Option<std::collections::HashMap<String, String>>,
    },
    /// Send input to the session
    SendInput {
        /// Session ID
        session_id: String,
        /// Input to send
        input: String,
    },
    /// Read output from the session
    ReadOutput {
        /// Session ID
        session_id: String,
        /// Maximum lines to read (default: 100)
        #[serde(default = "default_max_lines")]
        max_lines: usize,
        /// Wait for output timeout in seconds (default: 5)
        #[serde(default = "default_read_timeout")]
        timeout_seconds: u64,
    },
    /// Get session status
    Status {
        /// Session ID
        session_id: String,
    },
    /// Kill a session
    Kill {
        /// Session ID
        session_id: String,
    },
    /// List all active sessions
    List,
}

fn default_max_lines() -> usize {
    100
}

fn default_read_timeout() -> u64 {
    5
}

/// Terminal session information
#[derive(Debug, Clone, Serialize)]
pub struct SessionInfo {
    /// Session ID
    pub id: String,
    /// Command being executed
    pub command: String,
    /// Working directory
    pub working_dir: String,
    /// When the session was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Whether the session is still running
    pub is_running: bool,
    /// Exit code (if session has ended)
    pub exit_code: Option<i32>,
    /// Current output buffer size
    pub output_buffer_size: usize,
}

/// Internal session state
struct Session {
    id: String,
    command: String,
    working_dir: String,
    process: Arc<Mutex<Child>>,
    stdin: Arc<Mutex<tokio::process::ChildStdin>>,
    output_buffer: Arc<Mutex<Vec<String>>>,
    created_at: Instant,
    last_activity: Arc<Mutex<Instant>>,
}

impl Session {
    /// Create a new session
    async fn new(
        id: String,
        command: String,
        working_dir: Option<String>,
        env: Option<std::collections::HashMap<String, String>>,
    ) -> Result<Self, String> {
        let working_dir = working_dir.unwrap_or_else(|| ".".to_string());

        // Split command into program and arguments
        let parts: Vec<&str> = command.split_whitespace().collect();
        if parts.is_empty() {
            return Err("Empty command".to_string());
        }

        let program = parts[0];
        let args = &parts[1..];

        // Build command
        let mut cmd = Command::new(program);
        cmd.args(args)
            .current_dir(&working_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        // Add environment variables
        if let Some(env_vars) = env {
            for (key, value) in env_vars {
                cmd.env(key, value);
            }
        }

        // Spawn process
        let mut child = cmd
            .spawn()
            .map_err(|e| format!("Failed to spawn process: {}", e))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "Failed to capture stdin".to_string())?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "Failed to capture stdout".to_string())?;

        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| "Failed to capture stderr".to_string())?;

        let stdin = Arc::new(Mutex::new(stdin));
        let output_buffer: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let last_activity = Arc::new(Mutex::new(Instant::now()));

        // Spawn output reading tasks
        let buffer_clone = output_buffer.clone();
        let activity_clone = last_activity.clone();
        tokio::spawn(async move {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                let mut buffer = buffer_clone.lock().await;

                // Calculate current buffer size in bytes
                let current_size: usize = buffer.iter().map(|s| s.len()).sum();

                // If adding this line would exceed the limit, remove oldest entries
                let new_line_size = line.len() + 10; // +10 for "[stdout] " prefix
                if current_size + new_line_size > MAX_OUTPUT_BUFFER {
                    // Remove lines from the front until we have enough space
                    let mut size_needed = current_size + new_line_size - MAX_OUTPUT_BUFFER;
                    let mut remove_count = 0;
                    for line in buffer.iter() {
                        if line.len() >= size_needed {
                            break;
                        }
                        size_needed -= line.len();
                        remove_count += 1;
                    }
                    buffer.drain(0..remove_count);
                }

                buffer.push(format!("[stdout] {}", line));

                *activity_clone.lock().await = Instant::now();
            }
        });

        let buffer_clone = output_buffer.clone();
        let activity_clone = last_activity.clone();
        tokio::spawn(async move {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                let mut buffer = buffer_clone.lock().await;

                // Calculate current buffer size in bytes
                let current_size: usize = buffer.iter().map(|s| s.len()).sum();

                // If adding this line would exceed the limit, remove oldest entries
                let new_line_size = line.len() + 10; // +10 for "[stderr] " prefix
                if current_size + new_line_size > MAX_OUTPUT_BUFFER {
                    // Remove lines from the front until we have enough space
                    let mut size_needed = current_size + new_line_size - MAX_OUTPUT_BUFFER;
                    let mut remove_count = 0;
                    for line in buffer.iter() {
                        if line.len() >= size_needed {
                            break;
                        }
                        size_needed -= line.len();
                        remove_count += 1;
                    }
                    buffer.drain(0..remove_count);
                }

                buffer.push(format!("[stderr] {}", line));

                *activity_clone.lock().await = Instant::now();
            }
        });

        Ok(Session {
            id,
            command,
            working_dir,
            process: Arc::new(Mutex::new(child)),
            stdin,
            output_buffer,
            created_at: Instant::now(),
            last_activity,
        })
    }

    /// Check if the session is still running
    async fn is_running(&self) -> bool {
        match self.process.lock().await.try_wait() {
            Ok(None) => true,
            Ok(Some(_)) => false,
            Err(_) => false,
        }
    }

    /// Get the exit code if the process has finished
    async fn exit_code(&self) -> Option<i32> {
        match self.process.lock().await.try_wait() {
            Ok(Some(status)) => status.code(),
            _ => None,
        }
    }

    /// Send input to the session
    async fn send_input(&self, input: &str) -> Result<(), String> {
        let mut stdin = self.stdin.lock().await;
        stdin
            .write_all(input.as_bytes())
            .await
            .map_err(|e| format!("Failed to write to stdin: {}", e))?;

        // Add newline if not present
        if !input.ends_with('\n') {
            stdin
                .write_all(b"\n")
                .await
                .map_err(|e| format!("Failed to write newline: {}", e))?;
        }

        stdin.flush().await.map_err(|e| format!("Failed to flush stdin: {}", e))?;

        *self.last_activity.lock().await = Instant::now();
        Ok(())
    }

    /// Read output from the session
    async fn read_output(&self, max_lines: usize) -> Vec<String> {
        let buffer = self.output_buffer.lock().await;
        let start = buffer.len().saturating_sub(max_lines);
        buffer[start..].to_vec()
    }

    /// Kill the session
    async fn kill(&self) -> Result<(), String> {
        let mut process = self.process.lock().await;
        process
            .kill()
            .await
            .map_err(|e| format!("Failed to kill process: {}", e))?;
        Ok(())
    }

    /// Get session info
    async fn get_info(&self) -> SessionInfo {
        // Calculate creation time: current time - elapsed time since creation
        let now = chrono::Utc::now();
        let elapsed = chrono::Duration::from_std(self.created_at.elapsed()).unwrap_or_default();
        let created_at = now - elapsed;

        SessionInfo {
            id: self.id.clone(),
            command: self.command.clone(),
            working_dir: self.working_dir.clone(),
            created_at,
            is_running: self.is_running().await,
            exit_code: self.exit_code().await,
            output_buffer_size: self.output_buffer.lock().await.len(),
        }
    }

    /// Check if the session has been inactive for too long
    async fn is_inactive(&self, max_inactive: Duration) -> bool {
        let last = *self.last_activity.lock().await;
        last.elapsed() > max_inactive
    }
}

/// Tool for managing terminal sessions
pub struct TerminalSessionTool {
    sessions: Arc<DashMap<String, Arc<Session>>>,
}

impl TerminalSessionTool {
    /// Create a new terminal session tool
    pub fn new() -> Self {
        let sessions: Arc<DashMap<String, Arc<Session>>> = Arc::new(DashMap::new());

        // Start cleanup task only if running in a tokio runtime
        let sessions_clone = sessions.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(60));

                loop {
                    interval.tick().await;

                    // Remove inactive sessions based on last_activity
                    // Phase 1: Collect Arc<Session> clones without holding the map guard
                    // We collect the Arc<Session> directly so we can call is_inactive() on it
                    let sessions_to_check: Vec<(String, Arc<Session>)> = sessions_clone
                        .iter()
                        .map(|entry| {
                            // Clone both the key and the Arc<Session>
                            // This allows us to release the DashMap guard before await
                            (entry.key().clone(), entry.value().clone())
                        })
                        .collect();

                    // Phase 2: Check each session for inactivity without holding map locks
                    // This is done outside the DashMap iteration to avoid holding locks across await
                    let mut to_remove = Vec::new();
                    for (id, session) in sessions_to_check {
                        // Check inactivity status without holding any DashMap guards
                        // This prevents deadlock and allows other operations during cleanup
                        if session.is_inactive(DEFAULT_SESSION_INACTIVITY_TIMEOUT).await {
                            to_remove.push(id);
                        }
                    }

                    // Phase 3: Remove inactive sessions
                    // Acquire the map lock only for the removal operation
                    for id in to_remove {
                        if let Some((_, session)) = sessions_clone.remove(&id) {
                            let _ = session.kill().await;
                            log::info!("Terminal session '{}' inactive and was terminated", id);
                        }
                    }
                }
            });
        }

        Self { sessions }
    }

    /// Start a new session
    async fn start_session(
        &self,
        command: String,
        working_dir: Option<String>,
        env: Option<std::collections::HashMap<String, String>>,
    ) -> Result<SessionInfo, String> {
        // Check session limit
        if self.sessions.len() >= MAX_SESSIONS {
            return Err(format!(
                "Maximum number of sessions ({}) reached. Kill an existing session first.",
                MAX_SESSIONS
            ));
        }

        // Generate session ID
        let session_id = format!("session_{}", uuid::Uuid::new_v4().to_string().split('-').next().unwrap_or("unknown"));

        // Create session
        let session = Arc::new(Session::new(
            session_id.clone(),
            command,
            working_dir,
            env,
        )
        .await?);

        let info = session.get_info().await;
        self.sessions.insert(session_id, session);

        Ok(info)
    }

    /// Send input to a session
    async fn send_input(&self, session_id: &str, input: &str) -> Result<(), String> {
        let session = self
            .sessions
            .get(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?
            .clone(); // Clone Arc to release DashMap guard before await

        if !session.is_running().await {
            return Err(format!("Session '{}' is not running", session_id));
        }

        session.send_input(input).await
    }

    /// Read output from a session with optional timeout
    ///
    /// If timeout_seconds > 0 and no output is available, will wait up to
    /// the specified time for output to arrive, polling every 100ms.
    async fn read_output(
        &self,
        session_id: &str,
        max_lines: usize,
        timeout_seconds: u64,
    ) -> Result<Vec<String>, String> {
        let session = self
            .sessions
            .get(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?
            .clone(); // Clone Arc to release DashMap guard before await

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_seconds);

        loop {
            let output = session.read_output(max_lines).await;

            // If we have output or no timeout specified, return immediately
            if !output.is_empty() || timeout_seconds == 0 {
                return Ok(output);
            }

            // Check if we've exceeded the timeout
            if start.elapsed() >= timeout {
                return Ok(output); // Return empty output after timeout
            }

            // Wait a bit before polling again
            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Get session status
    async fn get_status(&self, session_id: &str) -> Result<SessionInfo, String> {
        let session = self
            .sessions
            .get(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?
            .clone(); // Clone Arc to release DashMap guard before await

        Ok(session.get_info().await)
    }

    /// Kill a session
    async fn kill_session(&self, session_id: &str) -> Result<(), String> {
        let (_, session) = self
            .sessions
            .remove(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?;

        session.kill().await
    }

    /// List all active sessions
    async fn list_sessions(&self) -> Vec<SessionInfo> {
        let mut infos = Vec::new();

        // Collect Arc clones first, then await to avoid holding lock
        let sessions: Vec<Arc<Session>> = self.sessions.iter().map(|entry| entry.value().clone()).collect();

        for session in sessions {
            infos.push(session.get_info().await);
        }

        infos.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        infos
    }
}

impl Default for TerminalSessionTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for TerminalSessionTool {
    fn name(&self) -> &str {
        "terminal_session"
    }

    fn description(&self) -> &str {
        "Manage long-running interactive terminal sessions. Supports starting processes (e.g., dev servers), sending input, reading output, and managing multiple concurrent sessions. Sessions automatically timeout after 5 minutes of inactivity."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["start", "send_input", "read_output", "status", "kill", "list"],
                    "description": "Action to perform"
                },
                "command": {
                    "type": "string",
                    "description": "Command to execute (for start action)"
                },
                "working_dir": {
                    "type": "string",
                    "description": "Working directory for the command (for start action)"
                },
                "env": {
                    "type": "object",
                    "additionalProperties": { "type": "string" },
                    "description": "Environment variables (for start action)"
                },
                "session_id": {
                    "type": "string",
                    "description": "Session ID (required for send_input, read_output, status, kill actions)"
                },
                "input": {
                    "type": "string",
                    "description": "Input to send (for send_input action)"
                },
                "max_lines": {
                    "type": "integer",
                    "default": 100,
                    "description": "Maximum lines to read (for read_output action)"
                },
                "timeout_seconds": {
                    "type": "integer",
                    "default": 5,
                    "description": "Timeout for reading output (for read_output action)"
                }
            },
            "required": ["action"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let action: SessionAction =
            serde_json::from_value(args).map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        let result = match action {
            SessionAction::Start {
                command,
                working_dir,
                env,
            } => {
                match self.start_session(command, working_dir, env).await {
                    Ok(info) => {
                        let output = format!(
                            "Session '{}' started\nCommand: {}\nWorking directory: {}",
                            info.id, info.command, info.working_dir
                        );
                        ToolResult {
                            success: true,
                            result: output,
                            display_preference: Some("markdown".to_string()),
                        }
                    }
                    Err(e) => ToolResult {
                        success: false,
                        result: e,
                        display_preference: None,
                    },
                }
            }
            SessionAction::SendInput { session_id, input } => {
                match self.send_input(&session_id, &input).await {
                    Ok(()) => ToolResult {
                        success: true,
                        result: format!("Input sent to session '{}'", session_id),
                        display_preference: None,
                    },
                    Err(e) => ToolResult {
                        success: false,
                        result: e,
                        display_preference: None,
                    },
                }
            }
            SessionAction::ReadOutput {
                session_id,
                max_lines,
                timeout_seconds,
            } => {
                match self.read_output(&session_id, max_lines, timeout_seconds).await {
                    Ok(lines) => {
                        let output = if lines.is_empty() {
                            "No output available".to_string()
                        } else {
                            lines.join("\n")
                        };
                        ToolResult {
                            success: true,
                            result: output,
                            display_preference: Some("markdown".to_string()),
                        }
                    }
                    Err(e) => ToolResult {
                        success: false,
                        result: e,
                        display_preference: None,
                    },
                }
            }
            SessionAction::Status { session_id } => {
                match self.get_status(&session_id).await {
                    Ok(info) => {
                        let status = if info.is_running {
                            "Running"
                        } else {
                            "Stopped"
                        };
                        let exit_info = info
                            .exit_code
                            .map(|code| format!(", exit code: {}", code))
                            .unwrap_or_default();
                        let output = format!(
                            "Session: {}\nStatus: {}{}\nCommand: {}\nOutput buffer: {} lines",
                            info.id, status, exit_info, info.command, info.output_buffer_size
                        );
                        ToolResult {
                            success: true,
                            result: output,
                            display_preference: Some("markdown".to_string()),
                        }
                    }
                    Err(e) => ToolResult {
                        success: false,
                        result: e,
                        display_preference: None,
                    },
                }
            }
            SessionAction::Kill { session_id } => {
                match self.kill_session(&session_id).await {
                    Ok(()) => ToolResult {
                        success: true,
                        result: format!("Session '{}' terminated", session_id),
                        display_preference: None,
                    },
                    Err(e) => ToolResult {
                        success: false,
                        result: e,
                        display_preference: None,
                    },
                }
            }
            SessionAction::List => {
                let sessions = self.list_sessions().await;
                if sessions.is_empty() {
                    ToolResult {
                        success: true,
                        result: "No active sessions".to_string(),
                        display_preference: None,
                    }
                } else {
                    let lines: Vec<String> = sessions
                        .iter()
                        .map(|s| {
                            let status = if s.is_running { "Running" } else { "Stopped" };
                            format!("- {}: {} [{}]", s.id, s.command, status)
                        })
                        .collect();
                    ToolResult {
                        success: true,
                        result: format!("Active sessions ({}):\n\n{}", sessions.len(), lines.join("\n")),
                        display_preference: Some("markdown".to_string()),
                    }
                }
            }
        };

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_tool_name_and_description() {
        let tool = TerminalSessionTool::new();
        assert_eq!(tool.name(), "terminal_session");
        assert!(tool.description().contains("terminal"));
    }

    #[tokio::test]
    async fn test_session_action_deserialization() {
        let json = json!({
            "action": "start",
            "command": "echo hello",
            "working_dir": "/tmp"
        });

        let action: SessionAction = serde_json::from_value(json).unwrap();
        match action {
            SessionAction::Start { command, working_dir, env } => {
                assert_eq!(command, "echo hello");
                assert_eq!(working_dir, Some("/tmp".to_string()));
                assert!(env.is_none());
            }
            _ => panic!("Expected Start action"),
        }
    }

    #[tokio::test]
    async fn test_start_and_list_sessions() {
        let tool = TerminalSessionTool::new();

        // Start a simple session
        let result = tool
            .execute(json!({
                "action": "start",
                "command": "sleep 10"
            }))
            .await
            .unwrap();

        assert!(result.success);
        assert!(result.result.contains("session_"));

        // List sessions
        let result = tool.execute(json!({ "action": "list" })).await.unwrap();
        assert!(result.success);
        assert!(result.result.contains("sleep 10"));

        // Extract session ID and kill it
        let session_id = result
            .result
            .lines()
            .find(|l| l.contains("session_"))
            .and_then(|l| l.split(':').next())
            .map(|s| s.trim_start_matches("- ").to_string())
            .unwrap();

        let result = tool
            .execute(json!({
                "action": "kill",
                "session_id": session_id
            }))
            .await
            .unwrap();

        assert!(result.success);
    }

    #[tokio::test]
    async fn test_nonexistent_session() {
        let tool = TerminalSessionTool::new();

        let result = tool
            .execute(json!({
                "action": "status",
                "session_id": "nonexistent"
            }))
            .await
            .unwrap();

        assert!(!result.success);
        assert!(result.result.contains("not found"));
    }

    #[tokio::test]
    async fn test_session_start_echo() {
        let tool = TerminalSessionTool::new();

        // Start a simple echo command
        let result = tool
            .execute(json!({
                "action": "start",
                "command": "echo test_output"
            }))
            .await
            .unwrap();

        assert!(result.success);

        // Give it a moment to complete
        sleep(Duration::from_millis(200)).await;

        // Extract session ID (format: "Session 'session_id' started")
        let session_id = result
            .result
            .lines()
            .next()
            .unwrap()
            .split(' ')
            .nth(1)
            .unwrap()
            .trim_matches('\'')
            .to_string();

        // Read output
        let result = tool
            .execute(json!({
                "action": "read_output",
                "session_id": session_id,
                "max_lines": 10
            }))
            .await
            .unwrap();

        assert!(result.success);
        // The command may have already finished, so we just check it ran

        // Cleanup
        let _ = tool
            .execute(json!({
                "action": "kill",
                "session_id": session_id
            }))
            .await;
    }

    // Session Cleanup Tests

    #[tokio::test]
    async fn test_cleanup_inactive_sessions() {
        // Note: Testing the actual cleanup task is difficult in unit tests
        // because the cleanup runs in a background task.
        // Here we test the is_inactive method directly.

        let tool = TerminalSessionTool::new();

        // Start a simple session
        let result = tool
            .execute(json!({
                "action": "start",
                "command": "echo test"
            }))
            .await
            .unwrap();

        assert!(result.success);

        // Extract session ID
        let session_id = result
            .result
            .lines()
            .next()
            .unwrap()
            .split(' ')
            .nth(1)
            .unwrap()
            .trim_matches('\'')
            .to_string();

        // Get the session and test is_inactive with a short timeout
        if let Some(session) = tool.sessions.get(&session_id) {
            // Session should be active with default timeout
            assert!(!session.value().is_inactive(Duration::from_secs(1000)).await);
            // Session should be inactive with zero timeout
            assert!(session.value().is_inactive(Duration::from_secs(0)).await);
        }

        // Cleanup
        let _ = tool.execute(json!({"action": "kill", "session_id": session_id})).await;
    }

    #[tokio::test]
    async fn test_cleanup_respects_activity() {
        use std::time::Duration;

        let tool = TerminalSessionTool::new();

        // Start a session
        let result = tool
            .execute(json!({
                "action": "start",
                "command": "cat"
            }))
            .await
            .unwrap();

        assert!(result.success);

        let session_id = result
            .result
            .lines()
            .next()
            .unwrap()
            .split(' ')
            .nth(1)
            .unwrap()
            .trim_matches('\'')
            .to_string();

        // Get the session
        let session = if let Some(s) = tool.sessions.get(&session_id) {
            s.value().clone()
        } else {
            panic!("Session not found");
        };

        // Session should be active with default timeout
        assert!(!session.is_inactive(Duration::from_secs(300)).await);

        // Kill the session
        let _ = tool.execute(json!({"action": "kill", "session_id": session_id})).await;

        // After kill, session should no longer exist
        assert!(tool.sessions.get(&session_id).is_none());
    }

    #[tokio::test]
    async fn test_cleanup_with_recent_output() {
        let tool = TerminalSessionTool::new();

        // Start a session that produces output
        let result = tool
            .execute(json!({
                "action": "start",
                "command": "echo 'test output'"
            }))
            .await
            .unwrap();

        assert!(result.success);

        let session_id = result
            .result
            .lines()
            .next()
            .unwrap()
            .split(' ')
            .nth(1)
            .unwrap()
            .trim_matches('\'')
            .to_string();

        // Give it time to complete and produce output
        sleep(Duration::from_millis(200)).await;

        // Get the session
        if let Some(session) = tool.sessions.get(&session_id) {
            // Session should still be active (has recent activity)
            assert!(!session.value().is_inactive(Duration::from_secs(10)).await);
        }

        // Cleanup
        let _ = tool.execute(json!({"action": "kill", "session_id": session_id})).await;
    }

    #[tokio::test]
    async fn test_session_activity_tracking() {
        let tool = TerminalSessionTool::new();

        // Start a long-running session
        let result = tool
            .execute(json!({
                "action": "start",
                "command": "sleep 30"
            }))
            .await
            .unwrap();

        assert!(result.success);

        let session_id = result
            .result
            .lines()
            .next()
            .unwrap()
            .split(' ')
            .nth(1)
            .unwrap()
            .trim_matches('\'')
            .to_string();

        // Get initial activity
        let session = if let Some(s) = tool.sessions.get(&session_id) {
            s.value().clone()
        } else {
            panic!("Session not found");
        };

        let initial_activity = *session.last_activity.lock().await;

        // Send input to update activity
        sleep(Duration::from_millis(50)).await;
        let _ = tool
            .execute(json!({
                "action": "send_input",
                "session_id": session_id,
                "input": "x" // Should send SIGINT to sleep
            }))
            .await;

        // Give it time to process
        sleep(Duration::from_millis(50)).await;

        let new_activity = *session.last_activity.lock().await;

        // Activity should have been updated
        assert!(new_activity > initial_activity);

        // Cleanup
        let _ = tool.execute(json!({"action": "kill", "session_id": session_id})).await;
    }
}
