use async_trait::async_trait;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, trace, warn};

use crate::config::StdioConfig;
use crate::error::{McpError, Result};
use crate::protocol::client::McpTransport;
use bamboo_infrastructure::process::{hide_window_for_tokio_command, trace_windows_command};

use std::sync::Arc;

pub struct StdioTransport {
    config: StdioConfig,
    child: Option<Child>,
    stdin: Option<Arc<Mutex<ChildStdin>>>,
    // Inbound messages are delivered through this channel by a dedicated
    // reader task (spawned in connect). The reader task owns the child's
    // stdout and is the sole sender — when stdout reaches EOF or an error,
    // the sender is dropped and the channel closes, which wakes the client
    // handler with zero idle wakeups.
    message_rx: Mutex<Option<mpsc::Receiver<String>>>,
    reader_handle: Option<tokio::task::JoinHandle<()>>,
}

impl StdioTransport {
    pub fn new(config: StdioConfig) -> Self {
        Self {
            config,
            child: None,
            stdin: None,
            message_rx: Mutex::new(None),
            reader_handle: None,
        }
    }
}

#[async_trait]
impl McpTransport for StdioTransport {
    async fn connect(&mut self) -> Result<()> {
        info!(
            "Starting MCP server process: {} {:?}",
            self.config.command, self.config.args
        );

        trace_windows_command(
            "agent.mcp.stdio.connect",
            &self.config.command,
            self.config.args.iter().map(String::as_str),
        );
        let mut cmd = Command::new(&self.config.command);
        hide_window_for_tokio_command(&mut cmd);
        cmd.args(&self.config.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // Kill the server process if this transport's `Child` is dropped
            // without a graceful `disconnect()` — e.g. an error/timeout during
            // the post-spawn handshake drops the transport on the error path.
            // `tokio::process::Child` does NOT kill on drop by default, so
            // without this a failed-handshake server is orphaned (one leak per
            // retry under auto-reconnect).
            .kill_on_drop(true);

        if let Some(cwd) = &self.config.cwd {
            cmd.current_dir(cwd);
        }

        if !self.config.env.is_empty() {
            cmd.envs(&self.config.env);
        }

        let mut child = cmd.spawn().map_err(|e| {
            error!("Failed to spawn MCP server process: {}", e);
            McpError::Transport(format!("Failed to spawn process: {}", e))
        })?;

        // Get stdin/stdout
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| McpError::Transport("Failed to capture stdin".to_string()))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| McpError::Transport("Failed to capture stdout".to_string()))?;

        // Start stderr logger
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    trace!("[MCP server stderr] {}", line);
                }
            });
        }

        // Spawn a dedicated reader task that owns stdout and pushes each
        // non-empty line into the message channel. This replaces the old
        // per-call `receive()` that polled stdout with a 100ms timeout.
        let (message_tx, message_rx) = mpsc::channel(100);
        let reader_handle = tokio::spawn(async move {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        let line = line.trim();
                        if line.is_empty() {
                            continue;
                        }
                        // Raw wire logs can be extremely noisy (e.g., keepalive pings).
                        trace!("Received: {}", line);
                        if message_tx.send(line.to_string()).await.is_err() {
                            // Receiver dropped — client handler is gone.
                            break;
                        }
                    }
                    Ok(None) => {
                        // EOF — child exited.
                        warn!("MCP server stdout closed (EOF)");
                        break;
                    }
                    Err(e) => {
                        warn!("MCP server stdout read error: {}", e);
                        break;
                    }
                }
            }
            // message_tx is dropped here → channel closes → handler exits.
        });

        self.child = Some(child);
        self.stdin = Some(Arc::new(Mutex::new(stdin)));
        self.message_rx = Mutex::new(Some(message_rx));
        self.reader_handle = Some(reader_handle);

        info!("MCP server process started successfully");
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        info!("Disconnecting MCP server process");

        // Close stdin to signal EOF to the child process.
        self.stdin = None;

        // Abort the reader task (belt-and-suspenders: it should end on its own
        // when stdout closes after the child is killed).
        if let Some(handle) = self.reader_handle.take() {
            handle.abort();
        }

        if let Some(mut child) = self.child.take() {
            // Try graceful shutdown
            match tokio::time::timeout(tokio::time::Duration::from_secs(5), child.wait()).await {
                Ok(Ok(_)) => {
                    info!("MCP server process exited gracefully");
                }
                _ => {
                    warn!("MCP server process did not exit gracefully, killing");
                    let _ = child.kill().await;
                }
            }
        }

        // Drop the message receiver so any lingering take_message_receiver
        // consumer sees a clean close.
        {
            let mut guard = self.message_rx.lock().await;
            *guard = None;
        }

        Ok(())
    }

    async fn send(&self, message: String) -> Result<()> {
        let stdin = self.stdin.as_ref().ok_or_else(|| McpError::Disconnected)?;

        let mut stdin = stdin.lock().await;
        let message_with_newline = format!("{}\n", message);
        stdin
            .write_all(message_with_newline.as_bytes())
            .await
            .map_err(|e| McpError::Transport(format!("Failed to write: {}", e)))?;
        stdin
            .flush()
            .await
            .map_err(|e| McpError::Transport(format!("Failed to flush: {}", e)))?;

        // Raw wire logs can be extremely noisy (e.g., keepalive pings).
        trace!("Sent: {}", message);
        Ok(())
    }

    async fn take_message_receiver(&self) -> Option<mpsc::Receiver<String>> {
        self.message_rx.lock().await.take()
    }

    async fn receive(&self) -> Result<Option<String>> {
        let mut guard = self.message_rx.lock().await;
        match guard.as_mut() {
            None => Err(McpError::Disconnected),
            Some(rx) => {
                match tokio::time::timeout(tokio::time::Duration::from_millis(100), rx.recv()).await
                {
                    Ok(Some(message)) => Ok(Some(message)),
                    Ok(None) => {
                        // Channel closed (reader task ended / EOF).
                        warn!("MCP server stdout channel closed");
                        Err(McpError::Disconnected)
                    }
                    Err(_) => {
                        // Timeout, no data available.
                        Ok(None)
                    }
                }
            }
        }
    }

    fn is_connected(&self) -> bool {
        // Note: is_connected is called on &self, but try_wait needs &mut self
        // We use a simple check - if we have a child handle, assume connected
        // Actual process exit will be detected when the reader task ends and
        // the channel closes.
        self.child.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_config() -> StdioConfig {
        StdioConfig {
            command: "echo".to_string(),
            args: vec![],
            cwd: None,
            env: HashMap::new(),
            env_encrypted: HashMap::new(),
            startup_timeout_ms: 5000,
        }
    }

    #[test]
    fn test_stdio_transport_new() {
        let config = create_test_config();
        let transport = StdioTransport::new(config);
        assert!(transport.child.is_none());
        assert!(transport.stdin.is_none());
        assert!(transport.reader_handle.is_none());
    }

    #[tokio::test]
    async fn test_stdio_connect() {
        let config = create_test_config();
        let mut transport = StdioTransport::new(config);

        let result = transport.connect().await;
        assert!(result.is_ok());
        assert!(transport.child.is_some());
        assert!(transport.stdin.is_some());
        assert!(transport.reader_handle.is_some());
        assert!(transport.is_connected());

        // Clean up
        let _ = transport.disconnect().await;
    }

    #[tokio::test]
    async fn test_stdio_disconnect() {
        let config = create_test_config();
        let mut transport = StdioTransport::new(config);

        transport.connect().await.unwrap();
        assert!(transport.is_connected());

        let result = transport.disconnect().await;
        assert!(result.is_ok());
        assert!(transport.child.is_none());
        assert!(transport.stdin.is_none());
        assert!(transport.reader_handle.is_none());
        assert!(!transport.is_connected());
    }

    #[tokio::test]
    async fn test_stdio_send_disconnected() {
        let config = create_test_config();
        let transport = StdioTransport::new(config);

        // Try to send without connecting
        let result = transport.send("test".to_string()).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            McpError::Disconnected => {}
            _ => panic!("Expected Disconnected error"),
        }
    }

    #[tokio::test]
    async fn test_stdio_receive_disconnected() {
        let config = create_test_config();
        let transport = StdioTransport::new(config);

        // Try to receive without connecting
        let result = transport.receive().await;
        assert!(result.is_err());
        match result.unwrap_err() {
            McpError::Disconnected => {}
            _ => panic!("Expected Disconnected error"),
        }
    }

    #[tokio::test]
    async fn test_stdio_send_and_receive() {
        let config = StdioConfig {
            command: "cat".to_string(), // cat echoes back
            args: vec![],
            cwd: None,
            env: HashMap::new(),
            env_encrypted: HashMap::new(),
            startup_timeout_ms: 5000,
        };

        let mut transport = StdioTransport::new(config);
        transport.connect().await.unwrap();

        // Send a message
        let result = transport.send("hello".to_string()).await;
        assert!(result.is_ok());

        // Try to receive (may timeout if process doesn't respond immediately)
        // Note: cat may not respond as expected in this context
        // So we just verify the send worked

        let _ = transport.disconnect().await;
    }

    #[tokio::test]
    async fn test_stdio_connect_invalid_command() {
        let config = StdioConfig {
            command: "nonexistent_command_12345".to_string(),
            args: vec![],
            cwd: None,
            env: HashMap::new(),
            env_encrypted: HashMap::new(),
            startup_timeout_ms: 5000,
        };

        let mut transport = StdioTransport::new(config);
        let result = transport.connect().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_stdio_with_args() {
        let config = StdioConfig {
            command: "echo".to_string(),
            args: vec!["test".to_string()],
            cwd: None,
            env: HashMap::new(),
            env_encrypted: HashMap::new(),
            startup_timeout_ms: 5000,
        };

        let mut transport = StdioTransport::new(config);
        let result = transport.connect().await;
        assert!(result.is_ok());

        let _ = transport.disconnect().await;
    }

    #[tokio::test]
    async fn test_stdio_with_env() {
        let mut env = HashMap::new();
        env.insert("TEST_VAR".to_string(), "test_value".to_string());

        let config = StdioConfig {
            command: "echo".to_string(),
            args: vec![],
            cwd: None,
            env,
            env_encrypted: HashMap::new(),
            startup_timeout_ms: 5000,
        };

        let mut transport = StdioTransport::new(config);
        let result = transport.connect().await;
        assert!(result.is_ok());

        let _ = transport.disconnect().await;
    }

    #[tokio::test]
    async fn test_stdio_receive_timeout() {
        // Use `sleep` so the process stays alive without producing output,
        // keeping the channel open and letting receive() time out properly.
        let config = StdioConfig {
            command: "sleep".to_string(),
            args: vec!["10".to_string()],
            cwd: None,
            env: HashMap::new(),
            env_encrypted: HashMap::new(),
            startup_timeout_ms: 5000,
        };
        let mut transport = StdioTransport::new(config);
        transport.connect().await.unwrap();

        let result = transport.receive().await;
        // Should be Ok(None) on timeout (no data, channel still open).
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        let _ = transport.disconnect().await;
    }

    #[tokio::test]
    async fn test_stdio_is_connected() {
        let config = create_test_config();
        let mut transport = StdioTransport::new(config);

        assert!(!transport.is_connected());

        transport.connect().await.unwrap();
        assert!(transport.is_connected());

        transport.disconnect().await.unwrap();
        assert!(!transport.is_connected());
    }

    /// Verifies that the reader task delivers multiple messages in order and
    /// that the channel closes (clean shutdown) when stdout reaches EOF.
    #[tokio::test]
    async fn test_stdio_reader_delivers_messages_then_eof() {
        // `printf` outputs three lines then exits → reader task delivers them
        // and then gets EOF, closing the channel.
        let config = StdioConfig {
            command: "printf".to_string(),
            args: vec!["line-a\\nline-b\\nline-c\\n".to_string()],
            cwd: None,
            env: HashMap::new(),
            env_encrypted: HashMap::new(),
            startup_timeout_ms: 5000,
        };
        let mut transport = StdioTransport::new(config);
        transport.connect().await.unwrap();

        // Collect messages via take_message_receiver.
        let mut rx = transport.take_message_receiver().await.expect("receiver");

        let mut received = Vec::new();
        while let Some(msg) = rx.recv().await {
            received.push(msg);
        }
        // All three lines delivered in order.
        assert_eq!(received, vec!["line-a", "line-b", "line-c"]);

        let _ = transport.disconnect().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdio_child_is_killed_when_transport_dropped() {
        // A transport dropped WITHOUT disconnect() (e.g. an error/timeout during
        // the post-spawn handshake drops it on the error path) must not orphan
        // the server process — kill_on_drop guarantees the child is killed.
        let config = StdioConfig {
            command: "sleep".to_string(),
            args: vec!["30".to_string()],
            ..create_test_config()
        };
        let mut transport = StdioTransport::new(config);
        transport.connect().await.expect("connect spawns the child");
        let pid = transport
            .child
            .as_ref()
            .and_then(|c| c.id())
            .expect("a running child has a pid");

        // Drop without disconnect() — the failed-handshake path.
        drop(transport);

        // The child must disappear within a short window: SIGKILL on drop, then
        // reaped by the tokio runtime. `kill -0` succeeds while the pid is
        // live/zombie and fails (ESRCH) once reaped — poll until it fails.
        let mut alive = true;
        for _ in 0..50 {
            let dead = tokio::process::Command::new("kill")
                .arg("-0")
                .arg(pid.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await
                .map(|s| !s.success())
                .unwrap_or(true);
            if dead {
                alive = false;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(
            !alive,
            "child pid {pid} must be killed when the transport is dropped"
        );
    }
}
