use crate::core::process_utils::{
    build_command_environment, decode_process_line_lossy, hide_window_for_tokio_command,
    preferred_bash_shell, trace_windows_command, CommandEnvironmentDiagnostics,
};
use dashmap::DashMap;
use regex::Regex;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing::warn;

const MAX_OUTPUT_LINES: usize = 20_000;
const COMPLETED_SESSION_TTL_SECS: u64 = 300;

#[derive(Debug)]
pub struct ShellSession {
    pub id: String,
    pub command: String,
    pub environment: CommandEnvironmentDiagnostics,
    child: Arc<Mutex<Child>>,
    output: Arc<Mutex<Vec<String>>>,
    base_index: Arc<Mutex<usize>>,
    running: Arc<AtomicBool>,
    exit_code: Arc<Mutex<Option<i32>>>,
}

impl ShellSession {
    pub fn status(&self) -> &'static str {
        if self.running.load(Ordering::Relaxed) {
            "running"
        } else {
            "completed"
        }
    }

    pub async fn exit_code(&self) -> Option<i32> {
        *self.exit_code.lock().await
    }

    pub async fn read_output_since(
        &self,
        cursor: usize,
        filter: Option<&Regex>,
    ) -> (Vec<String>, usize, usize) {
        let output = self.output.lock().await;
        let base_index = self.base_index.lock().await;

        let base = *base_index;
        let effective_cursor = cursor.max(base);
        let dropped_lines = effective_cursor.saturating_sub(cursor);
        let start = effective_cursor.saturating_sub(base);
        let new_lines = if start >= output.len() {
            Vec::new()
        } else {
            output[start..]
                .iter()
                .filter(|line| filter.map(|re| re.is_match(line)).unwrap_or(true))
                .cloned()
                .collect()
        };

        let next_cursor = base + output.len();
        (new_lines, next_cursor, dropped_lines)
    }

    pub async fn kill(&self) -> Result<(), String> {
        let mut child = self.child.lock().await;
        child
            .kill()
            .await
            .map_err(|e| format!("Failed to kill shell '{}': {}", self.id, e))?;
        self.running.store(false, Ordering::Relaxed);
        Ok(())
    }
}

fn sessions() -> &'static DashMap<String, Arc<ShellSession>> {
    static SESSIONS: OnceLock<DashMap<String, Arc<ShellSession>>> = OnceLock::new();
    SESSIONS.get_or_init(DashMap::new)
}

async fn push_line(output: &Arc<Mutex<Vec<String>>>, base_index: &Arc<Mutex<usize>>, line: String) {
    let mut buffer = output.lock().await;
    buffer.push(line);
    if buffer.len() > MAX_OUTPUT_LINES {
        let overflow = buffer.len() - MAX_OUTPUT_LINES;
        buffer.drain(0..overflow);
        let mut base = base_index.lock().await;
        *base += overflow;
    }
}

async fn pump_stream_lines<T>(
    stream_name: &'static str,
    reader: T,
    output: Arc<Mutex<Vec<String>>>,
    base_index: Arc<Mutex<usize>>,
) where
    T: tokio::io::AsyncRead + Unpin,
{
    let mut reader = BufReader::new(reader);
    let mut line_bytes = Vec::new();

    loop {
        line_bytes.clear();
        match reader.read_until(b'\n', &mut line_bytes).await {
            Ok(0) => break,
            Ok(_) => {
                let line = decode_process_line_lossy(&mut line_bytes);
                push_line(&output, &base_index, line).await;
            }
            Err(e) => {
                warn!("Background shell {stream_name} read failed: {e}");
                break;
            }
        }
    }
}

pub async fn spawn_background(
    command: &str,
    cwd: Option<&Path>,
) -> Result<Arc<ShellSession>, String> {
    let shell = preferred_bash_shell();
    trace_windows_command(
        "agent.bash.background",
        &shell.program,
        [shell.arg, command],
    );
    let overrides = crate::core::Config::current_env_vars();
    let prepared_env = build_command_environment(&overrides).await;
    let mut cmd = Command::new(&shell.program);
    hide_window_for_tokio_command(&mut cmd);
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    prepared_env.apply_to_tokio_command(&mut cmd);
    cmd.arg(shell.arg)
        .arg(command)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("Failed to spawn background shell: {}", e))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Failed to capture shell stdout".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "Failed to capture shell stderr".to_string())?;

    let session_id = uuid::Uuid::new_v4().to_string();
    let output = Arc::new(Mutex::new(Vec::new()));
    let base_index = Arc::new(Mutex::new(0usize));
    let running = Arc::new(AtomicBool::new(true));
    let exit_code = Arc::new(Mutex::new(None));

    let session = Arc::new(ShellSession {
        id: session_id.clone(),
        command: command.to_string(),
        environment: prepared_env.diagnostics.clone(),
        child: Arc::new(Mutex::new(child)),
        output: output.clone(),
        base_index: base_index.clone(),
        running: running.clone(),
        exit_code: exit_code.clone(),
    });

    {
        let output = output.clone();
        let base_index = base_index.clone();
        tokio::spawn(async move {
            pump_stream_lines("stdout", stdout, output, base_index).await;
        });
    }

    {
        let output = output.clone();
        let base_index = base_index.clone();
        tokio::spawn(async move {
            pump_stream_lines("stderr", stderr, output, base_index).await;
        });
    }

    {
        let child = session.child.clone();
        let session_id_for_gc = session_id.clone();
        tokio::spawn(async move {
            loop {
                let poll = {
                    let mut guard = child.lock().await;
                    guard.try_wait()
                };
                match poll {
                    Ok(Some(status)) => {
                        *exit_code.lock().await = status.code();
                        running.store(false, Ordering::Relaxed);
                        break;
                    }
                    Ok(None) => {
                        sleep(Duration::from_millis(100)).await;
                    }
                    Err(_) => {
                        running.store(false, Ordering::Relaxed);
                        break;
                    }
                }
            }
            sleep(Duration::from_secs(COMPLETED_SESSION_TTL_SECS)).await;
            let _ = remove_shell(&session_id_for_gc);
        });
    }

    sessions().insert(session_id, session.clone());
    Ok(session)
}

pub fn get_shell(id: &str) -> Option<Arc<ShellSession>> {
    sessions().get(id).map(|entry| entry.value().clone())
}

pub fn remove_shell(id: &str) -> Option<Arc<ShellSession>> {
    sessions().remove(id).map(|(_, value)| value)
}
