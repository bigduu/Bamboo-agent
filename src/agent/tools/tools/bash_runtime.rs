use crate::core::process_utils::{hide_window_for_tokio_command, trace_windows_command};
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

const MAX_OUTPUT_LINES: usize = 20_000;
const COMPLETED_SESSION_TTL_SECS: u64 = 300;

#[cfg(target_os = "windows")]
const SHELL: (&str, &str) = ("cmd", "/c");
#[cfg(not(target_os = "windows"))]
const SHELL: (&str, &str) = ("sh", "-c");

#[derive(Debug)]
pub struct ShellSession {
    pub id: String,
    pub command: String,
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

pub async fn spawn_background(
    command: &str,
    cwd: Option<&Path>,
) -> Result<Arc<ShellSession>, String> {
    let (shell, arg) = SHELL;
    trace_windows_command("agent.bash.background", shell, [arg, command]);
    let mut cmd = Command::new(shell);
    hide_window_for_tokio_command(&mut cmd);
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    cmd.arg(arg)
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
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                push_line(&output, &base_index, line).await;
            }
        });
    }

    {
        let output = output.clone();
        let base_index = base_index.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                push_line(&output, &base_index, line).await;
            }
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
