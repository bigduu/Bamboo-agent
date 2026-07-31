//! Long-lived Codex app-server executor with Bamboo approval relay.
//!
//! The app-server process is retained by a warm actor worker, while logical
//! Codex threads are keyed by Bamboo session id. Server-to-client approval
//! requests are relayed through `HostBridge` and fail closed after 300 seconds.

use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::{AgentEvent, TokenUsage, ToolResult};
use bamboo_subagent::codex_discovery::discover_codex_app_server;
use bamboo_subagent::executor::{ChildExecutor, ChildOutcome, EventSink, HostBridge, SteerInbox};
use bamboo_subagent::executor_util::{build_rehydrated_turn, write_json_atomic};
use bamboo_subagent::proto::RunSpec;

use crate::codex_cli_executor::{
    read_bounded_line, terminate_child, CodexAuthConfig, CodexAuthMode, CodexPermissionConfig,
};

const MAX_STDOUT_LINE_BYTES: usize = 10 * 1024 * 1024;
const STDERR_TAIL_BYTES: usize = 16 * 1024;
const TOOL_RESULT_TRUNCATE_CHARS: usize = 20_000;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const APPROVAL_RELAY_TIMEOUT: Duration = Duration::from_secs(300);
const INTERRUPT_GRACE: Duration = Duration::from_secs(5);
const CODEX_PROVIDER_ENV: &str = "BAMBOO_CODEX_PROVIDER_KEY";
const SESSION_STORE_FILE: &str = "codex-app-server-sessions.json";
const TOKEN_FILE: &str = "codex-app-server-provider-token";
const MAX_LOGICAL_SESSIONS: usize = 256;

const ENV_ALLOWLIST: &[&str] = &[
    "HOME", "PATH", "SHELL", "TERM", "LANG", "TMPDIR", "USER", "LOGNAME",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppServerSessionState {
    thread_id: String,
    workspace: Option<String>,
    codex_home_mode: String,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AppServerSessionStore {
    #[serde(default)]
    sessions: HashMap<String, AppServerSessionState>,
}

struct AppServerConnection {
    child: Child,
    write_tx: mpsc::UnboundedSender<Value>,
    incoming_rx: mpsc::Receiver<Value>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>,
    next_id: AtomicU64,
    stderr_tail: Arc<Mutex<String>>,
    writer_task: tokio::task::JoinHandle<()>,
    reader_task: tokio::task::JoinHandle<()>,
}

impl AppServerConnection {
    fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
            && !self.writer_task.is_finished()
            && !self.reader_task.is_finished()
    }

    fn send(&self, value: Value) -> Result<(), String> {
        self.write_tx
            .send(value)
            .map_err(|_| "Codex app-server stdin writer closed".to_string())
    }

    async fn request(&self, method: &str, params: Value) -> Result<Value, String> {
        self.request_with_timeout(method, params, REQUEST_TIMEOUT)
            .await
    }

    async fn request_with_timeout(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        self.pending.lock().await.insert(id, reply_tx);
        if let Err(error) = self.send(json!({"id": id, "method": method, "params": params})) {
            self.pending.lock().await.remove(&id);
            return Err(error);
        }
        let response = match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => {
                return Err(format!(
                    "Codex app-server closed while waiting for {method} response"
                ))
            }
            Err(_) => {
                self.pending.lock().await.remove(&id);
                return Err(format!(
                    "Codex app-server {method} request timed out after {}s",
                    timeout.as_secs()
                ));
            }
        };
        if let Some(error) = response.get("error") {
            return Err(format!(
                "Codex app-server {method} failed: {}",
                value_text(error)
            ));
        }
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    }

    async fn stderr_summary(&self) -> String {
        self.stderr_tail.lock().await.trim().to_string()
    }
}

impl Drop for AppServerConnection {
    fn drop(&mut self) {
        self.writer_task.abort();
        self.reader_task.abort();
        #[cfg(unix)]
        if let Some(pgid) = self.child.id().map(|pid| pid as libc::pid_t) {
            // The executor normally uses the graceful interrupt plus
            // TERM/KILL ladder. Drop is the last-resort worker-shutdown path,
            // so synchronously kill the whole process group rather than
            // allowing an in-flight Codex tool descendant to outlive Bamboo.
            // SAFETY: a negative live child pid targets only its process group.
            let _ = unsafe { libc::kill(-pgid, libc::SIGKILL) };
        }
        let _ = self.child.start_kill();
    }
}

#[derive(Clone, Copy)]
struct AppServerRunPolicy<'a> {
    sandbox: &'a str,
    approval_policy: &'a str,
    network_access: bool,
}

/// `codex app-server` implementation of the Codex executor mode.
pub struct CodexAppServerExecutor {
    binary: PathBuf,
    version: String,
    model: Option<String>,
    permissions: CodexPermissionConfig,
    workspace: Option<String>,
    state_dir: PathBuf,
    forward_env: Vec<String>,
    auth: CodexAuthConfig,
    approval_timeout: Duration,
    run_lock: Mutex<()>,
    connection: Mutex<Option<AppServerConnection>>,
}

struct RunTokenGuard {
    file: std::fs::File,
}

impl Drop for RunTokenGuard {
    fn drop(&mut self) {
        if let Err(error) = self.file.set_len(0) {
            tracing::warn!(%error, "codex app-server: clear per-run provider token");
        }
    }
}

impl CodexAppServerExecutor {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        binary: Option<String>,
        model: Option<String>,
        workspace: Option<String>,
        state_dir: Option<PathBuf>,
        forward_env: Vec<String>,
        auth: CodexAuthConfig,
        permissions: CodexPermissionConfig,
    ) -> Result<Self, String> {
        let discovery = discover_codex_app_server(binary.as_deref()).await?;
        let state_dir = state_dir.ok_or_else(|| {
            "Codex app-server mode requires a Bamboo-managed state directory".to_string()
        })?;
        secure_directory(&state_dir).await?;
        let executor = Self {
            binary: PathBuf::from(discovery.path),
            version: discovery.version,
            model,
            permissions,
            workspace,
            state_dir,
            forward_env,
            auth,
            approval_timeout: APPROVAL_RELAY_TIMEOUT,
            run_lock: Mutex::new(()),
            connection: Mutex::new(None),
        };
        executor.prepare_auth_home().await?;
        Ok(executor)
    }

    fn codex_home(&self) -> Option<PathBuf> {
        self.auth
            .isolated()
            .then(|| self.state_dir.join("codex-app-server-home"))
    }

    fn codex_home_mode(&self) -> &'static str {
        if self.auth.isolated() {
            "isolated"
        } else {
            "inherit"
        }
    }

    fn token_path(&self) -> PathBuf {
        self.state_dir.join(TOKEN_FILE)
    }

    fn session_store_path(&self) -> PathBuf {
        self.state_dir.join(SESSION_STORE_FILE)
    }

    async fn prepare_auth_home(&self) -> Result<(), String> {
        let Some(home) = self.codex_home() else {
            return Ok(());
        };
        secure_directory(&home).await?;
        let auth_path = home.join("auth.json");
        match tokio::fs::remove_file(&auth_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "remove stale isolated Codex auth '{}': {error}",
                    auth_path.display()
                ))
            }
        }
        let helper = std::env::current_exe()
            .map_err(|error| format!("resolve Bamboo token helper executable: {error}"))?;
        let config = self
            .auth
            .generated_app_server_config_toml(&helper, &self.token_path())?;
        let config_path = home.join("config.toml");
        tokio::fs::write(&config_path, config)
            .await
            .map_err(|error| {
                format!(
                    "write isolated Codex app-server config '{}': {error}",
                    config_path.display()
                )
            })?;
        secure_file(&config_path).await?;
        Ok(())
    }

    fn install_run_token(&self, spec: &RunSpec) -> Result<Option<RunTokenGuard>, String> {
        if self.auth.mode() != CodexAuthMode::Bamboo {
            return Ok(None);
        }
        let token = spec
            .secrets
            .codex_provider_token
            .as_ref()
            .map(bamboo_subagent::proto::SecretValue::expose)
            .ok_or_else(|| {
                "Codex bamboo auth requires a fresh per-run provider token".to_string()
            })?;
        let mut guard = RunTokenGuard {
            file: open_secret_for_replace(&self.token_path())?,
        };
        guard
            .file
            .write_all(token.as_bytes())
            .map_err(|error| format!("write Codex app-server provider token: {error}"))?;
        guard
            .file
            .sync_data()
            .map_err(|error| format!("sync Codex app-server provider token: {error}"))?;
        Ok(Some(guard))
    }

    async fn load_session_store(&self) -> AppServerSessionStore {
        let Ok(bytes) = tokio::fs::read(self.session_store_path()).await else {
            return AppServerSessionStore::default();
        };
        serde_json::from_slice(&bytes).unwrap_or_default()
    }

    async fn save_session_store(&self, store: &AppServerSessionStore) {
        if let Err(error) = write_json_atomic(&self.session_store_path(), store).await {
            tracing::warn!(%error, "codex app-server: persist logical session map");
        }
    }

    async fn stored_thread(&self, logical_session: &str) -> Option<String> {
        let store = self.load_session_store().await;
        let state = store.sessions.get(logical_session)?;
        if state.workspace != self.workspace || state.codex_home_mode != self.codex_home_mode() {
            return None;
        }
        (!state.thread_id.trim().is_empty()).then(|| state.thread_id.clone())
    }

    async fn store_thread(&self, logical_session: &str, thread_id: &str) {
        let mut store = self.load_session_store().await;
        store.sessions.insert(
            logical_session.to_string(),
            AppServerSessionState {
                thread_id: thread_id.to_string(),
                workspace: self.workspace.clone(),
                codex_home_mode: self.codex_home_mode().to_string(),
                updated_at: Utc::now(),
            },
        );
        prune_session_store(&mut store, logical_session);
        self.save_session_store(&store).await;
    }

    async fn forget_thread(&self, logical_session: &str) {
        let mut store = self.load_session_store().await;
        if store.sessions.remove(logical_session).is_some() {
            self.save_session_store(&store).await;
        }
    }

    fn build_command(&self) -> Result<Command, String> {
        let mut command = Command::new(&self.binary);
        command.arg("app-server").arg("--listen").arg("stdio://");
        command.env_clear();
        for (key, value) in std::env::vars() {
            if ENV_ALLOWLIST.contains(&key.as_str()) || key.starts_with("LC_") {
                command.env(key, value);
            }
        }
        if let Some(home) = self.codex_home() {
            command.env("CODEX_HOME", home);
        }
        for name in &self.forward_env {
            if let Ok(value) = std::env::var(name) {
                command.env(name, value);
            }
        }
        if self.auth.mode() == CodexAuthMode::Custom {
            let key = self.auth.provider_key().ok_or_else(|| {
                "Codex custom provider key was not resolved at provisioning".to_string()
            })?;
            command.env(CODEX_PROVIDER_ENV, key);
        }
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
        command.kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        Ok(command)
    }

    async fn start_connection(&self) -> Result<AppServerConnection, String> {
        self.prepare_auth_home().await?;
        let mut child = self.build_command()?.spawn().map_err(|error| {
            format!(
                "spawn Codex app-server '{}': {error}",
                self.binary.display()
            )
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "Codex app-server has no stdin pipe".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "Codex app-server has no stdout pipe".to_string())?;
        let stderr = child.stderr.take();

        let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Value>();
        let writer_task = tokio::spawn(async move {
            let mut stdin = stdin;
            while let Some(value) = write_rx.recv().await {
                let Ok(mut bytes) = serde_json::to_vec(&value) else {
                    continue;
                };
                bytes.push(b'\n');
                if stdin.write_all(&bytes).await.is_err() || stdin.flush().await.is_err() {
                    break;
                }
            }
        });

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        // Match the proven cc-connect posture: bounded event buffering applies
        // backpressure instead of allowing a noisy app-server to grow memory
        // without limit. Client responses bypass this queue via `pending`.
        let (incoming_tx, incoming_rx) = mpsc::channel(128);
        let reader_pending = pending.clone();
        let reader_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            loop {
                let line = match read_bounded_line(&mut reader, MAX_STDOUT_LINE_BYTES).await {
                    Ok(Some(line)) => line,
                    Ok(None) => break,
                    Err(error) => {
                        let _ = incoming_tx
                            .send(json!({
                                "method": "bamboo/transport/error",
                                "params": {"message": error.to_string()}
                            }))
                            .await;
                        break;
                    }
                };
                let value: Value = match serde_json::from_slice(&line) {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = incoming_tx
                            .send(json!({
                                "method": "bamboo/transport/error",
                                "params": {"message": format!("invalid JSONL: {error}")}
                            }))
                            .await;
                        break;
                    }
                };
                if value.get("method").is_none() {
                    if let Some(id) = value.get("id").and_then(Value::as_u64) {
                        if let Some(reply) = reader_pending.lock().await.remove(&id) {
                            let _ = reply.send(value);
                            continue;
                        }
                    }
                }
                if incoming_tx.send(value).await.is_err() {
                    break;
                }
            }
            reader_pending.lock().await.clear();
        });

        let stderr_tail = Arc::new(Mutex::new(String::new()));
        if let Some(stderr) = stderr {
            let tail = stderr_tail.clone();
            tokio::spawn(async move { drain_stderr_tail(stderr, tail).await });
        }
        let connection = AppServerConnection {
            child,
            write_tx,
            incoming_rx,
            pending,
            next_id: AtomicU64::new(1),
            stderr_tail,
            writer_task,
            reader_task,
        };
        connection
            .request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "bamboo",
                        "title": "Bamboo",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {"experimentalApi": true}
                }),
            )
            .await?;
        connection.send(json!({"method": "initialized", "params": {}}))?;
        Ok(connection)
    }

    async fn ensure_connection<'a>(
        &'a self,
        slot: &'a mut Option<AppServerConnection>,
    ) -> Result<&'a mut AppServerConnection, String> {
        let alive = slot.as_mut().is_some_and(AppServerConnection::is_alive);
        if !alive {
            if let Some(mut stale) = slot.take() {
                terminate_child(&mut stale.child).await;
            }
            *slot = Some(self.start_connection().await?);
        }
        Ok(slot.as_mut().expect("connection installed"))
    }

    fn thread_params(&self, policy: AppServerRunPolicy<'_>) -> Value {
        let mut params = json!({
            "approvalPolicy": policy.approval_policy,
            "approvalsReviewer": (policy.approval_policy != "never").then_some("user"),
            "sandbox": policy.sandbox,
            "cwd": self.workspace,
            "model": self.model,
        });
        remove_null_object_fields(&mut params);
        params
    }

    async fn start_thread(
        &self,
        connection: &AppServerConnection,
        policy: AppServerRunPolicy<'_>,
    ) -> Result<String, String> {
        let result = connection
            .request("thread/start", self.thread_params(policy))
            .await?;
        result
            .pointer("/thread/id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_string)
            .ok_or_else(|| "Codex thread/start response omitted thread.id".to_string())
    }

    async fn resume_thread(
        &self,
        connection: &AppServerConnection,
        thread_id: &str,
        policy: AppServerRunPolicy<'_>,
    ) -> Result<(), String> {
        let mut params = self.thread_params(policy);
        params["threadId"] = Value::String(thread_id.to_string());
        connection
            .request("thread/resume", params)
            .await
            .map(|_| ())
    }

    async fn start_turn(
        &self,
        connection: &AppServerConnection,
        thread_id: &str,
        prompt: &str,
        reasoning_effort: Option<&str>,
        policy: AppServerRunPolicy<'_>,
    ) -> Result<String, String> {
        let mut params = json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": prompt, "text_elements": []}],
            "approvalPolicy": policy.approval_policy,
            "approvalsReviewer": (policy.approval_policy != "never").then_some("user"),
            "cwd": self.workspace,
            "model": self.model,
            "effort": reasoning_effort,
            "sandboxPolicy": sandbox_policy(
                policy.sandbox,
                self.workspace.as_deref(),
                policy.network_access,
            ),
        });
        remove_null_object_fields(&mut params);
        let result = connection.request("turn/start", params).await?;
        result
            .pointer("/turn/id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_string)
            .ok_or_else(|| "Codex turn/start response omitted turn.id".to_string())
    }

    #[allow(clippy::too_many_arguments)]
    async fn drive_turn(
        &self,
        connection: &mut AppServerConnection,
        thread_id: &str,
        turn_id: &str,
        events: &EventSink,
        steer: &mut SteerInbox,
        approval_tasks: &mut Vec<tokio::task::JoinHandle<()>>,
        force_cancelled: bool,
        auto_approve_permissions: bool,
    ) -> ChildOutcome {
        let mut state = AppRunState::default();
        let mut steer_open = true;
        loop {
            tokio::select! {
                maybe_message = steer.recv(), if steer_open => {
                    if let Some(message) = maybe_message {
                        if let Err(error) = connection.request("turn/steer", json!({
                            "threadId": thread_id,
                            "expectedTurnId": turn_id,
                            "input": [{"type": "text", "text": message, "text_elements": []}],
                        })).await {
                            events.emit(json!({
                                "type": "runner_progress",
                                "session_id": thread_id,
                                "round_count": 1,
                                "executor": "codex_app_server",
                                "phase": "steer_rejected",
                                "message": error,
                            }));
                        }
                    } else {
                        steer_open = false;
                    }
                }
                incoming = connection.incoming_rx.recv() => {
                    let Some(message) = incoming else {
                        let stderr = connection.stderr_summary().await;
                        let suffix = if stderr.is_empty() { String::new() } else { format!("; stderr: {stderr}") };
                        return ChildOutcome::error(format!("Codex app-server transport closed{suffix}"));
                    };
                    let method = message.get("method").and_then(Value::as_str).unwrap_or("");
                    if message.get("id").is_some() {
                        if is_approval_method(method) {
                            if !approval_matches_active_turn(&message, thread_id, turn_id) {
                                let _ = connection.send(approval_response(&message, false));
                                continue;
                            }
                            if auto_approve_permissions {
                                let _ = connection.send(approval_response(&message, true));
                            } else {
                                approval_tasks.push(spawn_approval_relay(
                                    connection.write_tx.clone(),
                                    message,
                                    events.host().cloned(),
                                    self.approval_timeout,
                                ));
                            }
                        } else {
                            let id = message.get("id").cloned().unwrap_or(Value::Null);
                            let _ = connection.send(json!({
                                "id": id,
                                "error": {"code": -32601, "message": format!("unsupported server request: {method}")}
                            }));
                        }
                        continue;
                    }
                    if method == "bamboo/transport/error" {
                        return ChildOutcome::error(error_message(&message, "Codex app-server transport error"));
                    }
                    if let Some(outcome) = handle_notification(
                        method,
                        message.get("params").unwrap_or(&Value::Null),
                        thread_id,
                        turn_id,
                        events,
                        &mut state,
                        force_cancelled,
                    ) {
                        return outcome;
                    }
                }
            }
        }
    }

    async fn run_inner(
        &self,
        spec: &RunSpec,
        events: &EventSink,
        steer: &mut SteerInbox,
        cancel: &CancellationToken,
    ) -> ChildOutcome {
        let logical_session = match spec
            .permission_policy
            .as_ref()
            .map(|policy| policy.session_id.trim())
            .filter(|id| !id.is_empty())
        {
            Some(id) => id.to_string(),
            None => return ChildOutcome::error(
                "Codex app-server mode requires a logical session id in RunSpec.permission_policy",
            ),
        };
        let parent_bypass = spec
            .permission_policy
            .as_ref()
            .map(|policy| policy.bypass_permissions)
            .unwrap_or(self.permissions.provisioned_bypass());
        let auto_approve_permissions = spec
            .permission_policy
            .as_ref()
            .is_some_and(|policy| policy.auto_approve_permissions);
        let approval_policy = if auto_approve_permissions {
            "never"
        } else {
            "on-request"
        };
        let requested_mode = if auto_approve_permissions {
            "auto"
        } else if parent_bypass {
            "bypass"
        } else {
            "default"
        };
        let executor_mapping = format!("codex_app_server:approvalPolicy={approval_policy}");
        let (sandbox, network_access, warnings) =
            self.permissions.app_server_posture(parent_bypass);
        let run_policy = AppServerRunPolicy {
            sandbox: &sandbox,
            approval_policy,
            network_access,
        };
        for warning in warnings {
            events.emit(json!({
                "type": "runner_progress",
                "session_id": logical_session,
                "round_count": 0,
                "level": "warning",
                "message": warning,
            }));
        }
        events.emit(json!({
            "type": "runner_progress",
            "session_id": logical_session,
            "round_count": 0,
            "executor": "codex_app_server",
            "binary": self.binary,
            "version": self.version,
            "model": self.model,
            "auth_mode": self.auth.mode().as_str(),
            "codex_home_mode": self.codex_home_mode(),
            "sandbox": sandbox,
            "approval_policy": approval_policy,
            "approvals_reviewer": (!auto_approve_permissions).then_some("user"),
            "network_access": network_access,
            "permission_profile": self.permissions.permission_profile(),
            "requested_mode": requested_mode,
            "effective_mode": requested_mode,
            "executor_mapping": executor_mapping,
        }));

        if spec.messages.is_empty() {
            self.forget_thread(&logical_session).await;
        }
        let mut slot = self.connection.lock().await;
        let connection = match self.ensure_connection(&mut slot).await {
            Ok(connection) => connection,
            Err(error) => return ChildOutcome::error(error),
        };

        while connection.incoming_rx.try_recv().is_ok() {}
        let stored = if spec.messages.is_empty() {
            None
        } else {
            self.stored_thread(&logical_session).await
        };
        let (thread_id, prompt) = if let Some(thread_id) = stored {
            match self.resume_thread(connection, &thread_id, run_policy).await {
                Ok(()) => (thread_id, spec.assignment.clone()),
                Err(error) => {
                    tracing::warn!(%error, "codex app-server: resume failed; rehydrating once");
                    events.emit(json!({
                        "type": "runner_progress",
                        "session_id": logical_session,
                        "round_count": 0,
                        "executor": "codex_app_server",
                        "phase": "resume_fallback",
                        "message": "resume failed; starting a new thread with bounded history rehydration",
                    }));
                    self.forget_thread(&logical_session).await;
                    let new_id = match self.start_thread(connection, run_policy).await {
                        Ok(id) => id,
                        Err(error) => return ChildOutcome::error(error),
                    };
                    (
                        new_id,
                        build_rehydrated_turn(&spec.messages, &spec.assignment),
                    )
                }
            }
        } else {
            let thread_id = match self.start_thread(connection, run_policy).await {
                Ok(id) => id,
                Err(error) => return ChildOutcome::error(error),
            };
            let prompt = if spec.messages.is_empty() {
                spec.assignment.clone()
            } else {
                build_rehydrated_turn(&spec.messages, &spec.assignment)
            };
            (thread_id, prompt)
        };
        self.store_thread(&logical_session, &thread_id).await;

        let turn_id = match self
            .start_turn(
                connection,
                &thread_id,
                &prompt,
                spec.reasoning_effort.as_deref(),
                run_policy,
            )
            .await
        {
            Ok(id) => id,
            Err(error) => return ChildOutcome::error(error),
        };
        events.emit(event_json(AgentEvent::RunnerProgress {
            session_id: thread_id.clone(),
            round_count: 1,
        }));
        let mut approval_tasks = Vec::new();
        let outcome = tokio::select! {
            outcome = self.drive_turn(
                connection,
                &thread_id,
                &turn_id,
                events,
                steer,
                &mut approval_tasks,
                false,
                auto_approve_permissions,
            ) => outcome,
            _ = cancel.cancelled() => {
                let interrupt = connection.request_with_timeout(
                    "turn/interrupt",
                    json!({"threadId": thread_id, "turnId": turn_id}),
                    INTERRUPT_GRACE,
                ).await;
                if let Err(error) = interrupt {
                    tracing::warn!(%error, "codex app-server: graceful interrupt failed");
                }
                match tokio::time::timeout(
                    INTERRUPT_GRACE,
                    self.drive_turn(
                        connection,
                        &thread_id,
                        &turn_id,
                        events,
                        steer,
                        &mut approval_tasks,
                        true,
                        auto_approve_permissions,
                    ),
                ).await {
                    Ok(_) => ChildOutcome::cancelled(),
                    Err(_) => {
                        if let Some(mut connection) = slot.take() {
                            terminate_child(&mut connection.child).await;
                        }
                        ChildOutcome::cancelled()
                    }
                }
            }
        };
        for task in approval_tasks {
            task.abort();
        }
        outcome
    }
}

#[async_trait]
impl ChildExecutor for CodexAppServerExecutor {
    async fn run(
        &self,
        spec: RunSpec,
        events: EventSink,
        mut steer: SteerInbox,
        cancel: CancellationToken,
    ) -> ChildOutcome {
        // A warm executor owns one long-lived app-server and one refreshable
        // token file. Serialize activations so a queued run cannot overwrite
        // or clear the token still in use by the active run.
        let _run_guard = self.run_lock.lock().await;
        let _token_guard = match self.install_run_token(&spec) {
            Ok(guard) => guard,
            Err(error) => return ChildOutcome::error(error),
        };
        let outcome = self.run_inner(&spec, &events, &mut steer, &cancel).await;
        outcome
    }
}

#[derive(Default)]
struct AppRunState {
    last_agent_message: String,
    last_agent_item_id: Option<String>,
    usage: TokenUsage,
    started_items: HashSet<String>,
}

fn handle_notification(
    method: &str,
    params: &Value,
    thread_id: &str,
    turn_id: &str,
    events: &EventSink,
    state: &mut AppRunState,
    force_cancelled: bool,
) -> Option<ChildOutcome> {
    if params
        .get("threadId")
        .and_then(Value::as_str)
        .is_some_and(|id| id != thread_id)
        || params
            .get("turnId")
            .and_then(Value::as_str)
            .is_some_and(|id| id != turn_id)
    {
        return None;
    }
    match method {
        "item/agentMessage/delta" => {
            if let Some(delta) = params.get("delta").and_then(Value::as_str) {
                let item_id = params
                    .get("itemId")
                    .and_then(Value::as_str)
                    .unwrap_or("codex-agent-message");
                if state.last_agent_item_id.as_deref() != Some(item_id) {
                    state.last_agent_item_id = Some(item_id.to_string());
                    state.last_agent_message.clear();
                }
                state.last_agent_message.push_str(delta);
                events.emit(event_json(AgentEvent::Token {
                    content: delta.to_string(),
                }));
            }
        }
        "item/reasoning/textDelta" | "item/reasoning/summaryTextDelta" => {
            if let Some(delta) = params.get("delta").and_then(Value::as_str) {
                events.emit(event_json(AgentEvent::ReasoningToken {
                    content: delta.to_string(),
                }));
            }
        }
        "item/commandExecution/outputDelta" | "item/fileChange/outputDelta" => {
            if let (Some(item_id), Some(delta)) = (
                params.get("itemId").and_then(Value::as_str),
                params.get("delta").and_then(Value::as_str),
            ) {
                events.emit(event_json(AgentEvent::ToolToken {
                    tool_call_id: item_id.to_string(),
                    content: delta.to_string(),
                }));
            }
        }
        "item/mcpToolCall/progress" => {
            if let (Some(item_id), Some(message)) = (
                params.get("itemId").and_then(Value::as_str),
                params.get("message").and_then(Value::as_str),
            ) {
                events.emit(event_json(AgentEvent::ToolToken {
                    tool_call_id: item_id.to_string(),
                    content: message.to_string(),
                }));
            }
        }
        "item/started" => {
            if let Some(item) = params.get("item") {
                emit_item_started(item, events, state);
            }
        }
        "item/completed" => {
            if let Some(item) = params.get("item") {
                emit_item_completed(item, events, state);
            }
        }
        "thread/tokenUsage/updated" => {
            state.usage = parse_app_server_usage(params.get("tokenUsage"));
        }
        "turn/completed" => {
            if force_cancelled {
                events.emit(event_json(AgentEvent::Cancelled {
                    message: Some("Codex app-server turn interrupted".to_string()),
                }));
                return Some(ChildOutcome::cancelled());
            }
            let turn = params.get("turn").unwrap_or(&Value::Null);
            match turn.get("status").and_then(Value::as_str) {
                Some("completed") => {
                    events.emit(event_json(AgentEvent::Complete { usage: state.usage }));
                    return Some(ChildOutcome::completed(state.last_agent_message.clone()));
                }
                Some("interrupted") => {
                    events.emit(event_json(AgentEvent::Cancelled {
                        message: Some("Codex app-server turn interrupted".to_string()),
                    }));
                    return Some(ChildOutcome::cancelled());
                }
                _ => {
                    let message = error_message(turn, "Codex app-server turn failed");
                    events.emit(event_json(AgentEvent::Error {
                        message: message.clone(),
                    }));
                    return Some(ChildOutcome::error(message));
                }
            }
        }
        "error" => {
            let message = error_message(params, "Codex app-server error");
            events.emit(event_json(AgentEvent::Error {
                message: message.clone(),
            }));
            return Some(ChildOutcome::error(message));
        }
        other => tracing::debug!(
            method = other,
            "codex app-server: unrecognized notification"
        ),
    }
    None
}

fn emit_item_started(item: &Value, events: &EventSink, state: &mut AppRunState) {
    let item_id = item
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("codex-item");
    if !state.started_items.insert(item_id.to_string()) {
        return;
    }
    let (tool_name, arguments) = match item.get("type").and_then(Value::as_str) {
        Some("commandExecution") => (
            "Bash".to_string(),
            json!({"command": item.get("command"), "cwd": item.get("cwd")}),
        ),
        Some("fileChange") => (
            "ApplyPatch".to_string(),
            json!({"changes": item.get("changes")}),
        ),
        Some("mcpToolCall") => (
            format!(
                "{}::{}",
                item.get("server").and_then(Value::as_str).unwrap_or("mcp"),
                item.get("tool").and_then(Value::as_str).unwrap_or("tool")
            ),
            item.get("arguments").cloned().unwrap_or_else(|| json!({})),
        ),
        Some("dynamicToolCall") => (
            item.get("tool")
                .or_else(|| item.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("DynamicTool")
                .to_string(),
            item.get("arguments").cloned().unwrap_or_else(|| json!({})),
        ),
        Some("webSearch") => ("WebSearch".to_string(), json!({"query": item.get("query")})),
        _ => return,
    };
    events.emit(event_json(AgentEvent::ToolStart {
        tool_call_id: item_id.to_string(),
        tool_name,
        arguments,
    }));
}

fn emit_item_completed(item: &Value, events: &EventSink, state: &mut AppRunState) {
    if item.get("type").and_then(Value::as_str) == Some("agentMessage") {
        if let Some(text) = item.get("text").and_then(Value::as_str) {
            state.last_agent_item_id = item.get("id").and_then(Value::as_str).map(str::to_string);
            state.last_agent_message = text.to_string();
        }
        return;
    }
    emit_item_started(item, events, state);
    let item_id = item
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("codex-item");
    if !state.started_items.contains(item_id) {
        return;
    }
    let status = item.get("status").and_then(Value::as_str).unwrap_or("");
    let error = item
        .get("error")
        .filter(|value| !value.is_null())
        .map(value_text)
        .or_else(|| {
            matches!(status, "failed" | "declined" | "error").then(|| {
                item.get("aggregatedOutput")
                    .map(value_text)
                    .unwrap_or_else(|| format!("Codex tool finished with status {status}"))
            })
        });
    if let Some(error) = error {
        events.emit(event_json(AgentEvent::ToolError {
            tool_call_id: item_id.to_string(),
            error: truncate_chars(&error, TOOL_RESULT_TRUNCATE_CHARS),
        }));
    } else {
        let result = item
            .get("aggregatedOutput")
            .or_else(|| item.get("result"))
            .or_else(|| item.get("changes"))
            .map(value_text)
            .unwrap_or_else(|| status.to_string());
        events.emit(event_json(AgentEvent::ToolComplete {
            tool_call_id: item_id.to_string(),
            result: ToolResult::text(true, truncate_chars(&result, TOOL_RESULT_TRUNCATE_CHARS)),
        }));
    }
}

fn is_approval_method(method: &str) -> bool {
    matches!(
        method,
        "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "execCommandApproval"
            | "applyPatchApproval"
    )
}

fn spawn_approval_relay(
    write_tx: mpsc::UnboundedSender<Value>,
    request: Value,
    host: Option<HostBridge>,
    timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
        let is_command = method.contains("commandExecution") || method == "execCommandApproval";
        let body = if is_command {
            json!({
                "tool_name": "Bash",
                "permission_type": "command_execution",
                "resource": params.get("command").cloned().unwrap_or(Value::Null),
                "question": params.get("reason").cloned().unwrap_or_else(|| Value::String("Codex requests permission to execute a command".to_string())),
                "input": params,
            })
        } else {
            json!({
                "tool_name": "ApplyPatch",
                "permission_type": "file_change",
                "resource": params.get("grantRoot").or_else(|| params.get("path")).cloned().unwrap_or(Value::Null),
                "question": params.get("reason").cloned().unwrap_or_else(|| Value::String("Codex requests permission to modify files".to_string())),
                "input": params,
            })
        };
        let approved = if let Some(host) = host {
            match tokio::time::timeout(timeout, host.approval_call(body)).await {
                Ok(Ok(reply)) => reply
                    .get("approved")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                Ok(Err(error)) => {
                    tracing::warn!(%error, "codex app-server: approval relay failed closed");
                    false
                }
                Err(_) => {
                    tracing::warn!(
                        seconds = timeout.as_secs(),
                        "codex app-server: approval relay timed out; denying"
                    );
                    false
                }
            }
        } else {
            tracing::warn!("codex app-server: approval host bridge unavailable; denying");
            false
        };
        let _ = write_tx.send(approval_response_parts(id, &method, approved));
    })
}

fn approval_matches_active_turn(request: &Value, thread_id: &str, turn_id: &str) -> bool {
    let params = request.get("params").unwrap_or(&Value::Null);
    !params
        .get("threadId")
        .and_then(Value::as_str)
        .is_some_and(|id| id != thread_id)
        && !params
            .get("turnId")
            .and_then(Value::as_str)
            .is_some_and(|id| id != turn_id)
}

fn approval_response(request: &Value, approved: bool) -> Value {
    approval_response_parts(
        request.get("id").cloned().unwrap_or(Value::Null),
        request.get("method").and_then(Value::as_str).unwrap_or(""),
        approved,
    )
}

fn approval_response_parts(id: Value, method: &str, approved: bool) -> Value {
    let decision = if matches!(method, "execCommandApproval" | "applyPatchApproval") {
        if approved {
            "approved"
        } else {
            "denied"
        }
    } else if approved {
        "accept"
    } else {
        "decline"
    };
    json!({"id": id, "result": {"decision": decision}})
}

fn sandbox_policy(sandbox: &str, workspace: Option<&str>, network_access: bool) -> Value {
    match sandbox {
        "read-only" => json!({"type": "readOnly", "networkAccess": false}),
        "danger-full-access" => json!({"type": "dangerFullAccess"}),
        _ => json!({
            "type": "workspaceWrite",
            "writableRoots": workspace.into_iter().collect::<Vec<_>>(),
            "networkAccess": network_access,
        }),
    }
}

fn parse_app_server_usage(value: Option<&Value>) -> TokenUsage {
    let usage = value
        .and_then(|value| value.get("last"))
        .unwrap_or(&Value::Null);
    TokenUsage {
        prompt_tokens: usage
            .get("inputTokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        completion_tokens: usage
            .get("outputTokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        total_tokens: usage
            .get("totalTokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    }
}

fn event_json(event: AgentEvent) -> Value {
    serde_json::to_value(event)
        .unwrap_or_else(|_| json!({"type": "error", "message": "serialize agent event"}))
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
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        value => value.to_string(),
    }
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let mut output: String = value.chars().take(max_chars).collect();
    output.push_str("\n… truncated by Bamboo …");
    output
}

fn remove_null_object_fields(value: &mut Value) {
    if let Some(object) = value.as_object_mut() {
        object.retain(|_, value| !value.is_null());
    }
}

fn prune_session_store(store: &mut AppServerSessionStore, current_session: &str) {
    while store.sessions.len() > MAX_LOGICAL_SESSIONS {
        let oldest = store
            .sessions
            .iter()
            .filter(|(session, _)| session.as_str() != current_session)
            .min_by_key(|(_, state)| state.updated_at)
            .map(|(session, _)| session.clone());
        let Some(oldest) = oldest else {
            break;
        };
        store.sessions.remove(&oldest);
    }
}

fn open_secret_for_replace(path: &Path) -> Result<std::fs::File, String> {
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options
        .open(path)
        .map_err(|error| format!("open Codex app-server provider token: {error}"))?;
    if !file
        .metadata()
        .map_err(|error| format!("inspect Codex app-server provider token: {error}"))?
        .is_file()
    {
        return Err("Codex app-server provider token path is not a regular file".to_string());
    }
    #[cfg(unix)]
    file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o600))
        .map_err(|error| format!("secure Codex app-server provider token: {error}"))?;
    Ok(file)
}

async fn secure_directory(path: &Path) -> Result<(), String> {
    tokio::fs::create_dir_all(path).await.map_err(|error| {
        format!(
            "create Codex app-server state '{}': {error}",
            path.display()
        )
    })?;
    #[cfg(unix)]
    tokio::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o700))
        .await
        .map_err(|error| {
            format!(
                "secure Codex app-server state '{}': {error}",
                path.display()
            )
        })?;
    Ok(())
}

async fn secure_file(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    tokio::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o600))
        .await
        .map_err(|error| format!("secure Codex app-server file '{}': {error}", path.display()))?;
    Ok(())
}

async fn drain_stderr_tail(stderr: tokio::process::ChildStderr, tail: Arc<Mutex<String>>) {
    use tokio::io::AsyncBufReadExt;
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
    use crate::codex_cli_executor::{
        resolve_codex_app_server_permission_config, resolve_codex_auth_config,
    };
    use bamboo_subagent::executor::HostBridge;
    use bamboo_subagent::proto::{PermissionPolicyContext, RunSecrets, SecretValue};

    #[cfg(unix)]
    fn write_stub_codex(path: &Path) {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::write(
            path,
            r###"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo 'codex-cli 0.144.5'
  exit 0
fi
if [ "$1" = "exec" ]; then
  echo '--json --output-last-message --config --sandbox --dangerously-bypass-approvals-and-sandbox stdin'
  exit 0
fi
if [ "$1" = "app-server" ] && [ "$2" = "--help" ]; then
  echo '--listen stdio:// --stdio'
  exit 0
fi
if [ "$1" != "app-server" ]; then
  exit 2
fi
IFS= read -r initialize
echo '{"id":1,"result":{"userAgent":"stub/0.144.5"}}'
IFS= read -r initialized
IFS= read -r thread_start
echo '{"id":2,"result":{"thread":{"id":"thread-stub"}}}'
IFS= read -r turn_start
echo '{"id":3,"result":{"turn":{"id":"turn-stub","status":"inProgress","items":[]}}}'
echo '{"id":41,"method":"item/commandExecution/requestApproval","params":{"threadId":"thread-stub","turnId":"turn-stub","itemId":"item-1","command":"touch marker","cwd":"/tmp","reason":"stub command","startedAtMs":1}}'
IFS= read -r approval
case "$approval" in
  *'"decision":"accept"'*) text='stub approved' ;;
  *) text='stub denied' ;;
esac
printf '{"method":"item/agentMessage/delta","params":{"threadId":"thread-stub","turnId":"turn-stub","itemId":"item-2","delta":"%s"}}\n' "$text"
echo '{"method":"turn/completed","params":{"threadId":"thread-stub","turn":{"id":"turn-stub","status":"completed","items":[]}}}'
while IFS= read -r ignored; do :; done
"###,
        )
        .unwrap();
        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).unwrap();
    }

    #[cfg(unix)]
    async fn run_stub(
        approved: Option<bool>,
        auto_approve_permissions: bool,
    ) -> (ChildOutcome, Vec<Value>) {
        let root = tempfile::tempdir().unwrap();
        let binary = root.path().join("codex-stub.sh");
        write_stub_codex(&binary);
        let permissions = resolve_codex_app_server_permission_config(
            Some("workspace-write"),
            Some("on-request"),
            false,
            false,
            None,
            false,
            false,
        )
        .unwrap();
        let executor = CodexAppServerExecutor::new(
            Some(binary.to_string_lossy().into_owned()),
            None,
            Some(root.path().to_string_lossy().into_owned()),
            Some(root.path().join("state")),
            Vec::new(),
            CodexAuthConfig::inherit(),
            permissions,
        )
        .await
        .unwrap();
        let (sink, mut event_rx) = EventSink::channel();
        let (sink, approval_task) = if let Some(approved) = approved {
            let (host, mut host_rx) = HostBridge::channel();
            let task = tokio::spawn(async move {
                let request = host_rx.recv().await.expect("approval request");
                assert_eq!(request.body["tool_name"], "Bash");
                request.reply.send(json!({"approved": approved})).unwrap();
            });
            (sink.with_host_bridge(host), Some(task))
        } else {
            (sink, None)
        };
        let outcome = executor
            .run(
                RunSpec {
                    assignment: "exercise approval".to_string(),
                    logical_session: None,
                    project_id: None,
                    reasoning_effort: None,
                    permission_policy: Some(PermissionPolicyContext {
                        revision: 1,
                        bypass_permissions: false,
                        auto_approve_permissions,
                        session_id: format!("stub-{approved:?}-{auto_approve_permissions}"),
                        workspace_path: Some(root.path().to_string_lossy().into_owned()),
                        inherit_session_grants: false,
                        policy: json!({}),
                    }),
                    messages: Vec::new(),
                    activation_run_id: None,
                    initial_session_messages: Vec::new(),
                    secrets: RunSecrets::default(),
                },
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        if let Some(task) = approval_task {
            task.await.unwrap();
        }
        let mut events = Vec::new();
        while let Ok(event) = event_rx.try_recv() {
            events.push(event);
        }
        (outcome, events)
    }

    #[test]
    fn recorded_fixture_covers_handshake_and_approval_round_trip() {
        let rows =
            include_str!("../tests/fixtures/codex-app-server/0.144.5-handshake-approval.jsonl")
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).expect("valid JSONL row"))
                .collect::<Vec<_>>();
        assert_eq!(rows[0]["message"]["method"], "initialize");
        assert!(rows.iter().any(|row| {
            row["direction"] == "client" && row["message"]["method"] == "initialized"
        }));
        assert!(rows.iter().any(|row| {
            row["direction"] == "client" && row["message"]["method"] == "thread/start"
        }));
        let approval = rows
            .iter()
            .find(|row| row["message"]["method"] == "item/fileChange/requestApproval")
            .expect("approval request");
        let approval_id = approval["message"]["id"].clone();
        assert!(rows.iter().any(|row| {
            row["direction"] == "client"
                && row["message"]["id"] == approval_id
                && row["message"]["result"]["decision"] == "accept"
        }));
        assert_eq!(rows.last().unwrap()["message"]["method"], "turn/completed");
    }

    #[tokio::test]
    async fn current_command_approval_relays_allow_to_accept() {
        let (write_tx, mut write_rx) = mpsc::unbounded_channel();
        let (host, mut host_rx) = HostBridge::channel();
        let task = spawn_approval_relay(
            write_tx,
            json!({
                "id": 9,
                "method": "item/commandExecution/requestApproval",
                "params": {"command": "touch marker", "cwd": "/tmp", "reason": "write marker"}
            }),
            Some(host),
            Duration::from_secs(1),
        );
        let request = host_rx.recv().await.expect("host approval request");
        assert_eq!(request.body["tool_name"], "Bash");
        assert_eq!(request.body["resource"], "touch marker");
        request
            .reply
            .send(json!({"approved": true}))
            .expect("host reply accepted");
        task.await.unwrap();
        let response = write_rx.recv().await.expect("app-server response");
        assert_eq!(response, json!({"id": 9, "result": {"decision": "accept"}}));
    }

    #[tokio::test]
    async fn approval_timeout_fails_closed_to_decline() {
        let (write_tx, mut write_rx) = mpsc::unbounded_channel();
        let (host, mut host_rx) = HostBridge::channel();
        let task = spawn_approval_relay(
            write_tx,
            json!({
                "id": "approval-10",
                "method": "item/fileChange/requestApproval",
                "params": {"grantRoot": "/tmp/workspace"}
            }),
            Some(host),
            Duration::from_millis(10),
        );
        let held_request = host_rx.recv().await.expect("host approval request");
        task.await.unwrap();
        drop(held_request);
        let response = write_rx.recv().await.expect("app-server response");
        assert_eq!(
            response,
            json!({"id": "approval-10", "result": {"decision": "decline"}})
        );
    }

    #[tokio::test]
    async fn legacy_apply_patch_denial_uses_legacy_decision_shape() {
        let (write_tx, mut write_rx) = mpsc::unbounded_channel();
        let task = spawn_approval_relay(
            write_tx,
            json!({"id": 11, "method": "applyPatchApproval", "params": {}}),
            None,
            Duration::from_secs(1),
        );
        task.await.unwrap();
        assert_eq!(
            write_rx.recv().await.unwrap(),
            json!({"id": 11, "result": {"decision": "denied"}})
        );
    }

    #[test]
    fn approval_from_another_loaded_thread_is_denied_without_relay() {
        let request = json!({
            "id": 12,
            "method": "item/commandExecution/requestApproval",
            "params": {"threadId": "other", "turnId": "turn-stub"}
        });
        assert!(!approval_matches_active_turn(
            &request,
            "thread-stub",
            "turn-stub"
        ));
        assert_eq!(
            approval_response(&request, false),
            json!({"id": 12, "result": {"decision": "decline"}})
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_stub_completes_full_handshake_and_allow_path() {
        let (outcome, events) = run_stub(Some(true), false).await;
        assert_eq!(outcome.result.as_deref(), Some("stub approved"));
        assert!(events.iter().any(|event| event["type"] == "complete"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_stub_returns_denial_to_model_and_completes() {
        let (outcome, events) = run_stub(Some(false), false).await;
        assert_eq!(outcome.result.as_deref(), Some("stub denied"));
        assert!(events
            .iter()
            .any(|event| event["type"] == "token" && event["content"] == "stub denied"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn auto_uses_never_policy_and_accepts_unexpected_approval_without_host() {
        let (outcome, events) = run_stub(None, true).await;
        assert_eq!(outcome.result.as_deref(), Some("stub approved"));
        assert!(events.iter().any(|event| {
            event["executor"] == "codex_app_server"
                && event["approval_policy"] == "never"
                && event["approvals_reviewer"].is_null()
                && event["requested_mode"] == "auto"
                && event["effective_mode"] == "auto"
                && event["executor_mapping"] == "codex_app_server:approvalPolicy=never"
        }));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bamboo_auth_token_file_is_per_run_secret_and_cleared() {
        let root = tempfile::tempdir().unwrap();
        let binary = root.path().join("codex-stub.sh");
        write_stub_codex(&binary);
        let auth = resolve_codex_auth_config(
            Some("bamboo"),
            false,
            Some("http://127.0.0.1:9562/openai/v1".to_string()),
            Some("responses".to_string()),
            None,
            &[],
            &[],
        )
        .unwrap();
        let permissions = resolve_codex_app_server_permission_config(
            Some("workspace-write"),
            Some("on-request"),
            false,
            false,
            None,
            false,
            false,
        )
        .unwrap();
        let executor = CodexAppServerExecutor::new(
            Some(binary.to_string_lossy().into_owned()),
            None,
            Some(root.path().to_string_lossy().into_owned()),
            Some(root.path().join("state")),
            Vec::new(),
            auth,
            permissions,
        )
        .await
        .unwrap();
        let spec = RunSpec {
            assignment: "token lifecycle".to_string(),
            logical_session: None,
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: Vec::new(),
            activation_run_id: None,
            initial_session_messages: Vec::new(),
            secrets: RunSecrets {
                codex_provider_token: Some(SecretValue::new("bcx1_app_server_secret")),
            },
        };
        let token_guard = executor
            .install_run_token(&spec)
            .unwrap()
            .expect("bamboo auth installs a token guard");
        assert_eq!(
            tokio::fs::read_to_string(executor.token_path())
                .await
                .unwrap(),
            "bcx1_app_server_secret"
        );
        let config = tokio::fs::read_to_string(
            executor
                .codex_home()
                .expect("isolated home")
                .join("config.toml"),
        )
        .await
        .unwrap();
        assert!(config.contains("codex-provider-token"));
        assert!(!config.contains("bcx1_app_server_secret"));
        drop(token_guard);
        assert!(tokio::fs::read(executor.token_path())
            .await
            .unwrap()
            .is_empty());
    }

    #[test]
    fn logical_session_store_is_bounded_and_preserves_current_session() {
        let mut store = AppServerSessionStore::default();
        let base = Utc::now();
        for index in 0..=MAX_LOGICAL_SESSIONS {
            store.sessions.insert(
                format!("session-{index}"),
                AppServerSessionState {
                    thread_id: format!("thread-{index}"),
                    workspace: Some("/workspace".to_string()),
                    codex_home_mode: "inherit".to_string(),
                    updated_at: base + chrono::Duration::seconds(index as i64),
                },
            );
        }

        prune_session_store(&mut store, "session-0");

        assert_eq!(store.sessions.len(), MAX_LOGICAL_SESSIONS);
        assert!(store.sessions.contains_key("session-0"));
        assert!(!store.sessions.contains_key("session-1"));
    }
}
