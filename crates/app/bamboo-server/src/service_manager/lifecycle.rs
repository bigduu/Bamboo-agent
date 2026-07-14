//! The supervisor task: spawn → run (crash/health-detect) → graceful-stop
//! or restart-with-backoff. One task per service, owned entirely by
//! [`super::ServiceManager::start_service`]/[`super::ServiceManager::stop_service`].

use std::process::Stdio;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio_util::sync::CancellationToken;

use bamboo_infrastructure::process::hide_window_for_tokio_command;
use bamboo_plugin::manifest::{HealthCheckKind, ShutdownSignal};

use super::{ServiceRuntime, ServiceState, ServiceStatusSnapshot};

/// Minimal env allowlist applied BEFORE the manifest's declared `env` (see
/// module docs' "Security: `env_clear()`"). Just enough for a normal
/// self-contained binary to run: locate the dynamic loader / shared libs
/// (`PATH`), find a home directory for any runtime that assumes one exists.
const UNIX_ENV_ALLOWLIST: &[&str] = &["PATH", "HOME", "TMPDIR", "TEMP", "TMP", "LANG", "LC_ALL"];
#[cfg(target_os = "windows")]
const WINDOWS_ENV_ALLOWLIST: &[&str] = &[
    "PATH",
    "SystemRoot",
    "SystemDrive",
    "USERPROFILE",
    "APPDATA",
    "LOCALAPPDATA",
    "TEMP",
    "TMP",
];

async fn set_state(runtime: &Arc<ServiceRuntime>, state: ServiceState) {
    *runtime.state.write().await = state;
}

async fn set_last_error(runtime: &Arc<ServiceRuntime>, message: impl Into<String>) {
    *runtime.last_error.write().await = Some(message.into());
}

pub(super) async fn snapshot(runtime: &Arc<ServiceRuntime>) -> ServiceStatusSnapshot {
    let state = *runtime.state.read().await;
    let pid_raw = runtime.pid.load(Ordering::SeqCst);
    ServiceStatusSnapshot {
        id: runtime.config.id.clone(),
        plugin_id: runtime.config.plugin_id.clone(),
        state,
        pid: if pid_raw == 0 { None } else { Some(pid_raw) },
        restart_count: runtime.restart_count.load(Ordering::SeqCst),
        last_error: runtime.last_error.read().await.clone(),
    }
}

/// Build the child command (`env_clear()`, the minimal allowlist, declared
/// env, `BAMBOO_PLUGIN_SERVICE_CONFIG`, `kill_on_drop`, hidden window, piped
/// stdio).
///
/// Spawning (not just building) is done here too, so a spawn failure
/// (`ENOENT` etc.) is a single `io::Result` the caller can treat as a crash.
fn spawn_child(config: &super::ServiceRuntimeConfig) -> std::io::Result<Child> {
    let mut cmd = Command::new(&config.command);
    hide_window_for_tokio_command(&mut cmd);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Kill the child if this `Child` is dropped without going through
        // our own graceful/hard-kill sequence (e.g. the supervisor task is
        // aborted) — same rationale as `bamboo-mcp`'s stdio transport.
        .kill_on_drop(true)
        .args(&config.args)
        // Security (issue #479 §2): clear bamboo-server's own environment
        // first — a service must never inherit ambient secrets — then apply
        // only the minimal allowlist below plus the manifest's declared env.
        .env_clear();

    #[cfg(not(target_os = "windows"))]
    for key in UNIX_ENV_ALLOWLIST {
        if let Ok(value) = std::env::var(key) {
            cmd.env(key, value);
        }
    }
    #[cfg(target_os = "windows")]
    for key in WINDOWS_ENV_ALLOWLIST {
        if let Ok(value) = std::env::var(key) {
            cmd.env(key, value);
        }
    }

    cmd.env("BAMBOO_PLUGIN_SERVICE_CONFIG", &config.user_config_path);
    // Declared env applied LAST so a plugin author can deliberately override
    // an allowlisted variable (e.g. a custom TMPDIR) if they need to.
    cmd.envs(&config.env);

    if let Some(cwd) = &config.cwd {
        cmd.current_dir(cwd);
    }

    cmd.spawn()
}

/// Mirrors `bamboo_mcp::transports::stdio`'s stderr-logger pattern, applied
/// to BOTH streams (a service has no stdin/stdout wire protocol to reserve —
/// everything it prints is just a log line).
fn spawn_stdio_logger(service_id: &str, child: &mut Child) {
    if let Some(stdout) = child.stdout.take() {
        let id = service_id.to_string();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::info!(service_id = %id, "[service stdout] {line}");
            }
        });
    }
    if let Some(stderr) = child.stderr.take() {
        let id = service_id.to_string();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::warn!(service_id = %id, "[service stderr] {line}");
            }
        });
    }
}

/// Send SIGTERM to `pid` by shelling out to `kill -TERM` — deliberately NOT
/// `libc::kill`/`nix` (this workspace already ships a precedent for this
/// exact approach in `bamboo_infrastructure::process::registry::ProcessRegistry::kill_process_by_pid`,
/// and it avoids adding a new dependency). A best-effort signal: failure just
/// means the graceful-timeout below falls through to a hard kill anyway.
#[cfg(not(target_os = "windows"))]
async fn send_graceful_signal(pid: u32) {
    let _ = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .await;
}

/// Windows has no SIGTERM equivalent worth shelling out for here; per issue
/// #479's design ("Windows: kill after grace"), the grace period below is
/// still honoured — this just skips sending an ineffective signal.
#[cfg(target_os = "windows")]
async fn send_graceful_signal(_pid: u32) {}

/// Graceful (signal → poll up to `timeout_ms`) then hard (`SIGKILL`/
/// `TerminateProcess` via `Child::start_kill`) stop of an already-spawned
/// child. Always waits for the child to actually exit before returning (a
/// stopped service must never straddle "we asked it to stop" and "it's
/// actually gone" — the caller is about to report `Stopped`/restart).
async fn terminate_child(child: &mut Child, graceful: &bamboo_plugin::manifest::GracefulShutdown) {
    let pid = child.id();
    if graceful.signal == ShutdownSignal::Term {
        if let Some(pid) = pid {
            send_graceful_signal(pid).await;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_millis(graceful.timeout_ms);
        loop {
            match child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => {
                    if tokio::time::Instant::now() >= deadline {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(_) => break,
            }
        }
    }
    // Still alive (or `signal: none`) — escalate.
    let _ = child.start_kill();
    let _ = child.wait().await;
}

/// TCP/HTTP reachability probe. `ProcessAlive` is intentionally NOT handled
/// here — its "health" is exactly "has the child not exited yet", which
/// [`supervise_running_child`] already tracks via `child.wait()` without any
/// separate polling.
async fn probe_health(spec: &bamboo_plugin::manifest::HealthCheckSpec) -> Result<(), String> {
    let timeout = Duration::from_millis(spec.timeout_ms.max(1));
    match spec.kind {
        HealthCheckKind::ProcessAlive => Ok(()),
        HealthCheckKind::Tcp => {
            let target = spec
                .target
                .as_deref()
                .ok_or_else(|| "tcp health_check missing target".to_string())?;
            match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(target)).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(error)) => Err(format!("tcp health check to '{target}' failed: {error}")),
                Err(_) => Err(format!("tcp health check to '{target}' timed out")),
            }
        }
        HealthCheckKind::Http => {
            let target = spec
                .target
                .as_deref()
                .ok_or_else(|| "http health_check missing target".to_string())?;
            let client = reqwest::Client::new();
            match tokio::time::timeout(timeout, client.get(target).send()).await {
                Ok(Ok(response)) if response.status().is_success() => Ok(()),
                Ok(Ok(response)) => Err(format!(
                    "http health check to '{target}' returned status {}",
                    response.status()
                )),
                Ok(Err(error)) => Err(format!("http health check to '{target}' failed: {error}")),
                Err(_) => Err(format!("http health check to '{target}' timed out")),
            }
        }
    }
}

enum RunOutcome {
    /// `stop_service` was called while the child was running.
    StoppedByRequest,
    /// The child process exited on its own (crash, or a clean self-exit —
    /// either way, unexpected for a service that's supposed to be
    /// long-running).
    Exited(String),
    /// A `Tcp`/`Http` health check failed while the process was still alive.
    /// The child is still running at this point — the caller kills it before
    /// treating this like a crash.
    Unhealthy(String),
}

/// Own the running child until it exits, is stopped, or (for `Tcp`/`Http`
/// health checks) is found unhealthy. Generalizes
/// `bamboo_mcp::manager::lifecycle::start_health_check`'s "ping on an
/// interval; a single failure means degraded + reconnect" pattern.
async fn supervise_running_child(
    runtime: &Arc<ServiceRuntime>,
    child: &mut Child,
    stop_token: &CancellationToken,
) -> RunOutcome {
    let health = runtime.config.health_check.clone();
    if matches!(health.kind, HealthCheckKind::ProcessAlive) {
        return tokio::select! {
            _ = stop_token.cancelled() => RunOutcome::StoppedByRequest,
            result = child.wait() => RunOutcome::Exited(
                result.map(|status| format!("process exited: {status}"))
                    .unwrap_or_else(|error| format!("failed to wait on child: {error}")),
            ),
        };
    }

    let mut ticker = tokio::time::interval(Duration::from_millis(health.interval_ms.max(100)));
    ticker.tick().await; // first tick is immediate; consume it to give the process startup grace before the first probe
    loop {
        tokio::select! {
            _ = stop_token.cancelled() => return RunOutcome::StoppedByRequest,
            result = child.wait() => {
                return RunOutcome::Exited(
                    result.map(|status| format!("process exited: {status}"))
                        .unwrap_or_else(|error| format!("failed to wait on child: {error}")),
                );
            }
            _ = ticker.tick() => {
                match probe_health(&health).await {
                    Ok(()) => {
                        // Recovered from a prior Degraded tick.
                        if *runtime.state.read().await == ServiceState::Degraded {
                            set_state(runtime, ServiceState::Running).await;
                        }
                    }
                    Err(reason) => {
                        set_state(runtime, ServiceState::Degraded).await;
                        return RunOutcome::Unhealthy(reason);
                    }
                }
            }
        }
    }
}

/// Compute the backoff (ms) for the given 1-based consecutive-attempt
/// number, mirroring `bamboo_mcp::manager::reconnect::attempt_reconnection`'s
/// exponential-doubling-with-ceiling.
fn compute_backoff_ms(policy: &bamboo_domain::mcp_config::ReconnectConfig, attempt: u32) -> u64 {
    // The first attempt is clamped too: a manifest declaring
    // initial_backoff_ms > max_backoff_ms (nothing validates against it)
    // must not exceed the configured ceiling on attempt 1.
    let mut backoff = policy
        .initial_backoff_ms
        .max(1)
        .min(policy.max_backoff_ms.max(1));
    for _ in 1..attempt {
        backoff = backoff.saturating_mul(2).min(policy.max_backoff_ms);
    }
    backoff
}

/// After a crash/unhealthy exit: decide whether to restart, and if so, sleep
/// out the backoff (interruptibly). Returns `true` to restart, `false` to
/// settle into `Stopped`.
async fn maybe_wait_before_restart(
    runtime: &Arc<ServiceRuntime>,
    stop_token: &CancellationToken,
    consecutive_attempts: &mut u32,
) -> bool {
    if runtime.shutdown.load(Ordering::SeqCst) {
        return false;
    }
    let policy = runtime.config.restart_policy.clone();
    if !policy.enabled {
        return false;
    }
    *consecutive_attempts += 1;
    if policy.max_attempts > 0 && *consecutive_attempts > policy.max_attempts {
        set_last_error(
            runtime,
            format!(
                "max restart attempts ({}) reached for service '{}'",
                policy.max_attempts, runtime.config.id
            ),
        )
        .await;
        return false;
    }
    runtime.restart_count.fetch_add(1, Ordering::SeqCst);
    let backoff_ms = compute_backoff_ms(&policy, *consecutive_attempts);
    set_state(runtime, ServiceState::Restarting).await;
    tokio::select! {
        _ = stop_token.cancelled() => false,
        _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => !runtime.shutdown.load(Ordering::SeqCst),
    }
}

/// The supervisor loop: spawn → run → (graceful stop | crash/unhealthy →
/// maybe restart with backoff). Exits (task completes) exactly when the
/// service settles into [`ServiceState::Stopped`] — either an intentional
/// stop, `restart_policy.enabled == false`, or `max_attempts` reached.
pub(super) async fn run_supervisor(runtime: Arc<ServiceRuntime>) {
    let stop_token = runtime.stop_token.clone();
    let mut consecutive_attempts: u32 = 0;

    loop {
        if runtime.shutdown.load(Ordering::SeqCst) {
            set_state(&runtime, ServiceState::Stopped).await;
            return;
        }
        set_state(&runtime, ServiceState::Starting).await;

        let mut child = match spawn_child(&runtime.config) {
            Ok(child) => child,
            Err(error) => {
                set_last_error(&runtime, format!("failed to spawn: {error}")).await;
                set_state(&runtime, ServiceState::Crashed).await;
                if !maybe_wait_before_restart(&runtime, &stop_token, &mut consecutive_attempts)
                    .await
                {
                    set_state(&runtime, ServiceState::Stopped).await;
                    return;
                }
                continue;
            }
        };

        runtime.pid.store(child.id().unwrap_or(0), Ordering::SeqCst);
        spawn_stdio_logger(&runtime.config.id, &mut child);
        set_state(&runtime, ServiceState::Running).await;
        // A genuinely-started child resets the backoff/attempt counter —
        // only a crash-loop (never reaching Running) should escalate delay.
        consecutive_attempts = 0;

        let outcome = supervise_running_child(&runtime, &mut child, &stop_token).await;
        runtime.pid.store(0, Ordering::SeqCst);

        match outcome {
            RunOutcome::StoppedByRequest => {
                set_state(&runtime, ServiceState::Stopping).await;
                terminate_child(&mut child, &runtime.config.graceful_shutdown).await;
                set_state(&runtime, ServiceState::Stopped).await;
                return;
            }
            RunOutcome::Exited(reason) => {
                set_last_error(&runtime, reason).await;
                set_state(&runtime, ServiceState::Crashed).await;
            }
            RunOutcome::Unhealthy(reason) => {
                set_last_error(&runtime, reason).await;
                terminate_child(&mut child, &runtime.config.graceful_shutdown).await;
                set_state(&runtime, ServiceState::Crashed).await;
            }
        }

        if runtime.shutdown.load(Ordering::SeqCst) {
            set_state(&runtime, ServiceState::Stopped).await;
            return;
        }
        if !maybe_wait_before_restart(&runtime, &stop_token, &mut consecutive_attempts).await {
            set_state(&runtime, ServiceState::Stopped).await;
            return;
        }
    }
}

#[cfg(test)]
mod backoff_tests {
    use super::compute_backoff_ms;
    use bamboo_domain::mcp_config::ReconnectConfig;

    fn policy(initial: u64, max: u64) -> ReconnectConfig {
        ReconnectConfig {
            initial_backoff_ms: initial,
            max_backoff_ms: max,
            ..ReconnectConfig::default()
        }
    }

    #[test]
    fn backoff_doubles_and_clamps_to_ceiling() {
        let p = policy(100, 500);
        assert_eq!(compute_backoff_ms(&p, 1), 100);
        assert_eq!(compute_backoff_ms(&p, 2), 200);
        assert_eq!(compute_backoff_ms(&p, 3), 400);
        assert_eq!(compute_backoff_ms(&p, 4), 500);
        assert_eq!(compute_backoff_ms(&p, 10), 500);
    }

    /// A misconfigured manifest with initial > max must be clamped on the
    /// FIRST attempt too (review finding on #482).
    #[test]
    fn backoff_first_attempt_is_clamped_when_initial_exceeds_max() {
        let p = policy(10_000, 500);
        assert_eq!(compute_backoff_ms(&p, 1), 500);
        assert_eq!(compute_backoff_ms(&p, 2), 500);
    }
}
