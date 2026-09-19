//! `ServiceManager` process tests. Legacy shell lifecycle cases are explicitly
//! Unix-only because they exercise `/bin/sh` and SIGTERM. NDJSON generation,
//! restart, EOF, saturation, and cleanup cases spawn this Rust test binary as
//! their helper and remain enabled on Unix and Windows without a `cmd.exe`
//! fork of the fixtures.

use std::io::{BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use bamboo_domain::mcp_config::ReconnectConfig;
use bamboo_plugin::manifest::{
    GracefulShutdown, HealthCheckSpec, ServiceInputProtocol, ShutdownSignal,
};

use super::{
    input::ServiceInputRuntime, ServiceInputHealth, ServiceInputSendError, ServiceManager,
    ServiceRuntimeConfig, ServiceState, MAX_SERVICE_INPUT_LINE_BYTES,
};

const HELPER_MODE_ENV: &str = "BAMBOO_TEST_SERVICE_INPUT_MODE";
const HELPER_OUTPUT_ENV: &str = "BAMBOO_TEST_SERVICE_INPUT_OUTPUT";
const HELPER_COUNT_ENV: &str = "BAMBOO_TEST_SERVICE_INPUT_COUNT";

fn base_config(id: &str, args: Vec<&str>) -> ServiceRuntimeConfig {
    ServiceRuntimeConfig {
        id: id.to_string(),
        plugin_id: format!("{id}-plugin"),
        name: None,
        command: PathBuf::from("/bin/sh"),
        args: args.into_iter().map(str::to_string).collect(),
        cwd: None,
        env: Default::default(),
        health_check: HealthCheckSpec::default(),
        restart_policy: ReconnectConfig {
            enabled: false,
            ..ReconnectConfig::default()
        },
        graceful_shutdown: GracefulShutdown::default(),
        input_protocol: ServiceInputProtocol::None,
        user_config_path: std::env::temp_dir()
            .join("bamboo-service-manager-tests")
            .join("does-not-exist.json"),
    }
}

fn helper_args() -> Vec<String> {
    vec![
        "service_input_child_helper".to_string(),
        "--nocapture".to_string(),
        "--test-threads=1".to_string(),
    ]
}

fn helper_config(id: &str, mode: &str, dir: &Path) -> ServiceRuntimeConfig {
    let mut config = base_config(id, Vec::new());
    config.command = std::env::current_exe().expect("current test executable");
    config.args = helper_args();
    config.input_protocol = ServiceInputProtocol::NdjsonV1;
    config.graceful_shutdown = GracefulShutdown {
        signal: ShutdownSignal::None,
        timeout_ms: 0,
    };
    config
        .env
        .insert(HELPER_MODE_ENV.to_string(), mode.to_string());
    config.env.insert(
        HELPER_OUTPUT_ENV.to_string(),
        dir.join("service-input-output.ndjson")
            .to_string_lossy()
            .into_owned(),
    );
    config.env.insert(
        HELPER_COUNT_ENV.to_string(),
        dir.join("service-input-count.txt")
            .to_string_lossy()
            .into_owned(),
    );
    config
}

fn direct_helper_command(mode: &str, dir: &Path) -> tokio::process::Command {
    let mut command =
        tokio::process::Command::new(std::env::current_exe().expect("current test executable"));
    command
        .args(helper_args())
        .env(HELPER_MODE_ENV, mode)
        .env(HELPER_OUTPUT_ENV, dir.join("service-input-output.ndjson"))
        .env(HELPER_COUNT_ENV, dir.join("service-input-count.txt"))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command
}

fn append_helper_line(path: &Path, line: &str) {
    let mut output = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open helper output");
    writeln!(output, "{line}").expect("append helper output");
    output.flush().expect("flush helper output");
}

#[cfg(unix)]
async fn process_is_alive(process_id: u32) -> bool {
    tokio::process::Command::new("kill")
        .args(["-0", &process_id.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok_and(|status| status.success())
}

#[cfg(windows)]
async fn process_is_alive(process_id: u32) -> bool {
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
async fn process_is_alive(_process_id: u32) -> bool {
    false
}

/// Cross-platform child executable used by the NDJSON process tests. A
/// normal test-suite invocation has no mode and returns immediately; spawned
/// copies inherit only the explicit declared test env above.
#[test]
fn service_input_child_helper() {
    let Ok(mode) = std::env::var(HELPER_MODE_ENV) else {
        return;
    };
    let output_path =
        PathBuf::from(std::env::var(HELPER_OUTPUT_ENV).expect("helper output path is declared"));
    match mode.as_str() {
        "restart" => {
            let count_path = PathBuf::from(
                std::env::var(HELPER_COUNT_ENV).expect("helper count path is declared"),
            );
            let attempt = std::fs::read_to_string(&count_path)
                .ok()
                .and_then(|raw| raw.trim().parse::<u64>().ok())
                .unwrap_or(0)
                + 1;
            std::fs::write(&count_path, attempt.to_string()).expect("write helper count");

            let stdin = std::io::stdin();
            for line in stdin.lock().lines() {
                let line = line.expect("read NDJSON line");
                append_helper_line(&output_path, &format!("{attempt}:{line}"));
                if attempt == 1 {
                    // The supervisor must retire this binding and publish a
                    // different generation for the restarted process.
                    return;
                }
            }
            append_helper_line(&output_path, &format!("{attempt}:EOF"));
        }
        "eof" => {
            let mut input = String::new();
            std::io::stdin()
                .read_to_string(&mut input)
                .expect("read until stdin EOF");
            std::fs::write(output_path, input).expect("persist bytes observed before EOF");
        }
        "sleep" => std::thread::sleep(Duration::from_secs(30)),
        other => panic!("unknown helper mode: {other}"),
    }
}

/// Poll `f` until it returns `Some`, or panic after `timeout`.
async fn poll_until<T, F, Fut>(timeout: Duration, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(value) = f().await {
            return value;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("condition not met within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn start_service_spawns_process_and_reports_running_with_pid() {
    let manager = ServiceManager::new();
    let config = base_config("sleeper", vec!["-c", "sleep 5"]);
    manager.start_service(config).await.expect("start");

    let status = poll_until(Duration::from_secs(2), || async {
        let status = manager.status("sleeper").await?;
        (status.state == ServiceState::Running).then_some(status)
    })
    .await;
    assert!(status.pid.is_some(), "a running service must report a pid");
    assert_eq!(status.restart_count, 0);
    assert!(status.input.is_none());
    assert!(
        manager.input_sender("sleeper").await.is_none(),
        "legacy services must retain null stdin and expose no sender"
    );

    manager.stop_service("sleeper").await.expect("stop");
    assert!(!manager.is_running("sleeper"));
    assert!(manager.status("sleeper").await.is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn starting_an_already_running_service_id_is_rejected() {
    let manager = ServiceManager::new();
    manager
        .start_service(base_config("dup", vec!["-c", "sleep 5"]))
        .await
        .expect("first start");

    let error = manager
        .start_service(base_config("dup", vec!["-c", "sleep 5"]))
        .await
        .expect_err("duplicate id must be rejected");
    assert!(matches!(
        error,
        super::ServiceManagerError::AlreadyRunning(_)
    ));

    manager.stop_service("dup").await.expect("cleanup stop");
}

#[cfg(unix)]
#[tokio::test]
async fn stop_service_on_a_process_alive_service_is_graceful_and_fast() {
    let manager = ServiceManager::new();
    manager
        .start_service(base_config("graceful", vec!["-c", "sleep 30"]))
        .await
        .expect("start");
    poll_until(Duration::from_secs(2), || async {
        let status = manager.status("graceful").await?;
        (status.state == ServiceState::Running).then_some(())
    })
    .await;

    let started = tokio::time::Instant::now();
    manager.stop_service("graceful").await.expect("stop");
    // `sleep` terminates immediately on SIGTERM (its default disposition),
    // so a graceful stop must complete WAY under the 5s default
    // `graceful_shutdown.timeout_ms` — proves the SIGTERM path (not just the
    // hard-kill-after-timeout fallback) actually ran.
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "graceful stop of a SIGTERM-responsive process took {:?}, expected well under 2s",
        started.elapsed()
    );
    assert!(!manager.is_running("graceful"));
}

#[cfg(unix)]
#[tokio::test]
async fn stop_service_escalates_to_hard_kill_when_process_ignores_sigterm() {
    let manager = ServiceManager::new();
    let mut config = base_config("stubborn", vec!["-c", "trap '' TERM; sleep 30"]);
    config.graceful_shutdown = GracefulShutdown {
        signal: ShutdownSignal::Term,
        timeout_ms: 300,
    };
    manager.start_service(config).await.expect("start");
    poll_until(Duration::from_secs(2), || async {
        let status = manager.status("stubborn").await?;
        (status.state == ServiceState::Running).then_some(())
    })
    .await;

    let started = tokio::time::Instant::now();
    manager.stop_service("stubborn").await.expect("stop");
    let elapsed = started.elapsed();
    // Must have waited out (roughly) the grace period before the hard kill,
    // but still completed promptly rather than hanging forever.
    assert!(
        elapsed >= Duration::from_millis(250) && elapsed < Duration::from_secs(5),
        "expected the grace period then a hard kill, got {elapsed:?}"
    );
    assert!(!manager.is_running("stubborn"));
}

#[cfg(unix)]
#[tokio::test]
async fn crash_triggers_restart_with_backoff_and_can_still_be_stopped() {
    let manager = ServiceManager::new();
    let mut config = base_config("crasher", vec!["-c", "exit 1"]);
    config.restart_policy = ReconnectConfig {
        enabled: true,
        initial_backoff_ms: 20,
        max_backoff_ms: 50,
        max_attempts: 0, // unlimited — the test stops it explicitly
    };
    manager.start_service(config).await.expect("start");

    // A process that spawns fine but exits immediately must eventually be
    // observed restarting (restart_count > 0) rather than just settling
    // into Crashed forever.
    poll_until(Duration::from_secs(3), || async {
        let status = manager.status("crasher").await?;
        (status.restart_count > 0).then_some(())
    })
    .await;

    // Must still be stoppable (and promptly) while it's crash-looping,
    // whether it's caught mid-run or mid-backoff-sleep — this is the
    // `shutdown` AtomicBool / stop_token cancellation contract.
    let started = tokio::time::Instant::now();
    manager.stop_service("crasher").await.expect("stop");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(!manager.is_running("crasher"));
}

#[tokio::test]
async fn unspawnable_command_respects_max_attempts_and_settles_stopped() {
    let manager = ServiceManager::new();
    let missing_root = tempfile::tempdir().unwrap();
    let mut config = base_config("ghost", vec![]);
    config.command = missing_root.path().join("missing-service-binary");
    config.restart_policy = ReconnectConfig {
        enabled: true,
        initial_backoff_ms: 10,
        max_backoff_ms: 20,
        max_attempts: 2,
    };
    manager.start_service(config).await.expect("start");

    let status = poll_until(Duration::from_secs(3), || async {
        let status = manager.status("ghost").await?;
        (status.state == ServiceState::Stopped).then_some(status)
    })
    .await;
    assert_eq!(
        status.restart_count, 2,
        "a command that never spawns must stop after exactly max_attempts restarts, not loop forever"
    );
    assert!(status.last_error.is_some());
    // Settled on its own (no explicit stop_service call) — still present in
    // the manager (only `stop_service` removes the entry), just Stopped.
    assert!(manager.is_running("ghost"));
}

#[tokio::test]
async fn intentional_stop_during_backoff_sleep_does_not_restart() {
    let manager = ServiceManager::new();
    let missing_root = tempfile::tempdir().unwrap();
    let mut config = base_config("loopy", vec![]);
    config.command = missing_root.path().join("missing-service-binary");
    config.restart_policy = ReconnectConfig {
        enabled: true,
        initial_backoff_ms: 5_000, // long enough that the test reliably lands mid-sleep
        max_backoff_ms: 5_000,
        max_attempts: 0,
    };
    manager.start_service(config).await.expect("start");

    // Give the supervisor a moment to hit its first crash and enter the
    // (5s) backoff sleep, then stop it — this must cancel the sleep rather
    // than block until it elapses.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = tokio::time::Instant::now();
    manager.stop_service("loopy").await.expect("stop");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "stop_service must interrupt an in-progress backoff sleep, took {:?}",
        started.elapsed()
    );
    assert!(!manager.is_running("loopy"));
}

#[tokio::test]
async fn process_alive_health_check_is_the_default() {
    let spec = HealthCheckSpec::default();
    assert_eq!(
        spec.kind,
        bamboo_plugin::manifest::HealthCheckKind::ProcessAlive
    );
}

#[tokio::test]
async fn ndjson_process_restart_rebinds_once_and_rejects_stale_and_stopped_handles() {
    let dir = tempfile::tempdir().unwrap();
    let output_path = dir.path().join("service-input-output.ndjson");
    let manager = ServiceManager::new();
    let mut config = helper_config("ndjson-restart", "restart", dir.path());
    config.restart_policy = ReconnectConfig {
        enabled: true,
        initial_backoff_ms: 20,
        max_backoff_ms: 20,
        max_attempts: 0,
    };
    manager.start_service(config).await.expect("start helper");

    let first = poll_until(Duration::from_secs(5), || async {
        manager.input_sender("ndjson-restart").await
    })
    .await;
    let first_generation = first.generation();
    first
        .try_send(&serde_json::json!({"generation": 1, "value": "first"}))
        .expect("queue first generation line");
    poll_until(Duration::from_secs(5), || {
        let output_path = output_path.clone();
        async move {
            let raw = std::fs::read_to_string(output_path).ok()?;
            raw.contains("1:{\"generation\":1,\"value\":\"first\"}")
                .then_some(())
        }
    })
    .await;

    let second = poll_until(Duration::from_secs(5), || async {
        let sender = manager.input_sender("ndjson-restart").await?;
        (sender.generation() != first_generation).then_some(sender)
    })
    .await;
    assert_eq!(
        second.generation(),
        first_generation + 1,
        "one child restart must publish exactly one replacement binding"
    );
    assert_eq!(
        first.try_send(&serde_json::json!({"must": "not reach replacement"})),
        Err(ServiceInputSendError::StaleGeneration {
            generation: first_generation
        })
    );

    second
        .try_send(&serde_json::json!({"generation": 2, "value": "second"}))
        .expect("queue replacement generation line");
    poll_until(Duration::from_secs(5), || {
        let output_path = output_path.clone();
        async move {
            let raw = std::fs::read_to_string(output_path).ok()?;
            raw.contains("2:{\"generation\":2,\"value\":\"second\"}")
                .then_some(())
        }
    })
    .await;

    let status = manager.status("ndjson-restart").await.expect("status");
    let input = status.input.expect("NDJSON diagnostics");
    assert_eq!(input.generation, Some(second.generation()));
    assert_eq!(input.health, ServiceInputHealth::Ready);
    assert_eq!(input.accepted_lines, 2);
    assert_eq!(input.dropped_stale_generation, 1);
    let safe_status = serde_json::to_string(&input).expect("serialize diagnostics");
    assert!(!safe_status.contains("first"));
    assert!(!safe_status.contains("second"));

    manager.stop_service("ndjson-restart").await.expect("stop");
    assert_eq!(
        second.try_send(&serde_json::json!({"after": "stop"})),
        Err(ServiceInputSendError::Stopped {
            generation: second.generation()
        })
    );

    // A fresh same-id runtime must use the manager-lifetime allocator, not
    // restart at generation 1 (ABA across upgrade/reinstall).
    let replacement = helper_config("ndjson-restart", "sleep", dir.path());
    manager
        .start_service(replacement)
        .await
        .expect("start fresh same-id runtime");
    let third = poll_until(Duration::from_secs(5), || async {
        manager.input_sender("ndjson-restart").await
    })
    .await;
    assert!(third.generation() > second.generation());
    assert_eq!(
        second.try_send(&serde_json::json!({"must": "not cross ABA"})),
        Err(ServiceInputSendError::Stopped {
            generation: second.generation()
        })
    );
    manager
        .stop_service("ndjson-restart")
        .await
        .expect("cleanup replacement");
}

#[tokio::test]
async fn stop_racing_unreturned_start_awaits_prepublished_supervisor_and_writer_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(ServiceManager::new());
    let config = helper_config("publish-race", "sleep", dir.path());
    let (published_tx, published_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let start_manager = manager.clone();
    let start_task = tokio::spawn(async move {
        start_manager
            .start_service_with_publish_hook(config, || async move {
                let _ = published_tx.send(());
                let _ = resume_rx.await;
            })
            .await
    });
    published_rx.await.expect("runtime published");

    // Force the former failure window: start_service has published but has
    // not returned, while its child and generation writer are live.
    let (sender, pid) = poll_until(Duration::from_secs(5), || {
        let manager = manager.clone();
        async move {
            let sender = manager.input_sender("publish-race").await?;
            let status = manager.status("publish-race").await?;
            Some((sender, status.pid?))
        }
    })
    .await;
    assert!(
        process_is_alive(pid).await,
        "helper must be live before stop"
    );

    tokio::time::timeout(Duration::from_secs(5), manager.stop_service("publish-race"))
        .await
        .expect("stop must not lose the supervisor handle")
        .expect("stop published runtime");
    assert!(!manager.is_running("publish-race"));
    assert!(manager.status("publish-race").await.is_none());
    assert_eq!(
        sender.try_send(&serde_json::json!({"after": "racing stop"})),
        Err(ServiceInputSendError::Stopped {
            generation: sender.generation()
        })
    );
    assert!(
        !process_is_alive(pid).await,
        "stop returned before the supervised child was reaped"
    );

    let _ = resume_tx.send(());
    start_task
        .await
        .expect("join paused start")
        .expect("start linearized before stop");
}

#[tokio::test]
async fn closing_generation_writer_delivers_process_eof_after_ordered_lines() {
    let dir = tempfile::tempdir().unwrap();
    let output_path = dir.path().join("service-input-output.ndjson");
    let mut child = direct_helper_command("eof", dir.path())
        .spawn()
        .expect("spawn EOF helper");
    let input = ServiceInputRuntime::new("eof-helper".to_string(), Arc::new(AtomicU64::new(0)));
    let bound = input.bind_child(&mut child).await.expect("bind stdin");
    let sender = input.sender().await.expect("generation sender");
    sender
        .try_send(&serde_json::json!({"sequence": 1}))
        .expect("first line");
    sender
        .try_send(&serde_json::json!({"sequence": 2}))
        .expect("second line");
    poll_until(Duration::from_secs(5), || async {
        (input.snapshot(false).await.written_lines == 2).then_some(())
    })
    .await;

    bound.close(&input, true).await;
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("helper observed EOF promptly")
        .expect("wait helper");
    assert!(status.success());
    assert_eq!(
        std::fs::read_to_string(output_path).expect("helper output"),
        "{\"sequence\":1}\n{\"sequence\":2}\n"
    );
    assert_eq!(
        sender.try_send(&serde_json::json!({"sequence": 3})),
        Err(ServiceInputSendError::Stopped {
            generation: sender.generation()
        })
    );
}

#[tokio::test]
async fn process_pipe_saturation_is_bounded_and_cleanup_cancels_blocked_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = direct_helper_command("sleep", dir.path())
        .spawn()
        .expect("spawn non-reading helper");
    let input =
        ServiceInputRuntime::new("saturation-helper".to_string(), Arc::new(AtomicU64::new(0)));
    let bound = input
        .bind_child_for_test(&mut child, 1)
        .await
        .expect("bind one-slot queue");
    let sender = input.sender().await.expect("generation sender");

    // Larger than normal OS pipe buffers on supported platforms, keeping the
    // sole writer occupied while the one queue slot is filled behind it.
    let oversized = "x".repeat(MAX_SERVICE_INPUT_LINE_BYTES - 3);
    sender.try_send(&oversized).expect("occupy writer");
    poll_until(Duration::from_secs(5), || async {
        (sender.remaining_capacity() == 1).then_some(())
    })
    .await;
    sender
        .try_send(&serde_json::json!({"queued": 1}))
        .expect("fill the bounded queue");
    let started = tokio::time::Instant::now();
    assert_eq!(
        sender.try_send(&serde_json::json!({"queued": 2})),
        Err(ServiceInputSendError::QueueFull {
            generation: sender.generation()
        })
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "queue-full producer path waited on child I/O"
    );
    assert_eq!(input.snapshot(false).await.dropped_queue_full, 1);

    let cleanup_started = tokio::time::Instant::now();
    bound.close(&input, true).await;
    assert!(
        cleanup_started.elapsed() < Duration::from_secs(2),
        "cancelling a blocked stdin write must be prompt"
    );
    let _ = child.start_kill();
    let _ = child.wait().await;
}

struct SerializationFailure;

impl serde::Serialize for SerializationFailure {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom("sentinel must not escape"))
    }
}

#[tokio::test]
async fn serialization_and_broken_stdin_fail_without_payload_diagnostics() {
    let input = ServiceInputRuntime::new("broken-helper".to_string(), Arc::new(AtomicU64::new(0)));
    let (writer, reader) = tokio::io::duplex(64);
    drop(reader);
    let bound = input
        .bind_writer_for_test(writer, 2)
        .await
        .expect("bind broken test writer");
    let sender = input.sender().await.expect("sender");

    assert_eq!(
        sender.try_send(&SerializationFailure),
        Err(ServiceInputSendError::Serialization)
    );
    let oversized = "x".repeat(MAX_SERVICE_INPUT_LINE_BYTES);
    assert_eq!(
        sender.try_send(&oversized),
        Err(ServiceInputSendError::Oversize {
            max_bytes: MAX_SERVICE_INPUT_LINE_BYTES
        })
    );
    sender
        .try_send(&serde_json::json!({"secret": "must-not-appear"}))
        .expect("the first write is accepted before async break detection");
    poll_until(Duration::from_secs(5), || async {
        match sender.try_send(&serde_json::json!({"probe": true})) {
            Err(ServiceInputSendError::BrokenStdin { .. }) => Some(()),
            Err(ServiceInputSendError::QueueFull { .. }) | Ok(()) => None,
            other => panic!("unexpected broken-stdin probe outcome: {other:?}"),
        }
    })
    .await;

    let snapshot = input.snapshot(false).await;
    assert_eq!(snapshot.health, ServiceInputHealth::BrokenStdin);
    assert_eq!(snapshot.serialization_failures, 1);
    assert_eq!(snapshot.oversize_lines, 1);
    assert_eq!(snapshot.write_failures, 1);
    assert!(snapshot.dropped_broken_stdin >= 1);
    let safe = serde_json::to_string(&snapshot).expect("serialize safe diagnostics");
    assert!(!safe.contains("must-not-appear"));
    assert!(!safe.contains("sentinel"));
    bound.close(&input, false).await;
    assert_eq!(
        sender.try_send(&serde_json::json!({"after": "restart retirement"})),
        Err(ServiceInputSendError::StaleGeneration {
            generation: sender.generation()
        })
    );
}

#[tokio::test]
async fn intentional_stop_overrides_broken_stdin_and_bound_drop_aborts_writer() {
    let input = ServiceInputRuntime::new("broken-stop".to_string(), Arc::new(AtomicU64::new(0)));
    let (writer, reader) = tokio::io::duplex(16);
    drop(reader);
    let bound = input
        .bind_writer_for_test(writer, 1)
        .await
        .expect("bind writer");
    let sender = input.sender().await.expect("sender");
    sender.try_send(&"line").expect("accept before break");
    poll_until(Duration::from_secs(5), || async {
        (input.snapshot(false).await.health == ServiceInputHealth::BrokenStdin).then_some(())
    })
    .await;
    input.stop_active().await;
    bound.close(&input, true).await;
    assert_eq!(
        sender.try_send(&"after stop"),
        Err(ServiceInputSendError::Stopped {
            generation: sender.generation()
        })
    );

    let drop_runtime =
        ServiceInputRuntime::new("drop-cleanup".to_string(), Arc::new(AtomicU64::new(1)));
    let (writer, mut reader) = tokio::io::duplex(1);
    let bound = drop_runtime
        .bind_writer_for_test(writer, 1)
        .await
        .expect("bind drop writer");
    let dropped_sender = drop_runtime.sender().await.expect("drop sender");
    dropped_sender
        .try_send(&"x".repeat(1024))
        .expect("block writer");
    drop(bound);
    let mut eof = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(2),
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut eof),
    )
    .await
    .expect("BoundServiceInput::drop aborted writer and closed pipe")
    .expect("read drop EOF");
    assert_eq!(
        dropped_sender.try_send(&"after drop"),
        Err(ServiceInputSendError::StaleGeneration {
            generation: dropped_sender.generation()
        })
    );
    drop_runtime.stop_active().await;
}
