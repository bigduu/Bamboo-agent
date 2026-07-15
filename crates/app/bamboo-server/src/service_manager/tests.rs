//! `ServiceManager` unit tests against REAL trivial child processes (`/bin/sh
//! -c ...`) — fast (sub-second) and mac/linux-safe. Windows-specific
//! behavior (no SIGTERM shell-out) is exercised indirectly by every test
//! here still passing on any platform (the graceful-shutdown tests only
//! assert the child eventually stops, not which signal path was taken);
//! [`stop_service_escalates_to_hard_kill_when_process_ignores_sigterm`] is
//! gated `cfg(not(target_os = "windows"))` since it specifically exercises
//! `trap '' TERM` + SIGTERM shell-out, which has no Windows equivalent here.

use std::path::PathBuf;
use std::time::Duration;

use bamboo_domain::mcp_config::ReconnectConfig;
use bamboo_plugin::manifest::{GracefulShutdown, HealthCheckSpec, ShutdownSignal};

use super::{ServiceManager, ServiceRuntimeConfig, ServiceState};

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
        user_config_path: PathBuf::from("/tmp/bamboo-service-manager-tests/does-not-exist.json"),
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

    manager.stop_service("sleeper").await.expect("stop");
    assert!(!manager.is_running("sleeper"));
    assert!(manager.status("sleeper").await.is_none());
}

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

#[cfg(not(target_os = "windows"))]
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
    let mut config = base_config("ghost", vec![]);
    config.command = PathBuf::from("/nonexistent/bamboo-service-manager-test-binary");
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
    let mut config = base_config("loopy", vec![]);
    config.command = PathBuf::from("/nonexistent/bamboo-service-manager-test-binary-2");
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
