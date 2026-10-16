use super::*;
use std::os::unix::fs::PermissionsExt;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

async fn counted_snapshot() -> (VerifiedCli, PathBuf) {
    let temporary = tempfile::tempdir().unwrap();
    let count = temporary.path().join("executions");
    let bytes = format!(
        "#!/bin/sh\nset -eu\nprintf x >> {}\nprintf '%s\\n' '{}'\n",
        quoted(&count),
        missing()
    )
    .into_bytes();
    (
        VerifiedCli::snapshot(&bytes, temporary).await.unwrap(),
        count,
    )
}

// Model the kernel's busy inode with one owned writable descriptor. The
// separate Linux fork probe demonstrates that such a descriptor can survive
// parent close in another process even when it is close-on-exec.
fn hold_owned_writer(command: &VerifiedCli) -> std::fs::File {
    std::fs::set_permissions(&command.executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let writer = std::fs::OpenOptions::new()
        .append(true)
        .open(&command.executable)
        .unwrap();
    std::fs::set_permissions(&command.executable, std::fs::Permissions::from_mode(0o500)).unwrap();
    writer
}

#[tokio::test(flavor = "current_thread")]
async fn busy_snapshot_recovers_after_writer_release_without_duplicate_execution() {
    let (command, count) = counted_snapshot().await;
    let private = command._temporary.path().to_path_buf();
    let before = std::fs::read(&command.executable).unwrap();
    let writer = hold_owned_writer(&command);
    let error = std::process::Command::new(&command.executable)
        .output()
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::ETXTBSY));

    // Tokio does not synchronously poll spawned tasks. On this current-thread
    // runtime the first exec therefore sees the real busy writer; releasing it
    // requires the recovery path to yield, not a timed fixture race.
    let released = tokio::spawn(async move { drop(writer) });
    let result = run_cli(&command, &[], Duration::from_secs(1), false).await;
    released.await.unwrap();
    let report = result.expect("the same verified snapshot can run after the owned writer closes");
    assert_eq!(report.state, AppStateKind::Missing);
    assert_eq!(std::fs::read(&count).unwrap(), b"x");
    assert_eq!(std::fs::read(&command.executable).unwrap(), before);
    assert_eq!(
        std::fs::metadata(&command.executable)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o500
    );
    drop(command);
    assert!(!private.exists());
}

#[tokio::test(flavor = "current_thread")]
async fn persistent_busy_writer_stops_at_the_original_operation_budget() {
    let (command, count) = counted_snapshot().await;
    let writer = hold_owned_writer(&command);
    let budget = Duration::from_millis(25);
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_millis(250),
        run_cli(&command, &[], budget, false),
    )
    .await;
    let elapsed = started.elapsed();
    drop(writer);
    assert_eq!(
        result
            .expect("a busy exec must not restart the operation budget")
            .unwrap_err(),
        "status_failed"
    );
    assert!(
        elapsed >= budget,
        "busy exec must retain its bounded recovery opportunity"
    );
    assert!(!count.exists(), "a failed exec must not run the CLI");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn persistent_busy_writer_bounds_real_process_creation_attempts() {
    let (command, count) = counted_snapshot().await;
    let writer = hold_owned_writer(&command);
    let error = std::process::Command::new(&command.executable)
        .output()
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::ETXTBSY));

    let mut process = tokio::process::Command::new(&command.executable);
    let mut attempts = 0;
    let budget = INSTALL_TIMEOUT;
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        budget + Duration::from_secs(1),
        spawn_verified_cli_with(budget, || {
            attempts += 1;
            assert!(
                attempts <= 200,
                "persistent ETXTBSY must not create a process-attempt burst"
            );
            process.spawn()
        }),
    )
    .await
    .expect("busy recovery must retain the original deadline");
    let elapsed = started.elapsed();
    drop(writer);

    assert_eq!(result.unwrap_err(), "status_failed");
    assert!(elapsed >= budget && elapsed <= budget + Duration::from_millis(2));
    assert!(
        (2..=200).contains(&attempts),
        "persistent ETXTBSY must back off real spawn attempts; observed {attempts}"
    );
    assert!(!count.exists(), "a failed exec must not run the CLI");
}

#[tokio::test(flavor = "current_thread")]
async fn permanent_spawn_error_fails_without_a_retry_yield_or_execution() {
    let (command, count) = counted_snapshot().await;
    std::fs::set_permissions(&command.executable, std::fs::Permissions::from_mode(0o400)).unwrap();
    let error = std::process::Command::new(&command.executable)
        .output()
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EACCES));

    let marker = Arc::new(AtomicBool::new(false));
    let later = Arc::clone(&marker);
    let queued = tokio::spawn(async move { later.store(true, Ordering::SeqCst) });
    let result = run_cli(&command, &[], Duration::from_secs(1), false).await;
    let yielded = marker.load(Ordering::SeqCst);
    queued.await.unwrap();
    assert_eq!(result.unwrap_err(), "status_failed");
    assert!(
        !yielded,
        "a permanent spawn failure must not enter recovery"
    );
    assert!(!count.exists());
}
