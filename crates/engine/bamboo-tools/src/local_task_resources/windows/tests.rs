//! These tests must execute on native Windows. Unix tests are not evidence for
//! Job Object ownership. CI asserts this exact five-test filter before running.
use super::*;
use base64::Engine;
use chrono::Utc;
use std::{path::Path, process::Stdio};
use tokio::io::AsyncReadExt;
use windows_sys::Win32::{
    Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT},
    System::Threading::{OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE},
};

fn resources(run: &str) -> LocalTaskResources {
    LocalTaskResources::new("same-windows-session", run, Utc::now())
}

fn quoted(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "''"))
}

fn encoded(script: &str) -> String {
    let bytes: Vec<u8> = script.encode_utf16().flat_map(u16::to_le_bytes).collect();
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn powershell(script: &str) -> Command {
    let mut command = Command::new("powershell.exe");
    command
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-EncodedCommand",
            &encoded(script),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

// The descendant is deliberately longer-lived than its leader and inherits
// containment through the job. The release gate makes the fast-exit case
// deterministic without relying on sleeps or a recycled PID lookup.
fn tree_script(directory: &Path, exit_after_release: bool) -> String {
    tree_script_with_trace(directory, exit_after_release, false)
}

// Fixture-only phase evidence. PIDs here are observations, never stop authority.
fn tree_script_with_trace(directory: &Path, exit_after_release: bool, trace: bool) -> String {
    let marker = |name: &str| {
        if trace {
            format!(
                "try {{ [IO.File]::WriteAllText({}, [string]$PID) }} catch {{ Write-Output 'fixture-marker-failed:{name}' }}; ",
                quoted(&directory.join(name))
            )
        } else {
            String::new()
        }
    };
    let leader_started = marker("leader-started");
    let descendant_started = marker("descendant-started");
    let spawn_returned = if trace {
        format!(
            "try {{ [IO.File]::WriteAllText({}, [string]$p.Id) }} catch {{ Write-Output 'fixture-marker-failed:spawn-returned' }}; ",
            quoted(&directory.join("spawn-returned"))
        )
    } else {
        String::new()
    };
    let descendant = encoded(&format!(
        "{descendant_started}[IO.File]::WriteAllText({}, [string]$PID); Write-Output 'descendant-ready'; Start-Sleep -Seconds 30; [IO.File]::WriteAllText({}, 'escaped')",
        quoted(&directory.join("ready")), quoted(&directory.join("late"))
    ));
    let tail = if exit_after_release {
        format!(
            "while (!(Test-Path -LiteralPath {})) {{ Start-Sleep -Milliseconds 10 }}; exit 0",
            quoted(&directory.join("release"))
        )
    } else {
        "Start-Sleep -Seconds 30".to_string()
    };
    format!(
        "{leader_started}$ErrorActionPreference='Stop'; $p=Start-Process -FilePath (Join-Path $PSHOME 'powershell.exe') -ArgumentList @('-NoProfile','-NonInteractive','-EncodedCommand','{descendant}') -PassThru -NoNewWindow; {spawn_returned}while (!(Test-Path -LiteralPath {})) {{ Start-Sleep -Milliseconds 10 }}; Write-Output 'leader-ready'; {tail}",
        quoted(&directory.join("ready"))
    )
}

async fn process_from_ready(directory: &Path) -> OwnedHandle {
    let pid = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(directory.join("ready")) {
                if let Ok(pid) = text.parse::<u32>() {
                    break pid;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("native descendant did not start");
    observe_process(pid)
}

fn observe_process(pid: u32) -> OwnedHandle {
    // Observation only. All termination is through the production job handle.
    let raw = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
    assert!(
        !raw.is_null(),
        "cannot observe owned process: {}",
        io::Error::last_os_error()
    );
    unsafe { OwnedHandle::from_raw_handle(raw.cast()) }
}

fn is_running(process: &OwnedHandle) -> bool {
    match unsafe { WaitForSingleObject(process.as_raw_handle().cast(), 0) } {
        WAIT_TIMEOUT => true,
        WAIT_OBJECT_0 => false,
        other => panic!(
            "process wait failed: {other}: {}",
            io::Error::last_os_error()
        ),
    }
}

async fn actually_stopped(process: OwnedHandle) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while is_running(&process) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actual descendant process did not stop");
    // Release observation handles before the production job count is checked.
    drop(process);
}

async fn spawn_tree(scope: &LocalTaskResources, directory: &Path, fast: bool) -> ManagedBashChild {
    scope
        .scope(async { ManagedBashChild::spawn(&mut powershell(&tree_script(directory, fast))) })
        .await
        .unwrap()
}

#[tokio::test]
async fn local_task_resources_windows_foreground_cancel_reaps_descendants() {
    let directory = tempfile::tempdir().unwrap();
    let scope = resources("foreground");
    let mut child = spawn_tree(&scope, directory.path(), false).await;
    let descendant = process_from_ready(directory.path()).await;
    assert!(is_running(&descendant));
    let mut stdout = child.take_stdout().unwrap();
    let mut pump = scope
        .spawn_owned(async move {
            let mut output = Vec::new();
            stdout.read_to_end(&mut output).await.unwrap();
            output
        })
        .unwrap();
    let (receipt, ()) = tokio::join!(scope.close_and_drain(), actually_stopped(descendant));
    assert_eq!(receipt.unwrap().identity(), scope.identity());
    assert!(!child.wait().await.unwrap().success());
    // Receipt includes actual EOF/pump completion, not merely process status.
    let _ = pump.try_recv().expect("receipt preceded pump completion");
    assert!(!directory.path().join("late").exists());
}

#[tokio::test]
async fn local_task_resources_windows_background_scope_drain() {
    use crate::tools::bash_runtime;
    use futures::FutureExt;
    let directory = tempfile::tempdir().unwrap();
    let scope = resources("background");
    let script = encoded(&tree_script_with_trace(directory.path(), false, true));
    let command = format!("powershell.exe -NoProfile -NonInteractive -EncodedCommand {script}");
    let shell = scope
        .scope(bash_runtime::spawn_background(
            &command,
            Some(directory.path()),
            None,
            Some("same-windows-session".into()),
            false,
            None,
        ))
        .await
        .unwrap();
    let descendant = match std::panic::AssertUnwindSafe(process_from_ready(directory.path()))
        .catch_unwind()
        .await
    {
        Ok(process) => process,
        Err(failure) => {
            // Preserve the real failed readiness observation. Cleanup below is
            // only this fixture's retained Job, never a discovered PID tree.
            let reason = failure
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| failure.downcast_ref::<&str>().copied())
                .unwrap_or("non-string readiness panic");
            let phases = [
                "leader-started",
                "spawn-returned",
                "descendant-started",
                "ready",
            ]
            .map(|name| {
                let observed = std::fs::read_to_string(directory.path().join(name))
                    .map(|text| text.chars().take(128).collect::<String>())
                    .map_err(|error| (error.kind(), error.raw_os_error()));
                (name, observed)
            });
            let selected = bamboo_infrastructure::process::preferred_bash_shell();
            let before_status = shell.status().to_string();
            let before_exit = shell.exit_code().await;
            // close_and_drain owns its finalizer independently of this wait.
            // A diagnostic timeout is explicitly unconfirmed, never "stopped".
            let cleanup =
                tokio::time::timeout(Duration::from_secs(10), scope.close_and_drain()).await;
            let (lines, _, _) = shell.read_output_since(0, None).await;
            let mut tail = lines.iter().rev().take(16).collect::<Vec<_>>();
            tail.reverse();
            let tail = tail
                .into_iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join("\n")
                .chars()
                .take(4096)
                .collect::<String>();
            // This branch always fails, even if cleanup succeeds. Status and
            // fixture PIDs are evidence, not process ownership or acceptance.
            panic!(
                "background readiness failed: {reason}; phases={phases:?}; shell={selected:?}; env_source={}; before_status={before_status}; before_exit={before_exit:?}; owned_cleanup={cleanup:?}; after_status={}; after_exit={:?}; captured_tail={tail:?}",
                shell.environment.source.as_str(),
                shell.status(),
                shell.exit_code().await,
            );
        }
    };
    let (receipt, ()) = tokio::join!(scope.close_and_drain(), actually_stopped(descendant));
    receipt.unwrap();
    assert_eq!(shell.status(), "completed");
    assert!(shell.exit_code().await.is_some());
    assert!(bash_runtime::get_shell(&shell.id).is_none());
    let (output, _, _) = shell.read_output_since(0, None).await;
    // Readout is stable after all completion/pump tasks have joined.
    assert_eq!(output, shell.read_output_since(0, None).await.0);
    assert!(!directory.path().join("late").exists());
}

#[tokio::test]
async fn local_task_resources_windows_fast_leader_descendants() {
    let directory = tempfile::tempdir().unwrap();
    let scope = resources("fast-leader");
    let mut child = spawn_tree(&scope, directory.path(), true).await;
    let descendant = process_from_ready(directory.path()).await;
    assert!(is_running(&descendant));
    std::fs::write(directory.path().join("release"), "exit now").unwrap();
    // No cancellation: natural leader exit must still terminate descendants.
    let (status, ()) = tokio::join!(child.wait(), actually_stopped(descendant));
    assert!(status.unwrap().success());
    scope.close_and_drain().await.unwrap();
    assert!(!directory.path().join("late").exists());
}

#[tokio::test]
async fn local_task_resources_windows_execution_isolation() {
    let directory_a = tempfile::tempdir().unwrap();
    let directory_b = tempfile::tempdir().unwrap();
    let a = resources("old-run");
    let b = resources("new-run");
    let mut child_a = spawn_tree(&a, directory_a.path(), false).await;
    let mut child_b = spawn_tree(&b, directory_b.path(), false).await;
    let process_a = process_from_ready(directory_a.path()).await;
    let process_b = process_from_ready(directory_b.path()).await;
    let (receipt, ()) = tokio::join!(a.close_and_drain(), actually_stopped(process_a));
    receipt.unwrap();
    child_a.wait().await.unwrap();
    assert!(
        is_running(&process_b),
        "closing an old run killed its successor"
    );
    assert_ne!(a.identity().owner_id, b.identity().owner_id);
    let (receipt, ()) = tokio::join!(b.close_and_drain(), actually_stopped(process_b));
    receipt.unwrap();
    child_b.wait().await.unwrap();
}

#[tokio::test]
async fn local_task_resources_windows_assignment_failure_reaps_suspended_child() {
    // Cover both refusal before assignment and failure after assignment but
    // before resume. Neither child may execute a single script statement.
    for assigned in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("must-not-execute");
        let scope = resources("setup-failure");
        let mut observed = None;
        let result = spawn_with_setup(
            &mut powershell(&format!(
                "[IO.File]::WriteAllText({}, 'ran')",
                quoted(&marker)
            )),
            &scope,
            |job, child| {
                observed = Some(observe_process(child.id().unwrap()));
                if assigned {
                    job.assign(child)?;
                }
                Err(io::Error::other("injected assignment/resume failure"))
            },
        );
        assert!(result.is_err());
        let (receipt, ()) =
            tokio::join!(scope.close_and_drain(), actually_stopped(observed.unwrap()));
        assert!(receipt.unwrap_err().to_string().contains("setup failed"));
        assert!(
            !marker.exists(),
            "suspended child executed before containment"
        );
    }
}

async fn spawn_initial_tree(directory: &Path, fast: bool) -> InitialWindowsProcess {
    let (process, setup) = spawn_initial(&mut powershell(&tree_script(directory, fast))).unwrap();
    if let Err(error) = setup {
        let cleanup = process.stop_and_reap().await;
        panic!("initial Windows setup failed: {error}; cleanup: {cleanup:?}");
    }
    process
}

async fn initial_leader_exit(process: &InitialWindowsProcess) -> io::Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if process.leader_has_exited()? {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "initial leader did not exit"))?
}

#[tokio::test]
async fn initial_exec_windows_live_cancel_drains_retained_job() {
    let directory = tempfile::tempdir().unwrap();
    let process = spawn_initial_tree(directory.path(), false).await;
    let descendant = process_from_ready(directory.path()).await;
    let descendant_was_live = is_running(&descendant);
    let leader_was_live = process.leader_has_exited().map(|exited| !exited);
    let (cleanup, ()) = tokio::join!(process.stop_and_reap(), actually_stopped(descendant));
    assert!(descendant_was_live);
    assert!(leader_was_live.unwrap());
    assert!(!cleanup.unwrap().success());
    assert!(!directory.path().join("late").exists());
}

#[tokio::test]
async fn initial_exec_windows_cancel_after_leader_reap_drains_retained_job() {
    let directory = tempfile::tempdir().unwrap();
    let mut process = spawn_initial_tree(directory.path(), true).await;
    let descendant = process_from_ready(directory.path()).await;
    std::fs::write(directory.path().join("release"), "exit now").unwrap();
    let observed_exit = initial_leader_exit(&process).await;
    // Reap the leader before cancellation: only the retained job can still
    // contain and drain its surviving descendant. No PID is stop authority.
    let first_wait = tokio::time::timeout(Duration::from_secs(15), process.child.wait()).await;
    let descendant_survived_reap = is_running(&descendant);
    let (cleanup, ()) = tokio::join!(process.stop_and_reap(), actually_stopped(descendant));
    observed_exit.unwrap();
    assert!(first_wait.unwrap().unwrap().success());
    assert!(descendant_survived_reap);
    assert!(cleanup.unwrap().success());
    assert!(!directory.path().join("late").exists());
}

fn initial_delivery_tree_script(directory: &Path) -> String {
    let descendant = encoded(&format!(
        "$ErrorActionPreference='Stop'; [IO.File]::WriteAllText({}, [string]$PID); Write-Output 'descendant-ready'; $deadline=[DateTime]::UtcNow.AddSeconds(30); while (!(Test-Path -LiteralPath {})) {{ if ([DateTime]::UtcNow -ge $deadline) {{ exit 7 }}; Start-Sleep -Milliseconds 10 }}; [IO.File]::WriteAllText({}, 'delivered-survivor')",
        quoted(&directory.join("ready")),
        quoted(&directory.join("survivor-release")),
        quoted(&directory.join("late"))
    ));
    format!(
        "$ErrorActionPreference='Stop'; $p=Start-Process -FilePath (Join-Path $PSHOME 'powershell.exe') -ArgumentList @('-NoProfile','-NonInteractive','-EncodedCommand','{descendant}') -PassThru -NoNewWindow; while (!(Test-Path -LiteralPath {})) {{ Start-Sleep -Milliseconds 10 }}; Write-Output 'leader-ready'; while (!(Test-Path -LiteralPath {})) {{ Start-Sleep -Milliseconds 10 }}; exit 0",
        quoted(&directory.join("ready")),
        quoted(&directory.join("release"))
    )
}

// A failing assertion must not leave the deliberately delivered descendant
// waiting for the test. This is only a fixture gate, never process authority.
struct InitialDeliveryGate(std::path::PathBuf);

impl Drop for InitialDeliveryGate {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.0, "finish delivered fixture");
    }
}

#[tokio::test]
async fn initial_exec_windows_delivery_closes_last_job_handle_without_killing_descendant() {
    let directory = tempfile::tempdir().unwrap();
    let survivor_release = directory.path().join("survivor-release");
    let gate = InitialDeliveryGate(survivor_release.clone());
    let (mut process, setup) = spawn_initial(&mut powershell(&initial_delivery_tree_script(
        directory.path(),
    )))
    .unwrap();
    if let Err(error) = setup {
        let cleanup = process.stop_and_reap().await;
        panic!("initial Windows setup failed: {error}; cleanup: {cleanup:?}");
    }
    let descendant = process_from_ready(directory.path()).await;
    let mut stdout = process.child.stdout.take().unwrap();
    std::fs::write(directory.path().join("release"), "exit leader").unwrap();
    let status = tokio::time::timeout(Duration::from_secs(15), process.child.wait()).await;
    let status = match status {
        Ok(status) => status,
        Err(error) => {
            let (cleanup, ()) = tokio::join!(process.stop_and_reap(), actually_stopped(descendant));
            panic!("initial Windows leader wait timed out: {error}; cleanup: {cleanup:?}");
        }
    };
    let survived_reap = is_running(&descendant);
    let mut output = Vec::new();
    // The pending descendant still owns its inherited pipe. A natural leader
    // exit must retain the existing two-second capture bound before delivery.
    let capture =
        tokio::time::timeout(Duration::from_secs(2), stdout.read_to_end(&mut output)).await;
    let survived_capture_bound = is_running(&descendant);
    if let Err(error) = process.disarm() {
        let (cleanup, ()) = tokio::join!(process.stop_and_reap(), actually_stopped(descendant));
        panic!("initial Windows disarm failed: {error}; cleanup: {cleanup:?}");
    }
    // There is no duplicate job handle in this test. disarm has closed the
    // last handle, so retaining a hidden kill-on-close job cannot mask a bug.
    let last_job_handle_closed = process.job.is_none();
    let survived_disarm = is_running(&descendant);
    drop(process);
    let survived_owner_drop = is_running(&descendant);
    drop(stdout);
    std::fs::write(&survivor_release, "finish normally").unwrap();
    actually_stopped(descendant).await;
    drop(gate);
    assert!(last_job_handle_closed);
    assert!(survived_disarm);
    assert!(status.unwrap().success());
    assert!(survived_reap);
    assert!(capture.is_err(), "delivered descendant pipe closed early");
    assert!(survived_capture_bound);
    assert!(survived_owner_drop);
    assert_eq!(
        std::fs::read_to_string(directory.path().join("late")).unwrap(),
        "delivered-survivor"
    );
}

#[tokio::test]
async fn initial_exec_windows_disarm_failure_retains_job_until_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let mut process = spawn_initial_tree(directory.path(), false).await;
    let descendant = process_from_ready(directory.path()).await;
    let disarm = process.disarm_with(|_| Err(io::Error::other("injected disarm failure")));
    let remained_armed = process.job.is_some();
    let descendant_was_live = is_running(&descendant);
    let (cleanup, ()) = tokio::join!(process.stop_and_reap(), actually_stopped(descendant));
    assert!(disarm.unwrap_err().to_string().contains("disarm failure"));
    assert!(remained_armed);
    assert!(descendant_was_live);
    assert!(!cleanup.unwrap().success());
    assert!(!directory.path().join("late").exists());
}

#[tokio::test]
async fn initial_exec_windows_setup_failure_retains_suspended_child_for_cleanup() {
    for assigned in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("must-not-execute");
        let mut observed = None;
        let (process, setup) = spawn_initial_with_setup(
            &mut powershell(&format!(
                "[IO.File]::WriteAllText({}, 'ran')",
                quoted(&marker)
            )),
            |job, child| {
                observed = Some(observe_process(child.id().unwrap()));
                if assigned {
                    job.assign(child)?;
                }
                Err(io::Error::other(
                    "injected initial assignment/resume failure",
                ))
            },
        )
        .expect("setup failure must return the real suspended child");
        let remained_armed = process.job.is_some();
        let (cleanup, ()) =
            tokio::join!(process.stop_and_reap(), actually_stopped(observed.unwrap()));
        assert!(setup.unwrap_err().to_string().contains("assignment/resume"));
        assert!(remained_armed);
        assert!(!cleanup.unwrap().success());
        assert!(
            !marker.exists(),
            "initial child executed before successful containment/resume"
        );
    }
}
