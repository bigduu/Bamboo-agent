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
    let descendant = encoded(&format!(
        "[IO.File]::WriteAllText({}, [string]$PID); Write-Output 'descendant-ready'; Start-Sleep -Seconds 30; [IO.File]::WriteAllText({}, 'escaped')",
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
        "$ErrorActionPreference='Stop'; $p=Start-Process -FilePath (Join-Path $PSHOME 'powershell.exe') -ArgumentList @('-NoProfile','-NonInteractive','-EncodedCommand','{descendant}') -PassThru -NoNewWindow; while (!(Test-Path -LiteralPath {})) {{ Start-Sleep -Milliseconds 10 }}; Write-Output 'leader-ready'; {tail}",
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
    let directory = tempfile::tempdir().unwrap();
    let scope = resources("background");
    let script = encoded(&tree_script(directory.path(), false));
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
    let descendant = process_from_ready(directory.path()).await;
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
