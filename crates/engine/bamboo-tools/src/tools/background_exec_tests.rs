use super::*;
use crate::tools::WriteStdinTool;
use serde_json::Value;
use tokio::time::timeout;

struct ReleaseGate(std::path::PathBuf);

impl Drop for ReleaseGate {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.0, b"release");
    }
}

fn payload(outcome: ToolOutcome) -> Value {
    let ToolOutcome::Completed(result) = outcome else {
        panic!("ordinary exec result");
    };
    serde_json::from_str(&result.result).unwrap()
}

async fn start(owner: &str, gate: &Path, suffix: &str) -> (ToolCtx, String) {
    let gate = gate.to_string_lossy().replace('\'', "'\\''");
    launch(
        owner,
        format!("while [ ! -f '{gate}' ]; do sleep 0.01; done; {suffix}"),
    )
    .await
}

async fn launch(owner: &str, command: String) -> (ToolCtx, String) {
    let mut ctx = ToolCtx::none("background-exec-test");
    ctx.session_id = Some(Arc::from(owner));
    let result = payload(
        ExecCommandTool::new()
            .invoke(json!({"cmd": command, "yield_time_ms": 250}), ctx.clone())
            .await
            .unwrap(),
    );
    assert_eq!(result["status"], "running");
    (ctx, result["session_id"].as_str().unwrap().to_owned())
}

#[cfg(unix)]
#[tokio::test]
async fn background_exec_stop_reaps_noninteractive_command_group_and_leaves_other_handle_running() {
    use BackgroundExecStopOutcome::{NotFound, Stopped};

    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let pid_file = dir.path().join("descendant.pid");
    let path = |path: &Path| path.to_string_lossy().replace('\'', "'\\''");
    let command = format!(
        "(while [ ! -f '{}' ]; do sleep 0.01; done) & printf '%s\\n' \"$!\" > '{}'; wait",
        path(&gate),
        path(&pid_file),
    );
    let owner = "stop-background-owner";
    let (_, handle) = launch(owner, command).await;
    let (_, other) = start(owner, &gate, "printf neighboring-command-finished").await;
    let descendant = std::fs::read_to_string(&pid_file).unwrap();
    assert!(descendant.trim().parse::<u32>().unwrap() > 1);
    assert_eq!(exec_wait_target(owner, &handle).await, Some(true));
    let job = reserve_background_job(owner, &handle).unwrap();
    let other_job = reserve_background_job(owner, &other).unwrap();
    assert!(!job.stop_confirmed().await);
    assert_eq!(
        stop_owned_background_exec("other-owner", &handle).await,
        NotFound
    );
    assert_eq!(
        stop_owned_background_exec(owner, "unknown-command").await,
        NotFound
    );
    assert_eq!(
        bash_runtime::get_shell(&handle).unwrap().status(),
        "running"
    );

    let (first, retry) = tokio::join!(
        stop_owned_background_exec(owner, &handle),
        stop_owned_background_exec(owner, &handle),
    );
    assert_eq!((first, retry), (Stopped, Stopped));
    assert_eq!(job.completion().await.status, "killed");
    assert!(job.stop_confirmed().await);
    let observed = std::process::Command::new("/bin/ps")
        .args(["-o", "state=", "-p", descendant.trim()])
        .output()
        .unwrap();
    let state = std::str::from_utf8(&observed.stdout).unwrap().trim();
    assert!(
        state.is_empty() || state.starts_with('Z'),
        "descendant state: {state}"
    );
    assert_eq!(bash_runtime::get_shell(&other).unwrap().status(), "running");

    tokio::fs::write(&gate, b"release").await.unwrap();
    let completion = timeout(Duration::from_secs(5), other_job.completion())
        .await
        .unwrap();
    assert_eq!(completion.exit_code, Some(0));
    assert!(other_job.stop_confirmed().await);
    assert!(completion
        .output_tail
        .contains("neighboring-command-finished"));
}

#[tokio::test]
async fn background_exec_stop_does_not_confirm_failed_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let owner = "unconfirmed-stop-owner";
    let (_, handle) = start(owner, &gate, "exit 0").await;
    let shell = bash_runtime::get_shell(&handle).unwrap();
    let capture = shell.polled.as_ref().unwrap();
    capture
        .fail_cleanup_after_reap
        .store(true, Ordering::Release);

    assert_eq!(
        stop_owned_background_exec(owner, &handle).await,
        BackgroundExecStopOutcome::Unconfirmed,
    );
    assert!(capture.process_exited());
    assert!(capture.is_complete().await);
}

#[tokio::test]
async fn background_exec_stop_accepts_an_already_reaped_empty_command_group() {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let owner = "naturally-finished-stop-owner";
    let (_, handle) = start(owner, &gate, "exit 0").await;
    let job = reserve_background_job(owner, &handle).unwrap();
    tokio::fs::write(&gate, b"release").await.unwrap();
    let completion = timeout(Duration::from_secs(5), job.completion())
        .await
        .unwrap();
    assert_eq!(completion.exit_code, Some(0));
    assert!(job.stop_confirmed().await);
    assert_eq!(
        stop_owned_background_exec(owner, &handle).await,
        BackgroundExecStopOutcome::Stopped,
    );
}

#[cfg(unix)]
#[tokio::test]
async fn background_exec_stop_does_not_signal_a_group_after_reaping_its_leader() {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("leader-release");
    let descendant_gate = dir.path().join("descendant-release");
    let descendant_finished = dir.path().join("descendant-finished");
    let _release_leader = ReleaseGate(gate.clone());
    let _release_descendant = ReleaseGate(descendant_gate.clone());
    let path = |path: &Path| path.to_string_lossy().replace('\'', "'\\''");
    let command = format!(
        "(while [ ! -f '{}' ]; do sleep 0.01; done; : > '{}') </dev/null >/dev/null 2>&1 & \
         while [ ! -f '{}' ]; do sleep 0.01; done; exit 0",
        path(&descendant_gate),
        path(&descendant_finished),
        path(&gate),
    );
    let owner = "reaped-leader-stop-owner";
    let (_, handle) = launch(owner, command).await;
    let job = reserve_background_job(owner, &handle).unwrap();
    tokio::fs::write(&gate, b"release").await.unwrap();
    let completion = timeout(Duration::from_secs(5), job.completion())
        .await
        .unwrap();
    assert_eq!(completion.exit_code, Some(0));
    assert!(!job.stop_confirmed().await);
    assert_eq!(
        stop_owned_background_exec(owner, &handle).await,
        BackgroundExecStopOutcome::Unconfirmed,
    );
    tokio::fs::write(&descendant_gate, b"release")
        .await
        .unwrap();
    timeout(Duration::from_secs(5), async {
        while !descendant_finished.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Stop must not signal a process group after its retained leader was reaped");
}

#[tokio::test]
async fn background_exec_observes_real_failure_without_consuming_the_pull_result() {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let (ctx, handle) = start("background-owner", &gate, "printf '后台结果'; exit 7").await;
    assert!(reserve_background_job("different-owner", &handle).is_none());
    let job = reserve_background_job("background-owner", &handle).unwrap();
    assert!(reserve_background_job("background-owner", &handle).is_none());
    assert!(timeout(Duration::from_millis(20), job.completion())
        .await
        .is_err());
    assert_eq!(
        bash_runtime::get_shell(&handle).unwrap().status(),
        "running"
    );
    drop(job);
    let job = reserve_background_job("background-owner", &handle).unwrap();
    tokio::fs::write(&gate, b"release").await.unwrap();
    let info = timeout(Duration::from_secs(5), job.completion())
        .await
        .unwrap();
    assert_eq!(info.bash_id, handle);
    assert_eq!(info.exit_code, Some(7));
    assert_eq!(info.status, "completed");
    let snapshot: Value = serde_json::from_str(&info.output_tail).unwrap();
    assert_eq!(snapshot["output"], "后台结果");
    assert_eq!(snapshot["exit_code"], 7);
    assert!(bash_runtime::get_shell(&handle).is_some());
    let pulled = payload(
        WriteStdinTool::new()
            .invoke(json!({"session_id": handle}), ctx)
            .await
            .unwrap(),
    );
    assert_eq!(pulled["output"], snapshot["output"]);
    assert_eq!(pulled["exit_code"], 7);
    assert!(bash_runtime::get_shell(&handle).is_none());
}

#[tokio::test]
async fn background_exec_can_observe_completion_before_handoff_with_bounded_output() {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let (ctx, handle) = start(
        "already-finished-owner",
        &gate,
        "printf '%16000s' x; exit 3",
    )
    .await;
    let shell = bash_runtime::get_shell(&handle).unwrap();
    tokio::fs::write(&gate, b"release").await.unwrap();
    timeout(Duration::from_secs(5), async {
        while !shell.polled.as_ref().unwrap().is_complete().await {
            shell.polled.as_ref().unwrap().wait(100).await;
        }
    })
    .await
    .unwrap();
    let job = reserve_background_job("already-finished-owner", &handle).unwrap();
    let info = timeout(Duration::from_secs(1), job.completion())
        .await
        .unwrap();
    let snapshot: Value = serde_json::from_str(&info.output_tail).unwrap();
    assert_eq!(info.exit_code, Some(3));
    assert!(info.output_tail.len() < 4608);
    assert!(snapshot["output_omitted_bytes"].as_u64().unwrap() > 10_000);
    let pulled = payload(
        WriteStdinTool::new()
            .invoke(
                json!({"session_id": handle, "max_output_tokens": 8192}),
                ctx,
            )
            .await
            .unwrap(),
    );
    assert_eq!(pulled["output"].as_str().unwrap().len(), 16_000);
    assert_eq!(pulled["exit_code"], 3);
}

#[tokio::test]
async fn background_exec_preserves_capture_error_instead_of_reporting_success() {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let (ctx, handle) = start("capture-error-owner", &gate, "exit 0").await;
    let shell = bash_runtime::get_shell(&handle).unwrap();
    shell
        .polled
        .as_ref()
        .unwrap()
        .record_error("controlled capture failure".into())
        .await;
    let job = reserve_background_job("capture-error-owner", &handle).unwrap();
    tokio::fs::write(&gate, b"release").await.unwrap();
    let info = timeout(Duration::from_secs(5), job.completion())
        .await
        .unwrap();
    assert_eq!(info.status, "error");
    let snapshot: Value = serde_json::from_str(&info.output_tail).unwrap();
    assert_eq!(snapshot["error"], "controlled capture failure");
    let pulled = payload(
        WriteStdinTool::new()
            .invoke(json!({"session_id": handle}), ctx)
            .await
            .unwrap(),
    );
    assert_eq!(pulled["status"], "error");
    assert_eq!(pulled["error"], snapshot["error"]);
}
