use super::*;
use crate::tools::WriteStdinTool;
use serde_json::Value;

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
    let mut ctx = ToolCtx::none("background-exec-test");
    ctx.session_id = Some(Arc::from(owner));
    let gate = gate.to_string_lossy().replace('\'', "'\\''");
    let result = payload(
        ExecCommandTool::new()
            .invoke(
                json!({"cmd": format!("while [ ! -f '{gate}' ]; do sleep 0.01; done; {suffix}"), "yield_time_ms": 250}),
                ctx.clone(),
            )
            .await
            .unwrap(),
    );
    assert_eq!(result["status"], "running");
    (ctx, result["session_id"].as_str().unwrap().to_owned())
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
