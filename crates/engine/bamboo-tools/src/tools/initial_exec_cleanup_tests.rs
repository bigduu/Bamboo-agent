//! Cleanup observability controls for the selected a4 owner. The P1 fork
//! controls only adapt their cleanup guard; assertions and deadlines are kept.
//! A test-only fault after real process reap exercises cleanup-error readout,
//! not an actual OS signal failure.
use super::*;
use crate::tools::WriteStdinTool;
use bamboo_infrastructure::process::{
    CommandEnvironmentDiagnostics, CommandEnvironmentSource, PythonDiscoveryDiagnostics,
};
use bamboo_infrastructure::test_support::override_command_environment;
use std::collections::HashMap;

fn context(owner: &str) -> ToolCtx {
    let mut ctx = ToolCtx::none("initial-cleanup-test");
    ctx.session_id = Some(Arc::from(owner));
    ctx
}

async fn cancel_real_initial(
    inject_error: bool,
    prepare_terminal: bool,
) -> (String, Arc<bash_runtime::ShellSession>, bool) {
    let _environment = override_command_environment(
        HashMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
        CommandEnvironmentDiagnostics {
            source: CommandEnvironmentSource::InheritedProcess,
            import_shell: None,
            import_error: None,
            path: Some("/usr/bin:/bin".into()),
            path_entries: Some(2),
            python: PythonDiscoveryDiagnostics {
                configured: None,
                resolved: None,
                invocation: None,
                source: None,
                tried: Vec::new(),
                tried_preview: Vec::new(),
                tried_total: 0,
                tried_truncated: false,
                hint: None,
            },
        },
    );
    assert!(crate::LocalTaskResources::current().is_none());
    let owner = format!("initial-cleanup-{}", uuid::Uuid::new_v4());
    let directory = tempfile::tempdir().unwrap();
    workspace_state::publish_resolved_workspace(&owner, directory.path().to_path_buf());
    let tool = ExecCommandTool::new();
    let command = if prepare_terminal {
        "printf prepared"
    } else {
        "exec /bin/sleep 30"
    };
    let mut invoke = tool.invoke(json!({"cmd":command}), context(&owner));
    assert!(futures::poll!(invoke.as_mut()).is_pending());
    let ids = bash_runtime::uncollected_polled_shells_for_session(&owner);
    let shell = bash_runtime::get_shell(&ids[0]).unwrap();
    let state = shell.polled.as_ref().unwrap();
    let prepared_retained = if prepare_terminal {
        timeout(
            Duration::from_secs(2),
            shell.initial_handoff.as_ref().unwrap().capture_ready(),
        )
        .await
        .expect("natural initial capture was not ready");
        // Use a4's real capture-ready boundary. The invoking future remains
        // unpolled, so delivery has not committed and its cursor is untouched.
        bash_runtime::get_shell(&shell.id).is_some()
    } else {
        true
    };
    if inject_error {
        state.record_error("earlier capture error".into()).await;
        state.fail_cleanup_after_reap.store(true, Ordering::Release);
    }
    // The sleeping case has not polled its supervisors; the natural-exit
    // case has only prepared output. Neither has completed actual cleanup.
    // Cancellation must preserve the registry while that cleanup starts.
    let unreaped = !state.process_exited();
    drop(invoke);
    let retained_before_cleanup =
        prepared_retained && unreaped && bash_runtime::get_shell(&shell.id).is_some();
    state.wait(2000).await;
    if !state.is_complete().await {
        shell.kill().await.unwrap();
        state.wait(2000).await;
    }
    // Cleanup is checked before assertions about the earlier retention result.
    assert!(
        state.process_exited(),
        "cleanup control did not reap its actual child"
    );
    assert!(
        state.is_complete().await,
        "cleanup control did not finish capture"
    );
    (owner, shell, retained_before_cleanup)
}

#[tokio::test]
async fn cancelled_initial_success_keeps_entry_until_real_cleanup_then_removes_it() {
    let (_, shell, retained) = cancel_real_initial(false, false).await;
    let removed = bash_runtime::get_shell(&shell.id).is_none();
    assert!(
        retained,
        "cancellation removed the only entry before cleanup"
    );
    assert!(
        removed,
        "successful initial cleanup left its registry entry"
    );
}

#[tokio::test]
async fn cancelled_initial_cleanup_error_stays_owner_readable() {
    let (owner, shell, retained_before_cleanup) = cancel_real_initial(true, false).await;
    let retained_error = bash_runtime::get_shell(&shell.id).is_some();
    let denied = WriteStdinTool::new()
        .invoke(json!({"session_id":shell.id}), context("different-owner"))
        .await;
    let readable = WriteStdinTool::new()
        .invoke(json!({"session_id":shell.id}), context(&owner))
        .await;
    // Remove explicitly even if a regression prevents the normal terminal
    // continuation from consuming this already-reaped result.
    bash_runtime::remove_shell(&shell.id);
    assert!(retained_before_cleanup && retained_error);
    assert!(matches!(denied, Err(ToolError::Execution(_))));
    let ToolOutcome::Completed(result) = readable.unwrap() else {
        panic!("expected ordinary result");
    };
    let value: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(value["status"], "error");
    assert_eq!(
        value["error"],
        "Shell wait failed: controlled post-reap cleanup error"
    );
    assert!(value.get("session_id").is_none());
    assert!(bash_runtime::get_shell(&shell.id).is_none());
}

#[tokio::test]
async fn prepared_initial_cleanup_error_stays_owner_readable() {
    let (owner, shell, retained) = cancel_real_initial(true, true).await;
    let readable = WriteStdinTool::new()
        .invoke(json!({"session_id":shell.id}), context(&owner))
        .await;
    bash_runtime::remove_shell(&shell.id);
    assert!(
        retained,
        "prepared initial result removed its entry before delivery committed"
    );
    let ToolOutcome::Completed(result) = readable.unwrap() else {
        panic!("expected ordinary result");
    };
    let value: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(value["status"], "error");
    assert_eq!(
        value["error"],
        "Shell wait failed: controlled post-reap cleanup error"
    );
}

#[tokio::test]
async fn cancelled_initial_cleanup_error_keeps_existing_bounded_cache_ttl() {
    let (_, shell, retained_before_cleanup) = cancel_real_initial(true, false).await;
    let retained_error = bash_runtime::get_shell(&shell.id).is_some();
    // Only advance virtual time after actual OS wait and capture have ended.
    tokio::task::yield_now().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(301)).await;
    tokio::task::yield_now().await;
    let expired = bash_runtime::get_shell(&shell.id).is_none();
    bash_runtime::remove_shell(&shell.id);
    assert!(retained_before_cleanup && retained_error);
    assert!(
        expired,
        "failed initial result exceeded the existing 300-second cache TTL"
    );
}
