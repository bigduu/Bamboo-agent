//! Manual live smoke for Codex app-server mode.
//!
//! Run explicitly with an authenticated Codex installation:
//! `cargo test --test e2e_codex_app_server_manual -- --ignored --nocapture`

use std::sync::Arc;
use std::time::Duration;

use bamboo_agent::codex_app_server_executor::CodexAppServerExecutor;
use bamboo_agent::codex_cli_executor::{
    resolve_codex_app_server_permission_config, CodexAuthConfig,
};
use bamboo_subagent::executor::{ChildExecutor, EventSink, HostBridge, SteerInbox};
use bamboo_subagent::proto::{PermissionPolicyContext, RunSecrets, RunSpec};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[tokio::test]
#[ignore = "requires an authenticated real Codex CLI and intentionally exercises allow/deny command approvals"]
async fn live_app_server_relays_allow_and_deny_across_resume() {
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let home = std::env::var_os("HOME").expect("HOME is required for the live sandbox probe");
    let marker = std::path::PathBuf::from(home).join(format!(
        ".bamboo-codex-app-server-approval-{}",
        std::process::id()
    ));
    let _ = tokio::fs::remove_file(&marker).await;

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
        None,
        None,
        Some(workspace.path().to_string_lossy().into_owned()),
        Some(state.path().to_path_buf()),
        Vec::new(),
        CodexAuthConfig::inherit(),
        permissions,
    )
    .await
    .unwrap();

    let (sink, mut event_rx) = EventSink::channel();
    let (host, mut host_rx) = HostBridge::channel();
    let approvals = Arc::new(Mutex::new(Vec::new()));
    let seen = approvals.clone();
    let approval_task = tokio::spawn(async move {
        let mut decisions = std::collections::VecDeque::from([true, false]);
        while let Some(request) = host_rx.recv().await {
            seen.lock().await.push(request.body.clone());
            let approved = decisions.pop_front().unwrap_or(false);
            let _ = request
                .reply
                .send(serde_json::json!({"approved": approved}));
        }
    });
    let prompt = format!(
        "Use a shell command to create exactly this file outside the workspace, then report whether it succeeded: {}",
        marker.display()
    );
    let outcome = tokio::time::timeout(
        Duration::from_secs(180),
        executor.run(
            RunSpec {
                assignment: prompt,
                logical_session: None,
                project_id: None,
                reasoning_effort: None,
                permission_policy: Some(PermissionPolicyContext {
                    revision: 1,
                    requested_mode: "default".to_string(),
                    effective_mode: "default".to_string(),
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    session_id: "manual-codex-app-server".to_string(),
                    workspace_path: Some(workspace.path().to_string_lossy().into_owned()),
                    inherit_session_grants: false,
                    policy: serde_json::to_value(
                        bamboo_tools::permission::SerializablePermissionConfig::default(),
                    )
                    .unwrap(),
                }),
                messages: Vec::new(),
                activation_run_id: None,
                initial_session_messages: Vec::new(),
                secrets: RunSecrets::default(),
            },
            sink.with_host_bridge(host.clone()),
            SteerInbox::disconnected(),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("live Codex turn timeout");
    assert_eq!(
        outcome.status,
        bamboo_subagent::proto::TerminalStatus::Completed
    );
    let allowed_executed = marker.exists();
    let _ = tokio::fs::remove_file(&marker).await;
    assert!(allowed_executed, "approved command did not execute");
    let mut saw_complete = false;
    while let Ok(event) = event_rx.try_recv() {
        saw_complete |= event["type"] == "complete";
    }
    assert!(saw_complete);

    let denied_marker = marker.with_file_name(format!(
        ".bamboo-codex-app-server-denied-{}",
        std::process::id()
    ));
    let _ = tokio::fs::remove_file(&denied_marker).await;
    let (deny_sink, mut deny_events) = EventSink::channel();
    let deny_outcome = tokio::time::timeout(
        Duration::from_secs(180),
        executor.run(
            RunSpec {
                assignment: format!(
                    "Again use a shell command to create this exact file outside the workspace, then explain the result: {}",
                    denied_marker.display()
                ),
                logical_session: None,
                project_id: None,
                reasoning_effort: None,
                permission_policy: Some(PermissionPolicyContext {
                    revision: 2,
                    requested_mode: "default".to_string(),
                    effective_mode: "default".to_string(),
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    session_id: "manual-codex-app-server".to_string(),
                    workspace_path: Some(workspace.path().to_string_lossy().into_owned()),
                    inherit_session_grants: false,
                    policy: serde_json::to_value(
                        bamboo_tools::permission::SerializablePermissionConfig::default(),
                    )
                    .unwrap(),
                }),
                messages: vec![serde_json::json!({"role": "user", "content": "prior turn"})],
                activation_run_id: None,
                initial_session_messages: Vec::new(),
                secrets: RunSecrets::default(),
            },
            deny_sink.with_host_bridge(host),
            SteerInbox::disconnected(),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("live Codex deny turn timeout");
    approval_task.abort();

    let denied_executed = denied_marker.exists();
    let _ = tokio::fs::remove_file(&denied_marker).await;
    assert_eq!(
        deny_outcome.status,
        bamboo_subagent::proto::TerminalStatus::Completed
    );
    assert!(!denied_executed, "denied command unexpectedly executed");
    assert!(
        deny_outcome
            .result
            .as_deref()
            .is_some_and(|text| !text.is_empty()),
        "model did not receive/surface the denied tool result"
    );
    assert!(
        approvals.lock().await.len() >= 2,
        "expected allow and deny approvals"
    );
    assert!(
        std::iter::from_fn(|| deny_events.try_recv().ok()).any(|event| event["type"] == "complete")
    );
}
