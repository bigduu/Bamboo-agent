//! Exact-Root cancellation through the real Supervisor tool, store and router.
//! The controlled runners are in-memory fixtures, not native provider runs.

use super::*;
use std::sync::atomic::Ordering;

use bamboo_domain::SupervisorCurrentUserTarget;
use bamboo_tools::permission::{PermissionMatcher, PermissionMatcherKind, PermissionType};

async fn current_target(fixture: &Fixture) -> SupervisorCurrentUserTarget {
    fixture
        .state
        .storage
        .inspect_supervisor_current_user_target(&fixture.reference, TARGET)
        .await
        .unwrap()
}

fn grant_cancel(fixture: &Fixture) {
    fixture
        .state
        .permission_checker
        .permission_config()
        .unwrap()
        .grant_typed_scoped_session_permission(
            &fixture.reference.session_id,
            PermissionType::ExecuteCommand,
            PermissionMatcher {
                id: "fixture-exact-cancel".into(),
                kind: PermissionMatcherKind::ExactResource,
                value: format!("session_control cancel {TARGET}"),
            },
        )
        .unwrap();
}

async fn invoke(fixture: &Fixture, arguments: Value) -> Result<Value, ToolError> {
    let supervisor = fixture
        .state
        .storage
        .load_execution_admission_snapshot(&fixture.reference.session_id)
        .await
        .unwrap()
        .unwrap();
    let call = ToolCall {
        id: "fixture-exact-cancel-call".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "session_control".into(),
            arguments: arguments.to_string(),
        },
    };
    let mut ctx = ToolExecutionContext::none(&call.id);
    ctx.session_id = Some(&supervisor.id);
    ctx.root_session_id = Some(&supervisor.id);
    ctx = ctx.with_executing_supervisor(
        ExecutingSupervisorObservation::capture_from_executing_session(&supervisor),
    );
    let outcome = bamboo_agent_core::tools::with_executing_session_caller(
        Some(Arc::new(bamboo_domain::SessionCallerReference::from(
            &supervisor,
        ))),
        fixture
            .state
            .tools_for(ToolSurface::Root)
            .execute_with_context_outcome(&call, ctx),
    )
    .await?;
    let ToolOutcome::Completed(result) = outcome else {
        panic!("exact cancellation must return its own synchronous observation");
    };
    assert!(result.success, "{}", result.result);
    Ok(serde_json::from_str(&result.result).unwrap())
}

fn args(action: &str, target: &SupervisorCurrentUserTarget, run: Option<&str>) -> Value {
    let mut value = json!({"action": action, "target_session_id": TARGET, "target_ref": target});
    if let Some(run) = run {
        value["expected_run_id"] = json!(run);
    }
    value
}

#[actix_web::test]
async fn supervisor_exact_cancel_reports_request_then_finalization_and_fences_successor() {
    let fixture = Box::pin(Fixture::new()).await;
    grant_cancel(&fixture);
    let target = current_target(&fixture).await;
    let before = serde_json::to_value(fixture.reload().await).unwrap();
    let inbox_before = fixture.state.session_inbox.inspect(TARGET).await.unwrap();

    let mut old = crate::app_state::AgentRunner::new();
    old.status = crate::app_state::AgentStatus::Running;
    let old_run = old.run_id.clone();
    let old_token = old.cancel_token.clone();
    let old_owner = fixture
        .state
        .session_activation_router
        .register_session_run(&fixture.original, &old_run)
        .await
        .unwrap();
    fixture
        .state
        .agent_runners
        .write()
        .await
        .insert(TARGET.into(), old);

    let observed = invoke(&fixture, args("observe_run", &target, None))
        .await
        .unwrap();
    assert_eq!(observed["run_id"], old_run);
    let requested = invoke(&fixture, args("cancel", &target, Some(&old_run)))
        .await
        .unwrap();
    assert_eq!(requested["status"], "cancel_requested");
    assert_eq!(requested["run_id"], old_run);
    assert_eq!(requested["completion_verified"], false);
    assert!(old_token.is_cancelled());
    {
        let runners = fixture.state.agent_runners.read().await;
        let old = runners.get(TARGET).unwrap();
        assert!(matches!(old.status, crate::app_state::AgentStatus::Running));
        assert!(old.completed_at.is_none());
    }
    assert_eq!(
        fixture.state.session_inbox.inspect(TARGET).await.unwrap(),
        inbox_before
    );
    assert_eq!(
        serde_json::to_value(fixture.reload().await).unwrap(),
        before
    );

    let result: Result<(), bamboo_agent_core::AgentError> =
        Err(bamboo_agent_core::AgentError::Cancelled);
    assert!(
        bamboo_engine::execution::finalize_runner_exact(
            &fixture.state.agent_runners,
            TARGET,
            &old_run,
            &result,
        )
        .await
    );
    old_owner.finish(0).await.unwrap();
    // An ordinary metadata-only save may advance the selector while the
    // Root lifetime and authority remain intact.
    let mut updated = fixture.reload().await;
    updated.title = "metadata updated after finalization".into();
    updated.metadata_version += 1;
    fixture.state.storage.save_session(&updated).await.unwrap();
    let fresh = current_target(&fixture).await;
    assert_ne!(fresh.metadata_version, target.metadata_version);
    let after_metadata = serde_json::to_value(fixture.reload().await).unwrap();
    let finished = invoke(&fixture, args("cancel", &fresh, Some(&old_run)))
        .await
        .unwrap();
    assert_eq!(finished["status"], "already_finished");

    let mut successor = crate::app_state::AgentRunner::new();
    successor.status = crate::app_state::AgentStatus::Running;
    let successor_id = successor.run_id.clone();
    let successor_token = successor.cancel_token.clone();
    fixture
        .state
        .agent_runners
        .write()
        .await
        .insert(TARGET.into(), successor);
    let successor_owner = fixture
        .state
        .session_activation_router
        .register_session_run(&fixture.original, &successor_id)
        .await
        .unwrap();
    let stale = invoke(&fixture, args("cancel", &fresh, Some(&old_run)))
        .await
        .unwrap();
    assert_eq!(stale["status"], "run_conflict");
    assert!(!successor_token.is_cancelled());
    assert_eq!(
        fixture.state.session_inbox.inspect(TARGET).await.unwrap(),
        inbox_before
    );
    assert_eq!(
        serde_json::to_value(fixture.reload().await).unwrap(),
        after_metadata
    );
    let successor_result: Result<(), bamboo_agent_core::AgentError> = Ok(());
    assert!(
        bamboo_engine::execution::finalize_runner_exact(
            &fixture.state.agent_runners,
            TARGET,
            &successor_id,
            &successor_result,
        )
        .await
    );
    successor_owner.finish(0).await.unwrap();
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn supervisor_exact_cancel_rejects_stale_target_and_invalid_run_without_effect() {
    let fixture = Box::pin(Fixture::new()).await;
    grant_cancel(&fixture);
    let target = current_target(&fixture).await;
    let mut runner = crate::app_state::AgentRunner::new();
    runner.status = crate::app_state::AgentStatus::Running;
    let run_id = runner.run_id.clone();
    let token = runner.cancel_token.clone();
    let owner = fixture
        .state
        .session_activation_router
        .register_session_run(&fixture.original, &run_id)
        .await
        .unwrap();
    fixture
        .state
        .agent_runners
        .write()
        .await
        .insert(TARGET.into(), runner);
    let inbox_before = fixture.state.session_inbox.inspect(TARGET).await.unwrap();

    let mut stale = target.clone();
    stale.created_at += chrono::Duration::seconds(1);
    assert!(invoke(&fixture, args("cancel", &stale, Some(&run_id)))
        .await
        .is_err());
    assert!(
        invoke(&fixture, args("cancel", &target, Some("invalid run")))
            .await
            .is_err()
    );
    assert!(invoke(&fixture, args("cancel", &target, None))
        .await
        .is_err());
    // Bypass the executor's permission layer to verify that the native tool
    // still rejects an ordinary caller with no Supervisor observation.
    let tool = fixture
        .state
        .tools_for(ToolSurface::Root)
        .tool_instance_for_exact("session_control")
        .unwrap();
    let mut ordinary = ToolCtx::none("ordinary-cancel-attempt");
    ordinary.session_id = Some(Arc::from(TARGET));
    ordinary.executing_caller = Some(Arc::new(bamboo_domain::SessionCallerReference::from(
        &fixture.original,
    )));
    assert!(tool
        .invoke(args("cancel", &target, Some(&run_id)), ordinary)
        .await
        .is_err());
    assert!(!token.is_cancelled());
    assert_eq!(
        fixture.state.session_inbox.inspect(TARGET).await.unwrap(),
        inbox_before
    );
    assert_eq!(fixture.probe.requests.lock().unwrap().len(), 0);
    assert!(!fixture.probe.blocked.load(Ordering::SeqCst));
    owner.abandon().await;
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn legacy_stop_status_is_not_exact_finalization_proof() {
    let fixture = Box::pin(Fixture::new()).await;
    grant_cancel(&fixture);
    let target = current_target(&fixture).await;
    let mut runner = crate::app_state::AgentRunner::new();
    runner.status = crate::app_state::AgentStatus::Running;
    let run_id = runner.run_id.clone();
    let owner = fixture
        .state
        .session_activation_router
        .register_session_run(&fixture.original, &run_id)
        .await
        .unwrap();
    fixture
        .state
        .agent_runners
        .write()
        .await
        .insert(TARGET.into(), runner);
    invoke(&fixture, args("observe_run", &target, None))
        .await
        .unwrap();
    {
        // This is precisely what the legacy HTTP Stop path can expose before
        // the original executor has finished; it is not a terminal receipt.
        let mut runners = fixture.state.agent_runners.write().await;
        let runner = runners.get_mut(TARGET).unwrap();
        runner.cancel_token.cancel();
        runner.status = crate::app_state::AgentStatus::Cancelled;
        runner.completed_at = Some(chrono::Utc::now());
    }
    let pending = invoke(&fixture, args("cancel", &target, Some(&run_id)))
        .await
        .unwrap();
    assert_eq!(pending["status"], "cancel_requested");
    assert_eq!(pending["completion_verified"], false);
    let result: Result<(), bamboo_agent_core::AgentError> =
        Err(bamboo_agent_core::AgentError::Cancelled);
    assert!(
        bamboo_engine::execution::finalize_runner_exact(
            &fixture.state.agent_runners,
            TARGET,
            &run_id,
            &result,
        )
        .await
    );
    owner.finish(0).await.unwrap();
    let finished = invoke(&fixture, args("cancel", &target, Some(&run_id)))
        .await
        .unwrap();
    assert_eq!(finished["status"], "already_finished");
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn supervisor_cancel_rejects_old_owner_after_same_id_root_recreation() {
    let fixture = Box::pin(Fixture::new()).await;
    grant_cancel(&fixture);
    let mut old = crate::app_state::AgentRunner::new();
    old.status = crate::app_state::AgentStatus::Running;
    let old_run = old.run_id.clone();
    let old_token = old.cancel_token.clone();
    let owner = fixture
        .state
        .session_activation_router
        .register_session_run(&fixture.original, &old_run)
        .await
        .unwrap();
    fixture
        .state
        .agent_runners
        .write()
        .await
        .insert(TARGET.into(), old);
    let old_ref = current_target(&fixture).await;
    invoke(&fixture, args("observe_run", &old_ref, None))
        .await
        .unwrap();

    // Simulate a stale old Host owner outliving the canonical deletion; the
    // new Root is real storage with the same ID and a different birth.
    fixture.state.storage.delete_session(TARGET).await.unwrap();
    let mut replacement = Session::new(TARGET, "replacement-model");
    replacement.created_at = fixture.original.created_at + chrono::Duration::seconds(1);
    replacement.set_project_id_meta(fixture.original.project_id_meta().unwrap());
    replacement.set_workspace_path_meta(fixture.original.workspace_path_meta().unwrap());
    replacement.add_message(Message::user("replacement Root content"));
    fixture
        .state
        .storage
        .save_session(&replacement)
        .await
        .unwrap();
    let fresh_ref = current_target(&fixture).await;
    assert_ne!(fresh_ref.created_at, old_ref.created_at);
    let before = serde_json::to_value(fixture.reload().await).unwrap();
    let inbox_before = fixture.state.session_inbox.inspect(TARGET).await.unwrap();

    assert!(invoke(&fixture, args("cancel", &old_ref, Some(&old_run)))
        .await
        .is_err());
    let conflict = invoke(&fixture, args("cancel", &fresh_ref, Some(&old_run)))
        .await
        .unwrap();
    assert_eq!(conflict["status"], "run_conflict");
    assert!(!old_token.is_cancelled());
    assert_eq!(
        fixture.state.session_inbox.inspect(TARGET).await.unwrap(),
        inbox_before
    );
    assert_eq!(
        serde_json::to_value(fixture.reload().await).unwrap(),
        before
    );
    owner.abandon().await;
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn supervisor_cancel_real_root_keeps_queued_followup_for_successor() {
    let fixture = Box::pin(Fixture::new()).await;
    grant_cancel(&fixture);
    fixture.probe.blocked.store(true, Ordering::SeqCst);
    let entered = fixture.probe.entered.notified();
    fixture
        .followup("cancel-start", "start the original work")
        .await;
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("the actual local provider must enter its held first run");
    let selected = current_target(&fixture).await;
    let observed = invoke(&fixture, args("observe_run", &selected, None))
        .await
        .unwrap();
    let run_id = observed["run_id"].as_str().unwrap().to_owned();
    let queued = fixture
        .followup("cancel-queued", "preserve this queued followup")
        .await;
    let fresh = current_target(&fixture).await;
    let requested = invoke(&fixture, args("cancel", &fresh, Some(&run_id)))
        .await
        .unwrap();
    assert_eq!(requested["status"], "cancel_requested");
    assert_eq!(requested["completion_verified"], false);
    fixture.probe.release.notify_one();
    fixture.settled(2).await;

    let after = fixture.reload().await;
    fixture.assert_context(&after);
    assert_eq!(
        after
            .messages
            .iter()
            .filter(|message| message.id == queued["receipt_id"].as_str().unwrap())
            .count(),
        1,
        "cancelling the old run must not discard or duplicate the queued input"
    );
    assert_eq!(fixture.probe.requests.lock().unwrap().len(), 2);
    fixture.state.shutdown().await;
}
