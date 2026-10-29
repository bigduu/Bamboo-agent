//! Exact-Root cancellation through the real Supervisor tool, store and router.
//! The controlled runners are in-memory fixtures, not native provider runs.

use super::*;
use std::sync::atomic::Ordering;

use bamboo_domain::{Storage, SupervisorCurrentUserTarget};
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
    let value: Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(result.success, value["status"] != "run_conflict");
    Ok(value)
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
async fn stopped_bound_run_cannot_be_rebound_to_a_changed_workspace() {
    for previously_observed in [true, false] {
        let fixture = Box::pin(Fixture::new()).await;
        grant_cancel(&fixture);
        let original = current_target(&fixture).await;
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
        let workspace = fixture._home.path().join("changed-target-workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        if previously_observed {
            let observation = invoke(&fixture, args("observe_run", &original, None))
                .await
                .unwrap();
            assert_eq!(observation["status"], "running");
            // A Root may itself switch Workspace while still running. This
            // slice conservatively refuses to rebind the old observation.
            let mut changed = fixture.reload().await;
            let original_workspace = changed.workspace_path_meta().unwrap();
            changed.set_workspace_path_meta(workspace.canonicalize().unwrap().to_string_lossy());
            changed.metadata_version += 1;
            fixture.state.storage.save_session(&changed).await.unwrap();
            let switched = current_target(&fixture).await;
            let refused = invoke(&fixture, args("observe_run", &switched, None))
                .await
                .unwrap();
            assert_eq!(refused["status"], "run_conflict");
            assert!(!token.is_cancelled());
            changed.set_workspace_path_meta(original_workspace);
            changed.metadata_version += 1;
            fixture.state.storage.save_session(&changed).await.unwrap();
        }
        assert!(crate::handlers::agent::stop::cancel_session(&fixture.state, TARGET).await);
        assert!(token.is_cancelled());
        assert!(!fixture
            .state
            .agent_runners
            .read()
            .await
            .get(TARGET)
            .unwrap()
            .exact_execution_finalized());
        if !previously_observed {
            let unproven = invoke(&fixture, args("observe_run", &original, None))
                .await
                .unwrap();
            assert_eq!(unproven["status"], "run_conflict");
        }

        let mut changed = fixture.reload().await;
        changed.set_workspace_path_meta(workspace.canonicalize().unwrap().to_string_lossy());
        changed.metadata_version += 1;
        fixture.state.storage.save_session(&changed).await.unwrap();
        let fresh = current_target(&fixture).await;
        let observed = invoke(&fixture, args("observe_run", &fresh, None))
            .await
            .unwrap();
        assert_eq!(observed["status"], "run_conflict");
        let cancelled = invoke(&fixture, args("cancel", &fresh, Some(&run_id)))
            .await
            .unwrap();
        assert_eq!(cancelled["status"], "run_conflict");

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
        let finished = invoke(&fixture, args("cancel", &fresh, Some(&run_id)))
            .await
            .unwrap();
        assert_eq!(finished["status"], "run_conflict");
        assert!(fixture.probe.requests.lock().unwrap().is_empty());
        fixture.state.shutdown().await;
    }
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
    let mut replacement = fixture
        .state
        .session_store
        .recreate_root_session(TARGET, "replacement-model")
        .await
        .unwrap();
    replacement.set_project_id_meta(fixture.original.project_id_meta().unwrap());
    replacement.set_workspace_path_meta(fixture.original.workspace_path_meta().unwrap());
    replacement.metadata_version += 1;
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

#[actix_web::test]
async fn supervisor_exact_cancel_uses_the_same_boundary_in_standard_and_ultra() {
    for ultra in [false, true] {
        let fixture = Box::pin(Fixture::new()).await;
        let mut supervisor = fixture
            .state
            .storage
            .load_session(&fixture.reference.session_id)
            .await
            .unwrap()
            .unwrap();
        supervisor.set_root_orchestration_only(ultra).unwrap();
        fixture
            .state
            .storage
            .save_session(&supervisor)
            .await
            .unwrap();
        grant_cancel(&fixture);
        let selected = current_target(&fixture).await;
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
        let observed = invoke(&fixture, args("observe_run", &selected, None))
            .await
            .unwrap();
        assert_eq!(observed["run_id"], run_id, "ultra={ultra}");
        let requested = invoke(&fixture, args("cancel", &selected, Some(&run_id)))
            .await
            .unwrap();
        assert_eq!(requested["status"], "cancel_requested", "ultra={ultra}");
        assert!(token.is_cancelled(), "ultra={ultra}");
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
        fixture.state.shutdown().await;
    }
}

#[actix_web::test]
async fn cancelled_root_control_admission_cannot_signal_late_owner() {
    use bamboo_server_tools::session_control::root_run::SupervisedRootRunPort;
    let fixture = Box::pin(Fixture::new()).await;
    let target = current_target(&fixture).await;
    let supervisor = fixture
        .state
        .storage
        .load_execution_admission_snapshot(&fixture.reference.session_id)
        .await
        .unwrap()
        .unwrap();
    let caller = bamboo_domain::SessionCallerReference::from(&supervisor);
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
    let port = crate::app_state::supervised_root_run::SupervisedRootRunController {
        store: fixture.state.session_store.clone(),
        router: fixture.state.session_activation_router.clone(),
        runners: fixture.state.agent_runners.clone(),
        live_retry: fixture.state.live_retry.clone(),
        fresh_sync: fixture.state.fresh_sync.clone(),
    };
    let gate = bamboo_domain::AdmissionGate::default();
    gate.cancel_if_pending();
    assert!(port
        .cancel_run(&caller, &target, &run_id, gate)
        .await
        .is_err());
    assert!(!token.is_cancelled());
    assert_eq!(
        fixture
            .state
            .session_inbox
            .inspect(TARGET)
            .await
            .unwrap()
            .generation,
        0
    );
    owner.abandon().await;
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn default_supervisor_cancel_observes_root_but_requires_operation_approval() {
    let fixture = Box::pin(Fixture::new_with_legacy_setup(false)).await;
    let target = current_target(&fixture).await;
    let supervisor = fixture
        .state
        .storage
        .load_execution_admission_snapshot(&fixture.reference.session_id)
        .await
        .unwrap()
        .unwrap();
    let caller = bamboo_domain::SessionCallerReference::from(&supervisor);
    assert!(fixture
        .state
        .storage
        .find_host_session_scope(&caller)
        .await
        .unwrap()
        .is_none());
    let scope = SupervisorSessionService::new(fixture.state.session_store.clone())
        .inspect_scope(&fixture.reference)
        .await
        .unwrap();
    assert!(scope.allowed_projects.is_empty());
    assert!(fixture
        .state
        .storage
        .inspect_supervisor_link(&fixture.reference, TARGET)
        .await
        .unwrap()
        .link
        .is_none());

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

    let observed = invoke(&fixture, args("observe_run", &target, None))
        .await
        .unwrap();
    assert_eq!(observed["run_id"], run_id);
    let rejected = invoke(&fixture, args("cancel", &target, Some(&run_id)))
        .await
        .unwrap_err();
    assert!(rejected
        .to_string()
        .contains("Permission approval required"));
    assert!(!token.is_cancelled());
    assert_eq!(
        fixture
            .state
            .session_inbox
            .inspect(TARGET)
            .await
            .unwrap()
            .generation,
        0
    );
    // The same first-run policy becomes an interactive request when the
    // actual executor has an event sink. It is still not a cancel receipt.
    let call = ToolCall {
        id: "unapproved-model-cancel".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "session_control".into(),
            arguments: args("cancel", &target, Some(&run_id)).to_string(),
        },
    };
    let (events, mut received) = tokio::sync::mpsc::channel(4);
    let mut context = ToolExecutionContext::none(&call.id);
    context.session_id = Some(&supervisor.id);
    context.root_session_id = Some(&supervisor.id);
    context.event_tx = Some(&events);
    context = context.with_executing_supervisor(
        ExecutingSupervisorObservation::capture_from_executing_session(&supervisor),
    );
    let outcome = bamboo_agent_core::tools::with_executing_session_caller(
        Some(Arc::new(caller)),
        fixture
            .state
            .tools_for(ToolSurface::Root)
            .execute_with_context_outcome(&call, context),
    )
    .await
    .unwrap();
    let ToolOutcome::Completed(approval) = outcome else {
        panic!("the unapproved request must not invoke Root cancellation");
    };
    let payload: Value = serde_json::from_str(&approval.result).unwrap();
    assert_eq!(payload["status"], "awaiting_permission_approval");
    let question = payload["question"].as_str().unwrap();
    assert!(question.contains("request cancellation of the selected Session run"));
    assert!(!question.contains("shell"));
    let event = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .expect("the approval event must be delivered");
    assert!(matches!(
        event,
        Some(bamboo_agent_core::AgentEvent::ToolApprovalRequested { tool_call_id, .. })
            if tool_call_id == call.id
    ));
    assert!(!token.is_cancelled());
    grant_cancel(&fixture);
    let requested = invoke(&fixture, args("cancel", &target, Some(&run_id)))
        .await
        .unwrap();
    assert_eq!(requested["status"], "cancel_requested");
    assert!(token.is_cancelled());
    owner.abandon().await;
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn installed_or_revoked_finite_host_scope_cannot_cancel_even_with_send_right() {
    use bamboo_domain::{
        HostScopeSessionObservation, HostSessionRight, HostSessionScopeBinding,
        HostSessionTargetScope, HOST_SESSION_SCOPE_VERSION,
    };

    let fixture = Box::pin(Fixture::new_with_legacy_setup(false)).await;
    grant_cancel(&fixture);
    let supervisor = fixture
        .state
        .storage
        .load_session(&fixture.reference.session_id)
        .await
        .unwrap()
        .unwrap();
    let current = fixture.reload().await;
    let target = current_target(&fixture).await;
    let mut runner = crate::app_state::AgentRunner::new();
    runner.status = crate::app_state::AgentStatus::Running;
    let run_id = runner.run_id.clone();
    let token = runner.cancel_token.clone();
    let owner = fixture
        .state
        .session_activation_router
        .register_session_run(&current, &run_id)
        .await
        .unwrap();
    fixture
        .state
        .agent_runners
        .write()
        .await
        .insert(TARGET.into(), runner);
    fixture
        .state
        .session_store
        .install_host_session_scope(
            0,
            HostSessionScopeBinding {
                schema_version: HOST_SESSION_SCOPE_VERSION,
                grant_id: uuid::Uuid::new_v4(),
                revision: 1,
                caller: HostScopeSessionObservation::capture(&supervisor, vec![]).unwrap(),
                targets: [(
                    current.id.clone(),
                    HostSessionTargetScope {
                        observation: HostScopeSessionObservation::capture(&current, vec![])
                            .unwrap(),
                        rights: [HostSessionRight::ListMetadata, HostSessionRight::SendPeer].into(),
                    },
                )]
                .into(),
                creates: Default::default(),
            },
        )
        .await
        .unwrap();

    for revoked in [false, true] {
        if revoked {
            fixture
                .state
                .session_store
                .revoke_host_session_scope(&fixture.reference.session_id, 1)
                .await
                .unwrap();
        }
        let error = invoke(&fixture, args("cancel", &target, Some(&run_id)))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("host_session_scope_legacy_namespace_changed"),
            "{error}"
        );
        assert!(!token.is_cancelled());
        assert_eq!(
            fixture
                .state
                .session_inbox
                .inspect(TARGET)
                .await
                .unwrap()
                .generation,
            0
        );
    }
    owner.abandon().await;
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn native_supervisor_dispatch_reaches_exact_cancel_in_standard_and_ultra() {
    for (ultra, explicit_deny) in [(false, false), (true, false), (false, true), (true, true)] {
        // An isolated Host, its real tool factory, Router and storage are used.
        // Auto is selected in this synthetic Supervisor only; it is not bypass.
        let fixture = Box::pin(Fixture::new_with_legacy_setup(false)).await;
        let mut supervisor = fixture
            .state
            .storage
            .load_session(&fixture.reference.session_id)
            .await
            .unwrap()
            .unwrap();
        supervisor.set_root_orchestration_only(ultra).unwrap();
        supervisor
            .agent_runtime_state
            .get_or_insert_with(Default::default)
            .set_permission_mode(bamboo_domain::SessionPermissionMode::Auto);
        let workspace = fixture._home.path().join("supervisor-workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        supervisor.set_workspace_path_meta(workspace.to_string_lossy());
        fixture
            .state
            .storage
            .save_session(&supervisor)
            .await
            .unwrap();
        let flags = bamboo_agent_core::tools::ToolExecutionSessionFlags::from_session(&supervisor);
        assert!(flags.auto_approve_permissions);
        assert!(!flags.bypass_permissions);
        let caller = bamboo_domain::SessionCallerReference::from(&supervisor);
        assert!(fixture
            .state
            .storage
            .find_host_session_scope(&caller)
            .await
            .unwrap()
            .is_none());
        assert!(
            SupervisorSessionService::new(fixture.state.session_store.clone())
                .inspect_scope(&fixture.reference)
                .await
                .unwrap()
                .allowed_projects
                .is_empty()
        );
        assert!(fixture
            .state
            .storage
            .inspect_supervisor_link(&fixture.reference, TARGET)
            .await
            .unwrap()
            .link
            .is_none());

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
        let target = current_target(&fixture).await;
        let observation = invoke(&fixture, args("observe_run", &target, None))
            .await
            .unwrap();
        assert_eq!(observation["run_id"], run_id);
        if explicit_deny {
            fixture
                .state
                .permission_checker
                .permission_config()
                .unwrap()
                .deny_scoped_session_permission(
                    &fixture.reference.session_id,
                    PermissionType::ExecuteCommand,
                    format!("session_control cancel {TARGET}"),
                );
        }
        let call_id = format!("model-exact-cancel-{ultra}-{explicit_deny}");
        *fixture.probe.next.lock().unwrap() = Some(ToolCall {
            id: call_id.clone(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "session_control".into(),
                arguments: args("cancel", &target, Some(&run_id)).to_string(),
            },
        });
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        tokio::time::timeout(
            Duration::from_secs(30),
            fixture.state.agent.execute_direct(
                &mut supervisor,
                bamboo_engine::ExecuteRequestBuilder::new(
                    "Cancel the observed Root run",
                    tx,
                    tokio_util::sync::CancellationToken::new(),
                )
                .tools(fixture.state.tools_for(ToolSurface::Root))
                .provider_override(fixture.probe.clone())
                .model("supervisor-model")
                .build(),
            ),
        )
        .await
        .expect("bounded synthetic Supervisor run")
        .unwrap();
        drain.await.unwrap();
        {
            let schemas = fixture.probe.control_schemas.lock().unwrap();
            let offered = schemas.first().and_then(Option::as_ref).unwrap();
            assert!(offered["properties"]["action"]["enum"]
                .as_array()
                .unwrap()
                .contains(&json!("cancel")));
        }
        let durable = fixture
            .state
            .storage
            .load_session(&fixture.reference.session_id)
            .await
            .unwrap()
            .unwrap();
        let result = durable
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some(call_id.as_str()))
            .expect("the real Supervisor dispatcher must persist its tool result");
        assert_eq!(result.tool_success, Some(!explicit_deny));
        if explicit_deny {
            assert!(
                result.content.contains("permission denied"),
                "{}",
                result.content
            );
            assert!(!token.is_cancelled());
        } else {
            let receipt: Value = serde_json::from_str(&result.content).expect(&result.content);
            assert_eq!(receipt["status"], "cancel_requested");
            assert_eq!(receipt["run_id"], run_id);
            assert_eq!(receipt["completion_verified"], false);
            assert!(token.is_cancelled());
        }
        assert_eq!(
            fixture
                .state
                .session_inbox
                .inspect(TARGET)
                .await
                .unwrap()
                .generation,
            0
        );
        owner.abandon().await;
        fixture.state.shutdown().await;
    }
}
