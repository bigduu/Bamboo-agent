use super::*;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};

struct DeclaredHook {
    declared: Arc<AtomicBool>,
    result: HookResult,
}

#[async_trait]
impl AgentHook for DeclaredHook {
    fn point(&self) -> AgentHookPoint {
        AgentHookPoint::AfterToolExecution
    }

    fn may_suspend(&self) -> bool {
        self.declared.load(Ordering::SeqCst)
    }

    async fn run(&self, _: AgentHookPoint, _: &HookPayload, _: &Session) -> HookResult {
        self.result.clone()
    }
}

fn payload() -> HookPayload {
    HookPayload::ToolResult {
        tool_name: "loop_control".into(),
        tool_call_id: "native-contract".into(),
        outcome: bamboo_domain::HookToolOutcome {
            success: true,
            result: Some("original immutable receipt".into()),
            error: None,
            needs_human: false,
            duration_ms: 0,
        },
    }
}

#[tokio::test]
async fn non_suspending_declaration_is_frozen_and_violation_aborts_with_context() {
    let declared = Arc::new(AtomicBool::new(false));
    let mut runner = HookDispatcher::new();
    runner.register(Arc::new(DeclaredHook {
        declared: declared.clone(),
        result: HookResult::WithContext {
            text: "outer feedback".into(),
            result: Box::new(HookResult::WithContext {
                text: "inner feedback".into(),
                result: Box::new(HookResult::Suspend {
                    reason: "requested wait".into(),
                }),
            }),
        },
    }));
    declared.store(true, Ordering::SeqCst);
    assert!(!runner.has_suspending_hooks_for(AgentHookPoint::AfterToolExecution));
    let session = Session::new("non-suspending-contract", "synthetic-only");
    let report = runner
        .run_hooks(AgentHookPoint::AfterToolExecution, &payload(), &session)
        .await;
    assert!(
        matches!(&report.outcome.decision, HookResult::Abort { reason }
        if reason.contains("non-suspending") && reason.contains("requested wait"))
    );
    assert_eq!(
        report.outcome.injected_contexts,
        ["outer feedback", "inner feedback"]
    );
    assert_eq!(report.executions.len(), 1);
    assert!(matches!(
        &report.executions[0].result,
        HookResult::WithContext { .. }
    ));
}

#[tokio::test]
async fn non_suspending_declaration_preserves_allow_deny_ask_and_feedback() {
    for decision in [
        HookResult::Continue,
        HookResult::Allow,
        HookResult::Deny {
            reason: "trusted denial".into(),
        },
        HookResult::Ask,
        HookResult::Abort {
            reason: "trusted abort".into(),
        },
    ] {
        let mut runner = HookDispatcher::new();
        runner.register(Arc::new(DeclaredHook {
            declared: Arc::new(AtomicBool::new(false)),
            result: HookResult::WithContext {
                result: Box::new(decision.clone()),
                text: "retained feedback".into(),
            },
        }));
        let session = Session::new("non-suspending-controls", "synthetic-only");
        let report = runner
            .run_hooks(AgentHookPoint::AfterToolExecution, &payload(), &session)
            .await;
        assert_eq!(report.outcome.decision, decision);
        assert_eq!(report.outcome.injected_contexts, ["retained feedback"]);
        assert_eq!(report.executions.len(), 1);
    }
}

struct UnknownHook;

#[async_trait]
impl AgentHook for UnknownHook {
    fn point(&self) -> AgentHookPoint {
        AgentHookPoint::AfterToolExecution
    }

    async fn run(&self, _: AgentHookPoint, _: &HookPayload, _: &Session) -> HookResult {
        HookResult::Suspend {
            reason: "ordinary suspension".into(),
        }
    }
}

#[tokio::test]
async fn unknown_native_hook_keeps_original_suspension_contract() {
    let mut runner = HookDispatcher::new();
    runner.register(Arc::new(UnknownHook));
    assert!(runner.has_suspending_hooks_for(AgentHookPoint::AfterToolExecution));
    let report = runner
        .run_hooks(
            AgentHookPoint::AfterToolExecution,
            &payload(),
            &Session::new("ordinary-native", "synthetic-only"),
        )
        .await;
    assert_eq!(
        report.outcome.decision,
        HookResult::Suspend {
            reason: "ordinary suspension".into()
        }
    );
}
