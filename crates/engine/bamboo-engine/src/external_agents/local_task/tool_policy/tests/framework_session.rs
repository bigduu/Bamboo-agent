//! Synthetic local Actor fixtures; never a provider call or real account scope.
use super::*;
use crate::session_app::approval_replay::{
    framework_fixture::{park, FixtureTools},
    resolve_permission_replay_owner, restore_permission_replay_execution,
};
use bamboo_agent_core::tools::{with_retained_tool_execution, FrameworkSessionToolKind as Kind};
use std::sync::atomic::AtomicU8;

struct OuterGate {
    inner: Arc<dyn ToolExecutor>,
    gates: AtomicUsize,
    mode: AtomicU8,
    pause: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl OuterGate {
    fn new(inner: Arc<dyn ToolExecutor>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            gates: AtomicUsize::new(0),
            mode: AtomicU8::new(0),
            pause: AtomicBool::new(false),
            entered: Default::default(),
            resume: Default::default(),
        })
    }
}
#[async_trait]
impl ToolExecutor for OuterGate {
    async fn execute(&self, _: &ToolCall) -> Result<ToolResult, ToolError> {
        panic!("proof uses exact native gate")
    }
    async fn check_permissions_for_resolved(
        &self,
        call: &ToolCall,
        name: &str,
        args: &serde_json::Value,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.gates.fetch_add(1, Ordering::SeqCst);
        if self.pause.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        match self.mode.load(Ordering::SeqCst) {
            1 => Err(ToolError::Execution("outer native Deny".into())),
            2 => Ok(Some(ToolOutcome::NeedsHuman {
                question: bamboo_domain::PendingQuestion {
                    tool_call_id: call.id.clone(),
                    tool_name: name.into(),
                    question: "outer native Ask".into(),
                    options: vec![],
                    allow_custom: false,
                    source: Default::default(),
                },
                result: ToolResult::text(false, "outer native Ask"),
            })),
            _ => {
                self.inner
                    .check_permissions_for_resolved(call, name, args, ctx)
                    .await
            }
        }
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        self.inner.list_tools()
    }
    fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
        self.inner.exact_tool_owner(name)
    }
}

fn local(
    f: &Fixture,
    child: &Session,
    inner: Arc<dyn ToolExecutor>,
    names: &[&str],
    disabled: &[String],
) -> LocalChildTools {
    LocalChildTools::new(
        inner,
        names.iter().map(|name| (*name).into()).collect(),
        child,
        f.store.clone(),
        f.owner.clone(),
        Some(disabled),
    )
    .unwrap()
}
async fn invoke(
    tools: &LocalChildTools,
    child: &Session,
    call: &ToolCall,
) -> Result<ToolOutcome, ToolError> {
    let mut ctx = ToolExecutionContext::none(&call.id);
    ctx.session_id = Some(&child.id);
    tools.execute_with_context_outcome(call, ctx).await
}

#[tokio::test]
async fn ordinary_child_uses_actual_host_ports_and_fresh_root_aliases_for_every_version() {
    let f = Fixture::new().await;
    let host = FixtureTools::new(&Kind::ALL.map(Kind::execution_name));
    for version in [None, Some(1), Some(2)] {
        let mut child = f.child.clone();
        child.session_version = version;
        let tools = local(&f, &child, host.clone(), &[], &[]);
        assert_eq!(tools.list_tools().len(), 4);
        for name in ["spawn_session", "spawnSession", "default::spawn_session"] {
            let call = call(name);
            let original = call.clone();
            invoke(&tools, &child, &call).await.unwrap();
            assert_eq!(call, original);
        }
        // Exact dispatch must use the same fresh selector, never legacy alias
        // fallback to a Child tool below the immutable local native ceiling.
        let call = call("spawnSession");
        let mut ctx = ToolExecutionContext::none(&call.id);
        ctx.session_id = Some(&child.id);
        tools
            .execute_exact_with_context_outcome(&call, "spawn_session", ctx)
            .await
            .unwrap();
    }
    assert_eq!(host.gates(), 12);
    assert!(host
        .invocations()
        .iter()
        .all(|name| name == "spawn_session"));
    assert_eq!(bamboo_subagent::proto::NativeToolCeiling::NAMES.len(), 8);
}

#[tokio::test]
async fn same_name_and_contract_without_host_proof_never_install_child_ports() {
    let f = Fixture::new().await;
    let host = FixtureTools::new(&Kind::ALL.map(Kind::execution_name));
    host.proofs_enabled.store(false, Ordering::SeqCst);
    assert_eq!(
        host.execution_contract_for_exact("spawn_session"),
        Some(bamboo_agent_core::tools::FRAMEWORK_ROOT_SPAWN_CONTRACT_V1)
    );
    let tools = local(
        &f,
        &f.child,
        host.clone(),
        &Kind::ALL.map(Kind::execution_name),
        &[],
    );
    assert!(tools.list_tools().is_empty());
    for kind in Kind::ALL {
        assert!(!tools.owns_exact_tool(kind.execution_name()));
        assert!(invoke(&tools, &f.child, &call(kind.execution_name()))
            .await
            .is_err());
    }
    assert_eq!(host.gates(), 0);
    assert!(host.invocations().is_empty());
}

#[tokio::test]
async fn child_proof_rebinds_complete_outer_deny_ask_and_single_native_evaluation() {
    let f = Fixture::new().await;
    let host = FixtureTools::new(&["spawn_session"]);
    let outer = OuterGate::new(host.clone());
    let tools = local(&f, &f.child, outer.clone(), &[], &[]);
    outer.mode.store(1, Ordering::SeqCst);
    assert!(invoke(&tools, &f.child, &call("spawnSession"))
        .await
        .unwrap_err()
        .to_string()
        .contains("outer native Deny"));
    outer.mode.store(2, Ordering::SeqCst);
    assert!(matches!(
        invoke(&tools, &f.child, &call("spawnSession"))
            .await
            .unwrap(),
        ToolOutcome::NeedsHuman { .. }
    ));
    assert_eq!(host.gates(), 0);
    assert!(host.invocations().is_empty());
    outer.mode.store(0, Ordering::SeqCst);
    invoke(&tools, &f.child, &call("spawnSession"))
        .await
        .unwrap();
    assert_eq!(outer.gates.load(Ordering::SeqCst), 3);
    assert_eq!(host.gates(), 1);
    assert_eq!(host.invocations(), ["spawn_session"]);
}

#[tokio::test]
async fn framework_ports_preserve_readonly_strict_profile_ticket_and_disabled_boundaries() {
    let f = Fixture::new().await;
    let host = FixtureTools::new(&Kind::ALL.map(Kind::execution_name));
    let mut readonly = f.child.clone();
    readonly.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState::new("tool-run"));
    readonly.agent_runtime_state.as_mut().unwrap().read_only = true;
    let tools = local(&f, &readonly, host.clone(), &[], &[]);
    assert_eq!(
        tools
            .list_tools()
            .iter()
            .map(|tool| tool.function.name.as_str())
            .collect::<BTreeSet<_>>(),
        ["session_history", "session_history_current"].into()
    );
    for kind in Kind::ALL {
        assert_eq!(
            invoke(&tools, &readonly, &call(kind.execution_name()))
                .await
                .is_ok(),
            kind.is_reader()
        );
    }
    for key in [
        crate::session_app::child_session::named_profile::NAMED_PROFILE_BINDING_KEY,
        crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY,
    ] {
        let mut strict = f.child.clone();
        strict.metadata.insert(key.into(), "fixture".into());
        let tools = local(&f, &strict, host.clone(), &[], &[]);
        assert!(tools.list_tools().is_empty());
        for kind in Kind::ALL {
            assert!(invoke(&tools, &strict, &call(kind.execution_name()))
                .await
                .is_err());
        }
    }
    let disabled = local(&f, &f.child, host.clone(), &[], &["spawnSession".into()]);
    assert!(!disabled.owns_exact_tool("spawn_session"));
    host.restriction.blocked.store(true, Ordering::SeqCst);
    let live = local(&f, &f.child, host.clone(), &[], &[]);
    assert!(invoke(&live, &f.child, &call("session_history"))
        .await
        .is_err());
    assert_eq!(host.gates(), 2);
}

#[tokio::test]
async fn awaited_native_gate_cannot_outlive_child_actor_or_live_disabled_posture() {
    for stop in [false, true] {
        let f = Fixture::new().await;
        let host = FixtureTools::new(&["spawn_session"]);
        let outer = OuterGate::new(host.clone());
        outer.pause.store(true, Ordering::SeqCst);
        let tools = local(&f, &f.child, outer.clone(), &[], &[]);
        let child = f.child.clone();
        let pending =
            tokio::spawn(async move { invoke(&tools, &child, &call("spawnSession")).await });
        outer.entered.notified().await;
        if stop {
            f.store
                .finish_activation(
                    &f.owner.fence,
                    chrono::Utc::now(),
                    bamboo_domain::ActorActivationFinish::Cancelled,
                )
                .await
                .unwrap();
        } else {
            host.restriction.blocked.store(true, Ordering::SeqCst);
        }
        outer.resume.notify_one();
        assert!(pending.await.unwrap().is_err());
        assert_eq!(outer.gates.load(Ordering::SeqCst), 1);
        assert_eq!(host.gates(), 1);
        assert!(host.invocations().is_empty());
    }
}

struct LegacySwitch {
    fresh: Arc<FixtureTools>,
    legacy: Arc<Base>,
}

#[tokio::test]
async fn retained_framework_instance_still_requires_current_plan_and_disabled_gates() {
    let f = Fixture::new().await;
    let host = FixtureTools::new(&["spawn_session"]);
    let tools = local(&f, &f.child, host.clone(), &[], &[]);
    let mut child = f.child.clone();
    let call = call("default::spawnSession");
    let target = park(
        &mut child,
        &call,
        "spawn_session",
        true,
        Some(bamboo_agent_core::tools::FRAMEWORK_ROOT_SPAWN_CONTRACT_V1),
    );
    let original = serde_json::to_value(&child).unwrap();
    assert_eq!(
        resolve_permission_replay_owner(&child, &target, &tools)
            .unwrap()
            .as_deref(),
        Some("spawn_session")
    );
    let execution = restore_permission_replay_execution(&child, &target, "spawn_session").unwrap();
    for plan in [true, false] {
        host.restriction.blocked.store(!plan, Ordering::SeqCst);
        let mut ctx = ToolExecutionContext::none(&call.id);
        ctx.session_id = Some(&child.id);
        ctx.plan_read_only = plan;
        assert!(with_retained_tool_execution(
            execution.clone(),
            tools.execute_exact_with_context_outcome(&call, "spawn_session", ctx)
        )
        .await
        .is_err());
    }
    assert_eq!(host.gates(), 0);
    assert!(host.invocations().is_empty());
    assert_eq!(serde_json::to_value(&child).unwrap(), original);
}
#[async_trait]
impl ToolExecutor for LegacySwitch {
    async fn execute(&self, _: &ToolCall) -> Result<ToolResult, ToolError> {
        panic!("exact only")
    }
    async fn execute_exact_with_context_outcome(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        if retained_tool_execution_for(call, name)?.is_some_and(|retained| {
            retained.contract_known() && retained.execution_contract().is_none()
        }) {
            self.legacy
                .execute_exact_with_context_outcome(call, name, ctx)
                .await
        } else {
            self.fresh
                .execute_exact_with_context_outcome(call, name, ctx)
                .await
        }
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        self.fresh.list_tools()
    }
    fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
        if retained_tool_owner_lookup(name).is_some_and(|retained| {
            retained.contract_known() && retained.execution_contract().is_none()
        }) {
            self.legacy.exact_tool_owner(name)
        } else {
            self.fresh.exact_tool_owner(name)
        }
    }
}

#[tokio::test]
async fn central_replay_and_child_keep_frozen_known_custom_owner_without_root_reinterpretation() {
    let f = Fixture::new().await;
    let fresh = FixtureTools::new(&["spawn_session"]);
    let legacy = Arc::new(Base {
        names: Some(&["spawn_session"]),
        ..Default::default()
    });
    let inner = Arc::new(LegacySwitch {
        fresh: fresh.clone(),
        legacy: legacy.clone(),
    });
    let tools = local(&f, &f.child, inner.clone(), &["spawn_session"], &[]);
    let mut child = f.child.clone();
    let parked_call = call("default::spawnSession");
    let target = park(&mut child, &parked_call, "spawn_session", true, None);
    let before = serde_json::to_value(&child).unwrap();
    assert_eq!(
        resolve_permission_replay_owner(&child, &target, &tools)
            .unwrap()
            .as_deref(),
        Some("spawn_session")
    );
    let execution = restore_permission_replay_execution(&child, &target, "spawn_session").unwrap();
    with_retained_tool_execution(execution, invoke(&tools, &child, &parked_call))
        .await
        .unwrap();
    assert_eq!(legacy.executions.load(Ordering::SeqCst), 1);
    assert!(fresh.invocations().is_empty());
    assert_eq!(serde_json::to_value(&child).unwrap(), before);
    let mut strict = child.clone();
    strict.metadata.insert(
        crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY.into(),
        "fixture".into(),
    );
    let strict_tools = local(&f, &strict, inner, &["spawn_session"], &[]);
    assert!(
        resolve_permission_replay_owner(&strict, &target, &strict_tools)
            .unwrap()
            .is_none()
    );
    // A new call has no old selector and uses the actual Root instance.
    invoke(&tools, &child, &call("spawnSession")).await.unwrap();
    assert_eq!(fresh.invocations(), ["spawn_session"]);
}
