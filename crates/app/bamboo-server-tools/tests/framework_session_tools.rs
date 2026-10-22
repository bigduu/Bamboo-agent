//! Composition fixtures only; no real grant, Root writer or provider execution.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    with_executing_session_caller, with_retained_tool_execution, FrameworkSessionToolKind as Kind,
    FunctionCall, RetainedToolExecution, Tool, ToolCall, ToolCtx, ToolError, ToolExecutionContext,
    ToolExecutor, ToolOutcome, ToolResult, FRAMEWORK_ROOT_SPAWN_CONTRACT_V1,
};
use bamboo_domain::{Session, SessionCallerReference, Storage};
use bamboo_server_tools::framework_session_tools::assemble_framework_session_tools;
use bamboo_server_tools::session_control::root_creation::{
    SessionRootCreatePort, SupervisedRootCreateRequest,
};
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};
use tokio::sync::RwLock;

struct ChildSpy(Arc<AtomicUsize>);
#[async_trait]
impl Tool for ChildSpy {
    fn name(&self) -> &str {
        "SubAgent"
    }
    fn description(&self) -> &str {
        "must never receive a fresh Root call"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object"})
    }
    async fn invoke(&self, _: Value, _: ToolCtx) -> Result<ToolOutcome, ToolError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::Completed(ToolResult::text(true, "Child spy")))
    }
}

struct LegacySpawnSpy(Arc<AtomicUsize>);
#[async_trait]
impl Tool for LegacySpawnSpy {
    fn name(&self) -> &str {
        "spawn_session"
    }
    fn description(&self) -> &str {
        "frozen pre-framework custom owner"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object"})
    }
    async fn invoke(&self, _: Value, _: ToolCtx) -> Result<ToolOutcome, ToolError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            "legacy custom",
        )))
    }
}

#[derive(Default)]
struct CallerPort {
    creates: AtomicUsize,
    blocked: bool,
}
#[async_trait]
impl SessionRootCreatePort for CallerPort {
    async fn scope(&self, _: &SessionCallerReference) -> Result<ToolResult, ToolError> {
        Ok(ToolResult::text(
            !self.blocked,
            json!({
                "code":if self.blocked {"host_root_first_input_unavailable"} else {"fixture_only"}
            })
            .to_string(),
        ))
    }
    async fn create(
        &self,
        caller: &SessionCallerReference,
        request: SupervisedRootCreateRequest,
    ) -> Result<ToolResult, ToolError> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        Ok(ToolResult::text(
            true,
            json!({
                "caller":caller.session_id, "operation_id":request.operation_id,
                "fixture":"caller_port_reached", "completion_verified":false
            })
            .to_string(),
        ))
    }
}

struct Fixture {
    _home: tempfile::TempDir,
    store: Arc<SessionStoreV2>,
    tools: Arc<dyn ToolExecutor>,
    config: Arc<RwLock<bamboo_llm::Config>>,
    child_calls: Arc<AtomicUsize>,
    legacy_calls: Arc<AtomicUsize>,
}
impl Fixture {
    async fn new(port: Option<Arc<dyn SessionRootCreatePort>>) -> Self {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let config = Arc::new(RwLock::new(bamboo_llm::Config::default()));
        let permissions = Arc::new(bamboo_tools::BuiltinToolExecutor::new_with_config(
            config.clone(),
        ));
        let child_calls = Arc::new(AtomicUsize::new(0));
        permissions
            .register_tool(ChildSpy(child_calls.clone()))
            .unwrap();
        let legacy_calls = Arc::new(AtomicUsize::new(0));
        permissions
            .register_tool(LegacySpawnSpy(legacy_calls.clone()))
            .unwrap();
        let tools = assemble_framework_session_tools(
            permissions.clone(),
            permissions,
            store.clone(),
            store.clone(),
            config.clone(),
            port,
        );
        Self {
            _home: home,
            store,
            tools,
            config,
            child_calls,
            legacy_calls,
        }
    }
    fn call(name: &str) -> ToolCall {
        ToolCall {
            id: "fresh-root-fixture".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: name.into(),
                arguments: json!({
                    "operation_id":uuid::Uuid::new_v4(), "expected_state_revision":1,
                    "project_id":"explicit-project", "workspace_path":"/explicit/host-bound",
                    "message":"one bounded instruction"
                })
                .to_string(),
            },
        }
    }
    async fn invoke(
        &self,
        source: &Session,
        call: &ToolCall,
        plan: bool,
    ) -> Result<ToolResult, ToolError> {
        let tools = self.tools.for_session(source).unwrap();
        let mut ctx = ToolExecutionContext::none(&call.id);
        ctx.session_id = Some(&source.id);
        ctx.plan_read_only = plan;
        with_executing_session_caller(
            Some(Arc::new(SessionCallerReference::from(source))),
            tools.execute_with_context(call, ctx),
        )
        .await
    }
}

#[tokio::test]
async fn actual_shared_factory_proves_same_tool_arcs_and_rejects_same_named_shadow() {
    let fixture = Fixture::new(None).await;
    let caller = Session::new("proof-child-parent", "model");
    let child = Session::new_child_of("proof-child", &caller, "model", "child");
    let bound = fixture.tools.for_session(&child).unwrap();
    for kind in [Kind::History, Kind::CurrentHistory, Kind::SpawnRoot] {
        let proof = bound
            .framework_session_tool_for_exact(kind.execution_name())
            .unwrap();
        assert_eq!(proof.kind(), kind);
        assert!(Arc::ptr_eq(
            &proof.instance(),
            &bound
                .tool_instance_for_exact(kind.execution_name())
                .unwrap()
        ));
        assert_eq!(
            serde_json::to_value(proof.schema()).unwrap(),
            serde_json::to_value(
                bound
                    .list_tools()
                    .into_iter()
                    .find(|schema| schema.function.name == kind.execution_name())
                    .unwrap()
            )
            .unwrap()
        );
    }
    // A same-name custom replacement is a different Arc and has no proof.
    let proof = bound
        .framework_session_tool_for_exact("spawn_session")
        .unwrap();
    let shadow: Arc<dyn ToolExecutor> = Arc::new(bamboo_server_tools::OverlayToolExecutor::new(
        bound,
        Arc::new(LegacySpawnSpy(Arc::new(AtomicUsize::new(0)))),
    ));
    assert!(shadow
        .framework_session_tool_for_exact("spawn_session")
        .is_none());
    assert!(proof.rebind(shadow).is_err());
    assert!(fixture
        .tools
        .framework_session_tool_for_exact("session_message")
        .is_none());
}

#[tokio::test]
async fn actual_shared_factory_preserves_retained_custom_owner_and_fresh_nested_root_selection() {
    for version in [None, Some(1), Some(2)] {
        let port = Arc::new(CallerPort::default());
        let fixture = Fixture::new(Some(port.clone())).await;
        let mut caller = Session::new("retained-factory-caller", "model");
        caller.session_version = version;
        fixture.store.save_session(&caller).await.unwrap();
        let tools = fixture.tools.for_session(&caller).unwrap();
        let original = Fixture::call("default::spawnSession");
        let retained = RetainedToolExecution::new(&original, "spawn_session", true, None).unwrap();
        assert!(retained.owner_is_available(tools.as_ref()));
        assert!(!retained.framework_instance_is_available(tools.as_ref()));
        let mut ctx = ToolExecutionContext::none(&original.id);
        ctx.session_id = Some(&caller.id);
        with_retained_tool_execution(
            Some(retained),
            tools.execute_exact_with_context_outcome(&original, "spawn_session", ctx),
        )
        .await
        .unwrap();
        assert_eq!(fixture.legacy_calls.load(Ordering::SeqCst), 1);
        assert_eq!(port.creates.load(Ordering::SeqCst), 0);
        fixture
            .invoke(&caller, &Fixture::call("spawnSession"), false)
            .await
            .unwrap();
        assert_eq!(port.creates.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.child_calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn shared_factory_routes_every_supported_fresh_alias_to_the_installed_caller_port() {
    let port = Arc::new(CallerPort::default());
    let fixture = Fixture::new(Some(port.clone())).await;
    assert_eq!(
        fixture.tools.execution_contract_for_exact("spawn_session"),
        Some(FRAMEWORK_ROOT_SPAWN_CONTRACT_V1)
    );
    for (index, version) in [None, Some(1), Some(2)].into_iter().enumerate() {
        let mut caller = Session::new(format!("base-caller-{index}"), "model");
        caller.session_version = version;
        fixture.store.save_session(&caller).await.unwrap();
        let call = Fixture::call("spawnSession");
        let result = fixture.invoke(&caller, &call, false).await.unwrap();
        let result: Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(result["fixture"], "caller_port_reached");
        assert_eq!(result["caller"], caller.id);
        assert_eq!(result["completion_verified"], false);
    }
    assert_eq!(port.creates.load(Ordering::SeqCst), 3);
    assert_eq!(fixture.child_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unconfigured_or_blocked_host_ports_preserve_specific_codes_and_never_spawn_a_child() {
    for configured in [false, true] {
        let port = Arc::new(CallerPort {
            blocked: true,
            ..Default::default()
        });
        let fixture =
            Fixture::new(configured.then(|| port.clone() as Arc<dyn SessionRootCreatePort>)).await;
        let caller = Session::new("blocked-base-caller", "model");
        fixture.store.save_session(&caller).await.unwrap();
        for name in ["spawn_session", "spawnSession"] {
            let error = fixture
                .invoke(&caller, &Fixture::call(name), false)
                .await
                .unwrap_err();
            assert!(error.to_string().contains(if configured {
                "host_root_first_input_unavailable"
            } else {
                "session_root_creator_unconfigured"
            }));
        }
        assert_eq!(port.creates.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.child_calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn shared_factory_keeps_plan_and_disabled_gates_before_the_caller_writer() {
    let port = Arc::new(CallerPort::default());
    let fixture = Fixture::new(Some(port.clone())).await;
    let caller = Session::new("restricted-base-caller", "model");
    fixture.store.save_session(&caller).await.unwrap();
    let call = Fixture::call("spawnSession");
    assert!(fixture
        .invoke(&caller, &call, true)
        .await
        .unwrap_err()
        .to_string()
        .contains("Plan"));
    fixture.config.write().await.disabled_tools = vec!["spawnSession".into()];
    assert!(fixture
        .invoke(&caller, &call, false)
        .await
        .unwrap_err()
        .to_string()
        .contains("disabled"));
    assert_eq!(port.creates.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.child_calls.load(Ordering::SeqCst), 0);
}
