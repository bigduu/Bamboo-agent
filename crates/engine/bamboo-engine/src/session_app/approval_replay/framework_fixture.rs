//! Synthetic concrete-instance fixture shared by dispatch, catalog and replay.
//! This is not a real Host scope, Inbox, provider, or Root creation port.

use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    FrameworkSessionToolGuard, FrameworkSessionToolKind as Kind, HostFrameworkSessionTool, Tool,
    ToolCall, ToolClass, ToolCtx, ToolError, ToolExecutionContext, ToolExecutor, ToolOutcome,
    ToolResult, ToolSchema, FRAMEWORK_ROOT_SPAWN_CONTRACT_V1,
};
use serde_json::{json, Value};

struct Concrete {
    name: String,
    invocations: Arc<Mutex<Vec<String>>>,
}
#[async_trait]
impl Tool for Concrete {
    fn name(&self) -> &str {
        &self.name
    }
    fn description(&self) -> &str {
        "concrete framework fixture; no actual Session authority"
    }
    fn parameters_schema(&self) -> Value {
        if self.name == "session_history" {
            json!({"type":"object", "properties":{"action":{"enum":["list", "inspect", "get_meta", "read_messages"]}}})
        } else {
            json!({"type":"object"})
        }
    }
    fn execution_contract(&self) -> Option<&'static str> {
        (self.name == "spawn_session").then_some(FRAMEWORK_ROOT_SPAWN_CONTRACT_V1)
    }
    fn classify(&self, _: &Value) -> ToolClass {
        if Kind::from_execution_name(&self.name).is_some_and(Kind::is_reader) {
            ToolClass::READONLY_PARALLEL
        } else {
            ToolClass::MUTATING_SERIAL
        }
    }
    async fn invoke(&self, _: Value, _: ToolCtx) -> Result<ToolOutcome, ToolError> {
        self.invocations.lock().unwrap().push(self.name.clone());
        Ok(ToolOutcome::Completed(ToolResult::text(true, "fixture")))
    }
}

#[derive(Default)]
pub(crate) struct Restriction {
    pub blocked: AtomicBool,
}
#[async_trait]
impl FrameworkSessionToolGuard for Restriction {
    async fn check(&self, _: Kind, _: &ToolCall, _: &Value, _: &ToolCtx) -> Result<(), ToolError> {
        if self.blocked.load(Ordering::SeqCst) {
            Err(ToolError::Execution("fixture live disabled".into()))
        } else {
            Ok(())
        }
    }
}

struct NativeOwner {
    tools: BTreeMap<String, Arc<dyn Tool>>,
    gates: AtomicUsize,
}
#[async_trait]
impl ToolExecutor for NativeOwner {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        self.execute_with_context_outcome(call, ToolExecutionContext::none(&call.id))
            .await
            .map(ToolOutcome::into_tool_result)
    }
    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        let args = serde_json::from_str(&call.function.arguments).unwrap_or(json!({}));
        self.check_permissions_for_resolved(call, &call.function.name, &args, &ctx)
            .await?;
        self.tools
            .get(&call.function.name)
            .ok_or_else(|| ToolError::NotFound(call.function.name.clone()))?
            .invoke(args, ctx.to_tool_ctx())
            .await
    }
    async fn check_permissions_for_resolved(
        &self,
        _: &ToolCall,
        _: &str,
        _: &Value,
        _: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.gates.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        self.tools.values().map(|tool| tool.to_schema()).collect()
    }
    fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
        self.tools
            .contains_key(name)
            .then_some(self as &dyn ToolExecutor)
    }
    fn local_tool_instance(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.get(name).cloned()
    }
    fn local_execution_contract(&self, name: &str) -> Option<&'static str> {
        self.tools.get(name)?.execution_contract()
    }
}

pub(crate) struct FixtureTools {
    owner: Arc<NativeOwner>,
    proofs: BTreeMap<Kind, HostFrameworkSessionTool>,
    invocations: Arc<Mutex<Vec<String>>>,
    pub restriction: Arc<Restriction>,
    pub proofs_enabled: AtomicBool,
}
impl FixtureTools {
    pub fn new(names: &[&str]) -> Arc<Self> {
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let owner = Arc::new(NativeOwner {
            tools: names
                .iter()
                .map(|name| {
                    (
                        (*name).into(),
                        Arc::new(Concrete {
                            name: (*name).into(),
                            invocations: invocations.clone(),
                        }) as Arc<dyn Tool>,
                    )
                })
                .collect(),
            gates: AtomicUsize::new(0),
        });
        let restriction = Arc::new(Restriction::default());
        let proofs = Kind::ALL
            .into_iter()
            .filter_map(|kind| {
                let instance = owner.local_tool_instance(kind.execution_name())?;
                Some((
                    kind,
                    HostFrameworkSessionTool::from_host_instance(
                        kind,
                        instance,
                        owner.clone(),
                        restriction.clone(),
                    )
                    .unwrap(),
                ))
            })
            .collect();
        Arc::new(Self {
            owner,
            proofs,
            invocations,
            restriction,
            proofs_enabled: AtomicBool::new(true),
        })
    }
    pub fn invocations(&self) -> Vec<String> {
        self.invocations.lock().unwrap().clone()
    }
    pub fn gates(&self) -> usize {
        self.owner.gates.load(Ordering::SeqCst)
    }
}
#[async_trait]
impl ToolExecutor for FixtureTools {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        self.owner.execute(call).await
    }
    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        self.owner.execute_with_context_outcome(call, ctx).await
    }
    async fn check_permissions_for_resolved(
        &self,
        call: &ToolCall,
        name: &str,
        args: &Value,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.owner
            .check_permissions_for_resolved(call, name, args, ctx)
            .await
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        self.owner.list_tools()
    }
    fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
        self.owner
            .owns_exact_tool(name)
            .then_some(self as &dyn ToolExecutor)
    }
    fn local_execution_contract(&self, name: &str) -> Option<&'static str> {
        self.owner.local_execution_contract(name)
    }
    fn local_tool_instance(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.owner.local_tool_instance(name)
    }
    fn local_framework_session_tool(&self, name: &str) -> Option<HostFrameworkSessionTool> {
        self.proofs_enabled
            .load(Ordering::SeqCst)
            .then(|| self.proofs.get(&Kind::from_execution_name(name)?).cloned())
            .flatten()
    }
}

pub(crate) fn park(
    session: &mut bamboo_domain::Session,
    call: &ToolCall,
    name: &str,
    known: bool,
    contract: Option<&str>,
) -> super::PermissionReplayTarget {
    use bamboo_agent_core::tools::{PendingToolOperation, PENDING_TOOL_OPERATION_METADATA_KEY};
    use bamboo_tools::permission::{
        PermissionMode, PermissionReasonCode, PermissionRequest, PermissionType, RiskLevel,
    };
    let generation = "fixture-generation";
    let request = PermissionRequest {
        request_id: call.id.clone(),
        request_generation: generation.into(),
        session_id: session.id.clone(),
        workspace_path: None,
        tool_name: name.into(),
        permission_type: PermissionType::ExecuteCommand,
        resource: "fixture".into(),
        operation_summary: "synthetic replay".into(),
        risk_level: RiskLevel::Low,
        reason_code: PermissionReasonCode::RiskThreshold,
        effective_mode: PermissionMode::Default,
        bypass_requested: false,
        auto_approve_requested: false,
        policy_revision: 0,
        matched_rule: None,
        allowed_decisions: vec![],
        suggested_matchers: vec![],
    };
    session.add_message(bamboo_domain::Message::assistant(
        "",
        Some(vec![call.clone()]),
    ));
    let mut result = bamboo_domain::Message::tool_result(&call.id, "synthetic pending");
    let operation = PendingToolOperation::capture_with_contract(
        bamboo_domain::SessionCallerReference::from(&*session),
        &result.id,
        call,
        name,
        generation,
        contract,
        known,
    );
    let mut metadata = json!({"permission_request":request});
    metadata[PENDING_TOOL_OPERATION_METADATA_KEY] = operation.metadata_value();
    result.metadata = Some(metadata);
    session.add_message(result);
    super::find_permission_replay_target(session, &call.id, Some(generation)).unwrap()
}
