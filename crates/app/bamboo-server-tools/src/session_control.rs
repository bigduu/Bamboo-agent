//! Narrow model control over explicitly scoped independent Roots.

pub mod root_creation;
pub mod root_run;
pub mod versioned {
    //! Fresh Root creation with separately selected retained operation owners.
    //!
    //! Supported legacy Sessions use the same fresh Root entry. Their retained
    //! Child/custom operations still use their frozen semantic selector. Binding
    //! never creates a Project grant, checkout, execution or permission.

    use std::sync::Arc;

    use async_trait::async_trait;
    use bamboo_agent_core::tools::{
        retained_tool_execution_for, retained_tool_owner_lookup, RetainedToolExecution, Tool,
        ToolCall, ToolClass, ToolCtx, ToolError, ToolExecutionContext, ToolExecutor, ToolOutcome,
        ToolResult, ToolSchema, FRAMEWORK_ROOT_SPAWN_CONTRACT_V1,
    };
    use bamboo_domain::{Session, SessionCallerReference, Storage};
    use serde_json::Value;
    use tokio::sync::RwLock;

    use super::root_creation::{
        SessionRootCreatePort, SupervisedRootCreatePort, SupervisedRootCreateRequest,
        SupervisorRootCreateAdapter,
    };
    use crate::overlay_executor::OverlayToolExecutor;

    pub struct SpawnSessionTool {
        creator: Arc<dyn SessionRootCreatePort>,
        storage: Arc<dyn Storage>,
        config: Arc<RwLock<bamboo_llm::Config>>,
        installed: Arc<dyn ToolExecutor>,
    }

    impl SpawnSessionTool {
        pub fn new(
            creator: Arc<dyn SupervisedRootCreatePort>,
            storage: Arc<dyn Storage>,
            config: Arc<RwLock<bamboo_llm::Config>>,
            installed: Arc<dyn ToolExecutor>,
        ) -> Self {
            Self::new_with_caller_port(
                Arc::new(SupervisorRootCreateAdapter::new(creator, storage.clone())),
                storage,
                config,
                installed,
            )
        }

        /// Install a trusted Host implementation with explicit caller scope.
        /// The default constructor above keeps its strict Supervisor adapter.
        pub fn new_with_caller_port(
            creator: Arc<dyn SessionRootCreatePort>,
            storage: Arc<dyn Storage>,
            config: Arc<RwLock<bamboo_llm::Config>>,
            installed: Arc<dyn ToolExecutor>,
        ) -> Self {
            Self {
                creator,
                storage,
                config,
                installed,
            }
        }

        async fn validate_context(
            &self,
            ctx: &ToolCtx,
        ) -> Result<SessionCallerReference, ToolError> {
            if ctx.plan_read_only {
                return Err(ToolError::Execution(
                    "spawn_session is unavailable in Plan mode".into(),
                ));
            }
            let caller = ctx.session_id().ok_or_else(|| {
                ToolError::Execution("spawn_session requires the original executing Session".into())
            })?;
            let observation = ctx.executing_caller_for(caller).ok_or_else(|| {
                ToolError::Execution(
                    "spawn_session requires the original Host-observed caller identity".into(),
                )
            })?;
            let canonical = load_canonical(self.storage.as_ref(), caller).await?;
            if !observation.matches_session(&canonical) {
                return Err(ToolError::Execution(
                    "spawn_session requires the same live caller lifetime and authority snapshot"
                        .into(),
                ));
            }
            self.check_disabled(&canonical).await?;
            // Scope comes only from the installed Host port. An ordinary caller
            // has no implicit grant from its Project or management links.
            let scope = self.creator.scope(observation).await?;
            if !scope.success {
                // Preserve the Host's precise unavailable stage/code. In
                // particular, an unconfigured first-input writer must not look
                // like a missing Project grant or invite another operation.
                return Err(ToolError::Execution(format!(
                    "session_root_scope_unavailable: {}",
                    scope.result
                )));
            }
            // The scope read may await another Host boundary. Retain the
            // original observation; never refresh it from the later snapshot.
            let final_canonical = load_canonical(self.storage.as_ref(), caller).await?;
            if !observation.matches_session(&final_canonical) {
                return Err(ToolError::Execution(
                    "spawn_session caller changed during Host scope admission".into(),
                ));
            }
            self.check_disabled(&final_canonical).await?;
            Ok(observation.clone())
        }

        async fn check_disabled(&self, canonical: &Session) -> Result<(), ToolError> {
            // Repeat after the awaited scope read and on approval replay. A
            // disabled Root operation never becomes a Child operation.
            let config = self.config.read().await;
            if config.disabled_tool_references().iter().any(|reference| {
                bamboo_domain::resolve_session_tool_reference(
                    canonical.session_version,
                    reference,
                    |name| name == "spawn_session" || self.installed.owns_exact_tool(name),
                )
                .as_deref()
                    == Some("spawn_session")
            }) {
                return Err(ToolError::Execution(
                    "spawn_session is disabled by this Host".into(),
                ));
            }
            Ok(())
        }
    }

    async fn load_canonical(storage: &dyn Storage, id: &str) -> Result<Session, ToolError> {
        let session = storage
            .load_execution_admission_snapshot(id)
            .await
            .map_err(|error| {
                ToolError::Execution(format!("spawn contract admission failed: {error}"))
            })?
            .ok_or_else(|| ToolError::Execution("spawn contract Session is unavailable".into()))?;
        session
            .check_execution_compatibility()
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        Ok(session)
    }

    #[async_trait]
    impl Tool for SpawnSessionTool {
        fn name(&self) -> &str {
            "spawn_session"
        }
        fn execution_contract(&self) -> Option<&'static str> {
            Some(FRAMEWORK_ROOT_SPAWN_CONTRACT_V1)
        }
        fn description(&self) -> &str {
            "Create an independent Ordinary Root in an explicit existing Project/workspace. This fresh call has the same Root meaning in every supported Session version. The canonical Supervisor uses its current-user domain by default; other callers need their installed finite Host creation scope. Read session_control(action=scope) and use its exact revision, stable operation UUID and one first instruction. Missing or stale authority is a blocker, never a reason to guess a grant. Use a different existing Host-bound checkout when isolation is needed; this tool does not create or bind worktrees. Explicit SubAgent creates a bounded Child sharing its parent's workspace. Exact retries preserve every field and recover the same Root/Inbox admission/activation; receipts do not mean task completion. No history, model or work permissions are inherited, and default Supervisor access creates no Project ACL. A missing, disabled, stale or denied Root entry never falls back to Child spawning."
        }
        fn parameters_schema(&self) -> Value {
            super::root_creation::request_schema()
        }
        fn classify(&self, _args: &Value) -> ToolClass {
            ToolClass::MUTATING_SERIAL
        }
        async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
            let caller = self.validate_context(&ctx).await?;
            let request: SupervisedRootCreateRequest =
                serde_json::from_value(args).map_err(|error| {
                    ToolError::InvalidArguments(format!("invalid spawn_session request: {error}"))
                })?;
            request.validate()?;
            self.creator
                .create(&caller, request)
                .await
                .map(ToolOutcome::Completed)
        }
    }

    struct RootSpawnPermissionGate(Arc<bamboo_tools::BuiltinToolExecutor>);

    #[async_trait]
    impl ToolExecutor for RootSpawnPermissionGate {
        async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
            Err(ToolError::Execution(
                "permission-only Root operation gate".into(),
            ))
        }
        async fn check_permissions_for_resolved(
            &self,
            call: &ToolCall,
            execution_name: &str,
            args: &Value,
            ctx: &ToolExecutionContext<'_>,
        ) -> Result<Option<ToolOutcome>, ToolError> {
            if execution_name != "spawn_session" {
                return Err(ToolError::Execution(
                    "Root operation permission owner differs".into(),
                ));
            }
            self.0
                .check_framework_root_spawn_permissions(call, args, ctx)
                .await
        }
        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    #[derive(Clone)]
    pub struct VersionedRootTools {
        legacy: Arc<dyn ToolExecutor>,
        independent: Arc<dyn ToolExecutor>,
        spawn: Arc<SpawnSessionTool>,
        storage: Arc<dyn Storage>,
        contract: Option<bool>,
    }

    impl VersionedRootTools {
        pub fn new(
            legacy: Arc<dyn ToolExecutor>,
            spawn: Arc<SpawnSessionTool>,
            storage: Arc<dyn Storage>,
            permission_executor: Arc<bamboo_tools::BuiltinToolExecutor>,
        ) -> Self {
            let independent = Arc::new(
                OverlayToolExecutor::new(legacy.clone(), spawn.clone())
                    .with_additional_permission_gate(Arc::new(RootSpawnPermissionGate(
                        permission_executor,
                    ))),
            );
            Self {
                legacy,
                independent,
                spawn,
                storage,
                contract: None,
            }
        }
        fn selected(&self) -> &Arc<dyn ToolExecutor> {
            &self.independent
        }

        fn retained_selected(
            &self,
            execution_name: &str,
            execution: &RetainedToolExecution,
        ) -> Result<&Arc<dyn ToolExecutor>, ToolError> {
            match execution.execution_contract() {
                Some(FRAMEWORK_ROOT_SPAWN_CONTRACT_V1) if execution_name == "spawn_session" => {
                    Ok(&self.independent)
                }
                Some(FRAMEWORK_ROOT_SPAWN_CONTRACT_V1) => Err(ToolError::Execution(
                    "retained Root contract has a different exact owner".into(),
                )),
                _ if execution_name == "spawn_session" && !execution.contract_known() => {
                    Err(ToolError::Execution(
                        "retained spawn owner has unknown provenance; resend explicitly".into(),
                    ))
                }
                // The legacy chain still owns exact custom tools shadowed by
                // the new Root overlay. Do not resolve their old spelling again.
                _ => Ok(&self.legacy),
            }
        }
        fn classification_reference(&self, reference: &str) -> String {
            if bamboo_domain::independent_root_spawn_reference(reference) {
                bamboo_domain::resolve_session_tool_reference(Some(2), reference, |name| {
                    self.independent.owns_exact_tool(name)
                })
                .unwrap_or_else(|| "spawn_session".into())
            } else {
                reference.to_owned()
            }
        }
        async fn route<'a>(
            &'a self,
            reference: &str,
            ctx: &ToolExecutionContext<'_>,
        ) -> Result<(&'a Arc<dyn ToolExecutor>, String), ToolError> {
            let version = if bamboo_domain::independent_root_spawn_reference(reference) {
                let id = ctx.session_id.ok_or_else(|| {
                    ToolError::Execution(
                        "spawn_session requires a canonical Session context".into(),
                    )
                })?;
                let session = load_canonical(self.storage.as_ref(), id).await?;
                if self
                    .contract
                    .is_some_and(|v2| v2 != session.independent_root_spawn_contract())
                {
                    return Err(ToolError::Execution(
                        "spawn contract changed; reload the original Session".into(),
                    ));
                }
                session.session_version
            } else if self.contract == Some(true) {
                Some(2)
            } else {
                Some(1)
            };
            let tools = self.selected();
            let name = bamboo_domain::resolve_session_tool_reference(version, reference, |name| {
                tools.owns_exact_tool(name)
            })
            .ok_or_else(|| ToolError::NotFound(reference.to_owned()))?;
            if name == "spawn_session" {
                self.spawn.validate_context(&ctx.to_tool_ctx()).await?;
            }
            Ok((tools, name))
        }

        /// An exact owner was already selected by live admission or restored
        /// from a host-owned pending-operation binding. Revalidate the live
        /// Session/owner gates without interpreting the historical alias again.
        async fn route_exact<'a>(
            &'a self,
            call: &ToolCall,
            execution_name: &str,
            ctx: &ToolExecutionContext<'_>,
        ) -> Result<&'a Arc<dyn ToolExecutor>, ToolError> {
            if let Some(retained) = retained_tool_execution_for(call, execution_name)? {
                let tools = self.retained_selected(execution_name, &retained)?;
                if bamboo_domain::independent_root_spawn_reference(&call.function.name)
                    || execution_name == "spawn_session"
                {
                    let id = ctx.session_id.ok_or_else(|| {
                        ToolError::Execution("retained spawn requires its canonical Session".into())
                    })?;
                    let session = load_canonical(self.storage.as_ref(), id).await?;
                    if self.contract.is_some_and(|contract| {
                        contract != session.independent_root_spawn_contract()
                    }) {
                        return Err(ToolError::Execution(
                            "spawn contract changed; reload the original Session".into(),
                        ));
                    }
                }
                if !tools.owns_exact_tool(execution_name)
                    || (retained.contract_known()
                        && tools.execution_contract_for_exact(execution_name)
                            != retained.execution_contract())
                {
                    return Err(ToolError::Execution(
                        "retained exact owner or execution contract is unavailable".into(),
                    ));
                }
                if retained.execution_contract() == Some(FRAMEWORK_ROOT_SPAWN_CONTRACT_V1) {
                    self.spawn.validate_context(&ctx.to_tool_ctx()).await?;
                }
                return Ok(tools);
            }
            let tools = if bamboo_domain::independent_root_spawn_reference(&call.function.name)
                || execution_name == "spawn_session"
            {
                let id = ctx.session_id.ok_or_else(|| {
                    ToolError::Execution(
                        "retained spawn operation requires its canonical Session".into(),
                    )
                })?;
                let session = load_canonical(self.storage.as_ref(), id).await?;
                if self
                    .contract
                    .is_some_and(|v2| v2 != session.independent_root_spawn_contract())
                {
                    return Err(ToolError::Execution(
                        "spawn contract changed; reload the original Session".into(),
                    ));
                }
                self.selected()
            } else {
                self.selected()
            };
            if !tools.owns_exact_tool(execution_name) {
                return Err(ToolError::NotFound(execution_name.to_owned()));
            }
            if execution_name == "spawn_session" {
                self.spawn.validate_context(&ctx.to_tool_ctx()).await?;
            }
            Ok(tools)
        }
    }

    #[async_trait]
    impl ToolExecutor for VersionedRootTools {
        fn for_session(&self, session: &Session) -> Option<Arc<dyn ToolExecutor>> {
            let mut bound = self.clone();
            bound.contract = Some(session.independent_root_spawn_contract());
            Some(Arc::new(bound))
        }
        async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
            self.execute_with_context_outcome(call, ToolExecutionContext::none(&call.id))
                .await
                .map(ToolOutcome::into_tool_result)
        }
        async fn execute_with_context(
            &self,
            call: &ToolCall,
            ctx: ToolExecutionContext<'_>,
        ) -> Result<ToolResult, ToolError> {
            self.execute_with_context_outcome(call, ctx)
                .await
                .map(ToolOutcome::into_tool_result)
        }
        async fn execute_with_context_outcome(
            &self,
            call: &ToolCall,
            ctx: ToolExecutionContext<'_>,
        ) -> Result<ToolOutcome, ToolError> {
            let (tools, name) = self.route(&call.function.name, &ctx).await?;
            tools
                .execute_exact_with_context_outcome(call, &name, ctx)
                .await
        }
        async fn execute_exact_with_context_outcome(
            &self,
            call: &ToolCall,
            execution_name: &str,
            ctx: ToolExecutionContext<'_>,
        ) -> Result<ToolOutcome, ToolError> {
            let tools = self.route_exact(call, execution_name, &ctx).await?;
            tools
                .execute_exact_with_context_outcome(call, execution_name, ctx)
                .await
        }
        async fn check_permissions_for(
            &self,
            call: &ToolCall,
            ctx: &ToolExecutionContext<'_>,
        ) -> Result<Option<ToolOutcome>, ToolError> {
            let (tools, name) = self.route(&call.function.name, ctx).await?;
            tools.check_permissions_for_exact(call, &name, ctx).await
        }
        async fn check_permissions_for_exact(
            &self,
            call: &ToolCall,
            execution_name: &str,
            ctx: &ToolExecutionContext<'_>,
        ) -> Result<Option<ToolOutcome>, ToolError> {
            let tools = self.route_exact(call, execution_name, ctx).await?;
            tools
                .check_permissions_for_exact(call, execution_name, ctx)
                .await
        }
        async fn check_permissions_for_resolved(
            &self,
            call: &ToolCall,
            execution_name: &str,
            args: &Value,
            ctx: &ToolExecutionContext<'_>,
        ) -> Result<Option<ToolOutcome>, ToolError> {
            // Outer Work/broker overlays still use the real inner permission gate,
            // even though their registration is outside this wrapper's catalog.
            if execution_name == "spawn_session" {
                let tools = self.route_exact(call, execution_name, ctx).await?;
                return tools
                    .check_permissions_for_resolved(call, execution_name, args, ctx)
                    .await;
            }
            self.selected()
                .check_permissions_for_resolved(call, execution_name, args, ctx)
                .await
        }
        fn list_tools(&self) -> Vec<ToolSchema> {
            self.selected().list_tools()
        }
        fn owns_exact_tool(&self, name: &str) -> bool {
            if let Some(retained) = retained_tool_owner_lookup(name) {
                return self
                    .retained_selected(name, &retained)
                    .is_ok_and(|tools| tools.owns_exact_tool(name));
            }
            self.selected().owns_exact_tool(name)
        }
        fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
            if let Some(retained) = retained_tool_owner_lookup(name) {
                return self
                    .retained_selected(name, &retained)
                    .ok()
                    .and_then(|tools| tools.exact_tool_owner(name));
            }
            self.selected().exact_tool_owner(name)
        }
        fn tool_guidance(&self) -> Option<String> {
            self.selected().tool_guidance()
        }
        fn tool_mutability(&self, name: &str) -> bamboo_agent_core::ToolMutability {
            self.selected()
                .tool_mutability(&self.classification_reference(name))
        }
        fn tool_concurrency_safe(&self, name: &str) -> bool {
            self.selected()
                .tool_concurrency_safe(&self.classification_reference(name))
        }
        fn call_parallel_classification(
            &self,
            call: &ToolCall,
        ) -> (bamboo_agent_core::ToolMutability, bool) {
            let mut selected = call.clone();
            selected.function.name = self.classification_reference(&call.function.name);
            self.selected().call_parallel_classification(&selected)
        }
    }
}

use root_creation::{SupervisedRootCreatePort, SupervisedRootCreateRequest};
use root_run::SupervisedRootRunPort;

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_domain::{LoopControlRequest, SessionActivationDisposition, SupervisorReference};
use bamboo_engine::{SessionMessenger, SessionMessengerError};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum SessionControlArgs {
    Scope,
    Create {
        request: SupervisedRootCreateRequest,
    },
    Followup {
        target_session_id: String,
        #[serde(default)]
        target_ref: Option<bamboo_domain::SupervisorCurrentUserTarget>,
        operation_id: String,
        message: String,
    },
    ObserveRun {
        target_session_id: String,
        target_ref: bamboo_domain::SupervisorCurrentUserTarget,
    },
    Cancel {
        target_session_id: String,
        target_ref: bamboo_domain::SupervisorCurrentUserTarget,
        expected_run_id: String,
    },
    Loop {
        target_session_id: String,
        #[serde(default)]
        expected_link_revision: Option<u64>,
        command: LoopControlRequest,
        #[serde(default)]
        receipt_operation_id: Option<String>,
    },
}

pub struct SessionControlTool {
    messenger: Arc<SessionMessenger>,
    root_creator: Option<Arc<dyn SupervisedRootCreatePort>>,
    root_run: Option<Arc<dyn SupervisedRootRunPort>>,
}

impl SessionControlTool {
    pub fn new(messenger: Arc<SessionMessenger>) -> Self {
        Self {
            messenger,
            root_creator: None,
            root_run: None,
        }
    }

    pub fn with_root_creator(mut self, creator: Arc<dyn SupervisedRootCreatePort>) -> Self {
        self.root_creator = Some(creator);
        self
    }

    pub fn with_root_run(mut self, port: Arc<dyn SupervisedRootRunPort>) -> Self {
        self.root_run = Some(port);
        self
    }
}

#[async_trait]
impl Tool for SessionControlTool {
    fn name(&self) -> &str {
        "session_control"
    }

    fn description(&self) -> &str {
        if self.root_run.is_some() && self.root_creator.is_some() {
            return "Supervisor management covers all Projects and Sessions in this application's current-user domain by default. scope returns Projects, canonical Supervisor identity and state_revision; legacy allowed_projects/links are compatibility metadata, not a permission gate. create requires that revision, an existing Project/workspace, stable operation UUID and first instruction; it creates one independent Standard Ordinary Root with its own unchanged work permissions. It does not attach an ACL, create Ticket/Work or make a worktree. Prefer continuing an existing Session: obtain current_user_target from session_history list/inspect, then followup with that unchanged target_ref, stable operation_id and identical message on retry. Receipts describe creation/admission/activation, never completion. For an ordinary Root, observe_run uses that same target_ref and returns its current exact run_id; cancel requires the unchanged target_ref and observed expected_run_id. cancel_requested only signals that owner: wait for actual finalization before claiming it stopped. A stale target or run must never be redirected to a successor. No Supervisor history, prompt, model or permissions are inherited. SubAgent creates a bounded Child. loop retains its separate active-owner grant, finite opt-in and exact CAS; default management never grants that capability. Do not answer approvals, change permissions, substitute shell execution or create overlapping workers when blocked.";
        }
        if self.root_run.is_some() {
            return "Supervisor followup and exact Root run control preserve the current user's original Session context. Copy current_user_target from session_history. observe_run returns the current Root run_id; cancel requires that same target_ref and exact expected_run_id. cancel_requested signals only that owner and does not prove final termination. A stale target or run must never redirect to a successor. Existing work Sessions keep their permissions. loop remains separately Host-granted; do not answer approvals or use direct file/shell tools.";
        }
        if self.root_creator.is_some() {
            return "Supervisor management covers all Projects and Sessions in this application's current-user domain by default. scope returns Projects, canonical Supervisor identity and state_revision; legacy allowed_projects/links are compatibility metadata, not a permission gate. create requires that revision, an existing Project/workspace, stable operation UUID and first instruction; it creates one independent Standard Ordinary Root with its own unchanged work permissions. It does not attach an ACL, create Ticket/Work or make a worktree. Prefer continuing an existing Session: obtain current_user_target from session_history list/inspect, then followup with that unchanged target_ref, stable operation_id and identical message on retry. Receipts describe creation/admission/activation, never completion. No Supervisor history, prompt, model or permissions are inherited. SubAgent creates a bounded Child. loop retains its separate active-owner grant, finite opt-in and exact CAS; default management never grants that capability. Do not answer approvals, change permissions, substitute shell execution or create overlapping workers when blocked.";
        }
        "Supervisor followup preserves a current-user Session's original context and work permissions. Copy current_user_target from session_history list/inspect into target_ref; preserve it, operation_id and message on retry. A stale birth/generation/ancestry is denied, not redirected to a replacement Session. Admission/activation receipts do not prove consumption or completion. No per-Project authorization or attachment is needed. loop is a separate capability: it requires an explicit active-owner Host grant and existing finite opt-in; status/start/update/stop keep exact link/generation/control CAS and objective receipts. Default management does not enable loops, answer pending human approvals, grant permissions, create successors or run a shell. Plan and approval replay cannot issue loop commands."
    }

    fn parameters_schema(&self) -> Value {
        let mut schema = json!({
            "type": "object", "additionalProperties": false,
            "properties": {
                "action": {"type": "string", "enum": ["followup", "loop"],
                    "description": "followup requires target_session_id, operation_id and message. loop requires target_session_id and command; mutations also require expected_link_revision. Only loop status may query receipt_operation_id or omit the link revision."},
                "target_session_id": {"type": "string", "minLength": 1, "maxLength": 256},
                "target_ref": {"type":"object", "description":"followup only: copy the unchanged current_user_target returned by session_history list/inspect. This selects the exact birth/generation, never grants access. Refresh on a stale-target error; an uncertain delivery retains every original argument."},
                "operation_id": {"type": "string", "minLength": 1, "maxLength": 256,
                    "description": "followup only: stable operation id for this target; no whitespace padding, slashes or '..'. Reuse it only with the same message."},
                "message": {"type": "string", "minLength": 1, "maxLength": 16384,
                    "description": "followup only: one instruction, at most 16 KiB in UTF-8."},
                "expected_link_revision": {"type": ["integer", "null"], "minimum": 1,
                    "description": "loop only: exact current nonzero Supervisor link revision; required for start/update/stop. Status alone may omit it to bootstrap after full canonical authority and host-grant checks."},
                "command": scoped_loop_command_schema(),
                "receipt_operation_id": {"type": ["string", "null"], "format": "uuid",
                    "description": "loop status only: exact operation UUID for bounded target-owner receipt lookup; never a grant or a new command."}
            },
            "required": ["action", "target_session_id"],
            "oneOf": [
                {
                    "properties": {
                        "action": {"const": "followup"},
                        "command": false,
                        "expected_link_revision": false,
                        "receipt_operation_id": false
                    },
                    "required": ["operation_id", "message", "target_ref"]
                },
                {
                    "properties": {
                        "action": {"const": "loop"},
                        "operation_id": false,
                        "target_ref": false,
                        "message": false
                    },
                    "required": ["command"],
                    "oneOf": [
                        {
                            "properties": {
                                "command": {"properties": {"action": {"const": "status"}}}
                            }
                        },
                        {
                            "properties": {
                                "command": {"properties": {"action": {"enum": ["start", "update", "stop"]}}},
                                "expected_link_revision": {"type": "integer"},
                                "receipt_operation_id": {"type": "null"}
                            },
                            "required": ["expected_link_revision"]
                        }
                    ]
                }
            ]
        });
        if self.root_creator.is_some() {
            schema["properties"]["action"]["enum"] = json!(["followup", "loop", "scope", "create"]);
            schema["properties"]["action"]["description"] = json!("scope needs only action; create needs request; followup/loop need the existing target_session_id and action-specific fields.");
            schema["properties"]["request"] = root_creation::request_schema();
            schema["required"] = json!(["action"]);
            let variants = schema["oneOf"].as_array_mut().unwrap();
            for variant in variants.iter_mut() {
                variant["properties"]["request"] = Value::Bool(false);
                variant["required"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!("target_session_id"));
            }
            let forbidden = [
                "target_session_id",
                "target_ref",
                "operation_id",
                "message",
                "expected_link_revision",
                "command",
                "receipt_operation_id",
            ];
            let mut scope = json!({"properties":{"action":{"const":"scope"},"request":false}});
            let mut create =
                json!({"properties":{"action":{"const":"create"}},"required":["request"]});
            for field in forbidden {
                scope["properties"][field] = Value::Bool(false);
                create["properties"][field] = Value::Bool(false);
            }
            variants.push(scope);
            variants.push(create);
        }
        if self.root_run.is_some() {
            let actions = schema["properties"]["action"]["enum"]
                .as_array_mut()
                .expect("action enumeration");
            actions.push(json!("observe_run"));
            actions.push(json!("cancel"));
            schema["properties"]["expected_run_id"] = json!({
                "type":"string", "minLength":1, "maxLength":128,
                "description":"cancel only: copy the exact run_id from observe_run; never infer or refresh it during an uncertain retry."
            });
            schema["properties"]["target_ref"]["description"] = json!(
                "Copy the unchanged current_user_target from session_history. It selects the exact target birth/generation; it is never a grant."
            );
            let variants = schema["oneOf"].as_array_mut().expect("action variants");
            for variant in variants.iter_mut() {
                variant["properties"]["expected_run_id"] = Value::Bool(false);
            }
            let forbidden = [
                "operation_id",
                "message",
                "expected_link_revision",
                "command",
                "receipt_operation_id",
                "request",
            ];
            let mut observe = json!({
                "properties":{"action":{"const":"observe_run"}, "expected_run_id":false},
                "required":["target_session_id", "target_ref"]
            });
            let mut cancel = json!({
                "properties":{"action":{"const":"cancel"}},
                "required":["target_session_id", "target_ref", "expected_run_id"]
            });
            for field in forbidden {
                observe["properties"][field] = Value::Bool(false);
                cancel["properties"][field] = Value::Bool(false);
            }
            variants.push(observe);
            variants.push(cancel);
        }
        schema
    }

    fn classify(&self, args: &Value) -> ToolClass {
        if args["action"] == "scope" || args["action"] == "observe_run" {
            ToolClass::READONLY_PARALLEL
        } else {
            ToolClass::MUTATING_SERIAL
        }
    }

    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        if ctx.plan_read_only && args["action"] != "scope" {
            return Err(ToolError::Execution(
                "session_control is unavailable in Plan mode".into(),
            ));
        }
        let caller = ctx.session_id().ok_or_else(|| {
            ToolError::Execution("session_control requires a session_id in tool context".into())
        })?;
        let supervisor = ctx
            .executing_supervisor_for(caller)
            .ok_or_else(|| {
                ToolError::Execution(
                    "session_control requires the original executing Supervisor identity".into(),
                )
            })?
            .supervisor_reference();
        let original = ctx.executing_caller_for(caller).ok_or_else(|| {
            ToolError::Execution("session_control requires its original complete caller".into())
        })?;
        let (target_session_id, target_ref, operation_id, message) = match serde_json::from_value(
            args,
        )
        .map_err(|error| {
            ToolError::InvalidArguments(format!("invalid session_control arguments: {error}"))
        })? {
            SessionControlArgs::Scope => {
                let creator = self.root_creator.as_ref().ok_or_else(|| ToolError::Execution(
                    "supervised_root_create_unavailable: this Host has no scoped Root creation port".into(),
                ))?;
                return creator
                    .scope_observed(original)
                    .await
                    .map(ToolOutcome::Completed);
            }
            SessionControlArgs::Create { request } => {
                request.validate()?;
                let creator = self.root_creator.as_ref().ok_or_else(|| ToolError::Execution(
                    "supervised_root_create_unavailable: this Host has no scoped Root creation port".into(),
                ))?;
                return creator
                    .create_observed(original, request)
                    .await
                    .map(ToolOutcome::Completed);
            }
            SessionControlArgs::ObserveRun {
                target_session_id,
                target_ref,
            } => {
                validate_root_run_caller(original, &supervisor)?;
                validate_root_run_target(&target_session_id, &target_ref)?;
                let port = self.root_run.as_ref().ok_or_else(|| {
                    ToolError::Execution("supervised_root_run_unavailable".into())
                })?;
                return port
                    .observe_run(original, &target_ref)
                    .await
                    .map(ToolOutcome::Completed);
            }
            SessionControlArgs::Cancel {
                target_session_id,
                target_ref,
                expected_run_id,
            } => {
                validate_root_run_caller(original, &supervisor)?;
                validate_root_run_target(&target_session_id, &target_ref)?;
                if !bamboo_domain::valid_session_run_id(&expected_run_id) {
                    return Err(ToolError::InvalidArguments(
                        "invalid expected_run_id".into(),
                    ));
                }
                let port = self.root_run.as_ref().ok_or_else(|| {
                    ToolError::Execution("supervised_root_run_unavailable".into())
                })?;
                return port
                    .cancel_run(original, &target_ref, &expected_run_id)
                    .await
                    .map(ToolOutcome::Completed);
            }
            SessionControlArgs::Followup {
                target_session_id,
                target_ref,
                operation_id,
                message,
            } => (target_session_id, target_ref, operation_id, message),
            SessionControlArgs::Loop {
                target_session_id,
                expected_link_revision,
                command,
                receipt_operation_id,
            } => {
                validate_loop_arguments(
                    &supervisor,
                    &target_session_id,
                    expected_link_revision,
                    &command,
                    receipt_operation_id.as_deref(),
                )?;
                if bamboo_tools::permission::permission_replay_active(caller, &ctx.tool_call_id) {
                    return Err(ToolError::Execution(
                        "session_control loop requires a fresh command, not approval replay".into(),
                    ));
                }
                let receipt = self
                    .messenger
                    .supervisor_loop_control(
                        &supervisor,
                        &target_session_id,
                        expected_link_revision,
                        &command,
                        receipt_operation_id.as_deref(),
                    )
                    .await
                    .map_err(|error| ToolError::Execution(error.to_string()))?;
                // Preserve the host/target's actual queued or committed
                // result. Delivery alone never manufactures applied=true.
                return Ok(ToolOutcome::Completed(ToolResult::text(
                    true,
                    receipt.to_string(),
                )));
            }
        };
        let target_ref = target_ref.filter(|target| target.session_id == target_session_id)
            .ok_or_else(|| ToolError::InvalidArguments(
                "followup requires the exact current_user_target from session_history list/inspect; no Project grant or attachment is needed".into()
            ))?;
        let result = self
            .messenger
            .supervisor_current_user_followup(
                original,
                &supervisor,
                &target_ref,
                &operation_id,
                &message,
            )
            .await;
        let (delivery, activation, detail) = match result {
            Ok(receipt) => {
                let activation = match receipt.activation {
                    SessionActivationDisposition::ActiveNotified => "active_notified",
                    SessionActivationDisposition::ActivationReserved => "activation_reserved",
                    SessionActivationDisposition::ActivationCoalesced => "activation_coalesced",
                };
                (receipt.delivery, activation, None)
            }
            Err(SessionMessengerError::Activation {
                receipt, source, ..
            }) => (receipt, "activation_pending", Some(source.to_string())),
            Err(SessionMessengerError::ActivationEligibility {
                receipt, source, ..
            }) => (
                receipt,
                "activation_retry_required",
                Some(source.to_string()),
            ),
            Err(error) => return Err(ToolError::Execution(error.to_string())),
        };
        let activation_retry_required = activation == "activation_retry_required";
        Ok(ToolOutcome::Completed(ToolResult::text(!activation_retry_required, json!({
            "action": "followup", "target_session_id": target_session_id,
            "operation_id": operation_id, "admitted": true,
            "receipt_id": delivery.id, "generation": delivery.generation,
            "activation": activation, "activation_error": detail,
            "note": "Admission is durable; this is not completion. A waiting target remains waiting. Retry with the same operation_id and message if activation needs recovery; read results with session_history.read_messages."
        }).to_string())))
    }
}

fn validate_root_run_caller(
    caller: &bamboo_domain::SessionCallerReference,
    supervisor: &SupervisorReference,
) -> Result<(), ToolError> {
    if caller.session_id != supervisor.session_id
        || caller.authority_identity
            != (bamboo_domain::SessionAuthorityIdentity::Supervisor {
                incarnation_id: supervisor.incarnation_id,
            })
    {
        return Err(ToolError::Execution(
            "original Supervisor observations disagree".into(),
        ));
    }
    Ok(())
}

fn validate_root_run_target(
    target_session_id: &str,
    target: &bamboo_domain::SupervisorCurrentUserTarget,
) -> Result<(), ToolError> {
    bamboo_domain::validate_supervisor_target_id(target_session_id)
        .map_err(|error| ToolError::InvalidArguments(error.into()))?;
    if target.session_id != target_session_id || target.kind != bamboo_domain::SessionKind::Root {
        return Err(ToolError::InvalidArguments(
            "exact run control requires the observed ordinary Root target".into(),
        ));
    }
    Ok(())
}

/// Project the shared bounded fields into the remote adapter's strict actions.
/// The standalone self-tool schema and its public API remain unchanged.
fn scoped_loop_command_schema() -> Value {
    let mut schema = bamboo_tools::tools::loop_control::LoopControlTool::new().parameters_schema();
    // Match Rust str::trim/char::is_whitespace exactly. ECMAScript \s
    // includes FEFF but excludes NEL, so its shorthand is not equivalent.
    // This local wrapper leaves the shared self-tool schema and DTO unchanged.
    schema["properties"]["objective"]["pattern"] = json!(
        r"[^\u0009-\u000D\u0020\u0085\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]"
    );
    schema["properties"]["objective"]["maxLength"] =
        json!(bamboo_domain::MAX_SCOPED_LOOP_OBJECTIVE_CHARS);
    let patch_fields = [
        ("objective", "string"),
        ("max_iterations", "integer"),
        ("max_total_rounds", "integer"),
    ];
    // Serde Option patches accept null as omission. Start requires actual values
    // below; Update must still carry at least one non-null patch.
    for (field, kind) in patch_fields {
        schema["properties"][field]["type"] = json!([kind, "null"]);
    }
    schema["properties"]["objective"]["description"] = json!(
        "Required for start; optional update patch. Non-whitespace text, at most 4096 Unicode scalar values, \
         counting the original text including surrounding whitespace, and at most 16384 UTF-8 bytes. \
         This remote command contract is narrower than standalone self loop_control; no trimming or normalization occurs. \
         The same limit applies to start and objective updates. \
         An edit applies no earlier than the next iteration admission; at most one objective edit may be pending."
    );
    let variant = |action: &str, required: &[&str], optional: &[&str]| {
        let mut properties = serde_json::Map::new();
        properties.insert("action".into(), json!({"const": action}));
        for field in schema["properties"].as_object().unwrap().keys() {
            if field != "action"
                && !required.contains(&field.as_str())
                && !optional.contains(&field.as_str())
            {
                properties.insert(field.clone(), Value::Bool(false));
            }
        }
        json!({"properties": properties, "required": required})
    };
    let status = variant("status", &["action"], &[]);
    let mut start = variant(
        "start",
        &[
            "action",
            "operation_id",
            "expected_revision",
            "objective",
            "max_iterations",
            "max_total_rounds",
        ],
        &["expected_generation"],
    );
    let mut update = variant(
        "update",
        &["action", "operation_id", "generation", "expected_revision"],
        &["objective", "max_iterations", "max_total_rounds"],
    );
    let stop = variant(
        "stop",
        &["action", "operation_id", "generation", "expected_revision"],
        &[],
    );
    for (field, kind) in patch_fields {
        start["properties"][field] = json!({"type": kind});
    }
    update["anyOf"] = json!([
        {"required": ["objective"], "properties": {"objective": {"type": "string"}}},
        {"required": ["max_iterations"], "properties": {"max_iterations": {"type": "integer"}}},
        {"required": ["max_total_rounds"], "properties": {"max_total_rounds": {"type": "integer"}}}
    ]);
    schema["oneOf"] = json!([status, start, update, stop]);
    schema
}

fn validate_loop_arguments(
    supervisor: &SupervisorReference,
    target_session_id: &str,
    expected_link_revision: Option<u64>,
    command: &LoopControlRequest,
    receipt_operation_id: Option<&str>,
) -> Result<(), ToolError> {
    bamboo_domain::validate_supervisor_target_id(target_session_id)
        .map_err(|error| ToolError::InvalidArguments(error.into()))?;
    if target_session_id == supervisor.session_id {
        return Err(ToolError::InvalidArguments(
            "Supervisor loop control cannot target itself".into(),
        ));
    }
    bamboo_domain::validate_scoped_loop_control_command(command)
        .map_err(ToolError::InvalidArguments)?;
    if expected_link_revision == Some(0)
        || (!matches!(command, LoopControlRequest::Status) && expected_link_revision.is_none())
    {
        return Err(ToolError::InvalidArguments(
            "loop expected_link_revision must be nonzero and is required for mutations".into(),
        ));
    }
    if let Some(operation_id) = receipt_operation_id {
        if !matches!(command, LoopControlRequest::Status) {
            return Err(ToolError::InvalidArguments(
                "receipt_operation_id is only valid for loop status".into(),
            ));
        }
        uuid::Uuid::parse_str(operation_id).map_err(|_| {
            ToolError::InvalidArguments("receipt_operation_id must be a valid UUID".into())
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_root_run_requires_matching_original_supervisor_incarnation() {
        let mut session =
            bamboo_domain::Session::new(bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID, "model");
        let incarnation_id = uuid::Uuid::from_u128(1);
        session.authority_identity =
            bamboo_domain::SessionAuthorityIdentity::Supervisor { incarnation_id };
        let caller = bamboo_domain::SessionCallerReference::from(&session);
        let supervisor = SupervisorReference {
            session_id: session.id.clone(),
            incarnation_id,
        };
        assert!(validate_root_run_caller(&caller, &supervisor).is_ok());
        let mut replaced = supervisor.clone();
        replaced.incarnation_id = uuid::Uuid::from_u128(2);
        assert!(validate_root_run_caller(&caller, &replaced).is_err());
        let mut ordinary = caller;
        ordinary.authority_identity = bamboo_domain::SessionAuthorityIdentity::Ordinary;
        assert!(validate_root_run_caller(&ordinary, &supervisor).is_err());
    }

    #[test]
    fn status_bootstrap_and_receipt_lookup_are_valid_structures_not_authority_grants() {
        let supervisor = SupervisorReference {
            session_id: bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID.into(),
            incarnation_id: uuid::Uuid::from_u128(1),
        };
        for receipt in [None, Some("A4B85B5B-BA44-488D-9900-20DD7E016E88")] {
            let mut arguments = json!({
                "action":"loop", "target_session_id":"target-a", "command":{"action":"status"},
            });
            if let Some(operation_id) = receipt {
                arguments["receipt_operation_id"] = json!(operation_id);
            }
            let SessionControlArgs::Loop {
                target_session_id,
                expected_link_revision,
                command,
                receipt_operation_id,
            } = serde_json::from_value(arguments).unwrap()
            else {
                panic!("expected loop arguments");
            };
            assert_eq!(expected_link_revision, None);
            assert_eq!(receipt_operation_id.as_deref(), receipt);
            assert_eq!(command, LoopControlRequest::Status);
            // This validates wire structure only. Live authority/host-grant
            // checks remain mandatory in supervisor_loop_control.
            validate_loop_arguments(
                &supervisor,
                &target_session_id,
                expected_link_revision,
                &command,
                receipt_operation_id.as_deref(),
            )
            .unwrap();
        }
    }
}
