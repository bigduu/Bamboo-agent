//! Narrow model control over explicitly scoped independent Roots.

pub mod root_creation;
pub mod versioned {
    //! Versioned registration of the scoped independent-Root creation port.
    //!
    //! Legacy Sessions retain the SubAgent alias. Binding selects an installed
    //! owner and never creates a Project grant, checkout, execution or permission.

    use std::sync::Arc;

    use async_trait::async_trait;
    use bamboo_agent_core::tools::{
        ExecutingSupervisorObservation, Tool, ToolCall, ToolClass, ToolCtx, ToolError,
        ToolExecutionContext, ToolExecutor, ToolOutcome, ToolResult, ToolSchema,
    };
    use bamboo_domain::{Session, Storage};
    use serde_json::Value;
    use tokio::sync::RwLock;

    use super::root_creation::{SupervisedRootCreatePort, SupervisedRootCreateRequest};
    use crate::overlay_executor::OverlayToolExecutor;

    pub struct SpawnSessionTool {
        creator: Arc<dyn SupervisedRootCreatePort>,
        storage: Arc<dyn Storage>,
        config: Arc<RwLock<bamboo_llm::Config>>,
    }

    impl SpawnSessionTool {
        pub fn new(
            creator: Arc<dyn SupervisedRootCreatePort>,
            storage: Arc<dyn Storage>,
            config: Arc<RwLock<bamboo_llm::Config>>,
        ) -> Self {
            Self {
                creator,
                storage,
                config,
            }
        }

        async fn validate_context(&self, ctx: &ToolCtx) -> Result<(), ToolError> {
            if ctx.plan_read_only {
                return Err(ToolError::Execution(
                    "spawn_session is unavailable in Plan mode".into(),
                ));
            }
            let caller = ctx.session_id().ok_or_else(|| {
                ToolError::Execution(
                    "spawn_session requires the original executing Supervisor Session".into(),
                )
            })?;
            let observation = ctx.executing_supervisor_for(caller).ok_or_else(|| {
                ToolError::Execution(
                    "spawn_session requires the original executing Supervisor identity".into(),
                )
            })?;
            let canonical = load_canonical(self.storage.as_ref(), caller).await?;
            if !canonical.independent_root_spawn_contract()
                || ExecutingSupervisorObservation::capture_from_executing_session(&canonical)
                    != Some(observation)
            {
                return Err(ToolError::Execution(
                    "spawn_session requires the same live v2 Supervisor lifetime".into(),
                ));
            }
            // Recheck live configuration after a permission pause, too. A disabled
            // Root operation cannot become a Child operation or consume its request.
            let config = self.config.read().await;
            if config.disabled_tool_references().iter().any(|reference| {
                bamboo_domain::resolve_session_tool_reference(
                    canonical.session_version,
                    reference,
                    |name| name == "spawn_session" || name == "SubAgent",
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
        fn description(&self) -> &str {
            "v2 Supervisor-only creation of an independent Ordinary Root in an explicit, existing Host-authorized Project/workspace. Read session_control(action=scope) for allowed_projects/state_revision; supply that revision, a stable operation UUID and one first instruction. Use a different Host-bound checkout when isolation is needed; this tool does not create or bind worktrees. SubAgent creates a Child sharing its parent's workspace. Exact retries reuse every request field and recover the existing creation/link/Inbox admission/activation; receipts do not mean task completion. No history, model, permissions or Project grants are inherited. A missing, disabled, stale or denied Root entry is a blocker, never an alias for Child spawning."
        }
        fn parameters_schema(&self) -> Value {
            super::root_creation::request_schema()
        }
        fn classify(&self, _args: &Value) -> ToolClass {
            ToolClass::MUTATING_SERIAL
        }
        async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
            self.validate_context(&ctx).await?;
            let request: SupervisedRootCreateRequest =
                serde_json::from_value(args).map_err(|error| {
                    ToolError::InvalidArguments(format!("invalid spawn_session request: {error}"))
                })?;
            request.validate()?;
            let supervisor = ctx
                .executing_supervisor_for(ctx.session_id().expect("validated caller"))
                .expect("validated observation")
                .supervisor_reference();
            self.creator
                .create(&supervisor, request)
                .await
                .map(ToolOutcome::Completed)
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
        ) -> Self {
            let independent = Arc::new(OverlayToolExecutor::new(legacy.clone(), spawn.clone()));
            Self {
                legacy,
                independent,
                spawn,
                storage,
                contract: None,
            }
        }
        fn selected(&self) -> &Arc<dyn ToolExecutor> {
            if self.contract == Some(true) {
                &self.independent
            } else {
                &self.legacy
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
            let tools = if version == Some(2) {
                &self.independent
            } else {
                &self.legacy
            };
            let name = bamboo_domain::resolve_session_tool_reference(version, reference, |name| {
                tools.owns_exact_tool(name)
            })
            .ok_or_else(|| ToolError::NotFound(reference.to_owned()))?;
            if name == "spawn_session" && version == Some(2) {
                self.spawn.validate_context(&ctx.to_tool_ctx()).await?;
            }
            Ok((tools, name))
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
            let (tools, name) = self.route(&call.function.name, &ctx).await?;
            if name != execution_name {
                return Err(ToolError::Execution(
                    "approved execution owner differs from the Session spawn contract".into(),
                ));
            }
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
            let (tools, name) = self.route(&call.function.name, ctx).await?;
            if name != execution_name {
                return Err(ToolError::Execution(
                    "permission owner differs from the Session spawn contract".into(),
                ));
            }
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
                self.spawn.validate_context(&ctx.to_tool_ctx()).await?;
            }
            self.selected()
                .check_permissions_for_resolved(call, execution_name, args, ctx)
                .await
        }
        fn list_tools(&self) -> Vec<ToolSchema> {
            self.selected().list_tools()
        }
        fn owns_exact_tool(&self, name: &str) -> bool {
            self.selected().owns_exact_tool(name)
        }
        fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
            self.selected().exact_tool_owner(name)
        }
        fn tool_guidance(&self) -> Option<String> {
            self.selected().tool_guidance()
        }
        fn tool_mutability(&self, name: &str) -> bamboo_agent_core::ToolMutability {
            self.selected().tool_mutability(name)
        }
        fn tool_concurrency_safe(&self, name: &str) -> bool {
            self.selected().tool_concurrency_safe(name)
        }
        fn call_parallel_classification(
            &self,
            call: &ToolCall,
        ) -> (bamboo_agent_core::ToolMutability, bool) {
            self.selected().call_parallel_classification(call)
        }
    }
}

use root_creation::{SupervisedRootCreatePort, SupervisedRootCreateRequest};

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
        operation_id: String,
        message: String,
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
}

impl SessionControlTool {
    pub fn new(messenger: Arc<SessionMessenger>) -> Self {
        Self {
            messenger,
            root_creator: None,
        }
    }

    pub fn with_root_creator(mut self, creator: Arc<dyn SupervisedRootCreatePort>) -> Self {
        self.root_creator = Some(creator);
        self
    }
}

#[async_trait]
impl Tool for SessionControlTool {
    fn name(&self) -> &str {
        "session_control"
    }

    fn description(&self) -> &str {
        if self.root_creator.is_some() {
            return "Supervisor-only control of independent Ordinary Roots in explicitly Host-authorized Projects. scope reads current allowed_projects/state_revision. create requires that revision, an explicit Project and existing Host-bound workspace, a stable operation UUID and first instruction; creates a new Standard Ordinary Root, attaches only that new lifetime and durably submits the instruction. Reuse the complete request on uncertain results; receipts report creation/link/admission/activation, never completion. No Supervisor history, prompt, model, permissions or Project grants are inherited. It does not create a Ticket/Work or isolated worktree. SubAgent creates a bounded Child instead. Existing followup continues an explicitly attached Root using stable operation_id/message. loop retains its separate live-owner Host grant and finite loop opt-in; mutations need exact link/generation/control CAS. No arbitrary attach, permission change, approval answer, successor or cadence. Report actual blocked stage/reason; never substitute shell execution or create another overlapping worker.";
        }
        "Supervisor-only control of an already attached independent Root, preserving its original context and authority. followup: use a stable operation_id per target and exactly the same message on retry; returns durable admission/activation status, not task completion. loop: requires a separate explicit host grant for the target's current active execution and its existing finite loop opt-in. command is status/start/update/stop; only status may omit expected_link_revision or query receipt_operation_id. Mutations require the exact nonzero link revision and command generation/control CAS. A target-owner receipt distinguishes queued delivery, committed control and any objective still pending its next iteration. No new session, attachment, successor, cadence, approval answer or permission is created. Plan and approval replay cannot issue loop commands. Pending human questions remain unanswered; use session_history.read_messages for bounded history."
    }

    fn parameters_schema(&self) -> Value {
        let mut schema = json!({
            "type": "object", "additionalProperties": false,
            "properties": {
                "action": {"type": "string", "enum": ["followup", "loop"],
                    "description": "followup requires target_session_id, operation_id and message. loop requires target_session_id and command; mutations also require expected_link_revision. Only loop status may query receipt_operation_id or omit the link revision."},
                "target_session_id": {"type": "string", "minLength": 1, "maxLength": 256},
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
                    "required": ["operation_id", "message"]
                },
                {
                    "properties": {
                        "action": {"const": "loop"},
                        "operation_id": false,
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
        schema
    }

    fn classify(&self, args: &Value) -> ToolClass {
        if args["action"] == "scope" {
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
        let (target_session_id, operation_id, message) = match serde_json::from_value(args)
            .map_err(|error| {
                ToolError::InvalidArguments(format!("invalid session_control arguments: {error}"))
            })? {
            SessionControlArgs::Scope => {
                let creator = self.root_creator.as_ref().ok_or_else(|| ToolError::Execution(
                    "supervised_root_create_unavailable: this Host has no scoped Root creation port".into(),
                ))?;
                return creator.scope(&supervisor).await.map(ToolOutcome::Completed);
            }
            SessionControlArgs::Create { request } => {
                request.validate()?;
                let creator = self.root_creator.as_ref().ok_or_else(|| ToolError::Execution(
                    "supervised_root_create_unavailable: this Host has no scoped Root creation port".into(),
                ))?;
                return creator
                    .create(&supervisor, request)
                    .await
                    .map(ToolOutcome::Completed);
            }
            SessionControlArgs::Followup {
                target_session_id,
                operation_id,
                message,
            } => (target_session_id, operation_id, message),
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
        let result = self
            .messenger
            .supervisor_followup(&supervisor, &target_session_id, &operation_id, &message)
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
