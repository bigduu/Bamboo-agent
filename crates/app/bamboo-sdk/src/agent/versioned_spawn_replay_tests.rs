//! Real SDK parking/typed decision/reopen with version-selected concrete owners.
use super::*;
use bamboo_engine::session_app::supervisor::SupervisorSessionService;

struct VersionedProbeTool {
    name: &'static str,
    probe: Arc<Probe>,
}
#[async_trait]
impl Tool for VersionedProbeTool {
    fn name(&self) -> &str {
        self.name
    }
    fn description(&self) -> &str {
        "record one approved scoped operation"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object"})
    }
    fn classify(&self, _: &Value) -> bamboo_agent_core::ToolClass {
        bamboo_agent_core::ToolClass::MUTATING_SERIAL
    }
    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        self.probe.calls.lock().unwrap().push(Invocation {
            name: self.name.into(),
            arguments: args,
            generation: current_permission_replay_generation(
                ctx.session_id().unwrap(),
                &ctx.tool_call_id,
            ),
            flags: ToolExecutionSessionFlags {
                bypass_permissions: ctx.bypass_permissions,
                auto_approve_permissions: ctx.auto_approve_permissions,
                plan_read_only: ctx.plan_read_only,
            },
            supervisor_absent: ctx.executing_supervisor.is_none(),
        });
        self.probe.actions.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            "approved exact owner invoked",
        )))
    }
}

struct VersionedProbeSurface {
    legacy: Arc<dyn ToolExecutor>,
    independent: Arc<dyn ToolExecutor>,
}
#[async_trait]
impl ToolExecutor for VersionedProbeSurface {
    fn for_session(&self, session: &Session) -> Option<Arc<dyn ToolExecutor>> {
        Some(if session.independent_root_spawn_contract() {
            self.independent.clone()
        } else {
            self.legacy.clone()
        })
    }
    async fn execute(&self, _: &ToolCall) -> Result<ToolResult, ToolError> {
        panic!("SDK must bind the Session tool contract before execution")
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        self.legacy.list_tools()
    }
}

// Use the production overlay composition and the real Host operation gate.
// A bare custom tool named spawn_session is deliberately not a framework owner.
struct VersionedProbeRootPermissionGate(Arc<bamboo_tools::BuiltinToolExecutor>);

#[async_trait]
impl ToolExecutor for VersionedProbeRootPermissionGate {
    async fn execute(&self, _: &ToolCall) -> Result<ToolResult, ToolError> {
        panic!("permission-only framework probe gate cannot execute an operation")
    }
    async fn check_permissions_for_resolved(
        &self,
        call: &ToolCall,
        execution_name: &str,
        args: &Value,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        assert_eq!(execution_name, "spawn_session");
        self.0
            .check_framework_root_spawn_permissions(call, args, ctx)
            .await
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        Vec::new()
    }
}

fn versioned_agent(fixture: &Fixture) -> (Arc<PermissionConfig>, Agent) {
    let config = Arc::new(PermissionConfig::new());
    config.set_ask_rules(["spawn_session".into(), "SubAgent".into()]);
    let make = |name| -> Arc<bamboo_tools::BuiltinToolExecutor> {
        let registry = bamboo_tools::ToolRegistry::new();
        registry
            .register(VersionedProbeTool {
                name,
                probe: fixture.probe.clone(),
            })
            .unwrap();
        Arc::new(
            bamboo_tools::BuiltinToolExecutor::with_registry_and_permissions(
                registry,
                Arc::new(ConfigPermissionChecker::new(config.clone())),
            ),
        )
    };
    let root_builtin = make("spawn_session");
    let independent = Arc::new(
        bamboo_server_tools::overlay_executor::OverlayToolExecutor::new(
            root_builtin.clone(),
            Arc::new(VersionedProbeTool {
                name: "spawn_session",
                probe: fixture.probe.clone(),
            }),
        )
        .with_additional_permission_gate(Arc::new(VersionedProbeRootPermissionGate(root_builtin))),
    );
    let surface = Arc::new(VersionedProbeSurface {
        legacy: make("SubAgent"),
        independent,
    });
    let agent = fixture.agent_with_executor(Some(config.clone()), surface, None);
    (config, agent)
}

async fn versioned_pending(fixture: &Fixture, version: Option<u32>) -> (Session, Value) {
    let session = if version == Some(2) {
        let receipt = SupervisorSessionService::new(fixture.store.clone())
            .get_or_create_default("test-model")
            .await
            .unwrap();
        fixture.reload(&receipt.session_id).await
    } else {
        let mut session = Session::new("legacy-spawn-sdk", "test-model");
        session.session_version = version;
        fixture.store.save_session(&session).await.unwrap();
        session
    };
    let args = if version == Some(2) {
        json!({"operation_id":"7d4fd839-3f5f-4e26-b587-a0d2cb5724cb", "expected_state_revision":1,
            "project_id":"explicit-project", "workspace_path":"/explicit/worker", "message":"bounded instruction"})
    } else {
        json!({"action":"create", "prompt":"bounded child instruction"})
    };
    *fixture.probe.next_call.lock().unwrap() = Some(ToolCall {
        id: CALL.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "default::spawnSession".into(),
            arguments: args.to_string(),
        },
    });
    let (_, agent) = versioned_agent(fixture);
    let initial = events(agent.run_stream(session.clone(), "perform the explicit operation")).await;
    assert!(
        initial
            .iter()
            .any(|event| matches!(event, AgentEvent::NeedClarification { .. })),
        "{initial:?}"
    );
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
    (fixture.reload(&session.id).await, args)
}

#[tokio::test]
async fn versioned_spawn_typed_replay_reopens_with_original_owner_and_no_reinterpretation() {
    for version in [None, Some(1), Some(2)] {
        let mut fixture = Fixture::new().await;
        let (pending, args) = versioned_pending(&fixture, version).await;
        let request = current_request(&pending);
        let expected = if version == Some(2) {
            "spawn_session"
        } else {
            "SubAgent"
        };
        assert_eq!(request.tool_name, expected);
        fixture.reopen_store().await;
        let (_, unanswered) = versioned_agent(&fixture);
        events(unanswered.resume_stream(fixture.reload(&pending.id).await)).await;
        assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
        let approved = fixture.approve(&request).await;
        fixture.reopen_store().await;
        let (config, restarted) = versioned_agent(&fixture);
        let completed = events(restarted.resume_stream(approved)).await;
        assert!(
            !completed
                .iter()
                .any(|event| matches!(event, AgentEvent::NeedClarification { .. })),
            "{completed:?}"
        );
        assert_eq!(
            fixture.probe.actions.load(Ordering::SeqCst),
            1,
            "{version:?}"
        );
        let calls = fixture.probe.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, expected);
        assert_eq!(calls[0].arguments, args);
        assert_eq!(calls[0].flags, ToolExecutionSessionFlags::default());
        assert_eq!(
            calls[0].generation.as_deref(),
            Some(request.request_generation.as_str())
        );
        assert_eq!(calls[0].supervisor_absent, version != Some(2));
        assert!(!config.consume_once_for_generation(
            &pending.id,
            CALL,
            &request.request_generation,
            request.permission_type,
            &request.resource
        ));
        let saved = fixture.reload(&pending.id).await;
        assert_eq!(saved.session_version, version);
        assert!(!saved
            .metadata
            .contains_key(PERMISSION_REEXECUTE_METADATA_KEY));
        assert!(!saved
            .metadata
            .contains_key(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY));
        assert!(saved
            .messages
            .iter()
            .any(|m| m.tool_calls.as_ref().is_some_and(|calls| calls
                .iter()
                .any(|c| c.function.name == "default::spawnSession"))));
    }
}

#[tokio::test]
async fn versioned_spawn_conflicting_retained_contract_keeps_approval_markers_and_grants_absent() {
    let fixture = Fixture::new().await;
    let (pending, _) = versioned_pending(&fixture, Some(2)).await;
    let request = current_request(&pending);
    let mut approved = fixture.approve(&request).await;
    approved.observe_main_session_version(Some(1));
    let original = serde_json::to_value(&approved).unwrap();
    let (config, agent) = versioned_agent(&fixture);
    let error = agent
        .run_session(&mut approved)
        .await
        .expect_err("conflicting retained behavior must not replay");
    assert!(
        error.to_string().contains("session_version_mismatch"),
        "{error}"
    );
    assert_eq!(serde_json::to_value(approved).unwrap(), original);
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
    assert!(!config.consume_once_for_generation(
        &pending.id,
        CALL,
        &request.request_generation,
        request.permission_type,
        &request.resource
    ));
}

#[tokio::test]
async fn versioned_spawn_untyped_replay_without_root_port_never_enters_the_child_executor() {
    let fixture = Fixture::new().await;
    let (mut session, _) = parked("missing-root-port-v2", false);
    assert_eq!(session.session_version, Some(2));
    for message in &mut session.messages {
        if let Some(calls) = &mut message.tool_calls {
            for call in calls {
                call.function.name = "spawnSession".into();
                call.function.arguments =
                    json!({"action":"create", "prompt":"must not become a Child"}).to_string();
            }
        }
    }
    fixture.repo.save(&mut session).await.unwrap();
    let registry = bamboo_tools::ToolRegistry::new();
    registry
        .register(VersionedProbeTool {
            name: "SubAgent",
            probe: fixture.probe.clone(),
        })
        .unwrap();
    let executor = std::sync::Arc::new(bamboo_tools::BuiltinToolExecutor::with_registry(registry));
    let agent = fixture.agent_with_executor(None, executor, None);
    let mut approved = agent.answer(&session.id, "Approve").await.unwrap().session;
    agent.run_session(&mut approved).await.unwrap();
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
    assert!(fixture.probe.calls.lock().unwrap().is_empty());
    assert!(approved.messages.iter().any(
        |message| message.role == Role::Tool && message.content.contains("no longer available")
    ));
}
