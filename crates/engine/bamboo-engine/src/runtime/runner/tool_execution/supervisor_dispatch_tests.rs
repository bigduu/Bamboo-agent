//! Acceptance coverage through the real native/core dispatch and owned Tool seam.

use super::*;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{
    AgenticToolResult, FunctionCall, Tool, ToolClass, ToolCtx, ToolError,
    ToolExecutionSessionFlags, ToolOutcome, ToolResult,
};
use bamboo_domain::{SessionAuthorityIdentity, SessionKind, DEFAULT_SUPERVISOR_SESSION_ID};
use bamboo_storage::SessionStoreV2;
use std::sync::Mutex;
use tokio::sync::Semaphore;

use crate::session_app::supervisor::SupervisorSessionService;

type Observed = (Option<ExecutingSupervisorObservation>, bool);

struct NoopProvider;
#[async_trait::async_trait]
impl LLMProvider for NoopProvider {
    async fn chat_stream(
        &self,
        _: &[bamboo_agent_core::Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> bamboo_llm::provider::Result<bamboo_llm::LLMStream> {
        Ok(Box::pin(futures::stream::empty()))
    }
}

struct Probe {
    service: SupervisorSessionService,
    entered: mpsc::UnboundedSender<Option<ExecutingSupervisorObservation>>,
    resume: Arc<Semaphore>,
    observed: Arc<Mutex<Vec<Observed>>>,
}

#[async_trait::async_trait]
impl Tool for Probe {
    fn name(&self) -> &str {
        "work_inspect"
    }
    fn description(&self) -> &str {
        "Observe the executing lifetime"
    }
    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({"type":"object"})
    }
    fn classify(&self, _: &serde_json::Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        // This owned clone outlives the borrowed dispatch context and the wait.
        let retained = ctx.clone();
        self.entered.send(retained.executing_supervisor).unwrap();
        if args["pause"] == true {
            self.resume.acquire().await.unwrap().forget();
        }
        let allowed = if let Some(observation) = retained.executing_supervisor {
            self.service
                .inspect_scope(&observation.supervisor_reference())
                .await
                .is_ok()
        } else {
            false
        };
        self.observed
            .lock()
            .unwrap()
            .push((retained.executing_supervisor, allowed));
        let result = if args["queue"] == true {
            serde_json::to_string(&AgenticToolResult::NeedMoreActions {
                actions: vec![call("queued", false, false)],
                reason: "Continue the original dispatch".into(),
            })
            .unwrap()
        } else {
            serde_json::json!({"allowed":allowed}).to_string()
        };
        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result,
            display_preference: None,
            images: vec![],
        }))
    }
}

fn call(id: &str, pause: bool, queue: bool) -> ToolCall {
    ToolCall {
        id: id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "work_inspect".into(),
            arguments: serde_json::json!({"pause":pause,"queue":queue}).to_string(),
        },
    }
}

async fn native_dispatch(
    storage: Option<Arc<dyn Storage>>,
    tools: Arc<dyn ToolExecutor>,
    session: &mut Session,
    calls: &[ToolCall],
) {
    let _ = native_dispatch_events(storage, tools, session, calls).await;
}

async fn native_dispatch_events(
    storage: Option<Arc<dyn Storage>>,
    tools: Arc<dyn ToolExecutor>,
    session: &mut Session,
    calls: &[ToolCall],
) -> Vec<AgentEvent> {
    let config = AgentLoopConfig {
        storage,
        ..Default::default()
    };
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let llm: Arc<dyn LLMProvider> = Arc::new(NoopProvider);
    let session_id = session.id.clone();
    let frame = crate::runtime::runner::round_frame::RoundFrame {
        vision_support: true,
        session_id: &session_id,
        round_id: "supervisor-dispatch",
        turn: 0,
        debug_enabled: false,
        event_tx: &event_tx,
        metrics_collector: None,
        config: &config,
        llm: &llm,
        tools: &tools,
    };
    let schemas = tools.list_tools();
    let effective = legacy_effective_callable_set(&schemas);
    let mut runtime = AgentRuntimeState::new("supervisor-dispatch");
    let mut task_context = None;
    execute_round_tool_calls(RoundToolExecution {
        tool_calls: calls,
        frame: &frame,
        session,
        runtime_state: &mut runtime,
        policy_guard: &mut super::ToolPolicyGuard::default(),
        task_context: &mut task_context,
        compression_model_name: None,
        compression_model_provider: None,
        tool_schemas: &schemas,
        effective_callable_set: &effective,
    })
    .await
    .expect("native dispatch");
    std::iter::from_fn(|| event_rx.try_recv().ok()).collect()
}

#[tokio::test]
async fn supervisor_native_direct_files_and_processes_never_enter_executor_in_both_modes() {
    use bamboo_domain::{
        RootModeOperationAction, RootModeOperationDecision, RootModeOperationOutcome,
        RootModeOperationRequest,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FileTrap {
        name: &'static str,
        path: std::path::PathBuf,
        entered: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl Tool for FileTrap {
        fn name(&self) -> &str {
            self.name
        }
        fn description(&self) -> &str {
            "Record forbidden executor entry in an isolated temporary directory"
        }
        fn parameters_schema(&self) -> serde_json::Value {
            serde_json::json!({"type":"object"})
        }
        fn classify(&self, _: &serde_json::Value) -> ToolClass {
            match self.name {
                "Read" | "Grep" | "Glob" | "GetFileInfo" | "ViewImage" => {
                    ToolClass::READONLY_PARALLEL
                }
                _ => ToolClass::MUTATING_SERIAL,
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolCtx) -> Result<ToolOutcome, ToolError> {
            self.entered.fetch_add(1, Ordering::SeqCst);
            std::fs::write(&self.path, self.name).unwrap();
            Ok(ToolOutcome::Completed(ToolResult::text(
                true,
                "forbidden entry",
            )))
        }
    }

    let home = tempfile::tempdir().unwrap();
    let store: Arc<dyn Storage> =
        Arc::new(SessionStoreV2::new(home.path().join("data")).await.unwrap());
    SupervisorSessionService::new(store.clone())
        .get_or_create_default("model")
        .await
        .unwrap();
    let mut initial = store
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    initial.agent_runtime_state = Some(AgentRuntimeState::new("supervisor-dispatch"));
    store.save_session(&initial).await.unwrap();
    let names = [
        "Read",
        "Grep",
        "Glob",
        "GetFileInfo",
        "ViewImage",
        "Write",
        "Edit",
        "Bash",
        "exec_command",
        "write_stdin",
        "load_skill",
        "workflow_run",
        "mcp__vendor__Read",
    ];
    let trap_path = home.path().join("executor-was-entered");
    let entered = Arc::new(AtomicUsize::new(0));
    let mut builder = bamboo_tools::BuiltinToolExecutorBuilder::new();
    for name in names {
        builder = builder
            .with_tool(FileTrap {
                name,
                path: trap_path.clone(),
                entered: entered.clone(),
            })
            .unwrap();
    }
    let tools: Arc<dyn ToolExecutor> = Arc::new(builder.build());
    for ultra in [false, true] {
        let before = store
            .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        let decision = store
            .root_mode_operation(&RootModeOperationRequest {
                session_id: before.id.clone(),
                operation_id: format!(
                    "{}:{}",
                    before.root_mode_transition_epoch,
                    uuid::Uuid::new_v4()
                ),
                birth_token: before.root_mode_birth_token(),
                expected_epoch: before.root_mode_transition_epoch,
                requested_enabled: ultra,
                action: RootModeOperationAction::Select,
            })
            .await
            .unwrap();
        assert!(
            matches!(decision, RootModeOperationDecision::Terminal(receipt)
            if receipt.outcome == RootModeOperationOutcome::Committed
                && receipt.enabled_at_completion == ultra)
        );
        let mut session = store
            .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        let calls = names
            .into_iter()
            .chain(["default::Read", "default::exec_command"])
            .enumerate()
            .map(|(n, name)| ToolCall {
                id: format!("direct-{ultra}-{n}"),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: name.into(),
                    arguments: "{}".into(),
                },
            })
            .collect::<Vec<_>>();
        // Deliberately hand the real dispatcher a stale, fully exposed catalog.
        let events =
            native_dispatch_events(Some(store.clone()), tools.clone(), &mut session, &calls).await;
        assert_eq!(entered.load(Ordering::SeqCst), 0);
        assert!(!trap_path.exists());
        assert!(!events
            .iter()
            .any(|event| matches!(event, AgentEvent::ToolStart { .. })));
        for call in calls {
            assert!(
                session.messages.iter().any(|message| {
                    message.tool_call_id.as_deref() == Some(call.id.as_str())
                        && message.tool_success == Some(false)
                        && (call.function.name.starts_with("default::")
                            || message.content.contains("supervisor_delegation_required"))
                }),
                "mode={ultra}, tool={}",
                call.function.name
            );
        }
    }
}

async fn recreate(store: &Arc<dyn Storage>, service: &SupervisorSessionService) -> Session {
    assert!(store
        .delete_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap());
    service.get_or_create_default("replacement").await.unwrap();
    let mut session = store
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    session.agent_runtime_state = Some(AgentRuntimeState::new("replacement-run"));
    store.save_session(&session).await.unwrap();
    session
}

async fn dispatch_race(native: bool, count: usize) {
    let home = tempfile::tempdir().unwrap();
    let store: Arc<dyn Storage> = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let service = SupervisorSessionService::new(store.clone());
    service.get_or_create_default("initial").await.unwrap();
    let mut session = store
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    session.agent_runtime_state = Some(AgentRuntimeState::new("initial-run"));
    store.save_session(&session).await.unwrap();
    let original =
        ExecutingSupervisorObservation::capture_from_executing_session(&session).unwrap();
    let (entered, mut entered_rx) = mpsc::unbounded_channel();
    let resume = Arc::new(Semaphore::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let tools: Arc<dyn ToolExecutor> = Arc::new(
        bamboo_tools::BuiltinToolExecutorBuilder::new()
            .with_tool(Probe {
                service: service.clone(),
                entered,
                resume: resume.clone(),
                observed: observed.clone(),
            })
            .unwrap()
            .build(),
    );
    let calls = (0..count)
        .map(|n| call(&format!("call-{n}"), true, !native))
        .collect::<Vec<_>>();
    let execute = async {
        if native {
            native_dispatch(Some(store.clone()), tools.clone(), &mut session, &calls).await;
        } else {
            let (tx, _rx) = mpsc::channel(128);
            bamboo_agent_core::tools::result_handler::execute_sub_actions(
                &calls,
                &tx,
                &mut session,
                tools.as_ref(),
                ToolExecutionSessionFlags::default(),
                None,
            )
            .await;
        }
    };
    let replace = async {
        for _ in 0..count {
            assert_eq!(entered_rx.recv().await.unwrap(), Some(original));
        }
        // In the two-call case neither call can complete before both entered:
        // this exercises the actual native parallel batch, not serial replay.
        let fresh = recreate(&store, &service).await;
        assert_ne!(
            ExecutingSupervisorObservation::capture_from_executing_session(&fresh),
            Some(original)
        );
        resume.add_permits(count);
        fresh
    };
    let (_, mut fresh) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(execute, replace)
    })
    .await
    .expect("dispatch and replacement complete without deadlock");
    let expected_calls = if native { count } else { count * 2 };
    assert_eq!(
        *observed.lock().unwrap(),
        vec![(Some(original), false); expected_calls],
        "retained and queued contexts must keep the old lifetime and fail strict validation"
    );

    let fresh_observation = ExecutingSupervisorObservation::capture_from_executing_session(&fresh);
    native_dispatch(
        Some(store),
        tools,
        &mut fresh,
        &[call("fresh", false, false)],
    )
    .await;
    assert_eq!(
        observed.lock().unwrap().last(),
        Some(&(fresh_observation, true))
    );
}

#[tokio::test]
async fn supervisor_native_sequential_owned_call_keeps_deleted_lifetime() {
    dispatch_race(true, 1).await;
}

#[tokio::test]
async fn supervisor_native_parallel_owned_calls_keep_one_deleted_lifetime() {
    dispatch_race(true, 2).await;
}

#[tokio::test]
async fn supervisor_core_sub_actions_and_need_more_actions_keep_deleted_lifetime() {
    dispatch_race(false, 1).await;
}

#[tokio::test]
async fn supervisor_native_ordinary_reserved_id_and_child_cannot_acquire_identity() {
    let home = tempfile::tempdir().unwrap();
    let store: Arc<dyn Storage> = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let service = SupervisorSessionService::new(store.clone());
    service.get_or_create_default("initial").await.unwrap();
    let supervisor = store
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    let (entered, _entered_rx) = mpsc::unbounded_channel();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let tools: Arc<dyn ToolExecutor> = Arc::new(
        bamboo_tools::BuiltinToolExecutorBuilder::new()
            .with_tool(Probe {
                service,
                entered,
                resume: Arc::new(Semaphore::new(0)),
                observed: observed.clone(),
            })
            .unwrap()
            .build(),
    );
    let mut child = supervisor.clone();
    child.id = "child".into();
    child.kind = SessionKind::Child;
    child.parent_session_id = Some(supervisor.id.clone());
    child.spawn_depth = 1;
    let mut ordinary = supervisor;
    ordinary.authority_identity = SessionAuthorityIdentity::Ordinary;
    for mut session in [Session::new("ordinary", "model"), ordinary, child] {
        native_dispatch(
            None,
            tools.clone(),
            &mut session,
            &[call("negative", false, false)],
        )
        .await;
    }
    assert_eq!(*observed.lock().unwrap(), vec![(None, false); 3]);
}
