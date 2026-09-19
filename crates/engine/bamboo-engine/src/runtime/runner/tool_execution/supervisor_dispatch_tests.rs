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
        "Read"
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
            name: "Read".into(),
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
    let config = AgentLoopConfig {
        storage,
        ..Default::default()
    };
    let (event_tx, _event_rx) = mpsc::channel(128);
    let llm: Arc<dyn LLMProvider> = Arc::new(NoopProvider);
    let session_id = session.id.clone();
    let frame = crate::runtime::runner::round_frame::RoundFrame {
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
        task_context: &mut task_context,
        compression_model_name: None,
        compression_model_provider: None,
        tool_schemas: &schemas,
        effective_callable_set: &effective,
    })
    .await
    .expect("native dispatch");
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
