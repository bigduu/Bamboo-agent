// Included in pipeline::tests: controlled provider, real runner, synthetic tools.
// Native Host/worker acceptance is recorded separately, never inferred here.
struct RepeatedChildInspector {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl LLMProvider for RepeatedChildInspector {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[bamboo_agent_core::tools::ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let round = self.calls.fetch_add(1, Ordering::SeqCst);
        // A negative control finishes without relying on max_rounds or timeout.
        if round == 12 {
            return Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Done)])));
        }
        assert!(round < 12);
        Ok(Box::pin(stream::iter(vec![
            Ok(LLMChunk::ToolCalls(vec![activation_call(
                &format!("inspect-{round}"),
                "SubAgent",
                r#"{"intent":"inspect","target":"inspected-child"}"#,
            )])),
            Ok(LLMChunk::Done),
        ])))
    }
}

struct ChildInspectionExecutor {
    calls: AtomicUsize,
    change_at: Option<usize>,
}

#[async_trait::async_trait]
impl bamboo_agent_core::tools::ToolExecutor for ChildInspectionExecutor {
    async fn execute(
        &self,
        _: &ToolCall,
    ) -> Result<ToolResult, bamboo_agent_core::tools::ToolError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ToolResult::text(
            true,
            serde_json::json!({
                "actor_id":"inspected-child", "observed_status":"running", "is_running":true,
                "round_count":usize::from(self.change_at.is_some_and(|at| call >= at)),
            })
            .to_string(),
        ))
    }
    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        vec![loading_test_schema("SubAgent")]
    }
}

#[test]
fn supervisor_child_inspection_wait_bounds_provider_calls_and_resets_on_changed_evidence() {
    // The debug pipeline fixture exceeds libtest's small default stack. Only
    // this harness thread grows; native serve/worker acceptance keeps defaults.
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(supervisor_child_inspection_wait_fixture());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn supervisor_child_inspection_wait_fixture() {
    for ultra in [false, true] {
        for change_at in [None, Some(2)] {
            let inner = Arc::new(TestStorage::default());
            let storage: Arc<dyn Storage> = Arc::new(ChildIndexStorage {
                inner,
                children: vec![
                    ("inspected-child".into(), Some("running".into())),
                    ("unrelated-work".into(), Some("running".into())),
                ],
            });
            let mut session = Session::new(bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID, "model");
            session.authority_identity = bamboo_domain::SessionAuthorityIdentity::Supervisor {
                incarnation_id: uuid::Uuid::new_v4(),
            };
            session.set_root_orchestration_only(ultra).unwrap();
            session.agent_runtime_state = Some(AgentRuntimeState::new("fixture-permission-owner"));
            storage.save_session(&session).await.unwrap();
            let mut config = config_with_storage(storage.clone());
            config.model_name = Some("model".into());
            config.system_prompt = Some("Coordinate this bounded child.".into());
            config.prompt_memory_flags = crate::runtime::config::PromptMemoryFlags {
                project_prompt_injection: false,
                relevant_recall: false,
                relevant_recall_rerank: false,
                project_first_dream: false,
                ledger_agenda: false,
            };
            assert!(config.run_budget.max_rounds.is_none());
            let mut state = e2e_loop_state(&session.id);
            let provider = Arc::new(RepeatedChildInspector {
                calls: AtomicUsize::new(0),
            });
            let tools = Arc::new(ChildInspectionExecutor {
                calls: AtomicUsize::new(0),
                change_at,
            });
            let cancel = CancellationToken::new();
            let (tx, mut rx) = mpsc::channel(256);
            let sent_complete = super::run_pipeline(
                &mut session,
                &tx,
                provider.clone(),
                tools.clone(),
                &cancel,
                &config,
                &mut state,
            )
            .await
            .unwrap();
            let bound = change_at.unwrap_or(0) + 3;
            assert_eq!(provider.calls.load(Ordering::SeqCst), bound);
            assert_eq!(tools.calls.load(Ordering::SeqCst), bound);
            assert!(!sent_complete);
            assert!(
                !cancel.is_cancelled(),
                "a quiet long-running child must not be cancelled"
            );
            let durable = storage.load_session(&session.id).await.unwrap().unwrap();
            let wait = durable
                .agent_runtime_state
                .unwrap()
                .waiting_for_children
                .unwrap();
            assert_eq!(wait.child_session_ids, ["inspected-child"]);
            assert_eq!(wait.wait_for, ChildWaitPolicy::All);
            assert_eq!(
                session.metadata["runtime.suspend_reason"],
                "waiting_for_children"
            );
            assert_eq!(
                session
                    .messages
                    .iter()
                    .filter(|m| m.role == bamboo_agent_core::Role::Tool)
                    .count(),
                bound
            );
            while let Ok(event) = rx.try_recv() {
                assert!(!matches!(event, AgentEvent::Complete { .. }));
            }
        }
    }
}

#[tokio::test]
async fn child_inspection_wait_preserves_unrelated_planner_policy_and_original_lease() {
    for policy in [
        ChildWaitPolicy::All,
        ChildWaitPolicy::Any,
        ChildWaitPolicy::FirstError,
    ] {
        let inner = Arc::new(TestStorage::default());
        let storage: Arc<dyn Storage> = Arc::new(ChildIndexStorage {
            inner,
            children: vec![("inspected-child".into(), Some("running".into()))],
        });
        let mut session = Session::new("planner-parent", "model");
        let mut runtime = AgentRuntimeState::new("planner-run");
        let mut wait = WaitingForChildrenState::for_children(
            vec!["planner".into()],
            policy,
            Utc::now() - chrono::Duration::minutes(10),
        );
        wait.registered_by_tool_call_id = Some("original-planner-call".into());
        runtime.waiting_for_children = Some(wait.clone());
        session.agent_runtime_state = Some(runtime.clone());
        storage.save_session(&session).await.unwrap();
        assert!(super::maybe_suspend_for_children(
            &mut session,
            &config_with_storage(storage.clone()),
            &mut runtime,
            Some(&["inspected-child".into()]),
        )
        .await
        .unwrap()
        .is_some());
        assert_eq!(runtime.waiting_for_children, Some(wait.clone()));
        assert_eq!(
            storage
                .load_session(&session.id)
                .await
                .unwrap()
                .unwrap()
                .agent_runtime_state
                .unwrap()
                .waiting_for_children,
            Some(wait)
        );
    }
}

#[tokio::test]
async fn child_inspection_wait_never_registers_terminal_or_foreign_targets() {
    let inner = Arc::new(TestStorage::default());
    let storage: Arc<dyn Storage> = Arc::new(ChildIndexStorage {
        inner,
        children: vec![
            ("finished".into(), Some("completed".into())),
            ("unrelated-work".into(), Some("running".into())),
        ],
    });
    let mut session = Session::new("inspection-parent", "model");
    let mut runtime = AgentRuntimeState::new("inspection-run");
    assert!(super::maybe_suspend_for_children(
        &mut session,
        &config_with_storage(storage),
        &mut runtime,
        Some(&["finished".into(), "foreign-root".into()]),
    )
    .await
    .unwrap()
    .is_none());
    assert!(runtime.waiting_for_children.is_none());
}

struct RejectInspectionWaitSave;

#[async_trait::async_trait]
impl bamboo_domain::RuntimeSessionPersistence for RejectInspectionWaitSave {
    async fn save_runtime_session(&self, _: &mut Session) -> std::io::Result<()> {
        Err(std::io::Error::other("controlled checkpoint failure"))
    }
}

#[tokio::test]
async fn child_inspection_wait_failed_checkpoint_is_a_real_blocker_without_phantom_suspension() {
    let inner = Arc::new(TestStorage::default());
    let storage: Arc<dyn Storage> = Arc::new(ChildIndexStorage {
        inner,
        children: vec![("inspected-child".into(), Some("running".into()))],
    });
    let mut session = Session::new("wait-checkpoint-parent", "model");
    let mut runtime = AgentRuntimeState::new("wait-checkpoint-run");
    session.agent_runtime_state = Some(runtime.clone());
    storage.save_session(&session).await.unwrap();
    let before = serde_json::to_value(&session).unwrap();
    let mut config = config_with_storage(storage.clone());
    config.persistence = Some(Arc::new(RejectInspectionWaitSave));
    let error = super::maybe_suspend_for_children(
        &mut session,
        &config,
        &mut runtime,
        Some(&["inspected-child".into()]),
    )
    .await
    .err()
    .expect("failed persistence must not claim a durable wait");
    assert!(error.to_string().contains("wait could not be persisted"));
    assert!(runtime.waiting_for_children.is_none());
    assert_eq!(serde_json::to_value(&session).unwrap(), before);
    assert_eq!(
        serde_json::to_value(storage.load_session(&session.id).await.unwrap().unwrap()).unwrap(),
        before
    );
}
