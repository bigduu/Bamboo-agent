// Included inside pipeline::tests to reuse the real runner and storage helpers.
// These cases make no network requests and do not change any authority grant.

struct OrphanChildIndexStorage {
    inner: Arc<TestStorage>,
    index_error: Option<std::io::ErrorKind>,
    index_reads: AtomicUsize,
    saves: AtomicUsize,
}

impl OrphanChildIndexStorage {
    fn new(inner: Arc<TestStorage>, index_error: Option<std::io::ErrorKind>) -> Self {
        Self {
            inner,
            index_error,
            index_reads: AtomicUsize::new(0),
            saves: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl Storage for OrphanChildIndexStorage {
    async fn save_session(&self, session: &Session) -> std::io::Result<()> {
        self.saves.fetch_add(1, Ordering::SeqCst);
        self.inner.save_session(session).await
    }

    async fn load_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_session(id).await
    }

    async fn delete_session(&self, id: &str) -> std::io::Result<bool> {
        self.inner.delete_session(id).await
    }

    async fn list_child_run_statuses(
        &self,
        _parent: &str,
    ) -> std::io::Result<Vec<(String, Option<String>)>> {
        self.index_reads.fetch_add(1, Ordering::SeqCst);
        match self.index_error {
            Some(kind) => Err(std::io::Error::new(
                kind,
                "private child-index fixture detail",
            )),
            None => Ok(Vec::new()),
        }
    }
}

#[tokio::test]
async fn orphan_child_index_failure_preserves_history_and_existing_wait_exactly() {
    for kind in [
        std::io::ErrorKind::PermissionDenied,
        std::io::ErrorKind::InvalidData,
        std::io::ErrorKind::Other,
    ] {
        for policy in [
            None,
            Some(ChildWaitPolicy::All),
            Some(ChildWaitPolicy::Any),
            Some(ChildWaitPolicy::FirstError),
        ] {
            let inner = Arc::new(TestStorage::default());
            let storage = Arc::new(OrphanChildIndexStorage::new(inner.clone(), Some(kind)));
            let config = config_with_storage(storage.clone());
            let mut session = Session::new("unobservable-children-parent", "model");
            session.add_message(Message::user("already admitted task"));
            let mut runtime = AgentRuntimeState::new("original-parent-run");
            if let Some(policy) = policy {
                let registered_at = Utc::now() - chrono::Duration::minutes(5);
                runtime.status = AgentStatusState::Suspended;
                runtime.waiting_for_children = Some(WaitingForChildrenState {
                    child_session_ids: vec!["still-owned-child".into()],
                    wait_for: policy,
                    registered_at,
                    timeout_at: Some(registered_at + chrono::Duration::minutes(10)),
                    registered_by_tool_call_id: Some("original-wait-call".into()),
                });
                session.metadata.insert(
                    "runtime.suspend_reason".into(),
                    "waiting_for_children".into(),
                );
            }
            session.agent_runtime_state = Some(runtime.clone());
            inner.save_session(&session).await.unwrap();
            let before_session = serde_json::to_value(&session).unwrap();
            let before_runtime = runtime.clone();

            let error = maybe_suspend_for_orphaned_children(&mut session, &config, &mut runtime)
                .await
                .err()
                .expect("an unreadable child index must not mean no active children");

            assert!(matches!(&error, AgentError::Tool(_)));
            assert!(error
                .to_string()
                .contains("child status observation failed"));
            assert!(!error
                .to_string()
                .contains("private child-index fixture detail"));
            assert_eq!(runtime, before_runtime);
            assert_eq!(serde_json::to_value(&session).unwrap(), before_session);
            assert_eq!(storage.index_reads.load(Ordering::SeqCst), 1);
            assert_eq!(storage.saves.load(Ordering::SeqCst), 0);
            assert_eq!(
                serde_json::to_value(inner.load_session(&session.id).await.unwrap().unwrap())
                    .unwrap(),
                before_session,
            );
        }
    }
}

fn orphan_child_index_runner_config(storage: Arc<dyn Storage>) -> AgentLoopConfig {
    let mut config = config_with_storage(storage);
    config.model_name = Some("model".into());
    config.run_budget.max_rounds = Some(2);
    config.prompt_memory_flags = crate::runtime::config::PromptMemoryFlags {
        project_prompt_injection: false,
        relevant_recall: false,
        relevant_recall_rerank: false,
        project_first_dream: false,
        ledger_agenda: false,
    };
    config
}

#[tokio::test]
async fn orphan_child_index_failure_stops_real_pipeline_without_retry_or_complete() {
    let storage = Arc::new(OrphanChildIndexStorage::new(
        Arc::new(TestStorage::default()),
        Some(std::io::ErrorKind::PermissionDenied),
    ));
    let config = orphan_child_index_runner_config(storage.clone());
    let mut session = Session::new("child-index-error-runner", "model");
    session.add_message(Message::user("finish the existing work"));
    storage.inner.save_session(&session).await.unwrap();
    let mut state = e2e_loop_state(&session.id);
    let provider = Arc::new(ContextProbeProvider::default());
    let (tx, mut rx) = tokio::sync::mpsc::channel(128);

    let error = super::run_pipeline(
        &mut session,
        &tx,
        provider.clone(),
        Arc::new(AlwaysOkExecutor),
        &CancellationToken::new(),
        &config,
        &mut state,
    )
    .await
    .expect_err("the real terminal gate must reject unknown child state");

    assert!(matches!(&error, AgentError::Tool(_)));
    assert!(error
        .to_string()
        .contains("child status observation failed"));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(storage.index_reads.load(Ordering::SeqCst) > 0);
    assert!(state.runtime_state.waiting_for_children.is_none());
    assert_ne!(state.runtime_state.status, AgentStatusState::Completed);
    while let Ok(event) = rx.try_recv() {
        assert!(!matches!(event, AgentEvent::Complete { .. }));
    }
}

#[tokio::test]
async fn orphan_child_index_observed_empty_still_completes_real_pipeline() {
    let storage = Arc::new(OrphanChildIndexStorage::new(
        Arc::new(TestStorage::default()),
        None,
    ));
    let config = orphan_child_index_runner_config(storage.clone());
    let mut session = Session::new("child-index-empty-runner", "model");
    session.add_message(Message::user("finish the existing work"));
    storage.inner.save_session(&session).await.unwrap();
    let mut state = e2e_loop_state(&session.id);
    let provider = Arc::new(ContextProbeProvider::default());
    let (tx, mut rx) = tokio::sync::mpsc::channel(128);

    let sent_complete = super::run_pipeline(
        &mut session,
        &tx,
        provider.clone(),
        Arc::new(AlwaysOkExecutor),
        &CancellationToken::new(),
        &config,
        &mut state,
    )
    .await
    .expect("an observed empty index allows the ordinary completion path");

    assert!(sent_complete);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(storage.index_reads.load(Ordering::SeqCst) > 0);
    assert!(state.runtime_state.waiting_for_children.is_none());
    let mut saw_complete = false;
    while let Ok(event) = rx.try_recv() {
        saw_complete |= matches!(event, AgentEvent::Complete { .. });
    }
    assert!(saw_complete);
}
