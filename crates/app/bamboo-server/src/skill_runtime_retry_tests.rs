//! Issue #2115: exercise Retry through the real HTTP/Actor boundaries.
//! Synthetic providers and temporary files only; no external model or Session.

use super::*;

struct RetryScopeProvider {
    inner: Arc<ScopeChangingProvider>,
    retrying: AtomicBool,
    retry_workspaces: AtomicU64,
    retry_reads: AtomicU64,
    retry_results: AtomicU64,
}

#[async_trait]
impl LLMProvider for RetryScopeProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        if self.retrying.load(Ordering::Acquire) {
            // Retry is ordinary continuation. Do not manufacture a fresh
            // Native Skill input or reuse the revoked reader from history.
            if let Some(result) = messages.iter().find(|m| {
                m.tool_call_id.as_deref() == Some("retry-current-read")
            }) {
                self.retry_results.fetch_add(1, Ordering::Relaxed);
                assert_eq!(result.tool_success, Some(true), "{}", result.content);
                assert!(result.content.contains("NORMAL_TASK_ACTION"));
                return Ok(Box::pin(futures::stream::iter([
                    Ok(LLMChunk::Token("Current workspace read completed".into())),
                    Ok(LLMChunk::Done),
                ])));
            }
            let (call_id, name, args) = match messages.iter().find(|m| {
                m.tool_call_id.as_deref() == Some("retry-workspace-observation")
            }) {
                Some(result) => {
                    assert_eq!(result.tool_success, Some(true), "{}", result.content);
                    let observed: Value = serde_json::from_str(&result.content).unwrap();
                    let path = Path::new(observed["workspace"].as_str().unwrap());
                    assert_eq!(path.canonicalize().unwrap(), self.inner.workspace.canonicalize().unwrap());
                    self.retry_reads.fetch_add(1, Ordering::Relaxed);
                    ("retry-current-read", "Read", json!({"file_path":path.join("task.txt")}))
                }
                None => {
                    self.retry_workspaces.fetch_add(1, Ordering::Relaxed);
                    ("retry-workspace-observation", "Workspace", json!({}))
                }
            };
            return Ok(Box::pin(futures::stream::iter([
                Ok(LLMChunk::ToolCalls(vec![ToolCall {
                    id: call_id.into(),
                    tool_type: "function".into(),
                    function: FunctionCall { name: name.into(), arguments: args.to_string() },
                }])),
                Ok(LLMChunk::Done),
            ])));
        }
        self.inner.chat_stream(messages, tools, max, model).await
    }
}

#[actix_web::test]
async fn error_retry_after_scope_revocation_starts_from_current_workspace() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    write_skill(home.path(), "native-proof", true);
    std::fs::write(home.path().join("task.txt"), "STALE_WORKSPACE_MUST_NOT_BE_READ").unwrap();
    let current = home.path().join("current-workspace");
    std::fs::create_dir(&current).unwrap();
    let id = "retry-current-source";
    let provider = Arc::new(RetryScopeProvider {
        inner: Arc::new(ScopeChangingProvider {
            inner: provider(&current, 4096, false),
            state: Mutex::default(),
            session_id: id.into(),
            workspace: current.clone(),
            change: 0,
            changed: AtomicBool::new(false),
        }),
        retrying: AtomicBool::new(false),
        retry_workspaces: AtomicU64::new(0),
        retry_reads: AtomicU64::new(0),
        retry_results: AtomicU64::new(0),
    });
    let state = state(home.path(), provider.clone()).await;
    *provider.inner.state.lock().unwrap() = Some(state.clone());
    seed(&state, id).await;
    let (status, receipt) = http(
        &state,
        "/api/v1/chat",
        json!({"session_id":id,"message":"Inspect the temporary proof", "workspace_path":home.path()}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{receipt}");
    let input = receipt["message_id"].as_str().unwrap().to_owned();
    let (status, response) = http(&state, &format!("/api/v1/execute/{id}"), json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{response}");
    let terminal = done(&state, id).await;
    provider.inner.state.lock().unwrap().take();
    assert!(provider.inner.changed.load(Ordering::Acquire));
    {
        let trace = provider.inner.inner.trace.lock().unwrap();
        let scope_error = "Native canonical Session Source scope changed";
        assert!(
            matches!(&terminal, AgentStatus::Error(reason) if reason.contains(scope_error))
                || trace.failures.iter().any(|reason| reason.contains(scope_error)),
            "The old run must still be revoked: {terminal:?}; {:?}",
            trace.failures
        );
        assert!(!trace.task_started && !trace.task_completed);
    }
    provider.retrying.store(true, Ordering::Release);
    let before = state.storage.load_session(id).await.unwrap().unwrap();
    let before_user_ids: Vec<_> = before.messages.iter().filter(|m| m.role == Role::User)
        .map(|m| m.id.clone()).collect();
    let (status, response) = http(
        &state,
        &format!("/api/v1/sessions/{id}/messages/truncate"),
        json!({"mode":"error_retry"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Retry preparation: {response}");
    let (status, response) = http(&state, &format!("/api/v1/execute/{id}"), json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "Retry execute: {response}");
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    let stored = state.storage.load_session(id).await.unwrap().unwrap();
    assert_eq!(workspace_identity(&stored).unwrap(), Some(current.canonicalize().unwrap()));
    assert_eq!(stored.messages.iter().filter(|m| m.role == Role::User)
        .map(|m| m.id.clone()).collect::<Vec<_>>(), before_user_ids);
    assert_eq!(stored.messages.iter().filter(|m| m.id == input).count(), 1);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 0);
    assert_eq!(provider.retry_workspaces.load(Ordering::Acquire), 1);
    assert_eq!(provider.retry_reads.load(Ordering::Acquire), 1);
    assert_eq!(provider.retry_results.load(Ordering::Acquire), 1);
    assert_eq!(stored.messages.iter()
        .filter(|m| m.role == Role::Tool && m.tool_call_id.as_deref() == Some("retry-current-read"))
        .count(), 1);
}

fn interrupted_session(id: &str) -> Session {
    let mut session = Session::new(id, "test-model");
    session.title_generated = true;
    session.add_message(Message::user("Keep the completed action; recover the missing result"));
    let call = |id: &str| ToolCall {
        id: id.into(), tool_type: "function".into(),
        function: FunctionCall { name: "Read".into(), arguments: "{}".into() },
    };
    session.add_message(Message::assistant("", Some(vec![call("completed-action")])));
    session.add_message(Message::tool_result("completed-action", "PRESERVED_COMPLETED_RESULT"));
    session.add_message(Message::assistant("", Some(vec![call("missing-result")])));
    session.conversation_summary = Some(bamboo_domain::ConversationSummary::new(
        "Obsolete derived context", session.messages.len(), 4,
    ));
    session
}

async fn own_interrupted_root(
    state: &web::Data<AppState>, session: &Session,
) -> bamboo_engine::execution::SessionExecutionReservation {
    state.storage.save_session(session).await.unwrap();
    let sender = state.get_session_event_sender(&session.id).await;
    let bamboo_engine::execution::SessionExecutionReserveOutcome::Reserved(mut reservation) =
        bamboo_engine::execution::reserve_session_execution(
            &state.agent, &state.agent_runners, &state.session_event_senders,
            &session.id, &sender,
        ).await else { panic!("fresh synthetic Root reservation") };
    reservation.bind_root_actor(&state.agent, session).await.unwrap();
    assert!(reservation.root_actor_writer().is_some());
    reservation
}

#[actix_web::test]
async fn error_retry_of_activated_root_preserves_completed_results_and_user_identity() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = provider(home.path(), 4096, false);
    let state = state(home.path(), provider.clone()).await;
    let original = interrupted_session("retry-activated-root");
    let mut reservation = own_interrupted_root(&state, &original).await;
    reservation.finish_root_actor(bamboo_domain::ActorActivationFinish::Failed).await;
    reservation.abandon().await;
    let (status, response) = http(
        &state, &format!("/api/v1/sessions/{}/messages/truncate", original.id),
        json!({"mode":"error_retry"}),
    ).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let stored = state.storage.load_session(&original.id).await.unwrap().unwrap();
    assert_eq!(stored.created_at, original.created_at);
    assert_eq!(stored.messages.iter().filter(|m| m.role == Role::User).count(), 1);
    let result = stored.messages.iter().find(|m| m.tool_call_id.as_deref() == Some("completed-action"));
    assert_eq!(result.unwrap().content, "PRESERVED_COMPLETED_RESULT");
    assert!(bamboo_engine::session_app::truncation::unresolved_tool_call_ids(&stored.messages).is_empty());
    assert!(stored.conversation_summary.is_none());
    assert!(bamboo_engine::session_app::execute::has_pending_retry_resume(&stored));
    assert!(provider.trace.lock().unwrap().requests.is_empty());
}

#[actix_web::test]
async fn error_retry_rejects_a_live_owner_without_history_or_provider_effects() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = provider(home.path(), 4096, false);
    let state = state(home.path(), provider.clone()).await;
    let original = interrupted_session("retry-live-root");
    let reservation = own_interrupted_root(&state, &original).await;
    let before = state.storage.load_session(&original.id).await.unwrap().unwrap();
    let (status, response) = http(
        &state, &format!("/api/v1/sessions/{}/messages/truncate", original.id),
        json!({"mode":"error_retry"}),
    ).await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    let after = state.storage.load_session(&original.id).await.unwrap().unwrap();
    assert_eq!(serde_json::to_value(after).unwrap(), serde_json::to_value(before).unwrap());
    assert!(provider.trace.lock().unwrap().requests.is_empty());
    reservation.abandon().await;
}
