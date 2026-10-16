//! V2 TMP-only proposal: strict canonical receipt bytes stay immutable; native
//! feedback uses existing AgentRuntimeState.hook_contexts and ordinary checkpoint.
//! Real BuiltinToolExecutor, Router proof and LockedSessionStore come from Fixture.

use super::*;
use bamboo_agent_core::tools::ToolResult;
use bamboo_compression::{TiktokenTokenCounter, TokenCounter};

const POST_CAP: u32 = 512;
const RETRY_CALL: &str = "start-retry";

fn post_pair<'a>(messages: &'a [Message], id: &str) -> &'a Message {
    let mut pairs = messages.iter().filter(|message| {
        message.role == Role::Tool && message.tool_call_id.as_deref() == Some(id)
    });
    let pair = pairs.next().expect("one paired owner result");
    assert!(pairs.next().is_none(), "duplicate pair for {id}");
    pair
}

fn post_result<'a>(events: &'a [AgentEvent], id: &str) -> &'a ToolResult {
    let mut results = events.iter().filter_map(|event| match event {
        AgentEvent::ToolComplete {
            tool_call_id,
            result,
        } if tool_call_id == id => Some(result),
        _ => None,
    });
    let result = results.next().expect("one owner ToolComplete");
    assert!(results.next().is_none(), "duplicate completion for {id}");
    result
}

fn post_receipt(text: &str) -> LoopControlReceipt {
    assert!(TiktokenTokenCounter::default().count_text(text) <= POST_CAP);
    let body: Value = serde_json::from_str(text).expect("strict canonical receipt JSON");
    assert!(body.get("hook_feedback").is_none());
    assert!(body.get("hook_feedback_omitted_bytes").is_none());
    serde_json::from_str(text).expect("the original public receipt DTO stays unchanged")
}

fn assert_canonical_receipt(receipt: &LoopControlReceipt) {
    let encoded = serde_json::to_string(receipt).unwrap();
    let decoded = post_receipt(&encoded);
    assert_eq!(&decoded, receipt);
    assert!(uuid::Uuid::parse_str(&receipt.operation_id).is_ok());
    assert!(uuid::Uuid::parse_str(&receipt.generation).is_ok());
}

fn post_contexts(session: &Session) -> &[String] {
    session
        .agent_runtime_state
        .as_ref()
        .map(|runtime| runtime.hook_contexts.as_slice())
        .unwrap_or_default()
}

fn find_post_context<'a>(session: &'a Session, id: &str) -> Option<&'a str> {
    let mut matches = post_contexts(session).iter().filter_map(|text| {
        let body: Value = serde_json::from_str(text).ok()?;
        (body["tool_call_id"].as_str() == Some(id)).then_some(text.as_str())
    });
    let context = matches.next();
    assert!(
        matches.next().is_none(),
        "duplicate feedback context for {id}"
    );
    context
}

fn post_feedback_text(id: &str) -> String {
    format!("{id}: native feedback with \"quotes\" and newline\n界").repeat(4_000)
}

fn assert_feedback_context(base: &str, context: &str, id: &str) {
    let body: Value = serde_json::from_str(context).expect("feedback context must be JSON");
    assert_eq!(body.as_object().unwrap().len(), 3);
    assert_eq!(body["tool_call_id"], id);
    let retained = body["hook_feedback"].as_str().unwrap();
    let omitted = body["hook_feedback_omitted_bytes"].as_u64().unwrap() as usize;
    let original = post_feedback_text(id);
    assert!(omitted > 0, "large native feedback must be bounded");
    assert!(original.starts_with(retained));
    assert_eq!(retained.len() + omitted, original.len());
    let counter = TiktokenTokenCounter::default();
    let total = counter
        .count_text(base)
        .saturating_add(counter.count_text(context))
        .saturating_add(counter.count_text("\n\n---\n\n"));
    assert!(
        total <= POST_CAP,
        "receipt+complete context+separator used {total} tokens"
    );
}

#[derive(Clone)]
struct PostCheckpointFault {
    commit_before_error: bool,
    cancel_at_failure: Option<CancellationToken>,
}

struct PostHookPersistence {
    inner: Arc<dyn RuntimeSessionPersistence>,
    fault: Option<PostCheckpointFault>,
    failed: AtomicBool,
    feedback_attempt: Mutex<Option<Session>>,
    committed: Mutex<Vec<Session>>,
}

#[async_trait]
impl RuntimeSessionPersistence for PostHookPersistence {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.save_runtime_session(session).await
    }

    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }

    async fn load_runtime_admission_snapshot(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_admission_snapshot(id).await
    }

    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        let running = read_loop_state(session).unwrap().is_some_and(|state| {
            state.status == LoopStatus::Running
                && state
                    .control
                    .as_ref()
                    .is_some_and(|control| control.operations.len() == 1)
        });
        let has_feedback = find_post_context(session, "start").is_some();
        if let Some(fault) = self.fault.as_ref() {
            if running && has_feedback && !self.failed.swap(true, Ordering::SeqCst) {
                let pair = post_pair(&session.messages, "start");
                assert_feedback_context(
                    &pair.content,
                    find_post_context(session, "start").unwrap(),
                    "start",
                );
                *self.feedback_attempt.lock().unwrap() = Some(session.clone());
                if fault.commit_before_error {
                    self.inner.checkpoint_runtime_session(session).await?;
                    self.committed.lock().unwrap().push(session.clone());
                }
                if let Some(cancel) = fault.cancel_at_failure.as_ref() {
                    cancel.cancel();
                }
                return Err(std::io::Error::other(
                    "synthetic owner feedback context checkpoint failure",
                ));
            }
        }
        self.inner.checkpoint_runtime_session(session).await?;
        self.committed.lock().unwrap().push(session.clone());
        Ok(())
    }
}

struct PostHookWitness {
    call_id: String,
    before_feedback: Message,
    canonical: LoopControlReceipt,
}

struct PostMutationHook {
    feedback: bool,
    cancel_at_hook: Option<CancellationToken>,
    persistence: Arc<PostHookPersistence>,
    witnessed: Mutex<Vec<PostHookWitness>>,
}

#[async_trait]
impl bamboo_agent_core::AgentHook for PostMutationHook {
    fn may_suspend(&self) -> bool {
        false
    }

    fn point(&self) -> bamboo_domain::AgentHookPoint {
        bamboo_domain::AgentHookPoint::AfterToolExecution
    }

    fn matches(&self, payload: &bamboo_domain::HookPayload) -> bool {
        matches!(payload, bamboo_domain::HookPayload::ToolResult { tool_call_id, .. }
            if tool_call_id == "start" || tool_call_id == RETRY_CALL)
    }

    async fn run(
        &self,
        _: bamboo_domain::AgentHookPoint,
        payload: &bamboo_domain::HookPayload,
        session: &Session,
    ) -> bamboo_domain::HookResult {
        let bamboo_domain::HookPayload::ToolResult {
            tool_call_id,
            outcome,
            ..
        } = payload
        else {
            panic!("native post-tool hook needs the actual owner result");
        };
        assert!(outcome.success);
        let received = post_receipt(outcome.result.as_ref().unwrap());
        assert_canonical_receipt(&received);
        let state = read_loop_state(session).unwrap().unwrap();
        let canonical = &state.control.as_ref().unwrap().operations[0].receipt;
        assert_canonical_receipt(canonical);
        assert_eq!(&received, canonical);
        let pair = post_pair(&session.messages, tool_call_id);
        assert_eq!(pair.content, serde_json::to_string(canonical).unwrap());
        assert!(
            find_post_context(session, tool_call_id).is_none(),
            "this call's feedback cannot precede its own native hook"
        );
        let committed = self.persistence.committed.lock().unwrap();
        assert_eq!(
            post_pair(&committed.last().unwrap().messages, tool_call_id).content,
            pair.content,
            "acceptance and paired receipt must be durable before invoking the hook"
        );
        self.witnessed.lock().unwrap().push(PostHookWitness {
            call_id: tool_call_id.clone(),
            before_feedback: pair.clone(),
            canonical: canonical.clone(),
        });
        if let Some(cancel) = self.cancel_at_hook.as_ref() {
            cancel.cancel();
        }
        if self.feedback {
            bamboo_domain::HookResult::InjectContext {
                text: post_feedback_text(tool_call_id),
            }
        } else {
            bamboo_domain::HookResult::Continue
        }
    }
}

async fn post_fixture(
    feedback: bool,
    cancel_at_hook: Option<CancellationToken>,
    fault: Option<PostCheckpointFault>,
) -> (Fixture, Arc<PostHookPersistence>, Arc<PostMutationHook>) {
    let mut fixture = Fixture::new(2, 8).await;
    fixture.session.token_budget = Some(bamboo_domain::TokenBudget {
        max_tool_output_tokens: POST_CAP,
        ..Default::default()
    });
    fixture
        .storage
        .save_session(&fixture.session)
        .await
        .unwrap();
    let persistence = Arc::new(PostHookPersistence {
        inner: fixture.config.as_mut().unwrap().persistence.take().unwrap(),
        fault,
        failed: AtomicBool::new(false),
        feedback_attempt: Mutex::new(None),
        committed: Mutex::new(Vec::new()),
    });
    let hook = Arc::new(PostMutationHook {
        feedback,
        cancel_at_hook,
        persistence: persistence.clone(),
        witnessed: Mutex::new(Vec::new()),
    });
    let mut hooks = crate::runtime::HookRunner::new();
    hooks.register(hook.clone());
    fixture.config.as_mut().unwrap().hook_runner = Arc::new(hooks);
    fixture.config.as_mut().unwrap().persistence = Some(persistence.clone());
    (fixture, persistence, hook)
}

#[derive(Clone)]
struct RetryObservation {
    messages: Vec<Message>,
    durable: Session,
}

struct ReceiptRetryProvider {
    storage: Arc<dyn Storage>,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    seen: Mutex<Vec<RetryObservation>>,
}

#[async_trait]
impl LLMProvider for ReceiptRetryProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        use futures::StreamExt;
        let round = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        assert_eq!(self.active.fetch_add(1, Ordering::SeqCst), 0);
        let guard = ActiveStream(self.active.clone());
        assert!(tools
            .iter()
            .any(|tool| tool.function.name == "loop_control"));
        let durable = self
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        self.seen.lock().unwrap().push(RetryObservation {
            messages: messages.to_vec(),
            durable,
        });
        let output = match round {
            1 => start_call(2, 8),
            2 => control_call(
                RETRY_CALL,
                json!({
                    "action":"start",
                    "operation_id": START_OPERATION,
                    "expected_revision": 0,
                    "objective": FIRST_OBJECTIVE,
                    "max_iterations": 2,
                    "max_total_rounds": 8,
                }),
            ),
            3 => LLMChunk::Token("First bounded objective is complete.".into()),
            4 => LLMChunk::Token("Second bounded iteration is complete.".into()),
            _ => panic!("receipt retry must not create another iteration or summary"),
        };
        let stream = futures::stream::iter([Ok(output), Ok(LLMChunk::Done)]).map(move |chunk| {
            let _keep_stream_alive = &guard;
            chunk
        });
        Ok(Box::pin(stream))
    }
}

#[tokio::test]
async fn mutation_feedback_context_and_retry_preserve_exact_canonical_receipt_bytes() {
    let (mut fixture, persistence, hook) = post_fixture(true, None, None).await;
    let provider = Arc::new(ReceiptRetryProvider {
        storage: fixture.storage.clone(),
        calls: AtomicUsize::new(0),
        active: Arc::new(AtomicUsize::new(0)),
        seen: Mutex::new(Vec::new()),
    });
    let (result, events) = fixture
        .run(provider.clone(), CancellationToken::new())
        .await;
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 4);
    assert_eq!(provider.active.load(Ordering::SeqCst), 0);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::Complete { .. }))
            .count(),
        1
    );
    let durable = fixture
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    let state = read_loop_state(&durable).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::Completed);
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        (2, 2, 4)
    );
    let control = state.control.as_ref().unwrap();
    assert_eq!(control.control_revision, 1);
    assert_eq!(control.applied_revision, 1);
    assert_eq!(control.operations.len(), 1);
    let canonical = &control.operations[0].receipt;
    assert_canonical_receipt(canonical);
    assert_eq!(canonical.operation_id, START_OPERATION);
    assert_eq!(canonical.generation, state.generation);
    assert_eq!(canonical.accepted_revision, 1);
    assert_eq!(canonical.applied_revision_at_acceptance, 0);
    assert_eq!(canonical.objective_revision, 1);
    assert!(!canonical.applied);
    assert_eq!(canonical.effective_iteration, Some(1));
    let canonical_bytes = serde_json::to_string(canonical).unwrap();
    assert_eq!(post_contexts(&fixture.session), post_contexts(&durable));
    assert_eq!(post_contexts(&durable).len(), 2);

    for id in ["start", RETRY_CALL] {
        let result = post_result(&events, id);
        assert!(result.success);
        assert_eq!(post_receipt(&result.result), *canonical);
        assert_eq!(
            result.result, canonical_bytes,
            "delivery feedback must never rewrite a receipt"
        );
        for session in [&fixture.session, &durable] {
            let pair = post_pair(&session.messages, id);
            assert_eq!(pair.tool_success, Some(true));
            assert_eq!(pair.content, canonical_bytes);
            let context = find_post_context(session, id).expect("one separate per-call context");
            assert_feedback_context(&pair.content, context, id);
        }
    }
    assert_eq!(
        post_result(&events, "start").result,
        post_result(&events, RETRY_CALL).result
    );
    assert_ne!(
        find_post_context(&durable, "start"),
        find_post_context(&durable, RETRY_CALL),
        "different calls carry their own associated feedback, not different receipt bytes"
    );
    let seen = provider.seen.lock().unwrap().clone();
    for (index, id) in [(1, "start"), (2, RETRY_CALL)] {
        let pair = post_pair(&seen[index].messages, id);
        assert_eq!(pair.content, canonical_bytes);
        assert_eq!(
            post_pair(&seen[index].durable.messages, id).content,
            canonical_bytes
        );
    }
    for (index, observation) in seen.iter().enumerate().skip(1) {
        let expected_ids: &[&str] = if index == 1 {
            &["start"]
        } else {
            &["start", RETRY_CALL]
        };
        assert_eq!(
            post_contexts(&observation.durable).len(),
            expected_ids.len()
        );
        for id in expected_ids {
            let persisted = find_post_context(&observation.durable, id).unwrap();
            assert_eq!(Some(persisted), find_post_context(&durable, id));
            assert!(
                observation
                    .messages
                    .iter()
                    .any(|message| message.content.contains(persisted)),
                "provider round {} must see the exact durable feedback context for {id}",
                index + 1
            );
            assert_feedback_context(&canonical_bytes, persisted, id);
        }
    }
    let before_retry = read_loop_state(&seen[1].durable).unwrap().unwrap();
    let after_retry = read_loop_state(&seen[2].durable).unwrap().unwrap();
    assert_eq!(before_retry.control, after_retry.control);
    assert_eq!(
        before_retry.iterations_started,
        after_retry.iterations_started
    );
    assert_eq!(after_retry.rounds_started, before_retry.rounds_started + 1,
        "only normal provider-round admission is charged for the retry with stale expected_revision=0");
    {
        let witnessed = hook.witnessed.lock().unwrap();
        let committed = persistence.committed.lock().unwrap();
        assert_eq!(witnessed.len(), 2);
        for witness in witnessed.iter() {
            let final_pair = post_pair(&durable.messages, &witness.call_id);
            assert_eq!(
                serde_json::to_value(final_pair).unwrap(),
                serde_json::to_value(&witness.before_feedback).unwrap(),
                "the entire accepted ToolResult, including its UUID, must remain immutable"
            );
            assert_eq!(&witness.canonical, canonical);
            let final_context = find_post_context(&durable, &witness.call_id).unwrap();
            assert!(
                committed.iter().any(|session| {
                    find_post_context(session, &witness.call_id) == Some(final_context)
                        && serde_json::to_value(post_pair(&session.messages, &witness.call_id))
                            .unwrap()
                            == serde_json::to_value(&witness.before_feedback).unwrap()
                }),
                "context must be committed before ACK while the original pair stays byte-identical"
            );
        }
    }
    fixture.close().await;
}

async fn assert_post_hook_terminal(
    fixture: &mut Fixture,
    persistence: &PostHookPersistence,
    events: &[AgentEvent],
    status: LoopStatus,
) {
    assert!(!events
        .iter()
        .any(|event| matches!(event, AgentEvent::ToolComplete { .. })));
    assert!(!events
        .iter()
        .any(|event| matches!(event, AgentEvent::Complete { .. })));
    let state = read_loop_state(&fixture.session).unwrap().unwrap();
    assert_eq!(state.status, status);
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        (0, 0, 1)
    );
    let expected_runtime = if status == LoopStatus::Cancelled {
        bamboo_domain::AgentStatusState::Cancelled
    } else {
        bamboo_domain::AgentStatusState::Failed
    };
    assert_eq!(
        fixture.session.agent_runtime_state.as_ref().unwrap().status,
        expected_runtime
    );
    assert!(admitted_objectives(&fixture.session.messages).is_empty());
    let control = state.control.as_ref().unwrap();
    assert_eq!(control.control_revision, 1);
    assert_eq!(control.operations.len(), 1);
    let canonical = &control.operations[0].receipt;
    assert_canonical_receipt(canonical);
    assert_eq!(canonical.operation_id, START_OPERATION);
    assert!(!canonical.applied);
    let first_accepted = {
        let snapshots = persistence.committed.lock().unwrap();
        snapshots
            .iter()
            .find(|session| {
                session.messages.iter().any(|message| {
                    message.role == Role::Tool && message.tool_call_id.as_deref() == Some("start")
                })
            })
            .expect("initial accepted operation must already be durable")
            .clone()
    };
    let initial = read_loop_state(&first_accepted).unwrap().unwrap();
    assert_eq!(
        initial.control.as_ref().unwrap().operations,
        control.operations
    );
    let first_pair = post_pair(&first_accepted.messages, "start");
    let final_pair = post_pair(&fixture.session.messages, "start");
    assert_eq!(first_pair.id, final_pair.id);
    assert_eq!(
        final_pair.tool_success,
        Some(true),
        "the operation was already accepted"
    );
    assert_eq!(post_receipt(&final_pair.content), *canonical);
    assert_eq!(
        final_pair.content,
        serde_json::to_string(canonical).unwrap()
    );
    assert_eq!(
        serde_json::to_value(first_pair).unwrap(),
        serde_json::to_value(final_pair).unwrap(),
        "feedback checkpoint failure/cancel must not rewrite the first accepted pair"
    );

    // An outer conservative save must preserve one accepted operation/pair;
    // it must never turn the failed/cancelled delivery into another execution.
    persistence
        .save_runtime_session(&mut fixture.session)
        .await
        .unwrap();
    let durable = fixture
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_loop_state(&durable).unwrap(), Some(state));
    assert_eq!(
        durable.agent_runtime_state,
        fixture.session.agent_runtime_state
    );
    assert_eq!(post_contexts(&durable), post_contexts(&fixture.session));
    assert_eq!(post_pair(&durable.messages, "start").id, first_pair.id);
    assert_eq!(
        post_pair(&durable.messages, "start").content,
        post_pair(&fixture.session.messages, "start").content
    );
}

#[tokio::test]
async fn owner_after_tool_user_cancel_prevents_ack_even_without_feedback() {
    for with_feedback in [false, true] {
        let cancel = CancellationToken::new();
        let (mut fixture, persistence, hook) =
            post_fixture(with_feedback, Some(cancel.clone()), None).await;
        let provider = Arc::new(ScriptedProvider::new(
            Script::TwoIterations,
            fixture.storage.clone(),
        ));
        let (result, events) = fixture.run(provider.clone(), cancel.clone()).await;
        assert!(
            matches!(result, Err(AgentError::Cancelled)),
            "feedback={with_feedback}: {result:?}"
        );
        assert!(cancel.is_cancelled());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.active.load(Ordering::SeqCst), 0);
        assert_eq!(hook.witnessed.lock().unwrap().len(), 1);
        assert_post_hook_terminal(&mut fixture, &persistence, &events, LoopStatus::Cancelled).await;
        fixture.close().await;
    }
}

#[tokio::test]
async fn feedback_context_checkpoint_error_keeps_one_pair_and_accepted_operation() {
    for commit_before_error in [false, true] {
        let (mut fixture, persistence, hook) = post_fixture(
            true,
            None,
            Some(PostCheckpointFault {
                commit_before_error,
                cancel_at_failure: None,
            }),
        )
        .await;
        let provider = Arc::new(ScriptedProvider::new(
            Script::TwoIterations,
            fixture.storage.clone(),
        ));
        let (result, events) = fixture
            .run(provider.clone(), CancellationToken::new())
            .await;
        assert!(
            persistence.failed.load(Ordering::SeqCst),
            "second runtime-context checkpoint was not exercised"
        );
        assert!(
            matches!(result, Err(AgentError::Budget(ref reason))
            if reason == "counted loop: Session checkpoint failed; execution not admitted"),
            "commit={commit_before_error}: preserve the original checkpoint error: {result:?}"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.active.load(Ordering::SeqCst), 0);
        assert_eq!(hook.witnessed.lock().unwrap().len(), 1);
        {
            let attempted = persistence.feedback_attempt.lock().unwrap();
            let attempted = attempted.as_ref().unwrap();
            let pair = post_pair(&attempted.messages, "start");
            let context = find_post_context(attempted, "start").unwrap();
            assert_feedback_context(&pair.content, context, "start");
            let state = read_loop_state(attempted).unwrap().unwrap();
            assert_eq!(
                pair.content,
                serde_json::to_string(&state.control.as_ref().unwrap().operations[0].receipt)
                    .unwrap()
            );
        }
        assert_post_hook_terminal(&mut fixture, &persistence, &events, LoopStatus::Failed).await;
        fixture.close().await;
    }
}

#[tokio::test]
async fn user_cancel_dominates_feedback_context_checkpoint_error_before_and_after_commit() {
    for commit_before_error in [false, true] {
        let cancel = CancellationToken::new();
        let (mut fixture, persistence, hook) = post_fixture(
            true,
            None,
            Some(PostCheckpointFault {
                commit_before_error,
                cancel_at_failure: Some(cancel.clone()),
            }),
        )
        .await;
        let provider = Arc::new(ScriptedProvider::new(
            Script::TwoIterations,
            fixture.storage.clone(),
        ));
        let (result, events) = fixture.run(provider.clone(), cancel.clone()).await;
        assert!(persistence.failed.load(Ordering::SeqCst));
        assert!(cancel.is_cancelled());
        assert!(matches!(result, Err(AgentError::Cancelled)),
            "commit={commit_before_error}: cancellation must dominate the feedback checkpoint error: {result:?}");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.active.load(Ordering::SeqCst), 0);
        assert_eq!(hook.witnessed.lock().unwrap().len(), 1);
        assert_post_hook_terminal(&mut fixture, &persistence, &events, LoopStatus::Cancelled).await;
        fixture.close().await;
    }
}
