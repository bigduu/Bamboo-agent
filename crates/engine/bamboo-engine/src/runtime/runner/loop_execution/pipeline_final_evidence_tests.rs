use super::*;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

use bamboo_agent_core::{FunctionCall, ToolCall, ToolError, ToolResult, ToolSchema};
use bamboo_llm::{LLMChunk, LLMError, LLMRequestOptions, LLMStream};
use futures::{stream, StreamExt};
use serde_json::json;

const CANDIDATE: &str = "All checks passed and the task is complete.";
const CORRECTED: &str = "The check failed; the task remains incomplete.";
const DISCOVERY_PROGRESS: &str = "Looking up the available tools.";

#[derive(Clone, Copy)]
enum Verdict {
    Revise,
    InvalidReference,
    CancelAfterUsage,
    NativeSearch(bool),
}

struct FinalProvider {
    verdict: Verdict,
    main_calls: AtomicUsize,
    auxiliary_calls: AtomicUsize,
    stream_waiting: Arc<tokio::sync::Notify>,
    evidence_prompt: Mutex<String>,
}

impl FinalProvider {
    fn new(verdict: Verdict) -> Arc<Self> {
        Arc::new(Self {
            verdict,
            main_calls: AtomicUsize::new(0),
            auxiliary_calls: AtomicUsize::new(0),
            stream_waiting: Arc::new(tokio::sync::Notify::new()),
            evidence_prompt: Mutex::new(String::new()),
        })
    }
}

fn provider_usage(input: u64, output: u64) -> LLMChunk {
    LLMChunk::ProviderUsage {
        input_tokens: Some(input),
        output_tokens: Some(output),
        total_tokens: Some(input + output),
        reasoning_tokens: None,
        cache_creation_input_tokens: None,
        cache_read_input_tokens: None,
        cache_write_input_tokens: None,
    }
}

fn native_item(
    author: ProviderTranscriptAuthor,
    payload: serde_json::Value,
) -> ProviderTranscriptItem {
    ProviderTranscriptItem::try_from_payload(
        ProviderFamily::OpenAi,
        ProviderProtocol::OpenAiResponsesV1,
        ProviderTranscriptOrigin::Provider,
        author,
        payload,
    )
    .unwrap()
}

#[async_trait::async_trait]
impl LLMProvider for FinalProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.chat_stream_with_options(messages, tools, max_output_tokens, model, None)
            .await
    }

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        if options.and_then(|options| options.request_purpose.as_deref())
            != Some("final_evidence_check")
        {
            let round = self.main_calls.fetch_add(1, Ordering::SeqCst);
            let search = matches!(self.verdict, Verdict::NativeSearch(_)) && round == 0;
            let content = match (search, self.verdict) {
                (true, Verdict::NativeSearch(true)) => "",
                (true, _) => DISCOVERY_PROGRESS,
                _ => CANDIDATE,
            };
            let mut chunks = vec![
                Ok(LLMChunk::ResponseId("main-response".into())),
                Ok(LLMChunk::ReasoningToken("original signed thought".into())),
                Ok(LLMChunk::ReasoningSignature("original-signature".into())),
                Ok(LLMChunk::Token(content.into())),
            ];
            if search {
                chunks.push(Ok(LLMChunk::ProviderTranscriptItem(native_item(
                    ProviderTranscriptAuthor::Model,
                    json!({"type":"tool_search_call", "id":"native-search", "call_id":"search-1",
                        "execution":"client", "status":"completed", "arguments":{"query":"checks"}}),
                ))));
            }
            chunks.extend([Ok(provider_usage(100, 10)), Ok(LLMChunk::Done)]);
            return Ok(Box::pin(stream::iter(chunks)));
        }
        self.auxiliary_calls.fetch_add(1, Ordering::SeqCst);
        *self.evidence_prompt.lock().unwrap() = messages[1].content.clone();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].function.name, "report_final_evidence_check");
        if matches!(self.verdict, Verdict::CancelAfterUsage) {
            let notify = self.stream_waiting.clone();
            return Ok(Box::pin(stream::iter([Ok(provider_usage(7, 3))]).chain(
                stream::poll_fn(move |_| {
                    notify.notify_one();
                    std::task::Poll::Pending
                }),
            )));
        }
        let evidence_id = if matches!(self.verdict, Verdict::InvalidReference) {
            "invented-call"
        } else {
            "check-1"
        };
        let report = ToolCall {
            id: "evidence-report".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "report_final_evidence_check".into(),
                arguments: json!({
                    "verdict": "revise",
                    "corrected_answer": CORRECTED,
                    "reason": "The recorded check failed.",
                    "evidence_ids": [evidence_id]
                })
                .to_string(),
            },
        };
        Ok(Box::pin(stream::iter(vec![
            Ok(LLMChunk::ToolCalls(vec![report])),
            Ok(provider_usage(7, 3)),
            Ok(LLMChunk::Done),
        ])))
    }
}

struct NoExecution;

#[async_trait::async_trait]
impl ToolExecutor for NoExecution {
    async fn execute(&self, _: &ToolCall) -> Result<ToolResult, ToolError> {
        panic!("checking a final answer cannot execute a tool")
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        Vec::new()
    }
}

fn fixture(enabled: bool) -> (Session, AgentLoopConfig, LoopRunState) {
    let mut session = Session::new("pipeline-final-evidence", "model");
    session.add_message(Message::user("Run the check and report the result."));
    session.add_message(Message::assistant(
        "Checking",
        Some(vec![ToolCall {
            id: "check-1".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Bash".into(),
                arguments: r#"{"command":"cargo test"}"#.into(),
            },
        }]),
    ));
    session.add_message(Message::tool_result_with_status(
        "check-1",
        "test result: 1 failed; exit 101",
        false,
    ));
    let config = AgentLoopConfig {
        features_final_evidence_check: enabled,
        model_name: Some("model".into()),
        fast_model_name: Some("fast-model".into()),
        provider_type: Some("openai".into()),
        prompt_memory_flags: crate::runtime::config::PromptMemoryFlags {
            project_prompt_injection: false,
            relevant_recall: false,
            relevant_recall_rerank: false,
            project_first_dream: false,
            ledger_agenda: false,
        },
        ..Default::default()
    };
    let state = LoopRunState {
        session_id: session.id.clone(),
        execution_id: "final-evidence-execution".into(),
        current_inputs: None,
        model_name: "model".into(),
        metrics_collector: None,
        debug_logger: crate::runtime::runner::logging::DebugLogger::new(false),
        task_context: None,
        overflow_recovery: Default::default(),
        task_evaluation: Default::default(),
        gold_evaluation: Default::default(),
        auxiliary_models: Default::default(),
        runtime_state: AgentRuntimeState::new(&session.id),
    };
    (session, config, state)
}

async fn run(
    session: &mut Session,
    config: &AgentLoopConfig,
    state: &mut LoopRunState,
    provider: Arc<FinalProvider>,
    tx: &mpsc::Sender<AgentEvent>,
) -> Result<bool, AgentError> {
    let tools = Arc::new(NoExecution);
    let cancel = CancellationToken::new();
    run_pipeline(session, tx, provider, tools, &cancel, config, state).await
}

fn drain(rx: &mut mpsc::Receiver<AgentEvent>) -> (String, usize) {
    let mut content = String::new();
    let mut completes = 0;
    while let Ok(event) = rx.try_recv() {
        match event {
            AgentEvent::Token { content: chunk } => content.push_str(&chunk),
            AgentEvent::Complete { .. } => completes += 1,
            _ => {}
        }
    }
    (content, completes)
}

#[tokio::test]
async fn disabled_check_keeps_the_original_final_answer_without_an_auxiliary_call() {
    let (mut session, config, mut state) = fixture(false);
    let provider = FinalProvider::new(Verdict::Revise);
    let (tx, mut rx) = mpsc::channel(128);
    let result = run(&mut session, &config, &mut state, provider.clone(), &tx).await;
    assert!(result.unwrap());
    assert_eq!(drain(&mut rx), (CANDIDATE.to_string(), 1));
    assert_eq!(provider.main_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.auxiliary_calls.load(Ordering::SeqCst), 0);
    assert_eq!(session.messages.last().unwrap().content, CANDIDATE);
    assert_eq!(state.runtime_state.round.total_prompt_tokens, 100);
    assert_eq!(state.runtime_state.round.total_completion_tokens, 10);
    assert!(!session
        .metadata
        .contains_key("runtime.final_evidence_check"));
}

#[tokio::test]
async fn enabled_check_emits_and_saves_only_the_correction_and_accounts_once() {
    let (mut session, config, mut state) = fixture(true);
    let provider = FinalProvider::new(Verdict::Revise);
    let (tx, mut rx) = mpsc::channel(128);
    let result = run(&mut session, &config, &mut state, provider.clone(), &tx).await;
    assert!(result.unwrap());
    assert_eq!(drain(&mut rx), (CORRECTED.to_string(), 1));
    let answer = session.messages.last().unwrap();
    assert_eq!(answer.content, CORRECTED);
    assert!(answer.reasoning.is_none());
    assert!(answer.reasoning_signature.is_none());
    assert_eq!(provider.main_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.auxiliary_calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.runtime_state.round.total_prompt_tokens, 107);
    assert_eq!(state.runtime_state.round.total_completion_tokens, 13);
    assert_eq!(
        session
            .model_context_state
            .as_ref()
            .unwrap()
            .last_reset_reason,
        Some(bamboo_domain::ModelContextResetReason::ExplicitHistoryRewrite)
    );
    assert!(!session
        .metadata
        .contains_key("responses.previous_response_id"));
    let status: serde_json::Value =
        serde_json::from_str(&session.metadata["runtime.final_evidence_check"]).unwrap();
    assert_eq!(status["status"], "revised");
    assert_eq!(status["evidence_ids"], json!(["check-1"]));
}

#[tokio::test]
async fn invalid_verdict_does_not_publish_the_candidate_but_retains_billed_usage() {
    let (mut session, config, mut state) = fixture(true);
    let provider = FinalProvider::new(Verdict::InvalidReference);
    let (tx, mut rx) = mpsc::channel(128);
    let result = run(&mut session, &config, &mut state, provider.clone(), &tx).await;
    assert!(matches!(result, Err(AgentError::LLM(_))));
    assert_eq!(drain(&mut rx), (String::new(), 0));
    assert_eq!(provider.main_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.auxiliary_calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.runtime_state.round.total_prompt_tokens, 107);
    assert_eq!(state.runtime_state.round.total_completion_tokens, 13);
    assert_eq!(session.messages.last().unwrap().role, Role::Tool);
}

#[tokio::test]
async fn cancelled_check_never_emits_complete_and_keeps_received_auxiliary_usage() {
    let (mut session, config, mut state) = fixture(true);
    let provider = FinalProvider::new(Verdict::CancelAfterUsage);
    let (tx, mut rx) = mpsc::channel(128);
    let cancel = CancellationToken::new();
    let work = run_pipeline(
        &mut session,
        &tx,
        provider.clone(),
        Arc::new(NoExecution),
        &cancel,
        &config,
        &mut state,
    );
    let cancellation = async {
        provider.stream_waiting.notified().await;
        cancel.cancel();
    };
    let (result, ()) = tokio::join!(work, cancellation);
    assert!(matches!(result, Err(AgentError::Cancelled)));
    assert_eq!(drain(&mut rx), (String::new(), 0));
    assert_eq!(state.runtime_state.round.total_prompt_tokens, 107);
    assert_eq!(state.runtime_state.round.total_completion_tokens, 13);
    assert_eq!(provider.main_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.auxiliary_calls.load(Ordering::SeqCst), 1);
}

struct FinalGateProbe {
    deny_first: bool,
    calls: AtomicUsize,
    candidate_id: Mutex<Option<String>>,
    transcript_epoch: AtomicUsize,
}

fn native_replay_count(session: &Session) -> usize {
    let transcript = &session.provider_transcript;
    transcript
        .replayable_groups(
            transcript.active_family().unwrap(),
            transcript.active_protocol().unwrap(),
            transcript.active_provider_boundary_sha256().unwrap(),
        )
        .len()
}

#[async_trait::async_trait]
impl bamboo_agent_core::AgentHook for FinalGateProbe {
    fn point(&self) -> AgentHookPoint {
        AgentHookPoint::BeforeFinalize
    }
    async fn run(&self, _: AgentHookPoint, _: &HookPayload, session: &Session) -> HookResult {
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        if let Some(message) = session
            .messages
            .last()
            .filter(|message| message.role == Role::Assistant && message.tool_calls.is_none())
        {
            assert!(native_replay_count(session) > 0);
            // The main stream's store=false policy discards even a returned ResponseId.
            assert!(!session
                .metadata
                .contains_key("responses.previous_response_id"));
            *self.candidate_id.lock().unwrap() = Some(message.id.clone());
            self.transcript_epoch.store(
                session.provider_transcript.epoch() as usize,
                Ordering::SeqCst,
            );
        }
        if first && self.deny_first {
            HookResult::Deny {
                reason: "Report the failed check honestly.".into(),
            }
        } else {
            HookResult::Continue
        }
    }
}

fn install_probe(config: &mut AgentLoopConfig, deny_first: bool) -> Arc<FinalGateProbe> {
    let probe = Arc::new(FinalGateProbe {
        deny_first,
        calls: AtomicUsize::new(0),
        candidate_id: Mutex::new(None),
        transcript_epoch: AtomicUsize::new(0),
    });
    let mut hooks = crate::runtime::hooks::HookRunner::new();
    hooks.register(probe.clone());
    config.hook_runner = Arc::new(hooks);
    probe
}

#[tokio::test]
async fn stop_hook_continuation_keeps_original_evidence_and_checks_only_the_final_candidate() {
    let (mut session, mut config, mut state) = fixture(true);
    let probe = install_probe(&mut config, true);
    let provider = FinalProvider::new(Verdict::Revise);
    let (tx, mut rx) = mpsc::channel(128);
    let result = run(&mut session, &config, &mut state, provider.clone(), &tx).await;
    assert!(result.unwrap());
    assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
    assert_eq!(provider.main_calls.load(Ordering::SeqCst), 2);
    assert_eq!(provider.auxiliary_calls.load(Ordering::SeqCst), 1);
    let packet = provider.evidence_prompt.lock().unwrap();
    assert!(packet.contains("Run the check and report the result."));
    assert!(packet.contains("test result: 1 failed; exit 101"));
    assert_eq!(drain(&mut rx), (format!("{CANDIDATE}{CORRECTED}"), 1));
    assert_eq!(session.messages.last().unwrap().content, CORRECTED);
    assert_eq!(state.runtime_state.round.total_prompt_tokens, 207);
    assert_eq!(state.runtime_state.round.total_completion_tokens, 23);
}

#[tokio::test]
async fn native_discovery_replays_progress_once_with_the_committed_identity_before_final_check() {
    for (enabled, thought_only) in [(false, false), (true, false), (false, true), (true, true)] {
        let (mut session, config, mut state) = fixture(enabled);
        let provider = FinalProvider::new(Verdict::NativeSearch(thought_only));
        let (tx, mut rx) = mpsc::channel(128);
        let result = run(&mut session, &config, &mut state, provider.clone(), &tx).await;
        assert!(result.unwrap());
        let mut current_id = None;
        let mut texts = Vec::new();
        let mut reasoning_count = 0;
        let mut started_count = 0;
        let mut completes = 0;
        while let Ok(event) = rx.try_recv() {
            let stored = session
                .messages
                .iter()
                .find(|message| Some(&message.id) == current_id.as_ref());
            match event {
                AgentEvent::VisibleMessageStart { message_id, .. } => {
                    current_id = Some(message_id);
                    started_count += 1;
                }
                AgentEvent::Token { content } => {
                    assert_eq!(stored.unwrap().content, content);
                    texts.push(content);
                }
                AgentEvent::ReasoningToken { content } => {
                    if enabled {
                        assert_eq!(stored.unwrap().reasoning.as_deref(), Some(content.as_str()));
                    } else {
                        assert_eq!(content, "original signed thought");
                        let expected_starts = if thought_only { 0 } else { reasoning_count };
                        assert_eq!(started_count, expected_starts);
                    }
                    reasoning_count += 1;
                }
                AgentEvent::Complete { .. } => completes += 1,
                _ => {}
            }
        }
        let mut expected = if thought_only {
            vec![]
        } else {
            vec![DISCOVERY_PROGRESS]
        };
        expected.push(if enabled { CORRECTED } else { CANDIDATE });
        assert_eq!(texts, expected);
        assert_eq!(reasoning_count, if enabled { 1 } else { 2 });
        assert_eq!(completes, 1);
        assert_eq!(provider.main_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            provider.auxiliary_calls.load(Ordering::SeqCst),
            usize::from(enabled)
        );
    }
}

#[tokio::test]
async fn gold_committed_candidate_is_revised_in_place_without_a_duplicate_or_stale_native_chain() {
    for verdict in [Verdict::Revise, Verdict::InvalidReference] {
        let (mut session, mut config, mut state) = fixture(true);
        let boundary = bamboo_domain::provider_transcript_boundary_sha256(
            config.provider_name.as_deref(),
            config.provider_type.as_deref(),
        )
        .unwrap();
        session
            .activate_provider_transcript_route(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                &boundary,
            )
            .unwrap();
        let anchor = session.messages[1].id.clone();
        let call = session.messages[1].tool_calls.as_ref().unwrap()[0].clone();
        session.append_provider_transcript_group(&anchor, None, vec![
            native_item(ProviderTranscriptAuthor::Model,
                json!({"type":"tool_search_call", "id":"hosted-search", "call_id":"hosted-1",
                    "execution":"server", "status":"completed", "arguments":{"query":"checks"}})),
            native_item(ProviderTranscriptAuthor::ToolResult,
                json!({"type":"tool_search_output", "id":"hosted-output", "call_id":"hosted-1",
                    "execution":"server", "status":"completed", "tools":[]})),
            native_item(ProviderTranscriptAuthor::Model,
                json!({"type":"function_call", "call_id":call.id,
                    "name":call.function.name, "arguments":call.function.arguments})),
        ]).unwrap();
        config.gold_config = Some(crate::runtime::config::GoldConfig {
            enabled: true,
            auto_continue_enabled: true,
            goal: Some("report the check".into()),
            max_auto_continuations: 0,
            ..Default::default()
        });
        let probe = install_probe(&mut config, false);
        let provider = FinalProvider::new(verdict);
        let (tx, mut rx) = mpsc::channel(128);
        let result = run(&mut session, &config, &mut state, provider.clone(), &tx).await;
        let candidate_id = probe.candidate_id.lock().unwrap().clone().unwrap();
        let revised = matches!(verdict, Verdict::Revise);
        if revised {
            assert!(result.unwrap());
            assert_eq!(session.messages.last().unwrap().id, candidate_id);
            assert_eq!(session.messages.last().unwrap().content, CORRECTED);
        } else {
            assert!(matches!(result, Err(AgentError::LLM(_))));
            assert!(!session
                .messages
                .iter()
                .any(|message| message.id == candidate_id));
            assert_eq!(session.messages.last().unwrap().role, Role::Tool);
        }
        assert_eq!(
            session
                .messages
                .iter()
                .filter(|message| message.role == Role::Assistant && message.tool_calls.is_none())
                .count(),
            usize::from(revised)
        );
        assert!(
            session.provider_transcript.epoch() as usize
                > probe.transcript_epoch.load(Ordering::SeqCst)
        );
        assert_eq!(native_replay_count(&session), 0);
        assert!(!session
            .metadata
            .contains_key("responses.previous_response_id"));
        let expected = if revised { CORRECTED } else { "" };
        assert_eq!(drain(&mut rx), (expected.into(), usize::from(revised)));
        assert_eq!(provider.main_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.auxiliary_calls.load(Ordering::SeqCst), 1);
    }
}
