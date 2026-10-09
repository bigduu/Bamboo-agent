use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use bamboo_agent_core::FunctionCall;
use bamboo_llm::{LLMChunk, LLMError, LLMStream};
use futures::stream;

fn call(id: &str, name: &str, arguments: &str) -> ToolCall {
    ToolCall {
        id: id.to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: name.to_string(),
            arguments: arguments.to_string(),
        },
    }
}

fn session_with_result(result: &str, success: bool) -> Session {
    let mut session = Session::new("final-evidence-test", "main-model");
    session.add_message(Message::user("Run the checks and report their result."));
    session.add_message(Message::assistant(
        "Checking",
        Some(vec![call("check-1", "Bash", r#"{"command":"cargo test"}"#)]),
    ));
    session.add_message(Message::tool_result_with_status("check-1", result, success));
    session
}

fn report(verdict: &str, corrected: Option<&str>, ids: &[&str]) -> ToolCall {
    call(
        "report-1",
        REPORT_TOOL,
        &json!({
            "verdict": verdict,
            "corrected_answer": corrected,
            "reason": "The candidate must match the recorded check outcome.",
            "evidence_ids": ids,
        })
        .to_string(),
    )
}

fn usage(input: u64, output: u64) -> LLMChunk {
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

#[derive(Clone)]
enum Response {
    Chunks(Vec<LLMChunk>),
    PendingBootstrap,
    PendingStream,
    Failure,
    FailureAfterUsage(u64, u64),
}

struct Request {
    messages: Vec<Message>,
    tools: Vec<ToolSchema>,
    model: String,
    max_output_tokens: Option<u32>,
    options: LLMRequestOptions,
}

struct RecordingProvider {
    response: Response,
    requests: Mutex<Vec<Request>>,
    entered: tokio::sync::Notify,
}

impl RecordingProvider {
    fn new(response: Response) -> Arc<Self> {
        Arc::new(Self {
            response,
            requests: Mutex::new(Vec::new()),
            entered: tokio::sync::Notify::new(),
        })
    }
}

#[async_trait::async_trait]
impl LLMProvider for RecordingProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        panic!("evidence check must dispatch with purpose and request controls")
    }

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        self.requests.lock().unwrap().push(Request {
            messages: messages.to_vec(),
            tools: tools.to_vec(),
            model: model.to_string(),
            max_output_tokens,
            options: options.cloned().expect("request controls"),
        });
        self.entered.notify_one();
        match &self.response {
            Response::Chunks(chunks) => {
                Ok(Box::pin(stream::iter(chunks.clone().into_iter().map(Ok))))
            }
            Response::PendingBootstrap => std::future::pending().await,
            Response::PendingStream => Ok(Box::pin(stream::pending())),
            Response::Failure => Err(LLMError::Api("fixture provider unavailable".into())),
            Response::FailureAfterUsage(input, output) => Ok(Box::pin(stream::iter(vec![
                Ok(usage(*input, *output)),
                Ok(LLMChunk::Token("partial verdict".into())),
                Err(LLMError::Api("fixture stream interrupted".into())),
            ]))),
        }
    }
}

fn frame<'a>(
    cancel: &'a CancellationToken,
    timeout: &'a StreamTimeoutContext,
) -> FinalEvidenceFrame<'a> {
    FinalEvidenceFrame {
        model: "fast-evidence-model",
        reasoning_effort: Some(ReasoningEffort::Max),
        timeout_context: timeout,
        cancel_token: cancel,
        max_output_tokens: 4096,
        auxiliary_max_concurrency: 1,
        token_budget: None,
    }
}

#[test]
fn canonical_status_does_not_follow_result_keywords_and_missing_results_stay_missing() {
    let mut session = session_with_result("success: all done", false);
    session.add_message(Message::assistant(
        "Retry",
        Some(vec![call("pending-2", "Bash", "{}")]),
    ));
    session.add_message(Message::tool_result_with_status(
        "legacy-3", "success", true,
    ));
    session.messages.last_mut().unwrap().tool_success = None;
    let packet = collect_evidence(&session);
    assert_eq!(packet.tools.len(), 3);
    assert_eq!(packet.tools[0].success, Some(false));
    assert_eq!(packet.tools[1].result_count, 0);
    assert!(packet.tools[1].result.is_none());
    assert_eq!(packet.tools[2].success, None);
    assert!(packet.tools[2].incomplete);
}

#[test]
fn runtime_resume_keeps_evidence_but_a_new_external_request_starts_a_new_window() {
    let mut session = session_with_result("test failed", false);
    let mut resume = Message::user("Continue after child completion");
    resume.metadata = Some(json!({"runtime_kind": "child_completion_resume"}));
    session.add_message(resume);
    assert_eq!(collect_evidence(&session).tools.len(), 1);
    for kind in [
        "stop_hook_continuation",
        "run_budget_summary",
        "max_rounds_summary",
        "no_progress_continue",
    ] {
        let mut feedback = Message::user("Summarize the remaining work.");
        feedback.metadata = Some(json!({"runtime_kind": kind}));
        session.add_message(feedback);
        assert_eq!(collect_evidence(&session).tools.len(), 1);
    }
    session.add_message(Message::user("Explain a concept instead."));
    assert!(collect_evidence(&session).tools.is_empty());
}

#[test]
fn bounded_evidence_marks_omission_compression_and_duplicate_results() {
    let mut session = session_with_result(
        &format!("start{}terminal failure", "界".repeat(MAX_RESULT_CHARS)),
        false,
    );
    session.add_message(Message::tool_result_with_status("check-1", "second", true));
    session.messages.last_mut().unwrap().compressed = true;
    let packet = collect_evidence(&session);
    assert_eq!(packet.tools[0].result_count, 2);
    assert!(packet.tools[0].incomplete);

    let (bounded, truncated) = bounded_text(
        &format!("head{}terminal failure", "界".repeat(MAX_RESULT_CHARS)),
        MAX_RESULT_CHARS,
    );
    assert!(truncated);
    assert!(bounded.starts_with("head"));
    assert!(bounded.ends_with("terminal failure"));
    assert!(bounded.contains("[... omitted ...]"));
    for index in 0..MAX_RECORDS {
        session.add_message(Message::assistant(
            "",
            Some(vec![call(&format!("call-{index}"), "Read", "{}")]),
        ));
    }
    let packet = collect_evidence(&session);
    assert_eq!(packet.tools.len(), MAX_RECORDS);
    assert_eq!(packet.omitted_tool_records, 1);
    assert_eq!(packet.tools[0].tool_call_id, "call-0");
}

#[test]
fn verdict_requires_one_report_known_evidence_and_a_real_revision() {
    let packet = collect_evidence(&session_with_result("failed", false));
    assert!(matches!(
        parse_report(&[], &packet),
        Err(FinalEvidenceError::MissingOrMultipleVerdicts)
    ));
    assert!(matches!(
        parse_report(&[report("supported", None, &["invented-call"])], &packet),
        Err(FinalEvidenceError::UnknownEvidence)
    ));
    for invalid in [
        report("revise", None, &["check-1"]),
        report("revise", Some("   "), &["check-1"]),
        report("supported", Some("Changed text"), &["check-1"]),
        report("supported", None, &[]),
        call("report", REPORT_TOOL, "not json"),
    ] {
        assert!(matches!(
            parse_report(&[invalid], &packet),
            Err(FinalEvidenceError::InvalidVerdict)
        ));
    }
    let extra = call("unexpected", "Bash", "{}");
    assert!(matches!(
        parse_report(&[report("supported", None, &["check-1"]), extra], &packet),
        Err(FinalEvidenceError::MissingOrMultipleVerdicts)
    ));
}

#[tokio::test]
async fn dispatch_is_single_scoped_and_read_only_with_authoritative_usage() {
    let session = session_with_result("10 passed; exit 0", true);
    let original = serde_json::to_value(&session).unwrap();
    let provider = RecordingProvider::new(Response::Chunks(vec![
        LLMChunk::ToolCalls(vec![report("supported", None, &["check-1"])]),
        usage(91, 17),
        LLMChunk::Done,
    ]));
    let dispatched = AtomicUsize::new(0);
    let result = evaluate_final_evidence(
        &session,
        "10 tests passed.",
        provider.clone(),
        &frame(&CancellationToken::new(), &StreamTimeoutContext::default()),
        || {
            dispatched.fetch_add(1, Ordering::SeqCst);
        },
    )
    .await
    .unwrap();
    assert_eq!(result.verdict, Some(FinalEvidenceVerdict::Supported));
    assert!(!result.is_skipped());
    assert!(result.error.is_none());
    assert_eq!(result.usage.prompt_tokens, 91);
    assert_eq!(result.usage.completion_tokens, 17);
    assert_eq!(result.usage.total_tokens, 108);
    assert_eq!(dispatched.load(Ordering::SeqCst), 1);
    assert_eq!(serde_json::to_value(&session).unwrap(), original);
    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.model, "fast-evidence-model");
    assert_eq!(request.max_output_tokens, Some(4096));
    assert_eq!(request.tools.len(), 1);
    assert_eq!(request.tools[0].function.name, REPORT_TOOL);
    assert_eq!(
        request.options.request_purpose.as_deref(),
        Some("final_evidence_check")
    );
    assert_eq!(request.options.required_tool.as_deref(), Some(REPORT_TOOL));
    assert_eq!(request.options.parallel_tool_calls, Some(false));
    assert_eq!(
        request.options.reasoning_effort,
        Some(ReasoningEffort::High)
    );
    assert_eq!(
        request.options.session_id.as_deref(),
        Some(session.id.as_str())
    );
    assert!(request.messages[0].content.contains("untrusted data"));
    assert!(request.messages[0]
        .content
        .contains("success=null is unknown"));
}

/// Fixed provider responses certify the correction contract, not model quality.
/// A real-provider comparison is recorded separately by the integration owner.
#[tokio::test]
async fn fixed_semantic_cases_preserve_honest_reports_and_return_scoped_corrections() {
    let cases = [
        (
            false,
            "0 passed; 1 failed; exit 101",
            "All tests passed.",
            "revise",
            Some("The test failed; the fix is not verified."),
        ),
        (
            true,
            "Remote operation state: queued",
            "Published successfully.",
            "revise",
            Some("Publication was queued; completion is unverified."),
        ),
        (
            true,
            "10 passed; exit 0",
            "10 tests passed.",
            "supported",
            None,
        ),
        (
            false,
            "exit 101",
            "The check failed; more work remains.",
            "supported",
            None,
        ),
    ];
    for (success, evidence, candidate, verdict, correction) in cases {
        let session = session_with_result(evidence, success);
        let provider = RecordingProvider::new(Response::Chunks(vec![
            LLMChunk::ReasoningToken("Compare the recorded outcome with the final claim.".into()),
            LLMChunk::ToolCalls(vec![report(verdict, correction, &["check-1"])]),
            LLMChunk::Done,
        ]));
        let result = evaluate_final_evidence(
            &session,
            candidate,
            provider.clone(),
            &frame(&CancellationToken::new(), &StreamTimeoutContext::default()),
            || {},
        )
        .await
        .unwrap();
        assert_eq!(result.corrected_answer.as_deref(), correction);
        assert_eq!(result.evidence_ids, ["check-1"]);
        assert_eq!(
            result.verdict,
            Some(if verdict == "revise" {
                FinalEvidenceVerdict::Revise
            } else {
                FinalEvidenceVerdict::Supported
            })
        );
        let requests = provider.requests.lock().unwrap();
        assert!(requests[0].messages[1].content.contains(evidence));
        assert!(requests[0].messages[1].content.contains(candidate));
        assert!(
            result.usage.total_tokens > 0,
            "missing provider usage uses the existing tokenizer fallback"
        );
        assert!(
            result.usage.completion_tokens
                >= u64::from(
                    TiktokenTokenCounter::default()
                        .count_text("Compare the recorded outcome with the final claim.")
                )
        );
    }
}

#[tokio::test]
async fn no_evidence_skips_without_dispatch_and_invalid_verdict_keeps_usage() {
    let provider = RecordingProvider::new(Response::Chunks(vec![
        LLMChunk::ToolCalls(vec![report("supported", None, &["unknown"])]),
        usage(7, 3),
        LLMChunk::Done,
    ]));
    let skipped = evaluate_final_evidence(
        &Session::new("no-tools", "main"),
        "Hello",
        provider.clone(),
        &frame(&CancellationToken::new(), &StreamTimeoutContext::default()),
        || panic!("no-evidence check cannot dispatch"),
    )
    .await
    .unwrap();
    assert!(skipped.is_skipped());
    assert_eq!(skipped.usage.total_tokens, 0);
    assert!(provider.requests.lock().unwrap().is_empty());

    let invalid = evaluate_final_evidence(
        &session_with_result("failed", false),
        "All done",
        provider,
        &frame(&CancellationToken::new(), &StreamTimeoutContext::default()),
        || {},
    )
    .await
    .unwrap();
    assert!(!invalid.is_skipped());
    assert_eq!(invalid.verdict, None);
    assert_eq!(invalid.error, Some(FinalEvidenceError::UnknownEvidence));
    assert_eq!(invalid.usage.total_tokens, 10);
}

#[tokio::test]
async fn explicit_provider_zero_does_not_become_an_estimated_bill() {
    let provider = RecordingProvider::new(Response::Chunks(vec![
        LLMChunk::ToolCalls(vec![report("supported", None, &["check-1"])]),
        usage(0, 0),
        LLMChunk::Done,
    ]));
    let result = evaluate_final_evidence(
        &session_with_result("10 passed", true),
        "10 tests passed.",
        provider,
        &frame(&CancellationToken::new(), &StreamTimeoutContext::default()),
        || {},
    )
    .await
    .unwrap();
    assert_eq!(result.usage.total_tokens, 0);
}

#[tokio::test]
async fn cancellation_while_queued_does_not_dispatch_or_consume_a_slot() {
    let provider = RecordingProvider::new(Response::PendingBootstrap);
    let llm: Arc<dyn LLMProvider> = provider.clone();
    let held =
        crate::runtime::runner::auxiliary_budget::acquire(&llm, "fast-evidence-model", 1).await;
    let cancel = CancellationToken::new();
    let timeout = StreamTimeoutContext::default();
    let session = session_with_result("failed", false);
    let request_frame = frame(&cancel, &timeout);
    let mut check = Box::pin(evaluate_final_evidence(
        &session,
        "Done",
        llm.clone(),
        &request_frame,
        || panic!("cancelled queue cannot dispatch"),
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(1), check.as_mut())
            .await
            .is_err()
    );
    cancel.cancel();
    assert!(matches!(
        check.await,
        Err(FinalEvidenceFailure {
            error: AgentError::Cancelled,
            ..
        })
    ));
    assert!(provider.requests.lock().unwrap().is_empty());
    drop(held);
    let _available = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        crate::runtime::runner::auxiliary_budget::acquire(&llm, "fast-evidence-model", 1),
    )
    .await
    .expect("cancelled queued check must release its waiter");
}

#[tokio::test]
async fn cancellation_stops_bootstrap_and_stream_without_retrying() {
    for response in [Response::PendingBootstrap, Response::PendingStream] {
        let provider = RecordingProvider::new(response);
        let cancel = CancellationToken::new();
        let task_provider = provider.clone();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            evaluate_final_evidence(
                &session_with_result("failed", false),
                "Done",
                task_provider,
                &frame(&task_cancel, &StreamTimeoutContext::default()),
                || {},
            )
            .await
        });
        provider.entered.notified().await;
        cancel.cancel();
        assert!(matches!(
            task.await.unwrap(),
            Err(FinalEvidenceFailure {
                error: AgentError::Cancelled,
                ..
            })
        ));
        assert_eq!(provider.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn existing_watchdog_bounds_an_unresponsive_auxiliary_request() {
    let provider = RecordingProvider::new(Response::PendingBootstrap);
    let task_provider = provider.clone();
    let task = tokio::spawn(async move {
        let timeout = StreamTimeoutContext::new(
            bamboo_config::StreamTimeoutConfig {
                transport_idle_timeout_secs: 1,
                first_semantic_timeout_secs: 1,
                semantic_idle_timeout_secs: 1,
            },
            Some("fixture-provider"),
            Some("fast-evidence-model"),
        );
        evaluate_final_evidence(
            &session_with_result("failed", false),
            "Done",
            task_provider,
            &frame(&CancellationToken::new(), &timeout),
            || {},
        )
        .await
    });
    provider.entered.notified().await;
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    assert!(matches!(
        task.await.unwrap(),
        Err(FinalEvidenceFailure {
            error: AgentError::StreamTimeout(_),
            ..
        })
    ));
    assert_eq!(provider.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn unavailable_provider_is_an_error_without_a_fabricated_pass_or_retry() {
    let provider = RecordingProvider::new(Response::Failure);
    let result = evaluate_final_evidence(
        &session_with_result("failed", false),
        "All done",
        provider.clone(),
        &frame(&CancellationToken::new(), &StreamTimeoutContext::default()),
        || {},
    )
    .await;
    assert!(matches!(
        result,
        Err(FinalEvidenceFailure {
            error: AgentError::LLM(_),
            ..
        })
    ));
    assert_eq!(provider.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn run_token_budget_checks_prompt_and_clamps_completion_before_dispatch() {
    let provider = RecordingProvider::new(Response::Chunks(vec![
        LLMChunk::ToolCalls(vec![report("supported", None, &["check-1"])]),
        LLMChunk::Done,
    ]));
    let session = session_with_result("10 passed", true);
    let cancel = CancellationToken::new();
    let timeout = StreamTimeoutContext::default();
    let mut request_frame = frame(&cancel, &timeout);
    request_frame.token_budget = Some(1);
    assert!(matches!(
        evaluate_final_evidence(&session, "10 tests passed.", provider.clone(), &request_frame,
            || panic!("exhausted budget cannot dispatch")).await,
        Err(FinalEvidenceFailure { error: AgentError::Budget(message), usage }) if message == "final evidence token budget exhausted" && usage.total_tokens == 0
    ));
    assert!(provider.requests.lock().unwrap().is_empty());

    let packet = collect_evidence(&session);
    let messages = build_messages(&packet, "10 tests passed.");
    let counter = TiktokenTokenCounter::default();
    let prompt = u64::from(counter.count_messages(&messages))
        + u64::from(counter.count_text(&serde_json::to_string(&report_schema()).unwrap()));
    request_frame.token_budget = Some(prompt + 37);
    evaluate_final_evidence(
        &session,
        "10 tests passed.",
        provider.clone(),
        &request_frame,
        || {},
    )
    .await
    .unwrap();
    assert_eq!(
        provider.requests.lock().unwrap()[0].max_output_tokens,
        Some(37)
    );
}

#[tokio::test]
async fn interrupted_evaluator_keeps_received_provider_usage_including_explicit_zero() {
    for (input, output) in [(42, 8), (0, 0)] {
        let provider = RecordingProvider::new(Response::FailureAfterUsage(input, output));
        let result = evaluate_final_evidence(
            &session_with_result("failed", false),
            "All done",
            provider.clone(),
            &frame(&CancellationToken::new(), &StreamTimeoutContext::default()),
            || {},
        )
        .await;
        let failure = match result {
            Err(failure) => failure,
            Ok(_) => panic!("interrupted stream cannot yield a final verdict"),
        };
        assert!(matches!(failure.error, AgentError::LLM(_)));
        assert_eq!(failure.usage.prompt_tokens, input);
        assert_eq!(failure.usage.completion_tokens, output);
        assert_eq!(failure.usage.total_tokens, input + output);
        assert_eq!(provider.requests.lock().unwrap().len(), 1);
    }
}
