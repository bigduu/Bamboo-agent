use std::time::Duration;

use futures::{stream, StreamExt};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_agent_core::{AgentError, AgentEvent, StreamTimeoutPhase};
use bamboo_config::StreamTimeoutConfig;
use bamboo_llm::provider::LLMError;
use bamboo_llm::providers::common::openai_compat::parse_openai_compat_sse_data_strict;
use bamboo_llm::providers::common::openai_responses::ResponsesSseParser;
use bamboo_llm::providers::common::sse::llm_stream_from_sse_multi_requiring_done;
use bamboo_llm::{LLMChunk, LLMStream};

use super::consume::consume_llm_stream_internal;
use super::{
    await_stream_bootstrap, consume_llm_stream, consume_llm_stream_silent, ProviderUsageSnapshot,
    StreamTimeoutContext,
};

fn build_stream(items: Vec<bamboo_llm::provider::Result<LLMChunk>>) -> LLMStream {
    Box::pin(stream::iter(items))
}

fn timeout_context(
    transport_secs: u64,
    first_semantic_secs: u64,
    semantic_secs: u64,
) -> StreamTimeoutContext {
    StreamTimeoutContext::new(
        StreamTimeoutConfig {
            transport_idle_timeout_secs: transport_secs,
            first_semantic_timeout_secs: first_semantic_secs,
            semantic_idle_timeout_secs: semantic_secs,
        },
        Some("test-provider"),
        Some("test-model"),
    )
    .allow_turn_retry_before_semantic_output()
}

#[tokio::test]
async fn consume_llm_stream_accumulates_tokens_and_tool_calls() {
    let stream = build_stream(vec![
        Ok(LLMChunk::ResponseId("resp_123".to_string())),
        Ok(LLMChunk::ReasoningToken("thinking".to_string())),
        Ok(LLMChunk::Token("hi".to_string())),
        Ok(LLMChunk::ToolCalls(vec![ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "test_tool".to_string(),
                arguments: "{".to_string(),
            },
        }])),
        Ok(LLMChunk::ToolCalls(vec![ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: String::new(),
                arguments: "}".to_string(),
            },
        }])),
        Ok(LLMChunk::Done),
    ]);

    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(8);
    let output = consume_llm_stream(stream, &event_tx, &CancellationToken::new(), "session-1")
        .await
        .expect("stream should succeed");

    assert_eq!(output.response_id.as_deref(), Some("resp_123"));
    assert_eq!(output.content, "hi");
    assert_eq!(output.reasoning_content, "thinking");
    assert_eq!(output.token_count, 2);
    assert_eq!(output.tool_calls.len(), 1);
    assert_eq!(output.tool_calls[0].function.name, "test_tool");
    assert_eq!(output.tool_calls[0].function.arguments, "{}");

    let reasoning_event = event_rx.recv().await.expect("missing reasoning event");
    assert!(matches!(reasoning_event, AgentEvent::ReasoningToken { .. }));

    let token_event = event_rx.recv().await.expect("missing token event");
    assert!(matches!(token_event, AgentEvent::Token { .. }));
}

/// #520: a provider-minted reasoning signature is surfaced on the output so
/// the persisted assistant message can replay a SIGNED thinking block.
#[tokio::test]
async fn consume_llm_stream_captures_reasoning_signature() {
    let stream = build_stream(vec![
        Ok(LLMChunk::ReasoningToken("thinking".to_string())),
        Ok(LLMChunk::ReasoningSignature("sig_abc".to_string())),
        Ok(LLMChunk::Token("hi".to_string())),
        Ok(LLMChunk::Done),
    ]);

    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(8);
    let output = consume_llm_stream(stream, &event_tx, &CancellationToken::new(), "session-sig")
        .await
        .expect("stream should succeed");

    assert_eq!(output.reasoning_content, "thinking");
    assert_eq!(output.reasoning_signature.as_deref(), Some("sig_abc"));
}

/// #520: the empty-string marker permanently invalidates the signature for
/// the stream (multi-block/redacted turns), even if another one follows.
#[tokio::test]
async fn consume_llm_stream_honors_signature_invalidation_marker() {
    let stream = build_stream(vec![
        Ok(LLMChunk::ReasoningSignature("sig_first".to_string())),
        Ok(LLMChunk::ReasoningSignature(String::new())),
        Ok(LLMChunk::ReasoningSignature("sig_late".to_string())),
        Ok(LLMChunk::Done),
    ]);

    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(8);
    let output = consume_llm_stream(
        stream,
        &event_tx,
        &CancellationToken::new(),
        "session-sig-invalid",
    )
    .await
    .expect("stream should succeed");

    assert_eq!(
        output.reasoning_signature, None,
        "invalidation is permanent for the stream"
    );
}

#[tokio::test]
async fn consume_llm_stream_silent_does_not_emit_events() {
    let stream = build_stream(vec![
        Ok(LLMChunk::Token("hello".to_string())),
        Ok(LLMChunk::Done),
    ]);

    let output = consume_llm_stream_silent(stream, &CancellationToken::new(), "session-2")
        .await
        .expect("silent stream should succeed");

    assert!(output.response_id.is_none());
    assert_eq!(output.content, "hello");
    assert!(output.reasoning_content.is_empty());
    assert_eq!(output.token_count, 5);
    assert!(output.tool_calls.is_empty());
}

#[tokio::test]
async fn consume_llm_stream_records_provider_usage_snapshots_without_double_counting() {
    let usage = LLMChunk::ProviderUsage {
        input_tokens: Some(101),
        output_tokens: Some(37),
        total_tokens: Some(138),
        reasoning_tokens: Some(11),
        cache_creation_input_tokens: Some(0),
        cache_read_input_tokens: Some(29),
        cache_write_input_tokens: Some(64),
    };
    let stream = build_stream(vec![
        Ok(usage.clone()),
        // Repeated cumulative snapshots are idempotent, not additive.
        Ok(usage),
        // Omitted fields must not invent or erase authoritative totals.
        Ok(LLMChunk::ProviderUsage {
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
            reasoning_tokens: None,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            cache_write_input_tokens: None,
        }),
        Ok(LLMChunk::Done),
    ]);

    let output =
        consume_llm_stream_silent(stream, &CancellationToken::new(), "session-provider-usage")
            .await
            .expect("stream should succeed");

    assert_eq!(output.input_tokens, 72);
    assert_eq!(output.output_tokens, 37);
    assert_eq!(output.thinking_tokens, 11);
    assert_eq!(output.cache_creation_input_tokens, 0);
    assert_eq!(output.cache_read_input_tokens, 29);
    assert_eq!(
        output.provider_usage,
        Some(ProviderUsageSnapshot {
            input_tokens: Some(101),
            output_tokens: Some(37),
            total_tokens: Some(138),
            reasoning_tokens: Some(11),
            cache_creation_input_tokens: Some(0),
            cache_read_input_tokens: Some(29),
            cache_write_input_tokens: Some(64),
        })
    );
}

#[tokio::test]
async fn provider_usage_snapshot_distinguishes_omitted_from_zero_for_every_field() {
    let omitted = consume_llm_stream_silent(
        build_stream(vec![
            Ok(LLMChunk::ProviderUsage {
                input_tokens: None,
                output_tokens: None,
                total_tokens: None,
                reasoning_tokens: None,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                cache_write_input_tokens: None,
            }),
            Ok(LLMChunk::Done),
        ]),
        &CancellationToken::new(),
        "session-provider-usage-omitted",
    )
    .await
    .expect("omitted snapshot");
    assert_eq!(
        omitted.provider_usage,
        Some(ProviderUsageSnapshot::default())
    );

    let explicit_zero = consume_llm_stream_silent(
        build_stream(vec![
            Ok(LLMChunk::ProviderUsage {
                input_tokens: Some(9),
                output_tokens: Some(9),
                total_tokens: Some(18),
                reasoning_tokens: Some(9),
                cache_creation_input_tokens: Some(9),
                cache_read_input_tokens: Some(9),
                cache_write_input_tokens: Some(9),
            }),
            Ok(LLMChunk::ProviderUsage {
                input_tokens: Some(0),
                output_tokens: Some(0),
                total_tokens: Some(0),
                reasoning_tokens: Some(0),
                cache_creation_input_tokens: Some(0),
                cache_read_input_tokens: Some(0),
                cache_write_input_tokens: Some(0),
            }),
            Ok(LLMChunk::Done),
        ]),
        &CancellationToken::new(),
        "session-provider-usage-zero",
    )
    .await
    .expect("zero snapshot");
    assert_eq!(
        explicit_zero.provider_usage,
        Some(ProviderUsageSnapshot {
            input_tokens: Some(0),
            output_tokens: Some(0),
            total_tokens: Some(0),
            reasoning_tokens: Some(0),
            cache_creation_input_tokens: Some(0),
            cache_read_input_tokens: Some(0),
            cache_write_input_tokens: Some(0),
        })
    );
}

#[tokio::test]
async fn provider_output_and_reasoning_reconcile_independently_in_both_orders() {
    let cases = [
        (Some(120), Some(20), 120, 20),
        (Some(0), Some(0), 0, 0),
        (None, Some(20), 56, 20),
        (Some(120), None, 120, 78),
    ];

    for (case_index, (provider_output, provider_reasoning, expected_output, expected_reasoning)) in
        cases.into_iter().enumerate()
    {
        for provider_first in [true, false] {
            let provider = LLMChunk::ProviderUsage {
                input_tokens: None,
                output_tokens: provider_output,
                total_tokens: None,
                reasoning_tokens: provider_reasoning,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                cache_write_input_tokens: None,
            };
            let legacy = LLMChunk::UsageSummary {
                output_tokens: 56,
                thinking_tokens: 78,
            };
            let chunks = if provider_first {
                vec![provider, legacy, LLMChunk::Done]
            } else {
                vec![legacy, provider, LLMChunk::Done]
            };
            let output = consume_llm_stream_silent(
                build_stream(chunks.into_iter().map(Ok).collect()),
                &CancellationToken::new(),
                &format!("session-provider-output-{case_index}-{provider_first}"),
            )
            .await
            .expect("mixed provider and legacy usage");

            assert_eq!(output.output_tokens, expected_output);
            assert_eq!(output.thinking_tokens, expected_reasoning);
            assert_eq!(
                output.provider_usage,
                Some(ProviderUsageSnapshot {
                    input_tokens: None,
                    output_tokens: provider_output,
                    total_tokens: None,
                    reasoning_tokens: provider_reasoning,
                    cache_creation_input_tokens: None,
                    cache_read_input_tokens: None,
                    cache_write_input_tokens: None,
                })
            );

            let log_record = crate::token_usage_log::TokenUsageRecord::new(
                "2026-07-29T00:00:00Z".to_string(),
                "session-provider-output",
                "test-model",
                "openai",
                1,
                None,
                output.cache_creation_input_tokens,
                output.cache_read_input_tokens,
                output
                    .provider_usage
                    .and_then(|usage| usage.cache_write_input_tokens)
                    .unwrap_or(0),
                output.input_tokens,
                output.output_tokens,
                output.thinking_tokens,
            );
            assert_eq!(log_record.output_tokens, expected_output);
            assert_eq!(log_record.thinking_tokens, expected_reasoning);
        }
    }
}

#[tokio::test]
async fn provider_cache_reconciles_without_input_total_in_both_orders() {
    let cases = [
        (None, Some(50), 11, 50),
        (Some(0), Some(0), 0, 0),
        (Some(7), None, 7, 50),
    ];

    for (case_index, (provider_creation, provider_read, expected_creation, expected_read)) in
        cases.into_iter().enumerate()
    {
        for provider_first in [true, false] {
            let provider = LLMChunk::ProviderUsage {
                input_tokens: None,
                output_tokens: None,
                total_tokens: None,
                reasoning_tokens: None,
                cache_creation_input_tokens: provider_creation,
                cache_read_input_tokens: provider_read,
                cache_write_input_tokens: None,
            };
            let legacy = LLMChunk::CacheUsage {
                cache_creation_input_tokens: 11,
                cache_read_input_tokens: 50,
                input_tokens: 80,
            };
            let chunks = if provider_first {
                vec![provider, legacy, LLMChunk::Done]
            } else {
                vec![legacy, provider, LLMChunk::Done]
            };
            let output = consume_llm_stream_silent(
                build_stream(chunks.into_iter().map(Ok).collect()),
                &CancellationToken::new(),
                &format!("session-provider-cache-{case_index}-{provider_first}"),
            )
            .await
            .expect("mixed provider and legacy cache usage");

            assert_eq!(output.input_tokens, 80);
            assert_eq!(
                output.cache_creation_input_tokens, expected_creation,
                "provider creation availability must be order-independent"
            );
            assert_eq!(
                output.cache_read_input_tokens, expected_read,
                "provider read availability must be order-independent"
            );
            assert_eq!(
                output.provider_usage,
                Some(ProviderUsageSnapshot {
                    input_tokens: None,
                    output_tokens: None,
                    total_tokens: None,
                    reasoning_tokens: None,
                    cache_creation_input_tokens: provider_creation,
                    cache_read_input_tokens: provider_read,
                    cache_write_input_tokens: None,
                })
            );

            let log_record = crate::token_usage_log::TokenUsageRecord::new(
                "2026-07-29T00:00:00Z".to_string(),
                "session-provider-cache",
                "test-model",
                "openai",
                1,
                None,
                output.cache_creation_input_tokens,
                output.cache_read_input_tokens,
                output
                    .provider_usage
                    .and_then(|usage| usage.cache_write_input_tokens)
                    .unwrap_or(0),
                output.input_tokens,
                output.output_tokens,
                output.thinking_tokens,
            );
            assert_eq!(log_record.cache_creation_input_tokens, expected_creation);
            assert_eq!(log_record.cache_read_input_tokens, expected_read);
        }
    }
}

#[tokio::test]
async fn openai_chat_usage_parser_reaches_stream_output_once() {
    let usage = parse_openai_compat_sse_data_strict(
        r#"{"id":"chatcmpl_1","choices":[],"usage":{"prompt_tokens":1000,"completion_tokens":120,"prompt_tokens_details":{"cached_tokens":768},"completion_tokens_details":{"reasoning_tokens":20}}}"#,
    )
    .expect("chat usage chunk");
    assert!(matches!(usage, LLMChunk::ProviderUsage { .. }));

    let stream = build_stream(vec![Ok(usage), Ok(LLMChunk::Done)]);
    let output = consume_llm_stream_silent(
        stream,
        &CancellationToken::new(),
        "session-openai-chat-usage",
    )
    .await
    .expect("stream should succeed");

    assert_eq!(output.input_tokens, 232);
    assert_eq!(output.output_tokens, 120);
    assert_eq!(output.thinking_tokens, 20);
    assert_eq!(output.cache_creation_input_tokens, 0);
    assert_eq!(output.cache_read_input_tokens, 768);
    assert_eq!(
        output.provider_usage.and_then(|usage| usage.input_tokens),
        Some(1000)
    );
}

#[tokio::test]
async fn openai_responses_completed_usage_reaches_stream_output_once() {
    let mut parser = ResponsesSseParser::new();
    let chunks = parser
        .handle_event_multi(
            "response.completed",
            r#"{"type":"response.completed","response":{"id":"resp_usage","output":[{"id":"msg_usage","type":"message","content":[{"type":"output_text","text":"answer"}]}],"usage":{"input_tokens":55,"output_tokens":21,"input_tokens_details":{"cached_tokens":13},"output_tokens_details":{"reasoning_tokens":8}}}}"#,
        )
        .expect("responses completed event");

    assert_eq!(
        chunks
            .iter()
            .filter(|chunk| matches!(chunk, LLMChunk::ProviderUsage { .. }))
            .count(),
        1
    );
    assert!(chunks
        .iter()
        .all(|chunk| !matches!(chunk, LLMChunk::CacheUsage { .. })));

    let stream = build_stream(chunks.into_iter().map(Ok).collect());
    let output = consume_llm_stream_silent(
        stream,
        &CancellationToken::new(),
        "session-openai-responses-usage",
    )
    .await
    .expect("stream should succeed");

    assert_eq!(output.response_id.as_deref(), Some("resp_usage"));
    assert_eq!(output.content, "answer");
    assert_eq!(output.input_tokens, 42);
    assert_eq!(output.output_tokens, 21);
    assert_eq!(output.thinking_tokens, 8);
    assert_eq!(output.cache_creation_input_tokens, 0);
    assert_eq!(output.cache_read_input_tokens, 13);
    assert_eq!(
        output.provider_usage.and_then(|usage| usage.input_tokens),
        Some(55)
    );
}

#[tokio::test]
async fn provider_total_clamps_flat_cache_subset_without_mutating_raw_snapshot() {
    let aggregate_overflow = consume_llm_stream_silent(
        build_stream(vec![
            Ok(LLMChunk::ProviderUsage {
                input_tokens: Some(100),
                output_tokens: None,
                total_tokens: None,
                reasoning_tokens: None,
                cache_creation_input_tokens: Some(50),
                cache_read_input_tokens: Some(80),
                cache_write_input_tokens: None,
            }),
            Ok(LLMChunk::Done),
        ]),
        &CancellationToken::new(),
        "session-provider-cache-overflow",
    )
    .await
    .expect("aggregate cache overflow");

    assert_eq!(aggregate_overflow.input_tokens, 0);
    assert_eq!(aggregate_overflow.cache_read_input_tokens, 80);
    assert_eq!(aggregate_overflow.cache_creation_input_tokens, 20);
    assert_eq!(
        aggregate_overflow.input_tokens
            + aggregate_overflow.cache_read_input_tokens
            + aggregate_overflow.cache_creation_input_tokens,
        100
    );
    assert_eq!(
        aggregate_overflow.provider_usage,
        Some(ProviderUsageSnapshot {
            input_tokens: Some(100),
            output_tokens: None,
            total_tokens: None,
            reasoning_tokens: None,
            cache_creation_input_tokens: Some(50),
            cache_read_input_tokens: Some(80),
            cache_write_input_tokens: None,
        }),
        "raw anomalous provider values remain available for diagnostics"
    );

    let cache_exceeds_input = consume_llm_stream_silent(
        build_stream(vec![
            Ok(LLMChunk::ProviderUsage {
                input_tokens: Some(100),
                output_tokens: None,
                total_tokens: None,
                reasoning_tokens: None,
                cache_creation_input_tokens: Some(30),
                cache_read_input_tokens: Some(120),
                cache_write_input_tokens: None,
            }),
            Ok(LLMChunk::Done),
        ]),
        &CancellationToken::new(),
        "session-provider-cache-exceeds-input",
    )
    .await
    .expect("cache exceeds input");

    assert_eq!(cache_exceeds_input.input_tokens, 0);
    assert_eq!(cache_exceeds_input.cache_read_input_tokens, 100);
    assert_eq!(cache_exceeds_input.cache_creation_input_tokens, 0);
    assert_eq!(
        cache_exceeds_input.input_tokens
            + cache_exceeds_input.cache_read_input_tokens
            + cache_exceeds_input.cache_creation_input_tokens,
        100,
        "normalized compatibility fields never exceed authoritative input"
    );
}

#[tokio::test]
async fn consume_llm_stream_returns_single_prefix_stream_error_message() {
    let stream = build_stream(vec![Err(LLMError::Stream(
        "Transport error: error decoding response body".to_string(),
    ))]);

    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(4);
    let err =
        match consume_llm_stream(stream, &event_tx, &CancellationToken::new(), "session-3").await {
            Ok(_) => panic!("stream should fail"),
            Err(err) => err,
        };

    match err {
        AgentError::LLM(message) => {
            assert_eq!(
                message,
                "Stream error: Transport error: error decoding response body"
            );
            assert!(!message.starts_with("Stream error: Stream error:"));
        }
        other => panic!("expected AgentError::LLM, got {other:?}"),
    }

    assert!(matches!(event_rx.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn consume_llm_stream_aborts_already_cancelled_stalled_stream() {
    // A provider stream that never yields and never ends. Before the `select!`
    // fix, `.next().await` would block forever; now a cancelled token must
    // return promptly instead of hanging.
    let stream: LLMStream = Box::pin(stream::pending());
    let cancel = CancellationToken::new();
    cancel.cancel();

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        consume_llm_stream_silent(stream, &cancel, "session-cancelled"),
    )
    .await
    .expect("must not hang: cancellation should interrupt the stalled stream");

    assert!(matches!(result, Err(AgentError::Cancelled)));
}

#[tokio::test]
async fn consume_llm_stream_interrupts_blocked_next_on_mid_stream_cancel() {
    // Proves a *blocked* `stream.next().await` (not just between chunks) is
    // interrupted when the token is cancelled while the consume call is running.
    let stream: LLMStream = Box::pin(stream::pending());
    let cancel = CancellationToken::new();
    let canceller = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        canceller.cancel();
    });

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        consume_llm_stream_silent(stream, &cancel, "session-cancel-mid"),
    )
    .await
    .expect("must not hang: mid-stream cancellation should interrupt the blocked next()");

    assert!(matches!(result, Err(AgentError::Cancelled)));
}

#[tokio::test(start_paused = true)]
async fn stalled_stream_bootstrap_times_out_before_response_headers() {
    let context = timeout_context(2, 20, 20);
    let result = await_stream_bootstrap(
        std::future::pending::<()>(),
        &CancellationToken::new(),
        "session-bootstrap-timeout",
        &context,
    )
    .await;

    let timeout = match result {
        Err(AgentError::StreamTimeout(timeout)) => timeout,
        Err(other) => panic!("expected bootstrap StreamTimeout, got {other:?}"),
        Ok(_) => panic!("expected bootstrap StreamTimeout, got success"),
    };
    assert_eq!(timeout.phase(), StreamTimeoutPhase::Bootstrap);
    assert!(timeout.retry_safe());
    assert!(!timeout.semantic_output_started());
    let message = timeout.to_string();
    assert!(message.contains("phase=bootstrap"));
    assert!(message.contains("deadline_ms=2000"));
    assert!(message.contains("provider=test-provider"));
    assert!(message.contains("model=test-model"));
    assert!(message.contains("last_transport_ms_ago=2000"));
    assert!(message.contains("last_semantic_ms_ago=never"));
    assert!(message.contains("retry_safe=true"));
}

#[tokio::test(start_paused = true)]
async fn auxiliary_stream_timeout_does_not_authorize_turn_replay() {
    let context = StreamTimeoutContext::new(
        StreamTimeoutConfig {
            transport_idle_timeout_secs: 2,
            first_semantic_timeout_secs: 20,
            semantic_idle_timeout_secs: 20,
        },
        Some("aux-provider"),
        Some("aux-model"),
    )
    .begin_request();
    let result = await_stream_bootstrap(
        std::future::pending::<()>(),
        &CancellationToken::new(),
        "session-aux-bootstrap-timeout",
        &context,
    )
    .await;

    let timeout = match result {
        Err(AgentError::StreamTimeout(timeout)) => timeout,
        Err(other) => panic!("expected bootstrap StreamTimeout, got {other:?}"),
        Ok(_) => panic!("expected bootstrap StreamTimeout, got success"),
    };
    assert_eq!(timeout.phase(), StreamTimeoutPhase::Bootstrap);
    assert!(!timeout.retry_safe());
    assert!(!timeout.semantic_output_started());
    assert!(timeout.to_string().contains("retry_safe=false"));
}

#[tokio::test(start_paused = true)]
async fn bootstrap_time_counts_toward_first_semantic_deadline() {
    let started_at = tokio::time::Instant::now();
    let context = timeout_context(10, 2, 20).begin_request();
    await_stream_bootstrap(
        async { tokio::time::sleep(Duration::from_secs(1)).await },
        &CancellationToken::new(),
        "session-bootstrap-semantic-budget",
        &context,
    )
    .await
    .expect("bootstrap should complete inside the transport deadline");

    let result = consume_llm_stream_internal(
        Box::pin(stream::pending()),
        None,
        &CancellationToken::new(),
        "session-bootstrap-semantic-budget",
        &context,
    )
    .await;

    let timeout = match result {
        Err(AgentError::StreamTimeout(timeout)) => timeout,
        Err(other) => panic!("expected first-semantic StreamTimeout, got {other:?}"),
        Ok(_) => panic!("expected first-semantic StreamTimeout, got success"),
    };
    assert_eq!(timeout.phase(), StreamTimeoutPhase::FirstSemantic);
    assert!(timeout.retry_safe());
    assert_eq!(started_at.elapsed(), Duration::from_secs(2));
}

#[tokio::test(start_paused = true)]
async fn truly_silent_transport_times_out_with_actionable_diagnostic() {
    let stream: LLMStream = Box::pin(stream::pending());
    let context = timeout_context(2, 20, 20);

    let result = consume_llm_stream_internal(
        stream,
        None,
        &CancellationToken::new(),
        "session-transport-timeout",
        &context,
    )
    .await;

    let timeout = match result {
        Err(AgentError::StreamTimeout(timeout)) => timeout,
        Err(other) => panic!("expected transport StreamTimeout, got {other:?}"),
        Ok(_) => panic!("expected transport StreamTimeout, got success"),
    };
    assert_eq!(timeout.phase(), StreamTimeoutPhase::TransportIdle);
    assert!(timeout.retry_safe());
    let message = timeout.to_string();
    assert!(message.contains("phase=transport_idle"));
    assert!(message.contains("deadline_ms=2000"));
    assert!(message.contains("provider=test-provider"));
    assert!(message.contains("model=test-model"));
    assert!(message.contains("last_transport_ms_ago=2000"));
    assert!(message.contains("last_semantic_ms_ago=never"));
    assert!(message.contains("semantic_output_started=false"));
    assert!(message.contains("retry_safe=true"));
    assert!(!message.contains("prompt"));
}

#[tokio::test(start_paused = true)]
async fn stream_stall_after_semantic_output_is_not_retry_safe() {
    let stream: LLMStream = Box::pin(
        stream::once(async { Ok::<_, LLMError>(LLMChunk::Token("first".to_string())) })
            .chain(stream::pending()),
    );
    let context = timeout_context(2, 20, 20);

    let result = consume_llm_stream_internal(
        stream,
        None,
        &CancellationToken::new(),
        "session-timeout",
        &context,
    )
    .await;

    let timeout = match result {
        Err(AgentError::StreamTimeout(timeout)) => timeout,
        Err(other) => panic!("expected transport StreamTimeout, got {other:?}"),
        Ok(_) => panic!("expected transport StreamTimeout, got success"),
    };
    assert_eq!(timeout.phase(), StreamTimeoutPhase::TransportIdle);
    assert!(!timeout.retry_safe());
    let message = timeout.to_string();
    assert!(message.contains("phase=transport_idle"));
    assert!(message.contains("semantic_output_started=true"));
    assert!(message.contains("retry_safe=false"));
}

#[tokio::test(start_paused = true)]
async fn transport_keepalives_allow_first_semantic_output_after_120_seconds() {
    let stream: LLMStream = Box::pin(stream::unfold(0u8, |step| async move {
        match step {
            0..=4 => {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Some((Ok::<_, LLMError>(LLMChunk::TransportActivity), step + 1))
            }
            5 => {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Some((Ok::<_, LLMError>(LLMChunk::Token("late".to_string())), 6))
            }
            6 => Some((Ok::<_, LLMError>(LLMChunk::Done), 7)),
            _ => None,
        }
    }));
    let context = timeout_context(60, 240, 60);

    let output = consume_llm_stream_internal(
        stream,
        None,
        &CancellationToken::new(),
        "session-keepalive",
        &context,
    )
    .await
    .expect("stream should succeed");

    assert_eq!(output.content, "late");
}

#[tokio::test(start_paused = true)]
async fn transport_keepalives_allow_midstream_semantic_gap_after_120_seconds() {
    let stream: LLMStream = Box::pin(
        stream::once(async { Ok::<_, LLMError>(LLMChunk::Token("first".to_string())) }).chain(
            stream::unfold(0u8, |step| async move {
                match step {
                    0..=4 => {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        Some((Ok::<_, LLMError>(LLMChunk::TransportActivity), step + 1))
                    }
                    5 => {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        Some((Ok::<_, LLMError>(LLMChunk::Token("second".to_string())), 6))
                    }
                    6 => Some((Ok::<_, LLMError>(LLMChunk::Done), 7)),
                    _ => None,
                }
            }),
        ),
    );
    let context = timeout_context(60, 240, 240);

    let output = consume_llm_stream_internal(
        stream,
        None,
        &CancellationToken::new(),
        "session-midstream-keepalive",
        &context,
    )
    .await
    .expect("live stream should survive a 180-second semantic gap");

    assert_eq!(output.content, "firstsecond");
}

#[tokio::test(start_paused = true)]
async fn sse_comment_heartbeats_survive_transport_timeout_until_responses_completion() {
    let chunks = vec![
        (
            Duration::ZERO,
            bytes::Bytes::from_static(
                b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":\"first\"}\n\n",
            ),
        ),
        (
            Duration::from_secs(30),
            bytes::Bytes::from_static(b": keep-alive\n\n"),
        ),
        (
            Duration::from_secs(30),
            bytes::Bytes::from_static(b": keep-alive\n\n"),
        ),
        (
            Duration::from_secs(30),
            bytes::Bytes::from_static(b": keep-alive\n\n"),
        ),
        (
            Duration::from_secs(30),
            bytes::Bytes::from_static(b": keep-alive\n\n"),
        ),
        (
            Duration::from_secs(30),
            bytes::Bytes::from_static(
                b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":\"second\"}\n\n",
            ),
        ),
        (
            Duration::ZERO,
            bytes::Bytes::from_static(
                b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_done\"}}\n\n",
            ),
        ),
    ];
    let body_stream = stream::unfold(chunks.into_iter(), |mut chunks| async move {
        let (delay, chunk) = chunks.next()?;
        tokio::time::sleep(delay).await;
        Some((Ok::<_, std::io::Error>(chunk), chunks))
    });
    let response = reqwest::Response::from(
        http::Response::builder()
            .status(200)
            .header("content-type", "text/event-stream")
            .body(reqwest::Body::wrap_stream(body_stream))
            .expect("http response"),
    );
    let mut parser = ResponsesSseParser::new();
    let stream = llm_stream_from_sse_multi_requiring_done(
        response,
        move |event, data| parser.handle_event_multi(event, data),
        "OpenAI Responses",
    );
    let context = timeout_context(45, 200, 200);

    let output = consume_llm_stream_internal(
        stream,
        None,
        &CancellationToken::new(),
        "session-comment-heartbeat",
        &context,
    )
    .await
    .expect("comment heartbeats should preserve transport liveness");

    assert_eq!(output.content, "firstsecond");
    assert_eq!(output.response_id.as_deref(), Some("resp_done"));
}

#[tokio::test(start_paused = true)]
async fn keepalives_do_not_make_first_semantic_deadline_unbounded() {
    let stream: LLMStream = Box::pin(stream::unfold((), |_| async {
        tokio::time::sleep(Duration::from_secs(20)).await;
        Some((Ok::<_, LLMError>(LLMChunk::TransportActivity), ()))
    }));
    let context = timeout_context(60, 120, 120);

    let result = consume_llm_stream_internal(
        stream,
        None,
        &CancellationToken::new(),
        "session-first-semantic",
        &context,
    )
    .await;

    let timeout = match result {
        Err(AgentError::StreamTimeout(timeout)) => timeout,
        Err(other) => panic!("expected first-semantic StreamTimeout, got {other:?}"),
        Ok(_) => panic!("expected first-semantic StreamTimeout, got success"),
    };
    assert_eq!(timeout.phase(), StreamTimeoutPhase::FirstSemantic);
    assert!(timeout.retry_safe());
    let message = timeout.to_string();
    assert!(message.contains("phase=first_semantic"));
    assert!(message.contains("semantic_output_started=false"));
}

#[tokio::test(start_paused = true)]
async fn keepalives_do_not_hide_midstream_semantic_stall() {
    let stream: LLMStream = Box::pin(
        stream::once(async { Ok::<_, LLMError>(LLMChunk::Token("first".to_string())) }).chain(
            stream::unfold((), |_| async {
                tokio::time::sleep(Duration::from_secs(20)).await;
                Some((Ok::<_, LLMError>(LLMChunk::TransportActivity), ()))
            }),
        ),
    );
    let context = timeout_context(60, 120, 90);

    let result = consume_llm_stream_internal(
        stream,
        None,
        &CancellationToken::new(),
        "session-semantic-stall",
        &context,
    )
    .await;

    let timeout = match result {
        Err(AgentError::StreamTimeout(timeout)) => timeout,
        Err(other) => panic!("expected semantic-idle StreamTimeout, got {other:?}"),
        Ok(_) => panic!("expected semantic-idle StreamTimeout, got success"),
    };
    assert_eq!(timeout.phase(), StreamTimeoutPhase::SemanticIdle);
    assert!(!timeout.retry_safe());
    let message = timeout.to_string();
    assert!(message.contains("phase=semantic_idle"));
    assert!(message.contains("semantic_output_started=true"));
    assert!(message.contains("retry_safe=false"));
}

#[tokio::test]
async fn consume_llm_stream_continues_when_subscriber_disconnects() {
    // Issue #23: when the subscriber drops its receiver, every token send
    // fails. Previously this was `let _ = event_tx.send(...).await;`, so the
    // failure was invisible (no log). The send must not panic and — because
    // accumulation happens *before* the forward — the stream must still
    // complete and return its full content. The failure path now emits a warn
    // instead of silently swallowing; the await (backpressure) semantics are
    // preserved (no try_send / timeout drop is introduced).
    let stream = build_stream(vec![
        Ok(LLMChunk::ReasoningToken("think".to_string())),
        Ok(LLMChunk::Token("hello".to_string())),
        Ok(LLMChunk::Token("world".to_string())),
        Ok(LLMChunk::Done),
    ]);

    // Create the channel and immediately drop the receiver, modelling a
    // subscriber that disconnected before streaming began. A capacity of 1 is
    // deliberate: it forces every send straight onto the disconnected path.
    let (event_tx, event_rx) = mpsc::channel::<AgentEvent>(1);
    drop(event_rx);

    // Must not panic, hang, or error despite every event send failing.
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        consume_llm_stream(
            stream,
            &event_tx,
            &CancellationToken::new(),
            "session-disconnect",
        ),
    )
    .await
    .expect("stream must complete when the subscriber is disconnected, not hang")
    .expect("stream should succeed even though event sends fail");

    // Content is accumulated into `state` before the forward attempt, so it is
    // fully preserved regardless of the event channel state. This is the key
    // behavioral guarantee: a dropped subscriber must never corrupt the run.
    assert_eq!(output.reasoning_content, "think");
    assert_eq!(output.content, "helloworld");
    assert_eq!(output.token_count, 10);
}
