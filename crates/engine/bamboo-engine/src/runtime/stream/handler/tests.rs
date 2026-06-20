use std::time::Duration;

use futures::{stream, StreamExt};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_agent_core::{AgentError, AgentEvent};
use bamboo_llm::provider::LLMError;
use bamboo_llm::{LLMChunk, LLMStream};

use super::consume::consume_llm_stream_internal;
use super::{consume_llm_stream, consume_llm_stream_silent};

fn build_stream(items: Vec<bamboo_llm::provider::Result<LLMChunk>>) -> LLMStream {
    Box::pin(stream::iter(items))
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

#[tokio::test]
async fn consume_llm_stream_times_out_when_stream_stalls_after_first_chunk() {
    // Issue #28: a stream that yields one chunk, then never yields again and
    // never closes, models a provider connection that stalls mid-stream. The
    // inter-chunk idle deadline must trip and return a StreamTimeout error
    // instead of hanging forever.
    let stream: LLMStream = Box::pin(
        stream::once(async { Ok::<_, LLMError>(LLMChunk::Token("first".to_string())) })
            .chain(stream::pending()),
    );

    let idle_timeout = Duration::from_millis(80);
    let started = std::time::Instant::now();

    let result = tokio::time::timeout(
        // Generous outer bound; the idle timeout must trip well before this.
        Duration::from_secs(2),
        consume_llm_stream_internal(
            stream,
            None,
            &CancellationToken::new(),
            "session-timeout",
            idle_timeout,
        ),
    )
    .await
    .expect("idle timeout must interrupt the stalled stream, not hang");

    let elapsed = started.elapsed();
    match result {
        Err(AgentError::StreamTimeout(_)) => {}
        Err(other) => panic!("expected AgentError::StreamTimeout, got Err({other:?})"),
        Ok(_) => panic!("expected AgentError::StreamTimeout, got Ok(_)"),
    }

    // The first chunk arrives immediately, so the idle timer effectively starts
    // after it. The timeout should fire ~80ms later. A mild lower bound guards
    // against an "instant timeout" regression; the upper bound guards against
    // the timeout being ignored.
    assert!(
        elapsed >= Duration::from_millis(50),
        "timeout fired too early ({elapsed:?}): the first chunk should reset the idle timer"
    );
    assert!(
        elapsed < Duration::from_millis(800),
        "timeout fired too late ({elapsed:?}): the stalled stream was not interrupted in time"
    );
}

#[tokio::test]
async fn consume_llm_stream_idle_timeout_does_not_affect_healthy_stream() {
    // Issue #28: each chunk arrives well within the idle window (25ms << 80ms),
    // but the *total* stream duration (~150ms) far exceeds the idle timeout.
    // Because the deadline resets on every received chunk, a legitimately long
    // stream that keeps producing data must complete normally — the idle
    // timeout only fires on a true stall (no chunk at all).
    let idle_timeout = Duration::from_millis(80);
    let per_chunk_delay = Duration::from_millis(25);
    let chunk_count: u32 = 6;

    let stream: LLMStream = Box::pin(stream::unfold(0u32, move |i| async move {
        if i >= chunk_count {
            return None;
        }
        tokio::time::sleep(per_chunk_delay).await;
        Some((
            Ok::<_, LLMError>(LLMChunk::Token(format!("chunk-{i}"))),
            i + 1,
        ))
    }));

    let output = tokio::time::timeout(
        Duration::from_secs(3),
        consume_llm_stream_internal(
            stream,
            None,
            &CancellationToken::new(),
            "session-healthy",
            idle_timeout,
        ),
    )
    .await
    .expect("healthy stream must complete without timing out")
    .expect("stream should succeed");

    assert_eq!(output.content, "chunk-0chunk-1chunk-2chunk-3chunk-4chunk-5");
    // `token_count` is the total character count of the stream, not the chunk
    // count: 6 chunks × 7 chars ("chunk-N") = 42.
    assert_eq!(output.token_count, output.content.chars().count());
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
