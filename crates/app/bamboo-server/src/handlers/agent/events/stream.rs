use actix_web::http::header;
use actix_web::{web, HttpResponse};
use std::time::Duration;
use tokio::sync::broadcast;

use bamboo_agent_core::AgentEvent;

use crate::app_state::AppState;

use super::terminal::has_running_child;

/// Identifies which token-class channel a coalescible event belongs to.
///
/// Only `Token`, `ReasoningToken`, and `ToolToken` are coalescible. `ToolToken`
/// is keyed by `tool_call_id` so that interleaved output from different tool
/// calls never merges into the same frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CoalesceKey {
    Token,
    ReasoningToken,
    ToolToken(String),
}

/// Classifies an event as coalescible (returning its channel key) or not
/// (`None`). Non-token events must flush any pending buffer and be emitted
/// immediately, unchanged.
pub(crate) fn coalesce_key(event: &AgentEvent) -> Option<CoalesceKey> {
    match event {
        AgentEvent::Token { .. } => Some(CoalesceKey::Token),
        AgentEvent::ReasoningToken { .. } => Some(CoalesceKey::ReasoningToken),
        AgentEvent::ToolToken { tool_call_id, .. } => {
            Some(CoalesceKey::ToolToken(tool_call_id.clone()))
        }
        _ => None,
    }
}

/// Upper bound on the concatenated `content` of a single pending merged event.
///
/// The `batch_ms` window bounds coalescing latency, but not the *size* of the
/// buffer: a fast producer can emit an unbounded number of token chunks within
/// one window, all concatenated into one growing `String`. This cap force-flushes
/// the pending event once its content exceeds the threshold, so a pathological
/// token stream cannot amplify memory. 64 KiB is far larger than any real token
/// burst yet small enough to bound the allocation.
pub(crate) const MAX_PENDING_CONTENT_BYTES: usize = 64 * 1024;

/// Returns the byte length of a coalescible event's `content`, or 0 for
/// non-token events (which are never buffered).
pub(crate) fn pending_content_len(event: &AgentEvent) -> usize {
    match event {
        AgentEvent::Token { content }
        | AgentEvent::ReasoningToken { content }
        | AgentEvent::ToolToken { content, .. } => content.len(),
        _ => 0,
    }
}

/// Appends `content` from a same-key incoming token event onto the pending
/// merged event of the same variant. Callers guarantee both events share the
/// same [`CoalesceKey`], so the variants always match.
pub(crate) fn merge_token_into(pending: &mut AgentEvent, incoming: AgentEvent) {
    match (pending, incoming) {
        (AgentEvent::Token { content }, AgentEvent::Token { content: more })
        | (AgentEvent::ReasoningToken { content }, AgentEvent::ReasoningToken { content: more })
        | (AgentEvent::ToolToken { content, .. }, AgentEvent::ToolToken { content: more, .. }) => {
            content.push_str(&more);
        }
        // Unreachable: callers only merge events with identical keys.
        (_, _) => debug_assert!(false, "merge_token_into called with mismatched variants"),
    }
}

/// Pure, synchronous token-coalescing buffer.
///
/// Holds at most one pending merged token event. Consecutive token events of
/// the same channel ([`CoalesceKey`]) are concatenated into that single pending
/// event; any other event (or a token of a different channel) flushes the
/// pending event first so that **stream order is strictly preserved**.
///
/// With `batch_ms == 0` the caller never buffers (it calls [`Coalescer::push`]
/// and emits the returned events, but a `batch_ms == 0` window means the
/// deadline fires immediately so nothing accumulates — see `live_stream_response`).
#[derive(Default)]
pub(crate) struct Coalescer {
    pending: Option<AgentEvent>,
}

impl Coalescer {
    /// Feeds one event in. Returns the ordered list of events that must be
    /// emitted right now (a flushed pending event followed by the new event
    /// when the new event is non-coalescible). A coalescible event that merges
    /// into / starts the pending buffer yields an empty list — it stays buffered
    /// until a flush (different kind, deadline, terminal, heartbeat, or close).
    pub(crate) fn push(&mut self, event: AgentEvent) -> Vec<AgentEvent> {
        match coalesce_key(&event) {
            // Non-coalescible: flush pending first, then emit this event.
            None => {
                let mut out = Vec::new();
                if let Some(pending) = self.pending.take() {
                    out.push(pending);
                }
                out.push(event);
                out
            }
            // Coalescible: merge if the pending buffer has the same key,
            // otherwise flush the old buffer and start a new one.
            Some(key) => {
                let same_key = self
                    .pending
                    .as_ref()
                    .and_then(coalesce_key)
                    .is_some_and(|pk| pk == key);
                if same_key {
                    if let Some(pending) = self.pending.as_mut() {
                        merge_token_into(pending, event);
                        // Force-flush if the merged buffer has grown too large,
                        // so an unbounded token burst within one window cannot
                        // amplify memory. Order is preserved: the oversized
                        // buffer is emitted now, before any later event.
                        if pending_content_len(pending) >= MAX_PENDING_CONTENT_BYTES {
                            return self.pending.take().into_iter().collect();
                        }
                    }
                    Vec::new()
                } else {
                    let mut out = Vec::new();
                    if let Some(pending) = self.pending.take() {
                        out.push(pending);
                    }
                    self.pending = Some(event);
                    out
                }
            }
        }
    }

    /// Removes and returns the pending merged event, if any. Used to flush on a
    /// deadline tick, heartbeat, terminal event, or stream close so buffered
    /// content is never lost.
    pub(crate) fn take_pending(&mut self) -> Option<AgentEvent> {
        self.pending.take()
    }

    /// Whether a merged event is currently buffered.
    pub(crate) fn has_pending(&self) -> bool {
        self.pending.is_some()
    }
}

/// Serializes an event to an SSE `data:` frame, or `None` if serialization fails.
fn event_sse_data(event: &AgentEvent) -> Option<String> {
    serde_json::to_string(event)
        .ok()
        .map(|event_json| format!("data: {}\n\n", event_json))
}

pub(super) fn terminal_response(
    budget_event_to_replay: Option<AgentEvent>,
    critical_events_to_replay: Vec<AgentEvent>,
    terminal_event: AgentEvent,
) -> HttpResponse {
    HttpResponse::Ok()
        .append_header((header::CONTENT_TYPE, "text/event-stream; charset=utf-8"))
        .append_header((header::CACHE_CONTROL, "no-cache, no-transform"))
        .append_header((header::CONNECTION, "keep-alive"))
        .append_header(("X-Accel-Buffering", "no"))
        .streaming(async_stream::stream! {
            // Replay cached critical state events first (task list, sub-sessions, …).
            for event in &critical_events_to_replay {
                if let Some(sse_data) = as_sse_data(Some(event)) {
                    yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
                }
            }
            if let Some(sse_data) = as_sse_data(budget_event_to_replay.as_ref()) {
                yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
            }
            if let Ok(event_json) = serde_json::to_string(&terminal_event) {
                let sse_data = format!("data: {}\n\n", event_json);
                yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
            }
            yield Ok::<_, actix_web::Error>(web::Bytes::from(done_sse_data()));
        })
}

pub(super) fn live_stream_response(
    budget_event_to_replay: Option<AgentEvent>,
    critical_events_to_replay: Vec<AgentEvent>,
    mut receiver: broadcast::Receiver<AgentEvent>,
    state: web::Data<AppState>,
    session_id: String,
    batch_ms: u64,
) -> HttpResponse {
    HttpResponse::Ok()
        .append_header((header::CONTENT_TYPE, "text/event-stream; charset=utf-8"))
        .append_header((header::CACHE_CONTROL, "no-cache, no-transform"))
        .append_header((header::CONNECTION, "keep-alive"))
        .append_header(("X-Accel-Buffering", "no"))
        .streaming(async_stream::stream! {
            // Replay cached critical state events first (task list, sub-sessions, …).
            for event in &critical_events_to_replay {
                if let Some(sse_data) = as_sse_data(Some(event)) {
                    yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
                }
            }
            if let Some(sse_data) = as_sse_data(budget_event_to_replay.as_ref()) {
                yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
            }

            // Some platforms (notably desktop/webview stacks in release mode) can terminate idle
            // HTTP streams. Emit a small SSE comment heartbeat periodically to keep the
            // connection alive even when the agent is "thinking" (no tokens yet).
            let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
            // Skip the immediate tick.
            heartbeat.tick().await;

            // The parent's own turn can finish (emit a terminal event) while its
            // child actors are still running. Children are designed to outlive the
            // parent turn and forward their progress/preview onto THIS stream, so we
            // must NOT close it on the parent terminal while children remain — doing
            // so leaves child events broadcasting to a receiver-less channel. Hold
            // the terminal open and close only once no running child is left.
            let mut awaiting_children = false;

            // Token coalescing (v2-P0). When `batch_ms == 0` we take the legacy
            // fast path below — every event is emitted immediately, byte-for-byte
            // unchanged, with no buffering and no added latency (desktop default).
            //
            // When `batch_ms > 0` consecutive token-class events of the same
            // channel are merged into a single SSE frame, flushed when a
            // different event arrives, the `batch_ms` deadline elapses, a
            // heartbeat ticks, a terminal event is forwarded, or the stream
            // closes. This cuts per-token frame overhead on mobile.
            if batch_ms == 0 {
                loop {
                    tokio::select! {
                        _ = heartbeat.tick() => {
                            if awaiting_children && !has_running_child(&state, &session_id).await {
                                yield Ok::<_, actix_web::Error>(web::Bytes::from(done_sse_data()));
                                break;
                            }
                            yield Ok::<_, actix_web::Error>(web::Bytes::from(keepalive_sse_data()));
                        }
                        recv = receiver.recv() => {
                            match recv {
                                Ok(event) => {
                                    let is_terminal = is_terminal_event(&event);
                                    let is_child_completed =
                                        matches!(event, AgentEvent::SubAgentCompleted { .. });
                                    let Ok(event_json) = serde_json::to_string(&event) else {
                                        continue;
                                    };
                                    let sse_data = format!("data: {}\n\n", event_json);
                                    yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
                                    if is_terminal {
                                        // Forward the parent terminal, but keep the stream
                                        // alive while child actors are still running.
                                        if has_running_child(&state, &session_id).await {
                                            awaiting_children = true;
                                            continue;
                                        }
                                        yield Ok::<_, actix_web::Error>(web::Bytes::from(done_sse_data()));
                                        break;
                                    }
                                    // A child just finished: if it was the last running
                                    // child after the parent already terminated, close now.
                                    if awaiting_children
                                        && is_child_completed
                                        && !has_running_child(&state, &session_id).await
                                    {
                                        yield Ok::<_, actix_web::Error>(web::Bytes::from(done_sse_data()));
                                        break;
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(_skipped)) => {
                                    // Best-effort stream; late subscribers can open history.
                                    continue;
                                }
                                Err(broadcast::error::RecvError::Closed) => {
                                    // Should not happen for long-lived session senders, but exit cleanly.
                                    break;
                                }
                            }
                        }
                    }
                }
            } else {
                // Coalescing path (`batch_ms > 0`).
                let mut coalescer = Coalescer::default();
                // Bound buffering latency: when something is pending, flush no
                // later than `batch_ms` after it was first buffered. We arm the
                // deadline lazily (only while a buffer is pending) so an idle
                // stream never busy-loops on the flush timer.
                let flush_window = Duration::from_millis(batch_ms);
                let mut flush_deadline: Option<tokio::time::Instant> = None;

                loop {
                    // A far-future sleep keeps the `select!` arm well-typed when
                    // no buffer is pending; it is only polled to completion when
                    // a real deadline is armed.
                    let sleep_until = flush_deadline
                        .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(86_400));

                    tokio::select! {
                        _ = tokio::time::sleep_until(sleep_until), if flush_deadline.is_some() => {
                            if let Some(pending) = coalescer.take_pending() {
                                if let Some(sse_data) = event_sse_data(&pending) {
                                    yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
                                }
                            }
                            flush_deadline = None;
                        }
                        _ = heartbeat.tick() => {
                            // Flush any buffered tokens before the keepalive so
                            // ordering is preserved relative to the heartbeat.
                            if let Some(pending) = coalescer.take_pending() {
                                if let Some(sse_data) = event_sse_data(&pending) {
                                    yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
                                }
                                flush_deadline = None;
                            }
                            if awaiting_children && !has_running_child(&state, &session_id).await {
                                yield Ok::<_, actix_web::Error>(web::Bytes::from(done_sse_data()));
                                break;
                            }
                            yield Ok::<_, actix_web::Error>(web::Bytes::from(keepalive_sse_data()));
                        }
                        recv = receiver.recv() => {
                            match recv {
                                Ok(event) => {
                                    let is_terminal = is_terminal_event(&event);
                                    let is_child_completed =
                                        matches!(event, AgentEvent::SubAgentCompleted { .. });

                                    // Feed the event through the coalescer. This
                                    // returns the ordered events to emit now: a
                                    // flushed pending buffer (if the new event is a
                                    // different kind / channel) followed by the new
                                    // event itself when it is non-coalescible. A
                                    // coalescible event that merges/starts the buffer
                                    // returns nothing and stays buffered.
                                    for out in coalescer.push(event) {
                                        if let Some(sse_data) = event_sse_data(&out) {
                                            yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
                                        }
                                    }

                                    // Maintain the flush deadline: arm it when a
                                    // buffer becomes pending, clear it when drained.
                                    if coalescer.has_pending() {
                                        if flush_deadline.is_none() {
                                            flush_deadline =
                                                Some(tokio::time::Instant::now() + flush_window);
                                        }
                                    } else {
                                        flush_deadline = None;
                                    }

                                    if is_terminal {
                                        // Pending tokens were already flushed by
                                        // `push` (a terminal event is non-coalescible),
                                        // so the terminal frame is emitted in order.
                                        if has_running_child(&state, &session_id).await {
                                            awaiting_children = true;
                                            continue;
                                        }
                                        yield Ok::<_, actix_web::Error>(web::Bytes::from(done_sse_data()));
                                        break;
                                    }
                                    if awaiting_children
                                        && is_child_completed
                                        && !has_running_child(&state, &session_id).await
                                    {
                                        yield Ok::<_, actix_web::Error>(web::Bytes::from(done_sse_data()));
                                        break;
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(_skipped)) => {
                                    // A lag gap means intervening events were
                                    // dropped. Flush the pending buffer so we do
                                    // not merge tokens across the gap and fabricate
                                    // adjacency the producer never emitted.
                                    if let Some(pending) = coalescer.take_pending() {
                                        if let Some(sse_data) = event_sse_data(&pending) {
                                            yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
                                        }
                                        flush_deadline = None;
                                    }
                                    continue;
                                }
                                Err(broadcast::error::RecvError::Closed) => {
                                    // Flush any buffered tokens before closing so
                                    // content is never lost.
                                    if let Some(pending) = coalescer.take_pending() {
                                        if let Some(sse_data) = event_sse_data(&pending) {
                                            yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        })
}

fn as_sse_data(event: Option<&AgentEvent>) -> Option<String> {
    let event = event?;
    serde_json::to_string(event)
        .ok()
        .map(|event_json| format!("data: {}\n\n", event_json))
}

fn done_sse_data() -> &'static str {
    "data: [DONE]\n\n"
}

fn keepalive_sse_data() -> &'static str {
    "data: [KEEPALIVE]\n\n"
}

fn is_terminal_event(event: &AgentEvent) -> bool {
    matches!(
        event,
        AgentEvent::Complete { .. } | AgentEvent::Cancelled { .. } | AgentEvent::Error { .. }
    )
}

#[cfg(test)]
mod coalesce_tests {
    use super::*;
    use bamboo_agent_core::TokenUsage;

    fn token(content: &str) -> AgentEvent {
        AgentEvent::Token {
            content: content.to_string(),
        }
    }

    fn reasoning(content: &str) -> AgentEvent {
        AgentEvent::ReasoningToken {
            content: content.to_string(),
        }
    }

    fn tool_token(id: &str, content: &str) -> AgentEvent {
        AgentEvent::ToolToken {
            tool_call_id: id.to_string(),
            content: content.to_string(),
        }
    }

    fn tool_start(id: &str) -> AgentEvent {
        AgentEvent::ToolStart {
            tool_call_id: id.to_string(),
            tool_name: "Bash".to_string(),
            arguments: serde_json::json!({}),
        }
    }

    fn complete() -> AgentEvent {
        AgentEvent::Complete {
            usage: TokenUsage::default(),
        }
    }

    fn content_of(event: &AgentEvent) -> &str {
        match event {
            AgentEvent::Token { content }
            | AgentEvent::ReasoningToken { content }
            | AgentEvent::ToolToken { content, .. } => content,
            other => panic!("expected a token-class event, got {other:?}"),
        }
    }

    /// (a) Drive a full sequence through the coalescer, mirroring how the
    /// `batch_ms == 0` fast path would behave if it used `push` + immediate
    /// drain (it does not — it bypasses the coalescer entirely — but this
    /// documents that `push` never *hides* a non-coalescible event and never
    /// drops content). The `batch_ms == 0` zero-regression guarantee is
    /// asserted by code inspection: that branch is the verbatim legacy loop.
    #[test]
    fn push_then_flush_loses_nothing_and_preserves_order() {
        let mut c = Coalescer::default();
        let mut emitted: Vec<AgentEvent> = Vec::new();

        for ev in [token("a"), token("b"), tool_start("t1"), token("c")] {
            emitted.extend(c.push(ev));
        }
        if let Some(p) = c.take_pending() {
            emitted.push(p);
        }

        // a+b merged → "ab", then tool_start, then trailing "c" flushed.
        assert_eq!(emitted.len(), 3);
        assert_eq!(content_of(&emitted[0]), "ab");
        assert!(matches!(emitted[1], AgentEvent::ToolStart { .. }));
        assert_eq!(content_of(&emitted[2]), "c");
    }

    /// (b) Consecutive Token events merge into one with concatenated content.
    #[test]
    fn consecutive_tokens_merge() {
        let mut c = Coalescer::default();
        assert!(c.push(token("Hello")).is_empty());
        assert!(c.push(token(", ")).is_empty());
        assert!(c.push(token("world")).is_empty());

        let pending = c.take_pending().expect("buffer should hold merged token");
        assert!(matches!(pending, AgentEvent::Token { .. }));
        assert_eq!(content_of(&pending), "Hello, world");
        assert!(!c.has_pending());
    }

    /// ReasoningToken merges with ReasoningToken (same channel as Token logic).
    #[test]
    fn consecutive_reasoning_tokens_merge_but_not_with_token() {
        let mut c = Coalescer::default();
        assert!(c.push(reasoning("think")).is_empty());
        assert!(c.push(reasoning("ing")).is_empty());
        // A plain Token is a *different* channel → flushes reasoning first.
        let out = c.push(token("answer"));
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], AgentEvent::ReasoningToken { .. }));
        assert_eq!(content_of(&out[0]), "thinking");
        // The token is now pending.
        let pending = c.take_pending().unwrap();
        assert!(matches!(pending, AgentEvent::Token { .. }));
        assert_eq!(content_of(&pending), "answer");
    }

    /// (c) A non-token event flushes the pending buffer FIRST (order preserved).
    #[test]
    fn non_token_event_flushes_pending_first() {
        let mut c = Coalescer::default();
        assert!(c.push(token("partial")).is_empty());
        let out = c.push(tool_start("t1"));
        assert_eq!(out.len(), 2, "flushed token then the tool_start");
        assert!(matches!(out[0], AgentEvent::Token { .. }));
        assert_eq!(content_of(&out[0]), "partial");
        assert!(matches!(out[1], AgentEvent::ToolStart { .. }));
        assert!(!c.has_pending());
    }

    /// A non-token event with NO pending buffer passes straight through.
    #[test]
    fn non_token_event_without_pending_passes_through() {
        let mut c = Coalescer::default();
        let out = c.push(tool_start("t1"));
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], AgentEvent::ToolStart { .. }));
        assert!(!c.has_pending());
    }

    /// (d) ToolToken with a DIFFERENT tool_call_id does NOT merge — the old
    /// buffer flushes and a new one starts.
    #[test]
    fn tool_token_different_id_does_not_merge() {
        let mut c = Coalescer::default();
        assert!(c.push(tool_token("t1", "aa")).is_empty());
        assert!(c.push(tool_token("t1", "bb")).is_empty());
        // Different id → flush t1's merged buffer, start t2.
        let out = c.push(tool_token("t2", "cc"));
        assert_eq!(out.len(), 1);
        match &out[0] {
            AgentEvent::ToolToken {
                tool_call_id,
                content,
            } => {
                assert_eq!(tool_call_id, "t1");
                assert_eq!(content, "aabb");
            }
            other => panic!("expected ToolToken t1, got {other:?}"),
        }
        // t2 is pending with its own content.
        match c.take_pending().unwrap() {
            AgentEvent::ToolToken {
                tool_call_id,
                content,
            } => {
                assert_eq!(tool_call_id, "t2");
                assert_eq!(content, "cc");
            }
            other => panic!("expected ToolToken t2, got {other:?}"),
        }
    }

    /// Same-id ToolToken events DO merge.
    #[test]
    fn tool_token_same_id_merges() {
        let mut c = Coalescer::default();
        assert!(c.push(tool_token("t1", "x")).is_empty());
        assert!(c.push(tool_token("t1", "y")).is_empty());
        match c.take_pending().unwrap() {
            AgentEvent::ToolToken {
                tool_call_id,
                content,
            } => {
                assert_eq!(tool_call_id, "t1");
                assert_eq!(content, "xy");
            }
            other => panic!("expected ToolToken t1, got {other:?}"),
        }
    }

    /// (e) A terminal event flushes pending tokens before terminating. The
    /// coalescer treats a terminal (`Complete`) as a non-coalescible event, so
    /// `push` returns the flushed token THEN the terminal, in order — which is
    /// exactly the order the stream yields them before emitting `[DONE]`.
    #[test]
    fn terminal_event_flushes_pending_first() {
        let mut c = Coalescer::default();
        assert!(c.push(token("final answer")).is_empty());
        let out = c.push(complete());
        assert_eq!(out.len(), 2);
        assert!(matches!(out[0], AgentEvent::Token { .. }));
        assert_eq!(content_of(&out[0]), "final answer");
        assert!(matches!(out[1], AgentEvent::Complete { .. }));
        assert!(!c.has_pending());
    }

    /// The pending buffer force-flushes once its content exceeds
    /// `MAX_PENDING_CONTENT_BYTES`, so an unbounded token burst within one
    /// window cannot amplify memory. The flush happens on the `push` that
    /// crosses the threshold and preserves order (oldest content first).
    #[test]
    fn oversized_buffer_force_flushes() {
        let mut c = Coalescer::default();
        // Each chunk is half the cap; the second push crosses it.
        let chunk = "x".repeat(MAX_PENDING_CONTENT_BYTES / 2 + 1);
        assert!(
            c.push(token(&chunk)).is_empty(),
            "first chunk stays buffered (under cap)"
        );
        let out = c.push(token(&chunk));
        assert_eq!(out.len(), 1, "second chunk crosses the cap → force-flush");
        assert!(matches!(out[0], AgentEvent::Token { .. }));
        assert_eq!(content_of(&out[0]).len(), chunk.len() * 2);
        // Buffer is drained after the force-flush; subsequent tokens start fresh.
        assert!(!c.has_pending());
        assert!(c.push(token("next")).is_empty());
        assert_eq!(content_of(&c.take_pending().unwrap()), "next");
    }

    /// The merged event keeps the SAME serialized shape (a `Token`), not a new
    /// event type — the client schema is unchanged.
    #[test]
    fn merged_event_serializes_as_token() {
        let mut c = Coalescer::default();
        c.push(token("foo"));
        c.push(token("bar"));
        let merged = c.take_pending().unwrap();
        let value = serde_json::to_value(&merged).unwrap();
        assert_eq!(value["type"], "token");
        assert_eq!(value["content"], "foobar");
    }
}
