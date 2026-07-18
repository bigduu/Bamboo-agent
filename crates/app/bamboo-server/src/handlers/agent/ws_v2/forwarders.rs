//! Per-channel forwarder tasks for the v2 WS multiplex.
//!
//! Each subscribed channel runs its OWN task with its OWN broadcast receiver,
//! pushing [`ServerEnvelope`]s onto its OWN bounded `mpsc` queue. The driver
//! holds a `StreamMap<channel, ReceiverStream>` and drains every per-channel
//! queue with a fair merge (see below) to the WS session.
//!
//! Backpressure (RFC §10-Q3): the design now gives **per-channel independence at
//! BOTH ends**:
//!   1. *Source* — each forwarder owns its own broadcast receiver, so a
//!      slow/lagging channel only overruns its own broadcast ring and is never
//!      blocked at the source by another channel (the lag recovery is local).
//!   2. *Socket* — each forwarder owns its own bounded outbound queue
//!      (`OUTBOUND_BUFFER`), and the driver merges them with `tokio_stream`'s
//!      `StreamMap`, whose poll order ROTATES its start index every poll. So a
//!      sustained burst on one channel fills only its OWN queue (its forwarder
//!      then awaits on `send`, applying backpressure to THAT channel alone) and
//!      can no longer head-of-line another channel's frames at the socket — the
//!      shared-FIFO head-of-line point of the first cut is removed.
//!
//! Fairness guarantee (honest): the merge is **fair-ish, not strict
//! round-robin**. `StreamMap` polls all ready per-channel queues starting from a
//! randomized index each poll, so over time no channel is systematically starved, and a
//! flooding channel cannot monopolize the socket while another has frames ready.
//! It does NOT guarantee exact 1:1 interleaving or any latency bound; it
//! guarantees starvation-freedom and that per-channel backpressure stays local.
//!
//! Ordering WITHIN a channel is preserved end-to-end: a single forwarder pushes
//! to a single FIFO `mpsc`, and `StreamMap` drains each inner stream in order; it
//! only interleaves ACROSS channels.
//!
//! The driver keeps a `JoinHandle` per channel AND the matching queue receiver in
//! the `StreamMap`, both keyed by the channel id, so `unsubscribe` (or teardown)
//! aborts exactly that forwarder AND drops its queue, leaving no orphaned
//! broadcast reader and no stale queued frame.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc};

use bamboo_agent_core::AgentEvent;
use bamboo_engine::events::change_feed::ChangeEvent;
use bamboo_engine::events::journal;

use actix_web::web;

use super::envelope::{
    feed_reset_control, gap_control, terminal_control, Encoding, OutFrame, ServerEnvelope,
};
use crate::app_state::{AgentStatus, AppState};
use crate::handlers::agent::events::{has_running_child, terminal_event_if_ready, Coalescer};
use crate::handlers::agent::stream::{plan_replay, ReplayPlan};

/// What the driver sends to the WS writer: an already-encoded frame (a JSON text
/// frame or a MessagePack binary frame), tagged so the driver picks
/// `session.text` vs `session.binary` (v2-P3, #181). The forwarder encodes per
/// the connection's [`Encoding`] up front, keeping the final encode out of the
/// driver's hot select loop.
pub(crate) type OutboundTx = mpsc::Sender<OutFrame>;

/// Encode a server envelope per `encoding` and push it onto the shared outbound
/// mpsc. Returns `false` if the driver-side receiver is gone (connection
/// closing), so the forwarder stops.
async fn send_env(out: &OutboundTx, encoding: Encoding, env: ServerEnvelope) -> bool {
    match env.encode(encoding) {
        Some(frame) => out.send(frame).await.is_ok(),
        // A serialization failure is per-event; skip it but keep the forwarder
        // alive (matches the v1 SSE `serde_json::to_string(...).ok()` discipline).
        None => true,
    }
}

/// Spawn the `feed` forwarder.
///
/// Replicates the v1 SSE feed's subscribe-first → replay → live-skip → re-seek
/// discipline **exactly** (it reuses the same [`plan_replay`] / journal reads),
/// so a resuming client sees every event after its cursor exactly once with no
/// duplication across the handoff. The caller MUST have already subscribed
/// (`receiver`) before computing `latest_at_start`, so events written during
/// replay are buffered in the ring (no gap).
pub(crate) fn spawn_feed_forwarder(
    out: OutboundTx,
    encoding: Encoding,
    mut receiver: broadcast::Receiver<Arc<ChangeEvent>>,
    events_dir: PathBuf,
    since: u64,
    latest_at_start: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Phase A: replay the durable journal from the cursor (with a feed_reset
        // directive when the cursor predates the retained window).
        let ReplayPlan {
            reset_from,
            events,
            last_replayed,
        } = plan_replay(&events_dir, since, latest_at_start);
        let mut last_replayed = last_replayed;

        if let Some(from) = reset_from {
            // seq 0 on a control frame: the reset itself carries no feed seq; the
            // client resyncs via REST and the live tail serves anything newer.
            if !send_env(
                &out,
                encoding,
                ServerEnvelope::control("feed", 0, feed_reset_control(from)),
            )
            .await
            {
                return;
            }
        }
        for ce in events {
            if !send_env(&out, encoding, feed_envelope(&ce)).await {
                return;
            }
        }

        // Phase B: live tail with overlap-dedupe and lagged re-seek.
        loop {
            match receiver.recv().await {
                Ok(ce) => {
                    if ce.seq <= last_replayed {
                        continue; // dedupe the replay/live overlap
                    }
                    if !send_env(&out, encoding, feed_envelope(&ce)).await {
                        return;
                    }
                    last_replayed = ce.seq;
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // Ring overran during a slow consumer; recover the gap from
                    // the durable journal (the backstop), then continue live.
                    if let Ok(events) = journal::read_since(&events_dir, last_replayed) {
                        for ce in events {
                            if ce.seq <= last_replayed {
                                continue;
                            }
                            if !send_env(&out, encoding, feed_envelope(&ce)).await {
                                return;
                            }
                            last_replayed = ce.seq;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}

/// Build a `feed` envelope: `seq` = the `ChangeEvent.seq`, and the WHOLE
/// `ChangeEvent` is serialized as `event` to preserve its schema byte-for-byte.
fn feed_envelope(ce: &ChangeEvent) -> ServerEnvelope {
    let event = serde_json::to_value(ce).unwrap_or(serde_json::Value::Null);
    ServerEnvelope::event("feed", ce.seq, event)
}

/// Per-session monotonic envelope-seq counter for an `agent.{sid}` channel.
/// `AgentEvent` carries no seq of its own, so the forwarder mints one.
#[derive(Default)]
pub(crate) struct AgentSeq(AtomicU64);

impl AgentSeq {
    /// Return the next seq (1-based, strictly increasing).
    pub(crate) fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// Spawn an `agent.{sid}` forwarder.
///
/// Mirrors the v1 per-session SSE path: replay cached critical state events
/// (and the last budget event) first, then live-tail. Reuses the v1
/// [`Coalescer`] for token batching when `batch_ms > 0` (default 0 = no
/// coalescing). On any terminal event it emits a `terminal` control frame.
///
/// `ch` is the full channel id (`agent.{sid}`).
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_agent_forwarder(
    state: web::Data<AppState>,
    session_id: String,
    out: OutboundTx,
    encoding: Encoding,
    ch: String,
    mut receiver: broadcast::Receiver<AgentEvent>,
    budget_event_to_replay: Option<AgentEvent>,
    critical_events_to_replay: Vec<AgentEvent>,
    batch_ms: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let seq = AgentSeq::default();

        // Held for the whole forwarder task's lifetime: dropped on graceful
        // completion (terminal/closed) OR on `JoinHandle::abort()` (channel
        // unsubscribe / connection teardown both abort this task — Tokio
        // still runs its local drop glue), decrementing the session's
        // live-watcher count. Never read — only its RAII drop matters.
        let _watcher_guard = crate::app_state::watchers::WatcherGuard::new(
            state.session_watchers.clone(),
            &session_id,
        );

        // Replay cached critical state events first (task list, sub-sessions, …),
        // then the last budget event — mirroring the v1 SSE replay order.
        for event in critical_events_to_replay {
            if !emit_agent_event(&out, encoding, &ch, &seq, event).await {
                return;
            }
        }
        if let Some(event) = budget_event_to_replay {
            if !emit_agent_event(&out, encoding, &ch, &seq, event).await {
                return;
            }
        }

        if batch_ms == 0 {
            // Fast path: every event emitted immediately, byte-for-byte (desktop
            // default), with no buffering.
            //
            // Terminal handling mirrors the v1 SSE stream: the parent's own turn
            // can finish while its child sub-agents are still running. Children
            // outlive the parent turn and forward their progress/preview onto THIS
            // session's broadcast, so we must NOT close the channel on the parent
            // terminal while descendants remain — doing so would silently drop
            // every later child event. Hold the channel open and emit the
            // `terminal` control only once no running child is left.
            let mut awaiting_children = false;
            loop {
                match receiver.recv().await {
                    Ok(event) => {
                        let is_terminal = is_terminal_event(&event);
                        let is_child_completed =
                            matches!(event, AgentEvent::SubAgentCompleted { .. });
                        if !emit_agent_event(&out, encoding, &ch, &seq, event).await {
                            return;
                        }
                        if is_terminal {
                            if has_running_child(&state, &session_id).await {
                                awaiting_children = true;
                                continue;
                            }
                            let _ = send_env(
                                &out,
                                encoding,
                                ServerEnvelope::control(
                                    &ch,
                                    seq.next(),
                                    terminal_control("complete"),
                                ),
                            )
                            .await;
                            return;
                        }
                        // A child just finished: if it was the last running child
                        // after the parent already terminated, close now.
                        if awaiting_children
                            && is_child_completed
                            && !has_running_child(&state, &session_id).await
                        {
                            let _ = send_env(
                                &out,
                                encoding,
                                ServerEnvelope::control(
                                    &ch,
                                    seq.next(),
                                    terminal_control("complete"),
                                ),
                            )
                            .await;
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        match handle_agent_lag(
                            &state,
                            &session_id,
                            &out,
                            encoding,
                            &ch,
                            &seq,
                            skipped,
                            awaiting_children,
                        )
                        .await
                        {
                            LagOutcome::Continue => continue,
                            LagOutcome::Stop | LagOutcome::Disconnected => return,
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }

        // Coalescing path (`batch_ms > 0`): reuse the v1 Coalescer so token-class
        // events of the same channel merge into one frame, flushed on a different
        // event / the deadline / a terminal / a lag gap / close. Order is
        // preserved by the Coalescer (push returns the flushed pending FIRST).
        let mut coalescer = Coalescer::default();
        let flush_window = Duration::from_millis(batch_ms);
        let mut flush_deadline: Option<tokio::time::Instant> = None;
        // See the fast-path comment: keep the channel open after the parent
        // terminal while child sub-agents still run.
        let mut awaiting_children = false;

        loop {
            let sleep_until = flush_deadline
                .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(86_400));

            tokio::select! {
                _ = tokio::time::sleep_until(sleep_until), if flush_deadline.is_some() => {
                    if let Some(pending) = coalescer.take_pending() {
                        if !emit_agent_event(&out, encoding, &ch, &seq, pending).await {
                            return;
                        }
                    }
                    flush_deadline = None;
                }
                recv = receiver.recv() => {
                    match recv {
                        Ok(event) => {
                            let is_terminal = is_terminal_event(&event);
                            let is_child_completed =
                                matches!(event, AgentEvent::SubAgentCompleted { .. });
                            // Feed through the coalescer: it returns the ordered
                            // events to emit now (a flushed pending buffer then the
                            // new event when non-coalescible). Terminal events are
                            // non-coalescible, so any pending tokens flush first.
                            for out_event in coalescer.push(event) {
                                if !emit_agent_event(&out, encoding, &ch, &seq, out_event).await {
                                    return;
                                }
                            }
                            if coalescer.has_pending() {
                                if flush_deadline.is_none() {
                                    flush_deadline =
                                        Some(tokio::time::Instant::now() + flush_window);
                                }
                            } else {
                                flush_deadline = None;
                            }
                            if is_terminal {
                                // Keep the channel open while children run (v1
                                // parity); the pending buffer was already flushed
                                // above since a terminal is non-coalescible.
                                if has_running_child(&state, &session_id).await {
                                    awaiting_children = true;
                                    continue;
                                }
                                let _ = send_env(
                                    &out,
                                    encoding,
                                    ServerEnvelope::control(&ch, seq.next(), terminal_control("complete")),
                                )
                                .await;
                                return;
                            }
                            if awaiting_children
                                && is_child_completed
                                && !has_running_child(&state, &session_id).await
                            {
                                let _ = send_env(
                                    &out,
                                    encoding,
                                    ServerEnvelope::control(&ch, seq.next(), terminal_control("complete")),
                                )
                                .await;
                                return;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            // A lag gap dropped intervening events; flush the
                            // pending buffer so we never merge tokens across the
                            // gap and fabricate adjacency.
                            if let Some(pending) = coalescer.take_pending() {
                                if !emit_agent_event(&out, encoding, &ch, &seq, pending).await {
                                    return;
                                }
                                flush_deadline = None;
                            }
                            match handle_agent_lag(
                                &state,
                                &session_id,
                                &out,
                                encoding,
                                &ch,
                                &seq,
                                skipped,
                                awaiting_children,
                            )
                            .await
                            {
                                LagOutcome::Continue => {}
                                LagOutcome::Stop | LagOutcome::Disconnected => return,
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            // Flush any buffered tokens before closing.
                            if let Some(pending) = coalescer.take_pending() {
                                let _ = emit_agent_event(&out, encoding, &ch, &seq, pending).await;
                            }
                            return;
                        }
                    }
                }
            }
        }
    })
}

/// Spawn a one-shot `agent.{sid}` replay for a session that was already
/// terminal when the client subscribed. This mirrors the v1 SSE terminal
/// response: cached critical state, budget state, synthesized terminal event,
/// then exactly one terminal control before the per-channel queue closes.
pub(crate) fn spawn_agent_terminal_forwarder(
    out: OutboundTx,
    encoding: Encoding,
    ch: String,
    budget_event_to_replay: Option<AgentEvent>,
    critical_events_to_replay: Vec<AgentEvent>,
    terminal_event: AgentEvent,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let seq = AgentSeq::default();
        for event in critical_events_to_replay {
            if !emit_agent_event(&out, encoding, &ch, &seq, event).await {
                return;
            }
        }
        if let Some(event) = budget_event_to_replay {
            if !emit_agent_event(&out, encoding, &ch, &seq, event).await {
                return;
            }
        }
        if !emit_agent_event(&out, encoding, &ch, &seq, terminal_event).await {
            return;
        }
        let _ = send_env(
            &out,
            encoding,
            ServerEnvelope::control(&ch, seq.next(), terminal_control("complete")),
        )
        .await;
    })
}

/// What the agent forwarder should do after a broadcast-lag recovery attempt.
#[derive(Debug, PartialEq, Eq)]
enum LagOutcome {
    /// Keep live-tailing (the run is still live, or the socket is closing).
    Continue,
    /// The lost gap swallowed the run's terminal; the synthesized terminal was
    /// emitted and the channel is done.
    Stop,
    /// The outbound queue is gone (connection closing) — stop silently.
    Disconnected,
}

/// Recover from a broadcast-ring overrun on an `agent.{sid}` channel (#543).
///
/// Agent events have NO durable journal (unlike the feed), so a lag gap is
/// unrecoverable data loss — the events are gone. What we CAN do:
///
/// 1. Tell the client: emit a `{type:"gap", skipped}` control so it reconciles
///    the session's authoritative state via REST instead of trusting a
///    transcript with a hole in it.
/// 2. Self-heal a swallowed terminal: if the runner is no longer `Running` and
///    the one-shot predicate ([`terminal_event_if_ready`] — not suspended, no
///    pending resume, no running child) says the run is over, the gap ate the
///    terminal event. Emit the synthesized terminal + the `terminal` control
///    and close, exactly as if the real one had been delivered — otherwise the
///    channel goes silent forever and the client shows the last tool call as
///    running indefinitely ON A HEALTHY SOCKET (the keepalive watchdog cannot
///    catch this: keepalives keep arriving).
///
/// `own_terminal_already_emitted` is the caller's `awaiting_children` state:
/// the parent's REAL terminal event already reached the client and the channel
/// is only held open for child sub-agents. If the gap then swallows the last
/// `SubAgentCompleted`, self-heal must send ONLY the `terminal` control —
/// mirroring the normal `is_child_completed && !has_running_child` close — and
/// never a second, synthesized terminal event for a session whose completion
/// the client already saw.
async fn handle_agent_lag(
    state: &web::Data<AppState>,
    session_id: &str,
    out: &OutboundTx,
    encoding: Encoding,
    ch: &str,
    seq: &AgentSeq,
    skipped: u64,
    own_terminal_already_emitted: bool,
) -> LagOutcome {
    tracing::warn!(
        "[{}] ws_v2 agent channel lagged: {} events lost to broadcast-ring overrun; \
         emitting gap control (client must reconcile via REST)",
        session_id,
        skipped
    );
    if !send_env(
        out,
        encoding,
        ServerEnvelope::control(ch, seq.next(), gap_control(skipped)),
    )
    .await
    {
        return LagOutcome::Disconnected;
    }

    let runner_status = {
        let runners = state.agent_runners.read().await;
        runners.get(session_id).map(|runner| runner.status.clone())
    };
    if matches!(runner_status, Some(AgentStatus::Running)) {
        return LagOutcome::Continue;
    }
    let Some(terminal_event) = terminal_event_if_ready(state, session_id, runner_status).await
    else {
        return LagOutcome::Continue;
    };

    tracing::warn!(
        "[{}] ws_v2 agent channel: lag gap swallowed the run's terminal \
         (own_terminal_already_emitted={}); closing the channel",
        session_id,
        own_terminal_already_emitted
    );
    // Only synthesize a terminal EVENT when the client never saw the real one.
    // In the awaiting-children window the parent's terminal was already
    // delivered — a second one would be a spurious duplicate completion.
    if !own_terminal_already_emitted
        && !emit_agent_event(out, encoding, ch, seq, terminal_event).await
    {
        return LagOutcome::Disconnected;
    }
    let _ = send_env(
        out,
        encoding,
        ServerEnvelope::control(ch, seq.next(), terminal_control("complete")),
    )
    .await;
    LagOutcome::Stop
}

/// Serialize an `AgentEvent` into an `{ch, seq, event}` envelope and push it,
/// encoded per `encoding`.
async fn emit_agent_event(
    out: &OutboundTx,
    encoding: Encoding,
    ch: &str,
    seq: &AgentSeq,
    event: AgentEvent,
) -> bool {
    let value = match serde_json::to_value(&event) {
        Ok(v) => v,
        // Skip an unserializable event but keep the forwarder alive (v1 parity).
        Err(_) => return true,
    };
    send_env(out, encoding, ServerEnvelope::event(ch, seq.next(), value)).await
}

/// Whether an agent event terminates the run (mirrors the v1 SSE predicate).
fn is_terminal_event(event: &AgentEvent) -> bool {
    matches!(
        event,
        AgentEvent::Complete { .. } | AgentEvent::Cancelled { .. } | AgentEvent::Error { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AgentRunner;

    #[test]
    fn agent_seq_is_monotonic_and_one_based() {
        let seq = AgentSeq::default();
        assert_eq!(seq.next(), 1);
        assert_eq!(seq.next(), 2);
        assert_eq!(seq.next(), 3);
    }

    /// The live-skip predicate used in the feed handoff drops `seq <= cursor`
    /// and keeps `seq > cursor`, so the replay/live overlap is deduped without
    /// dropping anything past the cursor.
    #[test]
    fn feed_live_skip_predicate() {
        let last_replayed = 1006u64;
        let skip = |seq: u64| seq <= last_replayed;
        assert!(skip(1005));
        assert!(skip(1006)); // boundary: already replayed → skip
        assert!(!skip(1007)); // first new event → keep
        assert!(!skip(2000));
    }

    /// Smoke test that the v1 `Coalescer` is reachable + behaves from the ws_v2
    /// module (proves the `pub(crate)` widening is wired, not just compiles).
    #[test]
    fn coalescer_reuse_smoke() {
        let mut c = Coalescer::default();
        assert!(c
            .push(AgentEvent::Token {
                content: "Hel".into()
            })
            .is_empty());
        assert!(c
            .push(AgentEvent::Token {
                content: "lo".into()
            })
            .is_empty());
        // A non-token event flushes the merged token first, in order.
        let out = c.push(AgentEvent::Complete {
            usage: Default::default(),
        });
        assert_eq!(out.len(), 2);
        match &out[0] {
            AgentEvent::Token { content } => assert_eq!(content, "Hello"),
            other => panic!("expected merged Token, got {other:?}"),
        }
        assert!(matches!(out[1], AgentEvent::Complete { .. }));
    }

    #[test]
    fn terminal_event_predicate() {
        assert!(is_terminal_event(&AgentEvent::Complete {
            usage: Default::default()
        }));
        assert!(is_terminal_event(&AgentEvent::Cancelled { message: None }));
        assert!(is_terminal_event(&AgentEvent::Error {
            message: "x".into()
        }));
        assert!(!is_terminal_event(&AgentEvent::Token {
            content: "x".into()
        }));
    }

    #[tokio::test]
    async fn completed_subscription_replays_state_then_one_terminal_control() {
        let (out_tx, mut out_rx) = mpsc::channel::<OutFrame>(64);
        let handle = spawn_agent_terminal_forwarder(
            out_tx,
            Encoding::Json,
            "agent.done".to_string(),
            None,
            vec![AgentEvent::Token {
                content: "critical-state".into(),
            }],
            AgentEvent::Complete {
                usage: Default::default(),
            },
        );

        let replay = next_json(&mut out_rx).await;
        assert_eq!(replay["seq"], 1);
        assert_eq!(replay["event"]["content"], "critical-state");
        let terminal_event = next_json(&mut out_rx).await;
        assert_eq!(terminal_event["seq"], 2);
        assert_eq!(terminal_event["event"]["type"], "complete");
        let terminal_control = next_json(&mut out_rx).await;
        assert_eq!(terminal_control["seq"], 3);
        assert_eq!(terminal_control["control"]["type"], "terminal");
        handle.await.expect("one-shot forwarder completes");
        assert!(
            out_rx.recv().await.is_none(),
            "terminal control is emitted once"
        );
    }

    #[test]
    fn feed_envelope_uses_change_event_seq_and_full_payload() {
        let ce = ChangeEvent {
            seq: 99,
            ts: chrono::Utc::now(),
            session_id: Some("s1".into()),
            event: AgentEvent::Token {
                content: "hi".into(),
            },
        };
        let env = feed_envelope(&ce);
        assert_eq!(env.seq, 99);
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(v["ch"], "feed");
        assert_eq!(v["seq"], 99);
        // The whole ChangeEvent (seq, ts, session_id, event) is preserved.
        assert_eq!(v["event"]["seq"], 99);
        assert_eq!(v["event"]["session_id"], "s1");
        assert_eq!(v["event"]["event"]["type"], "token");
    }

    // ── Broadcast-lag recovery (#543) ────────────────────────────────────────

    /// Receive + decode the next JSON frame from the forwarder's outbound queue.
    async fn next_json(rx: &mut mpsc::Receiver<OutFrame>) -> serde_json::Value {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("frame arrives before timeout")
            .expect("outbound queue still open")
        {
            OutFrame::Text(text) => serde_json::from_str(&text).expect("frame is JSON"),
            OutFrame::Binary(_) => panic!("JSON mode must not emit binary frames"),
        }
    }

    async fn test_state(session_id: &str) -> (web::Data<AppState>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let state = web::Data::new(AppState::new(tmp.path().to_path_buf()).await.unwrap());
        let mut session = bamboo_agent_core::Session::new(session_id, "test-model");
        // Last message = Assistant → does not prevent the one-shot terminal.
        session.add_message(bamboo_agent_core::Message::assistant("done", None));
        state.save_session(&mut session).await;
        (state, tmp)
    }

    /// Overflow a 4-slot ring with 10 events BEFORE the forwarder polls, so its
    /// first `recv()` yields `Lagged(6)` (the ring retains the newest 4). The
    /// sender is returned so the caller keeps the channel open.
    fn lagged_channel() -> (
        broadcast::Sender<AgentEvent>,
        broadcast::Receiver<AgentEvent>,
    ) {
        let (tx, rx) = broadcast::channel::<AgentEvent>(4);
        for i in 0..10 {
            let _ = tx.send(AgentEvent::Token {
                content: format!("t{i}"),
            });
        }
        (tx, rx)
    }

    /// The gap swallowed the run's terminal (no runner → the run is over, and
    /// the session state allows the one-shot terminal): the forwarder must emit
    /// the gap control, a SYNTHESIZED terminal event, and the terminal control,
    /// then close — never strand the client on a silent channel (#543).
    #[tokio::test]
    async fn lag_emits_gap_control_and_synthesized_terminal_when_run_finished() {
        let (state, _tmp) = test_state("lag-done").await;
        let (_tx, rx) = lagged_channel();
        let (out_tx, mut out_rx) = mpsc::channel::<OutFrame>(64);
        let _handle = spawn_agent_forwarder(
            state.clone(),
            "lag-done".to_string(),
            out_tx,
            Encoding::Json,
            "agent.lag-done".to_string(),
            rx,
            None,
            Vec::new(),
            0,
        );

        let gap = next_json(&mut out_rx).await;
        assert_eq!(gap["ch"], "agent.lag-done");
        assert_eq!(gap["control"]["type"], "gap");
        assert_eq!(gap["control"]["skipped"], 6);

        let terminal_event = next_json(&mut out_rx).await;
        assert_eq!(terminal_event["event"]["type"], "complete");

        let terminal = next_json(&mut out_rx).await;
        assert_eq!(terminal["control"]["type"], "terminal");

        // The forwarder is done: its sender is dropped, closing the queue.
        assert!(
            tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
                .await
                .expect("close arrives before timeout")
                .is_none(),
            "channel must close after the synthesized terminal"
        );
    }

    /// The run is still live (runner `Running`): the forwarder emits the gap
    /// control so the client reconciles, then KEEPS tailing — the retained ring
    /// events and later live events still flow.
    #[tokio::test]
    async fn lag_emits_gap_control_and_keeps_tailing_when_runner_running() {
        let (state, _tmp) = test_state("lag-live").await;
        {
            let mut runners = state.agent_runners.write().await;
            let runner = runners
                .entry("lag-live".to_string())
                .or_insert_with(AgentRunner::new);
            runner.status = AgentStatus::Running;
        }
        let (tx, rx) = lagged_channel();
        let (out_tx, mut out_rx) = mpsc::channel::<OutFrame>(64);
        let _handle = spawn_agent_forwarder(
            state.clone(),
            "lag-live".to_string(),
            out_tx,
            Encoding::Json,
            "agent.lag-live".to_string(),
            rx,
            None,
            Vec::new(),
            0,
        );

        let gap = next_json(&mut out_rx).await;
        assert_eq!(gap["control"]["type"], "gap");
        assert_eq!(gap["control"]["skipped"], 6);

        // The ring's retained tail (t6..t9) still flows after the gap.
        for i in 6..10 {
            let frame = next_json(&mut out_rx).await;
            assert_eq!(frame["event"]["type"], "token");
            assert_eq!(frame["event"]["content"], format!("t{i}"));
        }

        // And the channel is still LIVE: a later event arrives too.
        let _ = tx.send(AgentEvent::Token {
            content: "after-gap".to_string(),
        });
        let frame = next_json(&mut out_rx).await;
        assert_eq!(frame["event"]["content"], "after-gap");
    }

    /// Review regression (#544): a lag inside the awaiting-children window —
    /// the parent's REAL terminal was already delivered, the channel is only
    /// held open for a child sub-agent, and the gap swallows the final
    /// `SubAgentCompleted`. Self-heal must close with ONLY the `terminal`
    /// control (mirroring the normal child-completion close) and never emit a
    /// second, synthesized terminal event for a completion the client already
    /// saw.
    #[tokio::test]
    async fn lag_during_awaiting_children_closes_without_duplicate_terminal_event() {
        let (state, _tmp) = test_state("lag-parent").await;
        // A REAL child session (kind=Child, root=parent) with a Running runner,
        // so the parent's terminal holds the channel open (awaiting_children).
        let mut child =
            bamboo_agent_core::Session::new_child("lag-child", "lag-parent", "test-model", "child");
        state.save_session(&mut child).await;
        {
            let mut runners = state.agent_runners.write().await;
            let runner = runners
                .entry("lag-child".to_string())
                .or_insert_with(AgentRunner::new);
            runner.status = AgentStatus::Running;
        }

        let (tx, rx) = broadcast::channel::<AgentEvent>(4);
        let (out_tx, mut out_rx) = mpsc::channel::<OutFrame>(64);
        let _handle = spawn_agent_forwarder(
            state.clone(),
            "lag-parent".to_string(),
            out_tx,
            Encoding::Json,
            "agent.lag-parent".to_string(),
            rx,
            None,
            Vec::new(),
            0,
        );

        // The parent's REAL terminal: emitted to the client, and the running
        // child holds the channel open (no terminal control yet).
        let _ = tx.send(AgentEvent::Complete {
            usage: Default::default(),
        });
        let real_terminal = next_json(&mut out_rx).await;
        assert_eq!(real_terminal["event"]["type"], "complete");

        // The child finishes: its runner goes away — but the SubAgentCompleted
        // that would have closed the channel is swallowed by a ring overrun.
        {
            let mut runners = state.agent_runners.write().await;
            runners.remove("lag-child");
        }
        // Overflow the 4-slot ring synchronously (current-thread test runtime:
        // the forwarder cannot run between these sends), so its next recv()
        // yields Lagged and the SubAgentCompleted inside the gap is lost.
        let _ = tx.send(AgentEvent::SubAgentCompleted {
            parent_session_id: "lag-parent".to_string(),
            child_session_id: "lag-child".to_string(),
            status: "completed".to_string(),
            error: None,
        });
        for i in 0..9 {
            let _ = tx.send(AgentEvent::Token {
                content: format!("child-tail-{i}"),
            });
        }

        // Recovery: the gap control, then ONLY the terminal control — no second
        // synthesized `complete` event.
        let gap = next_json(&mut out_rx).await;
        assert_eq!(gap["control"]["type"], "gap");
        let terminal = next_json(&mut out_rx).await;
        assert_eq!(
            terminal["control"]["type"], "terminal",
            "self-heal must close with the control only — a synthesized terminal \
             event here would be a duplicate completion: {terminal}"
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
                .await
                .expect("close arrives before timeout")
                .is_none(),
            "channel must close after the terminal control"
        );
    }
}
