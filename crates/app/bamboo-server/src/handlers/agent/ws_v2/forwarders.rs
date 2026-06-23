//! Per-channel forwarder tasks for the v2 WS multiplex.
//!
//! Each subscribed channel runs its OWN task with its OWN broadcast receiver,
//! pushing [`ServerEnvelope`]s onto a single shared `mpsc` that the driver
//! drains to the WS session. This per-forwarder design gives **per-channel lag
//! independence** (a slow/lagging channel only overruns its own broadcast ring,
//! never blocking another channel) — the §10-Q3 backpressure requirement.
//!
//! The driver keeps a `JoinHandle` per channel so `unsubscribe` (or teardown)
//! aborts exactly that forwarder, leaving no orphaned broadcast reader.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc};

use bamboo_agent_core::AgentEvent;
use bamboo_engine::events::change_feed::ChangeEvent;
use bamboo_engine::events::journal;

use super::envelope::{feed_reset_control, terminal_control, ServerEnvelope};
use crate::handlers::agent::events::Coalescer;
use crate::handlers::agent::stream::{plan_replay, ReplayPlan};

/// What the driver sends to the WS writer: a fully-built envelope's text frame.
pub(crate) type OutboundTx = mpsc::Sender<String>;

/// Push a server envelope onto the shared outbound mpsc. Returns `false` if the
/// driver-side receiver is gone (connection closing), so the forwarder stops.
async fn send_env(out: &OutboundTx, env: ServerEnvelope) -> bool {
    match env.to_text() {
        Some(text) => out.send(text).await.is_ok(),
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
                ServerEnvelope::control("feed", 0, feed_reset_control(from)),
            )
            .await
            {
                return;
            }
        }
        for ce in events {
            if !send_env(&out, feed_envelope(&ce)).await {
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
                    if !send_env(&out, feed_envelope(&ce)).await {
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
                            if !send_env(&out, feed_envelope(&ce)).await {
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
    out: OutboundTx,
    ch: String,
    mut receiver: broadcast::Receiver<AgentEvent>,
    budget_event_to_replay: Option<AgentEvent>,
    critical_events_to_replay: Vec<AgentEvent>,
    batch_ms: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let seq = AgentSeq::default();

        // Replay cached critical state events first (task list, sub-sessions, …),
        // then the last budget event — mirroring the v1 SSE replay order.
        for event in critical_events_to_replay {
            if !emit_agent_event(&out, &ch, &seq, event).await {
                return;
            }
        }
        if let Some(event) = budget_event_to_replay {
            if !emit_agent_event(&out, &ch, &seq, event).await {
                return;
            }
        }

        if batch_ms == 0 {
            // Fast path: every event emitted immediately, byte-for-byte (desktop
            // default), with no buffering. Terminal events close the channel.
            loop {
                match receiver.recv().await {
                    Ok(event) => {
                        let is_terminal = is_terminal_event(&event);
                        if !emit_agent_event(&out, &ch, &seq, event).await {
                            return;
                        }
                        if is_terminal {
                            let _ = send_env(
                                &out,
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
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
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

        loop {
            let sleep_until = flush_deadline
                .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(86_400));

            tokio::select! {
                _ = tokio::time::sleep_until(sleep_until), if flush_deadline.is_some() => {
                    if let Some(pending) = coalescer.take_pending() {
                        if !emit_agent_event(&out, &ch, &seq, pending).await {
                            return;
                        }
                    }
                    flush_deadline = None;
                }
                recv = receiver.recv() => {
                    match recv {
                        Ok(event) => {
                            let is_terminal = is_terminal_event(&event);
                            // Feed through the coalescer: it returns the ordered
                            // events to emit now (a flushed pending buffer then the
                            // new event when non-coalescible). Terminal events are
                            // non-coalescible, so any pending tokens flush first.
                            for out_event in coalescer.push(event) {
                                if !emit_agent_event(&out, &ch, &seq, out_event).await {
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
                                let _ = send_env(
                                    &out,
                                    ServerEnvelope::control(&ch, seq.next(), terminal_control("complete")),
                                )
                                .await;
                                return;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // A lag gap dropped intervening events; flush the
                            // pending buffer so we never merge tokens across the
                            // gap and fabricate adjacency.
                            if let Some(pending) = coalescer.take_pending() {
                                if !emit_agent_event(&out, &ch, &seq, pending).await {
                                    return;
                                }
                                flush_deadline = None;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            // Flush any buffered tokens before closing.
                            if let Some(pending) = coalescer.take_pending() {
                                let _ = emit_agent_event(&out, &ch, &seq, pending).await;
                            }
                            return;
                        }
                    }
                }
            }
        }
    })
}

/// Serialize an `AgentEvent` into an `{ch, seq, event}` envelope and push it.
async fn emit_agent_event(out: &OutboundTx, ch: &str, seq: &AgentSeq, event: AgentEvent) -> bool {
    let value = match serde_json::to_value(&event) {
        Ok(v) => v,
        // Skip an unserializable event but keep the forwarder alive (v1 parity).
        Err(_) => return true,
    };
    send_env(out, ServerEnvelope::event(ch, seq.next(), value)).await
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
}
