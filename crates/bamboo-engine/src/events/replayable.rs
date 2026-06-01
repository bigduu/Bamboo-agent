//! Replayable session-event publishing infrastructure.
//!
//! This module owns the **single canonical path** for publishing UI-replayable
//! session events. Every replayable event must go through
//! [`publish_replayable_session_event`] so that:
//!
//! 1. The event is cached on the session's runner for late-subscriber replay
//!    *before* any subscriber sees it on the live broadcast channel.
//! 2. The cache-then-broadcast ordering is uniform across all writers, so
//!    reconnecting clients can never miss an event that earlier subscribers
//!    received.
//!
//! ## Invariant
//!
//! No code in the workspace may pair `runner.push_critical_event` with
//! `sender.send` outside this helper or the server's `spawn_event_forwarder`
//! (in `handlers::agent::execute::runtime::events`).
//! Hand-rolling the pair has historically led to inverted ordering (broadcast
//! first, cache second), which leaves a small window where a late subscriber
//! receives the live event but the cache is still empty.
//!
//! ## When to use this helper
//!
//! Use this helper for **synchronous** writers that hold an
//! [`AgentSessionContext`] directly — e.g. HTTP handlers (`patch_session`,
//! `regenerate_title`) and background tasks (`title_gen`). The runtime
//! forwarder (`spawn_event_forwarder`) handles the equivalent ordering for
//! events that flow through the engine's mpsc channel.

use bamboo_agent_core::AgentEvent;

use crate::app_context::AgentSessionContext;

/// Publishes a replayable session event with the correct cache-then-broadcast
/// ordering. See module docs for the invariant.
///
/// Order is fixed and **must not** be changed:
///
/// 1. `runner.push_critical_event(event.clone())` — populate the late-subscriber
///    replay cache while the event is still un-broadcast.
/// 2. `sender.send(event)` — broadcast to all live subscribers.
///
/// If no runner exists for `session_id` (the session has not started yet, or
/// has already terminated), the cache step is silently skipped and the event
/// is still broadcast. This matches the semantics of the runtime forwarder.
pub async fn publish_replayable_session_event(
    ctx: &dyn AgentSessionContext,
    session_id: &str,
    event: AgentEvent,
) {
    {
        let mut runners = ctx.agent_runners().write().await;
        if let Some(runner) = runners.get_mut(session_id) {
            runner.push_critical_event(event.clone());
        }
    }

    // Mirror onto the account-wide change feed (sequenced + journaled). The
    // sink filters ephemeral events internally; metadata events published here
    // (title/pinned) are durable and will be sequenced.
    ctx.account_sink().record(Some(session_id), &event);

    let sender = ctx.get_session_event_sender(session_id).await;
    let _ = sender.send(event);
}

