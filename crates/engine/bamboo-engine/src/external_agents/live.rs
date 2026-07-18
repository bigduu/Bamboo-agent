//! Live actor registry: in-band delivery to currently-running actor children.
//!
//! While `ActorChildRunner` drives a child over WebSocket, it registers a frame
//! sender here keyed by `child_session_id`. `send_message` (running, no
//! interrupt) consults this map: when the child is live, the message rides the
//! existing WS as a `ParentFrame::Message` and is admitted by the worker's
//! agent loop at its next round boundary — the same mechanism in-process
//! children use, extended across the process boundary. When the child is not
//! live, callers fall back to the durable `pending_injected_messages` queue.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use bamboo_agent_core::AgentEvent;
use bamboo_domain::poison::PoisonRecover;
use bamboo_subagent::proto::ParentFrame;
use tokio::sync::mpsc;

fn map() -> &'static Mutex<HashMap<String, mpsc::UnboundedSender<ParentFrame>>> {
    static MAP: OnceLock<Mutex<HashMap<String, mpsc::UnboundedSender<ParentFrame>>>> =
        OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Process-global registry of pending human-loop approval requests, keyed by
/// `child_id` → set of `request_id`s currently awaiting a decision. Only the
/// human-in-the-loop path (top orchestrator) registers here; trusted internal
/// paths (model-review, escalation-bridge) do NOT — so the external handler's
/// [`deliver_approval_checked`] correctly rejects any stray external POST aimed
/// at a request that isn't a genuinely-pending human-loop one.
#[derive(Clone)]
struct PendingApproval {
    parent_session_id: String,
    tool_name: String,
    permission: String,
    resource: String,
    created_at: String,
    version: u64,
    event_tx: mpsc::Sender<AgentEvent>,
}

fn pending() -> &'static Mutex<HashMap<(String, String), PendingApproval>> {
    static PENDING: OnceLock<Mutex<HashMap<(String, String), PendingApproval>>> = OnceLock::new();
    PENDING.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Record a `(child_id, request_id)` as a pending human-loop approval. Called
/// just before surfacing `ChildApprovalRequested` so an external POST can be
/// correlated against a genuinely-pending request.
pub fn register_pending_approval_observed(
    parent_session_id: &str,
    child_id: &str,
    request_id: &str,
    tool_name: &str,
    permission: &str,
    resource: &str,
    event_tx: mpsc::Sender<AgentEvent>,
) -> (u64, String) {
    let now = chrono::Utc::now();
    let version = now.timestamp_micros().max(0) as u64;
    let created_at = now.to_rfc3339();
    pending().lock().recover_poison().insert(
        (child_id.to_string(), request_id.to_string()),
        PendingApproval {
            parent_session_id: parent_session_id.to_string(),
            tool_name: tool_name.to_string(),
            permission: permission.to_string(),
            resource: resource.to_string(),
            created_at: created_at.clone(),
            version,
            event_tx,
        },
    );
    (version, created_at)
}

#[cfg(test)]
fn register_pending_approval(child_id: &str, request_id: &str) {
    let (event_tx, _rx) = mpsc::channel(1);
    let _ = register_pending_approval_observed(
        "test-parent",
        child_id,
        request_id,
        "test-tool",
        "test-permission",
        "test-resource",
        event_tx,
    );
}

/// One-shot consume of a `(child_id, request_id)` pending pair: remove it and
/// return whether it WAS present. A second call for the same pair returns
/// `false`, so a request can't be answered (or replayed) twice.
pub fn take_pending_approval(child_id: &str, request_id: &str) -> bool {
    pending()
        .lock()
        .recover_poison()
        .remove(&(child_id.to_string(), request_id.to_string()))
        .is_some()
}

/// Drop all pending approvals for a child (e.g. when its live connection ends).
pub fn clear_pending_approvals_for(child_id: &str) {
    let records: Vec<_> = {
        let mut guard = pending().lock().recover_poison();
        let keys: Vec<_> = guard
            .keys()
            .filter(|(child, _)| child == child_id)
            .cloned()
            .collect();
        keys.into_iter()
            .filter_map(|key| guard.remove(&key).map(|record| (key.1, record)))
            .collect()
    };
    for (request_id, record) in records {
        emit_resolution(
            child_id,
            &request_id,
            record,
            "delivery_failed",
            Some("child_disconnected"),
        );
    }
}

pub fn expire_pending_approval(child_id: &str, request_id: &str) -> bool {
    let record = pending()
        .lock()
        .recover_poison()
        .remove(&(child_id.to_string(), request_id.to_string()));
    let Some(record) = record else {
        return false;
    };
    emit_resolution(
        child_id,
        request_id,
        record,
        "expired",
        Some("approval_timeout"),
    );
    true
}

fn emit_resolution(
    child_id: &str,
    request_id: &str,
    record: PendingApproval,
    status: &str,
    reason: Option<&str>,
) {
    let now = chrono::Utc::now();
    let event = AgentEvent::ChildApprovalChanged {
        parent_session_id: record.parent_session_id,
        child_session_id: child_id.to_string(),
        request_id: request_id.to_string(),
        version: (now.timestamp_micros().max(0) as u64).max(record.version.saturating_add(1)),
        status: status.to_string(),
        reason: reason.map(str::to_string),
        tool_name: record.tool_name,
        permission: record.permission,
        resource: record.resource,
        created_at: record.created_at,
        resolved_at: Some(now.to_rfc3339()),
    };
    match record.event_tx.try_send(event) {
        Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
        Err(mpsc::error::TrySendError::Full(event)) => {
            let tx = record.event_tx;
            tokio::spawn(async move {
                let _ = tx.send(event).await;
            });
        }
    }
}

/// Validated external entry point: deliver an approval decision ONLY if the
/// `(child_id, request_id)` pair is currently pending. Consumes the pending
/// entry (one-shot) before delivering, so the same request can't be replayed,
/// and rejects (returns `false`) any `request_id` that isn't currently pending
/// — unknown, already-answered/timed-out, or a non-human-loop path
/// (model-review / escalation) that never registered. This is the entry the
/// external HTTP handler must use.
pub fn deliver_approval_checked(child_id: &str, request_id: &str, approved: bool) -> bool {
    let record = pending()
        .lock()
        .recover_poison()
        .remove(&(child_id.to_string(), request_id.to_string()));
    let Some(record) = record else {
        return false;
    };
    let delivered = deliver_approval(child_id, request_id, approved);
    let status = if delivered {
        if approved {
            "approved"
        } else {
            "denied"
        }
    } else {
        "delivery_failed"
    };
    emit_resolution(
        child_id,
        request_id,
        record,
        status,
        (!delivered).then_some("child_not_live"),
    );
    delivered
}

/// Unregisters the child on drop, so a panicking/returning runner can't leak
/// a stale sender.
pub struct LiveActorGuard {
    child_id: String,
}

impl Drop for LiveActorGuard {
    fn drop(&mut self) {
        map().lock().recover_poison().remove(&self.child_id);
        // A disconnecting child can't answer any still-pending approval — drop
        // them so a late external POST finds nothing pending and is rejected.
        clear_pending_approvals_for(&self.child_id);
    }
}

/// Register a live child's frame sender for the duration of its run.
pub fn register(child_id: &str, tx: mpsc::UnboundedSender<ParentFrame>) -> LiveActorGuard {
    map()
        .lock()
        .recover_poison()
        .insert(child_id.to_string(), tx);
    LiveActorGuard {
        child_id: child_id.to_string(),
    }
}

/// Deliver an in-band steering message to a live child. Returns `false` when
/// the child is not live (caller should use the durable queue instead).
pub fn deliver_message(child_id: &str, text: &str) -> bool {
    let guard = map().lock().recover_poison();
    match guard.get(child_id) {
        Some(tx) => tx
            .send(ParentFrame::Message {
                text: text.to_string(),
            })
            .is_ok(),
        None => false,
    }
}

/// Deliver a host/human approval decision to a live child's pending gated-tool
/// request (Phase 2: child → parent approval delegation). Sends
/// `ParentFrame::ApprovalReply{id, approved}` over the child's live WS
/// connection; `drive()` forwards it to the worker, whose pending map resolves
/// the `host.approval_call` the child's gated tool is blocked on (approve ⇒ the
/// tool proceeds, deny ⇒ it fails closed). This is the decision-DOWN half of the
/// human-in-the-loop route: a parent-side responder (e.g. a `/respond`-style
/// handler) calls this with the `request_id` it surfaced to the human. Returns
/// `false` when the child is not live (no connection to answer on — the caller
/// should treat that as a denied/expired request).
pub fn deliver_approval(child_id: &str, request_id: &str, approved: bool) -> bool {
    let guard = map().lock().recover_poison();
    match guard.get(child_id) {
        Some(tx) => tx
            .send(ParentFrame::ApprovalReply {
                id: request_id.to_string(),
                approved,
            })
            .is_ok(),
        None => false,
    }
}

/// Whether a child currently has a live actor connection.
pub fn is_live(child_id: &str) -> bool {
    map().lock().recover_poison().contains_key(child_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_deliver_unregister() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let guard = register("c-live", tx);
        assert!(is_live("c-live"));
        assert!(deliver_message("c-live", "hi"));
        match rx.try_recv() {
            Ok(ParentFrame::Message { text }) => assert_eq!(text, "hi"),
            other => panic!("expected message frame, got {other:?}"),
        }

        drop(guard);
        assert!(!is_live("c-live"));
        assert!(!deliver_message("c-live", "gone"));
    }

    #[test]
    fn deliver_fails_when_receiver_dropped() {
        let (tx, rx) = mpsc::unbounded_channel();
        let _guard = register("c-dead", tx);
        drop(rx);
        assert!(!deliver_message("c-dead", "hi"));
    }

    #[test]
    fn deliver_approval_routes_reply_frame() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let guard = register("c-appr", tx);
        assert!(deliver_approval("c-appr", "req-7", true));
        match rx.try_recv() {
            Ok(ParentFrame::ApprovalReply { id, approved }) => {
                assert_eq!(id, "req-7");
                assert!(approved);
            }
            other => panic!("expected approval reply, got {other:?}"),
        }
        drop(guard);
        // Not-live child ⇒ false (no connection to answer on).
        assert!(!deliver_approval("c-appr", "req-8", false));
    }

    #[test]
    fn pending_approval_is_one_shot() {
        register_pending_approval("c-pend", "req-1");
        // First take consumes it; the second finds nothing.
        assert!(take_pending_approval("c-pend", "req-1"));
        assert!(!take_pending_approval("c-pend", "req-1"));
    }

    #[test]
    fn take_of_unregistered_pair_is_false() {
        // Unknown child entirely.
        assert!(!take_pending_approval("c-unknown", "req-x"));
        // Known child, but an unregistered request_id.
        register_pending_approval("c-known", "req-real");
        assert!(!take_pending_approval("c-known", "req-bogus"));
        // The real one is still pending (a bogus take didn't disturb it).
        assert!(take_pending_approval("c-known", "req-real"));
    }

    #[test]
    fn deliver_approval_checked_only_delivers_for_registered_pair() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _guard = register("c-checked", tx);

        // Not registered ⇒ rejected, nothing on the wire.
        assert!(!deliver_approval_checked("c-checked", "req-stray", true));
        assert!(rx.try_recv().is_err());

        // Registered ⇒ delivered, frame rides the wire, and consumed.
        register_pending_approval("c-checked", "req-ok");
        assert!(deliver_approval_checked("c-checked", "req-ok", true));
        match rx.try_recv() {
            Ok(ParentFrame::ApprovalReply { id, approved }) => {
                assert_eq!(id, "req-ok");
                assert!(approved);
            }
            other => panic!("expected approval reply, got {other:?}"),
        }
        // One-shot: a replay is rejected (and nothing further on the wire).
        assert!(!deliver_approval_checked("c-checked", "req-ok", true));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn clear_pending_approvals_for_drops_them() {
        register_pending_approval("c-clear", "req-a");
        register_pending_approval("c-clear", "req-b");
        clear_pending_approvals_for("c-clear");
        assert!(!take_pending_approval("c-clear", "req-a"));
        assert!(!take_pending_approval("c-clear", "req-b"));
    }

    #[tokio::test]
    async fn observed_approval_emits_exactly_one_terminal_outcome() {
        let (wire_tx, _wire_rx) = mpsc::unbounded_channel();
        let _guard = register("c-audit", wire_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        register_pending_approval_observed(
            "parent-audit",
            "c-audit",
            "req-audit",
            "Bash",
            "execute",
            "/tmp/x",
            event_tx,
        );

        assert!(deliver_approval_checked("c-audit", "req-audit", false));
        assert!(!deliver_approval_checked("c-audit", "req-audit", true));
        assert!(matches!(
            event_rx.recv().await,
            Some(AgentEvent::ChildApprovalChanged { status, .. }) if status == "denied"
        ));
        assert!(event_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn timeout_and_disconnect_emit_terminal_outcomes() {
        let (event_tx, mut event_rx) = mpsc::channel(8);
        register_pending_approval_observed(
            "parent-audit",
            "c-expire",
            "req-expire",
            "Bash",
            "execute",
            "/tmp/x",
            event_tx.clone(),
        );
        assert!(expire_pending_approval("c-expire", "req-expire"));
        assert!(!expire_pending_approval("c-expire", "req-expire"));
        assert!(matches!(
            event_rx.recv().await,
            Some(AgentEvent::ChildApprovalChanged { status, .. }) if status == "expired"
        ));

        register_pending_approval_observed(
            "parent-audit",
            "c-disconnect",
            "req-disconnect",
            "Write",
            "write",
            "/tmp/y",
            event_tx,
        );
        clear_pending_approvals_for("c-disconnect");
        assert!(matches!(
            event_rx.recv().await,
            Some(AgentEvent::ChildApprovalChanged { status, .. }) if status == "delivery_failed"
        ));
    }
}
