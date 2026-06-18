//! Live actor registry: in-band delivery to currently-running actor children.
//!
//! While `ActorChildRunner` drives a child over WebSocket, it registers a frame
//! sender here keyed by `child_session_id`. `send_message` (running, no
//! interrupt) consults this map: when the child is live, the message rides the
//! existing WS as a `ParentFrame::Message` and is admitted by the worker's
//! agent loop at its next round boundary — the same mechanism in-process
//! children use, extended across the process boundary. When the child is not
//! live, callers fall back to the durable `pending_injected_messages` queue.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

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
fn pending() -> &'static Mutex<HashMap<String, HashSet<String>>> {
    static PENDING: OnceLock<Mutex<HashMap<String, HashSet<String>>>> = OnceLock::new();
    PENDING.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Record a `(child_id, request_id)` as a pending human-loop approval. Called
/// just before surfacing `ChildApprovalRequested` so an external POST can be
/// correlated against a genuinely-pending request.
pub fn register_pending_approval(child_id: &str, request_id: &str) {
    pending()
        .lock()
        .unwrap()
        .entry(child_id.to_string())
        .or_default()
        .insert(request_id.to_string());
}

/// One-shot consume of a `(child_id, request_id)` pending pair: remove it and
/// return whether it WAS present. A second call for the same pair returns
/// `false`, so a request can't be answered (or replayed) twice.
pub fn take_pending_approval(child_id: &str, request_id: &str) -> bool {
    let mut guard = pending().lock().unwrap();
    let Some(set) = guard.get_mut(child_id) else {
        return false;
    };
    let took = set.remove(request_id);
    if set.is_empty() {
        guard.remove(child_id);
    }
    took
}

/// Drop all pending approvals for a child (e.g. when its live connection ends).
pub fn clear_pending_approvals_for(child_id: &str) {
    pending().lock().unwrap().remove(child_id);
}

/// Validated external entry point: deliver an approval decision ONLY if the
/// `(child_id, request_id)` pair is currently pending. Consumes the pending
/// entry (one-shot) before delivering, so the same request can't be replayed,
/// and rejects (returns `false`) any `request_id` that isn't currently pending
/// — unknown, already-answered/timed-out, or a non-human-loop path
/// (model-review / escalation) that never registered. This is the entry the
/// external HTTP handler must use.
pub fn deliver_approval_checked(child_id: &str, request_id: &str, approved: bool) -> bool {
    if take_pending_approval(child_id, request_id) {
        deliver_approval(child_id, request_id, approved)
    } else {
        false
    }
}

/// Unregisters the child on drop, so a panicking/returning runner can't leak
/// a stale sender.
pub struct LiveActorGuard {
    child_id: String,
}

impl Drop for LiveActorGuard {
    fn drop(&mut self) {
        map().lock().unwrap().remove(&self.child_id);
        // A disconnecting child can't answer any still-pending approval — drop
        // them so a late external POST finds nothing pending and is rejected.
        clear_pending_approvals_for(&self.child_id);
    }
}

/// Register a live child's frame sender for the duration of its run.
pub fn register(child_id: &str, tx: mpsc::UnboundedSender<ParentFrame>) -> LiveActorGuard {
    map().lock().unwrap().insert(child_id.to_string(), tx);
    LiveActorGuard {
        child_id: child_id.to_string(),
    }
}

/// Deliver an in-band steering message to a live child. Returns `false` when
/// the child is not live (caller should use the durable queue instead).
pub fn deliver_message(child_id: &str, text: &str) -> bool {
    let guard = map().lock().unwrap();
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
    let guard = map().lock().unwrap();
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
    map().lock().unwrap().contains_key(child_id)
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
}
