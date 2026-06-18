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

use bamboo_subagent::proto::ParentFrame;
use tokio::sync::mpsc;

fn map() -> &'static Mutex<HashMap<String, mpsc::UnboundedSender<ParentFrame>>> {
    static MAP: OnceLock<Mutex<HashMap<String, mpsc::UnboundedSender<ParentFrame>>>> =
        OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Unregisters the child on drop, so a panicking/returning runner can't leak
/// a stale sender.
pub struct LiveActorGuard {
    child_id: String,
}

impl Drop for LiveActorGuard {
    fn drop(&mut self) {
        map().lock().unwrap().remove(&self.child_id);
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
}
