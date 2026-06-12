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
}
