//! Broker routing core: per-session durable mailboxes + push subscriptions.
//!
//! Transport-agnostic (no WebSocket here) so it is unit-testable in-process. The
//! WS server is a thin shell over this: a connection's `Deliver` calls
//! [`BrokerCore::deliver`], `Subscribe` calls [`BrokerCore::subscribe`] and
//! forwards the returned stream as `Message` frames, `Ack` calls
//! [`BrokerCore::ack`].
//!
//! Durability + delivery semantics come straight from the underlying
//! [`Mailbox`] (maildir, atomic, crash-safe, at-least-once): `deliver` persists
//! before returning; `subscribe` first re-pushes crash leftovers (`recover`),
//! then claims pending (`drain`); each subsequent `deliver` claims-and-pushes the
//! new message. A pushed-but-unacked message stays in `cur/` and is re-pushed on
//! the next `subscribe` — consumers dedupe by [`MsgId`].

use std::collections::HashMap;
use std::path::PathBuf;

use bamboo_subagent::{InboxMessage, Mailbox, MsgId};
use tokio::sync::{mpsc, Mutex};

use crate::error::BrokerResult;

/// In-process routing engine: owns the mailbox root and the live subscriber table.
pub struct BrokerCore {
    root: PathBuf,
    /// session_id -> live subscriber sink. Present only while a client is subscribed.
    subscribers: Mutex<HashMap<String, mpsc::UnboundedSender<InboxMessage>>>,
}

impl BrokerCore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            subscribers: Mutex::new(HashMap::new()),
        }
    }

    /// Mailbox for one session: `<root>/mailboxes/<session_id>`.
    fn mailbox(&self, session_id: &str) -> Mailbox {
        Mailbox::at(self.root.join("mailboxes").join(session_id))
    }

    /// Durably enqueue `msg` into `to`'s mailbox, then — if `to` is currently
    /// subscribed — claim and push it immediately. Returns the stored [`MsgId`].
    pub async fn deliver(&self, to: &str, msg: &InboxMessage) -> BrokerResult<MsgId> {
        let id = self.mailbox(to).deliver(msg).await?;
        self.push_new(to).await?;
        Ok(id)
    }

    /// Register a subscriber for `session_id` and return the stream of pushed
    /// messages. Immediately re-pushes crash leftovers (`recover`) then any
    /// pending backlog (`drain`). A prior subscriber for the same id is replaced.
    pub async fn subscribe(
        &self,
        session_id: &str,
    ) -> BrokerResult<mpsc::UnboundedReceiver<InboxMessage>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.subscribers
            .lock()
            .await
            .insert(session_id.to_string(), tx.clone());

        let mb = self.mailbox(session_id);
        // Crash leftovers first (claimed-but-unacked from a previous connection),
        // then newly delivered, all in time order.
        for d in mb.recover().await? {
            let _ = tx.send(d.msg);
        }
        for d in mb.drain().await? {
            let _ = tx.send(d.msg);
        }
        Ok(rx)
    }

    /// Drop the subscriber for `session_id` (connection closed). Unacked messages
    /// remain in `cur/` for redelivery on the next subscribe.
    pub async fn unsubscribe(&self, session_id: &str) {
        self.subscribers.lock().await.remove(session_id);
    }

    /// Acknowledge a processed message: delete it from `session_id`'s mailbox.
    pub async fn ack(&self, session_id: &str, id: &MsgId) -> BrokerResult<()> {
        self.mailbox(session_id).ack(id).await?;
        Ok(())
    }

    /// True if a client is currently subscribed to `session_id`.
    pub async fn is_subscribed(&self, session_id: &str) -> bool {
        self.subscribers.lock().await.contains_key(session_id)
    }

    /// Claim newly-delivered messages for `session_id` and push to its live
    /// subscriber. No-op when no one is subscribed (the message stays durably in
    /// `new/` until someone subscribes). Does NOT `recover` — in-flight `cur/`
    /// messages are only re-pushed on a fresh `subscribe`, so a live subscriber
    /// is not spammed with not-yet-acked duplicates.
    async fn push_new(&self, session_id: &str) -> BrokerResult<()> {
        let tx = {
            let subs = self.subscribers.lock().await;
            match subs.get(session_id) {
                Some(tx) => tx.clone(),
                None => return Ok(()),
            }
        };
        for d in self.mailbox(session_id).drain().await? {
            let _ = tx.send(d.msg);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_subagent::{AgentRef, InboxKind};
    use chrono::Utc;
    use tempfile::TempDir;

    fn msg(seq: u32) -> InboxMessage {
        InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "from".into(),
                role: None,
            },
            kind: InboxKind::Ask,
            body: serde_json::json!({ "seq": seq }),
            created_at: Utc::now(),
            correlation_id: None,
        }
    }

    fn core() -> (TempDir, BrokerCore) {
        let d = TempDir::new().unwrap();
        let c = BrokerCore::new(d.path());
        (d, c)
    }

    #[tokio::test]
    async fn deliver_then_subscribe_drains_backlog() {
        let (_d, c) = core();
        let m = msg(1);
        c.deliver("child", &m).await.unwrap();
        // not subscribed yet -> message waits durably
        assert!(!c.is_subscribed("child").await);

        let mut rx = c.subscribe("child").await.unwrap();
        let got = rx.try_recv().expect("backlog delivered on subscribe");
        assert_eq!(got.id, m.id);
    }

    #[tokio::test]
    async fn subscribe_then_deliver_pushes_live() {
        let (_d, c) = core();
        let mut rx = c.subscribe("child").await.unwrap();
        assert!(rx.try_recv().is_err()); // empty initially

        let m = msg(2);
        c.deliver("child", &m).await.unwrap();
        let got = rx.recv().await.expect("live push");
        assert_eq!(got.id, m.id);
    }

    #[tokio::test]
    async fn ack_removes_so_resubscribe_does_not_redeliver() {
        let (_d, c) = core();
        let m = msg(3);
        c.deliver("child", &m).await.unwrap();
        let mut rx = c.subscribe("child").await.unwrap();
        let got = rx.recv().await.unwrap();
        assert_eq!(got.id, m.id);

        // ack + drop subscription, then resubscribe: nothing redelivered.
        c.ack("child", &got.id).await.unwrap();
        c.unsubscribe("child").await;
        let mut rx2 = c.subscribe("child").await.unwrap();
        assert!(rx2.try_recv().is_err(), "acked message must not redeliver");
    }

    #[tokio::test]
    async fn unacked_message_redelivers_on_resubscribe() {
        let (_d, c) = core();
        let m = msg(4);
        c.deliver("child", &m).await.unwrap();
        let mut rx = c.subscribe("child").await.unwrap();
        let got = rx.recv().await.unwrap(); // pushed, NOT acked
        assert_eq!(got.id, m.id);

        // connection drops without ack -> message stays in cur/ -> re-pushed.
        c.unsubscribe("child").await;
        let mut rx2 = c.subscribe("child").await.unwrap();
        let again = rx2.try_recv().expect("unacked message redelivers");
        assert_eq!(again.id, m.id);
    }

    #[tokio::test]
    async fn deliver_to_unsubscribed_is_durable_and_isolated_per_session() {
        let (_d, c) = core();
        c.deliver("a", &msg(1)).await.unwrap();
        c.deliver("b", &msg(2)).await.unwrap();
        // subscriber for "a" sees only a's mailbox.
        let mut rx_a = c.subscribe("a").await.unwrap();
        assert!(rx_a.try_recv().is_ok());
        assert!(rx_a.try_recv().is_err());
    }
}
