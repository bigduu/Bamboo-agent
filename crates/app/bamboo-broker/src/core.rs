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

/// An item pushed to a live subscriber's sink: either a durable mailbox message
/// or an ephemeral out-of-band control signal. Both ride the same subscriber
/// channel (so the server's single push arm handles them in arrival order), but
/// a `Cancel` never touches the mailbox — it is not persisted, claimed, or
/// acked. #50.
#[derive(Debug)]
pub enum PushItem {
    /// A durable message claimed from the subscriber's mailbox.
    Message(InboxMessage),
    /// Out-of-band cancel for the in-flight run correlated to this id.
    Cancel(MsgId),
}

/// In-process routing engine: owns the mailbox root and the live subscriber table.
pub struct BrokerCore {
    root: PathBuf,
    /// session_id -> live subscriber sink. Present only while a client is subscribed.
    subscribers: Mutex<HashMap<String, mpsc::UnboundedSender<PushItem>>>,
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
    ) -> BrokerResult<mpsc::UnboundedReceiver<PushItem>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.subscribers
            .lock()
            .await
            .insert(session_id.to_string(), tx.clone());

        let mb = self.mailbox(session_id);
        // Crash leftovers first (claimed-but-unacked from a previous connection),
        // then newly delivered, all in time order.
        for d in mb.recover().await? {
            let _ = tx.send(PushItem::Message(d.msg));
        }
        for d in mb.drain().await? {
            let _ = tx.send(PushItem::Message(d.msg));
        }
        Ok(rx)
    }

    /// Out-of-band cancel: if `to` is currently subscribed, push an ephemeral
    /// [`PushItem::Cancel`] to its live sink. Does NOT touch the mailbox (not
    /// durable, never claimed/acked/recovered), so a cancel can never queue
    /// behind the very work it cancels. Returns true iff a live subscriber
    /// received it (a cancel for an offline session is a meaningless no-op — the
    /// run isn't happening). #50.
    pub async fn cancel(&self, to: &str, correlation_id: &MsgId) -> bool {
        let subs = self.subscribers.lock().await;
        match subs.get(to) {
            Some(tx) => tx.send(PushItem::Cancel(correlation_id.clone())).is_ok(),
            None => false,
        }
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
            let _ = tx.send(PushItem::Message(d.msg));
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

    fn expect_message(item: PushItem) -> InboxMessage {
        match item {
            PushItem::Message(m) => m,
            PushItem::Cancel(c) => panic!("expected a message, got Cancel({c:?})"),
        }
    }

    #[tokio::test]
    async fn deliver_then_subscribe_drains_backlog() {
        let (_d, c) = core();
        let m = msg(1);
        c.deliver("child", &m).await.unwrap();
        // not subscribed yet -> message waits durably
        assert!(!c.is_subscribed("child").await);

        let mut rx = c.subscribe("child").await.unwrap();
        let got = expect_message(rx.try_recv().expect("backlog delivered on subscribe"));
        assert_eq!(got.id, m.id);
    }

    #[tokio::test]
    async fn subscribe_then_deliver_pushes_live() {
        let (_d, c) = core();
        let mut rx = c.subscribe("child").await.unwrap();
        assert!(rx.try_recv().is_err()); // empty initially

        let m = msg(2);
        c.deliver("child", &m).await.unwrap();
        let got = expect_message(rx.recv().await.expect("live push"));
        assert_eq!(got.id, m.id);
    }

    #[tokio::test]
    async fn ack_removes_so_resubscribe_does_not_redeliver() {
        let (_d, c) = core();
        let m = msg(3);
        c.deliver("child", &m).await.unwrap();
        let mut rx = c.subscribe("child").await.unwrap();
        let got = expect_message(rx.recv().await.unwrap());
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
        let got = expect_message(rx.recv().await.unwrap()); // pushed, NOT acked
        assert_eq!(got.id, m.id);

        // connection drops without ack -> message stays in cur/ -> re-pushed.
        c.unsubscribe("child").await;
        let mut rx2 = c.subscribe("child").await.unwrap();
        let again = expect_message(rx2.try_recv().expect("unacked message redelivers"));
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

    #[tokio::test]
    async fn cancel_pushes_control_item_without_touching_mailbox() {
        let (_d, c) = core();
        let cid = MsgId::new();

        // No live subscriber -> cancel is a meaningless no-op.
        assert!(!c.cancel("worker", &cid).await);

        // Subscribed -> the live subscriber receives a Cancel control item.
        let mut rx = c.subscribe("worker").await.unwrap();
        assert!(
            c.cancel("worker", &cid).await,
            "a live subscriber received the cancel"
        );
        match rx.try_recv().expect("cancel was pushed") {
            PushItem::Cancel(got) => assert_eq!(got, cid),
            PushItem::Message(_) => panic!("expected a Cancel, got a Message"),
        }

        // Out-of-band: the cancel left NO durable mailbox trace, so a fresh
        // subscribe re-pushes nothing (no new/ or cur/ entry was created).
        c.unsubscribe("worker").await;
        let mut rx2 = c.subscribe("worker").await.unwrap();
        assert!(
            rx2.try_recv().is_err(),
            "cancel must not persist anything to the mailbox"
        );
    }
}
