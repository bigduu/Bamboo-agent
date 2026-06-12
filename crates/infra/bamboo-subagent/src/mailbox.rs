//! Maildir-style persistent mailbox (design §3.4).
//!
//! ```text
//! mailbox/
//!   new/      delivered, unprocessed   <unix_nanos>-<msgid>.json
//!   cur/      claimed, being processed
//!   corrupt/  quarantined parse failures
//! ```
//!
//! - **Multi-writer / single-reader, lock-free.** Senders [`deliver`](Mailbox::deliver) via
//!   atomic temp+rename into `new/`; the owning actor [`drain`](Mailbox::drain)s by renaming
//!   `new/ -> cur/` (claim), processes, then [`ack`](Mailbox::ack)s (delete from `cur/`).
//! - **Crash-safe, at-least-once.** A crash between claim and ack leaves the message in `cur/`;
//!   [`recover`](Mailbox::recover) re-yields it on next activation. Dedupe is the consumer's job
//!   (see [`AdmittedSet`]), keyed by [`MsgId`].

use std::collections::HashSet;
use std::io::ErrorKind;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{atomic_write, Result, StoreError};

/// Idempotency key for a delivered message.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MsgId(pub String);

impl MsgId {
    pub fn new() -> Self {
        MsgId(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for MsgId {
    fn default() -> Self {
        Self::new()
    }
}

/// Sender identity attached to an inbox message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRef {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

/// In-band message kind (control signals like `cancel` do NOT travel here — they are out-of-band).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InboxKind {
    Task,
    Ask,
    Handoff,
    Reply,
}

/// A message addressed to an actor's mailbox. `body` is the opaque chat payload (domain `Message`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InboxMessage {
    pub id: MsgId,
    pub from: AgentRef,
    pub kind: InboxKind,
    pub body: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// A claimed message plus its location in `cur/` (for `ack`).
#[derive(Debug, Clone)]
pub struct Delivered {
    pub msg: InboxMessage,
    pub cur_path: PathBuf,
}

/// Per-actor mailbox rooted at a `mailbox/` directory.
pub struct Mailbox {
    dir: PathBuf,
}

impl Mailbox {
    pub fn at(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn new_dir(&self) -> PathBuf {
        self.dir.join("new")
    }
    fn cur_dir(&self) -> PathBuf {
        self.dir.join("cur")
    }
    fn corrupt_dir(&self) -> PathBuf {
        self.dir.join("corrupt")
    }

    pub async fn ensure_dirs(&self) -> Result<()> {
        for d in [self.new_dir(), self.cur_dir(), self.corrupt_dir()] {
            tokio::fs::create_dir_all(&d)
                .await
                .map_err(|e| StoreError::io(&d, e))?;
        }
        Ok(())
    }

    // ---- sender side (multi-writer, lock-free) ----------------------------

    /// Atomically deliver `msg` into `new/`. Safe under concurrent writers.
    pub async fn deliver(&self, msg: &InboxMessage) -> Result<MsgId> {
        let bytes = serde_json::to_vec_pretty(msg).map_err(|e| StoreError::decode(&self.dir, e))?;
        let nanos = msg.created_at.timestamp_nanos_opt().unwrap_or(0).max(0);
        // 20-digit zero-padded prefix => lexicographic order == time order; msgid breaks ties.
        let name = format!("{nanos:020}-{}.json", msg.id.0);
        // atomic_write puts its temp in new/ as a hidden `.`-file that drain skips.
        atomic_write(&self.new_dir().join(&name), &bytes).await?;
        Ok(msg.id.clone())
    }

    // ---- receiver side (single reader = the actor) ------------------------

    /// Claim and return all pending messages in `new/`, in delivery order.
    /// Each is renamed `new/ -> cur/`; corrupt files are quarantined and skipped.
    pub async fn drain(&self) -> Result<Vec<Delivered>> {
        self.ensure_dirs().await?;
        let names = self.sorted_json_names(&self.new_dir()).await?;
        let mut out = Vec::new();
        for name in names {
            let src = self.new_dir().join(&name);
            let dst = self.cur_dir().join(&name);
            // claim; if it's already gone (lost race), skip.
            if tokio::fs::rename(&src, &dst).await.is_err() {
                continue;
            }
            match read_msg(&dst).await {
                Ok(msg) => out.push(Delivered { msg, cur_path: dst }),
                Err(_) => {
                    let _ = tokio::fs::rename(&dst, &self.corrupt_dir().join(&name)).await;
                }
            }
        }
        Ok(out)
    }

    /// Acknowledge a processed message: delete it from `cur/`. Idempotent (no-op if gone).
    pub async fn ack(&self, id: &MsgId) -> Result<()> {
        let needle = format!("-{}.json", id.0);
        let cur = self.cur_dir();
        let mut rd = match tokio::fs::read_dir(&cur).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(StoreError::io(&cur, e)),
        };
        while let Some(ent) = rd.next_entry().await.map_err(|e| StoreError::io(&cur, e))? {
            let fname = ent.file_name().to_string_lossy().into_owned();
            if fname.ends_with(&needle) {
                tokio::fs::remove_file(ent.path())
                    .await
                    .map_err(|e| StoreError::io(ent.path(), e))?;
                return Ok(());
            }
        }
        Ok(())
    }

    /// Re-yield messages left in `cur/` by a previous (crashed) activation, in order.
    pub async fn recover(&self) -> Result<Vec<Delivered>> {
        self.ensure_dirs().await?;
        let names = self.sorted_json_names(&self.cur_dir()).await?;
        let mut out = Vec::new();
        for name in names {
            let path = self.cur_dir().join(&name);
            match read_msg(&path).await {
                Ok(msg) => out.push(Delivered {
                    msg,
                    cur_path: path,
                }),
                Err(_) => {
                    let _ = tokio::fs::rename(&path, &self.corrupt_dir().join(&name)).await;
                }
            }
        }
        Ok(out)
    }

    /// True if `new/` has no pending messages.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.sorted_json_names(&self.new_dir()).await?.is_empty())
    }

    async fn sorted_json_names(&self, dir: &std::path::Path) -> Result<Vec<String>> {
        let mut rd = match tokio::fs::read_dir(dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(StoreError::io(dir, e)),
        };
        let mut names = Vec::new();
        while let Some(ent) = rd.next_entry().await.map_err(|e| StoreError::io(dir, e))? {
            let fname = ent.file_name().to_string_lossy().into_owned();
            if fname.starts_with('.') || !fname.ends_with(".json") {
                continue; // skip hidden temp files / non-messages
            }
            names.push(fname);
        }
        names.sort();
        Ok(names)
    }
}

async fn read_msg(path: &std::path::Path) -> Result<InboxMessage> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| StoreError::io(path, e))?;
    serde_json::from_slice(&bytes).map_err(|e| StoreError::decode(path, e))
}

/// Consumer-side dedupe set for at-least-once delivery; persist with the session state.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AdmittedSet {
    ids: HashSet<MsgId>,
}

impl AdmittedSet {
    pub fn contains(&self, id: &MsgId) -> bool {
        self.ids.contains(id)
    }
    /// Record `id` as admitted. Returns `true` if newly inserted (i.e. should admit now).
    pub fn insert(&mut self, id: MsgId) -> bool {
        self.ids.insert(id)
    }
    pub fn len(&self) -> usize {
        self.ids.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;
    use tempfile::TempDir;

    fn mailbox() -> (TempDir, Mailbox) {
        let dir = TempDir::new().unwrap();
        let mb = Mailbox::at(dir.path().join("mailbox"));
        (dir, mb)
    }

    fn msg(seq: u32) -> InboxMessage {
        InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "parent".into(),
                role: None,
            },
            kind: InboxKind::Task,
            body: json!({ "seq": seq }),
            created_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn deliver_then_drain_then_ack() {
        let (_d, mb) = mailbox();
        let m = msg(1);
        mb.deliver(&m).await.unwrap();

        assert!(!mb.is_empty().await.unwrap());
        let batch = mb.drain().await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].msg.id, m.id);
        assert!(mb.is_empty().await.unwrap()); // moved out of new/

        mb.ack(&m.id).await.unwrap();
        // nothing left in cur/ -> recover yields nothing
        assert!(mb.recover().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn multi_writer_no_loss() {
        let (_d, mb) = mailbox();
        mb.ensure_dirs().await.unwrap();
        let dir = mb.dir.clone();

        let mut handles = Vec::new();
        for i in 0..50u32 {
            let d = dir.clone();
            handles.push(tokio::spawn(async move {
                let mb = Mailbox::at(d);
                mb.deliver(&msg(i)).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let batch = mb.drain().await.unwrap();
        assert_eq!(batch.len(), 50);
        let ids: HashSet<_> = batch.iter().map(|d| d.msg.id.clone()).collect();
        assert_eq!(ids.len(), 50); // all unique, none lost
    }

    #[tokio::test]
    async fn drain_is_time_ordered() {
        let (_d, mb) = mailbox();
        let base = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        for i in 0..5u32 {
            let mut m = msg(i);
            m.created_at = base + chrono::Duration::seconds(i as i64);
            mb.deliver(&m).await.unwrap();
        }
        let batch = mb.drain().await.unwrap();
        let seqs: Vec<u32> = batch
            .iter()
            .map(|d| d.msg.body["seq"].as_u64().unwrap() as u32)
            .collect();
        assert_eq!(seqs, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn recover_returns_unacked_leftovers() {
        let (_d, mb) = mailbox();
        let m = msg(1);
        mb.deliver(&m).await.unwrap();
        let batch = mb.drain().await.unwrap(); // claimed into cur/, not acked
        assert_eq!(batch.len(), 1);

        // simulate crash + reactivation: a fresh handle on the same dir
        let mb2 = Mailbox::at(mb.dir.clone());
        let recovered = mb2.recover().await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].msg.id, m.id);
    }

    #[tokio::test]
    async fn corrupt_file_is_quarantined() {
        let (_d, mb) = mailbox();
        mb.ensure_dirs().await.unwrap();
        // a well-formed message + a bogus one
        mb.deliver(&msg(1)).await.unwrap();
        tokio::fs::write(mb.new_dir().join("00000000000000000001-bogus.json"), b"not json")
            .await
            .unwrap();

        let batch = mb.drain().await.unwrap();
        assert_eq!(batch.len(), 1); // the good one came through
        let mut rd = tokio::fs::read_dir(mb.corrupt_dir()).await.unwrap();
        let mut corrupt = 0;
        while rd.next_entry().await.unwrap().is_some() {
            corrupt += 1;
        }
        assert_eq!(corrupt, 1); // the bogus one quarantined
    }

    #[tokio::test]
    async fn admitted_set_dedupes() {
        let mut seen = AdmittedSet::default();
        let id = MsgId::new();
        assert!(seen.insert(id.clone())); // first time -> admit
        assert!(seen.contains(&id));
        assert!(!seen.insert(id.clone())); // redelivery -> skip
        assert_eq!(seen.len(), 1);
    }
}
