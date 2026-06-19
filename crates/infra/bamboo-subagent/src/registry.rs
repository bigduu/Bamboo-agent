//! Tier-2 registry: the parent's in-memory table of owned children + persistent index upkeep.
//!
//! This is the transport-agnostic *logic*; the HTTP veneer (`POST /internal/subagents/*`) lives in
//! the host server (actix) and is a thin shim over these methods. The registry is the single writer
//! of the persistent indices (design §5.4) — it updates `children.json` / `index.json` via the store.

use std::collections::HashMap;
use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};

use crate::error::Result;
use crate::store::{ChildEntry, ChildStatus, ProjectKey, SubagentStore};

/// A child's registration as a parent's owned actor.
#[derive(Debug, Clone, PartialEq)]
pub struct Registration {
    pub child_id: String,
    pub parent_id: String,
    pub subagent_type: String,
    pub title: String,
    pub responsibility: String,
    pub endpoint: String,
    pub pid: u32,
    pub status: ChildStatus,
    pub last_heartbeat: DateTime<Utc>,
}

/// Input for `register` (the child reports this on startup).
#[derive(Debug, Clone)]
pub struct RegisterChild {
    pub child_id: String,
    pub parent_id: String,
    pub subagent_type: String,
    pub title: String,
    pub responsibility: String,
    pub endpoint: String,
    pub pid: u32,
}

/// In-memory owned-children table + persistent index maintainer.
pub struct Registry {
    store: SubagentStore,
    key: ProjectKey,
    table: Mutex<HashMap<String, Registration>>,
}

impl Registry {
    pub fn new(store: SubagentStore, key: ProjectKey) -> Self {
        Self {
            store,
            key,
            table: Mutex::new(HashMap::new()),
        }
    }

    /// Register a child: record it in the table and write its index entry (status `Running`).
    pub async fn register(&self, r: RegisterChild, now: DateTime<Utc>) -> Result<()> {
        let reg = Registration {
            child_id: r.child_id.clone(),
            parent_id: r.parent_id.clone(),
            subagent_type: r.subagent_type.clone(),
            title: r.title.clone(),
            responsibility: r.responsibility.clone(),
            endpoint: r.endpoint,
            pid: r.pid,
            status: ChildStatus::Running,
            last_heartbeat: now,
        };
        self.table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(r.child_id.clone(), reg.clone());
        self.persist_entry(&reg, now).await
    }

    /// Heartbeat: refresh liveness and (optionally) status.
    pub async fn heartbeat(
        &self,
        child_id: &str,
        status: Option<ChildStatus>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let reg = {
            let mut table = self.table.lock().unwrap_or_else(|e| e.into_inner());
            let Some(reg) = table.get_mut(child_id) else {
                return Ok(()); // unknown child -> ignore
            };
            reg.last_heartbeat = now;
            if let Some(s) = status {
                reg.status = s;
            }
            reg.clone()
        };
        self.persist_entry(&reg, now).await
    }

    /// Deregister: drop from the table and write the final status into the index.
    pub async fn deregister(
        &self,
        child_id: &str,
        final_status: ChildStatus,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let reg = self
            .table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(child_id);
        if let Some(mut reg) = reg {
            reg.status = final_status;
            self.persist_entry(&reg, now).await?;
        }
        Ok(())
    }

    /// Snapshot of currently-registered children (for `GET /internal/subagents`).
    pub fn list(&self) -> Vec<Registration> {
        let mut v: Vec<_> = self
            .table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        v.sort_by(|a, b| a.child_id.cmp(&b.child_id));
        v
    }

    pub fn get(&self, child_id: &str) -> Option<Registration> {
        self.table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(child_id)
            .cloned()
    }

    /// Child ids whose last heartbeat is older than `ttl` (candidates for reaping).
    pub fn stale(&self, ttl: Duration, now: DateTime<Utc>) -> Vec<String> {
        self.table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|r| now - r.last_heartbeat > ttl)
            .map(|r| r.child_id.clone())
            .collect()
    }

    async fn persist_entry(&self, reg: &Registration, now: DateTime<Utc>) -> Result<()> {
        self.store
            .upsert_child(
                &self.key,
                &reg.parent_id,
                ChildEntry {
                    child_id: reg.child_id.clone(),
                    subagent_type: reg.subagent_type.clone(),
                    status: reg.status,
                    title: reg.title.clone(),
                    responsibility: reg.responsibility.clone(),
                    updated_at: now,
                },
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Registry) {
        let d = TempDir::new().unwrap();
        let store = SubagentStore::open(d.path());
        let reg = Registry::new(store, ProjectKey::from_raw("proj"));
        (d, reg)
    }

    fn reg_input(id: &str) -> RegisterChild {
        RegisterChild {
            child_id: id.into(),
            parent_id: "p1".into(),
            subagent_type: "researcher".into(),
            title: id.into(),
            responsibility: format!("do {id}"),
            endpoint: "ws://127.0.0.1:1".into(),
            pid: 100,
        }
    }

    #[tokio::test]
    async fn register_reflects_in_table_and_index() {
        let (_d, reg) = setup();
        let now = Utc::now();
        reg.register(reg_input("c1"), now).await.unwrap();

        // in-memory table
        let listed = reg.list();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].status, ChildStatus::Running);

        // persistent index updated via the store
        let children = reg.store.list_children(&reg.key, "p1").await.unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].child_id, "c1");
        assert!(reg
            .store
            .resolve_child(&reg.key, "c1")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn heartbeat_updates_status_and_deregister_finalizes() {
        let (_d, reg) = setup();
        let now = Utc::now();
        reg.register(reg_input("c1"), now).await.unwrap();

        reg.heartbeat("c1", Some(ChildStatus::Idle), now)
            .await
            .unwrap();
        assert_eq!(reg.get("c1").unwrap().status, ChildStatus::Idle);

        reg.deregister("c1", ChildStatus::Completed, now)
            .await
            .unwrap();
        assert!(reg.get("c1").is_none()); // out of the live table
                                          // final status persisted in the index
        let children = reg.store.list_children(&reg.key, "p1").await.unwrap();
        assert_eq!(children[0].status, ChildStatus::Completed);
    }

    #[tokio::test]
    async fn stale_detects_missed_heartbeats() {
        let (_d, reg) = setup();
        let t0 = Utc::now();
        reg.register(reg_input("c1"), t0).await.unwrap();
        // none stale right away
        assert!(reg.stale(Duration::seconds(10), t0).is_empty());
        // 20s later with a 10s ttl -> stale
        let stale = reg.stale(Duration::seconds(10), t0 + Duration::seconds(20));
        assert_eq!(stale, vec!["c1".to_string()]);
    }
}
