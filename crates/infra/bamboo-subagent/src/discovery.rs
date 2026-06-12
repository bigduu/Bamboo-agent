//! Tier-1 discovery: a process-independent, file-based fabric (design §4).
//!
//! Each actor `publish`es an [`AgentRecord`] as `<dir>/<agent_id>.json` (atomic temp+rename).
//! Others `discover` by scanning the directory and dropping stale records (lease expired).
//! Long-running service agents announce here; owned children use the Tier-2 registry instead.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use crate::error::{atomic_write, Result, StoreError};
use crate::proto::AgentRecord;

/// File-backed discovery fabric rooted at a directory (e.g. `$XDG_RUNTIME_DIR/bamboo/<key>/agents`).
pub struct Fabric {
    dir: PathBuf,
}

impl Fabric {
    pub fn at(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn record_path(&self, agent_id: &str) -> PathBuf {
        self.dir.join(format!("{agent_id}.json"))
    }

    /// Announce or refresh a record (atomic). Re-publishing with a bumped `lease_expires_at`
    /// is the renewal mechanism.
    pub async fn publish(&self, rec: &AgentRecord) -> Result<()> {
        let path = self.record_path(&rec.agent_id);
        let bytes = serde_json::to_vec_pretty(rec).map_err(|e| StoreError::decode(&path, e))?;
        atomic_write(&path, &bytes).await
    }

    /// Remove a record (clean shutdown).
    pub async fn withdraw(&self, agent_id: &str) -> Result<()> {
        match tokio::fs::remove_file(self.record_path(agent_id)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StoreError::io(self.record_path(agent_id), e)),
        }
    }

    /// Live records as of now (stale ones — lease expired — are filtered out).
    pub async fn discover(&self) -> Result<Vec<AgentRecord>> {
        self.discover_as_of(Utc::now()).await
    }

    /// Live records as of `now` (deterministic; used by tests). Sorted by `agent_id`.
    pub async fn discover_as_of(&self, now: DateTime<Utc>) -> Result<Vec<AgentRecord>> {
        let mut out = Vec::new();
        let mut rd = match tokio::fs::read_dir(&self.dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(StoreError::io(&self.dir, e)),
        };
        while let Some(ent) = rd.next_entry().await.map_err(|e| StoreError::io(&self.dir, e))? {
            let fname = ent.file_name().to_string_lossy().into_owned();
            if fname.starts_with('.') || !fname.ends_with(".json") {
                continue;
            }
            if let Some(rec) = read_record(&ent.path()).await? {
                if rec.lease_expires_at > now {
                    out.push(rec);
                }
            }
        }
        out.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        Ok(out)
    }

    /// Resolve one agent by id, if live as of now.
    pub async fn resolve(&self, agent_id: &str) -> Result<Option<AgentRecord>> {
        match read_record(&self.record_path(agent_id)).await? {
            Some(rec) if rec.lease_expires_at > Utc::now() => Ok(Some(rec)),
            _ => Ok(None),
        }
    }

    /// Delete stale record files; returns how many were removed.
    pub async fn gc(&self) -> Result<usize> {
        let now = Utc::now();
        let mut removed = 0;
        let mut rd = match tokio::fs::read_dir(&self.dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(StoreError::io(&self.dir, e)),
        };
        while let Some(ent) = rd.next_entry().await.map_err(|e| StoreError::io(&self.dir, e))? {
            let fname = ent.file_name().to_string_lossy().into_owned();
            if fname.starts_with('.') || !fname.ends_with(".json") {
                continue;
            }
            let stale = match read_record(&ent.path()).await {
                Ok(Some(rec)) => rec.lease_expires_at <= now,
                _ => true, // unreadable/corrupt -> treat as stale
            };
            if stale && tokio::fs::remove_file(ent.path()).await.is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

async fn read_record(path: &Path) -> Result<Option<AgentRecord>> {
    match tokio::fs::read(path).await {
        Ok(bytes) => match serde_json::from_slice(&bytes) {
            Ok(rec) => Ok(Some(rec)),
            Err(_) => Ok(None), // corrupt -> not discoverable
        },
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StoreError::io(path, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use tempfile::TempDir;

    fn rec(id: &str, expires: DateTime<Utc>) -> AgentRecord {
        AgentRecord {
            agent_id: id.into(),
            role: "service".into(),
            labels: vec![],
            endpoint: "ws://127.0.0.1:1".into(),
            pid: 1,
            version: "0".into(),
            started_at: Utc::now(),
            lease_expires_at: expires,
        }
    }

    #[tokio::test]
    async fn publish_discover_withdraw() {
        let d = TempDir::new().unwrap();
        let fab = Fabric::at(d.path());
        let now = Utc::now();
        fab.publish(&rec("a", now + Duration::seconds(30))).await.unwrap();
        fab.publish(&rec("b", now + Duration::seconds(30))).await.unwrap();

        let live = fab.discover_as_of(now).await.unwrap();
        assert_eq!(live.iter().map(|r| r.agent_id.clone()).collect::<Vec<_>>(), vec!["a", "b"]);
        assert!(fab.resolve("a").await.unwrap().is_some());

        fab.withdraw("a").await.unwrap();
        let live = fab.discover_as_of(now).await.unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].agent_id, "b");
    }

    #[tokio::test]
    async fn expired_lease_is_not_discovered_and_gc_removes_it() {
        let d = TempDir::new().unwrap();
        let fab = Fabric::at(d.path());
        let now = Utc::now();
        fab.publish(&rec("fresh", now + Duration::seconds(30))).await.unwrap();
        fab.publish(&rec("stale", now - Duration::seconds(1))).await.unwrap();

        let live = fab.discover_as_of(now).await.unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].agent_id, "fresh");
        assert!(fab.resolve("stale").await.unwrap().is_none());

        assert_eq!(fab.gc().await.unwrap(), 1); // stale removed
    }
}
