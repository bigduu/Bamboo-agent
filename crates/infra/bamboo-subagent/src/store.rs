//! Project-keyed session store + denormalized indices.
//!
//! Layout (see design §5.2):
//! ```text
//! <root>/projects/<key>/
//!   index.json                         ProjectIndex(roots + child_lookup)
//!   sessions/<parent-id>/
//!     session.json                     opaque session payload (authoritative)
//!     children.json                    ChildrenIndex (denormalized cache)
//!     mailbox/{new,cur,corrupt}/       see `mailbox`
//!     children/<child-id>/
//!       session.json                   opaque session payload (authoritative, isolated)
//!       mailbox/{new,cur,corrupt}/
//! ```
//!
//! Invariants:
//! - `session.json` is authoritative; `index.json` / `children.json` are caches, fully
//!   rebuildable via [`SubagentStore::rebuild_index`].
//! - Index files have a single writer (the registry); this type does not take cross-process locks.
//! - Every write is atomic (temp + rename). Aggregates are kept sorted by id for determinism.

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{atomic_write, Result, StoreError};
use crate::mailbox::Mailbox;

/// Stable encoding of a project's workspace path (mirrors `~/.claude/projects/<key>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectKey(String);

impl ProjectKey {
    /// Derive a filesystem-safe key from a workspace path. Deterministic.
    pub fn from_workspace(workspace: &Path) -> Self {
        let encoded: String = workspace
            .to_string_lossy()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect();
        ProjectKey(encoded)
    }

    /// Wrap an already-computed key verbatim.
    pub fn from_raw(key: impl Into<String>) -> Self {
        ProjectKey(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Logical location of a session: a root (parent) or a child under a parent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionLoc {
    Root {
        key: ProjectKey,
        session_id: String,
    },
    Child {
        key: ProjectKey,
        parent_id: String,
        child_id: String,
    },
}

/// Project index: all root sessions + an `child_id -> parent_id` lookup table (O(1) resolve).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectIndex {
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub roots: Vec<RootEntry>,
    #[serde(default)]
    pub child_lookup: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RootEntry {
    pub session_id: String,
    pub title: String,
    pub updated_at: DateTime<Utc>,
}

/// Per-parent index: a denormalized list of children for one-read listing.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ChildrenIndex {
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub children: Vec<ChildEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChildEntry {
    pub child_id: String,
    pub subagent_type: String,
    pub status: ChildStatus,
    pub title: String,
    pub responsibility: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildStatus {
    Pending,
    Running,
    Idle,
    Completed,
    Error,
    Cancelled,
}

/// Fields the index needs from a root session payload (for rebuild).
#[derive(Debug, Clone, PartialEq)]
pub struct RootFields {
    pub title: String,
    pub updated_at: DateTime<Utc>,
}

/// Fields the index needs from a child session payload (for rebuild).
#[derive(Debug, Clone, PartialEq)]
pub struct ChildFields {
    pub subagent_type: String,
    pub status: ChildStatus,
    pub title: String,
    pub responsibility: String,
    pub updated_at: DateTime<Utc>,
}

/// Decouples index rebuild from the (opaque) session payload shape: the caller knows how to
/// read its own session JSON, the store knows the directory structure.
pub trait MetaExtractor: Sync {
    fn root(&self, session_id: &str, payload: &serde_json::Value) -> RootFields;
    fn child(&self, child_id: &str, payload: &serde_json::Value) -> ChildFields;
}

/// Filesystem-backed store rooted at `<root>` (default `~/.bamboo`, injected for tests).
pub struct SubagentStore {
    root: PathBuf,
}

impl SubagentStore {
    pub fn open(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    // ---- path layout -------------------------------------------------------

    fn project_dir(&self, key: &ProjectKey) -> PathBuf {
        self.root.join("projects").join(key.as_str())
    }
    fn index_file(&self, key: &ProjectKey) -> PathBuf {
        self.project_dir(key).join("index.json")
    }
    fn sessions_dir(&self, key: &ProjectKey) -> PathBuf {
        self.project_dir(key).join("sessions")
    }
    fn parent_dir(&self, key: &ProjectKey, parent_id: &str) -> PathBuf {
        self.sessions_dir(key).join(parent_id)
    }
    fn children_index_file(&self, key: &ProjectKey, parent_id: &str) -> PathBuf {
        self.parent_dir(key, parent_id).join("children.json")
    }
    fn child_dir(&self, key: &ProjectKey, parent_id: &str, child_id: &str) -> PathBuf {
        self.parent_dir(key, parent_id).join("children").join(child_id)
    }
    fn session_dir(&self, loc: &SessionLoc) -> PathBuf {
        match loc {
            SessionLoc::Root { key, session_id } => self.parent_dir(key, session_id),
            SessionLoc::Child {
                key,
                parent_id,
                child_id,
            } => self.child_dir(key, parent_id, child_id),
        }
    }
    fn session_file(&self, loc: &SessionLoc) -> PathBuf {
        self.session_dir(loc).join("session.json")
    }

    /// Mailbox handle for the actor at `loc` (`<session_dir>/mailbox`).
    pub fn mailbox(&self, loc: &SessionLoc) -> Mailbox {
        Mailbox::at(self.session_dir(loc).join("mailbox"))
    }

    // ---- session payload (opaque, atomic) ---------------------------------

    pub async fn save_session<T: Serialize>(&self, loc: &SessionLoc, payload: &T) -> Result<()> {
        let path = self.session_file(loc);
        let bytes = serde_json::to_vec_pretty(payload).map_err(|e| StoreError::decode(&path, e))?;
        atomic_write(&path, &bytes).await
    }

    pub async fn load_session<T: DeserializeOwned>(&self, loc: &SessionLoc) -> Result<T> {
        let path = self.session_file(loc);
        let bytes = tokio::fs::read(&path)
            .await
            .map_err(|e| StoreError::io(&path, e))?;
        serde_json::from_slice(&bytes).map_err(|e| StoreError::decode(&path, e))
    }

    pub async fn session_exists(&self, loc: &SessionLoc) -> bool {
        tokio::fs::try_exists(self.session_file(loc))
            .await
            .unwrap_or(false)
    }

    // ---- index reads -------------------------------------------------------

    pub async fn list_roots(&self, key: &ProjectKey) -> Result<Vec<RootEntry>> {
        let idx: ProjectIndex = self.read_json(&self.index_file(key)).await?;
        Ok(idx.roots)
    }

    pub async fn list_children(
        &self,
        key: &ProjectKey,
        parent_id: &str,
    ) -> Result<Vec<ChildEntry>> {
        let idx: ChildrenIndex = self
            .read_json(&self.children_index_file(key, parent_id))
            .await?;
        Ok(idx.children)
    }

    /// O(1) resolve: consult the project `child_lookup` table.
    pub async fn resolve_child(
        &self,
        key: &ProjectKey,
        child_id: &str,
    ) -> Result<Option<SessionLoc>> {
        let idx: ProjectIndex = self.read_json(&self.index_file(key)).await?;
        Ok(idx.child_lookup.get(child_id).map(|parent_id| SessionLoc::Child {
            key: key.clone(),
            parent_id: parent_id.clone(),
            child_id: child_id.to_string(),
        }))
    }

    // ---- index writes (single-writer = registry) --------------------------

    pub async fn upsert_root(&self, key: &ProjectKey, entry: RootEntry) -> Result<()> {
        let path = self.index_file(key);
        let mut idx: ProjectIndex = self.read_json(&path).await?;
        match idx.roots.iter_mut().find(|r| r.session_id == entry.session_id) {
            Some(slot) => *slot = entry,
            None => idx.roots.push(entry),
        }
        idx.roots.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        self.write_json(&path, &idx).await
    }

    pub async fn upsert_child(
        &self,
        key: &ProjectKey,
        parent_id: &str,
        entry: ChildEntry,
    ) -> Result<()> {
        // 1. children.json (per parent)
        let cpath = self.children_index_file(key, parent_id);
        let mut cidx: ChildrenIndex = self.read_json(&cpath).await?;
        match cidx.children.iter_mut().find(|c| c.child_id == entry.child_id) {
            Some(slot) => *slot = entry.clone(),
            None => cidx.children.push(entry.clone()),
        }
        cidx.children.sort_by(|a, b| a.child_id.cmp(&b.child_id));
        self.write_json(&cpath, &cidx).await?;

        // 2. index.json child_lookup (written after, so a crash converges on rebuild)
        let ipath = self.index_file(key);
        let mut idx: ProjectIndex = self.read_json(&ipath).await?;
        idx.child_lookup.insert(entry.child_id, parent_id.to_string());
        self.write_json(&ipath, &idx).await
    }

    pub async fn remove_child(
        &self,
        key: &ProjectKey,
        parent_id: &str,
        child_id: &str,
    ) -> Result<()> {
        let cpath = self.children_index_file(key, parent_id);
        let mut cidx: ChildrenIndex = self.read_json(&cpath).await?;
        cidx.children.retain(|c| c.child_id != child_id);
        self.write_json(&cpath, &cidx).await?;

        let ipath = self.index_file(key);
        let mut idx: ProjectIndex = self.read_json(&ipath).await?;
        idx.child_lookup.remove(child_id);
        self.write_json(&ipath, &idx).await
    }

    // ---- self-heal ---------------------------------------------------------

    /// Rebuild `index.json` + every `children.json` by scanning the session payloads.
    /// Authoritative recovery path: safe to call any time; idempotent.
    pub async fn rebuild_index(
        &self,
        key: &ProjectKey,
        extractor: &dyn MetaExtractor,
    ) -> Result<()> {
        let sessions = self.sessions_dir(key);
        let mut idx = ProjectIndex::default();

        let mut parents = match tokio::fs::read_dir(&sessions).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                // No sessions yet: write an empty project index.
                return self.write_json(&self.index_file(key), &idx).await;
            }
            Err(e) => return Err(StoreError::io(&sessions, e)),
        };

        while let Some(p) = parents
            .next_entry()
            .await
            .map_err(|e| StoreError::io(&sessions, e))?
        {
            if !is_dir(&p).await {
                continue;
            }
            let parent_id = p.file_name().to_string_lossy().into_owned();

            if let Some(val) = self.try_read_value(&p.path().join("session.json")).await? {
                let rf = extractor.root(&parent_id, &val);
                idx.roots.push(RootEntry {
                    session_id: parent_id.clone(),
                    title: rf.title,
                    updated_at: rf.updated_at,
                });
            }

            // children
            let mut cidx = ChildrenIndex::default();
            let cdir = p.path().join("children");
            if let Ok(mut kids) = tokio::fs::read_dir(&cdir).await {
                while let Some(c) = kids
                    .next_entry()
                    .await
                    .map_err(|e| StoreError::io(&cdir, e))?
                {
                    if !is_dir(&c).await {
                        continue;
                    }
                    let child_id = c.file_name().to_string_lossy().into_owned();
                    if let Some(val) = self.try_read_value(&c.path().join("session.json")).await? {
                        let cf = extractor.child(&child_id, &val);
                        cidx.children.push(ChildEntry {
                            child_id: child_id.clone(),
                            subagent_type: cf.subagent_type,
                            status: cf.status,
                            title: cf.title,
                            responsibility: cf.responsibility,
                            updated_at: cf.updated_at,
                        });
                        idx.child_lookup.insert(child_id, parent_id.clone());
                    }
                }
            }
            cidx.children.sort_by(|a, b| a.child_id.cmp(&b.child_id));
            self.write_json(&self.children_index_file(key, &parent_id), &cidx)
                .await?;
        }

        idx.roots.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        self.write_json(&self.index_file(key), &idx).await
    }

    // ---- helpers -----------------------------------------------------------

    async fn read_json<T: DeserializeOwned + Default>(&self, path: &Path) -> Result<T> {
        match tokio::fs::read(path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| StoreError::decode(path, e)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(T::default()),
            Err(e) => Err(StoreError::io(path, e)),
        }
    }

    async fn try_read_value(&self, path: &Path) -> Result<Option<serde_json::Value>> {
        match tokio::fs::read(path).await {
            Ok(bytes) => {
                let v = serde_json::from_slice(&bytes).map_err(|e| StoreError::decode(path, e))?;
                Ok(Some(v))
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StoreError::io(path, e)),
        }
    }

    async fn write_json<T: Serialize>(&self, path: &Path, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(value).map_err(|e| StoreError::decode(path, e))?;
        atomic_write(path, &bytes).await
    }
}

async fn is_dir(entry: &tokio::fs::DirEntry) -> bool {
    match entry.file_type().await {
        Ok(ft) => ft.is_dir(),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;
    use tempfile::TempDir;

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    }

    fn key() -> ProjectKey {
        ProjectKey::from_raw("proj")
    }

    fn store() -> (TempDir, SubagentStore) {
        let dir = TempDir::new().unwrap();
        let store = SubagentStore::open(dir.path());
        (dir, store)
    }

    fn child_payload(title: &str, kind: &str, status: &str) -> serde_json::Value {
        json!({
            "title": title,
            "subagent_type": kind,
            "status": status,
            "responsibility": format!("do {title}"),
            "updated_at": ts().to_rfc3339(),
        })
    }

    /// Test extractor: read the index fields straight out of the opaque payload.
    struct Extract;
    impl MetaExtractor for Extract {
        fn root(&self, _id: &str, p: &serde_json::Value) -> RootFields {
            RootFields {
                title: p["title"].as_str().unwrap_or_default().to_string(),
                updated_at: parse_ts(&p["updated_at"]),
            }
        }
        fn child(&self, _id: &str, p: &serde_json::Value) -> ChildFields {
            ChildFields {
                subagent_type: p["subagent_type"].as_str().unwrap_or_default().to_string(),
                status: parse_status(p["status"].as_str().unwrap_or("pending")),
                title: p["title"].as_str().unwrap_or_default().to_string(),
                responsibility: p["responsibility"].as_str().unwrap_or_default().to_string(),
                updated_at: parse_ts(&p["updated_at"]),
            }
        }
    }
    fn parse_ts(v: &serde_json::Value) -> DateTime<Utc> {
        v.as_str()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(ts)
    }
    fn parse_status(s: &str) -> ChildStatus {
        match s {
            "running" => ChildStatus::Running,
            "idle" => ChildStatus::Idle,
            "completed" => ChildStatus::Completed,
            "error" => ChildStatus::Error,
            "cancelled" => ChildStatus::Cancelled,
            _ => ChildStatus::Pending,
        }
    }

    #[tokio::test]
    async fn session_round_trips() {
        let (_d, s) = store();
        let loc = SessionLoc::Root {
            key: key(),
            session_id: "p1".into(),
        };
        let payload = json!({"hello": "world", "n": 42});
        s.save_session(&loc, &payload).await.unwrap();
        assert!(s.session_exists(&loc).await);
        let got: serde_json::Value = s.load_session(&loc).await.unwrap();
        assert_eq!(got, payload);
    }

    #[tokio::test]
    async fn upsert_list_and_resolve_child() {
        let (_d, s) = store();
        let k = key();
        let entry = ChildEntry {
            child_id: "c1".into(),
            subagent_type: "researcher".into(),
            status: ChildStatus::Running,
            title: "t".into(),
            responsibility: "r".into(),
            updated_at: ts(),
        };
        s.upsert_child(&k, "p1", entry.clone()).await.unwrap();

        let listed = s.list_children(&k, "p1").await.unwrap();
        assert_eq!(listed, vec![entry]);

        let loc = s.resolve_child(&k, "c1").await.unwrap();
        assert_eq!(
            loc,
            Some(SessionLoc::Child {
                key: k.clone(),
                parent_id: "p1".into(),
                child_id: "c1".into(),
            })
        );
        assert_eq!(s.resolve_child(&k, "missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn upsert_replaces_in_place() {
        let (_d, s) = store();
        let k = key();
        let mut e = ChildEntry {
            child_id: "c1".into(),
            subagent_type: "x".into(),
            status: ChildStatus::Pending,
            title: "t".into(),
            responsibility: "r".into(),
            updated_at: ts(),
        };
        s.upsert_child(&k, "p1", e.clone()).await.unwrap();
        e.status = ChildStatus::Completed;
        s.upsert_child(&k, "p1", e.clone()).await.unwrap();
        let listed = s.list_children(&k, "p1").await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].status, ChildStatus::Completed);
    }

    #[tokio::test]
    async fn remove_child_clears_index_and_lookup() {
        let (_d, s) = store();
        let k = key();
        let e = ChildEntry {
            child_id: "c1".into(),
            subagent_type: "x".into(),
            status: ChildStatus::Pending,
            title: "t".into(),
            responsibility: "r".into(),
            updated_at: ts(),
        };
        s.upsert_child(&k, "p1", e).await.unwrap();
        s.remove_child(&k, "p1", "c1").await.unwrap();
        assert!(s.list_children(&k, "p1").await.unwrap().is_empty());
        assert_eq!(s.resolve_child(&k, "c1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn rebuild_matches_incremental() {
        let (_d, s) = store();
        let k = key();

        // Author session payloads (authoritative) + maintain indices incrementally.
        let root = SessionLoc::Root {
            key: k.clone(),
            session_id: "p1".into(),
        };
        s.save_session(&root, &json!({"title": "Parent", "updated_at": ts().to_rfc3339()}))
            .await
            .unwrap();
        s.upsert_root(
            &k,
            RootEntry {
                session_id: "p1".into(),
                title: "Parent".into(),
                updated_at: ts(),
            },
        )
        .await
        .unwrap();

        for (cid, kind) in [("c1", "researcher"), ("c2", "coder")] {
            let loc = SessionLoc::Child {
                key: k.clone(),
                parent_id: "p1".into(),
                child_id: cid.into(),
            };
            s.save_session(&loc, &child_payload(cid, kind, "running"))
                .await
                .unwrap();
            s.upsert_child(
                &k,
                "p1",
                ChildEntry {
                    child_id: cid.into(),
                    subagent_type: kind.into(),
                    status: ChildStatus::Running,
                    title: cid.into(),
                    responsibility: format!("do {cid}"),
                    updated_at: ts(),
                },
            )
            .await
            .unwrap();
        }

        let index_path = s.index_file(&k);
        let children_path = s.children_index_file(&k, "p1");
        let before_index: ProjectIndex = s.read_json(&index_path).await.unwrap();
        let before_children: ChildrenIndex = s.read_json(&children_path).await.unwrap();

        // Nuke the caches and rebuild from session payloads.
        tokio::fs::remove_file(&index_path).await.unwrap();
        tokio::fs::remove_file(&children_path).await.unwrap();
        s.rebuild_index(&k, &Extract).await.unwrap();

        let after_index: ProjectIndex = s.read_json(&index_path).await.unwrap();
        let after_children: ChildrenIndex = s.read_json(&children_path).await.unwrap();
        assert_eq!(after_index, before_index);
        assert_eq!(after_children, before_children);
    }

    #[tokio::test]
    async fn rebuild_converges_after_partial_write() {
        let (_d, s) = store();
        let k = key();
        let loc = SessionLoc::Child {
            key: k.clone(),
            parent_id: "p1".into(),
            child_id: "c1".into(),
        };
        // children.json written but index.json child_lookup missing (simulated crash mid-upsert).
        s.save_session(&loc, &child_payload("c1", "researcher", "running"))
            .await
            .unwrap();
        s.write_json(
            &s.children_index_file(&k, "p1"),
            &ChildrenIndex {
                version: 0,
                children: vec![ChildEntry {
                    child_id: "c1".into(),
                    subagent_type: "researcher".into(),
                    status: ChildStatus::Running,
                    title: "c1".into(),
                    responsibility: "do c1".into(),
                    updated_at: ts(),
                }],
            },
        )
        .await
        .unwrap();
        // lookup is empty -> resolve misses
        assert_eq!(s.resolve_child(&k, "c1").await.unwrap(), None);

        s.rebuild_index(&k, &Extract).await.unwrap();
        // now converged
        assert!(s.resolve_child(&k, "c1").await.unwrap().is_some());
    }
}
