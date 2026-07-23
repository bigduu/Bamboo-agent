//! Session storage V2 (folder-per-session + global index).
//!
//! Storage layout under `bamboo_home_dir`:
//! - `sessions.json` (global index, O(1) session_id -> rel_path)
//! - `sessions/<root_id>/session.json`
//! - `sessions/<root_id>/children/<child_id>/session.json`
//! - `.../attachments/` (files; session.json stores references, never base64)
//!
//! Notes:
//! - This is a greenfield format (no migration). Old on-disk layouts are ignored.
//! - The global index is a rebuildable cache, not the source of truth. Each
//!   `session.json` is authoritative; the index only speeds up lookups. A
//!   *missing* `sessions.json` starts an empty index; a *corrupt/unparseable*
//!   one is backed up to `sessions.json.bak` and the index is rebuilt by
//!   scanning `sessions/<root>/[children/<child>/]session.json` (see
//!   [`SessionStoreV2::rebuild_index_from_disk`]) so a bad index is never
//!   boot-fatal and never orphans intact sessions. Directory scanning is used
//!   only for this recovery path, never in hot paths.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};

use base64::Engine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use bamboo_domain::ProviderModelRef;
use bamboo_domain::ReasoningEffort;
use bamboo_domain::{ProjectId, Role, Session, SessionKind, TokenBudgetUsage};

use crate::search_index::{should_index_session, SessionSearchIndex};
use bamboo_domain::AttachmentReader;
use bamboo_domain::Storage;

pub(crate) fn other_io_error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

/// Filename of the runtime control-plane sidecar, stored alongside
/// `session.json` in each session directory.
const RUNTIME_SIDECAR_FILE: &str = "runtime.json";
const SESSIONS_INDEX_VERSION: u32 = 4;

/// Filename of the append-only per-LLM-call token-usage log, stored alongside
/// `session.json` in each session directory. One JSON line per call.
const TOKEN_USAGE_FILE: &str = "token-usage.jsonl";

/// Marker (under `bamboo_home_dir`) recording that the one-shot runtime sidecar
/// migration has completed, so it is skipped on subsequent boots.
const RUNTIME_SIDECAR_MIGRATION_MARKER: &str = ".runtime_sidecar_migrated";

/// Build the sidecar snapshot: the full session minus its `messages` history.
/// Every field except `messages` is authoritative in the sidecar; on load the
/// message history is taken back from `session.json`.
fn runtime_sidecar_snapshot(session: &Session) -> Session {
    let mut snapshot = session.clone();
    snapshot.messages.clear();
    snapshot
}

/// Overlay the runtime sidecar onto the session loaded from `session.json`.
///
/// The sidecar holds the freshest control-plane (metadata, `agent_runtime_state`,
/// title group, …) because every save — full or runtime-only — writes it. The
/// large `messages` history is only ever written by full saves into
/// `session.json`, so it is preserved from `main`.
fn overlay_runtime_sidecar(main: Session, sidecar: Option<Session>) -> Session {
    match sidecar {
        Some(mut side) => {
            side.messages = main.messages;
            side
        }
        None => main,
    }
}

/// Normalize the persisted compatibility metadata through the authoritative
/// typed Project id parser before mirroring it into the rebuildable index.
/// Malformed legacy metadata is isolated to that session instead of poisoning
/// the entire session index.
fn normalized_project_id(session: &Session) -> Option<String> {
    let raw = session.project_id_meta()?;
    match raw.trim().parse::<ProjectId>() {
        Ok(project_id) => Some(project_id.into_string()),
        Err(error) => {
            tracing::warn!(
                session_id = %session.id,
                %error,
                "ignoring malformed Project id while updating session index"
            );
            None
        }
    }
}

/// Reject a session id that could escape the storage directory (empty, or
/// containing a path separator or `..`). Shared with [`crate::jsonl`] so every
/// store applies the same guard. #31.
pub(crate) fn validate_session_id(session_id: &str) -> io::Result<()> {
    if session_id.is_empty()
        || session_id.contains('/')
        || session_id.contains('\\')
        || session_id.contains("..")
    {
        return Err(other_io_error(format!("invalid session id: {session_id}")));
    }
    Ok(())
}

/// Where a session's agent physically runs: the deployment kind plus the host.
/// Mirrored into the index from `session.metadata["placement"]` so the frontend
/// can show "which machine this session runs on" without loading session.json.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionPlacement {
    /// Deployment kind: `"local"` (this backend's own host), `"docker"`, or
    /// `"ssh"` (a remote node the child was deployed to).
    pub kind: String,
    /// Host the agent runs on — the backend's hostname for `local`, or the
    /// target host for a remote/ssh deployment.
    pub host: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionIndexEntry {
    pub id: String,
    pub kind: SessionKind,
    /// Path relative to `bamboo_home_dir` (e.g. "sessions/<id>" or "sessions/<root>/children/<id>").
    pub rel_path: String,
    pub title: String,
    #[serde(default)]
    pub title_version: u64,
    pub pinned: bool,
    pub parent_session_id: Option<String>,
    pub root_session_id: String,
    pub spawn_depth: u32,
    #[serde(default)]
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_ref: Option<ProviderModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Workspace path mirrored from the session's typed runtime metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
    /// Stable Project identity mirrored from typed session runtime metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Raw session-level Gold config JSON mirrored from `session.metadata["gold_config"]`.
    /// Kept as a string here to avoid making infrastructure depend on bamboo-engine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gold_config_json: Option<String>,
    /// If the session was created by a schedule, store the schedule id here for fast filtering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_schedule_id: Option<String>,
    /// If the session was created by a specific schedule run, keep the run id here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_run_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_activity_at: DateTime<Utc>,
    pub message_count: usize,
    pub has_attachments: bool,
    /// Whether the session currently has a pending question awaiting user response.
    /// Mirrored into the index from `session.has_pending_question()` so the frontend
    /// can display the question dialog badge without loading session.json.
    #[serde(default)]
    pub has_pending_question: bool,
    /// Active plan mode runtime state mirrored into the index from
    /// `session.agent_runtime_state.plan_mode`, so lightweight session-list/detail
    /// APIs can surface plan mode without loading every session.json.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_mode: Option<bamboo_domain::PlanModeState>,
    /// Per-session "bypass permissions" toggle mirrored into the index from
    /// `session.agent_runtime_state.bypass_permissions`, so the session-list API
    /// can surface it without loading every session.json.
    #[serde(default)]
    pub bypass_permissions: bool,
    /// Last known run status for this session
    /// ("pending" | "running" | "completed" | "error" | "cancelled" | "skipped").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_status: Option<String>,
    /// Last known terminal error message, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_error: Option<String>,
    /// Last token usage information (updated after each LLM call).
    ///
    /// Stored in the global index so the frontend can display token usage without
    /// loading full session.json for every row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<TokenBudgetUsage>,
    /// SubAgent profile id for child sessions spawned by `SubAgent.create`.
    /// Mirrored into the index from `session.metadata["subagent_type"]` so the
    /// frontend can render role badges (e.g. "general-purpose", "plan") on the
    /// child-session list without loading each session.json.
    /// `None` for root sessions and for legacy children created before this
    /// field was introduced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagent_type: Option<String>,
    /// Child lifecycle: `Some("resident")` for a reusable resident agent (a
    /// stable session reused for successive tasks); `None`/absent for the
    /// default one-shot child. Mirrored from `session.metadata["lifecycle"]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<String>,
    /// For a resident agent, the stable reuse key (scoped to `root_session_id`).
    /// Mirrored from `session.metadata["resident_name"]`; lets a later
    /// `SubAgent.create` find and reuse the resident without loading session.json.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resident_name: Option<String>,
    /// Where this session's agent physically runs (deployment kind + host).
    /// Mirrored from `session.metadata["placement"]`. `None` for legacy rows and
    /// for local sessions that were never stamped; the session DTO layer defaults
    /// `None` to the backend's own local host so the frontend always has a value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<SessionPlacement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionsIndex {
    pub version: u32,
    pub updated_at: DateTime<Utc>,
    pub sessions: HashMap<String, SessionIndexEntry>,
}

impl SessionsIndex {
    fn empty() -> Self {
        Self {
            version: SESSIONS_INDEX_VERSION,
            updated_at: Utc::now(),
            sessions: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct SessionStoreV2 {
    bamboo_home_dir: PathBuf,
    sessions_dir: PathBuf,
    index_path: PathBuf,
    search_index: SessionSearchIndex,
    index: RwLock<SessionsIndex>,
    /// Serializes on-disk index writes (and any multi-step operations that must be atomic-ish).
    write_lock: Mutex<()>,
}

impl SessionStoreV2 {
    /// Open (or create) the V2 session store rooted at `bamboo_home_dir`.
    ///
    /// Loading the global index (`sessions.json`) is fault-tolerant, because it
    /// is only a cache over the authoritative per-session `session.json` files:
    /// - **missing** → start with a fresh empty index (normal first boot);
    /// - **valid** → use it as-is;
    /// - **corrupt/unparseable** → back it up to `sessions.json.bak`, log an
    ///   error, start empty, and rebuild the index from disk (see
    ///   [`Self::rebuild_index_from_disk`]) so a single bad byte can never make
    ///   the server refuse to boot or orphan intact sessions on disk.
    pub async fn new(bamboo_home_dir: PathBuf) -> io::Result<Self> {
        let sessions_dir = bamboo_home_dir.join("sessions");
        let index_path = bamboo_home_dir.join("sessions.json");
        let search_index = SessionSearchIndex::new(bamboo_home_dir.join("session_search.db"));

        fs::create_dir_all(&sessions_dir).await?;
        search_index.init().await?;

        // A corrupt index must not be boot-fatal: back it up and rebuild from
        // the on-disk session tree after construction. Only a *corrupt* file
        // triggers this; a *missing* one keeps the fresh-empty-index path.
        let mut needs_rebuild = false;
        let index = if index_path.exists() {
            let raw = fs::read_to_string(&index_path).await?;
            match serde_json::from_str::<SessionsIndex>(&raw) {
                Ok(index) if index.version >= SESSIONS_INDEX_VERSION => index,
                Ok(index) => {
                    tracing::info!(
                        "migrating sessions index from version {} to version {} by rebuilding from session.json",
                        index.version,
                        SESSIONS_INDEX_VERSION,
                    );
                    needs_rebuild = true;
                    let mut rebuilding = SessionsIndex::empty();
                    // Keep an old-version marker on every incremental rebuild
                    // persist. If the process crashes mid-scan, the next boot
                    // must resume instead of accepting a partial current index.
                    rebuilding.version = index.version.min(SESSIONS_INDEX_VERSION - 1);
                    rebuilding
                }
                Err(error) => {
                    // Best-effort backup so the corrupt bytes are preserved for
                    // forensics but no longer block the (about to be rebuilt)
                    // index. If the backup rename fails we still rebuild — the
                    // rebuild's fresh persist would overwrite the corrupt file
                    // anyway, and the session.json files remain untouched.
                    // NOTE: `fs::rename` clobbers any pre-existing
                    // `sessions.json.bak` (only the latest corruption is kept) —
                    // an accepted tradeoff for the recovery path.
                    let backup_path = bamboo_home_dir.join("sessions.json.bak");
                    match fs::rename(&index_path, &backup_path).await {
                        Ok(()) => tracing::error!(
                            "sessions.json is corrupt ({error}); backed up to {} and rebuilding \
                             the index by scanning the session tree",
                            backup_path.display()
                        ),
                        Err(rename_error) => tracing::error!(
                            "sessions.json is corrupt ({error}); failed to back it up to {} \
                             ({rename_error}); rebuilding the index from disk anyway",
                            backup_path.display()
                        ),
                    }
                    needs_rebuild = true;
                    let mut rebuilding = SessionsIndex::empty();
                    rebuilding.version = 0;
                    rebuilding
                }
            }
        } else {
            let index = SessionsIndex::empty();
            // Persist immediately so "index is mandatory" holds from boot.
            let tmp = index_path.with_extension(format!("json.tmp.{}", Uuid::new_v4()));
            fs::write(
                &tmp,
                serde_json::to_vec_pretty(&index).map_err(|e| other_io_error(e.to_string()))?,
            )
            .await?;
            atomic_rename(&tmp, &index_path).await?;
            index
        };

        let storage = Self {
            bamboo_home_dir,
            sessions_dir,
            index_path,
            search_index,
            index: RwLock::new(index),
            write_lock: Mutex::new(()),
        };

        if needs_rebuild {
            storage.rebuild_index_from_disk().await?;
        }

        Ok(storage)
    }

    /// Rebuild the global index by scanning the on-disk session tree.
    ///
    /// Called by [`Self::new`] after a corrupt `sessions.json` was backed up and
    /// replaced with an empty index. The layout is deterministic:
    /// - `sessions/<root_id>/session.json`
    /// - `sessions/<root_id>/children/<child_id>/session.json`
    ///
    /// so every session is recoverable without the index. Each session is loaded
    /// from its directory via [`Self::load_session_from_dir`] — which parses
    /// `session.json` and **overlays the `runtime.json` sidecar exactly like
    /// [`Storage::load_session`]**, so recovered index entries reflect the
    /// freshest control-plane (a runtime-only save updates only the sidecar) and
    /// agree with the FTS index that [`Self::rebuild_search_index`] builds via
    /// `load_session`. The result is folded back in via
    /// [`Self::upsert_index_from_session`] with the same `rel_path`
    /// [`Self::save_session`] would compute — derived from the on-disk directory
    /// names (the physical location), which is what `abs_path_from_rel` + load
    /// rely on. A single unreadable/corrupt session is skipped with a warning,
    /// and directory-level read errors are logged + tolerated (never
    /// `?`-propagated) so one bad file/dir never re-introduces a boot-fatal
    /// failure or aborts recovery of the rest.
    async fn rebuild_index_from_disk(&self) -> io::Result<()> {
        let mut recovered = 0usize;

        let mut root_dirs = match fs::read_dir(&self.sessions_dir).await {
            Ok(rd) => rd,
            // No sessions directory at all — nothing to recover.
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };

        loop {
            // Directory-iteration errors are tolerated, not `?`-propagated: an
            // error here would abort the whole rebuild and make `new()` return
            // Err — the exact boot-fatal failure this rebuild exists to prevent.
            let root_entry = match root_dirs.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!("index rebuild: error scanning sessions dir: {error}");
                    break;
                }
            };
            if !root_entry
                .file_type()
                .await
                .map(|t| t.is_dir())
                .unwrap_or(false)
            {
                continue;
            }
            let Ok(root_id) = root_entry.file_name().into_string() else {
                // Non-UTF-8 directory name cannot be a valid session id.
                continue;
            };

            // Recover the root session (if its session.json is present + valid).
            if let Some(session) = Self::load_session_from_dir(&root_entry.path(), &root_id).await {
                let rel_path = Self::root_rel_path(&root_id);
                match self.upsert_index_from_session(&session, rel_path).await {
                    Ok(()) => recovered += 1,
                    Err(error) => {
                        tracing::warn!("index rebuild: failed to index root {root_id}: {error}")
                    }
                }
            }

            // Recover its children (a flat `children/<child_id>/` layer).
            let children_dir = root_entry.path().join("children");
            let mut child_dirs = match fs::read_dir(&children_dir).await {
                Ok(rd) => rd,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => {
                    tracing::warn!("index rebuild: cannot read children of {root_id}: {error}");
                    continue;
                }
            };
            loop {
                let child_entry = match child_dirs.next_entry().await {
                    Ok(Some(entry)) => entry,
                    Ok(None) => break,
                    Err(error) => {
                        tracing::warn!(
                            "index rebuild: error scanning children of {root_id}: {error}"
                        );
                        break;
                    }
                };
                if !child_entry
                    .file_type()
                    .await
                    .map(|t| t.is_dir())
                    .unwrap_or(false)
                {
                    continue;
                }
                let Ok(child_id) = child_entry.file_name().into_string() else {
                    continue;
                };
                if let Some(session) =
                    Self::load_session_from_dir(&child_entry.path(), &child_id).await
                {
                    let rel_path = Self::child_rel_path(&root_id, &child_id);
                    match self.upsert_index_from_session(&session, rel_path).await {
                        Ok(()) => recovered += 1,
                        Err(error) => tracing::warn!(
                            "index rebuild: failed to index child {child_id}: {error}"
                        ),
                    }
                }
            }
        }

        // Re-materialize sessions.json even when nothing was recovered (we may
        // have renamed the only copy to sessions.json.bak), so the "index file
        // always exists after boot" invariant holds.
        self.update_index(|index| {
            // Publishing the current version is the commit point for a complete rebuild.
            // `persist_index_locked` writes a temp file and atomically renames it.
            index.version = SESSIONS_INDEX_VERSION;
            Ok(())
        })
        .await?;

        tracing::info!("index rebuild from disk complete: recovered {recovered} session(s)");

        // Rebuild the FTS index from the freshly recovered sessions.
        if let Err(error) = self.rebuild_search_index().await {
            tracing::warn!("index rebuild: failed to rebuild search index: {error}");
        }
        Ok(())
    }

    /// Load a session from a known on-disk directory during index rebuild,
    /// mirroring [`Storage::load_session`] but resolving the directory by scan
    /// (the index is not yet populated during rebuild): parse `session.json`,
    /// overlay the `runtime.json` sidecar via the shared
    /// [`Self::read_runtime_sidecar_at`] + [`overlay_runtime_sidecar`] so the
    /// freshest control-plane wins, then drop a stale Root token_budget. A
    /// missing `session.json` yields `None` silently; a corrupt/unreadable one is
    /// skipped with a warning; a sidecar read error degrades to "no sidecar"
    /// rather than failing recovery. `id` is used only for log context.
    async fn load_session_from_dir(abs_dir: &Path, id: &str) -> Option<Session> {
        let raw = match fs::read_to_string(abs_dir.join("session.json")).await {
            Ok(raw) => raw,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
            Err(error) => {
                tracing::warn!("index rebuild: skipping unreadable session {id}: {error}");
                return None;
            }
        };
        let main: Session = match serde_json::from_str(&raw) {
            Ok(session) => session,
            Err(error) => {
                tracing::warn!("index rebuild: skipping corrupt session {id}: {error}");
                return None;
            }
        };
        let sidecar =
            match Self::read_runtime_sidecar_at(&abs_dir.join(RUNTIME_SIDECAR_FILE), id).await {
                Ok(sidecar) => sidecar,
                Err(error) => {
                    tracing::warn!("index rebuild: cannot read runtime sidecar for {id}: {error}");
                    None
                }
            };
        let mut session = overlay_runtime_sidecar(main, sidecar);
        session.clear_stale_root_token_budget();
        Some(session)
    }

    pub fn search_index(&self) -> &SessionSearchIndex {
        &self.search_index
    }

    pub fn bamboo_home_dir(&self) -> &Path {
        &self.bamboo_home_dir
    }

    pub fn index_path(&self) -> &Path {
        &self.index_path
    }

    pub async fn rebuild_search_index(&self) -> io::Result<()> {
        let session_ids = {
            let index = self.index.read().await;
            index.sessions.keys().cloned().collect::<Vec<_>>()
        };
        for session_id in session_ids {
            if let Some(session) = self.load_session(&session_id).await? {
                if !should_index_session(session.updated_at) {
                    continue;
                }
                if let Err(error) = self.search_index.upsert_session(&session).await {
                    tracing::warn!(
                        "failed to rebuild search index entry for {}: {}",
                        session_id,
                        error
                    );
                }
            }
        }
        Ok(())
    }

    pub fn sessions_root_dir(&self) -> &Path {
        &self.sessions_dir
    }

    fn root_rel_path(session_id: &str) -> String {
        format!("sessions/{session_id}")
    }

    fn child_rel_path(root_id: &str, child_id: &str) -> String {
        format!("sessions/{root_id}/children/{child_id}")
    }

    fn abs_path_from_rel(&self, rel: &str) -> PathBuf {
        self.bamboo_home_dir.join(rel)
    }

    async fn persist_index_locked(&self, index: &SessionsIndex) -> io::Result<()> {
        let tmp = self
            .index_path
            .with_extension(format!("json.tmp.{}", Uuid::new_v4()));
        let bytes = serde_json::to_vec_pretty(index).map_err(|e| other_io_error(e.to_string()))?;
        fs::write(&tmp, bytes).await?;
        atomic_rename(&tmp, &self.index_path).await?;
        Ok(())
    }

    async fn update_index<F, T>(&self, f: F) -> io::Result<T>
    where
        F: FnOnce(&mut SessionsIndex) -> io::Result<T>,
    {
        let _guard = self.write_lock.lock().await;
        let mut index = self.index.write().await;
        let out = f(&mut index)?;
        index.updated_at = Utc::now();
        self.persist_index_locked(&index).await?;
        Ok(out)
    }

    pub async fn list_index_entries(&self) -> Vec<SessionIndexEntry> {
        let index = self.index.read().await;
        let mut items: Vec<_> = index.sessions.values().cloned().collect();
        items.sort_by_key(|b| std::cmp::Reverse(b.updated_at));
        items
    }

    pub async fn get_index_entry(&self, session_id: &str) -> Option<SessionIndexEntry> {
        let index = self.index.read().await;
        index.sessions.get(session_id).cloned()
    }

    pub async fn resolve_rel_path(&self, session_id: &str) -> Option<String> {
        self.get_index_entry(session_id).await.map(|e| e.rel_path)
    }

    async fn ensure_session_dirs(&self, session: &Session) -> io::Result<String> {
        validate_session_id(&session.id)?;

        let rel_path = match session.kind {
            SessionKind::Root => Self::root_rel_path(&session.id),
            SessionKind::Child => {
                let root_id = session.root_session_id.trim();
                let parent_id = session.parent_session_id.as_deref().unwrap_or("").trim();
                if root_id.is_empty() || parent_id.is_empty() {
                    return Err(other_io_error(
                        "child session missing root_session_id/parent_session_id",
                    ));
                }
                // Nesting is allowed: a child's parent may itself be a child.
                // All descendants live flat under the tree root's directory
                // (`child_rel_path` keys on `root_id`, which stays constant for
                // the whole tree), so depth needs no path change.
                validate_session_id(root_id)?;
                Self::child_rel_path(root_id, &session.id)
            }
        };

        let abs_dir = self.abs_path_from_rel(&rel_path);
        fs::create_dir_all(&abs_dir).await?;
        // Ensure expected subdirs (lazy; cheap).
        fs::create_dir_all(abs_dir.join("attachments")).await?;
        if session.kind == SessionKind::Root {
            fs::create_dir_all(abs_dir.join("children")).await?;
        }
        Ok(rel_path)
    }

    async fn session_json_path(&self, session_id: &str) -> io::Result<Option<PathBuf>> {
        if let Some(rel) = self.resolve_rel_path(session_id).await {
            Ok(Some(self.abs_path_from_rel(&rel).join("session.json")))
        } else {
            Ok(None)
        }
    }

    async fn runtime_json_path(&self, session_id: &str) -> io::Result<Option<PathBuf>> {
        if let Some(rel) = self.resolve_rel_path(session_id).await {
            Ok(Some(
                self.abs_path_from_rel(&rel).join(RUNTIME_SIDECAR_FILE),
            ))
        } else {
            Ok(None)
        }
    }

    /// Write the runtime control-plane sidecar: a full session snapshot with the
    /// (potentially huge) `messages` history cleared. This is what makes
    /// runtime-only saves O(1) in conversation length.
    async fn write_runtime_sidecar(&self, abs_dir: &Path, session: &Session) -> io::Result<()> {
        let path = abs_dir.join(RUNTIME_SIDECAR_FILE);
        let snapshot = runtime_sidecar_snapshot(session);
        let tmp = path.with_extension(format!("json.tmp.{}", Uuid::new_v4()));
        let bytes =
            serde_json::to_vec_pretty(&snapshot).map_err(|e| other_io_error(e.to_string()))?;
        fs::write(&tmp, bytes).await?;
        atomic_rename(&tmp, &path).await?;
        Ok(())
    }

    /// One-shot migration: create the runtime sidecar (`runtime.json`) for every
    /// existing session that predates the message/control-plane split.
    ///
    /// Loading already tolerates a missing sidecar (it falls back to the embedded
    /// control-plane in `session.json`), so this is an *optimization* migration,
    /// not a correctness one — but running it once means the fast runtime-save
    /// path is in effect immediately for legacy sessions, and the denormalized
    /// `children` id vectors (now `#[serde(skip)]`) drop out of the sidecar.
    ///
    /// Idempotent and cheap on later boots: guarded by a marker file, and any
    /// session that already has a sidecar is skipped. Returns the number of
    /// sidecars created.
    pub async fn migrate_runtime_sidecars(&self) -> io::Result<usize> {
        let marker = self.bamboo_home_dir.join(RUNTIME_SIDECAR_MIGRATION_MARKER);
        if fs::try_exists(&marker).await.unwrap_or(false) {
            return Ok(0);
        }

        let entries = self.list_index_entries().await;
        let mut migrated = 0usize;
        for entry in entries {
            let abs_dir = self.abs_path_from_rel(&entry.rel_path);
            let sidecar_path = abs_dir.join(RUNTIME_SIDECAR_FILE);
            if fs::try_exists(&sidecar_path).await.unwrap_or(false) {
                continue;
            }
            let session_path = abs_dir.join("session.json");
            // Read session.json directly (not load_session) — there is no sidecar
            // to overlay yet, and we want the raw embedded control-plane.
            let raw = match fs::read_to_string(&session_path).await {
                Ok(raw) => raw,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            let session: Session = match serde_json::from_str(&raw) {
                Ok(session) => session,
                Err(error) => {
                    tracing::warn!(
                        "runtime sidecar migration: skipping unreadable session {}: {}",
                        entry.id,
                        error
                    );
                    continue;
                }
            };
            self.write_runtime_sidecar(&abs_dir, &session).await?;
            migrated += 1;
        }

        // Persist the marker last, atomically, so an interrupted migration simply
        // re-runs (it is idempotent) instead of being falsely marked complete.
        let tmp = marker.with_extension(format!("tmp.{}", Uuid::new_v4()));
        fs::write(&tmp, b"runtime-sidecar-v1\n").await?;
        atomic_rename(&tmp, &marker).await?;

        if migrated > 0 {
            tracing::info!("runtime sidecar migration: created {migrated} sidecar(s)");
        }
        Ok(migrated)
    }

    /// Read the runtime sidecar (a Session snapshot with empty `messages`), if it
    /// exists. Returns `None` when the session has no sidecar yet (e.g. legacy
    /// sessions not yet migrated). Path is resolved through the index.
    async fn read_runtime_sidecar(&self, session_id: &str) -> io::Result<Option<Session>> {
        let Some(path) = self.runtime_json_path(session_id).await? else {
            return Ok(None);
        };
        Self::read_runtime_sidecar_at(&path, session_id).await
    }

    /// Read + deserialize a runtime sidecar (`runtime.json`) from a known path.
    /// A missing file yields `None`; a corrupt one is ignored with a warning
    /// (the authoritative copy still lives in `session.json`). Shared by
    /// [`Self::read_runtime_sidecar`] (index-resolved path) and the index
    /// rebuild (directory-scanned path) so both overlay the sidecar identically.
    async fn read_runtime_sidecar_at(path: &Path, id: &str) -> io::Result<Option<Session>> {
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path).await?;
        match serde_json::from_str::<Session>(&raw) {
            Ok(mut side) => {
                // The control-plane path (`load_runtime_control_plane`) returns
                // this directly, so migrate a stale Root token_budget here too (#230).
                side.clear_stale_root_token_budget();
                Ok(Some(side))
            }
            Err(error) => {
                // A corrupt sidecar must never make a session unloadable — the
                // authoritative copy still lives in session.json. Warn and ignore.
                tracing::warn!("ignoring corrupt runtime sidecar for {id}: {error}");
                Ok(None)
            }
        }
    }

    async fn attachments_dir(&self, session_id: &str) -> io::Result<Option<PathBuf>> {
        if let Some(rel) = self.resolve_rel_path(session_id).await {
            Ok(Some(self.abs_path_from_rel(&rel).join("attachments")))
        } else {
            Ok(None)
        }
    }

    async fn compute_has_attachments(&self, session_id: &str) -> bool {
        let Ok(Some(dir)) = self.attachments_dir(session_id).await else {
            return false;
        };
        let Ok(mut rd) = fs::read_dir(dir).await else {
            return false;
        };
        rd.next_entry().await.ok().flatten().is_some()
    }

    async fn upsert_index_from_session(
        &self,
        session: &Session,
        rel_path: String,
    ) -> io::Result<()> {
        let has_attachments = self.compute_has_attachments(&session.id).await;
        // Read the well-known runtime keys via the typed accessors, which prefer
        // `runtime_metadata` and fall back to the legacy `metadata` strings.
        let last_run_status = session
            .last_run_status()
            .filter(|value| !value.trim().is_empty());
        let last_run_error = session
            .last_run_error()
            .filter(|value| !value.trim().is_empty());
        let created_by_schedule_id = session
            .metadata
            .get("created_by_schedule_id")
            .cloned()
            .filter(|v| !v.trim().is_empty());
        let schedule_run_id = session
            .metadata
            .get("schedule_run_id")
            .cloned()
            .filter(|v| !v.trim().is_empty());
        let subagent_type = session.subagent_type().filter(|v| !v.trim().is_empty());
        let lifecycle = session
            .metadata
            .get("lifecycle")
            .cloned()
            .filter(|v| !v.trim().is_empty());
        let resident_name = session
            .metadata
            .get("resident_name")
            .cloned()
            .filter(|v| !v.trim().is_empty());
        let gold_config_json = session
            .metadata
            .get("gold_config")
            .cloned()
            .filter(|v| !v.trim().is_empty());
        let plan_mode = session
            .agent_runtime_state
            .as_ref()
            .and_then(|state| state.plan_mode.clone());
        let bypass_permissions = session
            .agent_runtime_state
            .as_ref()
            .is_some_and(|state| state.bypass_permissions);
        // Placement (which machine the agent runs on) is stamped by the spawn
        // path into `metadata["placement"]` as a JSON `{kind,host}` object for
        // remote/deployed children; local sessions leave it unset and the DTO
        // layer defaults them to this backend's own host.
        let placement = session
            .metadata
            .get("placement")
            .and_then(|v| serde_json::from_str::<SessionPlacement>(v).ok());
        let workspace_path = session
            .workspace_path_meta()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let project_id = normalized_project_id(session);
        self.update_index(|index| {
            index.sessions.insert(
                session.id.clone(),
                SessionIndexEntry {
                    id: session.id.clone(),
                    kind: session.kind,
                    rel_path,
                    title: session.title.clone(),
                    title_version: session.title_version,
                    pinned: session.pinned,
                    parent_session_id: session.parent_session_id.clone(),
                    root_session_id: session.root_session_id.clone(),
                    spawn_depth: session.spawn_depth,
                    model: session.model.clone(),
                    model_ref: session.model_ref.clone(),
                    reasoning_effort: session.reasoning_effort,
                    workspace_path,
                    project_id,
                    gold_config_json,
                    created_by_schedule_id,
                    schedule_run_id,
                    created_at: session.created_at,
                    updated_at: session.updated_at,
                    last_activity_at: session.updated_at,
                    message_count: session.messages.len(),
                    has_attachments,
                    has_pending_question: session.has_pending_question(),
                    plan_mode,
                    bypass_permissions,
                    last_run_status,
                    last_run_error,
                    token_usage: session.token_usage.clone(),
                    subagent_type,
                    lifecycle,
                    resident_name,
                    placement,
                },
            );
            Ok(())
        })
        .await?;
        Ok(())
    }

    pub async fn write_image_attachment(
        &self,
        session: &Session,
        raw_base64_or_data_url: &str,
        mime_hint: Option<&str>,
    ) -> io::Result<(String, String)> {
        let (mime, base64_data) =
            parse_data_url_base64(raw_base64_or_data_url).unwrap_or_else(|| {
                (
                    mime_hint.unwrap_or("image/png").trim().to_string(),
                    raw_base64_or_data_url.trim().to_string(),
                )
            });

        let bytes = base64::engine::general_purpose::STANDARD
            .decode(base64_data.as_bytes())
            .map_err(|e| other_io_error(format!("invalid base64 image data: {e}")))?;

        let attachment_id = Uuid::new_v4().to_string();
        let ext = mime_to_extension(mime.as_str()).unwrap_or("bin");

        let rel_path = self.ensure_session_dirs(session).await?;
        let abs_dir = self.abs_path_from_rel(&rel_path);
        let attachments_dir = abs_dir.join("attachments");
        fs::create_dir_all(&attachments_dir).await?;

        let path = attachments_dir.join(format!("{attachment_id}.{ext}"));
        let tmp = path.with_extension(format!("{ext}.tmp.{}", Uuid::new_v4()));
        fs::write(&tmp, &bytes).await?;
        atomic_rename(&tmp, &path).await?;

        Ok((
            attachment_id.clone(),
            format!("bamboo-attachment://{}/{}", session.id, attachment_id),
        ))
    }

    /// Read an attachment by id, returning bytes + inferred MIME.
    pub async fn read_attachment(
        &self,
        session_id: &str,
        attachment_id: &str,
    ) -> io::Result<Option<(Vec<u8>, String)>> {
        validate_session_id(session_id)?;
        validate_session_id(attachment_id)?;
        let Some(dir) = self.attachments_dir(session_id).await? else {
            return Ok(None);
        };
        if !dir.exists() {
            return Ok(None);
        }

        let mut rd = fs::read_dir(&dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if !file_name.starts_with(attachment_id) {
                continue;
            }
            // Match "<id>.<ext>"
            if file_name.len() <= attachment_id.len() + 1
                || !file_name.as_bytes()[attachment_id.len()].eq(&b'.')
            {
                continue;
            }
            let ext = file_name.split('.').next_back().unwrap_or("bin");
            let mime = extension_to_mime(ext).unwrap_or("application/octet-stream");
            let bytes = fs::read(entry.path()).await?;
            return Ok(Some((bytes, mime.to_string())));
        }

        Ok(None)
    }

    pub async fn clear_session(&self, session_id: &str) -> io::Result<bool> {
        let Some(mut session) = self.load_session(session_id).await? else {
            return Ok(false);
        };

        // Keep only the first System message if present; drop all other messages.
        let system_msg = session
            .messages
            .iter()
            .find(|m| matches!(m.role, Role::System))
            .cloned();
        session.messages.clear();
        if let Some(system) = system_msg {
            session.messages.push(system);
        }

        // Clearing history invalidates derived context state.
        session.token_usage = None;
        session.conversation_summary = None;
        session.updated_at = Utc::now();

        // Remove attachments on disk.
        if let Ok(Some(dir)) = self.attachments_dir(session_id).await {
            let _ = fs::remove_dir_all(&dir).await;
            let _ = fs::create_dir_all(&dir).await;
        }

        self.save_session(&session).await?;
        Ok(true)
    }

    pub async fn cleanup(&self, mode: CleanupMode, keep_pinned: bool) -> io::Result<CleanupResult> {
        // All decisions are index-only.
        let entries = {
            self.index
                .read()
                .await
                .sessions
                .values()
                .cloned()
                .collect::<Vec<_>>()
        };

        let pinned_child_roots: HashSet<String> = if keep_pinned {
            entries
                .iter()
                .filter(|e| e.kind == SessionKind::Child && e.pinned)
                .filter_map(|e| e.parent_session_id.clone())
                .collect()
        } else {
            HashSet::new()
        };

        // Helper to decide whether an entry is protected.
        let is_protected = |e: &SessionIndexEntry| -> bool {
            if !keep_pinned {
                return false;
            }
            if e.pinned {
                return true;
            }
            // A root with pinned child cannot be deleted.
            if e.kind == SessionKind::Root && pinned_child_roots.contains(&e.id) {
                return true;
            }
            false
        };

        // Determine deletions as a set of session ids (roots and/or children).
        let mut delete_child_ids = HashSet::<String>::new();
        let mut delete_root_ids = HashSet::<String>::new();

        match mode {
            CleanupMode::Children => {
                for e in entries.iter().filter(|e| e.kind == SessionKind::Child) {
                    if is_protected(e) {
                        continue;
                    }
                    delete_child_ids.insert(e.id.clone());
                }
            }
            CleanupMode::All | CleanupMode::Empty => {
                // First decide which roots can be deleted.
                for root in entries.iter().filter(|e| e.kind == SessionKind::Root) {
                    if is_protected(root) {
                        continue;
                    }
                    if mode == CleanupMode::Empty && root.message_count > 1 {
                        continue;
                    }
                    delete_root_ids.insert(root.id.clone());
                }

                // For roots we keep, we may still delete some children (e.g., unpinned, or empty).
                for child in entries.iter().filter(|e| e.kind == SessionKind::Child) {
                    if delete_root_ids.contains(&child.root_session_id) {
                        continue; // will be deleted with root.
                    }
                    if is_protected(child) {
                        continue;
                    }
                    if mode == CleanupMode::Empty && child.message_count > 1 {
                        continue;
                    }
                    delete_child_ids.insert(child.id.clone());
                }
            }
        }

        // Pre-compute full deleted id set for a truthful response payload.
        let mut deleted_ids = HashSet::<String>::new();
        for root_id in delete_root_ids.iter() {
            for e in entries.iter().filter(|e| e.root_session_id == *root_id) {
                deleted_ids.insert(e.id.clone());
            }
        }
        for child_id in delete_child_ids.iter() {
            deleted_ids.insert(child_id.clone());
        }

        // Apply deletions (roots first; they delete children implicitly).
        for root_id in delete_root_ids.iter() {
            let _ = self.delete_session_recursive(root_id, true).await?;
        }
        for child_id in delete_child_ids.iter() {
            let _ = self.delete_session_recursive(child_id, true).await?;
        }
        let mut deleted_session_ids: Vec<String> = deleted_ids.into_iter().collect();
        deleted_session_ids.sort();
        Ok(CleanupResult {
            deleted_count: deleted_session_ids.len(),
            deleted_session_ids,
        })
    }

    /// Development-only: hard reset all sessions and the index.
    ///
    /// This is the supported "greenfield" mechanism. It deletes:
    /// - `bamboo_home_dir/sessions/`
    /// - `bamboo_home_dir/sessions.json` (rewritten to empty index)
    pub async fn dev_reset(&self) -> io::Result<()> {
        let _guard = self.write_lock.lock().await;

        // Remove the sessions directory entirely.
        let _ = fs::remove_dir_all(&self.sessions_dir).await;
        fs::create_dir_all(&self.sessions_dir).await?;

        // Reset in-memory index and persist.
        {
            let mut index = self.index.write().await;
            *index = SessionsIndex::empty();
            self.persist_index_locked(&index).await?;
        }

        Ok(())
    }

    /// Delete a session. If the session is a root, deletes its entire directory (and all child sessions).
    /// If the session is a child, deletes only that child directory.
    ///
    /// `force=true` ignores pinned protection; callers must enforce confirmations at the API/UI layer.
    pub async fn delete_session_recursive(
        &self,
        session_id: &str,
        force: bool,
    ) -> io::Result<bool> {
        let entry = self.get_index_entry(session_id).await;
        let Some(entry) = entry else {
            return Ok(false);
        };

        if !force && entry.pinned {
            return Err(other_io_error(
                "refusing to delete pinned session without force",
            ));
        }

        match entry.kind {
            SessionKind::Child => {
                let abs_dir = self.abs_path_from_rel(&entry.rel_path);
                let _ = fs::remove_dir_all(&abs_dir).await;
                self.update_index(|index| {
                    index.sessions.remove(session_id);
                    Ok(())
                })
                .await?;
                if let Err(error) = self.search_index.delete_session(session_id).await {
                    tracing::warn!(
                        "failed to delete session search index row for {}: {}",
                        session_id,
                        error
                    );
                }
                Ok(true)
            }
            SessionKind::Root => {
                let root_id = entry.id.clone();
                let abs_dir = self.abs_path_from_rel(&entry.rel_path);
                let _ = fs::remove_dir_all(&abs_dir).await;

                let to_remove_ids = {
                    let index = self.index.read().await;
                    index
                        .sessions
                        .values()
                        .filter(|e| e.root_session_id == root_id)
                        .map(|e| e.id.clone())
                        .collect::<Vec<_>>()
                };

                self.update_index(|index| {
                    for id in &to_remove_ids {
                        index.sessions.remove(id);
                    }
                    Ok(())
                })
                .await?;

                for id in to_remove_ids {
                    if let Err(error) = self.search_index.delete_session(&id).await {
                        tracing::warn!(
                            "failed to delete session search index row for {}: {}",
                            id,
                            error
                        );
                    }
                }
                Ok(true)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupMode {
    All,
    Empty,
    Children,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupResult {
    pub deleted_count: usize,
    pub deleted_session_ids: Vec<String>,
}

/// Atomically write `bytes` to `path`: write a uniquely-named temp file in the
/// same directory, fsync it to durable storage, then atomically rename over the
/// target. A crash (OOM, panic, power loss) mid-write can therefore never leave
/// `path` truncated or half-written — a reader sees either the old content or the
/// complete new content, never a torn write. The temp is cleaned up on a write
/// failure. Shared by the persistence layers (vs. a plain `fs::write` overwrite).
/// #35.
///
/// Residuals (tracked in #166): the rename + parent directory are not fsync'd, so
/// after a power loss the file may revert to the OLD complete content (still never
/// torn); a crash BETWEEN temp-create and rename leaks an orphan `*.tmp.*` (disk
/// litter, not corruption — no sweep yet); and [`atomic_rename`] is
/// remove-then-rename on Windows, where a crash in that window can lose the target.
pub(crate) async fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;

    let tmp = path.with_extension(format!("tmp.{}", Uuid::new_v4()));
    let write_result = async {
        let mut file = fs::File::create(&tmp).await?;
        file.write_all(bytes).await?;
        // fsync so the bytes are durable before the rename publishes them.
        file.sync_all().await
    }
    .await;
    if let Err(e) = write_result {
        let _ = fs::remove_file(&tmp).await;
        return Err(e);
    }
    atomic_rename(&tmp, path).await
}

async fn atomic_rename(from: &Path, to: &Path) -> io::Result<()> {
    // Best-effort atomic on Unix. On Windows, rename cannot overwrite.
    match fs::rename(from, to).await {
        Ok(()) => Ok(()),
        Err(err) => {
            if to.exists() {
                let _ = fs::remove_file(to).await;
            }
            fs::rename(from, to).await.map_err(|e| {
                other_io_error(format!(
                    "failed to rename {:?} -> {:?}: {} (original: {})",
                    from, to, e, err
                ))
            })
        }
    }
}

fn parse_data_url_base64(url: &str) -> Option<(String, String)> {
    // data:<mime>;base64,<data...>
    let trimmed = url.trim();
    if !trimmed.starts_with("data:") {
        return None;
    }
    let trimmed = trimmed.strip_prefix("data:")?;
    let (header, data) = trimmed.split_once(',')?;
    if !header.contains(";base64") {
        return None;
    }
    let mime = header.split(';').next()?.trim().to_string();
    Some((mime, data.trim().to_string()))
}

fn mime_to_extension(mime: &str) -> Option<&'static str> {
    match mime.trim().to_ascii_lowercase().as_str() {
        "image/png" => Some("png"),
        "image/jpeg" => Some("jpg"),
        "image/webp" => Some("webp"),
        "image/gif" => Some("gif"),
        "image/bmp" => Some("bmp"),
        _ => None,
    }
}

fn extension_to_mime(ext: &str) -> Option<&'static str> {
    match ext.trim().to_ascii_lowercase().as_str() {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "webp" => Some("image/webp"),
        "gif" => Some("image/gif"),
        "bmp" => Some("image/bmp"),
        _ => None,
    }
}

#[async_trait::async_trait]
impl Storage for SessionStoreV2 {
    async fn save_session(&self, session: &Session) -> io::Result<()> {
        let rel_path = self.ensure_session_dirs(session).await?;
        let abs_dir = self.abs_path_from_rel(&rel_path);
        let path = abs_dir.join("session.json");

        // Refresh the runtime sidecar BEFORE session.json. If the process
        // crashes between the two writes, the sidecar then carries a
        // control-plane that is at least as fresh as session.json, and the
        // load-time overlay (sidecar wins for non-message fields) stays correct.
        // Writing session.json first could leave a stale sidecar that silently
        // reverts the just-saved control-plane on the next load.
        self.write_runtime_sidecar(&abs_dir, session).await?;

        let tmp = path.with_extension(format!("json.tmp.{}", Uuid::new_v4()));
        let bytes =
            serde_json::to_vec_pretty(session).map_err(|e| other_io_error(e.to_string()))?;
        fs::write(&tmp, bytes).await?;
        atomic_rename(&tmp, &path).await?;

        self.upsert_index_from_session(session, rel_path).await?;
        if let Err(error) = self.search_index.upsert_session(session).await {
            tracing::warn!(
                "failed to update session search index for {}: {}",
                session.id,
                error
            );
        }
        Ok(())
    }

    async fn load_session(&self, session_id: &str) -> io::Result<Option<Session>> {
        validate_session_id(session_id)?;
        let Some(path) = self.session_json_path(session_id).await? else {
            return Ok(None);
        };
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path).await?;
        let session: Session = serde_json::from_str(&raw)
            .map_err(|e| other_io_error(format!("invalid session.json: {e}")))?;
        let sidecar = self.read_runtime_sidecar(session_id).await?;
        let mut session = overlay_runtime_sidecar(session, sidecar);
        // Drop a stale pre-#180 Root token_budget cache so it re-resolves (#230).
        session.clear_stale_root_token_budget();
        Ok(Some(session))
    }

    async fn delete_session(&self, session_id: &str) -> io::Result<bool> {
        // Historical API deletes sessions. In V2, treat this as recursive and forced.
        self.delete_session_recursive(session_id, true).await
    }

    async fn save_runtime_state(&self, session: &Session) -> io::Result<()> {
        // Fast path: write ONLY the small runtime sidecar (no messages), leaving
        // session.json — which carries the full conversation history — untouched.
        // This is O(1) in conversation length, unlike `save_session`.
        let Some(rel) = self.resolve_rel_path(&session.id).await else {
            // Session was never fully persisted yet — fall back to a full save so
            // session.json and the index get created.
            return self.save_session(session).await;
        };
        let abs_dir = self.abs_path_from_rel(&rel);
        self.write_runtime_sidecar(&abs_dir, session).await?;

        // Workspace and Project ownership are part of the list/index API
        // contract. Runtime updates must therefore be reflected without waiting
        // for a later full session save. Avoid rewriting the global index when
        // neither normalized value changed.
        let workspace_path = session
            .workspace_path_meta()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let project_id = normalized_project_id(session);
        let runtime_index_changed = self
            .get_index_entry(&session.id)
            .await
            .is_some_and(|entry| {
                entry.workspace_path != workspace_path || entry.project_id != project_id
            });
        if runtime_index_changed {
            self.update_index(|index| {
                if let Some(entry) = index.sessions.get_mut(&session.id) {
                    entry.workspace_path = workspace_path;
                    entry.project_id = project_id;
                }
                Ok(())
            })
            .await?;
        }
        Ok(())
    }

    async fn load_runtime_control_plane(&self, session_id: &str) -> io::Result<Option<Session>> {
        validate_session_id(session_id)?;
        // Prefer the sidecar (cheap: no messages). Fall back to a full load for
        // sessions that predate the sidecar (not yet migrated).
        if let Some(side) = self.read_runtime_sidecar(session_id).await? {
            return Ok(Some(side));
        }
        self.load_session(session_id).await
    }

    async fn list_child_run_statuses(
        &self,
        parent_session_id: &str,
    ) -> io::Result<Vec<(String, Option<String>)>> {
        let index = self.index.read().await;
        Ok(index
            .sessions
            .values()
            .filter(|entry| {
                entry.kind == SessionKind::Child
                    && entry.parent_session_id.as_deref() == Some(parent_session_id)
            })
            .map(|entry| (entry.id.clone(), entry.last_run_status.clone()))
            .collect())
    }

    async fn list_sessions_by_run_status(
        &self,
        status: &str,
    ) -> io::Result<Vec<(String, Option<String>)>> {
        let index = self.index.read().await;
        Ok(index
            .sessions
            .values()
            .filter(|entry| entry.last_run_status.as_deref() == Some(status))
            .map(|entry| (entry.id.clone(), entry.parent_session_id.clone()))
            .collect())
    }

    async fn append_token_usage_record(&self, session_id: &str, json_line: &str) -> io::Result<()> {
        use tokio::io::AsyncWriteExt;

        validate_session_id(session_id)?;
        // Resolve the session's own directory. If it isn't indexed yet (no
        // initial save has happened), skip silently — this is an analysis
        // sidecar, never authoritative state.
        let Some(rel) = self.resolve_rel_path(session_id).await else {
            return Ok(());
        };
        let path = self.abs_path_from_rel(&rel).join(TOKEN_USAGE_FILE);

        // Exactly one line per record, regardless of how the caller framed it.
        let mut line = json_line.trim_end_matches('\n').to_string();
        line.push('\n');

        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        file.write_all(line.as_bytes()).await?;
        // `flush` is LOAD-BEARING, not cosmetic (issues #378/#486):
        // `tokio::fs::File::write_all` only copies the bytes into the File's
        // internal buffer and schedules the actual OS write on the blocking
        // thread pool — it does NOT wait for it. Dropping the File does not
        // wait either (the write still happens "eventually" on the pool, and
        // any error is silently discarded). So without this flush a caller
        // that appends and then promptly reads the file back — exactly what
        // `append_token_usage_record_writes_jsonl_in_session_dir` does — can
        // observe the file BEFORE a still-in-flight append lands, which on a
        // loaded CI runner (saturated blocking pool) manifested as the
        // one-off "1 line instead of 2 / lost second append" failure.
        // `flush().await` drives the pending background write to completion
        // (and surfaces its error) before we return.
        file.flush().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl AttachmentReader for SessionStoreV2 {
    async fn read_attachment(
        &self,
        session_id: &str,
        attachment_id: &str,
    ) -> io::Result<Option<(Vec<u8>, String)>> {
        SessionStoreV2::read_attachment(self, session_id, attachment_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use tempfile::TempDir;

    async fn create_temp_storage() -> io::Result<(SessionStoreV2, TempDir)> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();
        let storage = SessionStoreV2::new(bamboo_home).await?;
        Ok((storage, temp_dir))
    }

    #[tokio::test]
    async fn test_new_creates_sessions_directory() -> io::Result<()> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();
        let sessions_dir = bamboo_home.join("sessions");

        assert!(!sessions_dir.exists());
        let _storage = SessionStoreV2::new(bamboo_home).await?;
        assert!(sessions_dir.exists());

        Ok(())
    }

    #[tokio::test]
    async fn root_token_budget_cleared_on_load_child_preserved() -> io::Result<()> {
        // #230: a Root persisted with a token_budget (pre-#180 stale cache) loads
        // with token_budget == None so it re-resolves; a Child's assigned
        // sub-budget survives the reload.
        let (storage, _dir) = create_temp_storage().await?;

        let mut root = Session::new("root-1", "m");
        root.token_budget = Some(bamboo_domain::TokenBudget::for_model(1000));
        storage.save_session(&root).await?;
        let loaded = storage.load_session("root-1").await?.expect("root present");
        assert!(
            loaded.token_budget.is_none(),
            "stale Root token_budget must be cleared on load"
        );

        let parent = Session::new("root-1", "m");
        let mut child = Session::new_child_of("child-1", &parent, "m", "c");
        child.token_budget = Some(bamboo_domain::TokenBudget::for_model(500));
        storage.save_session(&child).await?;
        let loaded_child = storage
            .load_session("child-1")
            .await?
            .expect("child present");
        assert!(
            loaded_child.token_budget.is_some(),
            "Child assigned sub-budget must be preserved on load"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_new_creates_index_file() -> io::Result<()> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();
        let index_path = bamboo_home.join("sessions.json");

        assert!(!index_path.exists());
        let _storage = SessionStoreV2::new(bamboo_home).await?;
        assert!(index_path.exists());

        Ok(())
    }

    // ── Runtime sidecar (③) ───────────────────────────────────────────────

    use bamboo_domain::session::types::Message;
    use bamboo_domain::AgentRuntimeState;

    fn session_with_history(id: &str, messages: usize, run_id: &str) -> Session {
        let mut s = Session::new(id.to_string(), "test-model".to_string());
        for i in 0..messages {
            s.add_message(Message::user(format!("msg-{i}")));
        }
        s.agent_runtime_state = Some(AgentRuntimeState::new(run_id));
        s
    }

    async fn read_session_json_raw(storage: &SessionStoreV2, id: &str) -> String {
        let path = storage.session_json_path(id).await.unwrap().unwrap();
        tokio::fs::read_to_string(path).await.unwrap()
    }

    #[tokio::test]
    async fn append_token_usage_record_writes_jsonl_in_session_dir() -> io::Result<()> {
        let (storage, _t) = create_temp_storage().await?;
        let s = session_with_history("tu-1", 1, "run-A");
        storage.save_session(&s).await?;

        storage
            .append_token_usage_record("tu-1", r#"{"round":1,"cache_read_input_tokens":0}"#)
            .await?;
        // A trailing newline in the caller's line must not produce a blank line.
        storage
            .append_token_usage_record("tu-1", "{\"round\":2,\"cache_read_input_tokens\":9000}\n")
            .await?;

        let rel = storage.resolve_rel_path("tu-1").await.unwrap();
        let path = storage.abs_path_from_rel(&rel).join(TOKEN_USAGE_FILE);
        assert!(
            path.exists(),
            "token-usage.jsonl should sit in the session dir"
        );

        let contents = tokio::fs::read_to_string(&path).await?;
        let lines: Vec<&str> = contents.lines().collect();
        // Storage is a unique per-test TempDir and every path resolves off the
        // instance `bamboo_home_dir` (not a process-global), so nothing outside
        // this test can write here — exactly two sequential appends ⇒ two lines.
        // If CI ever trips this again (#378), dump the file so the failure is
        // diagnosable (extra line + its source, or a lost append) instead of an
        // opaque count mismatch. Do NOT relax this to "records present": that
        // would mask a real double-write / lost-write regression.
        assert_eq!(
            lines.len(),
            2,
            "one line per appended record; actual token-usage.jsonl = {contents:?}"
        );
        assert!(lines[0].contains("\"round\":1"));
        assert!(lines[1].contains("\"round\":2"));
        // Each line is valid standalone JSON.
        for line in lines {
            serde_json::from_str::<serde_json::Value>(line).expect("each line is valid JSON");
        }
        Ok(())
    }

    #[tokio::test]
    async fn append_token_usage_record_is_noop_for_unindexed_session() -> io::Result<()> {
        let (storage, _t) = create_temp_storage().await?;
        // No save_session → not indexed yet. Must not error, must not create a file.
        storage
            .append_token_usage_record("never-saved", r#"{"round":1}"#)
            .await?;
        assert!(storage.resolve_rel_path("never-saved").await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn save_session_writes_runtime_sidecar() -> io::Result<()> {
        let (storage, _t) = create_temp_storage().await?;
        let s = session_with_history("sc-1", 2, "run-A");
        storage.save_session(&s).await?;

        let sidecar_path = storage.runtime_json_path("sc-1").await?.unwrap();
        assert!(
            sidecar_path.exists(),
            "save_session must write runtime.json"
        );

        // Sidecar must NOT carry the message history.
        let side = storage.read_runtime_sidecar("sc-1").await?.unwrap();
        assert!(side.messages.is_empty(), "sidecar messages must be cleared");
        assert_eq!(side.agent_runtime_state.as_ref().unwrap().run_id, "run-A");
        Ok(())
    }

    #[tokio::test]
    async fn save_runtime_state_does_not_rewrite_session_json_messages() -> io::Result<()> {
        let (storage, _t) = create_temp_storage().await?;

        // Full save: 3 messages + run-A.
        let s = session_with_history("sc-2", 3, "run-A");
        storage.save_session(&s).await?;
        let raw_before = read_session_json_raw(&storage, "sc-2").await;
        assert!(raw_before.contains("msg-2"));

        // Runtime-only save: bump control-plane to run-B AND (deviously) add a
        // 4th in-memory message. The sidecar must persist run-B but IGNORE the
        // message, and session.json must be left byte-identical.
        let mut s2 = s.clone();
        s2.agent_runtime_state = Some(AgentRuntimeState::new("run-B"));
        s2.add_message(Message::user("msg-3-should-not-persist"));
        storage.save_runtime_state(&s2).await?;

        let raw_after = read_session_json_raw(&storage, "sc-2").await;
        assert_eq!(
            raw_before, raw_after,
            "save_runtime_state must not touch session.json"
        );

        // Load overlays the sidecar: run-B control-plane + original 3 messages.
        let loaded = storage.load_session("sc-2").await?.unwrap();
        assert_eq!(loaded.agent_runtime_state.as_ref().unwrap().run_id, "run-B");
        assert_eq!(
            loaded.messages.len(),
            3,
            "runtime-only save must not add a message"
        );
        Ok(())
    }

    #[tokio::test]
    async fn save_runtime_state_falls_back_to_full_save_when_unpersisted() -> io::Result<()> {
        let (storage, _t) = create_temp_storage().await?;
        // Session was never saved: no index entry, no dir. save_runtime_state
        // must fall back to a full save so session.json + index get created.
        let s = session_with_history("sc-3", 1, "run-A");
        storage.save_runtime_state(&s).await?;

        let loaded = storage.load_session("sc-3").await?;
        assert!(
            loaded.is_some(),
            "fallback full save must create the session"
        );
        assert_eq!(loaded.unwrap().messages.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_sidecar_is_ignored_and_session_still_loads() -> io::Result<()> {
        let (storage, _t) = create_temp_storage().await?;
        let s = session_with_history("sc-4", 2, "run-A");
        storage.save_session(&s).await?;

        // Corrupt the sidecar.
        let sidecar_path = storage.runtime_json_path("sc-4").await?.unwrap();
        tokio::fs::write(&sidecar_path, b"{ not valid json").await?;

        // Session still loads from session.json; corrupt sidecar is ignored.
        let loaded = storage.load_session("sc-4").await?.unwrap();
        assert_eq!(loaded.messages.len(), 2);
        assert_eq!(loaded.agent_runtime_state.as_ref().unwrap().run_id, "run-A");
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_index_is_backed_up_and_rebuilt_from_disk() -> io::Result<()> {
        // #342: a corrupt sessions.json must NOT be boot-fatal. On construction
        // the store backs it up to sessions.json.bak, rebuilds the index by
        // scanning the on-disk session tree, and every intact session.json (root
        // AND child) becomes reachable again.
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();

        // Persist a root and a child under it, then drop the store.
        {
            let storage = SessionStoreV2::new(bamboo_home.clone()).await?;
            let root = Session::new("root-1", "m");
            storage.save_session(&root).await?;
            let child = Session::new_child_of("child-1", &root, "m", "c");
            storage.save_session(&child).await?;
        }

        // Corrupt the global index (truncated / invalid JSON).
        let index_path = bamboo_home.join("sessions.json");
        tokio::fs::write(&index_path, b"{ not valid json").await?;

        // (a) Re-opening on the same dir must SUCCEED, not hard-error.
        let recovered = SessionStoreV2::new(bamboo_home.clone()).await?;

        // (b) Both sessions are indexed again, with the correct rel_paths, so
        // they actually resolve + load from disk.
        assert_eq!(
            recovered.resolve_rel_path("root-1").await.as_deref(),
            Some("sessions/root-1"),
            "root must be recovered with its on-disk rel_path"
        );
        assert_eq!(
            recovered.resolve_rel_path("child-1").await.as_deref(),
            Some("sessions/root-1/children/child-1"),
            "child must be recovered with its on-disk rel_path"
        );
        assert!(
            recovered.get_index_entry("root-1").await.is_some(),
            "root index entry must exist after rebuild"
        );
        assert!(
            recovered.load_session("root-1").await?.is_some(),
            "recovered root must load from disk"
        );
        let loaded_child = recovered
            .load_session("child-1")
            .await?
            .expect("recovered child must load from disk");
        assert_eq!(loaded_child.parent_session_id.as_deref(), Some("root-1"));
        assert_eq!(loaded_child.root_session_id, "root-1");

        // (c) The corrupt index was preserved as sessions.json.bak, and a fresh
        // valid sessions.json was re-materialized.
        assert!(
            bamboo_home.join("sessions.json.bak").exists(),
            "corrupt sessions.json must be backed up to sessions.json.bak"
        );
        assert!(
            index_path.exists(),
            "a fresh sessions.json must be written after rebuild"
        );

        Ok(())
    }

    #[tokio::test]
    async fn v2_index_migrates_workspace_paths_from_root_and_child_sessions() -> io::Result<()> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();

        {
            let storage = SessionStoreV2::new(bamboo_home.clone()).await?;
            let mut root = Session::new("workspace-root", "m");
            root.set_workspace_path_meta("  /workspaces/root  ");
            storage.save_session(&root).await?;

            let mut child = Session::new_child_of("workspace-child", &root, "m", "child");
            child.set_workspace_path_meta("/workspaces/child");
            storage.save_session(&child).await?;

            let legacy_without_workspace = Session::new("workspace-missing", "m");
            storage.save_session(&legacy_without_workspace).await?;

            // A malformed sibling must not prevent recovery of intact sessions.
            let broken_dir = bamboo_home.join("sessions/broken");
            tokio::fs::create_dir_all(&broken_dir).await?;
            tokio::fs::write(broken_dir.join("session.json"), b"{ invalid json").await?;
        }

        let index_path = bamboo_home.join("sessions.json");
        let mut legacy: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&index_path).await?)
                .map_err(|error| other_io_error(error.to_string()))?;
        legacy["version"] = serde_json::json!(2);
        for entry in legacy["sessions"]
            .as_object_mut()
            .expect("sessions object")
            .values_mut()
        {
            entry
                .as_object_mut()
                .expect("entry")
                .remove("workspace_path");
        }
        tokio::fs::write(
            &index_path,
            serde_json::to_vec_pretty(&legacy)
                .map_err(|error| other_io_error(error.to_string()))?,
        )
        .await?;

        let migrated = SessionStoreV2::new(bamboo_home.clone()).await?;
        assert_eq!(
            migrated
                .get_index_entry("workspace-root")
                .await
                .and_then(|entry| entry.workspace_path),
            Some("/workspaces/root".to_string())
        );
        assert_eq!(
            migrated
                .get_index_entry("workspace-child")
                .await
                .and_then(|entry| entry.workspace_path),
            Some("/workspaces/child".to_string())
        );
        assert!(migrated.get_index_entry("broken").await.is_none());
        assert_eq!(
            migrated
                .get_index_entry("workspace-missing")
                .await
                .and_then(|entry| entry.workspace_path),
            None
        );

        let persisted: SessionsIndex = serde_json::from_slice(&tokio::fs::read(index_path).await?)
            .map_err(|error| other_io_error(error.to_string()))?;
        assert_eq!(persisted.version, SESSIONS_INDEX_VERSION);
        Ok(())
    }

    #[tokio::test]
    async fn v3_index_migrates_project_ids_from_root_and_child_sessions() -> io::Result<()> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();

        {
            let storage = SessionStoreV2::new(bamboo_home.clone()).await?;
            let mut root = Session::new("project-root", "m");
            root.set_project_id_meta("01JROOTPROJECT00000000000000");
            storage.save_session(&root).await?;

            let mut child = Session::new_child_of("project-child", &root, "m", "child");
            child.metadata.insert(
                "project_id".to_string(),
                "01JCHILDPROJECT000000000000".to_string(),
            );
            storage.save_session(&child).await?;

            let unassigned = Session::new("project-unassigned", "m");
            storage.save_session(&unassigned).await?;
        }

        let index_path = bamboo_home.join("sessions.json");
        let mut legacy: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&index_path).await?)
                .map_err(|error| other_io_error(error.to_string()))?;
        legacy["version"] = serde_json::json!(3);
        for entry in legacy["sessions"]
            .as_object_mut()
            .expect("sessions object")
            .values_mut()
        {
            entry.as_object_mut().expect("entry").remove("project_id");
        }
        tokio::fs::write(
            &index_path,
            serde_json::to_vec_pretty(&legacy)
                .map_err(|error| other_io_error(error.to_string()))?,
        )
        .await?;

        let migrated = SessionStoreV2::new(bamboo_home.clone()).await?;
        assert_eq!(
            migrated
                .get_index_entry("project-root")
                .await
                .and_then(|entry| entry.project_id),
            Some("01JROOTPROJECT00000000000000".to_string())
        );
        assert_eq!(
            migrated
                .get_index_entry("project-child")
                .await
                .and_then(|entry| entry.project_id),
            Some("01JCHILDPROJECT000000000000".to_string())
        );
        assert_eq!(
            migrated
                .get_index_entry("project-unassigned")
                .await
                .and_then(|entry| entry.project_id),
            None
        );

        let persisted: SessionsIndex = serde_json::from_slice(&tokio::fs::read(index_path).await?)
            .map_err(|error| other_io_error(error.to_string()))?;
        assert_eq!(persisted.version, SESSIONS_INDEX_VERSION);
        Ok(())
    }

    #[tokio::test]
    async fn workspace_path_updates_index_on_full_and_runtime_only_saves() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let mut session = Session::new("workspace-update", "m");
        session.set_workspace_path_meta("  /workspaces/first  ");
        storage.save_session(&session).await?;
        assert_eq!(
            storage
                .get_index_entry(&session.id)
                .await
                .and_then(|entry| entry.workspace_path),
            Some("/workspaces/first".to_string())
        );

        session.set_workspace_path_meta(" /workspaces/latest ");
        storage.save_runtime_state(&session).await?;
        assert_eq!(
            storage
                .get_index_entry(&session.id)
                .await
                .and_then(|entry| entry.workspace_path),
            Some("/workspaces/latest".to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn project_id_updates_index_on_full_and_runtime_only_saves() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let mut session = Session::new("project-update", "m");
        session.set_project_id_meta(" 01JPROJECTFIRST000000000000 ");
        storage.save_session(&session).await?;
        assert_eq!(
            storage
                .get_index_entry(&session.id)
                .await
                .and_then(|entry| entry.project_id),
            Some("01JPROJECTFIRST000000000000".to_string())
        );

        session.set_project_id_meta(" 01JPROJECTLATEST00000000000 ");
        storage.save_runtime_state(&session).await?;
        assert_eq!(
            storage
                .get_index_entry(&session.id)
                .await
                .and_then(|entry| entry.project_id),
            Some("01JPROJECTLATEST00000000000".to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn malformed_legacy_project_id_isolated_from_session_index() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let mut malformed = Session::new("project-malformed", "m");
        malformed
            .metadata
            .insert("project_id".to_string(), "../unsafe".to_string());
        storage.save_session(&malformed).await?;

        let healthy = Session::new("project-healthy", "m");
        storage.save_session(&healthy).await?;

        assert_eq!(
            storage
                .get_index_entry(&malformed.id)
                .await
                .and_then(|entry| entry.project_id),
            None
        );
        assert!(storage.get_index_entry(&healthy.id).await.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_overlays_runtime_sidecar_control_plane() -> io::Result<()> {
        // #342 review: rebuild must overlay runtime.json (the freshest
        // control-plane) on top of session.json, exactly like load_session.
        // A runtime-only save updates ONLY the sidecar, so a session that
        // completed that way must be recovered as "completed", not the stale
        // "running" still baked into session.json.
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();

        {
            let storage = SessionStoreV2::new(bamboo_home.clone()).await?;

            // Full save: session.json + sidecar both carry "running".
            let mut root = Session::new("rb-overlay", "m");
            root.metadata
                .insert("last_run_status".into(), "running".into());
            storage.save_session(&root).await?;

            // Runtime-only save: bump ONLY the sidecar to "completed".
            // session.json is left byte-identical (still "running").
            let mut updated = root.clone();
            updated
                .metadata
                .insert("last_run_status".into(), "completed".into());
            storage.save_runtime_state(&updated).await?;

            // Sanity: session.json on disk still carries the stale status.
            let raw = read_session_json_raw(&storage, "rb-overlay").await;
            assert!(
                raw.contains("running"),
                "session.json must still carry the pre-sidecar status"
            );
        }

        // Corrupt the index, then reopen → triggers rebuild-from-disk.
        tokio::fs::write(bamboo_home.join("sessions.json"), b"{ not valid json").await?;
        let recovered = SessionStoreV2::new(bamboo_home.clone()).await?;

        // The rebuilt index entry must reflect the SIDECAR's fresh "completed",
        // NOT session.json's stale "running". Without the overlay fix the rebuild
        // reads session.json only and this is "running", so the test fails.
        let entry = recovered
            .get_index_entry("rb-overlay")
            .await
            .expect("root recovered into rebuilt index");
        assert_eq!(
            entry.last_run_status.as_deref(),
            Some("completed"),
            "rebuild must overlay runtime.json control-plane, not the stale session.json"
        );
        Ok(())
    }

    #[tokio::test]
    async fn missing_index_starts_empty_and_does_not_back_up() -> io::Result<()> {
        // A *missing* sessions.json keeps the fresh-empty-index behavior: no
        // rebuild is triggered and no sessions.json.bak is produced.
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();

        let storage = SessionStoreV2::new(bamboo_home.clone()).await?;
        assert!(storage.list_index_entries().await.is_empty());
        assert!(
            !bamboo_home.join("sessions.json.bak").exists(),
            "a missing index must not produce a .bak backup"
        );
        Ok(())
    }

    // ── ⑤ Runtime sidecar migration ──────────────────────────────────────

    #[tokio::test]
    async fn migration_backfills_sidecars_for_legacy_sessions() -> io::Result<()> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();
        let storage = SessionStoreV2::new(bamboo_home.clone()).await?;

        // Persist two sessions, then delete their sidecars to simulate the
        // legacy on-disk layout (session.json only).
        let a = session_with_history("mig-a", 3, "run-A");
        let b = session_with_history("mig-b", 1, "run-B");
        storage.save_session(&a).await?;
        storage.save_session(&b).await?;
        for id in ["mig-a", "mig-b"] {
            let sidecar = storage.runtime_json_path(id).await?.unwrap();
            tokio::fs::remove_file(&sidecar).await?;
            assert!(!sidecar.exists());
        }

        let migrated = storage.migrate_runtime_sidecars().await?;
        assert_eq!(migrated, 2, "both legacy sessions get a sidecar");

        // Sidecars now exist and carry the control-plane (no messages).
        for (id, run) in [("mig-a", "run-A"), ("mig-b", "run-B")] {
            let side = storage.read_runtime_sidecar(id).await?.unwrap();
            assert!(side.messages.is_empty());
            assert_eq!(side.agent_runtime_state.as_ref().unwrap().run_id, run);
        }
        // Full load still returns the messages from session.json.
        assert_eq!(
            storage.load_session("mig-a").await?.unwrap().messages.len(),
            3
        );

        // Marker written; a second run is a no-op.
        let marker = bamboo_home.join(RUNTIME_SIDECAR_MIGRATION_MARKER);
        assert!(marker.exists());
        assert_eq!(storage.migrate_runtime_sidecars().await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn migration_is_idempotent_and_skips_existing_sidecars() -> io::Result<()> {
        let (storage, _t) = create_temp_storage().await?;
        // Fresh save already writes a sidecar — migration must not double-count it.
        storage
            .save_session(&session_with_history("mig-c", 2, "run-C"))
            .await?;
        let first = storage.migrate_runtime_sidecars().await?;
        assert_eq!(first, 0, "session saved in new format needs no migration");
        // And a re-run remains a no-op.
        assert_eq!(storage.migrate_runtime_sidecars().await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn migration_drops_legacy_denormalized_children_from_sidecar() -> io::Result<()> {
        // A legacy session.json whose embedded runtime state still carries the
        // old denormalized children id vectors. After migration the sidecar must
        // not contain them (they are now derived from the index).
        let (storage, _t) = create_temp_storage().await?;
        let mut s = session_with_history("mig-legacy", 1, "run-L");
        storage.save_session(&s).await?;

        // Hand-write a legacy session.json containing children.active_ids and
        // remove the sidecar, simulating pre-split on-disk data.
        let dir = storage.abs_path_from_rel(&storage.resolve_rel_path("mig-legacy").await.unwrap());
        s.agent_runtime_state = Some(AgentRuntimeState::new("run-L"));
        let mut value = serde_json::to_value(&s).unwrap();
        value["agent_runtime_state"]["children"]["active_ids"] = serde_json::json!(["ghost-child"]);
        tokio::fs::write(
            dir.join("session.json"),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .await?;
        tokio::fs::remove_file(storage.runtime_json_path("mig-legacy").await?.unwrap()).await?;

        assert_eq!(storage.migrate_runtime_sidecars().await?, 1);

        let raw_sidecar =
            tokio::fs::read_to_string(storage.runtime_json_path("mig-legacy").await?.unwrap())
                .await?;
        assert!(
            !raw_sidecar.contains("ghost-child") && !raw_sidecar.contains("active_ids"),
            "legacy denormalized children must not survive migration: {raw_sidecar}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_child_run_statuses_filters_by_parent_and_reports_status() -> io::Result<()> {
        let (storage, _t) = create_temp_storage().await?;

        // Parent root + two children with distinct statuses, plus an unrelated
        // child under a different parent that must NOT appear.
        let parent = Session::new("p-root".to_string(), "m".to_string());
        storage.save_session(&parent).await?;
        let other = Session::new("p-other".to_string(), "m".to_string());
        storage.save_session(&other).await?;

        let mut c1 = Session::new_child("ch-done", "p-root", "m", "c1");
        c1.metadata
            .insert("last_run_status".to_string(), "completed".to_string());
        storage.save_session(&c1).await?;

        let c2 = Session::new_child("ch-pending", "p-root", "m", "c2");
        storage.save_session(&c2).await?;

        let foreign = Session::new_child("ch-foreign", "p-other", "m", "x");
        storage.save_session(&foreign).await?;

        let mut got = storage.list_child_run_statuses("p-root").await?;
        got.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(got.len(), 2, "only p-root's children: {got:?}");
        assert_eq!(got[0].0, "ch-done");
        assert_eq!(got[0].1.as_deref(), Some("completed"));
        assert_eq!(got[1].0, "ch-pending");
        // pending child has no terminal status mirrored yet.
        assert!(got[1].1.as_deref() != Some("completed"));
        Ok(())
    }

    #[tokio::test]
    async fn list_sessions_by_run_status_matches_index_and_reports_parent() -> io::Result<()> {
        let (storage, _t) = create_temp_storage().await?;

        let mut root = Session::new("r-susp".to_string(), "m".to_string());
        root.metadata
            .insert("last_run_status".to_string(), "suspended".to_string());
        storage.save_session(&root).await?;

        let mut child = Session::new_child("ch-run", "r-susp", "m", "c");
        child
            .metadata
            .insert("last_run_status".to_string(), "running".to_string());
        storage.save_session(&child).await?;

        let mut done = Session::new("r-done".to_string(), "m".to_string());
        done.metadata
            .insert("last_run_status".to_string(), "completed".to_string());
        storage.save_session(&done).await?;

        let suspended = storage.list_sessions_by_run_status("suspended").await?;
        assert_eq!(suspended, vec![("r-susp".to_string(), None)]);

        let running = storage.list_sessions_by_run_status("running").await?;
        assert_eq!(
            running,
            vec![("ch-run".to_string(), Some("r-susp".to_string()))]
        );

        assert!(storage
            .list_sessions_by_run_status("timeout")
            .await?
            .is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn load_runtime_control_plane_reads_sidecar_without_messages() -> io::Result<()> {
        let (storage, _t) = create_temp_storage().await?;
        let s = session_with_history("sc-5", 5, "run-A");
        storage.save_session(&s).await?;

        let cp = storage.load_runtime_control_plane("sc-5").await?.unwrap();
        assert!(
            cp.messages.is_empty(),
            "control-plane load must skip the message history"
        );
        assert_eq!(cp.agent_runtime_state.as_ref().unwrap().run_id, "run-A");
        Ok(())
    }

    #[tokio::test]
    async fn test_save_and_load_session() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let session = Session::new("session-1", "test-model");

        storage.save_session(&session).await?;
        let loaded = storage.load_session(&session.id).await?;

        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.id, session.id);
        assert_eq!(loaded.model, session.model);

        Ok(())
    }

    #[tokio::test]
    async fn test_load_session_returns_none_when_not_found() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let loaded = storage.load_session("nonexistent").await?;
        assert!(loaded.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn nested_grandchild_persists_under_root() -> io::Result<()> {
        // Nesting: a grandchild whose parent is itself a child (parent != root)
        // must persist (previously rejected with "no nesting") and load back
        // with its real parent lineage. All descendants live flat under the
        // tree root's directory.
        let (storage, _t) = create_temp_storage().await?;
        let root = Session::new("root-1", "m");
        storage.save_session(&root).await?;
        let child = Session::new_child_of("child-1", &root, "m", "c");
        storage.save_session(&child).await?;
        let grandchild = Session::new_child_of("gc-1", &child, "m", "g");
        storage.save_session(&grandchild).await?;

        let loaded = storage.load_session("gc-1").await?.expect("grandchild");
        assert_eq!(loaded.parent_session_id.as_deref(), Some("child-1"));
        assert_eq!(loaded.root_session_id, "root-1");
        assert_eq!(loaded.spawn_depth, 2);

        // The grandchild is indexed under the tree root, keyed by its real parent.
        let entry = storage.get_index_entry("gc-1").await.expect("indexed");
        assert_eq!(entry.parent_session_id.as_deref(), Some("child-1"));
        assert_eq!(entry.root_session_id, "root-1");
        Ok(())
    }

    #[tokio::test]
    async fn test_list_index_entries_empty() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let entries = storage.list_index_entries().await;
        assert!(entries.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_list_index_entries_with_sessions() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;

        let session1 = Session::new("session-1", "model-1");
        let session2 = Session::new("session-2", "model-2");

        storage.save_session(&session1).await?;
        storage.save_session(&session2).await?;

        let entries = storage.list_index_entries().await;
        assert_eq!(entries.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_index_entry() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let session = Session::new("session-1", "test-model");

        storage.save_session(&session).await?;

        let entry = storage.get_index_entry(&session.id).await;
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.id, session.id);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_index_entry_returns_none_when_not_found() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let entry = storage.get_index_entry("nonexistent").await;
        assert!(entry.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_session() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let session = Session::new("session-1", "test-model");

        storage.save_session(&session).await?;
        assert!(storage.load_session(&session.id).await?.is_some());

        let deleted = storage.delete_session(&session.id).await?;
        assert!(deleted);
        assert!(storage.load_session(&session.id).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_session_returns_false_when_not_found() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let deleted = storage.delete_session("nonexistent").await?;
        assert!(!deleted);
        Ok(())
    }

    #[test]
    fn test_validate_session_id_empty() {
        assert!(validate_session_id("").is_err());
    }

    #[test]
    fn test_validate_session_id_with_slash() {
        assert!(validate_session_id("session/1").is_err());
    }

    #[test]
    fn test_validate_session_id_with_backslash() {
        assert!(validate_session_id("session\\1").is_err());
    }

    #[test]
    fn test_validate_session_id_with_double_dot() {
        assert!(validate_session_id("session..1").is_err());
    }

    #[test]
    fn test_validate_session_id_valid() {
        assert!(validate_session_id("session-123").is_ok());
    }

    #[test]
    fn test_root_rel_path() {
        let path = SessionStoreV2::root_rel_path("session-123");
        assert_eq!(path, "sessions/session-123");
    }

    #[test]
    fn test_child_rel_path() {
        let path = SessionStoreV2::child_rel_path("root-1", "child-2");
        assert_eq!(path, "sessions/root-1/children/child-2");
    }

    #[test]
    fn test_mime_to_extension() {
        assert_eq!(mime_to_extension("image/png"), Some("png"));
        assert_eq!(mime_to_extension("image/jpeg"), Some("jpg"));
        assert_eq!(mime_to_extension("image/webp"), Some("webp"));
        assert_eq!(mime_to_extension("image/gif"), Some("gif"));
        assert_eq!(mime_to_extension("image/bmp"), Some("bmp"));
        assert_eq!(mime_to_extension("unknown/type"), None);
    }

    #[test]
    fn test_extension_to_mime() {
        assert_eq!(extension_to_mime("png"), Some("image/png"));
        assert_eq!(extension_to_mime("jpg"), Some("image/jpeg"));
        assert_eq!(extension_to_mime("jpeg"), Some("image/jpeg"));
        assert_eq!(extension_to_mime("webp"), Some("image/webp"));
        assert_eq!(extension_to_mime("gif"), Some("image/gif"));
        assert_eq!(extension_to_mime("bmp"), Some("image/bmp"));
        assert_eq!(extension_to_mime("unknown"), None);
    }

    #[test]
    fn test_extension_to_mime_case_insensitive() {
        assert_eq!(extension_to_mime("PNG"), Some("image/png"));
        assert_eq!(extension_to_mime("JPG"), Some("image/jpeg"));
        assert_eq!(extension_to_mime("JPEG"), Some("image/jpeg"));
    }

    #[test]
    fn test_extension_to_mime_with_whitespace() {
        assert_eq!(extension_to_mime("  png  "), Some("image/png"));
        assert_eq!(extension_to_mime("\tjpg\t"), Some("image/jpeg"));
    }
}
