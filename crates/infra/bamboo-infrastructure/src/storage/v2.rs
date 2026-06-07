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
//! - Directory scanning is only used for dev-only index rebuild/recovery (not in hot paths).

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
use bamboo_domain::{Role, Session, SessionKind, TokenBudgetUsage};

use crate::storage::search_index::{should_index_session, SessionSearchIndex};
use bamboo_domain::AttachmentReader;
use bamboo_domain::Storage;

fn other_io_error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

/// Filename of the runtime control-plane sidecar, stored alongside
/// `session.json` in each session directory.
const RUNTIME_SIDECAR_FILE: &str = "runtime.json";

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

fn validate_session_id(session_id: &str) -> io::Result<()> {
    if session_id.is_empty()
        || session_id.contains('/')
        || session_id.contains('\\')
        || session_id.contains("..")
    {
        return Err(other_io_error(format!("invalid session id: {session_id}")));
    }
    Ok(())
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
            version: 2,
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
    pub async fn new(bamboo_home_dir: PathBuf) -> io::Result<Self> {
        let sessions_dir = bamboo_home_dir.join("sessions");
        let index_path = bamboo_home_dir.join("sessions.json");
        let search_index = SessionSearchIndex::new(bamboo_home_dir.join("session_search.db"));

        fs::create_dir_all(&sessions_dir).await?;
        search_index.init().await?;

        let index = if index_path.exists() {
            let raw = fs::read_to_string(&index_path).await?;
            serde_json::from_str(&raw)
                .map_err(|e| other_io_error(format!("invalid sessions.json: {e}")))?
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

        Ok(storage)
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
                if parent_id != root_id {
                    return Err(other_io_error(
                        "child session parent_session_id must equal root_session_id (no nesting)",
                    ));
                }
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
            Ok(Some(self.abs_path_from_rel(&rel).join(RUNTIME_SIDECAR_FILE)))
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
    /// sessions not yet migrated).
    async fn read_runtime_sidecar(&self, session_id: &str) -> io::Result<Option<Session>> {
        let Some(path) = self.runtime_json_path(session_id).await? else {
            return Ok(None);
        };
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(&path).await?;
        match serde_json::from_str::<Session>(&raw) {
            Ok(side) => Ok(Some(side)),
            Err(error) => {
                // A corrupt sidecar must never make a session unloadable — the
                // authoritative copy still lives in session.json. Warn and ignore.
                tracing::warn!(
                    "ignoring corrupt runtime sidecar for {}: {}",
                    session_id,
                    error
                );
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
        let gold_config_json = session
            .metadata
            .get("gold_config")
            .cloned()
            .filter(|v| !v.trim().is_empty());
        let plan_mode = session
            .agent_runtime_state
            .as_ref()
            .and_then(|state| state.plan_mode.clone());
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
                    last_run_status,
                    last_run_error,
                    token_usage: session.token_usage.clone(),
                    subagent_type,
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
        Ok(Some(overlay_runtime_sidecar(session, sidecar)))
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
        self.write_runtime_sidecar(&abs_dir, session).await
    }

    async fn load_runtime_control_plane(
        &self,
        session_id: &str,
    ) -> io::Result<Option<Session>> {
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
    async fn save_session_writes_runtime_sidecar() -> io::Result<()> {
        let (storage, _t) = create_temp_storage().await?;
        let s = session_with_history("sc-1", 2, "run-A");
        storage.save_session(&s).await?;

        let sidecar_path = storage.runtime_json_path("sc-1").await?.unwrap();
        assert!(sidecar_path.exists(), "save_session must write runtime.json");

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
        assert!(loaded.is_some(), "fallback full save must create the session");
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
        assert_eq!(storage.load_session("mig-a").await?.unwrap().messages.len(), 3);

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
        storage.save_session(&session_with_history("mig-c", 2, "run-C")).await?;
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
        let dir = storage.abs_path_from_rel(
            &storage.resolve_rel_path("mig-legacy").await.unwrap(),
        );
        s.agent_runtime_state = Some(AgentRuntimeState::new("run-L"));
        let mut value = serde_json::to_value(&s).unwrap();
        value["agent_runtime_state"]["children"]["active_ids"] =
            serde_json::json!(["ghost-child"]);
        tokio::fs::write(
            dir.join("session.json"),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .await?;
        tokio::fs::remove_file(storage.runtime_json_path("mig-legacy").await?.unwrap()).await?;

        assert_eq!(storage.migrate_runtime_sidecars().await?, 1);

        let raw_sidecar = tokio::fs::read_to_string(
            storage.runtime_json_path("mig-legacy").await?.unwrap(),
        )
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
