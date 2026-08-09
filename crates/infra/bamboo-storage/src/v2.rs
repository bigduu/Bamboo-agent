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
use std::sync::atomic::{AtomicBool, Ordering};

use base64::Engine;
use chrono::{DateTime, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};
use uuid::Uuid;

use bamboo_domain::ProviderModelRef;
use bamboo_domain::ReasoningEffort;
use bamboo_domain::{ProjectId, Role, Session, SessionKind, TaskList, TokenBudgetUsage};

use crate::search_index::{should_index_session, SessionSearchIndex};
use bamboo_domain::AttachmentReader;
use bamboo_domain::Storage;

pub(crate) fn other_io_error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

/// Filename of the runtime control-plane sidecar, stored alongside
/// `session.json` in each session directory.
const RUNTIME_SIDECAR_FILE: &str = "runtime.json";
/// Private undo journals for recoverable two-session Task sidecar commits.
/// Filenames are random UUIDs; untrusted session ids are never used as paths.
const RUNTIME_TASK_TRANSACTION_DIR: &str = ".runtime-task-transactions";
const RUNTIME_TASK_TRANSACTION_LOCK_FILE: &str = ".runtime-task-transactions.lock";
const RUNTIME_TASK_TRANSACTION_VERSION: u32 = 1;
const SESSIONS_INDEX_VERSION: u32 = 4;

/// Filename of the append-only per-LLM-call token-usage log, stored alongside
/// `session.json` in each session directory. One JSON line per call.
const TOKEN_USAGE_FILE: &str = "token-usage.jsonl";

/// Marker (under `bamboo_home_dir`) recording that the one-shot runtime sidecar
/// migration has completed, so it is skipped on subsequent boots.
const RUNTIME_SIDECAR_MIGRATION_MARKER: &str = ".runtime_sidecar_migrated";
const SESSION_LIFECYCLE_LOCK_FILE: &str = ".session-lifecycle.lock";
const SESSION_INDEX_LOCK_FILE: &str = ".sessions-index.lock";

struct SessionIndexFileGuard {
    file: std::fs::File,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskControlPlaneUndo {
    session_id: String,
    task_list: Option<TaskList>,
    task_list_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RuntimeTaskTransactionJournal {
    version: u32,
    transaction_id: String,
    first: TaskControlPlaneUndo,
    second: TaskControlPlaneUndo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeTaskJournalMarkerState {
    Prepared,
    Committing,
    Committed,
}

impl RuntimeTaskJournalMarkerState {
    fn from_path(path: &Path) -> Option<Self> {
        match path.extension().and_then(|value| value.to_str()) {
            Some("json") => Some(Self::Prepared),
            Some("committing") => Some(Self::Committing),
            Some("committed") => Some(Self::Committed),
            _ => None,
        }
    }
}

enum RuntimeTaskJournalFinalizeError {
    Rollback(io::Error),
    RecoveryRequired(io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeTaskTransactionFault {
    FirstUpdatedWrite,
    SecondUpdatedWrite,
    JournalRemove,
    FirstRollbackWrite,
    SecondRollbackWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeTaskDurabilityEvent {
    JournalPublished,
    SingleUpdatedSidecarPublished,
    FirstUpdatedSidecarPublished,
    SecondUpdatedSidecarPublished,
    FirstRollbackSidecarPublished,
    SecondRollbackSidecarPublished,
    JournalDeactivated,
}

struct RuntimeTaskTransactionReadGuard {
    _process: OwnedRwLockReadGuard<()>,
    file: std::fs::File,
}

impl Drop for RuntimeTaskTransactionReadGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

struct RuntimeTaskTransactionWriteGuard {
    _process: OwnedRwLockWriteGuard<()>,
    file: std::fs::File,
}

impl Drop for RuntimeTaskTransactionWriteGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct RuntimeTaskFirstWritePause {
    reached: std::sync::Arc<tokio::sync::Barrier>,
    release: std::sync::Arc<tokio::sync::Barrier>,
}

impl Drop for SessionIndexFileGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

async fn lock_index_file_exclusive_at(bamboo_home_dir: &Path) -> io::Result<SessionIndexFileGuard> {
    let path = bamboo_home_dir.join(SESSION_INDEX_LOCK_FILE);
    let file = tokio::task::spawn_blocking(move || {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        FileExt::lock_exclusive(&file)?;
        Ok::<_, io::Error>(file)
    })
    .await
    .map_err(|error| other_io_error(format!("join session index lock task: {error}")))??;
    Ok(SessionIndexFileGuard { file })
}

async fn persist_index_path_locked(index_path: &Path, index: &SessionsIndex) -> io::Result<()> {
    let tmp = index_path.with_extension(format!("json.tmp.{}", Uuid::new_v4()));
    let bytes = serde_json::to_vec_pretty(index).map_err(|e| other_io_error(e.to_string()))?;
    fs::write(&tmp, bytes).await?;
    atomic_rename(&tmp, index_path).await
}

pub(crate) struct SessionLifecycleReadGuard {
    _process: OwnedRwLockReadGuard<()>,
    file: std::fs::File,
}

impl Drop for SessionLifecycleReadGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

pub(crate) struct SessionLifecycleWriteGuard {
    _process: OwnedRwLockWriteGuard<()>,
    file: std::fs::File,
}

impl Drop for SessionLifecycleWriteGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

/// Build the sidecar snapshot: the full session minus its `messages` history.
/// Every field except `messages` is authoritative in the sidecar; on load the
/// message history is taken back from `session.json`.
fn runtime_sidecar_snapshot(session: &Session) -> Session {
    let mut snapshot = session.clone();
    snapshot.messages.clear();
    if let Some(metadata) = snapshot.runtime_metadata.as_mut() {
        // Admission ids must be committed atomically with their transcript
        // messages in session.json. Duplicating them into runtime.json would
        // allow a crash between the two files to expose dedupe state without
        // the corresponding message.
        metadata.session_inbox_admission = None;
    }
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
            let admission = main
                .runtime_metadata
                .as_ref()
                .and_then(|metadata| metadata.session_inbox_admission.clone());
            side.messages = main.messages;
            if let Some(admission) = admission {
                side.runtime_metadata
                    .get_or_insert_with(Default::default)
                    .session_inbox_admission = Some(admission);
            } else if let Some(metadata) = side.runtime_metadata.as_mut() {
                metadata.session_inbox_admission = None;
            }
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
    #[serde(default = "default_title_generated")]
    pub title_generated: bool,
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
    /// Compatibility indicator mirrored from the effective permission mode, so
    /// old session-list clients still see a permissive session without loading
    /// every session.json. True for both Bypass and Auto.
    #[serde(default)]
    pub bypass_permissions: bool,
    /// Typed permission mode mirrored from the session runtime state.
    #[serde(default)]
    pub permission_mode: bamboo_domain::SessionPermissionMode,
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

fn default_title_generated() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionsIndex {
    pub version: u32,
    pub updated_at: DateTime<Utc>,
    pub sessions: HashMap<String, SessionIndexEntry>,
    /// Durable crash-resume marker for an old/corrupt index rebuild. It lets a
    /// second constructor join an in-progress rebuild without clearing entries
    /// already recovered (or concurrently published by live session saves).
    #[serde(default)]
    rebuild_in_progress: bool,
}

impl SessionsIndex {
    fn empty() -> Self {
        Self {
            version: SESSIONS_INDEX_VERSION,
            updated_at: Utc::now(),
            sessions: HashMap::new(),
            rebuild_in_progress: false,
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
    /// Coordinates destructive target lifecycle transitions with durable inbox
    /// operations. The Tokio lock covers one runtime; the file lock covers
    /// independent Bamboo processes sharing the same data directory.
    session_lifecycle_lock: std::sync::Arc<RwLock<()>>,
    /// Coordinates every runtime-sidecar read/write with paired Task commits
    /// and orphan recovery. The Tokio gate prevents same-instance re-entry;
    /// the dedicated fs2 file lock covers independent store instances and
    /// processes sharing this Bamboo home.
    runtime_task_transaction_gate: std::sync::Arc<RwLock<()>>,
    /// When true, ordinary control-plane access fails closed until the retained
    /// undo journal has restored both sides of an interrupted transaction.
    runtime_task_recovery_required: AtomicBool,
    #[cfg(test)]
    runtime_task_faults: std::sync::Mutex<Vec<RuntimeTaskTransactionFault>>,
    #[cfg(test)]
    runtime_task_first_write_pause: std::sync::Mutex<Option<RuntimeTaskFirstWritePause>>,
    #[cfg(test)]
    runtime_task_durability_events: std::sync::Mutex<Vec<RuntimeTaskDurabilityEvent>>,
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

        // Index inspection and every decision that can publish, rename, or
        // replace sessions.json share the same cross-process claim. In
        // particular, a constructor that waited behind another process must
        // re-read the now-current file instead of publishing a stale empty or
        // corrupt-rebuild decision.
        let index_file_claim = lock_index_file_exclusive_at(&bamboo_home_dir).await?;

        // A corrupt index must not be boot-fatal: back it up and rebuild from
        // the on-disk session tree after construction. Only a *corrupt* file
        // triggers this; a *missing* one keeps the fresh-empty-index path.
        let mut needs_rebuild = false;
        let index = match fs::read_to_string(&index_path).await {
            Ok(raw) => match serde_json::from_str::<SessionsIndex>(&raw) {
                Ok(index) if index.version >= SESSIONS_INDEX_VERSION => index,
                Ok(index) => {
                    tracing::info!(
                        "migrating sessions index from version {} to version {} by rebuilding from session.json",
                        index.version,
                        SESSIONS_INDEX_VERSION,
                    );
                    needs_rebuild = true;
                    if index.rebuild_in_progress {
                        // Join/resume an existing rebuild without clearing
                        // entries already recovered or added by live writers.
                        index
                    } else {
                        let mut rebuilding = SessionsIndex::empty();
                        // Publish the old-version marker while the inspection
                        // claim is still held. Incremental rebuild writes keep
                        // this marker so a crash forces the next boot to resume.
                        rebuilding.version = index.version.min(SESSIONS_INDEX_VERSION - 1);
                        rebuilding.rebuild_in_progress = true;
                        persist_index_path_locked(&index_path, &rebuilding).await?;
                        rebuilding
                    }
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
                    rebuilding.rebuild_in_progress = true;
                    // The corrupt-read decision, backup, and replacement marker
                    // are one claimed boundary. A waiting constructor re-reads
                    // this marker instead of acting on stale corrupt bytes.
                    persist_index_path_locked(&index_path, &rebuilding).await?;
                    rebuilding
                }
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let index = SessionsIndex::empty();
                // Persist immediately so "index is mandatory" holds from boot.
                // The missing-file decision and publish are indivisible with
                // respect to every other constructor/index writer.
                persist_index_path_locked(&index_path, &index).await?;
                index
            }
            Err(error) => return Err(error),
        };

        // Rebuild scanning and FTS work can be large. Release the global claim
        // after the atomic marker decision; each incremental index mutation
        // below takes only the ordinary short update_index claim and rebases
        // from disk, preserving concurrent live writes.
        drop(index_file_claim);

        let storage = Self {
            bamboo_home_dir,
            sessions_dir,
            index_path,
            search_index,
            index: RwLock::new(index),
            write_lock: Mutex::new(()),
            session_lifecycle_lock: std::sync::Arc::new(RwLock::new(())),
            runtime_task_transaction_gate: std::sync::Arc::new(RwLock::new(())),
            runtime_task_recovery_required: AtomicBool::new(false),
            #[cfg(test)]
            runtime_task_faults: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            runtime_task_first_write_pause: std::sync::Mutex::new(None),
            #[cfg(test)]
            runtime_task_durability_events: std::sync::Mutex::new(Vec::new()),
        };

        // Create and permission the private journal directory once at store
        // initialization. Clean hot-path reads only probe it and never repeat
        // mkdir/chmod work; journal creation also revalidates it before write.
        storage.ensure_runtime_task_transaction_dir().await?;

        // Constructor recovery takes the same cross-process exclusive gate as
        // a live commit. A second store can therefore recover an orphan, but
        // can never mistake another process's in-flight journal for one.
        storage.recover_all_runtime_task_transactions().await?;

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
    /// `load_session`. The result is folded back in via the guarded,
    /// no-regression index repair path with the same `rel_path`
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

            // Re-probe and publish under the shared lifecycle boundary so a
            // concurrent exclusive delete either completes before this read or
            // removes the entry after this upsert; a late rebuild can never
            // resurrect a deleted directory.
            match self
                .rebuild_index_entry_from_dir(
                    &root_entry.path(),
                    &root_id,
                    Self::root_rel_path(&root_id),
                )
                .await
            {
                Ok(true) => recovered += 1,
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!("index rebuild: failed to index root {root_id}: {error}")
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
                match self
                    .rebuild_index_entry_from_dir(
                        &child_entry.path(),
                        &child_id,
                        Self::child_rel_path(&root_id, &child_id),
                    )
                    .await
                {
                    Ok(true) => recovered += 1,
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!("index rebuild: failed to index child {child_id}: {error}")
                    }
                }
            }
        }

        // The per-entry lifecycle -> shared-Task probes above must not be held
        // across this final phase: deletion owns lifecycle -> shared-Task, so
        // taking Task first and then attempting another lifecycle probe would
        // invert that order. A single shared Task guard here instead seals the
        // scan result against a new paired transaction and rejects any durable
        // journal left by a transaction that crashed between entry probes. In
        // that case the old rebuild marker remains retryable for the next boot.
        {
            let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;

            // Re-materialize sessions.json even when nothing was recovered (we
            // may have renamed the only copy to sessions.json.bak), so the
            // "index file always exists after boot" invariant holds.
            self.update_index(|index| {
                // Publishing the current version is the commit point for a complete rebuild.
                // `persist_index_locked` writes a temp file and atomically renames it.
                index.version = SESSIONS_INDEX_VERSION;
                index.rebuild_in_progress = false;
                Ok(())
            })
            .await?;
        }

        tracing::info!("index rebuild from disk complete: recovered {recovered} session(s)");

        // Pair transactions only modify Task list/generation, neither of which
        // is indexed by FTS. Keep this best-effort pass outside the final guard
        // so the server's background rebuild retains per-session lock scope.
        if let Err(error) = self.rebuild_search_index().await {
            tracing::warn!("index rebuild: failed to rebuild search index: {error}");
        }
        Ok(())
    }

    async fn rebuild_index_entry_from_dir(
        &self,
        abs_dir: &Path,
        session_id: &str,
        rel_path: String,
    ) -> io::Result<bool> {
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
        let Some(session) = Self::load_session_from_dir(abs_dir, session_id).await else {
            return Ok(false);
        };
        self.repair_index_from_authoritative_session(&session, rel_path)
            .await?;
        Ok(true)
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

    async fn open_session_lifecycle_file(&self, exclusive: bool) -> io::Result<std::fs::File> {
        let path = self.bamboo_home_dir.join(SESSION_LIFECYCLE_LOCK_FILE);
        tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path)?;
            if exclusive {
                FileExt::lock_exclusive(&file)?;
            } else {
                FileExt::lock_shared(&file)?;
            }
            Ok(file)
        })
        .await
        .map_err(|error| other_io_error(format!("join session lifecycle lock task: {error}")))?
    }

    async fn open_runtime_task_transaction_file(
        &self,
        exclusive: bool,
    ) -> io::Result<std::fs::File> {
        let path = self
            .bamboo_home_dir
            .join(RUNTIME_TASK_TRANSACTION_LOCK_FILE);
        let open = |path: PathBuf| async move {
            tokio::task::spawn_blocking(move || {
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&path)?;
                if exclusive {
                    FileExt::lock_exclusive(&file)?;
                } else {
                    FileExt::lock_shared(&file)?;
                }
                Ok::<_, io::Error>(file)
            })
            .await
            .map_err(|error| other_io_error(format!("join runtime Task lock task: {error}")))?
        };

        match open(path.clone()).await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // Keep the existing save-session recovery contract when an
                // embedding removes an otherwise idle Bamboo home between
                // construction and first use. The normal hot path performs no
                // mkdir; only a missing lock-file parent takes this retry.
                fs::create_dir_all(&self.bamboo_home_dir).await?;
                open(path).await
            }
            result => result,
        }
    }

    async fn lock_index_file_exclusive(&self) -> io::Result<SessionIndexFileGuard> {
        lock_index_file_exclusive_at(&self.bamboo_home_dir).await
    }

    pub(crate) async fn lock_session_lifecycle_shared(
        &self,
    ) -> io::Result<SessionLifecycleReadGuard> {
        let process = self.session_lifecycle_lock.clone().read_owned().await;
        let file = self.open_session_lifecycle_file(false).await?;
        Ok(SessionLifecycleReadGuard {
            _process: process,
            file,
        })
    }

    async fn lock_session_lifecycle_exclusive(&self) -> io::Result<SessionLifecycleWriteGuard> {
        let process = self.session_lifecycle_lock.clone().write_owned().await;
        let file = self.open_session_lifecycle_file(true).await?;
        Ok(SessionLifecycleWriteGuard {
            _process: process,
            file,
        })
    }

    async fn lock_runtime_task_transaction_shared(
        &self,
    ) -> io::Result<RuntimeTaskTransactionReadGuard> {
        let process = self
            .runtime_task_transaction_gate
            .clone()
            .read_owned()
            .await;
        let file = self.open_runtime_task_transaction_file(false).await?;
        Ok(RuntimeTaskTransactionReadGuard {
            _process: process,
            file,
        })
    }

    async fn lock_runtime_task_transaction_exclusive(
        &self,
    ) -> io::Result<RuntimeTaskTransactionWriteGuard> {
        let process = self
            .runtime_task_transaction_gate
            .clone()
            .write_owned()
            .await;
        let file = self.open_runtime_task_transaction_file(true).await?;
        Ok(RuntimeTaskTransactionWriteGuard {
            _process: process,
            file,
        })
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
        persist_index_path_locked(&self.index_path, index).await
    }

    async fn update_index<F, T>(&self, f: F) -> io::Result<T>
    where
        F: FnOnce(&mut SessionsIndex) -> io::Result<T>,
    {
        let _process = self.write_lock.lock().await;
        let _file = self.lock_index_file_exclusive().await?;
        self.update_index_under_claim(f).await
    }

    async fn update_index_under_claim<F, T>(&self, f: F) -> io::Result<T>
    where
        F: FnOnce(&mut SessionsIndex) -> io::Result<T>,
    {
        let mut memory = self.index.write().await;
        // Independent stores/processes have distinct in-memory snapshots. The
        // fixed file claim makes disk the rebase point for every mutation so a
        // stale writer cannot erase another process's just-published entries.
        // Missing disk falls back to this process's old-version rebuild marker.
        let mut index = match fs::read_to_string(&self.index_path).await {
            Ok(raw) => match serde_json::from_str::<SessionsIndex>(&raw) {
                Ok(index) => index,
                Err(_) if memory.version < SESSIONS_INDEX_VERSION => memory.clone(),
                Err(error) => {
                    return Err(other_io_error(format!(
                        "invalid sessions.json during locked update: {error}"
                    )))
                }
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => memory.clone(),
            Err(error) => return Err(error),
        };
        let out = f(&mut index)?;
        index.updated_at = Utc::now();
        self.persist_index_locked(&index).await?;
        *memory = index;
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

    /// Recover a root session by its deterministic authoritative directory,
    /// without trusting the rebuildable global index.
    ///
    /// Session-create idempotency uses this after an ambiguous commit: a crash
    /// can happen after `session.json` is durable but before `sessions.json` is
    /// published. When the authoritative file exists, this method loads it
    /// directly (including the runtime sidecar overlay) and repairs the global
    /// index before returning. FTS remains best-effort and is deliberately not
    /// part of this recovery barrier.
    pub async fn recover_root_session_from_disk(
        &self,
        session_id: &str,
    ) -> io::Result<Option<Session>> {
        validate_session_id(session_id)?;
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
        let Some(session) = self.load_authoritative_root_session(session_id).await? else {
            return Ok(None);
        };
        self.repair_index_from_authoritative_session(&session, Self::root_rel_path(session_id))
            .await?;
        Ok(Some(session))
    }

    /// Strictly probe the deterministic root `session.json` without consulting
    /// or mutating the rebuildable index. Missing is distinct from corrupt or
    /// unreadable so an idempotent pending retry never overwrites damaged
    /// authoritative data with the reserved UUID.
    pub async fn probe_root_session_from_disk(
        &self,
        session_id: &str,
    ) -> io::Result<Option<Session>> {
        validate_session_id(session_id)?;
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
        self.load_authoritative_root_session(session_id).await
    }

    async fn load_authoritative_root_session(
        &self,
        session_id: &str,
    ) -> io::Result<Option<Session>> {
        let abs_dir = self.sessions_dir.join(session_id);
        let raw = match fs::read_to_string(abs_dir.join("session.json")).await {
            Ok(raw) => raw,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        // Unlike best-effort global-index rebuild, operation recovery must not
        // collapse an unreadable/corrupt authoritative result into "missing":
        // doing so could turn a repairable failure into terminal 410 truth.
        let main: Session = serde_json::from_str(&raw).map_err(|error| {
            other_io_error(format!("invalid authoritative session.json: {error}"))
        })?;
        let sidecar =
            Self::read_runtime_sidecar_at(&abs_dir.join(RUNTIME_SIDECAR_FILE), session_id).await?;
        let mut session = overlay_runtime_sidecar(main, sidecar);
        session.clear_stale_root_token_budget();
        if session.id != session_id || session.kind != SessionKind::Root {
            return Err(other_io_error(
                "authoritative root session identity or kind mismatch",
            ));
        }
        Ok(Some(session))
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

    fn runtime_task_transaction_dir(&self) -> PathBuf {
        self.bamboo_home_dir.join(RUNTIME_TASK_TRANSACTION_DIR)
    }

    async fn ensure_runtime_task_transaction_dir(&self) -> io::Result<PathBuf> {
        let path = self.runtime_task_transaction_dir();
        let home_existed = fs::try_exists(&self.bamboo_home_dir).await?;
        fs::create_dir_all(&self.bamboo_home_dir).await?;
        if !home_existed {
            sync_parent_directory_entry(&self.bamboo_home_dir).await?;
        }
        let path_existed = fs::try_exists(&path).await?;
        fs::create_dir_all(&path).await?;
        // Journals contain only Task lists/generations (never transcripts or
        // arbitrary metadata), but Task descriptions may still be private user
        // data. Restrict the directory even when the process umask is loose.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).await?;
        }
        if !path_existed {
            // The journal file cannot be durable if the directory containing it
            // can itself disappear after a power loss.
            sync_parent_directory_entry(&path).await?;
        }
        Ok(path)
    }

    fn runtime_task_recovery_error() -> io::Error {
        other_io_error("runtime Task transaction recovery is required before control-plane access")
    }

    async fn ensure_no_runtime_task_journal_locked(&self) -> io::Result<()> {
        match self.runtime_task_journal_paths().await {
            Ok(paths) if paths.is_empty() => {
                self.runtime_task_recovery_required
                    .store(false, Ordering::Release);
                Ok(())
            }
            Ok(_) => {
                self.runtime_task_recovery_required
                    .store(true, Ordering::Release);
                Err(Self::runtime_task_recovery_error())
            }
            Err(error) => {
                self.runtime_task_recovery_required
                    .store(true, Ordering::Release);
                Err(error)
            }
        }
    }

    /// Acquire the ordinary shared sidecar boundary, then consult the durable
    /// journal directory rather than this instance's in-memory flag. Once the
    /// shared file lock is held no live commit can create/remove a journal, so
    /// an existing entry is necessarily recovery-required and access fails
    /// closed even in a freshly constructed independent store.
    async fn lock_runtime_task_sidecar_shared(
        &self,
    ) -> io::Result<RuntimeTaskTransactionReadGuard> {
        let guard = self.lock_runtime_task_transaction_shared().await?;
        self.ensure_no_runtime_task_journal_locked().await?;
        Ok(guard)
    }

    #[cfg(test)]
    pub(crate) fn inject_runtime_task_transaction_fault(&self, fault: RuntimeTaskTransactionFault) {
        self.runtime_task_faults
            .lock()
            .expect("runtime task fault lock")
            .push(fault);
    }

    fn record_runtime_task_durability_event(&self, event: RuntimeTaskDurabilityEvent) {
        #[cfg(test)]
        self.runtime_task_durability_events
            .lock()
            .expect("runtime task durability event lock")
            .push(event);
        #[cfg(not(test))]
        let _ = event;
    }

    #[cfg(test)]
    fn take_runtime_task_durability_events(&self) -> Vec<RuntimeTaskDurabilityEvent> {
        std::mem::take(
            &mut *self
                .runtime_task_durability_events
                .lock()
                .expect("runtime task durability event lock"),
        )
    }

    #[cfg(test)]
    fn pause_runtime_task_transaction_after_first_write(
        &self,
    ) -> (
        std::sync::Arc<tokio::sync::Barrier>,
        std::sync::Arc<tokio::sync::Barrier>,
    ) {
        let pause = RuntimeTaskFirstWritePause {
            reached: std::sync::Arc::new(tokio::sync::Barrier::new(2)),
            release: std::sync::Arc::new(tokio::sync::Barrier::new(2)),
        };
        *self
            .runtime_task_first_write_pause
            .lock()
            .expect("runtime task pause lock") = Some(pause.clone());
        (pause.reached, pause.release)
    }

    #[cfg(test)]
    async fn maybe_pause_runtime_task_transaction_after_first_write(&self) {
        let pause = self
            .runtime_task_first_write_pause
            .lock()
            .expect("runtime task pause lock")
            .take();
        if let Some(pause) = pause {
            pause.reached.wait().await;
            pause.release.wait().await;
        }
    }

    fn maybe_fail_runtime_task_transaction(
        &self,
        fault: RuntimeTaskTransactionFault,
    ) -> io::Result<()> {
        #[cfg(test)]
        {
            let mut faults = self
                .runtime_task_faults
                .lock()
                .expect("runtime task fault lock");
            if let Some(index) = faults.iter().position(|candidate| *candidate == fault) {
                faults.remove(index);
                return Err(other_io_error(format!(
                    "injected runtime Task transaction fault: {fault:?}"
                )));
            }
        }
        #[cfg(not(test))]
        let _ = fault;
        Ok(())
    }

    async fn load_runtime_control_plane_unchecked(
        &self,
        session_id: &str,
    ) -> io::Result<Option<Session>> {
        validate_session_id(session_id)?;
        if let Some(side) = self.read_runtime_sidecar(session_id).await? {
            return Ok(Some(side));
        }
        let Some(path) = self.session_json_path(session_id).await? else {
            return Ok(None);
        };
        let raw = match fs::read_to_string(path).await {
            Ok(raw) => raw,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut session: Session = serde_json::from_str(&raw)
            .map_err(|error| other_io_error(format!("invalid session.json: {error}")))?;
        session.messages.clear();
        session.clear_stale_root_token_budget();
        Ok(Some(session))
    }

    fn validate_runtime_task_recovery_identity(
        session: &Session,
        session_id: &str,
        expected_kind: SessionKind,
        expected_root_id: &str,
    ) -> io::Result<()> {
        if session.id != session_id
            || session.kind != expected_kind
            || session.root_session_id != expected_root_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime Task recovery identity mismatch for {session_id}: found id={}, kind={:?}, root={}",
                    session.id, session.kind, session.root_session_id
                ),
            ));
        }
        Ok(())
    }

    async fn runtime_task_recovery_real_directory(path: &Path) -> io::Result<bool> {
        let metadata = match fs::symlink_metadata(path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime Task recovery candidate is not a real directory: {}",
                    path.display()
                ),
            ));
        }
        Ok(true)
    }

    async fn runtime_task_recovery_real_file(path: &Path) -> io::Result<bool> {
        let metadata = match fs::symlink_metadata(path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime Task recovery candidate is not a real file: {}",
                    path.display()
                ),
            ));
        }
        Ok(true)
    }

    async fn read_runtime_task_recovery_main_at(
        abs_dir: &Path,
        session_id: &str,
        expected_kind: SessionKind,
        expected_root_id: &str,
    ) -> io::Result<Option<Session>> {
        if !Self::runtime_task_recovery_real_directory(abs_dir).await? {
            return Ok(None);
        }
        let path = abs_dir.join("session.json");
        if !Self::runtime_task_recovery_real_file(&path).await? {
            return Ok(None);
        }
        let raw = fs::read_to_string(&path).await?;
        let session: Session = serde_json::from_str(&raw).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid authoritative session.json for runtime Task recovery at {}: {error}",
                    path.display()
                ),
            )
        })?;
        Self::validate_runtime_task_recovery_identity(
            &session,
            session_id,
            expected_kind,
            expected_root_id,
        )?;
        Ok(Some(session))
    }

    /// Resolve one journal target without consulting the rebuildable index.
    ///
    /// Constructor recovery deliberately runs before corrupt-index rebuild so
    /// no half-published Task sidecar can be folded into index/FTS state. The
    /// caller holds the cross-process runtime-Task lock exclusively: session
    /// deletion therefore cannot mutate the tree while this strict scan finds
    /// either `sessions/<id>` or the unique
    /// `sessions/*/children/<id>`. Corrupt identity or duplicate candidates
    /// fail closed instead of selecting an arbitrary sidecar.
    async fn load_runtime_task_recovery_target(
        &self,
        session_id: &str,
    ) -> io::Result<Option<(PathBuf, Session)>> {
        validate_session_id(session_id)?;
        if !Self::runtime_task_recovery_real_directory(&self.sessions_dir).await? {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "runtime Task recovery sessions directory is missing",
            ));
        }
        let mut candidates = Vec::new();

        let root_dir = self.sessions_dir.join(session_id);
        if let Some(session) = Self::read_runtime_task_recovery_main_at(
            &root_dir,
            session_id,
            SessionKind::Root,
            session_id,
        )
        .await?
        {
            candidates.push((root_dir, session));
        }

        let mut root_dirs = match fs::read_dir(&self.sessions_dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(candidates.pop()),
            Err(error) => return Err(error),
        };
        while let Some(root_entry) = root_dirs.next_entry().await? {
            if !root_entry.file_type().await?.is_dir() {
                continue;
            }
            let Ok(root_id) = root_entry.file_name().into_string() else {
                continue;
            };
            if validate_session_id(&root_id).is_err() {
                continue;
            }
            let children_dir = root_entry.path().join("children");
            let children_metadata = match fs::symlink_metadata(&children_dir).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            // This is an intermediate container rather than an exact journal
            // target. Match rebuild semantics by ignoring malformed/symlinked
            // children trees; the exact target will then remain missing and
            // recovery fails closed without following it outside sessions/.
            if children_metadata.file_type().is_symlink() || !children_metadata.is_dir() {
                continue;
            }
            let child_dir = children_dir.join(session_id);
            if let Some(session) = Self::read_runtime_task_recovery_main_at(
                &child_dir,
                session_id,
                SessionKind::Child,
                &root_id,
            )
            .await?
            {
                if Self::read_runtime_task_recovery_main_at(
                    &root_entry.path(),
                    &root_id,
                    SessionKind::Root,
                    &root_id,
                )
                .await?
                .is_none()
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "runtime Task recovery child {session_id} has no authoritative root {root_id}"
                        ),
                    ));
                }
                candidates.push((child_dir, session));
            }
        }

        match candidates.len() {
            0 => Ok(None),
            1 => Ok(candidates.pop()),
            count => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime Task recovery target {session_id} is ambiguous across {count} authoritative directories"
                ),
            )),
        }
    }

    async fn validate_runtime_task_recovery_write_target(
        &self,
        abs_dir: &Path,
        current: &Session,
    ) -> io::Result<()> {
        if !Self::runtime_task_recovery_real_directory(&self.sessions_dir).await? {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "runtime Task recovery sessions directory is missing",
            ));
        }
        let expected_dir = match current.kind {
            SessionKind::Root => self.sessions_dir.join(&current.id),
            SessionKind::Child => self
                .sessions_dir
                .join(&current.root_session_id)
                .join("children")
                .join(&current.id),
        };
        if abs_dir != expected_dir {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime Task recovery path mismatch for {}: {}",
                    current.id,
                    abs_dir.display()
                ),
            ));
        }

        if current.kind == SessionKind::Child {
            let root_dir = self.sessions_dir.join(&current.root_session_id);
            if Self::read_runtime_task_recovery_main_at(
                &root_dir,
                &current.root_session_id,
                SessionKind::Root,
                &current.root_session_id,
            )
            .await?
            .is_none()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "runtime Task recovery child {} lost authoritative root {}",
                        current.id, current.root_session_id
                    ),
                ));
            }
            let children_dir = root_dir.join("children");
            if !Self::runtime_task_recovery_real_directory(&children_dir).await? {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "runtime Task recovery children directory disappeared for {}",
                        current.id
                    ),
                ));
            }
        }
        let Some(main) = Self::read_runtime_task_recovery_main_at(
            abs_dir,
            &current.id,
            current.kind,
            &current.root_session_id,
        )
        .await?
        else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "runtime Task recovery target disappeared for {}",
                    current.id
                ),
            ));
        };
        if main.parent_session_id != current.parent_session_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime Task recovery parent identity changed for {}",
                    current.id
                ),
            ));
        }

        let runtime_path = abs_dir.join(RUNTIME_SIDECAR_FILE);
        let _ = Self::runtime_task_recovery_real_file(&runtime_path).await?;
        Ok(())
    }

    async fn load_runtime_task_recovery_control_plane(
        &self,
        session_id: &str,
    ) -> io::Result<Option<(PathBuf, Session)>> {
        let Some((abs_dir, main)) = self.load_runtime_task_recovery_target(session_id).await?
        else {
            return Ok(None);
        };
        let runtime_path = abs_dir.join(RUNTIME_SIDECAR_FILE);
        let sidecar = if Self::runtime_task_recovery_real_file(&runtime_path).await? {
            Self::read_runtime_sidecar_at(&runtime_path, session_id).await?
        } else {
            None
        };
        if let Some(sidecar) = sidecar.as_ref() {
            Self::validate_runtime_task_recovery_identity(
                sidecar,
                session_id,
                main.kind,
                &main.root_session_id,
            )?;
            if sidecar.parent_session_id != main.parent_session_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("runtime Task recovery parent identity mismatch for {session_id}"),
                ));
            }
        }
        let mut session = overlay_runtime_sidecar(main, sidecar);
        session.messages.clear();
        session.clear_stale_root_token_budget();
        Ok(Some((abs_dir, session)))
    }

    async fn write_existing_runtime_sidecar_durable_unchecked(
        &self,
        session: &Session,
        event: RuntimeTaskDurabilityEvent,
    ) -> io::Result<()> {
        validate_session_id(&session.id)?;
        let Some(rel) = self.resolve_rel_path(&session.id).await else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("session {} has no persisted runtime target", session.id),
            ));
        };
        self.write_runtime_sidecar_durable(&self.abs_path_from_rel(&rel), session)
            .await?;
        self.record_runtime_task_durability_event(event);
        Ok(())
    }

    async fn runtime_task_journal_paths(&self) -> io::Result<Vec<PathBuf>> {
        let dir = self.runtime_task_transaction_dir();
        let mut entries = match fs::read_dir(dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut paths = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if entry
                .file_type()
                .await
                .map(|kind| kind.is_file())
                .unwrap_or(false)
                && RuntimeTaskJournalMarkerState::from_path(&path).is_some()
            {
                paths.push(path);
            }
        }
        paths.sort();
        Ok(paths)
    }

    fn validate_runtime_task_journal(
        path: &Path,
        journal: &RuntimeTaskTransactionJournal,
    ) -> io::Result<()> {
        if journal.version != RUNTIME_TASK_TRANSACTION_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported runtime Task transaction journal version {}",
                    journal.version
                ),
            ));
        }
        let file_id = path
            .file_stem()
            .and_then(|value| value.to_str())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid journal name"))?;
        Uuid::parse_str(file_id).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid runtime Task transaction journal UUID: {error}"),
            )
        })?;
        if journal.transaction_id != file_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime Task transaction journal id does not match its filename",
            ));
        }
        validate_session_id(&journal.first.session_id)?;
        validate_session_id(&journal.second.session_id)?;
        if journal.first.session_id >= journal.second.session_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime Task transaction journal pair is not lexically ordered",
            ));
        }
        Ok(())
    }

    async fn read_runtime_task_journal(
        &self,
        path: &Path,
    ) -> io::Result<RuntimeTaskTransactionJournal> {
        let raw = fs::read_to_string(path).await?;
        let journal: RuntimeTaskTransactionJournal =
            serde_json::from_str(&raw).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid runtime Task transaction journal: {error}"),
                )
            })?;
        Self::validate_runtime_task_journal(path, &journal)?;
        Ok(journal)
    }

    async fn write_runtime_task_journal(
        &self,
        journal: &RuntimeTaskTransactionJournal,
    ) -> io::Result<PathBuf> {
        let dir = self.ensure_runtime_task_transaction_dir().await?;
        let path = dir.join(format!("{}.json", journal.transaction_id));
        let bytes = serde_json::to_vec(journal)
            .map_err(|error| other_io_error(format!("serialize Task undo journal: {error}")))?;
        durable_atomic_write(&path, &bytes).await?;
        self.record_runtime_task_durability_event(RuntimeTaskDurabilityEvent::JournalPublished);
        Ok(path)
    }

    async fn remove_runtime_task_journal(&self, path: &Path) -> io::Result<()> {
        durable_deactivate_recovery_marker(path).await?;
        self.record_runtime_task_durability_event(RuntimeTaskDurabilityEvent::JournalDeactivated);
        Ok(())
    }

    async fn remove_runtime_task_journal_family(
        &self,
        journal: &RuntimeTaskTransactionJournal,
    ) -> io::Result<()> {
        let dir = self.runtime_task_transaction_dir();
        for extension in ["json", "committing", "committed"] {
            let path = dir.join(format!("{}.{}", journal.transaction_id, extension));
            match fs::try_exists(&path).await {
                Ok(true) => self.remove_runtime_task_journal(&path).await?,
                Ok(false) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Transition a prepared undo journal through a fail-closed intermediate
    /// name to a committed marker, then durably deactivate it. Recovery rolls
    /// back `.json`/`.committing`, but never rolls back `.committed`; a failure
    /// after the committed rename therefore cannot turn two durable sidecars
    /// into a later undo.
    async fn finalize_runtime_task_journal(
        &self,
        path: &Path,
    ) -> Result<(), RuntimeTaskJournalFinalizeError> {
        self.maybe_fail_runtime_task_transaction(RuntimeTaskTransactionFault::JournalRemove)
            .map_err(RuntimeTaskJournalFinalizeError::Rollback)?;

        let committing = path.with_extension("committing");
        atomic_rename(path, &committing)
            .await
            .map_err(RuntimeTaskJournalFinalizeError::Rollback)?;
        sync_parent_directory_entry(&committing)
            .await
            .map_err(RuntimeTaskJournalFinalizeError::Rollback)?;

        let committed = path.with_extension("committed");
        atomic_rename(&committing, &committed)
            .await
            .map_err(RuntimeTaskJournalFinalizeError::Rollback)?;
        sync_parent_directory_entry(&committed)
            .await
            .map_err(RuntimeTaskJournalFinalizeError::RecoveryRequired)?;
        self.remove_runtime_task_journal(&committed)
            .await
            .map_err(RuntimeTaskJournalFinalizeError::RecoveryRequired)
    }

    async fn restore_runtime_task_undo(
        &self,
        undo: &TaskControlPlaneUndo,
        fault: RuntimeTaskTransactionFault,
    ) -> io::Result<()> {
        self.maybe_fail_runtime_task_transaction(fault)?;
        let Some((abs_dir, mut current)) = self
            .load_runtime_task_recovery_control_plane(&undo.session_id)
            .await?
        else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("cannot recover missing session {}", undo.session_id),
            ));
        };
        current.task_list = undo.task_list.clone();
        current.set_task_list_version_meta(undo.task_list_version.clone());
        let event = match fault {
            RuntimeTaskTransactionFault::FirstRollbackWrite => {
                RuntimeTaskDurabilityEvent::FirstRollbackSidecarPublished
            }
            RuntimeTaskTransactionFault::SecondRollbackWrite => {
                RuntimeTaskDurabilityEvent::SecondRollbackSidecarPublished
            }
            _ => unreachable!("rollback helper only accepts rollback write faults"),
        };
        self.validate_runtime_task_recovery_write_target(&abs_dir, &current)
            .await?;
        self.write_runtime_sidecar_durable(&abs_dir, &current)
            .await?;
        self.record_runtime_task_durability_event(event);
        Ok(())
    }

    async fn rollback_runtime_task_journal(
        &self,
        journal: &RuntimeTaskTransactionJournal,
    ) -> io::Result<()> {
        let mut errors = Vec::new();
        if let Err(error) = self
            .restore_runtime_task_undo(
                &journal.first,
                RuntimeTaskTransactionFault::FirstRollbackWrite,
            )
            .await
        {
            errors.push(format!("{}: {error}", journal.first.session_id));
        }
        if let Err(error) = self
            .restore_runtime_task_undo(
                &journal.second,
                RuntimeTaskTransactionFault::SecondRollbackWrite,
            )
            .await
        {
            errors.push(format!("{}: {error}", journal.second.session_id));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(other_io_error(format!(
                "runtime Task transaction rollback failed for {}",
                errors.join(", ")
            )))
        }
    }

    async fn recover_runtime_task_journal(
        &self,
        path: &Path,
        journal: &RuntimeTaskTransactionJournal,
    ) -> io::Result<()> {
        let state = RuntimeTaskJournalMarkerState::from_path(path).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid runtime Task journal marker extension",
            )
        })?;
        if state != RuntimeTaskJournalMarkerState::Committed {
            self.rollback_runtime_task_journal(journal).await?;
        }
        self.remove_runtime_task_journal(path).await
    }

    async fn recover_all_runtime_task_transactions_locked(&self) -> io::Result<()> {
        let paths = self.runtime_task_journal_paths().await?;
        for path in paths {
            let journal = match self.read_runtime_task_journal(&path).await {
                Ok(journal) => journal,
                Err(error) => {
                    self.runtime_task_recovery_required
                        .store(true, Ordering::Release);
                    return Err(error);
                }
            };
            if let Err(error) = self.recover_runtime_task_journal(&path, &journal).await {
                self.runtime_task_recovery_required
                    .store(true, Ordering::Release);
                return Err(error);
            }
        }
        self.runtime_task_recovery_required
            .store(false, Ordering::Release);
        Ok(())
    }

    async fn recover_all_runtime_task_transactions(&self) -> io::Result<()> {
        let _guard = self.lock_runtime_task_transaction_exclusive().await?;
        self.recover_all_runtime_task_transactions_locked().await
    }

    async fn recover_runtime_task_transaction_for_pair_locked(
        &self,
        first_session_id: &str,
        second_session_id: &str,
    ) -> io::Result<()> {
        validate_session_id(first_session_id)?;
        validate_session_id(second_session_id)?;
        if first_session_id >= second_session_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "runtime Task recovery pair must be lexically ordered",
            ));
        }

        let paths = self.runtime_task_journal_paths().await?;
        for path in paths {
            let journal = self.read_runtime_task_journal(&path).await?;
            if RuntimeTaskJournalMarkerState::from_path(&path)
                == Some(RuntimeTaskJournalMarkerState::Committed)
            {
                self.recover_runtime_task_journal(&path, &journal).await?;
                continue;
            }
            if journal.first.session_id != first_session_id
                || journal.second.session_id != second_session_id
            {
                self.runtime_task_recovery_required
                    .store(true, Ordering::Release);
                return Err(other_io_error(format!(
                    "pending runtime Task transaction for {}/{} must be recovered before accessing {}/{}",
                    journal.first.session_id,
                    journal.second.session_id,
                    first_session_id,
                    second_session_id
                )));
            }
            if let Err(error) = self.recover_runtime_task_journal(&path, &journal).await {
                self.runtime_task_recovery_required
                    .store(true, Ordering::Release);
                return Err(error);
            }
        }
        self.runtime_task_recovery_required
            .store(false, Ordering::Release);
        Ok(())
    }

    async fn fail_runtime_task_transaction_with_rollback(
        &self,
        _path: &Path,
        journal: &RuntimeTaskTransactionJournal,
        primary: io::Error,
    ) -> io::Result<()> {
        let primary_kind = primary.kind();
        let primary_message = primary.to_string();
        match self.rollback_runtime_task_journal(journal).await {
            Ok(()) => match self.remove_runtime_task_journal_family(journal).await {
                Ok(()) => {
                    self.runtime_task_recovery_required
                        .store(false, Ordering::Release);
                    Err(io::Error::new(
                        primary_kind,
                        format!(
                            "runtime Task transaction failed and was rolled back: {primary_message}"
                        ),
                    ))
                }
                Err(cleanup_error) => {
                    self.runtime_task_recovery_required
                        .store(true, Ordering::Release);
                    Err(other_io_error(format!(
                        "runtime Task transaction failed ({primary_message}); rollback succeeded but journal cleanup failed ({cleanup_error}); recovery required"
                    )))
                }
            },
            Err(rollback_error) => {
                self.runtime_task_recovery_required
                    .store(true, Ordering::Release);
                Err(other_io_error(format!(
                    "runtime Task transaction failed ({primary_message}); {rollback_error}; recovery required"
                )))
            }
        }
    }

    async fn save_runtime_task_control_plane_if_matches(
        &self,
        original: &Session,
        updated: &Session,
    ) -> io::Result<bool> {
        validate_session_id(&original.id)?;
        validate_session_id(&updated.id)?;
        if original.id != updated.id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "single Task snapshots must preserve the session id",
            ));
        }
        if Self::runtime_task_non_task_snapshot(original)?
            != Self::runtime_task_non_task_snapshot(updated)?
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "single Task update modifies fields outside Task list/generation",
            ));
        }
        if updated.task_list.is_none() || updated.task_list_version_meta().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "single Task update requires a list and generation",
            ));
        }
        let Some(current) = self
            .load_runtime_control_plane_unchecked(&original.id)
            .await?
        else {
            return Ok(false);
        };
        if !Self::runtime_task_owned_snapshot_matches(&current, original)? {
            return Ok(false);
        }
        let mut committed = current;
        committed.task_list = updated.task_list.clone();
        committed.set_task_list_version_meta(
            updated
                .task_list_version_meta()
                .expect("validated updated Task generation"),
        );
        self.write_existing_runtime_sidecar_durable_unchecked(
            &committed,
            RuntimeTaskDurabilityEvent::SingleUpdatedSidecarPublished,
        )
        .await?;
        Ok(true)
    }

    async fn save_runtime_task_pair_transaction(
        &self,
        first_original: &Session,
        first_updated: &Session,
        second_original: &Session,
        second_updated: &Session,
    ) -> io::Result<bool> {
        for session in [
            first_original,
            first_updated,
            second_original,
            second_updated,
        ] {
            validate_session_id(&session.id)?;
        }
        if first_original.id != first_updated.id
            || second_original.id != second_updated.id
            || first_original.id >= second_original.id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "paired Task snapshots must preserve ids and be lexically ordered",
            ));
        }
        for (label, original, updated) in [
            ("first", first_original, first_updated),
            ("second", second_original, second_updated),
        ] {
            if Self::runtime_task_non_task_snapshot(original)?
                != Self::runtime_task_non_task_snapshot(updated)?
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "{label} paired Task update modifies fields outside Task list/generation"
                    ),
                ));
            }
        }
        if first_updated.task_list.is_none()
            || first_updated.task_list_version_meta().is_none()
            || first_updated.task_list_version_meta() != second_updated.task_list_version_meta()
            || serde_json::to_value(&first_updated.task_list)
                .map_err(|error| other_io_error(error.to_string()))?
                != serde_json::to_value(&second_updated.task_list)
                    .map_err(|error| other_io_error(error.to_string()))?
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "paired Task updates must publish identical Task lists/generations",
            ));
        }
        // This is the final CAS boundary. Different LockedSessionStore
        // instances have independent lexical mutexes, so both may stage from
        // the same generation before either reaches this storage transaction.
        // Re-read both Task-owned snapshots while the cross-process exclusive
        // lock is held; a loser returns a typed stale result without creating a
        // journal or touching either sidecar.
        let Some(current_first) = self
            .load_runtime_control_plane_unchecked(&first_original.id)
            .await?
        else {
            return Ok(false);
        };
        let Some(current_second) = self
            .load_runtime_control_plane_unchecked(&second_original.id)
            .await?
        else {
            return Ok(false);
        };
        if !Self::runtime_task_owned_snapshot_matches(&current_first, first_original)?
            || !Self::runtime_task_owned_snapshot_matches(&current_second, second_original)?
        {
            return Ok(false);
        }

        let first_version = current_first.task_list_version_meta().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "first current Task snapshot has no generation",
            )
        })?;
        let second_version = current_second.task_list_version_meta().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "second current Task snapshot has no generation",
            )
        })?;
        // The staging snapshots may predate unrelated status/round/inbox or
        // metadata writes from another store. Build the physical writes from
        // the just-revalidated durable snapshots and patch only Task-owned
        // fields, so the transaction remains narrowly targeted.
        let mut first_commit = current_first.clone();
        first_commit.task_list = first_updated.task_list.clone();
        first_commit.set_task_list_version_meta(
            first_updated
                .task_list_version_meta()
                .expect("validated updated Task generation"),
        );
        let mut second_commit = current_second.clone();
        second_commit.task_list = second_updated.task_list.clone();
        second_commit.set_task_list_version_meta(
            second_updated
                .task_list_version_meta()
                .expect("validated updated Task generation"),
        );

        let transaction_id = Uuid::new_v4().to_string();
        let journal = RuntimeTaskTransactionJournal {
            version: RUNTIME_TASK_TRANSACTION_VERSION,
            transaction_id,
            first: TaskControlPlaneUndo {
                session_id: current_first.id.clone(),
                task_list: current_first.task_list.clone(),
                task_list_version: first_version,
            },
            second: TaskControlPlaneUndo {
                session_id: current_second.id.clone(),
                task_list: current_second.task_list.clone(),
                task_list_version: second_version,
            },
        };
        let journal_path = self.write_runtime_task_journal(&journal).await?;
        self.runtime_task_recovery_required
            .store(true, Ordering::Release);

        if let Err(error) =
            self.maybe_fail_runtime_task_transaction(RuntimeTaskTransactionFault::FirstUpdatedWrite)
        {
            return self
                .fail_runtime_task_transaction_with_rollback(&journal_path, &journal, error)
                .await
                .map(|()| true);
        }
        if let Err(error) = self
            .write_existing_runtime_sidecar_durable_unchecked(
                &first_commit,
                RuntimeTaskDurabilityEvent::FirstUpdatedSidecarPublished,
            )
            .await
        {
            return self
                .fail_runtime_task_transaction_with_rollback(&journal_path, &journal, error)
                .await
                .map(|()| true);
        }

        #[cfg(test)]
        self.maybe_pause_runtime_task_transaction_after_first_write()
            .await;

        if let Err(error) = self
            .maybe_fail_runtime_task_transaction(RuntimeTaskTransactionFault::SecondUpdatedWrite)
        {
            return self
                .fail_runtime_task_transaction_with_rollback(&journal_path, &journal, error)
                .await
                .map(|()| true);
        }
        if let Err(error) = self
            .write_existing_runtime_sidecar_durable_unchecked(
                &second_commit,
                RuntimeTaskDurabilityEvent::SecondUpdatedSidecarPublished,
            )
            .await
        {
            return self
                .fail_runtime_task_transaction_with_rollback(&journal_path, &journal, error)
                .await
                .map(|()| true);
        }

        match self.finalize_runtime_task_journal(&journal_path).await {
            Ok(()) => {}
            Err(RuntimeTaskJournalFinalizeError::Rollback(error)) => {
                return self
                    .fail_runtime_task_transaction_with_rollback(&journal_path, &journal, error)
                    .await
                    .map(|()| true);
            }
            Err(RuntimeTaskJournalFinalizeError::RecoveryRequired(error)) => {
                // Both sidecars were synchronized before the committed marker
                // transition began. Never roll them back after a committed
                // marker may be visible: exclusive recovery will retain the
                // committed pair and durably deactivate that marker.
                self.runtime_task_recovery_required
                    .store(true, Ordering::Release);
                return Err(other_io_error(format!(
                    "runtime Task transaction committed durably but marker cleanup requires recovery: {error}"
                )));
            }
        }
        self.runtime_task_recovery_required
            .store(false, Ordering::Release);
        Ok(true)
    }

    fn runtime_task_owned_snapshot_matches(
        current: &Session,
        expected: &Session,
    ) -> io::Result<bool> {
        Ok(
            current.task_list_version_meta() == expected.task_list_version_meta()
                && serde_json::to_value(&current.task_list)
                    .map_err(|error| other_io_error(error.to_string()))?
                    == serde_json::to_value(&expected.task_list)
                        .map_err(|error| other_io_error(error.to_string()))?,
        )
    }

    fn ordinary_task_write_would_regress(
        incoming: &Session,
        durable: &Session,
    ) -> io::Result<bool> {
        if Self::runtime_task_owned_snapshot_matches(incoming, durable)? {
            return Ok(false);
        }
        let incoming_version = incoming.task_list_version_meta();
        let durable_version = durable.task_list_version_meta();
        match (incoming_version.as_deref(), durable_version.as_deref()) {
            (_, None) => Ok(false),
            (None, Some(_)) => Ok(true),
            (Some(incoming), Some(durable)) => {
                match (incoming.parse::<u64>(), durable.parse::<u64>()) {
                    (Ok(incoming), Ok(durable)) => Ok(incoming <= durable),
                    // Production Task generations are monotonic integers. An
                    // incomparable legacy/custom generation cannot prove it is
                    // newer, so preserve the durable Task snapshot fail-closed.
                    _ => Ok(true),
                }
            }
        }
    }

    /// Ordinary full/runtime saves may have queued on the shared file lock
    /// while a paired transaction advanced the durable generation. They must
    /// not silently report success after substituting a different Task
    /// snapshot: upper layers would then publish the caller's stale value into
    /// their cache. Fail before any write so the locked persistence boundary
    /// can adopt the durable Task fields and retry with one coherent snapshot.
    async fn reject_regressing_runtime_task(&self, incoming: &Session) -> io::Result<()> {
        if let Some(durable) = self
            .load_runtime_control_plane_unchecked(&incoming.id)
            .await?
        {
            if Self::ordinary_task_write_would_regress(incoming, &durable)? {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "Task control-plane changed while saving session {}",
                        incoming.id
                    ),
                ));
            }
        }
        Ok(())
    }

    fn runtime_task_non_task_snapshot(session: &Session) -> io::Result<serde_json::Value> {
        let mut snapshot = session.clone();
        snapshot.task_list = None;
        snapshot
            .metadata
            .remove(bamboo_domain::session::runtime_metadata::keys::TASK_LIST_VERSION);
        if let Some(runtime_metadata) = snapshot.runtime_metadata.as_mut() {
            runtime_metadata.task_list_version = None;
        }
        if snapshot
            .runtime_metadata
            .as_ref()
            .is_some_and(bamboo_domain::session::SessionRuntimeMetadata::is_empty)
        {
            snapshot.runtime_metadata = None;
        }
        serde_json::to_value(snapshot).map_err(|error| {
            other_io_error(format!("serialize Task transaction snapshot: {error}"))
        })
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

    /// Task CAS/transaction replacement with a file+directory durability
    /// boundary. Ordinary runtime saves intentionally keep the cheaper helper
    /// above; only authoritative Task commits and recovery pay these fsyncs.
    async fn write_runtime_sidecar_durable(
        &self,
        abs_dir: &Path,
        session: &Session,
    ) -> io::Result<()> {
        let path = abs_dir.join(RUNTIME_SIDECAR_FILE);
        let snapshot = runtime_sidecar_snapshot(session);
        let bytes = serde_json::to_vec_pretty(&snapshot)
            .map_err(|error| other_io_error(error.to_string()))?;
        durable_atomic_write(&path, &bytes).await
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
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
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
        self.upsert_index_from_session_inner(session, rel_path, false)
            .await
    }

    async fn repair_index_from_authoritative_session(
        &self,
        session: &Session,
        rel_path: String,
    ) -> io::Result<()> {
        self.upsert_index_from_session_inner(session, rel_path, true)
            .await
    }

    async fn upsert_index_from_session_inner(
        &self,
        session: &Session,
        rel_path: String,
        preserve_newer: bool,
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
        let permission_mode = session
            .agent_runtime_state
            .as_ref()
            .map(|state| state.effective_permission_mode())
            .unwrap_or_default();
        let bypass_permissions = permission_mode != bamboo_domain::SessionPermissionMode::Default;
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
        let session_id = session.id.clone();
        let entry = SessionIndexEntry {
            id: session.id.clone(),
            kind: session.kind,
            rel_path,
            title: session.title.clone(),
            title_version: session.title_version,
            title_generated: session.title_generated,
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
            permission_mode,
            last_run_status,
            last_run_error,
            token_usage: session.token_usage.clone(),
            subagent_type,
            lifecycle,
            resident_name,
            placement,
        };
        self.update_index(move |index: &mut SessionsIndex| {
            if preserve_newer {
                if let Some(existing) = index.sessions.get_mut(&session_id) {
                    if existing.updated_at > entry.updated_at {
                        // A live process may already have published fresher
                        // summary/control-plane fields than the authoritative
                        // transcript snapshot being probed. Preserve those
                        // fields while repairing canonical root identity/path.
                        existing.id = entry.id.clone();
                        existing.kind = entry.kind;
                        existing.rel_path = entry.rel_path.clone();
                        existing.parent_session_id = entry.parent_session_id.clone();
                        existing.root_session_id = entry.root_session_id.clone();
                        existing.spawn_depth = entry.spawn_depth;
                        return Ok(());
                    }
                }
            }
            index.sessions.insert(session_id, entry);
            Ok(())
        })
        .await
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
        let _lifecycle = self.lock_session_lifecycle_exclusive().await?;
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;

        // Remove the sessions directory entirely.
        let _ = fs::remove_dir_all(&self.sessions_dir).await;
        fs::create_dir_all(&self.sessions_dir).await?;

        // Reset through the same cross-process rebase/publish boundary as every
        // other index mutation; dev reset must not race a stale direct writer.
        self.update_index(|index| {
            *index = SessionsIndex::empty();
            Ok(())
        })
        .await
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
        let _lifecycle = self.lock_session_lifecycle_exclusive().await?;
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
        self.delete_session_recursive_locked(session_id, force)
            .await
    }

    async fn delete_session_recursive_locked(
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
/// torn), and a crash BETWEEN temp-create and rename leaks an orphan `*.tmp.*`
/// (disk litter, not corruption — no sweep yet). Windows replacement is a true
/// `MoveFileExW(REPLACE_EXISTING | WRITE_THROUGH)`, so it has no remove-first gap.
/// Task CAS/transaction paths use [`durable_atomic_write`] below, which also
/// synchronizes the published directory entry.
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
    #[cfg(not(windows))]
    {
        fs::rename(from, to).await
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
        };

        let source: Vec<u16> = from.as_os_str().encode_wide().chain(Some(0)).collect();
        let destination: Vec<u16> = to.as_os_str().encode_wide().chain(Some(0)).collect();
        let result = unsafe {
            MoveFileExW(
                source.as_ptr(),
                destination.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

/// Flush the directory entry containing `path` after a create/rename/remove.
/// Unix exposes directory handles through `std`; Windows does not, so Windows
/// transaction renames use `MoveFileExW(..., MOVEFILE_WRITE_THROUGH)` above as
/// the strongest portable completion boundary available here.
async fn sync_parent_directory_entry(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))?;
    #[cfg(unix)]
    {
        let parent = parent.to_path_buf();
        tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all())
            .await
            .map_err(|error| other_io_error(format!("join directory sync task: {error}")))?
    }
    #[cfg(windows)]
    {
        let _ = parent;
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = parent;
        Ok(())
    }
}

/// Transaction-only durable replacement. Unlike ordinary sidecar writes, the
/// temp contents and the published directory entry are both synchronized
/// before this returns. Windows uses a true replace-existing primitive, never
/// the target-loss-prone remove-then-rename fallback.
async fn durable_atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;

    let tmp = path.with_extension(format!("durable.tmp.{}", Uuid::new_v4()));
    let write_result = async {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp)
            .await?;
        file.write_all(bytes).await?;
        file.sync_all().await
    }
    .await;
    if let Err(error) = write_result {
        let _ = fs::remove_file(&tmp).await;
        return Err(error);
    }
    if let Err(error) = atomic_rename(&tmp, path).await {
        let _ = fs::remove_file(&tmp).await;
        return Err(error);
    }
    sync_parent_directory_entry(path).await
}

/// Durably remove an active recovery marker without relying on directory
/// unlink persistence. The marker is first renamed to an extension the scanner
/// never treats as active; a resurrected cleanup tombstone is therefore inert.
async fn durable_deactivate_recovery_marker(path: &Path) -> io::Result<()> {
    let tombstone = path.with_extension(format!("removed.{}", Uuid::new_v4()));
    atomic_rename(path, &tombstone).await?;
    sync_parent_directory_entry(&tombstone).await?;

    // Once the rename above is durable, cleanup cannot affect recovery
    // correctness. A power-loss-resurrected tombstone remains non-scannable.
    if fs::remove_file(&tombstone).await.is_ok() {
        let _ = sync_parent_directory_entry(&tombstone).await;
    }
    Ok(())
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
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
        self.reject_regressing_runtime_task(session).await?;
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
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
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
            // session.json and the index get created. Deliberately acquire no
            // shared Task guard before this call: `save_session` owns that
            // boundary, avoiding a same-instance shared-lock re-entry.
            return self.save_session(session).await;
        };
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
        self.reject_regressing_runtime_task(session).await?;
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
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
        self.load_runtime_control_plane_unchecked(session_id).await
    }

    async fn recover_task_control_plane_transaction(
        &self,
        first_session_id: &str,
        second_session_id: &str,
    ) -> io::Result<()> {
        let _guard = self.lock_runtime_task_transaction_exclusive().await?;
        self.recover_runtime_task_transaction_for_pair_locked(first_session_id, second_session_id)
            .await
    }

    async fn save_task_control_plane_if_matches(
        &self,
        original: &Session,
        updated: &Session,
    ) -> io::Result<bool> {
        let _guard = self.lock_runtime_task_transaction_exclusive().await?;
        self.recover_all_runtime_task_transactions_locked().await?;
        self.save_runtime_task_control_plane_if_matches(original, updated)
            .await
    }

    async fn save_task_control_planes_atomically(
        &self,
        first_original: &Session,
        first_updated: &Session,
        second_original: &Session,
        second_updated: &Session,
    ) -> io::Result<bool> {
        let _guard = self.lock_runtime_task_transaction_exclusive().await?;
        self.recover_runtime_task_transaction_for_pair_locked(
            &first_original.id,
            &second_original.id,
        )
        .await?;
        self.save_runtime_task_pair_transaction(
            first_original,
            first_updated,
            second_original,
            second_updated,
        )
        .await
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
    use bamboo_domain::{
        Message, SessionInboxError, SessionInboxPort, SessionMessageEnvelope, TaskItem,
        TaskItemStatus,
    };
    use std::io;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn create_temp_storage() -> io::Result<(SessionStoreV2, TempDir)> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();
        let storage = SessionStoreV2::new(bamboo_home).await?;
        Ok((storage, temp_dir))
    }

    fn transaction_task_list(root_id: &str, title: &str) -> TaskList {
        let now = Utc::now();
        TaskList {
            session_id: root_id.to_string(),
            title: title.to_string(),
            items: vec![TaskItem {
                id: "task-1".to_string(),
                description: format!("{title} work"),
                status: TaskItemStatus::InProgress,
                ..TaskItem::default()
            }],
            created_at: now,
            updated_at: now,
        }
    }

    async fn seed_runtime_task_transaction_pair(
        storage: &SessionStoreV2,
    ) -> io::Result<(Session, Session, Session, Session)> {
        let root_id = "tx-root";
        let child_id = "tx-child";

        let mut root = Session::new(root_id, "model");
        root.add_message(Message::user("root transcript secret"));
        root.metadata.insert(
            "unrelated.root".to_string(),
            "root metadata secret".to_string(),
        );
        root.set_task_list(transaction_task_list(root_id, "original root"));
        root.set_task_list_version_meta("1");
        storage.save_session(&root).await?;

        let mut child = Session::new_child(child_id, root_id, "model", "child");
        child.add_message(Message::user("child transcript secret"));
        child.metadata.insert(
            "unrelated.child".to_string(),
            "child metadata secret".to_string(),
        );
        child.set_task_list(transaction_task_list(root_id, "original child"));
        child.set_task_list_version_meta("1");
        storage.save_session(&child).await?;

        let child_original = storage
            .load_runtime_control_plane(child_id)
            .await?
            .expect("child control plane");
        let root_original = storage
            .load_runtime_control_plane(root_id)
            .await?
            .expect("root control plane");
        let evaluated = transaction_task_list(root_id, "evaluated");
        let mut child_updated = child_original.clone();
        child_updated.task_list = Some(evaluated.clone());
        child_updated.set_task_list_version_meta("2");
        let mut root_updated = root_original.clone();
        root_updated.task_list = Some(evaluated);
        root_updated.set_task_list_version_meta("2");

        // child < root lexically, matching the Storage transaction contract.
        Ok((child_original, child_updated, root_original, root_updated))
    }

    async fn assert_original_runtime_task_pair(storage: &SessionStoreV2) -> io::Result<()> {
        let child = storage
            .load_session("tx-child")
            .await?
            .expect("child remains");
        let root = storage
            .load_session("tx-root")
            .await?
            .expect("root remains");
        for (session, title, transcript, metadata_key, metadata_value) in [
            (
                &child,
                "original child",
                "child transcript secret",
                "unrelated.child",
                "child metadata secret",
            ),
            (
                &root,
                "original root",
                "root transcript secret",
                "unrelated.root",
                "root metadata secret",
            ),
        ] {
            assert_eq!(session.task_list_version_meta().as_deref(), Some("1"));
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some(title)
            );
            assert_eq!(session.messages.len(), 1);
            assert_eq!(session.messages[0].content, transcript);
            assert_eq!(
                session.metadata.get(metadata_key).map(String::as_str),
                Some(metadata_value)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn paired_task_success_records_durable_publish_order() -> io::Result<()> {
        let (storage, _temp) = create_temp_storage().await?;
        let (child_original, child_updated, root_original, root_updated) =
            seed_runtime_task_transaction_pair(&storage).await?;
        assert!(storage.take_runtime_task_durability_events().is_empty());

        assert!(
            storage
                .save_task_control_planes_atomically(
                    &child_original,
                    &child_updated,
                    &root_original,
                    &root_updated,
                )
                .await?
        );
        assert_eq!(
            storage.take_runtime_task_durability_events(),
            vec![
                RuntimeTaskDurabilityEvent::JournalPublished,
                RuntimeTaskDurabilityEvent::FirstUpdatedSidecarPublished,
                RuntimeTaskDurabilityEvent::SecondUpdatedSidecarPublished,
                RuntimeTaskDurabilityEvent::JournalDeactivated,
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn paired_task_second_write_failure_rolls_back_both_originals() -> io::Result<()> {
        let (storage, _temp) = create_temp_storage().await?;
        let (child_original, child_updated, root_original, root_updated) =
            seed_runtime_task_transaction_pair(&storage).await?;
        assert!(storage.take_runtime_task_durability_events().is_empty());
        storage
            .inject_runtime_task_transaction_fault(RuntimeTaskTransactionFault::SecondUpdatedWrite);

        let error = storage
            .save_task_control_planes_atomically(
                &child_original,
                &child_updated,
                &root_original,
                &root_updated,
            )
            .await
            .expect_err("second write must fail");
        assert!(error.to_string().contains("rolled back"), "{error}");
        assert_original_runtime_task_pair(&storage).await?;
        assert!(storage.runtime_task_journal_paths().await?.is_empty());
        assert_eq!(
            storage.take_runtime_task_durability_events(),
            vec![
                RuntimeTaskDurabilityEvent::JournalPublished,
                RuntimeTaskDurabilityEvent::FirstUpdatedSidecarPublished,
                RuntimeTaskDurabilityEvent::FirstRollbackSidecarPublished,
                RuntimeTaskDurabilityEvent::SecondRollbackSidecarPublished,
                RuntimeTaskDurabilityEvent::JournalDeactivated,
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn paired_task_commit_marker_removal_failure_rolls_back_before_error() -> io::Result<()> {
        let (storage, _temp) = create_temp_storage().await?;
        let (child_original, child_updated, root_original, root_updated) =
            seed_runtime_task_transaction_pair(&storage).await?;
        storage.inject_runtime_task_transaction_fault(RuntimeTaskTransactionFault::JournalRemove);

        let error = storage
            .save_task_control_planes_atomically(
                &child_original,
                &child_updated,
                &root_original,
                &root_updated,
            )
            .await
            .expect_err("journal removal failure cannot publish success");
        assert!(error.to_string().contains("rolled back"), "{error}");
        assert_original_runtime_task_pair(&storage).await?;
        assert!(storage.runtime_task_journal_paths().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn paired_task_rollback_failure_retains_journal_and_fails_closed_until_recovery(
    ) -> io::Result<()> {
        let (storage, _temp) = create_temp_storage().await?;
        let (child_original, child_updated, root_original, root_updated) =
            seed_runtime_task_transaction_pair(&storage).await?;
        storage
            .inject_runtime_task_transaction_fault(RuntimeTaskTransactionFault::SecondUpdatedWrite);
        storage
            .inject_runtime_task_transaction_fault(RuntimeTaskTransactionFault::FirstRollbackWrite);

        let error = storage
            .save_task_control_planes_atomically(
                &child_original,
                &child_updated,
                &root_original,
                &root_updated,
            )
            .await
            .expect_err("rollback failure must surface");
        assert!(error.to_string().contains("recovery required"), "{error}");
        assert_eq!(storage.runtime_task_journal_paths().await?.len(), 1);
        assert!(
            storage.load_session("tx-child").await.is_err(),
            "ordinary access must fail closed while an undo journal remains"
        );

        storage
            .recover_task_control_plane_transaction("tx-child", "tx-root")
            .await?;
        assert_original_runtime_task_pair(&storage).await?;
        assert!(storage.runtime_task_journal_paths().await?.is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn independent_store_sidecar_access_and_constructor_wait_for_live_pair_commit(
    ) -> io::Result<()> {
        let temp = TempDir::new().map_err(io::Error::other)?;
        let home = temp.path().to_path_buf();
        let first_store = Arc::new(SessionStoreV2::new(home.clone()).await?);
        let (child_original, child_updated, root_original, root_updated) =
            seed_runtime_task_transaction_pair(&first_store).await?;
        // This store starts with its own recovery flag clear. Correctness must
        // therefore come from the shared file lock + durable journal check.
        let second_store = Arc::new(SessionStoreV2::new(home.clone()).await?);

        let (first_write_reached, release_first_write) =
            first_store.pause_runtime_task_transaction_after_first_write();
        let commit_store = first_store.clone();
        let commit_child_original = child_original.clone();
        let commit_child_updated = child_updated.clone();
        let commit_root_original = root_original.clone();
        let commit_root_updated = root_updated.clone();
        let commit = tokio::spawn(async move {
            commit_store
                .save_task_control_planes_atomically(
                    &commit_child_original,
                    &commit_child_updated,
                    &commit_root_original,
                    &commit_root_updated,
                )
                .await
        });
        first_write_reached.wait().await;
        assert_eq!(first_store.runtime_task_journal_paths().await?.len(), 1);

        let load_store = second_store.clone();
        let mut full_load = tokio::spawn(async move { load_store.load_session("tx-child").await });
        let control_store = second_store.clone();
        let mut control_load =
            tokio::spawn(async move { control_store.load_runtime_control_plane("tx-root").await });
        let save_store = second_store.clone();
        // Queue an actually stale v1 writer. Once the shared lock is granted it
        // must fail before writing rather than silently substitute the durable
        // v2 Task fields while reporting the caller's v1 snapshot as saved.
        let mut save_snapshot = child_original.clone();
        save_snapshot
            .metadata
            .insert("independent.writer".to_string(), "preserved".to_string());
        let mut runtime_save =
            tokio::spawn(async move { save_store.save_runtime_state(&save_snapshot).await });
        let full_save_store = second_store.clone();
        let mut full_save_snapshot = root_original.clone();
        full_save_snapshot.metadata.insert(
            "independent.full-writer".to_string(),
            "preserved".to_string(),
        );
        let mut full_save =
            tokio::spawn(async move { full_save_store.save_session(&full_save_snapshot).await });
        let constructor_home = home.clone();
        let mut constructor =
            tokio::spawn(async move { SessionStoreV2::new(constructor_home).await });

        let blocked_for = std::time::Duration::from_millis(100);
        assert!(
            tokio::time::timeout(blocked_for, &mut full_load)
                .await
                .is_err(),
            "full load must not observe the first published sidecar"
        );
        assert!(
            tokio::time::timeout(blocked_for, &mut control_load)
                .await
                .is_err(),
            "control-plane load must not observe the first published sidecar"
        );
        assert!(
            tokio::time::timeout(blocked_for, &mut runtime_save)
                .await
                .is_err(),
            "runtime save must not overwrite a half-published transaction"
        );
        assert!(
            tokio::time::timeout(blocked_for, &mut full_save)
                .await
                .is_err(),
            "full save must not overwrite a half-published transaction"
        );
        assert!(
            tokio::time::timeout(blocked_for, &mut constructor)
                .await
                .is_err(),
            "a second constructor must not recover another store's live journal"
        );

        release_first_write.wait().await;
        assert!(commit.await.map_err(io::Error::other)??);

        let full = full_load
            .await
            .map_err(io::Error::other)??
            .expect("child remains");
        let control = control_load
            .await
            .map_err(io::Error::other)??
            .expect("root remains");
        let runtime_error = runtime_save
            .await
            .map_err(io::Error::other)?
            .expect_err("stale runtime save must report a Task conflict");
        assert_eq!(runtime_error.kind(), io::ErrorKind::WouldBlock);
        let full_save_error = full_save
            .await
            .map_err(io::Error::other)?
            .expect_err("stale full save must report a Task conflict");
        assert_eq!(full_save_error.kind(), io::ErrorKind::WouldBlock);
        let constructed = constructor.await.map_err(io::Error::other)??;
        for session in [&full, &control] {
            assert_eq!(session.task_list_version_meta().as_deref(), Some("2"));
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some("evaluated")
            );
        }
        for storage in [second_store.as_ref(), &constructed] {
            let child = storage.load_session("tx-child").await?.expect("child");
            let root = storage.load_session("tx-root").await?.expect("root");
            for session in [&child, &root] {
                assert_eq!(session.task_list_version_meta().as_deref(), Some("2"));
                assert_eq!(
                    session.task_list.as_ref().map(|list| list.title.as_str()),
                    Some("evaluated")
                );
            }
            assert!(!child.metadata.contains_key("independent.writer"));
            assert!(!root.metadata.contains_key("independent.full-writer"));
        }
        assert!(first_store.runtime_task_journal_paths().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn paired_task_commit_rebases_on_current_non_task_control_plane() -> io::Result<()> {
        let temp = TempDir::new().map_err(io::Error::other)?;
        let home = temp.path().to_path_buf();
        let first_store = SessionStoreV2::new(home.clone()).await?;
        let (child_original, child_updated, root_original, root_updated) =
            seed_runtime_task_transaction_pair(&first_store).await?;
        let second_store = SessionStoreV2::new(home).await?;

        let mut current_child = second_store
            .load_runtime_control_plane("tx-child")
            .await?
            .expect("child");
        current_child.metadata.insert(
            "concurrent.child.status".to_string(),
            "must survive".to_string(),
        );
        second_store.save_runtime_state(&current_child).await?;
        let mut current_root = second_store
            .load_runtime_control_plane("tx-root")
            .await?
            .expect("root");
        current_root.metadata.insert(
            "concurrent.root.round".to_string(),
            "must survive".to_string(),
        );
        second_store.save_runtime_state(&current_root).await?;

        assert!(
            first_store
                .save_task_control_planes_atomically(
                    &child_original,
                    &child_updated,
                    &root_original,
                    &root_updated,
                )
                .await?,
            "Task-owned originals still match, so the narrow CAS should commit"
        );
        let child = first_store.load_session("tx-child").await?.expect("child");
        let root = first_store.load_session("tx-root").await?.expect("root");
        for session in [&child, &root] {
            assert_eq!(session.task_list_version_meta().as_deref(), Some("2"));
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some("evaluated")
            );
        }
        assert_eq!(
            child
                .metadata
                .get("concurrent.child.status")
                .map(String::as_str),
            Some("must survive")
        );
        assert_eq!(
            root.metadata
                .get("concurrent.root.round")
                .map(String::as_str),
            Some("must survive")
        );
        Ok(())
    }

    #[tokio::test]
    async fn paired_task_cas_accepts_legacy_only_generation_metadata() -> io::Result<()> {
        let (storage, _temp) = create_temp_storage().await?;
        let root_id = "legacy-task-root";
        let child_id = "legacy-task-child";
        let generation_key =
            bamboo_domain::session::runtime_metadata::keys::TASK_LIST_VERSION.to_string();
        let mut root = Session::new(root_id, "model");
        root.task_list = Some(transaction_task_list(root_id, "legacy root"));
        root.metadata
            .insert(generation_key.clone(), "1".to_string());
        assert!(root.runtime_metadata.is_none());
        storage.save_session(&root).await?;
        let mut child = Session::new_child(child_id, root_id, "model", "child");
        child.task_list = Some(transaction_task_list(root_id, "legacy child"));
        child.metadata.insert(generation_key, "1".to_string());
        assert!(child.runtime_metadata.is_none());
        storage.save_session(&child).await?;

        let child_original = storage
            .load_runtime_control_plane(child_id)
            .await?
            .expect("child");
        let root_original = storage
            .load_runtime_control_plane(root_id)
            .await?
            .expect("root");
        assert!(child_original.runtime_metadata.is_none());
        assert!(root_original.runtime_metadata.is_none());
        let evaluated = transaction_task_list(root_id, "legacy evaluated");
        let mut child_updated = child_original.clone();
        child_updated.task_list = Some(evaluated.clone());
        child_updated.set_task_list_version_meta("2");
        let mut root_updated = root_original.clone();
        root_updated.task_list = Some(evaluated);
        root_updated.set_task_list_version_meta("2");

        assert!(
            storage
                .save_task_control_planes_atomically(
                    &child_original,
                    &child_updated,
                    &root_original,
                    &root_updated,
                )
                .await?,
            "typed setter normalization must not look like a non-Task mutation"
        );
        for id in [child_id, root_id] {
            let session = storage.load_session(id).await?.expect("session");
            assert_eq!(session.task_list_version_meta().as_deref(), Some("2"));
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some("legacy evaluated")
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn ordinary_task_save_accepts_newer_generation_and_rejects_same_generation_divergence(
    ) -> io::Result<()> {
        let (storage, _temp) = create_temp_storage().await?;
        let mut initial = Session::new("ordinary-task-rebase", "model");
        initial.set_task_list(transaction_task_list(&initial.id, "generation one"));
        initial.set_task_list_version_meta("1");
        storage.save_session(&initial).await?;

        let mut newer = initial.clone();
        newer.set_task_list(transaction_task_list(&newer.id, "generation two"));
        newer.set_task_list_version_meta("2");
        storage.save_runtime_state(&newer).await?;
        let durable_newer = storage
            .load_session(&newer.id)
            .await?
            .expect("newer session");
        assert_eq!(
            durable_newer
                .task_list
                .as_ref()
                .map(|list| list.title.as_str()),
            Some("generation two"),
            "a legitimate monotonic Taskwrite must not be rebased away"
        );

        let mut divergent = durable_newer.clone();
        divergent.set_task_list(transaction_task_list(
            &divergent.id,
            "same generation divergent",
        ));
        divergent.set_task_list_version_meta("2");
        divergent
            .metadata
            .insert("ordinary.non-task".to_string(), "must persist".to_string());
        let conflict = storage
            .save_runtime_state(&divergent)
            .await
            .expect_err("same-generation divergence must fail before writing");
        assert_eq!(conflict.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(
            divergent.task_list.as_ref().map(|list| list.title.as_str()),
            Some("same generation divergent"),
            "save must not mutate the caller-owned snapshot"
        );
        let durable = storage
            .load_session(&divergent.id)
            .await?
            .expect("durable session");
        assert_eq!(durable.task_list_version_meta().as_deref(), Some("2"));
        assert_eq!(
            durable.task_list.as_ref().map(|list| list.title.as_str()),
            Some("generation two"),
            "same-generation divergent content must preserve durable truth"
        );
        assert!(!durable.metadata.contains_key("ordinary.non-task"));
        Ok(())
    }

    async fn assert_corrupt_index_orphan_task_journal_recovers(
        marker_state: RuntimeTaskJournalMarkerState,
    ) -> io::Result<()> {
        assert!(matches!(
            marker_state,
            RuntimeTaskJournalMarkerState::Prepared | RuntimeTaskJournalMarkerState::Committing
        ));
        let (storage, temp) = create_temp_storage().await?;
        let (child_original, child_updated, root_original, _root_updated) =
            seed_runtime_task_transaction_pair(&storage).await?;
        let transaction_id = Uuid::new_v4().to_string();
        let journal = RuntimeTaskTransactionJournal {
            version: RUNTIME_TASK_TRANSACTION_VERSION,
            transaction_id,
            first: TaskControlPlaneUndo {
                session_id: child_original.id.clone(),
                task_list: child_original.task_list.clone(),
                task_list_version: child_original
                    .task_list_version_meta()
                    .expect("child generation"),
            },
            second: TaskControlPlaneUndo {
                session_id: root_original.id.clone(),
                task_list: root_original.task_list.clone(),
                task_list_version: root_original
                    .task_list_version_meta()
                    .expect("root generation"),
            },
        };
        let mut journal_path = storage.write_runtime_task_journal(&journal).await?;
        let journal_json = fs::read_to_string(&journal_path).await?;
        assert!(!journal_json.contains("transcript secret"));
        assert!(!journal_json.contains("metadata secret"));
        assert!(!journal_json.contains("unrelated.child"));
        assert!(!journal_json.contains("unrelated.root"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(storage.runtime_task_transaction_dir())
                .await?
                .permissions()
                .mode();
            assert_eq!(mode & 0o077, 0, "journal directory must be private");
        }

        // Simulate process death after the first rename: leave the durable undo
        // journal and only the lexically first target at generation 2.
        storage
            .write_existing_runtime_sidecar_durable_unchecked(
                &child_updated,
                RuntimeTaskDurabilityEvent::FirstUpdatedSidecarPublished,
            )
            .await?;
        if marker_state == RuntimeTaskJournalMarkerState::Committing {
            let committing = journal_path.with_extension("committing");
            atomic_rename(&journal_path, &committing).await?;
            sync_parent_directory_entry(&committing).await?;
            journal_path = committing;
        }
        assert!(journal_path.exists());
        let child_sidecar = storage
            .runtime_json_path("tx-child")
            .await?
            .expect("child runtime sidecar path");
        let root_sidecar = storage
            .runtime_json_path("tx-root")
            .await?
            .expect("root runtime sidecar path");
        assert!(child_sidecar.exists());
        assert!(root_sidecar.exists());

        // Exercise the constructor ordering hazard: corrupt-index handling
        // publishes an empty rebuild marker before orphan Task recovery. Undo
        // must therefore locate these intact authoritative session directories
        // without consulting that temporarily empty index.
        fs::write(storage.index_path(), b"{ corrupt sessions index").await?;
        drop(storage);

        let reopened = SessionStoreV2::new(temp.path().to_path_buf()).await?;
        assert_eq!(
            reopened.take_runtime_task_durability_events(),
            vec![
                RuntimeTaskDurabilityEvent::FirstRollbackSidecarPublished,
                RuntimeTaskDurabilityEvent::SecondRollbackSidecarPublished,
                RuntimeTaskDurabilityEvent::JournalDeactivated,
            ]
        );
        assert_original_runtime_task_pair(&reopened).await?;
        assert!(reopened.runtime_task_journal_paths().await?.is_empty());
        assert!(temp.path().join("sessions.json.bak").exists());
        let rebuilt_raw = fs::read_to_string(reopened.index_path()).await?;
        let rebuilt: SessionsIndex = serde_json::from_str(&rebuilt_raw)
            .map_err(|error| other_io_error(format!("parse rebuilt sessions index: {error}")))?;
        assert_eq!(rebuilt.version, SESSIONS_INDEX_VERSION);
        assert!(!rebuilt.rebuild_in_progress);
        assert_eq!(rebuilt.sessions.len(), 2);
        assert_eq!(
            rebuilt
                .sessions
                .get("tx-root")
                .map(|entry| entry.rel_path.as_str()),
            Some("sessions/tx-root")
        );
        assert_eq!(
            rebuilt
                .sessions
                .get("tx-child")
                .map(|entry| entry.rel_path.as_str()),
            Some("sessions/tx-root/children/tx-child")
        );
        Ok(())
    }

    #[tokio::test]
    async fn reopening_store_recovers_orphan_task_journal_without_transcript_or_metadata_copy(
    ) -> io::Result<()> {
        for marker_state in [
            RuntimeTaskJournalMarkerState::Prepared,
            RuntimeTaskJournalMarkerState::Committing,
        ] {
            assert_corrupt_index_orphan_task_journal_recovers(marker_state).await?;
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rebuild_keeps_retry_marker_when_pair_transaction_crashes_mid_scan() -> io::Result<()> {
        let temp = TempDir::new().map_err(io::Error::other)?;
        let home = temp.path().to_path_buf();
        let rebuild_store = Arc::new(SessionStoreV2::new(home.clone()).await?);
        let (child_original, child_updated, root_original, root_updated) =
            seed_runtime_task_transaction_pair(&rebuild_store).await?;
        // Independent in-memory locks model the second Bamboo process that can
        // start a Task pair commit after constructor recovery releases its
        // exclusive cross-process guard.
        let commit_store = Arc::new(SessionStoreV2::new(home.clone()).await?);

        rebuild_store
            .update_index(|index| {
                *index = SessionsIndex::empty();
                index.version = 0;
                index.rebuild_in_progress = true;
                Ok(())
            })
            .await?;

        // Freeze the first per-entry lifecycle probe. Pair commits do not own
        // lifecycle, so the independent store can publish its journal and
        // first sidecar while rebuild is demonstrably mid-scan.
        let lifecycle = rebuild_store.lock_session_lifecycle_exclusive().await?;
        let rebuilding = Arc::clone(&rebuild_store);
        let rebuild = tokio::spawn(async move { rebuilding.rebuild_index_from_disk().await });
        tokio::task::yield_now().await;

        let (first_write_reached, _release_first_write) =
            commit_store.pause_runtime_task_transaction_after_first_write();
        let committing = Arc::clone(&commit_store);
        let commit = tokio::spawn(async move {
            committing
                .save_task_control_planes_atomically(
                    &child_original,
                    &child_updated,
                    &root_original,
                    &root_updated,
                )
                .await
        });
        first_write_reached.wait().await;
        assert_eq!(commit_store.runtime_task_journal_paths().await?.len(), 1);
        assert_eq!(
            commit_store.take_runtime_task_durability_events(),
            vec![
                RuntimeTaskDurabilityEvent::JournalPublished,
                RuntimeTaskDurabilityEvent::FirstUpdatedSidecarPublished,
            ]
        );

        // Cancellation models process death: the exclusive file guard drops,
        // while the prepared undo journal and first durable sidecar remain.
        commit.abort();
        assert!(
            commit
                .await
                .expect_err("commit task must be cancelled")
                .is_cancelled(),
            "cancelled commit must release the cross-process Task guard"
        );
        drop(lifecycle);

        let rebuild_error = rebuild
            .await
            .map_err(io::Error::other)?
            .expect_err("a pending journal must prevent rebuild finalization");
        assert!(
            rebuild_error.to_string().contains("recovery is required"),
            "{rebuild_error}"
        );
        let retryable: SessionsIndex =
            serde_json::from_slice(&fs::read(&home.join("sessions.json")).await?)
                .map_err(io::Error::other)?;
        assert_eq!(retryable.version, 0);
        assert!(retryable.rebuild_in_progress);
        assert!(retryable.sessions.is_empty());
        assert_eq!(commit_store.runtime_task_journal_paths().await?.len(), 1);

        drop(commit_store);
        drop(rebuild_store);
        let reopened = SessionStoreV2::new(home).await?;
        assert_eq!(
            reopened.take_runtime_task_durability_events(),
            vec![
                RuntimeTaskDurabilityEvent::FirstRollbackSidecarPublished,
                RuntimeTaskDurabilityEvent::SecondRollbackSidecarPublished,
                RuntimeTaskDurabilityEvent::JournalDeactivated,
            ]
        );
        assert_original_runtime_task_pair(&reopened).await?;
        assert!(reopened.runtime_task_journal_paths().await?.is_empty());
        let rebuilt: SessionsIndex =
            serde_json::from_slice(&fs::read(reopened.index_path()).await?)
                .map_err(io::Error::other)?;
        assert_eq!(rebuilt.version, SESSIONS_INDEX_VERSION);
        assert!(!rebuilt.rebuild_in_progress);
        assert_eq!(rebuilt.sessions.len(), 2);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn orphan_recovery_rejects_symlinked_child_without_writing_outside_sessions(
    ) -> io::Result<()> {
        use std::os::unix::fs::symlink;

        let (storage, temp) = create_temp_storage().await?;
        let (child_original, child_updated, root_original, _root_updated) =
            seed_runtime_task_transaction_pair(&storage).await?;
        let journal = RuntimeTaskTransactionJournal {
            version: RUNTIME_TASK_TRANSACTION_VERSION,
            transaction_id: Uuid::new_v4().to_string(),
            first: TaskControlPlaneUndo {
                session_id: child_original.id.clone(),
                task_list: child_original.task_list.clone(),
                task_list_version: child_original
                    .task_list_version_meta()
                    .expect("child generation"),
            },
            second: TaskControlPlaneUndo {
                session_id: root_original.id.clone(),
                task_list: root_original.task_list.clone(),
                task_list_version: root_original
                    .task_list_version_meta()
                    .expect("root generation"),
            },
        };
        let journal_path = storage.write_runtime_task_journal(&journal).await?;
        storage
            .write_existing_runtime_sidecar_durable_unchecked(
                &child_updated,
                RuntimeTaskDurabilityEvent::FirstUpdatedSidecarPublished,
            )
            .await?;

        let child_dir = temp.path().join("sessions/tx-root/children/tx-child");
        let outside_dir = temp.path().join("outside-session-tree");
        fs::rename(&child_dir, &outside_dir).await?;
        symlink(&outside_dir, &child_dir).map_err(io::Error::other)?;
        let outside_runtime = outside_dir.join(RUNTIME_SIDECAR_FILE);
        let outside_before = fs::read(&outside_runtime).await?;
        fs::write(storage.index_path(), b"{ corrupt sessions index").await?;
        drop(storage);

        let error = SessionStoreV2::new(temp.path().to_path_buf())
            .await
            .expect_err("symlinked journal target must fail closed");
        assert!(
            error.to_string().contains("not a real directory"),
            "{error}"
        );
        assert_eq!(
            fs::read(&outside_runtime).await?,
            outside_before,
            "recovery must not rewrite runtime.json outside sessions/"
        );
        assert!(fs::symlink_metadata(&child_dir)
            .await?
            .file_type()
            .is_symlink());
        assert!(journal_path.exists(), "failed recovery must retain undo");
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn orphan_recovery_rejects_symlinked_sessions_root_without_writing_outside_home(
    ) -> io::Result<()> {
        use std::os::unix::fs::symlink;

        let (storage, temp) = create_temp_storage().await?;
        let (child_original, child_updated, root_original, _root_updated) =
            seed_runtime_task_transaction_pair(&storage).await?;
        let journal = RuntimeTaskTransactionJournal {
            version: RUNTIME_TASK_TRANSACTION_VERSION,
            transaction_id: Uuid::new_v4().to_string(),
            first: TaskControlPlaneUndo {
                session_id: child_original.id.clone(),
                task_list: child_original.task_list.clone(),
                task_list_version: child_original
                    .task_list_version_meta()
                    .expect("child generation"),
            },
            second: TaskControlPlaneUndo {
                session_id: root_original.id.clone(),
                task_list: root_original.task_list.clone(),
                task_list_version: root_original
                    .task_list_version_meta()
                    .expect("root generation"),
            },
        };
        let journal_path = storage.write_runtime_task_journal(&journal).await?;
        storage
            .write_existing_runtime_sidecar_durable_unchecked(
                &child_updated,
                RuntimeTaskDurabilityEvent::FirstUpdatedSidecarPublished,
            )
            .await?;

        let sessions_dir = temp.path().join("sessions");
        let outside_home = TempDir::new().map_err(io::Error::other)?;
        let outside_dir = outside_home.path().join("sessions");
        fs::rename(&sessions_dir, &outside_dir).await?;
        symlink(&outside_dir, &sessions_dir).map_err(io::Error::other)?;
        let outside_runtime = outside_dir.join("tx-root/children/tx-child/runtime.json");
        let outside_before = fs::read(&outside_runtime).await?;
        fs::write(storage.index_path(), b"{ corrupt sessions index").await?;
        drop(storage);

        let error = SessionStoreV2::new(temp.path().to_path_buf())
            .await
            .expect_err("symlinked sessions root must fail closed");
        assert!(
            error.to_string().contains("not a real directory"),
            "{error}"
        );
        assert_eq!(
            fs::read(&outside_runtime).await?,
            outside_before,
            "recovery must not rewrite runtime.json outside Bamboo home"
        );
        assert!(fs::symlink_metadata(&sessions_dir)
            .await?
            .file_type()
            .is_symlink());
        assert!(journal_path.exists(), "failed recovery must retain undo");
        Ok(())
    }

    #[tokio::test]
    async fn reopening_store_keeps_durable_pair_when_committed_marker_survives() -> io::Result<()> {
        let (storage, temp) = create_temp_storage().await?;
        let (child_original, child_updated, root_original, root_updated) =
            seed_runtime_task_transaction_pair(&storage).await?;
        let journal = RuntimeTaskTransactionJournal {
            version: RUNTIME_TASK_TRANSACTION_VERSION,
            transaction_id: Uuid::new_v4().to_string(),
            first: TaskControlPlaneUndo {
                session_id: child_original.id.clone(),
                task_list: child_original.task_list.clone(),
                task_list_version: child_original
                    .task_list_version_meta()
                    .expect("child generation"),
            },
            second: TaskControlPlaneUndo {
                session_id: root_original.id.clone(),
                task_list: root_original.task_list.clone(),
                task_list_version: root_original
                    .task_list_version_meta()
                    .expect("root generation"),
            },
        };
        let prepared = storage.write_runtime_task_journal(&journal).await?;
        storage
            .write_existing_runtime_sidecar_durable_unchecked(
                &child_updated,
                RuntimeTaskDurabilityEvent::FirstUpdatedSidecarPublished,
            )
            .await?;
        storage
            .write_existing_runtime_sidecar_durable_unchecked(
                &root_updated,
                RuntimeTaskDurabilityEvent::SecondUpdatedSidecarPublished,
            )
            .await?;
        let committing = prepared.with_extension("committing");
        atomic_rename(&prepared, &committing).await?;
        sync_parent_directory_entry(&committing).await?;
        let committed = prepared.with_extension("committed");
        atomic_rename(&committing, &committed).await?;
        sync_parent_directory_entry(&committed).await?;
        drop(storage);

        let reopened = SessionStoreV2::new(temp.path().to_path_buf()).await?;
        assert_eq!(
            reopened.take_runtime_task_durability_events(),
            vec![RuntimeTaskDurabilityEvent::JournalDeactivated],
            "committed recovery must clean the marker without publishing either undo"
        );
        assert!(reopened.runtime_task_journal_paths().await?.is_empty());
        let child = reopened
            .load_session("tx-child")
            .await?
            .expect("child remains");
        let root = reopened
            .load_session("tx-root")
            .await?
            .expect("root remains");
        for (session, transcript, metadata_key, metadata_value) in [
            (
                &child,
                "child transcript secret",
                "unrelated.child",
                "child metadata secret",
            ),
            (
                &root,
                "root transcript secret",
                "unrelated.root",
                "root metadata secret",
            ),
        ] {
            assert_eq!(session.task_list_version_meta().as_deref(), Some("2"));
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some("evaluated")
            );
            assert_eq!(session.messages[0].content, transcript);
            assert_eq!(
                session.metadata.get(metadata_key).map(String::as_str),
                Some(metadata_value)
            );
        }
        Ok(())
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
    async fn first_save_recreates_removed_home_before_opening_task_lock() -> io::Result<()> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().join("removed-bamboo-home");
        let storage = SessionStoreV2::new(bamboo_home.clone()).await?;
        fs::remove_dir_all(&bamboo_home).await?;

        let session = Session::new("first-save-after-home-removal", "model");
        storage.save_session(&session).await?;

        assert!(
            bamboo_home
                .join(RUNTIME_TASK_TRANSACTION_LOCK_FILE)
                .exists(),
            "the Task transaction lock parent must be recreated on first use"
        );
        assert!(storage.load_session(&session.id).await?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn clean_sidecar_reads_do_not_create_or_rechmod_journal_directory() -> io::Result<()> {
        let (storage, _temp) = create_temp_storage().await?;
        let session = Session::new("clean-journal-probe", "model");
        storage.save_session(&session).await?;
        let journal_dir = storage.runtime_task_transaction_dir();
        fs::remove_dir(&journal_dir).await?;

        assert!(storage.load_session(&session.id).await?.is_some());
        assert!(
            !journal_dir.exists(),
            "ordinary clean reads must treat a missing journal directory as empty"
        );

        fs::create_dir(&journal_dir).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&journal_dir, std::fs::Permissions::from_mode(0o750)).await?;
            assert!(storage
                .load_runtime_control_plane(&session.id)
                .await?
                .is_some());
            let mode = fs::metadata(&journal_dir).await?.permissions().mode();
            assert_eq!(
                mode & 0o777,
                0o750,
                "ordinary clean reads must not repeat constructor/write chmod"
            );
        }
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

    #[tokio::test]
    async fn title_lifecycle_is_indexed_and_legacy_rows_fail_safe() -> io::Result<()> {
        let (storage, _dir) = create_temp_storage().await?;
        let session = Session::new("pending-title", "m");
        storage.save_session(&session).await?;

        let entry = storage
            .get_index_entry(&session.id)
            .await
            .expect("saved session is indexed");
        assert!(!entry.title_generated);

        let mut legacy = serde_json::to_value(&entry).unwrap();
        legacy.as_object_mut().unwrap().remove("title_generated");
        let decoded: SessionIndexEntry = serde_json::from_value(legacy).unwrap();
        assert!(decoded.title_generated);

        Ok(())
    }

    // ── Runtime sidecar (③) ───────────────────────────────────────────────

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
    async fn model_context_ledger_round_trips_through_session_persistence() -> io::Result<()> {
        use bamboo_domain::{
            deterministic_model_context_event_id, model_context_block_sha256,
            render_model_context_snapshot, ContextBlock, ContextBlockBaseline,
            ContextBlockPriority, ContextBlockStability, ContextBlockType, ModelContextEvent,
            ModelContextEventKind, ModelContextState,
        };

        let (storage, _t) = create_temp_storage().await?;
        let mut session = session_with_history("ledger-persistence", 2, "run-ledger");
        let block = ContextBlock::new(
            ContextBlockType::TaskSnapshot,
            ContextBlockPriority::High,
            ContextBlockStability::RoundDynamic,
            "Task",
            "durable context bytes",
        );
        let digest = model_context_block_sha256(&block);
        let id = deterministic_model_context_event_id(
            &session.id,
            3,
            ContextBlockType::TaskSnapshot,
            1,
            &digest,
        );
        let rendered_text = render_model_context_snapshot(&id, 3, 0, &block, 1, None);
        let state = ModelContextState {
            prefix_epoch: 3,
            next_sequence: 1,
            baselines: std::collections::BTreeMap::from([(
                ContextBlockType::TaskSnapshot,
                ContextBlockBaseline {
                    revision: 1,
                    content_sha256: digest.clone(),
                },
            )]),
            events: vec![ModelContextEvent {
                id,
                epoch: 3,
                sequence: 0,
                anchor_message_id: None,
                block_type: ContextBlockType::TaskSnapshot,
                revision: 1,
                supersedes_revision: None,
                kind: ModelContextEventKind::Snapshot,
                content_sha256: digest,
                rendered_text,
            }],
            cache_scope_sha256: Some("scope-hash".to_string()),
            transcript_item_sha256: vec!["item-hash".to_string()],
            ..ModelContextState::default()
        };
        session.model_context_state = Some(state.clone());

        storage.save_session(&session).await?;

        let sidecar = storage
            .read_runtime_sidecar(&session.id)
            .await?
            .expect("runtime sidecar");
        assert_eq!(sidecar.model_context_state.as_ref(), Some(&state));
        assert!(sidecar.messages.is_empty());

        let loaded = storage
            .load_session(&session.id)
            .await?
            .expect("persisted session");
        assert_eq!(loaded.model_context_state.as_ref(), Some(&state));
        assert_eq!(loaded.messages.len(), session.messages.len());
        assert!(loaded
            .messages
            .iter()
            .zip(&session.messages)
            .all(|(loaded, original)| {
                loaded.id == original.id
                    && loaded.role == original.role
                    && loaded.content == original.content
            }));
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

    #[tokio::test]
    async fn fresh_constructor_waits_for_index_claim_then_reloads_published_index() -> io::Result<()>
    {
        // Prepare valid index bytes containing one entry without pre-populating
        // the fresh target directory used by the race below.
        let seed_dir = TempDir::new().map_err(io::Error::other)?;
        let seed_store = SessionStoreV2::new(seed_dir.path().to_path_buf()).await?;
        let seeded = Session::new("init-race-survivor", "test-model");
        seed_store.save_session(&seeded).await?;
        let seeded_index = fs::read(seed_dir.path().join("sessions.json")).await?;

        let target_dir = TempDir::new().map_err(io::Error::other)?;
        let target_home = target_dir.path().to_path_buf();
        let claim = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(target_home.join(SESSION_INDEX_LOCK_FILE))?;
        FileExt::lock_exclusive(&claim)?;

        let contender_home = target_home.clone();
        let mut contender = tokio::spawn(async move { SessionStoreV2::new(contender_home).await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut contender)
                .await
                .is_err(),
            "fresh initialization must inspect and publish only after acquiring the index claim"
        );

        // Simulate the process currently owning the claim publishing an index
        // between the contender's start and its eventual inspection.
        fs::write(target_home.join("sessions.json"), seeded_index).await?;
        FileExt::unlock(&claim)?;

        let storage = tokio::time::timeout(std::time::Duration::from_secs(2), contender)
            .await
            .map_err(io::Error::other)?
            .map_err(io::Error::other)??;
        assert!(storage
            .get_index_entry("init-race-survivor")
            .await
            .is_some());
        let persisted: SessionsIndex =
            serde_json::from_slice(&fs::read(target_home.join("sessions.json")).await?)
                .map_err(io::Error::other)?;
        assert!(persisted.sessions.contains_key("init-race-survivor"));
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
    async fn recover_root_session_repairs_missing_global_index_from_authoritative_file(
    ) -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let session = Session::new("ambiguous-create", "test-model");
        storage.save_session(&session).await?;

        storage
            .update_index(|index| {
                index.sessions.remove(&session.id);
                Ok(())
            })
            .await?;
        assert!(storage.get_index_entry(&session.id).await.is_none());
        assert!(
            storage.load_session(&session.id).await?.is_none(),
            "ordinary lookup trusts the missing rebuildable index"
        );

        let recovered = storage
            .recover_root_session_from_disk(&session.id)
            .await?
            .expect("authoritative session.json must survive index loss");
        assert_eq!(recovered.id, session.id);
        assert!(storage.get_index_entry(&session.id).await.is_some());
        assert!(storage.load_session(&session.id).await?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn authoritative_recovery_repairs_path_without_regressing_newer_index_fields(
    ) -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let session = Session::new("ambiguous-newer-index", "test-model");
        storage.save_session(&session).await?;
        let newer_at = session.updated_at + chrono::Duration::hours(1);
        storage
            .update_index(|index| {
                let entry = index.sessions.get_mut(&session.id).unwrap();
                entry.title = "newer-live-title".to_string();
                entry.updated_at = newer_at;
                entry.rel_path = "sessions/wrong-root".to_string();
                Ok(())
            })
            .await?;

        storage
            .recover_root_session_from_disk(&session.id)
            .await?
            .expect("authoritative root remains available");
        let repaired = storage.get_index_entry(&session.id).await.unwrap();
        assert_eq!(repaired.rel_path, format!("sessions/{}", session.id));
        assert_eq!(repaired.title, "newer-live-title");
        assert_eq!(repaired.updated_at, newer_at);
        assert_eq!(repaired.kind, SessionKind::Root);
        assert_eq!(repaired.root_session_id, session.id);
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_entry_waits_for_delete_and_does_not_resurrect_removed_session(
    ) -> io::Result<()> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let storage =
            std::sync::Arc::new(SessionStoreV2::new(temp_dir.path().to_path_buf()).await?);
        let session = Session::new("rebuild-delete-race", "test-model");
        storage.save_session(&session).await?;
        let abs_dir = storage.sessions_dir.join(&session.id);
        let rel_path = SessionStoreV2::root_rel_path(&session.id);

        let delete_claim = storage.lock_session_lifecycle_exclusive().await?;
        let rebuilding = std::sync::Arc::clone(&storage);
        let session_id = session.id.clone();
        let mut late_rebuild = tokio::spawn(async move {
            rebuilding
                .rebuild_index_entry_from_dir(&abs_dir, &session_id, rel_path)
                .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut late_rebuild)
                .await
                .is_err(),
            "rebuild probe must wait behind the exclusive delete lifecycle claim"
        );

        fs::remove_dir_all(storage.sessions_dir.join(&session.id)).await?;
        storage
            .update_index(|index| {
                index.sessions.remove(&session.id);
                Ok(())
            })
            .await?;
        drop(delete_claim);

        assert!(!late_rebuild.await.map_err(io::Error::other)??);
        assert!(storage.get_index_entry(&session.id).await.is_none());
        let persisted: SessionsIndex =
            serde_json::from_slice(&fs::read(storage.index_path()).await?)
                .map_err(io::Error::other)?;
        assert!(!persisted.sessions.contains_key(&session.id));
        Ok(())
    }

    #[tokio::test]
    async fn recover_root_session_does_not_treat_corrupt_authoritative_file_as_missing(
    ) -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let session = Session::new("ambiguous-corrupt-create", "test-model");
        storage.save_session(&session).await?;
        fs::write(
            storage.sessions_dir.join(&session.id).join("session.json"),
            b"not-json",
        )
        .await?;

        let error = storage
            .recover_root_session_from_disk(&session.id)
            .await
            .expect_err("corrupt authoritative data must remain retryable, not look deleted");
        assert!(error
            .to_string()
            .contains("invalid authoritative session.json"));
        Ok(())
    }

    #[tokio::test]
    async fn independent_store_index_updates_rebase_without_losing_entries() -> io::Result<()> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();
        // Construct both before either write so the second instance starts with
        // the exact stale empty snapshot that previously lost the first entry.
        let first = SessionStoreV2::new(bamboo_home.clone()).await?;
        let second = SessionStoreV2::new(bamboo_home.clone()).await?;
        let first_session = Session::new("cross-process-index-first", "test-model");
        let second_session = Session::new("cross-process-index-second", "test-model");

        first.save_session(&first_session).await?;
        second.save_session(&second_session).await?;

        let persisted: SessionsIndex =
            serde_json::from_slice(&fs::read(bamboo_home.join("sessions.json")).await?)
                .map_err(io::Error::other)?;
        assert!(persisted.sessions.contains_key(&first_session.id));
        assert!(persisted.sessions.contains_key(&second_session.id));

        let reopened = SessionStoreV2::new(bamboo_home).await?;
        assert!(reopened.get_index_entry(&first_session.id).await.is_some());
        assert!(reopened.get_index_entry(&second_session.id).await.is_some());
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

    #[tokio::test]
    async fn delete_wins_before_delivery_without_recreating_an_orphan_inbox() -> io::Result<()> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let storage = Arc::new(SessionStoreV2::new(temp_dir.path().to_path_buf()).await?);
        let session = Session::new("delete-inbox-race", "model");
        storage.save_session(&session).await?;
        let rel_path = storage
            .resolve_rel_path(&session.id)
            .await
            .expect("saved session has an indexed path");
        let session_dir = storage.abs_path_from_rel(&rel_path);
        // A second store instance models another Bamboo process: its in-memory
        // index stays stale after the first instance deletes the target, so
        // correctness also depends on the shared file lock and post-lock
        // `session.json` validation.
        let delivery_storage = Arc::new(SessionStoreV2::new(temp_dir.path().to_path_buf()).await?);
        let inbox = crate::FileSessionInbox::new(
            delivery_storage,
            bamboo_domain::SessionInboxLimits::default(),
        );

        // Hold the exact lifecycle exclusion that deletion owns. A delivery
        // started now cannot resolve or recreate the target until the deletion
        // and index removal have linearized.
        let lifecycle = storage.lock_session_lifecycle_exclusive().await?;
        let target = session.id.clone();
        let delivery = tokio::spawn(async move {
            inbox
                .deliver(&SessionMessageEnvelope::user_input(target, "late"))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!delivery.is_finished());

        assert!(
            storage
                .delete_session_recursive_locked(&session.id, true)
                .await?
        );
        assert!(!session_dir.exists());
        drop(lifecycle);

        let error = tokio::time::timeout(std::time::Duration::from_secs(2), delivery)
            .await
            .expect("blocked delivery must resume after deletion")
            .expect("delivery task must not panic")
            .expect_err("a deleted target cannot acknowledge a new inbox");
        assert!(matches!(
            error,
            SessionInboxError::TargetNotFound(ref id) if id == &session.id
        ));
        assert!(
            !session_dir.exists(),
            "failed delivery must not recreate an orphan session/inbox tree"
        );
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
