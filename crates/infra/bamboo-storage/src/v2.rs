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

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::sync::{
    mpsc, Mutex, Notify, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock,
};
use uuid::Uuid;

use bamboo_domain::ProviderModelRef;
use bamboo_domain::ReasoningEffort;
use bamboo_domain::{
    MessagePart, ProjectId, Role, Session, SessionAuthorityIdentity, SessionKind,
    SupervisorBootstrapReceipt, TaskList, TokenBudgetUsage, DEFAULT_SUPERVISOR_SESSION_ID,
};

mod supervisor;
#[cfg(test)]
mod supervisor_tests;

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
/// Private rollback markers for crash-safe session copies. A live `.json`
/// marker means the copy has not crossed its success boundary yet, so startup
/// must remove every target projection before exposing the store.
const SESSION_COPY_TRANSACTION_DIR: &str = ".session-copy-transactions";
const SESSION_COPY_TRANSACTION_VERSION: u32 = 1;
const SESSIONS_INDEX_VERSION: u32 = 4;

/// Filename of the append-only per-LLM-call token-usage log, stored alongside
/// `session.json` in each session directory. One JSON line per call.
const TOKEN_USAGE_FILE: &str = "token-usage.jsonl";

/// Marker (under `bamboo_home_dir`) recording that the one-shot runtime sidecar
/// migration has completed, so it is skipped on subsequent boots.
const RUNTIME_SIDECAR_MIGRATION_MARKER: &str = ".runtime_sidecar_migrated";
const SESSION_LIFECYCLE_LOCK_FILE: &str = ".session-lifecycle.lock";
const SESSION_INDEX_LOCK_FILE: &str = ".sessions-index.lock";
const SESSION_WRITE_LOCK_DIR: &str = ".session-write-locks";
const SEARCH_INDEX_REVISION_FILE: &str = ".search-index-revision";
const PERSISTENCE_METRIC_WINDOW: usize = 1024;
const SEARCH_INDEX_MAX_ATTEMPTS: usize = 3;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionCopyTransactionJournal {
    version: u32,
    transaction_id: String,
    source_id: String,
    target_id: String,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionCopyJournalMarkerState {
    Prepared,
    Committing,
    Committed,
}

impl SessionCopyJournalMarkerState {
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

#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Clone)]
struct FullSavePause {
    session_id: String,
    reached: Arc<tokio::sync::Barrier>,
    release: Arc<tokio::sync::Barrier>,
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
    let bytes = serde_json::to_vec_pretty(index).map_err(|e| other_io_error(e.to_string()))?;
    durable_atomic_write(index_path, &bytes).await
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

/// Keeps the just-published copy isolated from cross-process storage writers
/// while the HTTP layer mirrors it into process-local projections.
pub struct SessionCopyProjectionGuard {
    _lifecycle: SessionLifecycleWriteGuard,
    _runtime_task: RuntimeTaskTransactionWriteGuard,
}

/// Build the sidecar snapshot: the full session minus its `messages` history.
/// Every field except `messages` is authoritative in the sidecar; on load the
/// message history is taken back from `session.json`.
fn runtime_sidecar_snapshot(session: &Session) -> Session {
    let mut snapshot = session.clone();
    snapshot.messages.clear();
    // Native transcript groups are committed atomically with the ordinary
    // message that anchors them. Never let runtime.json expose a load/search
    // item without the corresponding session.json history boundary.
    snapshot.provider_transcript = Default::default();
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
            side.provider_transcript = main.provider_transcript;
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

#[derive(Debug, Clone, Default, Serialize)]
pub struct DurationMetricsSnapshot {
    pub count: u64,
    pub total_ms: u64,
    pub last_ms: u64,
    pub max_ms: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SavePersistenceMetricsSnapshot {
    pub lock_wait: DurationMetricsSnapshot,
    pub lock_hold: DurationMetricsSnapshot,
    pub total: DurationMetricsSnapshot,
    pub directory_preparation: DurationMetricsSnapshot,
    pub serialization: DurationMetricsSnapshot,
    pub filesystem_commit: DurationMetricsSnapshot,
    pub index_publication: DurationMetricsSnapshot,
    pub search_enqueue: DurationMetricsSnapshot,
    pub last_serialized_bytes: usize,
    pub last_message_count: usize,
    pub last_index_entry_count: usize,
}

/// Bounded, aggregate persistence telemetry. Session ids are deliberately not
/// retained as metric labels; they appear only in structured traces for local
/// correlation, avoiding unbounded cardinality.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SessionPersistenceMetricsSnapshot {
    pub full_save: SavePersistenceMetricsSnapshot,
    pub runtime_save: SavePersistenceMetricsSnapshot,
    pub index_lock_wait: DurationMetricsSnapshot,
    pub index_lock_hold: DurationMetricsSnapshot,
    pub search_index: DurationMetricsSnapshot,
    pub create_latency: DurationMetricsSnapshot,
    pub waiting_saves: usize,
    pub active_saves: usize,
    pub pending_search_jobs: usize,
    pub active_search_jobs: usize,
}

#[derive(Debug, Default)]
struct DurationMetrics {
    count: u64,
    total_ms: u64,
    last_ms: u64,
    max_ms: u64,
    recent_ms: VecDeque<u64>,
}

impl DurationMetrics {
    fn record(&mut self, duration: Duration) {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.count = self.count.saturating_add(1);
        self.total_ms = self.total_ms.saturating_add(millis);
        self.last_ms = millis;
        self.max_ms = self.max_ms.max(millis);
        if self.recent_ms.len() == PERSISTENCE_METRIC_WINDOW {
            self.recent_ms.pop_front();
        }
        self.recent_ms.push_back(millis);
    }

    fn snapshot(&self) -> DurationMetricsSnapshot {
        let mut samples = self.recent_ms.iter().copied().collect::<Vec<_>>();
        samples.sort_unstable();
        let percentile = |percent: usize| {
            if samples.is_empty() {
                return 0;
            }
            let index = ((samples.len() - 1) * percent).div_ceil(100);
            samples[index]
        };
        DurationMetricsSnapshot {
            count: self.count,
            total_ms: self.total_ms,
            last_ms: self.last_ms,
            max_ms: self.max_ms,
            p50_ms: percentile(50),
            p95_ms: percentile(95),
        }
    }
}

#[derive(Debug, Default)]
struct SavePersistenceMetrics {
    lock_wait: DurationMetrics,
    lock_hold: DurationMetrics,
    total: DurationMetrics,
    directory_preparation: DurationMetrics,
    serialization: DurationMetrics,
    filesystem_commit: DurationMetrics,
    index_publication: DurationMetrics,
    search_enqueue: DurationMetrics,
    last_serialized_bytes: usize,
    last_message_count: usize,
    last_index_entry_count: usize,
}

impl SavePersistenceMetrics {
    fn snapshot(&self) -> SavePersistenceMetricsSnapshot {
        SavePersistenceMetricsSnapshot {
            lock_wait: self.lock_wait.snapshot(),
            lock_hold: self.lock_hold.snapshot(),
            total: self.total.snapshot(),
            directory_preparation: self.directory_preparation.snapshot(),
            serialization: self.serialization.snapshot(),
            filesystem_commit: self.filesystem_commit.snapshot(),
            index_publication: self.index_publication.snapshot(),
            search_enqueue: self.search_enqueue.snapshot(),
            last_serialized_bytes: self.last_serialized_bytes,
            last_message_count: self.last_message_count,
            last_index_entry_count: self.last_index_entry_count,
        }
    }
}

#[derive(Debug, Default)]
struct PersistenceMetricsState {
    full_save: SavePersistenceMetrics,
    runtime_save: SavePersistenceMetrics,
    index_lock_wait: DurationMetrics,
    index_lock_hold: DurationMetrics,
    search_index: DurationMetrics,
    create_latency: DurationMetrics,
}

#[derive(Debug, Default)]
struct SessionPersistenceMetrics {
    state: std::sync::Mutex<PersistenceMetricsState>,
    waiting_saves: AtomicUsize,
    active_saves: AtomicUsize,
}

#[derive(Debug, Clone, Copy)]
enum SaveKind {
    Full,
    Runtime,
}

impl SaveKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Runtime => "runtime",
        }
    }
}

#[derive(Debug, Default)]
struct SaveStageDurations {
    directory_preparation: Duration,
    serialization: Duration,
    filesystem_commit: Duration,
    index_publication: Duration,
    search_enqueue: Duration,
}

impl SessionPersistenceMetrics {
    fn with_save_mut<T>(
        &self,
        kind: SaveKind,
        f: impl FnOnce(&mut SavePersistenceMetrics) -> T,
    ) -> T {
        let mut state = self.state.lock().expect("session persistence metrics lock");
        match kind {
            SaveKind::Full => f(&mut state.full_save),
            SaveKind::Runtime => f(&mut state.runtime_save),
        }
    }

    fn record_lock_wait(&self, kind: SaveKind, duration: Duration) {
        self.with_save_mut(kind, |metrics| metrics.lock_wait.record(duration));
    }

    fn record_lock_hold(&self, kind: SaveKind, duration: Duration) {
        self.with_save_mut(kind, |metrics| metrics.lock_hold.record(duration));
    }

    fn record_save(
        &self,
        kind: SaveKind,
        total: Duration,
        stages: SaveStageDurations,
        serialized_bytes: usize,
        message_count: usize,
        index_entry_count: usize,
    ) {
        self.with_save_mut(kind, |metrics| {
            metrics.total.record(total);
            metrics
                .directory_preparation
                .record(stages.directory_preparation);
            metrics.serialization.record(stages.serialization);
            metrics.filesystem_commit.record(stages.filesystem_commit);
            metrics.index_publication.record(stages.index_publication);
            metrics.search_enqueue.record(stages.search_enqueue);
            metrics.last_serialized_bytes = serialized_bytes;
            metrics.last_message_count = message_count;
            metrics.last_index_entry_count = index_entry_count;
        });
    }

    fn record_index_lock_wait(&self, duration: Duration) {
        self.state
            .lock()
            .expect("session persistence metrics lock")
            .index_lock_wait
            .record(duration);
    }

    fn record_index_lock_hold(&self, duration: Duration) {
        self.state
            .lock()
            .expect("session persistence metrics lock")
            .index_lock_hold
            .record(duration);
    }

    fn record_search_index(&self, duration: Duration) {
        self.state
            .lock()
            .expect("session persistence metrics lock")
            .search_index
            .record(duration);
    }

    fn record_create_latency(&self, duration: Duration) {
        self.state
            .lock()
            .expect("session persistence metrics lock")
            .create_latency
            .record(duration);
    }

    fn snapshot(
        &self,
        pending_search_jobs: usize,
        active_search_jobs: usize,
    ) -> SessionPersistenceMetricsSnapshot {
        let state = self.state.lock().expect("session persistence metrics lock");
        SessionPersistenceMetricsSnapshot {
            full_save: state.full_save.snapshot(),
            runtime_save: state.runtime_save.snapshot(),
            index_lock_wait: state.index_lock_wait.snapshot(),
            index_lock_hold: state.index_lock_hold.snapshot(),
            search_index: state.search_index.snapshot(),
            create_latency: state.create_latency.snapshot(),
            waiting_saves: self.waiting_saves.load(Ordering::Relaxed),
            active_saves: self.active_saves.load(Ordering::Relaxed),
            pending_search_jobs,
            active_search_jobs,
        }
    }
}

#[derive(Debug)]
struct SessionWriteGuard {
    guard: Option<OwnedMutexGuard<()>>,
    file: Option<std::fs::File>,
    locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
    session_id: String,
    kind: Option<SaveKind>,
    acquired: Instant,
    metrics: Arc<SessionPersistenceMetrics>,
}

#[derive(Debug)]
struct WaitingSaveCounter {
    metrics: Arc<SessionPersistenceMetrics>,
}

impl Drop for WaitingSaveCounter {
    fn drop(&mut self) {
        self.metrics.waiting_saves.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for SessionWriteGuard {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            let _ = FileExt::unlock(&file);
        }
        self.guard.take();
        if let Some(kind) = self.kind {
            self.metrics.active_saves.fetch_sub(1, Ordering::Relaxed);
            let held = self.acquired.elapsed();
            self.metrics.record_lock_hold(kind, held);
            tracing::debug!(
                target: "bamboo.session_persistence",
                session_id = %self.session_id,
                save_type = kind.as_str(),
                phase = "session_lock_released",
                lock_hold_ms = held.as_millis() as u64,
                "session persistence lock released"
            );
        }
        self.locks
            .remove_if(&self.session_id, |_, lock| Arc::strong_count(lock) == 1);
    }
}

#[derive(Debug)]
enum SearchIndexMutation {
    Upsert {
        session: Box<Session>,
        revision_path: PathBuf,
        expected_revision: String,
    },
    Delete {
        session_id: String,
        revision_path: PathBuf,
    },
}

impl SearchIndexMutation {
    fn session_id(&self) -> &str {
        match self {
            Self::Upsert { session, .. } => &session.id,
            Self::Delete { session_id, .. } => session_id,
        }
    }
}

#[derive(Debug)]
struct PendingSearchIndexMutation {
    generation: u64,
    mutation: SearchIndexMutation,
}

#[derive(Debug, Default)]
struct SearchIndexQueueState {
    pending: HashMap<String, PendingSearchIndexMutation>,
    order: VecDeque<String>,
}

#[derive(Debug)]
struct SearchIndexQueue {
    state: Arc<std::sync::Mutex<SearchIndexQueueState>>,
    signal: mpsc::Sender<()>,
    next_generation: AtomicU64,
    in_flight: Arc<AtomicUsize>,
    idle: Arc<Notify>,
}

impl SearchIndexQueue {
    fn new(index: SessionSearchIndex, metrics: Arc<SessionPersistenceMetrics>) -> Self {
        let state = Arc::new(std::sync::Mutex::new(SearchIndexQueueState::default()));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let idle = Arc::new(Notify::new());
        // The signal only means "inspect the coalescing map". One buffered
        // wake-up is sufficient even under a large save burst, so the channel
        // itself cannot become another unbounded queue.
        let (signal, mut receiver) = mpsc::channel(1);
        let worker_state = state.clone();
        let worker_in_flight = in_flight.clone();
        let worker_idle = idle.clone();
        tokio::spawn(async move {
            while receiver.recv().await.is_some() {
                loop {
                    let next = {
                        let mut state = worker_state.lock().expect("session search queue lock");
                        let session_id = state.order.pop_front();
                        let next =
                            session_id.and_then(|session_id| state.pending.remove(&session_id));
                        if next.is_some() {
                            worker_in_flight.fetch_add(1, Ordering::Relaxed);
                        }
                        next
                    };
                    let Some(job) = next else {
                        worker_idle.notify_waiters();
                        break;
                    };

                    let started = Instant::now();
                    let session_id = job.mutation.session_id().to_string();
                    let operation = match &job.mutation {
                        SearchIndexMutation::Upsert { .. } => "upsert",
                        SearchIndexMutation::Delete { .. } => "delete",
                    };
                    let mut result = Ok(());
                    for attempt in 1..=SEARCH_INDEX_MAX_ATTEMPTS {
                        result = match &job.mutation {
                            SearchIndexMutation::Upsert {
                                session,
                                revision_path,
                                expected_revision,
                            } => {
                                index
                                    .upsert_session_if_current(
                                        session,
                                        revision_path,
                                        expected_revision,
                                    )
                                    .await
                            }
                            SearchIndexMutation::Delete {
                                session_id,
                                revision_path,
                            } => {
                                index
                                    .delete_session_if_source_missing(session_id, revision_path)
                                    .await
                            }
                        };
                        if result.is_ok() {
                            break;
                        }
                        if attempt < SEARCH_INDEX_MAX_ATTEMPTS {
                            tokio::time::sleep(Duration::from_millis(25 * attempt as u64)).await;
                        }
                    }
                    let elapsed = started.elapsed();
                    metrics.record_search_index(elapsed);
                    if let Err(error) = result {
                        tracing::warn!(
                            target: "bamboo.session_persistence",
                            session_id,
                            generation = job.generation,
                            operation,
                            elapsed_ms = elapsed.as_millis() as u64,
                            %error,
                            "deferred session search-index mutation failed; startup rebuild will retry"
                        );
                    } else {
                        tracing::debug!(
                            target: "bamboo.session_persistence",
                            session_id,
                            generation = job.generation,
                            operation,
                            elapsed_ms = elapsed.as_millis() as u64,
                            "deferred session search-index mutation completed"
                        );
                    }
                    worker_in_flight.fetch_sub(1, Ordering::Relaxed);
                    worker_idle.notify_waiters();
                }
            }
        });
        Self {
            state,
            signal,
            next_generation: AtomicU64::new(1),
            in_flight,
            idle,
        }
    }

    fn enqueue(&self, mutation: SearchIndexMutation) -> u64 {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let session_id = mutation.session_id().to_string();
        let mut state = self.state.lock().expect("session search queue lock");
        if !state.pending.contains_key(&session_id) {
            state.order.push_back(session_id.clone());
        }
        state.pending.insert(
            session_id,
            PendingSearchIndexMutation {
                generation,
                mutation,
            },
        );
        drop(state);
        if let Err(mpsc::error::TrySendError::Closed(_)) = self.signal.try_send(()) {
            tracing::error!(
                target: "bamboo.session_persistence",
                generation,
                "session search-index worker stopped unexpectedly"
            );
        }
        generation
    }

    fn enqueue_upsert(
        &self,
        session: &Session,
        revision_path: PathBuf,
        expected_revision: String,
    ) -> u64 {
        self.enqueue(SearchIndexMutation::Upsert {
            session: Box::new(session.clone()),
            revision_path,
            expected_revision,
        })
    }

    fn enqueue_delete(&self, session_id: &str, revision_path: PathBuf) -> u64 {
        self.enqueue(SearchIndexMutation::Delete {
            session_id: session_id.to_string(),
            revision_path,
        })
    }

    fn pending_len(&self) -> usize {
        self.state
            .lock()
            .expect("session search queue lock")
            .pending
            .len()
    }

    async fn flush(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            // Register before observing the queue. `notify_waiters` does not
            // retain a permit, so checking first would leave a lost-wakeup
            // window between the idle observation and `.await`.
            notified.as_mut().enable();
            if self.pending_len() == 0 && self.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Debug)]
pub struct SessionStoreV2 {
    bamboo_home_dir: PathBuf,
    sessions_dir: PathBuf,
    index_path: PathBuf,
    search_index: SessionSearchIndex,
    search_index_queue: SearchIndexQueue,
    index: RwLock<SessionsIndex>,
    /// Serializes on-disk index writes (and any multi-step operations that must be atomic-ish).
    write_lock: Mutex<()>,
    /// Serializes full and runtime-only writes for the same session while
    /// allowing unrelated session ids to make progress independently.
    session_write_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
    persistence_metrics: Arc<SessionPersistenceMetrics>,
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
    #[cfg(any(test, feature = "test-utils"))]
    full_save_pause: std::sync::Mutex<Option<FullSavePause>>,
    #[cfg(test)]
    supervisor_bootstrap_fault: std::sync::Mutex<Option<supervisor::SupervisorBootstrapFault>>,
}

const COPY_TRANSIENT_METADATA_KEYS: &[&str] = &[
    "agent.runtime.state",
    "assignment_prompt",
    "clarification_resume_pending",
    "conclusion_with_options_resume_pending",
    "created_by_connect_key",
    "created_by_schedule_id",
    "disabled_tools",
    "external_memory_rendered",
    "execute.startup_handoff_at",
    "execute.pending_turn_message_id",
    "goal.state",
    "gold.auto_continue_count",
    "gold.evaluation_count",
    "guardian.config",
    "guardian.state",
    "context_pressure_last_level",
    "llm_request_render",
    "last_run_error",
    "last_run_status",
    "lifecycle",
    "pending_injected_messages",
    "placement",
    "project_resources_rendered",
    "prompt_component_flags",
    "prompt_component_lengths",
    "prompt_composer_version",
    "prompt_fingerprint",
    "permission.reexecute_request_generation",
    "permission.reexecute_tool_call_id",
    "resident_context",
    "resident_name",
    "responses.previous_response_id",
    "responsibility",
    "retry_resume_pending",
    "retry_resume_reason",
    "runtime.budget_exceeded_kind",
    "runtime.completion_reason",
    "runtime.kind",
    "runtime.session_start_source",
    "runtime.suspend_reason",
    "schedule_run_id",
    "skill.context",
    "spawned_by",
    "subagent_type",
    "task_list_version",
    "todo_list_version",
];

fn copied_session_snapshot(source: &Session, new_id: &str) -> Session {
    let now = Utc::now();
    let mut copy = source.clone();
    copy.authority_identity = SessionAuthorityIdentity::Ordinary;
    copy.id = new_id.to_string();
    copy.kind = SessionKind::Root;
    copy.parent_session_id = None;
    copy.root_session_id = new_id.to_string();
    copy.spawn_depth = 0;
    copy.title = format!("{} (copy)", source.title.trim_end());
    copy.title_version = 0;
    copy.title_generated = true;
    copy.metadata_version = 0;
    copy.pinned = false;
    copy.created_at = now;
    copy.updated_at = now;
    copy.pending_question = None;
    // A child budget belongs to its original tree. The independent root must
    // resolve the current model limit on its first execution instead.
    copy.token_budget = None;
    copy.resolved_token_budget = None;
    copy.agent_runtime_state = source.agent_runtime_state.as_ref().map(|state| {
        let mut clean = bamboo_domain::AgentRuntimeState::default();
        clean.set_permission_mode(state.effective_permission_mode());
        clean
    });
    copy.force_manual_compression = None;
    // Preserve every transcript message, but detach it from the source's
    // provider cache/compression epoch so the first copied turn is a clean
    // history reconstruction rather than a continuation of the source run.
    // A trailing system-resume user message is still pending execution. It is
    // control-plane state rather than completed conversation history, so strip
    // only the consecutive tail. A resume message followed by an assistant
    // response has already been consumed and remains part of the transcript.
    while copy
        .messages
        .last()
        .is_some_and(bamboo_domain::is_system_resume_message)
    {
        copy.messages.pop();
    }
    copy.clear_derived_context_state();
    // A copied conversation is a normalized history fork, not a continuation
    // of the source provider's native loading/cache epoch.
    copy.provider_transcript = Default::default();
    copy.model_context_state = None;
    copy.prompt_snapshot = None;
    for message in &mut copy.messages {
        message.compression_level = 0;
    }
    // Task progress/generations belong to the source execution control plane,
    // not to the copied conversation transcript.
    copy.task_list = None;

    for key in COPY_TRANSIENT_METADATA_KEYS {
        copy.metadata.remove(*key);
    }
    for key in bamboo_domain::PERMISSION_AUDIT_METADATA_KEYS {
        copy.metadata.remove(*key);
    }
    copy.metadata.retain(|key, _| {
        let durable_workflow_config = matches!(
            key.as_str(),
            "workflow.selection.v1"
                | "workflow.orchestration_opt_in"
                | "workflow.active.v1"
                | "workflow.active.snapshot.v1"
        );
        !key.starts_with("a2a.")
            && !key.starts_with("execute.")
            && !key.starts_with("external.")
            && !key.starts_with("gold.last_")
            && !key.starts_with("prefix_cache_")
            && !key.starts_with("runtime_prompt_")
            && !key.starts_with("skill_runtime_")
            && (!key.starts_with("workflow.") || durable_workflow_config)
    });
    if let Some(runtime) = copy.runtime_metadata.as_mut() {
        runtime.subagent_type = None;
        runtime.last_run_status = None;
        runtime.last_run_error = None;
        runtime.pending_injected_messages = None;
        runtime.task_list_version = None;
        runtime.todo_list_version = None;
        runtime.session_inbox_admission = None;
        if runtime.is_empty() {
            copy.runtime_metadata = None;
        }
    }
    copy
}

fn rewrite_attachment_session_urls(session: &mut Session, source_id: &str, new_id: &str) {
    let source_prefix = format!("bamboo-attachment://{source_id}/");
    let target_prefix = format!("bamboo-attachment://{new_id}/");
    for message in &mut session.messages {
        if let Some(parts) = message.content_parts.as_mut() {
            for part in parts {
                if let MessagePart::ImageUrl { image_url } = part {
                    if let Some(attachment_id) = image_url.url.strip_prefix(&source_prefix) {
                        image_url.url = format!("{target_prefix}{attachment_id}");
                    }
                }
            }
        }
        if let Some(results) = message.image_ocr.as_mut() {
            for result in results {
                if let Some(attachment_id) = result.image_url.strip_prefix(&source_prefix) {
                    result.image_url = format!("{target_prefix}{attachment_id}");
                }
            }
        }
    }
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

        let persistence_metrics = Arc::new(SessionPersistenceMetrics::default());
        let search_index_queue =
            SearchIndexQueue::new(search_index.clone(), persistence_metrics.clone());
        let storage = Self {
            bamboo_home_dir,
            sessions_dir,
            index_path,
            search_index,
            search_index_queue,
            index: RwLock::new(index),
            write_lock: Mutex::new(()),
            session_write_locks: Arc::new(DashMap::new()),
            persistence_metrics,
            session_lifecycle_lock: std::sync::Arc::new(RwLock::new(())),
            runtime_task_transaction_gate: std::sync::Arc::new(RwLock::new(())),
            runtime_task_recovery_required: AtomicBool::new(false),
            #[cfg(test)]
            runtime_task_faults: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            runtime_task_first_write_pause: std::sync::Mutex::new(None),
            #[cfg(test)]
            runtime_task_durability_events: std::sync::Mutex::new(Vec::new()),
            #[cfg(any(test, feature = "test-utils"))]
            full_save_pause: std::sync::Mutex::new(None),
            #[cfg(test)]
            supervisor_bootstrap_fault: std::sync::Mutex::new(None),
        };

        // Create and permission the private journal directory once at store
        // initialization. Clean hot-path reads only probe it and never repeat
        // mkdir/chmod work; journal creation also revalidates it before write.
        storage.ensure_runtime_task_transaction_dir().await?;
        storage.ensure_session_copy_transaction_dir().await?;
        storage.ensure_session_write_lock_dir().await?;

        // Constructor recovery takes the same cross-process exclusive gate as
        // a live commit. A second store can therefore recover an orphan, but
        // can never mistake another process's in-flight journal for one.
        {
            let _lifecycle = storage.lock_session_lifecycle_exclusive().await?;
            let _runtime_task = storage.lock_runtime_task_transaction_exclusive().await?;
            storage
                .recover_all_runtime_task_transactions_locked()
                .await?;
            storage
                .recover_all_session_copy_transactions_locked()
                .await?;
        }

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
        if let Err(error) = supervisor::validate_overlay(&main, sidecar.as_ref()) {
            tracing::warn!("index rebuild: skipping invalid authority for {id}: {error}");
            return None;
        }
        let mut session = overlay_runtime_sidecar(main, sidecar);
        session.clear_stale_root_token_budget();
        Some(session)
    }

    /// Strict operation loader: unlike rebuild recovery, copy must distinguish
    /// a missing source from corrupt/unreadable authoritative state and must
    /// never silently fall back to stale `session.json` control-plane data.
    async fn load_session_from_dir_strict(
        abs_dir: &Path,
        id: &str,
        expected_kind: SessionKind,
        expected_root_id: &str,
    ) -> io::Result<Option<Session>> {
        let raw = match fs::read_to_string(abs_dir.join("session.json")).await {
            Ok(raw) => raw,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut main: Session = serde_json::from_str(&raw).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid source session.json: {error}"),
            )
        })?;
        let canonical_legacy_root = expected_kind == SessionKind::Root
            && expected_root_id == id
            && main.root_session_id.is_empty();
        if main.id != id
            || main.kind != expected_kind
            || (!canonical_legacy_root && main.root_session_id != expected_root_id)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "session identity does not match its expected canonical path",
            ));
        }
        // `root_session_id` predates some on-disk root sessions and therefore
        // deserializes to an empty string through serde(default). Only a root
        // found at its own canonical root path may use that legacy encoding;
        // normalize it before comparing/overlaying the runtime sidecar.
        if canonical_legacy_root {
            main.root_session_id = id.to_string();
        }
        let runtime_path = abs_dir.join(RUNTIME_SIDECAR_FILE);
        let sidecar = match fs::read_to_string(&runtime_path).await {
            Ok(raw) => {
                let mut sidecar: Session = serde_json::from_str(&raw).map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid source runtime.json: {error}"),
                    )
                })?;
                if expected_kind == SessionKind::Root
                    && expected_root_id == id
                    && sidecar.root_session_id.is_empty()
                {
                    sidecar.root_session_id = id.to_string();
                }
                if sidecar.id != id
                    || sidecar.kind != main.kind
                    || sidecar.root_session_id != main.root_session_id
                    || sidecar.parent_session_id != main.parent_session_id
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "source runtime sidecar identity does not match session.json",
                    ));
                }
                Some(sidecar)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        supervisor::validate_overlay(&main, sidecar.as_ref())?;
        let mut session = overlay_runtime_sidecar(main, sidecar);
        session.clear_stale_root_token_budget();
        Ok(Some(session))
    }

    async fn directory_has_regular_files(path: &Path) -> io::Result<bool> {
        let mut entries = match fs::read_dir(path).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn copy_source_identity_from_rel(
        source_id: &str,
        source_rel: &str,
    ) -> io::Result<(SessionKind, String)> {
        let parts = source_rel.split('/').collect::<Vec<_>>();
        match parts.as_slice() {
            ["sessions", id] if *id == source_id => Ok((SessionKind::Root, source_id.to_string())),
            ["sessions", root_id, "children", id] if *id == source_id => {
                validate_session_id(root_id)?;
                Ok((SessionKind::Child, (*root_id).to_string()))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "source session index path is not canonical",
            )),
        }
    }

    pub fn search_index(&self) -> &SessionSearchIndex {
        &self.search_index
    }

    /// Wait until every search-index mutation accepted before this observation
    /// has completed or exhausted its bounded retries. Durable session reads do
    /// not require this; it exists for callers that require search read-after-write.
    pub async fn flush_search_index(&self) {
        self.search_index_queue.flush().await;
    }

    pub fn persistence_metrics(&self) -> SessionPersistenceMetricsSnapshot {
        self.persistence_metrics.snapshot(
            self.search_index_queue.pending_len(),
            self.search_index_queue.in_flight.load(Ordering::Relaxed),
        )
    }

    pub fn record_session_create_latency(&self, duration: Duration) {
        self.persistence_metrics.record_create_latency(duration);
    }

    /// Publish a tiny, atomic revision marker after `session.json` is visible.
    /// The FTS worker checks this marker while holding SQLite's write
    /// transaction, so a delayed upsert cannot resurrect a deleted session or
    /// overwrite a later full save. Directory durability is not required for
    /// the marker: startup rebuild publishes a fresh revision after a crash.
    async fn publish_search_revision(&self, abs_dir: &Path) -> io::Result<(PathBuf, String)> {
        let revision_path = abs_dir.join(SEARCH_INDEX_REVISION_FILE);
        let revision = Uuid::new_v4().to_string();
        let tmp = revision_path.with_extension(format!("tmp.{}", Uuid::new_v4()));
        if let Err(error) = fs::write(&tmp, revision.as_bytes()).await {
            let _ = fs::remove_file(&tmp).await;
            return Err(error);
        }
        if let Err(error) = atomic_rename(&tmp, &revision_path).await {
            let _ = fs::remove_file(&tmp).await;
            return Err(error);
        }
        Ok((revision_path, revision))
    }

    async fn acquire_session_write_lock(
        &self,
        session_id: &str,
        kind: SaveKind,
    ) -> io::Result<SessionWriteGuard> {
        self.acquire_session_lock(session_id, Some(kind)).await
    }

    async fn acquire_session_maintenance_lock(
        &self,
        session_id: &str,
    ) -> io::Result<SessionWriteGuard> {
        self.acquire_session_lock(session_id, None).await
    }

    async fn open_session_write_lock_file_at(path: PathBuf) -> io::Result<std::fs::File> {
        tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)?;
            FileExt::lock_exclusive(&file)?;
            Ok(file)
        })
        .await
        .map_err(|error| other_io_error(format!("join session write-lock task: {error}")))?
    }

    async fn open_session_write_lock_file(&self, session_id: &str) -> io::Result<std::fs::File> {
        let path = self.session_write_lock_path(session_id);
        match Self::open_session_write_lock_file_at(path.clone()).await {
            Ok(file) => Ok(file),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.ensure_session_write_lock_dir().await?;
                Self::open_session_write_lock_file_at(path).await
            }
            Err(error) => Err(error),
        }
    }

    async fn acquire_session_lock(
        &self,
        session_id: &str,
        kind: Option<SaveKind>,
    ) -> io::Result<SessionWriteGuard> {
        validate_session_id(session_id)?;
        let waiting = kind.map(|_| {
            self.persistence_metrics
                .waiting_saves
                .fetch_add(1, Ordering::Relaxed);
            WaitingSaveCounter {
                metrics: self.persistence_metrics.clone(),
            }
        });
        let started = Instant::now();
        let lock = self
            .session_write_locks
            .entry(session_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        // Arm cleanup before either the process-local or cross-process wait.
        let mut session_guard = SessionWriteGuard {
            guard: None,
            file: None,
            locks: self.session_write_locks.clone(),
            session_id: session_id.to_string(),
            kind: None,
            acquired: Instant::now(),
            metrics: self.persistence_metrics.clone(),
        };
        session_guard.guard = Some(lock.lock_owned().await);
        let file = self.open_session_write_lock_file(session_id).await?;
        session_guard.file = Some(file);
        drop(waiting);
        if let Some(kind) = kind {
            self.persistence_metrics
                .active_saves
                .fetch_add(1, Ordering::Relaxed);
            let waited = started.elapsed();
            self.persistence_metrics.record_lock_wait(kind, waited);
            tracing::debug!(
                target: "bamboo.session_persistence",
                session_id,
                save_type = kind.as_str(),
                phase = "session_lock_acquired",
                lock_wait_ms = waited.as_millis() as u64,
                waiting_saves = self.persistence_metrics.waiting_saves.load(Ordering::Relaxed),
                "session persistence lock acquired"
            );
            session_guard.kind = Some(kind);
            session_guard.acquired = Instant::now();
        }
        Ok(session_guard)
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn pause_full_save_before_filesystem_commit_for_test(
        &self,
        session_id: &str,
    ) -> (Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>) {
        let reached = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Barrier::new(2));
        *self.full_save_pause.lock().expect("full save pause lock") = Some(FullSavePause {
            session_id: session_id.to_string(),
            reached: reached.clone(),
            release: release.clone(),
        });
        (reached, release)
    }

    #[cfg(any(test, feature = "test-utils"))]
    async fn maybe_pause_full_save_before_filesystem_commit(&self, session_id: &str) {
        let pause = {
            let mut configured = self.full_save_pause.lock().expect("full save pause lock");
            if configured
                .as_ref()
                .is_some_and(|pause| pause.session_id == session_id)
            {
                configured.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause.reached.wait().await;
            pause.release.wait().await;
        }
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
            // Use the same lock order as foreground saves. Serializing the
            // load+enqueue pair with that session's durable commit prevents a
            // background rebuild snapshot from being assigned a later queue
            // generation than a newer foreground save.
            let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
            let _session_write = self.acquire_session_maintenance_lock(&session_id).await?;
            if let Some(session) = self.load_session_unlocked(&session_id).await? {
                if !should_index_session(session.updated_at) {
                    continue;
                }
                let Some(session_path) = self.session_json_path(&session_id).await? else {
                    continue;
                };
                let abs_dir = session_path.parent().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "session path has no parent")
                })?;
                let (revision_path, revision) = self.publish_search_revision(abs_dir).await?;
                self.search_index_queue
                    .enqueue_upsert(&session, revision_path, revision);
            }
        }
        self.flush_search_index().await;
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
        let wait_started = Instant::now();
        let _process = self.write_lock.lock().await;
        let _file = self.lock_index_file_exclusive().await?;
        let waited = wait_started.elapsed();
        self.persistence_metrics.record_index_lock_wait(waited);
        let hold_started = Instant::now();
        let result = self.update_index_under_claim(f).await;
        let held = hold_started.elapsed();
        self.persistence_metrics.record_index_lock_hold(held);
        tracing::debug!(
            target: "bamboo.session_persistence",
            phase = "index_published",
            index_lock_wait_ms = waited.as_millis() as u64,
            index_lock_hold_ms = held.as_millis() as u64,
            outcome = if result.is_ok() { "ok" } else { "error" },
            "session index mutation completed"
        );
        result
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
        supervisor::validate_overlay(&main, sidecar.as_ref())?;
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

    fn session_copy_transaction_dir(&self) -> PathBuf {
        self.bamboo_home_dir.join(SESSION_COPY_TRANSACTION_DIR)
    }

    fn session_write_lock_dir(&self) -> PathBuf {
        self.bamboo_home_dir.join(SESSION_WRITE_LOCK_DIR)
    }

    fn session_write_lock_path(&self, session_id: &str) -> PathBuf {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let digest = Sha256::digest(session_id.as_bytes());
        let mut name = String::with_capacity(digest.len() * 2 + 5);
        for byte in digest {
            name.push(HEX[(byte >> 4) as usize] as char);
            name.push(HEX[(byte & 0x0f) as usize] as char);
        }
        name.push_str(".lock");
        self.session_write_lock_dir().join(name)
    }

    fn session_copy_staging_dir(&self, journal: &SessionCopyTransactionJournal) -> PathBuf {
        self.bamboo_home_dir
            .join(format!(".session-copy-{}", journal.transaction_id))
    }

    async fn ensure_private_transaction_dir(&self, path: PathBuf) -> io::Result<PathBuf> {
        let home_existed = fs::try_exists(&self.bamboo_home_dir).await?;
        fs::create_dir_all(&self.bamboo_home_dir).await?;
        if !home_existed {
            sync_parent_directory_entry(&self.bamboo_home_dir).await?;
        }
        let path_existed = fs::try_exists(&path).await?;
        fs::create_dir_all(&path).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).await?;
        }
        if !path_existed {
            sync_parent_directory_entry(&path).await?;
        }
        Ok(path)
    }

    async fn ensure_runtime_task_transaction_dir(&self) -> io::Result<PathBuf> {
        self.ensure_private_transaction_dir(self.runtime_task_transaction_dir())
            .await
    }

    async fn ensure_session_copy_transaction_dir(&self) -> io::Result<PathBuf> {
        self.ensure_private_transaction_dir(self.session_copy_transaction_dir())
            .await
    }

    async fn ensure_session_write_lock_dir(&self) -> io::Result<PathBuf> {
        self.ensure_private_transaction_dir(self.session_write_lock_dir())
            .await
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
        if session_id == DEFAULT_SUPERVISOR_SESSION_ID {
            if let Some(root) = self.load_root_authority_unchecked(session_id).await? {
                return Ok(Some(root));
            }
            // An Ordinary Child may already own this ID in another tree.
            // Only canonical Root absence permits its normal control-plane read.
        }
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
        supervisor::validate_identity(&session)?;
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
        supervisor::validate_overlay(&main, sidecar.as_ref())?;
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

    async fn session_copy_journal_paths(&self) -> io::Result<Vec<PathBuf>> {
        let mut entries = match fs::read_dir(self.session_copy_transaction_dir()).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut paths = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if entry.file_type().await?.is_file()
                && SessionCopyJournalMarkerState::from_path(&path).is_some()
            {
                paths.push(path);
            }
        }
        paths.sort();
        Ok(paths)
    }

    fn validate_session_copy_journal(
        path: &Path,
        journal: &SessionCopyTransactionJournal,
    ) -> io::Result<()> {
        if journal.version != SESSION_COPY_TRANSACTION_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported session copy transaction journal version {}",
                    journal.version
                ),
            ));
        }
        let file_id = path
            .file_stem()
            .and_then(|value| value.to_str())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid copy journal name")
            })?;
        Uuid::parse_str(file_id).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid session copy transaction UUID: {error}"),
            )
        })?;
        if journal.transaction_id != file_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "session copy transaction id does not match its filename",
            ));
        }
        validate_session_id(&journal.source_id)?;
        validate_session_id(&journal.target_id)?;
        if journal.source_id == journal.target_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "session copy transaction source and target must differ",
            ));
        }
        Ok(())
    }

    async fn read_session_copy_journal(
        &self,
        path: &Path,
    ) -> io::Result<SessionCopyTransactionJournal> {
        let raw = fs::read_to_string(path).await?;
        let journal = serde_json::from_str(&raw).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid session copy transaction journal: {error}"),
            )
        })?;
        Self::validate_session_copy_journal(path, &journal)?;
        Ok(journal)
    }

    async fn write_session_copy_journal(
        &self,
        journal: &SessionCopyTransactionJournal,
    ) -> io::Result<PathBuf> {
        let dir = self.ensure_session_copy_transaction_dir().await?;
        let path = dir.join(format!("{}.json", journal.transaction_id));
        let bytes = serde_json::to_vec(journal)
            .map_err(|error| other_io_error(format!("serialize session copy journal: {error}")))?;
        durable_atomic_write(&path, &bytes).await?;
        Ok(path)
    }

    async fn remove_session_copy_journal_family(
        &self,
        journal: &SessionCopyTransactionJournal,
    ) -> io::Result<()> {
        for extension in ["json", "committing", "committed"] {
            let path = self
                .session_copy_transaction_dir()
                .join(format!("{}.{}", journal.transaction_id, extension));
            match fs::try_exists(&path).await {
                Ok(true) => durable_deactivate_recovery_marker(&path).await?,
                Ok(false) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    async fn rollback_session_copy_transaction(
        &self,
        journal: &SessionCopyTransactionJournal,
    ) -> io::Result<()> {
        // Hide the target first. If cleanup later fails, the retained marker
        // makes startup retry before the store becomes available.
        let mut errors = Vec::new();
        if let Err(error) = self
            .update_index(|index| {
                index.sessions.remove(&journal.target_id);
                Ok(())
            })
            .await
        {
            errors.push(format!("index cleanup: {error}"));
            // The durable index could not be rewritten, but readers of this
            // still-live store must not retain a target whose directory is
            // about to be removed. The journal keeps cross-process recovery
            // retryable; this in-memory removal closes the local visibility gap.
            self.index.write().await.sessions.remove(&journal.target_id);
        }
        let target_dir = self.sessions_dir.join(&journal.target_id);
        let revision_path = target_dir.join(SEARCH_INDEX_REVISION_FILE);
        let staging_dir = self.session_copy_staging_dir(journal);
        for path in [&target_dir, &staging_dir] {
            match fs::remove_dir_all(path).await {
                Ok(()) => {
                    if let Err(error) = sync_parent_directory_entry(path).await {
                        errors.push(format!("sync cleanup {}: {error}", path.display()));
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => errors.push(format!("remove {}: {error}", path.display())),
            }
        }
        self.search_index_queue
            .enqueue_delete(&journal.target_id, revision_path);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(other_io_error(format!(
                "session copy rollback incomplete: {}",
                errors.join("; ")
            )))
        }
    }

    async fn recover_session_copy_journal(
        &self,
        path: &Path,
        journal: &SessionCopyTransactionJournal,
    ) -> io::Result<()> {
        let state = SessionCopyJournalMarkerState::from_path(path).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid session copy journal marker extension",
            )
        })?;
        if state == SessionCopyJournalMarkerState::Committed {
            let target_dir = self.sessions_dir.join(&journal.target_id);
            let target = Self::load_session_from_dir_strict(
                &target_dir,
                &journal.target_id,
                SessionKind::Root,
                &journal.target_id,
            )
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "committed copied session target is missing",
                )
            })?;
            if target.kind != SessionKind::Root || target.root_session_id != target.id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "committed copied session is not an independent root",
                ));
            }
            let has_attachments =
                Self::directory_has_regular_files(&target_dir.join("attachments")).await?;
            self.upsert_index_from_session_inner(
                &target,
                Self::root_rel_path(&target.id),
                false,
                Some(has_attachments),
            )
            .await?;
            let (revision_path, revision) = self.publish_search_revision(&target_dir).await?;
            self.search_index_queue
                .enqueue_upsert(&target, revision_path, revision);
        } else {
            self.rollback_session_copy_transaction(journal).await?;
        }
        durable_deactivate_recovery_marker(path).await
    }

    async fn recover_all_session_copy_transactions_locked(&self) -> io::Result<()> {
        for path in self.session_copy_journal_paths().await? {
            let journal = self.read_session_copy_journal(&path).await?;
            self.recover_session_copy_journal(&path, &journal).await?;
        }
        // A previous marker deactivation may have completed its rename to an
        // inert tombstone but failed the directory sync. Lifecycle mutations
        // that could remove a target must not proceed until every such visible
        // rename is itself durable, or power loss could resurrect a committed
        // marker after the target was deleted.
        sync_directory(&self.session_copy_transaction_dir()).await?;
        Ok(())
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
        if current.authority_identity != original.authority_identity
            || !Self::runtime_task_owned_snapshot_matches(&current, original)?
        {
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
        if current_first.authority_identity != first_original.authority_identity
            || current_second.authority_identity != second_original.authority_identity
            || !Self::runtime_task_owned_snapshot_matches(&current_first, first_original)?
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
        let bytes =
            serde_json::to_vec_pretty(&snapshot).map_err(|e| other_io_error(e.to_string()))?;
        atomic_write(&path, &bytes).await
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
            supervisor::validate_identity(&session)?;
            if !matches!(
                session.authority_identity,
                SessionAuthorityIdentity::Ordinary
            ) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "cannot reconstruct missing Supervisor authority from session.json",
                ));
            }
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
    /// for Ordinary compatibility. Supervisor callers validate the canonical
    /// pair and reject missing or corrupt runtime authority. Shared by
    /// [`Self::read_runtime_sidecar`] (index-resolved path) and the index
    /// rebuild (directory-scanned path) so both overlay the sidecar identically.
    async fn read_runtime_sidecar_at(path: &Path, id: &str) -> io::Result<Option<Session>> {
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path).await?;
        match serde_json::from_str::<Session>(&raw) {
            Ok(mut side) => {
                supervisor::validate_identity(&side)?;
                // The control-plane path (`load_runtime_control_plane`) returns
                // this directly, so migrate a stale Root token_budget here too (#230).
                side.clear_stale_root_token_budget();
                Ok(Some(side))
            }
            Err(error) => {
                // Ordinary Sessions may recover from session.json. Supervisor
                // pair validation rejects this fallback before use or writes.
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
        self.upsert_index_from_session_inner(session, rel_path, false, None)
            .await
    }

    async fn repair_index_from_authoritative_session(
        &self,
        session: &Session,
        rel_path: String,
    ) -> io::Result<()> {
        self.upsert_index_from_session_inner(session, rel_path, true, None)
            .await
    }

    async fn upsert_index_from_session_inner(
        &self,
        session: &Session,
        rel_path: String,
        preserve_newer: bool,
        known_has_attachments: Option<bool>,
    ) -> io::Result<()> {
        let has_attachments = match known_has_attachments {
            Some(value) => value,
            None => self.compute_has_attachments(&session.id).await,
        };
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
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
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

    /// Copy one session into a new, independent root session.
    ///
    /// The caller must serialize the source with the ordinary per-session
    /// persistence lock before invoking this method. The lifecycle write lock
    /// prevents a concurrent delete/rebuild from racing the source attachment
    /// snapshot or observing the private staging directory; the exclusive
    /// runtime/task lock also freezes cross-process writers. The fully written
    /// directory is renamed into place before the index entry is published;
    /// any pre-commit error removes every target artifact and index projection.
    /// Once the durable committed marker is published, recovery completes the
    /// successful copy instead of reverting it.
    pub async fn copy_session(&self, source_id: &str, new_id: &str) -> io::Result<Option<Session>> {
        Ok(self
            .copy_session_with_projection_guard(source_id, new_id)
            .await?
            .map(|(session, guard)| {
                drop(guard);
                session
            }))
    }

    /// Copy a session while retaining the cross-process publication boundary.
    /// The caller must drop the returned guard only after all process-local
    /// cache/workspace/feed projections derived from the returned snapshot are
    /// visible.
    pub async fn copy_session_with_projection_guard(
        &self,
        source_id: &str,
        new_id: &str,
    ) -> io::Result<Option<(Session, SessionCopyProjectionGuard)>> {
        validate_session_id(source_id)?;
        validate_session_id(new_id)?;
        if source_id == new_id {
            return Err(other_io_error("source and copied session ids must differ"));
        }

        let _lifecycle = self.lock_session_lifecycle_exclusive().await?;
        // Ordinary full/runtime saves hold the shared form of this lock. The
        // exclusive claim freezes every cross-process source writer while we
        // read session.json/runtime.json and copy referenced attachments.
        let _runtime_task = self.lock_runtime_task_transaction_exclusive().await?;
        self.recover_all_runtime_task_transactions_locked().await?;
        self.recover_all_session_copy_transactions_locked().await?;
        let Some(source_rel) = self.resolve_rel_path(source_id).await else {
            return Ok(None);
        };
        let (expected_source_kind, expected_source_root) =
            Self::copy_source_identity_from_rel(source_id, &source_rel)?;
        if self.resolve_rel_path(new_id).await.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "copied session id already exists",
            ));
        }

        let source_dir = self.abs_path_from_rel(&source_rel);
        let source = Self::load_session_from_dir_strict(
            &source_dir,
            source_id,
            expected_source_kind,
            &expected_source_root,
        )
        .await?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "indexed source session.json is missing",
            )
        })?;
        let target_rel = Self::root_rel_path(new_id);
        let target_dir = self.abs_path_from_rel(&target_rel);
        if fs::try_exists(&target_dir).await? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "copied session directory already exists",
            ));
        }
        let transaction_id = Uuid::new_v4().to_string();
        let journal = SessionCopyTransactionJournal {
            version: SESSION_COPY_TRANSACTION_VERSION,
            transaction_id,
            source_id: source_id.to_string(),
            target_id: new_id.to_string(),
        };
        let staging_dir = self.session_copy_staging_dir(&journal);
        let journal_path = self.write_session_copy_journal(&journal).await?;

        let mut copied = copied_session_snapshot(&source, new_id);
        rewrite_attachment_session_urls(&mut copied, source_id, new_id);
        let has_attachments = match self
            .write_copied_session(&source_dir, &staging_dir, &target_dir, &copied)
            .await
        {
            Ok(has_attachments) => has_attachments,
            Err(error) => {
                return self
                    .fail_session_copy_with_rollback(&journal_path, &journal, error)
                    .await
                    .map(|()| None)
            }
        };
        let (search_revision_path, search_revision) =
            match self.publish_search_revision(&target_dir).await {
                Ok(revision) => revision,
                Err(error) => {
                    return self
                        .fail_session_copy_with_rollback(&journal_path, &journal, error)
                        .await
                        .map(|()| None)
                }
            };
        let committing = journal_path.with_extension("committing");
        if let Err(error) = atomic_rename(&journal_path, &committing).await {
            return self
                .fail_session_copy_with_rollback(&journal_path, &journal, error)
                .await
                .map(|()| None);
        }
        if let Err(error) = sync_parent_directory_entry(&committing).await {
            return self
                .fail_session_copy_with_rollback(&committing, &journal, error)
                .await
                .map(|()| None);
        }
        let committed = journal_path.with_extension("committed");
        if let Err(error) = atomic_rename(&committing, &committed).await {
            return self
                .fail_session_copy_with_rollback(&committing, &journal, error)
                .await
                .map(|()| None);
        }
        if let Err(error) = sync_parent_directory_entry(&committed).await {
            // A rename is not a durable commit boundary until its containing
            // directory is synchronized. Do not acknowledge a copy whose
            // recovery decision could revert to `.committing` after power loss.
            return self
                .fail_session_copy_with_rollback(&committed, &journal, error)
                .await
                .map(|()| None);
        }
        // Only a durable committed marker may precede public index visibility.
        // A crash from here is completed by startup recovery; while this call
        // remains alive, any publication error is synchronously rolled back.
        if let Err(error) = self
            .upsert_index_from_session_inner(&copied, target_rel, false, Some(has_attachments))
            .await
        {
            return self
                .fail_session_copy_with_rollback(&committed, &journal, error)
                .await
                .map(|()| None);
        }
        self.search_index_queue
            .enqueue_upsert(&copied, search_revision_path, search_revision);
        // A retained committed marker is safe: every target-removing lifecycle
        // operation recovers committed copies before it mutates the tree. This
        // marker cleanup is therefore retryable maintenance after the durable
        // commit decision, not a reason to report a committed copy as failed.
        if let Err(error) = durable_deactivate_recovery_marker(&committed).await {
            tracing::warn!(session_id = new_id, %error, "copied session committed; journal cleanup deferred to lifecycle recovery");
        }
        Ok(Some((
            copied,
            SessionCopyProjectionGuard {
                _lifecycle,
                _runtime_task,
            },
        )))
    }

    async fn write_copied_session(
        &self,
        source_dir: &Path,
        staging_dir: &Path,
        target_dir: &Path,
        copied: &Session,
    ) -> io::Result<bool> {
        fs::create_dir(staging_dir).await?;
        fs::create_dir(staging_dir.join("children")).await?;
        let source_attachments = source_dir.join("attachments");
        let staging_attachments = staging_dir.join("attachments");
        fs::create_dir(&staging_attachments).await?;
        let mut has_attachments = false;
        if fs::try_exists(&source_attachments).await? {
            let mut entries = fs::read_dir(&source_attachments).await?;
            while let Some(entry) = entries.next_entry().await? {
                if !entry.file_type().await?.is_file() {
                    continue;
                }
                durable_copy_file(&entry.path(), &staging_attachments.join(entry.file_name()))
                    .await?;
                has_attachments = true;
            }
        }
        // Persist copied file names after each file's contents have reached
        // stable storage. The later staging-directory sync only persists the
        // `attachments/` directory entry, not entries inside that directory.
        sync_directory(&staging_attachments).await?;

        let runtime_snapshot = runtime_sidecar_snapshot(copied);
        let runtime_bytes = serde_json::to_vec_pretty(&runtime_snapshot)
            .map_err(|error| other_io_error(error.to_string()))?;
        durable_atomic_write(&staging_dir.join(RUNTIME_SIDECAR_FILE), &runtime_bytes).await?;
        let session_json =
            serde_json::to_vec_pretty(copied).map_err(|error| other_io_error(error.to_string()))?;
        durable_atomic_write(&staging_dir.join("session.json"), &session_json).await?;
        // Flush the staging directory after its children/attachments are all
        // complete, before its name is published under `sessions/`.
        sync_parent_directory_entry(&staging_dir.join("session.json")).await?;
        atomic_rename(staging_dir, target_dir).await?;
        // This rename crosses from `$BAMBOO_HOME` into `sessions/`: both
        // directory entries must be synchronized before publishing the target.
        sync_parent_directory_entry(staging_dir).await?;
        sync_parent_directory_entry(target_dir).await?;
        Ok(has_attachments)
    }

    async fn fail_session_copy_with_rollback(
        &self,
        marker_path: &Path,
        journal: &SessionCopyTransactionJournal,
        primary: io::Error,
    ) -> io::Result<()> {
        let primary_kind = primary.kind();
        let primary_message = primary.to_string();
        // A durable `.committed` marker tells startup to finish the copy. If a
        // live caller has not acknowledged success and chooses rollback, first
        // durably move that decision back to a rollback state. Otherwise a
        // partial cleanup could leave a committed marker pointing at a removed
        // target and make every subsequent startup fail closed forever.
        if SessionCopyJournalMarkerState::from_path(marker_path)
            == Some(SessionCopyJournalMarkerState::Committed)
        {
            let rollback_marker = marker_path.with_extension("committing");
            if let Err(error) = async {
                atomic_rename(marker_path, &rollback_marker).await?;
                sync_parent_directory_entry(&rollback_marker).await
            }
            .await
            {
                return Err(other_io_error(format!(
                    "session copy failed ({primary_message}); could not persist rollback decision ({error}); recovery required"
                )));
            }
        }
        match self.rollback_session_copy_transaction(journal).await {
            Ok(()) => match self.remove_session_copy_journal_family(journal).await {
                Ok(()) => Err(io::Error::new(
                    primary_kind,
                    format!("session copy failed and was rolled back: {primary_message}"),
                )),
                Err(cleanup_error) => Err(other_io_error(format!(
                    "session copy failed ({primary_message}); rollback succeeded but journal cleanup failed ({cleanup_error}); recovery required"
                ))),
            },
            Err(rollback_error) => Err(other_io_error(format!(
                "session copy failed ({primary_message}); rollback failed ({rollback_error}); recovery required"
            ))),
        }
    }

    pub async fn clear_session(&self, session_id: &str) -> io::Result<bool> {
        validate_session_id(session_id)?;
        let _lifecycle = self.lock_session_lifecycle_exclusive().await?;
        let _runtime_task = self.lock_runtime_task_transaction_exclusive().await?;
        self.recover_all_runtime_task_transactions_locked().await?;
        self.recover_all_session_copy_transactions_locked().await?;

        let Some(entry) = self.get_index_entry(session_id).await else {
            return Ok(false);
        };
        let rel_path = entry.rel_path.clone();
        let abs_dir = self.abs_path_from_rel(&rel_path);
        let Some(mut session) = Self::load_session_from_dir_strict(
            &abs_dir,
            session_id,
            entry.kind,
            &entry.root_session_id,
        )
        .await?
        else {
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
        let attachments_dir = abs_dir.join("attachments");
        match fs::remove_dir_all(&attachments_dir).await {
            Ok(()) => sync_parent_directory_entry(&attachments_dir).await?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        fs::create_dir_all(&attachments_dir).await?;
        sync_parent_directory_entry(&attachments_dir).await?;

        self.write_runtime_sidecar(&abs_dir, &session).await?;
        let path = abs_dir.join("session.json");
        let tmp = path.with_extension(format!("json.tmp.{}", Uuid::new_v4()));
        let bytes = serde_json::to_vec_pretty(&session)
            .map_err(|error| other_io_error(error.to_string()))?;
        fs::write(&tmp, bytes).await?;
        atomic_rename(&tmp, &path).await?;
        let (revision_path, revision) = self.publish_search_revision(&abs_dir).await?;
        self.upsert_index_from_session_inner(&session, rel_path, false, Some(false))
            .await?;
        self.search_index_queue
            .enqueue_upsert(&session, revision_path, revision);
        Ok(true)
    }

    pub async fn cleanup(&self, mode: CleanupMode, keep_pinned: bool) -> io::Result<CleanupResult> {
        let _lifecycle = self.lock_session_lifecycle_exclusive().await?;
        let _runtime_task = self.lock_runtime_task_transaction_exclusive().await?;
        self.recover_all_runtime_task_transactions_locked().await?;
        self.recover_all_session_copy_transactions_locked().await?;

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
            let _ = self.delete_session_recursive_locked(root_id, true).await?;
        }
        for child_id in delete_child_ids.iter() {
            let _ = self.delete_session_recursive_locked(child_id, true).await?;
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
        let _runtime_task = self.lock_runtime_task_transaction_exclusive().await?;
        self.recover_all_runtime_task_transactions_locked().await?;
        self.recover_all_session_copy_transactions_locked().await?;

        let deleted_search_sources = self
            .index
            .read()
            .await
            .sessions
            .values()
            .map(|entry| {
                (
                    entry.id.clone(),
                    self.abs_path_from_rel(&entry.rel_path)
                        .join(SEARCH_INDEX_REVISION_FILE),
                )
            })
            .collect::<Vec<_>>();

        // Remove the sessions directory entirely.
        let _ = fs::remove_dir_all(&self.sessions_dir).await;
        fs::create_dir_all(&self.sessions_dir).await?;

        // Reset through the same cross-process rebase/publish boundary as every
        // other index mutation; dev reset must not race a stale direct writer.
        self.update_index(|index| {
            *index = SessionsIndex::empty();
            Ok(())
        })
        .await?;
        for (session_id, revision_path) in deleted_search_sources {
            self.search_index_queue
                .enqueue_delete(&session_id, revision_path);
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
        let _lifecycle = self.lock_session_lifecycle_exclusive().await?;
        let _runtime_task = self.lock_runtime_task_transaction_exclusive().await?;
        self.recover_all_runtime_task_transactions_locked().await?;
        self.recover_all_session_copy_transactions_locked().await?;
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
                self.search_index_queue
                    .enqueue_delete(session_id, abs_dir.join(SEARCH_INDEX_REVISION_FILE));
                Ok(true)
            }
            SessionKind::Root => {
                let root_id = entry.id.clone();
                let abs_dir = self.abs_path_from_rel(&entry.rel_path);
                let _ = fs::remove_dir_all(&abs_dir).await;

                let to_remove = {
                    let index = self.index.read().await;
                    index
                        .sessions
                        .values()
                        .filter(|e| e.root_session_id == root_id)
                        .map(|entry| {
                            (
                                entry.id.clone(),
                                self.abs_path_from_rel(&entry.rel_path)
                                    .join(SEARCH_INDEX_REVISION_FILE),
                            )
                        })
                        .collect::<Vec<_>>()
                };

                self.update_index(|index| {
                    for (id, _) in &to_remove {
                        index.sessions.remove(id);
                    }
                    Ok(())
                })
                .await?;

                for (id, revision_path) in to_remove {
                    self.search_index_queue.enqueue_delete(&id, revision_path);
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
async fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || std::fs::File::open(path)?.sync_all())
            .await
            .map_err(|error| other_io_error(format!("join directory sync task: {error}")))?
    }
    #[cfg(windows)]
    {
        let _ = path;
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Ok(())
    }
}

async fn sync_parent_directory_entry(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))?;
    sync_directory(parent).await
}

async fn durable_copy_file(source: &Path, target: &Path) -> io::Result<u64> {
    let copied = fs::copy(source, target).await?;
    fs::File::open(target).await?.sync_all().await?;
    Ok(copied)
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

impl SessionStoreV2 {
    async fn load_session_unlocked(&self, session_id: &str) -> io::Result<Option<Session>> {
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
        supervisor::validate_overlay(&session, sidecar.as_ref())?;
        let mut session = overlay_runtime_sidecar(session, sidecar);
        // Drop a stale pre-#180 Root token_budget cache so it re-resolves (#230).
        session.clear_stale_root_token_budget();
        Ok(Some(session))
    }
}

#[async_trait::async_trait]
impl Storage for SessionStoreV2 {
    async fn get_or_create_default_supervisor(
        &self,
        initial_model: &str,
    ) -> io::Result<SupervisorBootstrapReceipt> {
        self.bootstrap_default_supervisor(initial_model).await
    }

    async fn load_root_authority(&self, session_id: &str) -> io::Result<Option<Session>> {
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _task = self.lock_runtime_task_sidecar_shared().await?;
        let _session = self.acquire_session_maintenance_lock(session_id).await?;
        self.load_root_authority_unchecked(session_id).await
    }

    async fn save_session(&self, session: &Session) -> io::Result<()> {
        let total_started = Instant::now();
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
        let _session_write = self
            .acquire_session_write_lock(&session.id, SaveKind::Full)
            .await?;
        self.validate_authority_for_save(session).await?;
        self.reject_regressing_runtime_task(session).await?;

        let mut stages = SaveStageDurations::default();
        let directory_started = Instant::now();
        let rel_path = self.ensure_session_dirs(session).await?;
        let abs_dir = self.abs_path_from_rel(&rel_path);
        let path = abs_dir.join("session.json");
        stages.directory_preparation = directory_started.elapsed();

        // Refresh the runtime sidecar BEFORE session.json. If the process
        // crashes between the two writes, the sidecar then carries a
        // control-plane that is at least as fresh as session.json, and the
        // load-time overlay (sidecar wins for non-message fields) stays correct.
        // Writing session.json first could leave a stale sidecar that silently
        // reverts the just-saved control-plane on the next load.
        let serialization_started = Instant::now();
        let runtime_snapshot = runtime_sidecar_snapshot(session);
        let runtime_bytes = serde_json::to_vec_pretty(&runtime_snapshot)
            .map_err(|error| other_io_error(error.to_string()))?;
        let session_bytes =
            serde_json::to_vec_pretty(session).map_err(|e| other_io_error(e.to_string()))?;
        stages.serialization = serialization_started.elapsed();
        let serialized_bytes = runtime_bytes.len().saturating_add(session_bytes.len());

        #[cfg(any(test, feature = "test-utils"))]
        self.maybe_pause_full_save_before_filesystem_commit(&session.id)
            .await;

        let filesystem_started = Instant::now();
        durable_atomic_write(&abs_dir.join(RUNTIME_SIDECAR_FILE), &runtime_bytes).await?;
        durable_atomic_write(&path, &session_bytes).await?;
        let (revision_path, revision) = self.publish_search_revision(&abs_dir).await?;
        stages.filesystem_commit = filesystem_started.elapsed();

        let index_started = Instant::now();
        self.upsert_index_from_session(session, rel_path).await?;
        stages.index_publication = index_started.elapsed();

        let enqueue_started = Instant::now();
        let search_generation =
            self.search_index_queue
                .enqueue_upsert(session, revision_path, revision);
        stages.search_enqueue = enqueue_started.elapsed();
        let index_entry_count = self.index.read().await.sessions.len();
        let total = total_started.elapsed();
        self.persistence_metrics.record_save(
            SaveKind::Full,
            total,
            stages,
            serialized_bytes,
            session.messages.len(),
            index_entry_count,
        );
        tracing::debug!(
            target: "bamboo.session_persistence",
            session_id = %session.id,
            save_type = "full",
            phase = "durable_commit",
            serialized_bytes,
            message_count = session.messages.len(),
            index_entry_count,
            search_generation,
            total_ms = total.as_millis() as u64,
            "session durable commit completed; search indexing deferred"
        );
        Ok(())
    }

    async fn load_session(&self, session_id: &str) -> io::Result<Option<Session>> {
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
        self.load_session_unlocked(session_id).await
    }

    async fn delete_session(&self, session_id: &str) -> io::Result<bool> {
        // Historical API deletes sessions. In V2, treat this as recursive and forced.
        self.delete_session_recursive(session_id, true).await
    }

    async fn save_runtime_state(&self, session: &Session) -> io::Result<()> {
        // Fast path: write ONLY the small runtime sidecar (no messages), leaving
        // session.json — which carries the full conversation history — untouched.
        // Ordinary sessions retain O(1) I/O in conversation length. Supervisor
        // validation additionally reads main-file bytes to verify its identity.
        let Some(rel) = self.resolve_rel_path(&session.id).await else {
            // Session was never fully persisted yet — fall back to a full save so
            // session.json and the index get created. Deliberately acquire no
            // shared Task guard before this call: `save_session` owns that
            // boundary, avoiding a same-instance shared-lock re-entry.
            return self.save_session(session).await;
        };
        let total_started = Instant::now();
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
        let _session_write = self
            .acquire_session_write_lock(&session.id, SaveKind::Runtime)
            .await?;
        self.validate_authority_for_save(session).await?;
        self.reject_regressing_runtime_task(session).await?;
        let abs_dir = self.abs_path_from_rel(&rel);
        let mut stages = SaveStageDurations::default();
        let serialization_started = Instant::now();
        let runtime_snapshot = runtime_sidecar_snapshot(session);
        let runtime_bytes = serde_json::to_vec_pretty(&runtime_snapshot)
            .map_err(|error| other_io_error(error.to_string()))?;
        stages.serialization = serialization_started.elapsed();
        let filesystem_started = Instant::now();
        atomic_write(&abs_dir.join(RUNTIME_SIDECAR_FILE), &runtime_bytes).await?;
        stages.filesystem_commit = filesystem_started.elapsed();

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
            let index_started = Instant::now();
            self.update_index(|index| {
                if let Some(entry) = index.sessions.get_mut(&session.id) {
                    entry.workspace_path = workspace_path;
                    entry.project_id = project_id;
                }
                Ok(())
            })
            .await?;
            stages.index_publication = index_started.elapsed();
        }
        let index_entry_count = self.index.read().await.sessions.len();
        let total = total_started.elapsed();
        self.persistence_metrics.record_save(
            SaveKind::Runtime,
            total,
            stages,
            runtime_bytes.len(),
            session.messages.len(),
            index_entry_count,
        );
        tracing::debug!(
            target: "bamboo.session_persistence",
            session_id = %session.id,
            save_type = "runtime",
            phase = "durable_commit",
            serialized_bytes = runtime_bytes.len(),
            message_count = session.messages.len(),
            index_entry_count,
            total_ms = total.as_millis() as u64,
            "session runtime-state commit completed"
        );
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
        ImageOcrResult, ImageUrlRef, Message, MessagePart, SessionInboxError, SessionInboxPort,
        SessionMessageEnvelope, TaskItem, TaskItemStatus,
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
        let mut s = session_with_history("sc-1", 2, "run-A");
        let assistant = Message::assistant("discovery", None);
        let anchor = assistant.id.clone();
        s.add_message(assistant);
        let item = bamboo_domain::ProviderTranscriptItem::try_from_payload(
            bamboo_domain::ProviderFamily::OpenAi,
            bamboo_domain::ProviderProtocol::OpenAiResponsesV1,
            bamboo_domain::ProviderTranscriptOrigin::Provider,
            bamboo_domain::ProviderTranscriptAuthor::Model,
            serde_json::json!({
                "type":"tool_search_call","id":"tsc_search_sidecar","execution":"client","call_id":"search_sidecar",
                "status":"completed","arguments":{"query":"weather"}
            }),
        )
        .unwrap();
        s.append_provider_transcript_group(&anchor, None, vec![item])
            .unwrap();
        storage.save_session(&s).await?;

        let sidecar_path = storage.runtime_json_path("sc-1").await?.unwrap();
        assert!(
            sidecar_path.exists(),
            "save_session must write runtime.json"
        );

        // Sidecar must NOT carry the message history.
        let side = storage.read_runtime_sidecar("sc-1").await?.unwrap();
        assert!(side.messages.is_empty(), "sidecar messages must be cleared");
        assert!(side.provider_transcript.is_empty());
        assert_eq!(side.agent_runtime_state.as_ref().unwrap().run_id, "run-A");
        let loaded = storage.load_session("sc-1").await?.unwrap();
        assert_eq!(loaded.provider_transcript.groups().len(), 1);
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
    async fn paused_full_save_does_not_block_unrelated_full_save() -> io::Result<()> {
        let temp = TempDir::new().map_err(io::Error::other)?;
        let storage = Arc::new(SessionStoreV2::new(temp.path().to_path_buf()).await?);
        let paused = session_with_history("convoy-full-a", 2, "run-a");
        let unrelated = session_with_history("convoy-full-b", 2, "run-b");
        let (reached, release) =
            storage.pause_full_save_before_filesystem_commit_for_test(&paused.id);

        let paused_store = storage.clone();
        let paused_save = tokio::spawn(async move { paused_store.save_session(&paused).await });
        reached.wait().await;

        let unrelated_store = storage.clone();
        let unrelated_save =
            tokio::spawn(async move { unrelated_store.save_session(&unrelated).await });
        tokio::time::timeout(Duration::from_secs(2), unrelated_save)
            .await
            .expect("unrelated full save must not wait for paused session")
            .expect("unrelated full-save task")?;

        release.wait().await;
        paused_save.await.expect("paused full-save task")?;
        assert!(storage.session_write_locks.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn paused_full_save_does_not_block_unrelated_runtime_save() -> io::Result<()> {
        let temp = TempDir::new().map_err(io::Error::other)?;
        let storage = Arc::new(SessionStoreV2::new(temp.path().to_path_buf()).await?);
        let paused = session_with_history("convoy-runtime-a", 2, "run-a");
        let mut unrelated = session_with_history("convoy-runtime-b", 2, "run-b");
        storage.save_session(&unrelated).await?;
        unrelated.agent_runtime_state = Some(AgentRuntimeState::new("run-b-next"));
        let (reached, release) =
            storage.pause_full_save_before_filesystem_commit_for_test(&paused.id);

        let paused_store = storage.clone();
        let paused_save = tokio::spawn(async move { paused_store.save_session(&paused).await });
        reached.wait().await;

        let unrelated_store = storage.clone();
        let unrelated_save =
            tokio::spawn(async move { unrelated_store.save_runtime_state(&unrelated).await });
        tokio::time::timeout(Duration::from_secs(2), unrelated_save)
            .await
            .expect("unrelated runtime save must not wait for paused session")
            .expect("unrelated runtime-save task")?;

        release.wait().await;
        paused_save.await.expect("paused full-save task")?;
        assert!(storage.session_write_locks.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn paused_full_save_serializes_same_session_runtime_save() -> io::Result<()> {
        let temp = TempDir::new().map_err(io::Error::other)?;
        let storage = Arc::new(SessionStoreV2::new(temp.path().to_path_buf()).await?);
        let paused = session_with_history("same-session-order", 2, "run-a");
        storage.save_session(&paused).await?;
        let mut runtime = paused.clone();
        runtime.agent_runtime_state = Some(AgentRuntimeState::new("run-after-full"));
        let (reached, release) =
            storage.pause_full_save_before_filesystem_commit_for_test(&paused.id);

        let paused_store = storage.clone();
        let paused_save = tokio::spawn(async move { paused_store.save_session(&paused).await });
        reached.wait().await;

        let runtime_store = storage.clone();
        let mut runtime_save =
            tokio::spawn(async move { runtime_store.save_runtime_state(&runtime).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut runtime_save)
                .await
                .is_err(),
            "same-session runtime save must wait for the paused full save"
        );

        release.wait().await;
        paused_save.await.expect("paused full-save task")?;
        runtime_save.await.expect("runtime-save task")?;
        let loaded = storage
            .load_session("same-session-order")
            .await?
            .expect("session remains readable");
        assert_eq!(
            loaded
                .agent_runtime_state
                .as_ref()
                .map(|state| state.run_id.as_str()),
            Some("run-after-full")
        );
        assert!(storage.session_write_locks.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_last_waiter_reclaims_session_write_locks() -> io::Result<()> {
        let temp = TempDir::new()?;
        let store = SessionStoreV2::new(temp.path().to_path_buf()).await?;
        for index in 0..512 {
            let id = format!("cancelled-child-{index}");
            let held = store.acquire_session_maintenance_lock(&id).await?;
            let mut waiter = Box::pin(store.acquire_session_write_lock(&id, SaveKind::Runtime));
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(
                    std::future::Future::poll(waiter.as_mut(), cx).is_pending()
                ))
                .await
            );
            drop(held);
            drop(waiter);
        }
        assert!(store.session_write_locks.is_empty());
        assert_eq!(
            store
                .persistence_metrics
                .waiting_saves
                .load(Ordering::Relaxed),
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn independent_stores_serialize_same_session_without_blocking_other_ids() -> io::Result<()>
    {
        let temp = TempDir::new().map_err(io::Error::other)?;
        let first = Arc::new(SessionStoreV2::new(temp.path().to_path_buf()).await?);
        let shared = session_with_history("cross-store-shared", 2, "run-a");
        first.save_session(&shared).await?;
        let second = Arc::new(SessionStoreV2::new(temp.path().to_path_buf()).await?);

        let unrelated = session_with_history("cross-store-unrelated", 2, "run-b");
        let mut runtime = shared.clone();
        runtime.agent_runtime_state = Some(AgentRuntimeState::new("run-after-cross-store-full"));
        let (reached, release) =
            first.pause_full_save_before_filesystem_commit_for_test(&shared.id);

        let first_save_store = first.clone();
        let paused = shared.clone();
        let first_save = tokio::spawn(async move { first_save_store.save_session(&paused).await });
        reached.wait().await;

        let unrelated_store = second.clone();
        let unrelated_save =
            tokio::spawn(async move { unrelated_store.save_session(&unrelated).await });
        tokio::time::timeout(Duration::from_secs(2), unrelated_save)
            .await
            .expect("another store must persist an unrelated id independently")
            .expect("unrelated cross-store save task")?;

        let cancelled_store = second.clone();
        let cancelled_runtime = runtime.clone();
        let cancelled =
            tokio::spawn(
                async move { cancelled_store.save_runtime_state(&cancelled_runtime).await },
            );
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancelled.abort();
        assert!(cancelled.await.is_err(), "contended save must be cancelled");
        assert_eq!(second.persistence_metrics().waiting_saves, 0);
        assert!(
            second.session_write_locks.is_empty(),
            "cancellation while the file lock is contended must reclaim the local lock entry"
        );

        let runtime_store = second.clone();
        let mut runtime_save =
            tokio::spawn(async move { runtime_store.save_runtime_state(&runtime).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut runtime_save)
                .await
                .is_err(),
            "the cross-process session lock must serialize the same id"
        );

        release.wait().await;
        first_save.await.expect("cross-store full-save task")?;
        runtime_save.await.expect("cross-store runtime-save task")?;
        let loaded = second
            .load_session("cross-store-shared")
            .await?
            .expect("shared session remains readable");
        assert_eq!(
            loaded
                .agent_runtime_state
                .as_ref()
                .map(|state| state.run_id.as_str()),
            Some("run-after-cross-store-full")
        );
        assert!(first.session_write_locks.is_empty());
        assert!(second.session_write_locks.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn deferred_search_index_coalesces_to_latest_session_and_delete() -> io::Result<()> {
        let (storage, _temp) = create_temp_storage().await?;
        let mut session = session_with_history("search-generation", 1, "run-a");
        session.title = "old queued title".to_string();
        storage.save_session(&session).await?;
        let stale_session = session.clone();
        let revision_path = storage
            .sessions_root_dir()
            .join(&session.id)
            .join(SEARCH_INDEX_REVISION_FILE);
        let stale_revision = fs::read_to_string(&revision_path).await?;
        session.title = "newest queued title".to_string();
        session.updated_at = Utc::now() + chrono::Duration::milliseconds(1);
        storage.save_session(&session).await?;
        storage.flush_search_index().await;

        storage
            .search_index()
            .upsert_session_if_current(&stale_session, &revision_path, &stale_revision)
            .await?;

        let newest = storage.search_index().search("newest", 10).await?;
        assert!(newest
            .iter()
            .any(|entry| entry.session_id == "search-generation"));
        let stale = storage.search_index().search("old queued", 10).await?;
        assert!(stale
            .iter()
            .all(|entry| entry.session_id != "search-generation"));

        let current_revision = fs::read_to_string(&revision_path).await?;
        assert!(
            storage
                .delete_session_recursive("search-generation", true)
                .await?
        );
        storage.flush_search_index().await;
        storage
            .search_index()
            .upsert_session_if_current(&session, &revision_path, &current_revision)
            .await?;
        let deleted = storage.search_index().search("newest", 10).await?;
        assert!(deleted
            .iter()
            .all(|entry| entry.session_id != "search-generation"));

        session.title = "recreated searchable title".to_string();
        session.updated_at = Utc::now() + chrono::Duration::milliseconds(2);
        storage.save_session(&session).await?;
        storage.flush_search_index().await;
        storage
            .search_index()
            .delete_session_if_source_missing(&session.id, &revision_path)
            .await?;
        let recreated = storage.search_index().search("recreated", 10).await?;
        assert!(recreated
            .iter()
            .any(|entry| entry.session_id == "search-generation"));
        Ok(())
    }

    #[tokio::test]
    async fn search_rebuild_repairs_unchanged_snapshots_without_replacing_message_ids(
    ) -> io::Result<()> {
        let (storage, temp) = create_temp_storage().await?;
        let mut session = session_with_history("search-repair", 2, "run-a");
        session.title = "repairable beacon".into();
        session.messages[0].content = "repairable quartz".into();
        storage.save_session(&session).await?;
        storage.flush_search_index().await;
        let conn = rusqlite::Connection::open(storage.search_index().db_path()).unwrap();
        let identities = |conn: &rusqlite::Connection| {
            conn.prepare(
                "SELECT message_id, search_rowid FROM session_messages_search ORDER BY message_id",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
        };
        let before = identities(&conn);
        conn.execute_batch(
            "DELETE FROM sessions_search_fts;
            UPDATE session_messages_search_fts SET content='stale payload';",
        )
        .unwrap();
        drop(storage);

        // Reopen the store and run the same rebuild used by server startup.
        let reopened = SessionStoreV2::new(temp.path().to_path_buf()).await?;
        reopened.rebuild_search_index().await?;
        assert_eq!(identities(&conn), before);
        assert_eq!(reopened.search_index().search("beacon", 10).await?.len(), 1);
        assert_eq!(reopened.search_index().search("quartz", 10).await?.len(), 1);
        assert!(reopened
            .search_index()
            .search("stale", 10)
            .await?
            .is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn persistence_metrics_report_bounded_create_latency_percentiles() -> io::Result<()> {
        let (storage, _temp) = create_temp_storage().await?;
        for millis in [10, 20, 30, 40, 50] {
            storage.record_session_create_latency(Duration::from_millis(millis));
        }
        let metrics = storage.persistence_metrics();
        assert_eq!(metrics.create_latency.count, 5);
        assert_eq!(metrics.create_latency.p50_ms, 30);
        assert_eq!(metrics.create_latency.p95_ms, 50);
        assert_eq!(metrics.create_latency.max_ms, 50);
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
    async fn copy_session_creates_independent_root_and_copies_attachments() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let root = Session::new("root-source", "model");
        storage.save_session(&root).await?;
        let mut source = Session::new_child("child-source", &root.id, "gpt-5", "Research");
        source.pinned = true;
        source.model_ref = Some(ProviderModelRef::new("openai-instance", "gpt-5"));
        source.reasoning_effort = Some(ReasoningEffort::High);
        source.set_project_id_meta("project-1");
        source.set_workspace_path_meta("/workspaces/project-1");
        source.set_provider_name("openai-instance");
        source
            .metadata
            .insert("base_system_prompt".into(), "You are exact.".into());
        source.set_pending_question(
            "call-1".into(),
            "Question".into(),
            "Proceed?".into(),
            vec![],
            true,
        );
        source.set_last_run_status("running");
        source.set_last_run_error("old failure");
        source.set_pending_injected_messages(vec![serde_json::json!({"content":"queued"})]);
        source
            .metadata
            .insert("gold_config".into(), r#"{"goal":"ship"}"#.into());
        source
            .metadata
            .insert("a2a.context_id".into(), "remote-context".into());
        source
            .metadata
            .insert("created_by_schedule_id".into(), "schedule-1".into());
        source
            .metadata
            .insert("assignment_prompt".into(), "child-only".into());
        source
            .metadata
            .insert("goal.state".into(), r#"{"status":"active"}"#.into());
        source
            .metadata
            .insert("permission.audit_revision".into(), "42".into());
        source
            .metadata
            .insert("runtime_prompt_snapshot".into(), "stale".into());
        source
            .metadata
            .insert("workflow.run_ids.v1".into(), "[]".into());
        source.metadata.insert(
            "workflow.selection.v1".into(),
            r#"{"id":"review","source":"user","revision":1,"args":{}}"#.into(),
        );
        source
            .metadata
            .insert("workflow.orchestration_opt_in".into(), "true".into());
        source
            .metadata
            .insert("workflow.active.v1".into(), "stale-active".into());
        source.metadata.insert(
            "workflow.active.snapshot.v1".into(),
            "durable-snapshot".into(),
        );
        source
            .metadata
            .insert("workflow.activation_event.v1".into(), "stale-event".into());
        source.metadata.insert(
            "skill_runtime_selected_skill_ids".into(),
            r#"["stale"]"#.into(),
        );
        source
            .metadata
            .insert("selected_skill_ids".into(), r#"["review"]"#.into());
        source.metadata.insert("skill_mode".into(), "code".into());
        source
            .metadata
            .insert("workspace_source".into(), "project_default".into());
        source.metadata.insert(
            "execute.pending_turn_message_id".into(),
            "message-old".into(),
        );
        source
            .metadata
            .insert("context_pressure_last_level".into(), "high".into());
        source
            .metadata
            .insert("permission.reexecute_tool_call_id".into(), "call-1".into());
        source.metadata.insert(
            "permission.reexecute_request_generation".into(),
            "generation-call-1".into(),
        );
        source
            .metadata
            .insert("prefix_cache_section_state".into(), "stale".into());
        source
            .metadata
            .insert("llm_request_render".into(), "stale".into());
        source
            .metadata
            .insert("custom.copy_config".into(), "preserve-me".into());
        source.task_list = Some(transaction_task_list(&source.id, "Source tasks"));
        source.set_task_list_version_meta("7");
        source.token_budget = Some(bamboo_domain::TokenBudget::for_model(1024));
        source.token_usage = Some(TokenBudgetUsage {
            system_tokens: 1,
            summary_tokens: 2,
            window_tokens: 3,
            total_tokens: 6,
            max_context_tokens: 1024,
            budget_limit: 900,
            truncation_occurred: false,
            segments_removed: 0,
            prompt_cached_tool_outputs: 0,
            prompt_cached_tool_tokens_saved: 0,
            thinking_tokens: 0,
            cache_read_input_tokens: 0,
        });
        source.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState::new("run-1"));
        source
            .agent_runtime_state
            .as_mut()
            .unwrap()
            .set_permission_mode(bamboo_domain::SessionPermissionMode::Auto);
        storage.save_session(&source).await?;

        let (attachment_id, attachment_url) = storage
            .write_image_attachment(&source, "data:image/png;base64,aGVsbG8=", Some("image/png"))
            .await?;
        let mut message = Message::user_with_parts(
            "look",
            vec![MessagePart::ImageUrl {
                image_url: ImageUrlRef {
                    url: attachment_url.clone(),
                    detail: None,
                },
            }],
        );
        message.image_ocr = Some(vec![ImageOcrResult {
            image_url: attachment_url,
            lines: vec![],
            error: None,
        }]);
        source.add_message(message);
        storage.save_session(&source).await?;
        let source_before = serde_json::to_value(&source).expect("serialize source snapshot");

        let copied = storage
            .copy_session(&source.id, "copy-root")
            .await?
            .expect("source exists");
        assert_eq!(copied.kind, SessionKind::Root);
        assert_eq!(copied.id, "copy-root");
        assert_eq!(copied.root_session_id, "copy-root");
        assert!(copied.parent_session_id.is_none());
        assert_eq!(copied.spawn_depth, 0);
        assert_eq!(copied.title, "Research (copy)");
        assert!(copied.title_generated);
        assert!(!copied.pinned);
        assert_eq!(copied.model_ref, source.model_ref);
        assert_eq!(copied.project_id_meta().as_deref(), Some("project-1"));
        assert_eq!(
            copied.workspace_path_meta().as_deref(),
            Some("/workspaces/project-1")
        );
        assert_eq!(copied.provider_name().as_deref(), Some("openai-instance"));
        assert_eq!(copied.reasoning_effort, Some(ReasoningEffort::High));
        assert_eq!(
            copied
                .metadata
                .get("base_system_prompt")
                .map(String::as_str),
            Some("You are exact.")
        );
        assert_eq!(
            copied.metadata.get("gold_config").map(String::as_str),
            Some(r#"{"goal":"ship"}"#)
        );
        assert!(!copied.has_pending_question());
        assert!(copied.last_run_status().is_none());
        assert!(copied.last_run_error().is_none());
        assert!(!copied.has_pending_injected_messages());
        assert!(!copied.metadata.contains_key("a2a.context_id"));
        assert!(!copied.metadata.contains_key("created_by_schedule_id"));
        assert!(!copied.metadata.contains_key("assignment_prompt"));
        assert!(!copied.metadata.contains_key("goal.state"));
        assert!(!copied.metadata.contains_key("permission.audit_revision"));
        assert!(!copied.metadata.contains_key("runtime_prompt_snapshot"));
        assert!(!copied.metadata.contains_key("workflow.run_ids.v1"));
        assert_eq!(
            copied.metadata.get("workflow.active.v1"),
            Some(&"stale-active".to_string())
        );
        assert_eq!(
            copied.metadata.get("workflow.active.snapshot.v1"),
            Some(&"durable-snapshot".to_string())
        );
        assert!(!copied.metadata.contains_key("workflow.activation_event.v1"));
        assert_eq!(
            copied.metadata.get("workflow.selection.v1"),
            source.metadata.get("workflow.selection.v1")
        );
        assert_eq!(
            copied.metadata.get("workflow.orchestration_opt_in"),
            Some(&"true".to_string())
        );
        assert!(!copied
            .metadata
            .contains_key("skill_runtime_selected_skill_ids"));
        assert_eq!(
            copied.metadata.get("selected_skill_ids"),
            Some(&r#"["review"]"#.to_string())
        );
        assert_eq!(copied.metadata.get("skill_mode"), Some(&"code".to_string()));
        assert_eq!(
            copied.metadata.get("workspace_source"),
            Some(&"project_default".to_string())
        );
        for key in [
            "execute.pending_turn_message_id",
            "context_pressure_last_level",
            "permission.reexecute_tool_call_id",
            "permission.reexecute_request_generation",
            "prefix_cache_section_state",
            "llm_request_render",
        ] {
            assert!(!copied.metadata.contains_key(key), "{key} must be cleared");
        }
        assert_eq!(
            copied.metadata.get("custom.copy_config"),
            Some(&"preserve-me".to_string())
        );
        assert!(copied.token_budget.is_none());
        assert!(copied.token_usage.is_none());
        assert!(copied.task_list.is_none());
        assert!(copied.task_list_version_meta().is_none());
        assert!(copied.prompt_snapshot.is_none());
        assert!(copied.model_context_state.is_none());
        assert_eq!(
            copied
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .effective_permission_mode(),
            bamboo_domain::SessionPermissionMode::Auto
        );
        assert!(copied
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .run_id
            .is_empty());
        let copied_url = match &copied
            .messages
            .last()
            .unwrap()
            .content_parts
            .as_ref()
            .unwrap()[0]
        {
            MessagePart::ImageUrl { image_url } => image_url.url.as_str(),
            _ => panic!("expected image"),
        };
        assert_eq!(
            copied_url,
            format!("bamboo-attachment://copy-root/{attachment_id}")
        );
        assert_eq!(
            copied.messages.last().unwrap().image_ocr.as_ref().unwrap()[0].image_url,
            format!("bamboo-attachment://copy-root/{attachment_id}")
        );
        assert_eq!(
            storage
                .read_attachment("copy-root", &attachment_id)
                .await?
                .expect("copied attachment")
                .0,
            b"hello"
        );
        let source_after_copy = storage
            .load_session(&source.id)
            .await?
            .expect("source remains after copy");
        assert_eq!(
            serde_json::to_value(source_after_copy).expect("serialize source after copy"),
            source_before
        );

        storage.delete_session(&source.id).await?;
        assert!(storage
            .read_attachment("copy-root", &attachment_id)
            .await?
            .is_some());
        assert!(storage.load_session("copy-root").await?.is_some());
        let parent_after = storage
            .load_session("root-source")
            .await?
            .expect("unrelated parent remains");
        assert_eq!(parent_after.id, root.id);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_copy_requests_create_independent_sessions() -> io::Result<()> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();
        let storage = Arc::new(SessionStoreV2::new(bamboo_home).await?);
        let mut source = Session::new("copy-source", "model");
        source.add_message(Message::user("same snapshot"));
        storage.save_session(&source).await?;

        let first_store = storage.clone();
        let second_store = storage.clone();
        let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(
                async move { first_store.copy_session("copy-source", "copy-one").await },
                async move { second_store.copy_session("copy-source", "copy-two").await }
            )
        })
        .await
        .expect("projection guards must be released before concurrent copy returns");
        let first = first?.expect("first copy");
        let second = second?.expect("second copy");
        assert_ne!(first.id, second.id);
        assert_eq!(first.messages.len(), source.messages.len());
        assert_eq!(second.messages.len(), source.messages.len());
        assert!(storage.get_index_entry(&first.id).await.is_some());
        assert!(storage.get_index_entry(&second.id).await.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn copy_session_missing_source_leaves_no_target() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        assert!(storage
            .copy_session("missing", "copy-root")
            .await?
            .is_none());
        assert!(storage.get_index_entry("copy-root").await.is_none());
        assert!(!storage.sessions_root_dir().join("copy-root").exists());
        Ok(())
    }

    #[tokio::test]
    async fn copy_session_rolls_back_directory_when_index_publish_fails() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let source = Session::new("copy-source", "model");
        storage.save_session(&source).await?;

        // Force the copy to fail after its fully-written staging directory has
        // been renamed, but before a visible index entry can be committed.
        fs::write(storage.index_path(), b"{not valid json").await?;
        let error = storage
            .copy_session(&source.id, "copy-root")
            .await
            .expect_err("corrupt index must fail publication");
        assert!(error.to_string().contains("invalid sessions.json"));
        assert!(storage.get_index_entry("copy-root").await.is_none());
        assert!(!storage.sessions_root_dir().join("copy-root").exists());
        assert_eq!(storage.session_copy_journal_paths().await?.len(), 1);
        let mut home_entries = fs::read_dir(storage.bamboo_home_dir()).await?;
        while let Some(entry) = home_entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().into_owned();
            assert!(
                !name.starts_with(".session-copy-") || name == SESSION_COPY_TRANSACTION_DIR,
                "staging directory leaked after rollback: {name}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn copy_session_rejects_corrupt_source_main_and_runtime_sidecar() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let source = Session::new("copy-source", "model");
        storage.save_session(&source).await?;
        let source_dir = storage.sessions_root_dir().join(&source.id);

        fs::write(source_dir.join("session.json"), b"not-json").await?;
        let error = storage
            .copy_session(&source.id, "copy-main-corrupt")
            .await
            .expect_err("corrupt authoritative source must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(storage.get_index_entry("copy-main-corrupt").await.is_none());

        storage.save_session(&source).await?;
        fs::write(source_dir.join(RUNTIME_SIDECAR_FILE), b"not-json").await?;
        let error = storage
            .copy_session(&source.id, "copy-sidecar-corrupt")
            .await
            .expect_err("corrupt source sidecar must fail instead of copying stale state");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(storage
            .get_index_entry("copy-sidecar-corrupt")
            .await
            .is_none());
        Ok(())
    }

    #[tokio::test]
    async fn copy_session_rejects_noncanonical_or_mismatched_index_identity() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let source = Session::new("copy-source", "model");
        storage.save_session(&source).await?;

        storage
            .update_index(|index| {
                index
                    .sessions
                    .get_mut(&source.id)
                    .expect("source index")
                    .rel_path = "sessions/another-root".to_string();
                Ok(())
            })
            .await?;
        let error = storage
            .copy_session(&source.id, "copy-target")
            .await
            .expect_err("noncanonical indexed source path must fail closed");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(storage.get_index_entry("copy-target").await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn copy_session_accepts_legacy_root_without_root_session_id() -> io::Result<()> {
        let (storage, _temp_dir) = create_temp_storage().await?;
        let source = Session::new("legacy-root", "model");
        storage.save_session(&source).await?;
        let source_dir = storage.sessions_root_dir().join(&source.id);

        for filename in ["session.json", RUNTIME_SIDECAR_FILE] {
            let path = source_dir.join(filename);
            let mut value: serde_json::Value =
                serde_json::from_slice(&fs::read(&path).await?).map_err(io::Error::other)?;
            value
                .as_object_mut()
                .expect("session snapshot object")
                .remove("root_session_id");
            fs::write(
                path,
                serde_json::to_vec_pretty(&value).map_err(io::Error::other)?,
            )
            .await?;
        }

        let copied = storage
            .copy_session(&source.id, "legacy-root-copy")
            .await?
            .expect("legacy root remains copyable");
        assert_eq!(copied.id, "legacy-root-copy");
        assert_eq!(copied.kind, SessionKind::Root);
        assert_eq!(copied.root_session_id, copied.id);
        assert!(storage.get_index_entry("legacy-root-copy").await.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn startup_rolls_back_prepared_session_copy_transaction() -> io::Result<()> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();
        let storage = SessionStoreV2::new(bamboo_home.clone()).await?;
        let source = Session::new("copy-source", "model");
        storage.save_session(&source).await?;
        let copied = storage
            .copy_session(&source.id, "copy-target")
            .await?
            .expect("copy succeeds");
        let journal = SessionCopyTransactionJournal {
            version: SESSION_COPY_TRANSACTION_VERSION,
            transaction_id: Uuid::new_v4().to_string(),
            source_id: source.id.clone(),
            target_id: copied.id.clone(),
        };
        storage.write_session_copy_journal(&journal).await?;
        drop(storage);

        let reopened = SessionStoreV2::new(bamboo_home).await?;
        assert!(reopened.get_index_entry(&copied.id).await.is_none());
        assert!(!reopened.sessions_root_dir().join(&copied.id).exists());
        assert!(reopened.session_copy_journal_paths().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn startup_finishes_committed_session_copy_transaction() -> io::Result<()> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();
        let storage = SessionStoreV2::new(bamboo_home.clone()).await?;
        let source = Session::new("copy-source", "model");
        storage.save_session(&source).await?;
        let copied = storage
            .copy_session(&source.id, "copy-target")
            .await?
            .expect("copy succeeds");
        let journal = SessionCopyTransactionJournal {
            version: SESSION_COPY_TRANSACTION_VERSION,
            transaction_id: Uuid::new_v4().to_string(),
            source_id: source.id.clone(),
            target_id: copied.id.clone(),
        };
        let prepared = storage.write_session_copy_journal(&journal).await?;
        let committed = prepared.with_extension("committed");
        atomic_rename(&prepared, &committed).await?;
        sync_parent_directory_entry(&committed).await?;
        storage
            .update_index(|index| {
                index.sessions.remove(&copied.id);
                Ok(())
            })
            .await?;
        drop(storage);

        let reopened = SessionStoreV2::new(bamboo_home).await?;
        assert!(reopened.get_index_entry(&copied.id).await.is_some());
        assert!(reopened.load_session(&copied.id).await?.is_some());
        assert!(reopened.session_copy_journal_paths().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn delete_recovers_retained_committed_copy_marker_before_removal() -> io::Result<()> {
        let temp_dir = TempDir::new().map_err(io::Error::other)?;
        let bamboo_home = temp_dir.path().to_path_buf();
        let storage = SessionStoreV2::new(bamboo_home.clone()).await?;
        let source = Session::new("copy-source", "model");
        storage.save_session(&source).await?;
        let copied = storage
            .copy_session(&source.id, "copy-target")
            .await?
            .expect("copy succeeds");

        // Model a successful copy whose post-commit marker deactivation was
        // interrupted. Deletion must settle that marker before removing the
        // target so restart cannot resurrect it or fail on a missing target.
        let journal = SessionCopyTransactionJournal {
            version: SESSION_COPY_TRANSACTION_VERSION,
            transaction_id: Uuid::new_v4().to_string(),
            source_id: source.id.clone(),
            target_id: copied.id.clone(),
        };
        let prepared = storage.write_session_copy_journal(&journal).await?;
        let committed = prepared.with_extension("committed");
        atomic_rename(&prepared, &committed).await?;
        sync_parent_directory_entry(&committed).await?;

        assert!(storage.delete_session(&copied.id).await?);
        assert!(storage.get_index_entry(&copied.id).await.is_none());
        assert!(!storage.sessions_root_dir().join(&copied.id).exists());
        assert!(storage.session_copy_journal_paths().await?.is_empty());
        drop(storage);

        let reopened = SessionStoreV2::new(bamboo_home).await?;
        assert!(reopened.get_index_entry(&copied.id).await.is_none());
        assert!(reopened.load_session(&copied.id).await?.is_none());
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
