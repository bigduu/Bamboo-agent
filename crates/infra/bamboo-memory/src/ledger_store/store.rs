use std::collections::HashSet;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use bamboo_domain::ledger::{LedgerRecord, LedgerScope, RecordKind, RecordStatus};
use bamboo_domain::TaskPriority;
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::Mutex;

use crate::atomic_fs::{atomic_write, atomic_write_batch};

use super::paths::LedgerPathResolver;
use super::{
    build_status_index, build_time_index, build_todo_markdown_view, parse_record_document,
    render_record_document, validate_record_id, validate_record_title, LedgerAuditEntry,
    LedgerRecordDocument, TimeIndex, AGENDA_VIEW_FILE, AUDIT_LOG_FILE, BY_STATUS_INDEX_FILE,
    BY_TIME_INDEX_FILE, TODO_VIEW_FILE,
};

/// Process-wide per-scope write locks, mirroring the memory store's
/// serialization discipline: every read-modify-write plus artifact refresh for
/// a scope happens under its lock.
fn scope_locks() -> &'static DashMap<PathBuf, Arc<Mutex<()>>> {
    static SCOPE_LOCKS: OnceLock<DashMap<PathBuf, Arc<Mutex<()>>>> = OnceLock::new();
    SCOPE_LOCKS.get_or_init(DashMap::new)
}

static RECORD_SEQ: AtomicU64 = AtomicU64::new(0);

/// A process-unique record id (`rec_<nanos-hex><seq>`); no uuid dependency
/// needed at human ledger volumes.
pub fn new_record_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = RECORD_SEQ.fetch_add(1, Ordering::Relaxed) & 0xfff;
    format!("rec_{nanos:x}{seq:03x}")
}

/// Filter for [`LedgerStore::list_records`]. Empty filter = every non-terminal
/// record.
#[derive(Debug, Clone, Default)]
pub struct RecordFilter {
    pub statuses: Option<HashSet<RecordStatus>>,
    pub kinds: Option<HashSet<RecordKind>>,
    pub tags: Vec<String>,
    pub parent_id: Option<String>,
    pub anchor_before: Option<DateTime<Utc>>,
    pub anchor_after: Option<DateTime<Utc>>,
    /// Include Done/Cancelled/Expired records when no explicit `statuses`
    /// filter is set.
    pub include_terminal: bool,
    pub limit: Option<usize>,
}

impl RecordFilter {
    fn matches(&self, record: &LedgerRecord) -> bool {
        if let Some(statuses) = &self.statuses {
            if !statuses.contains(&record.status) {
                return false;
            }
        } else if !self.include_terminal && record.status.is_terminal() {
            return false;
        }
        if let Some(kinds) = &self.kinds {
            if !kinds.contains(&record.kind) {
                return false;
            }
        }
        if let Some(parent_id) = &self.parent_id {
            if record.relations.parent_id.as_deref() != Some(parent_id.as_str()) {
                return false;
            }
        }
        if !self.tags.is_empty()
            && !self
                .tags
                .iter()
                .all(|tag| record.tags.iter().any(|have| have == tag))
        {
            return false;
        }
        let anchor = record.time_anchor();
        if let Some(before) = self.anchor_before {
            match anchor {
                Some(anchor) if anchor < before => {}
                _ => return false,
            }
        }
        if let Some(after) = self.anchor_after {
            match anchor {
                Some(anchor) if anchor >= after => {}
                _ => return false,
            }
        }
        true
    }
}

/// One agenda line: a record plus where it came from, so multi-scope agendas
/// (personal + current project) stay attributable.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgendaItem {
    pub id: String,
    pub scope: LedgerScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    pub kind: RecordKind,
    pub title: String,
    pub status: RecordStatus,
    pub priority: TaskPriority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub due_at: Option<DateTime<Utc>>,
}

/// Time-bucketed agenda: what the assistant leads with.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AgendaSnapshot {
    pub generated_at: DateTime<Utc>,
    /// Non-terminal records whose anchor is in the past.
    pub overdue: Vec<AgendaItem>,
    /// Anchored within the next 24 hours.
    pub today: Vec<AgendaItem>,
    /// Anchored after 24h and within the horizon.
    pub upcoming: Vec<AgendaItem>,
    /// Open records with no time anchor (top priorities first, capped).
    pub undated: Vec<AgendaItem>,
}

impl AgendaSnapshot {
    pub fn is_empty(&self) -> bool {
        self.overdue.is_empty()
            && self.today.is_empty()
            && self.upcoming.is_empty()
            && self.undated.is_empty()
    }
}

const UNDATED_AGENDA_CAP: usize = 10;

fn priority_rank(priority: &TaskPriority) -> u8 {
    match priority {
        TaskPriority::Critical => 3,
        TaskPriority::High => 2,
        TaskPriority::Medium => 1,
        TaskPriority::Low => 0,
    }
}

fn agenda_item(record: &LedgerRecord) -> AgendaItem {
    AgendaItem {
        id: record.id.clone(),
        scope: record.scope,
        project_key: record.project_key.clone(),
        kind: record.kind.clone(),
        title: record.title.clone(),
        status: record.status,
        priority: record.priority.clone(),
        anchor_at: record.time_anchor(),
        due_at: record.time.due_at,
    }
}

/// Bucket records into an agenda. Pure so views, the tool, and the prompt
/// layer all share one definition of "overdue"/"today".
pub fn build_agenda_snapshot(
    records: &[LedgerRecord],
    now: DateTime<Utc>,
    horizon_days: i64,
) -> AgendaSnapshot {
    let day_ahead = now + Duration::hours(24);
    let horizon = now + Duration::days(horizon_days.max(1));
    let mut snapshot = AgendaSnapshot {
        generated_at: now,
        ..AgendaSnapshot::default()
    };

    for record in records {
        if record.status.is_terminal() {
            continue;
        }
        match record.time_anchor() {
            Some(anchor) if anchor < now => snapshot.overdue.push(agenda_item(record)),
            Some(anchor) if anchor < day_ahead => snapshot.today.push(agenda_item(record)),
            Some(anchor) if anchor < horizon => snapshot.upcoming.push(agenda_item(record)),
            Some(_) => {}
            None => snapshot.undated.push(agenda_item(record)),
        }
    }

    let by_anchor = |left: &AgendaItem, right: &AgendaItem| {
        left.anchor_at
            .cmp(&right.anchor_at)
            .then_with(|| left.id.cmp(&right.id))
    };
    snapshot.overdue.sort_by(by_anchor);
    snapshot.today.sort_by(by_anchor);
    snapshot.upcoming.sort_by(by_anchor);
    snapshot.undated.sort_by(|left, right| {
        priority_rank(&right.priority)
            .cmp(&priority_rank(&left.priority))
            .then_with(|| left.id.cmp(&right.id))
    });
    snapshot.undated.truncate(UNDATED_AGENDA_CAP);
    snapshot
}

/// Render an agenda as markdown — used for `AGENDA.md` and reused by the
/// prompt-injection layer.
pub fn build_agenda_markdown(snapshot: &AgendaSnapshot) -> String {
    fn push_section(out: &mut String, heading: &str, items: &[AgendaItem]) {
        if items.is_empty() {
            return;
        }
        out.push_str(&format!("## {heading}\n"));
        for item in items {
            let when = item
                .anchor_at
                .map(|at| format!(" — {}", at.format("%Y-%m-%d %H:%M UTC")))
                .unwrap_or_default();
            out.push_str(&format!(
                "- `{}` [{}] {}{}\n",
                item.id,
                item.kind.as_str(),
                item.title,
                when,
            ));
        }
        out.push('\n');
    }

    let mut out = String::from("# Ledger Agenda\n\n");
    if snapshot.is_empty() {
        out.push_str("_(nothing scheduled or open)_\n");
        return out;
    }
    push_section(&mut out, "Overdue", &snapshot.overdue);
    push_section(&mut out, "Next 24 hours", &snapshot.today);
    push_section(&mut out, "Upcoming", &snapshot.upcoming);
    push_section(&mut out, "Open (no date)", &snapshot.undated);
    out
}

/// Persistence for ledger records under `{data_dir}/ledger/v1`.
#[derive(Debug, Clone)]
pub struct LedgerStore {
    resolver: LedgerPathResolver,
}

impl Default for LedgerStore {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl LedgerStore {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            resolver: LedgerPathResolver::from_data_dir(data_dir),
        }
    }

    pub fn with_defaults() -> Self {
        Self {
            resolver: LedgerPathResolver::default(),
        }
    }

    pub fn resolver(&self) -> &LedgerPathResolver {
        &self.resolver
    }

    fn scope_lock(&self, scope: LedgerScope, project_key: Option<&str>) -> Arc<Mutex<()>> {
        scope_locks()
            .entry(self.resolver.scope_root(scope, project_key))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Create or replace a record document. Preserves `created_at` (and the
    /// body, when `body` is `None`) of an existing record with the same id;
    /// always bumps `updated_at`.
    pub async fn write_record(
        &self,
        mut record: LedgerRecord,
        body: Option<String>,
    ) -> io::Result<LedgerRecordDocument> {
        validate_record_id(&record.id)?;
        validate_record_title(&record.title)?;
        if record.scope == LedgerScope::Project && record.project_key.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "project-scoped records require a project_key",
            ));
        }

        let scope = record.scope;
        let project_key = record.project_key.clone();
        let lock = self.scope_lock(scope, project_key.as_deref());
        let _guard = lock.lock().await;

        let path = self
            .resolver
            .record_path(scope, project_key.as_deref(), &record.id);
        let existing = self.load_record_at(&path).await?;
        let action = if existing.is_some() {
            "update"
        } else {
            "create"
        };
        let body = match (body, &existing) {
            (Some(body), _) => body,
            (None, Some(existing)) => existing.body.clone(),
            (None, None) => String::new(),
        };
        if let Some(existing) = &existing {
            record.created_at = existing.record.created_at;
        }
        record.updated_at = Utc::now();

        let rendered = render_record_document(&record, &body)?;
        atomic_write(&path, rendered.as_bytes()).await?;
        self.refresh_scope_artifacts(scope, project_key.as_deref())
            .await?;
        self.append_audit(
            scope,
            project_key.as_deref(),
            &record.id,
            action,
            &record.title,
        )
        .await?;

        Ok(LedgerRecordDocument { record, body, path })
    }

    pub async fn get_record(
        &self,
        scope: LedgerScope,
        project_key: Option<&str>,
        record_id: &str,
    ) -> io::Result<Option<LedgerRecordDocument>> {
        let record_id = validate_record_id(record_id)?;
        let path = self.resolver.record_path(scope, project_key, record_id);
        self.load_record_at(&path).await
    }

    /// Transition a record's status (recording history) and refresh artifacts.
    /// Returns the updated document, or `None` when either the record does not
    /// exist or the status is already the target (no-op).
    pub async fn transition_record(
        &self,
        scope: LedgerScope,
        project_key: Option<&str>,
        record_id: &str,
        status: RecordStatus,
        reason: Option<&str>,
    ) -> io::Result<Option<LedgerRecordDocument>> {
        let record_id = validate_record_id(record_id)?;
        let lock = self.scope_lock(scope, project_key);
        let _guard = lock.lock().await;

        let path = self.resolver.record_path(scope, project_key, record_id);
        let Some(mut doc) = self.load_record_at(&path).await? else {
            return Ok(None);
        };
        if !doc.record.transition_to(status, reason) {
            return Ok(None);
        }

        let rendered = render_record_document(&doc.record, &doc.body)?;
        atomic_write(&path, rendered.as_bytes()).await?;
        self.refresh_scope_artifacts(scope, project_key).await?;
        self.append_audit(
            scope,
            project_key,
            record_id,
            &format!("transition:{}", status.as_str()),
            reason.unwrap_or(""),
        )
        .await?;
        Ok(Some(doc))
    }

    /// Load and filter a scope's records, anchored records first (ascending),
    /// undated records after (most recently updated first).
    pub async fn list_records(
        &self,
        scope: LedgerScope,
        project_key: Option<&str>,
        filter: &RecordFilter,
    ) -> io::Result<Vec<LedgerRecordDocument>> {
        let mut docs: Vec<LedgerRecordDocument> = self
            .load_scope_records(scope, project_key)
            .await?
            .into_iter()
            .filter(|doc| filter.matches(&doc.record))
            .collect();
        docs.sort_by(|left, right| {
            match (left.record.time_anchor(), right.record.time_anchor()) {
                (Some(l), Some(r)) => l.cmp(&r),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => right.record.updated_at.cmp(&left.record.updated_at),
            }
            .then_with(|| left.record.id.cmp(&right.record.id))
        });
        if let Some(limit) = filter.limit {
            docs.truncate(limit);
        }
        Ok(docs)
    }

    /// Build a time-bucketed agenda across one or more scopes (typically the
    /// global scope plus the current project's).
    pub async fn agenda(
        &self,
        scopes: &[(LedgerScope, Option<String>)],
        now: DateTime<Utc>,
        horizon_days: i64,
    ) -> io::Result<AgendaSnapshot> {
        let mut records: Vec<LedgerRecord> = Vec::new();
        for (scope, project_key) in scopes {
            let docs = self
                .load_scope_records(*scope, project_key.as_deref())
                .await?;
            records.extend(docs.into_iter().map(|doc| doc.record));
        }
        Ok(build_agenda_snapshot(&records, now, horizon_days))
    }

    /// Cheap agenda read for the prompt layer: derived purely from
    /// `by_time.json` plus the undated slice of `by_status.json` would lose
    /// priority ordering, so today it re-reads records; scale is human-sized.
    pub async fn read_time_index(
        &self,
        scope: LedgerScope,
        project_key: Option<&str>,
    ) -> io::Result<Option<TimeIndex>> {
        let path = self
            .resolver
            .indexes_dir(scope, project_key)
            .join(BY_TIME_INDEX_FILE);
        match fs::read_to_string(&path).await {
            Ok(raw) => Ok(serde_json::from_str(&raw).ok()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Project keys that have a ledger scope directory on disk — the iteration
    /// surface for background maintenance across every scope.
    pub async fn list_project_keys(&self) -> io::Result<Vec<String>> {
        let dir = self.resolver.projects_root();
        let mut reader = match fs::read_dir(&dir).await {
            Ok(reader) => reader,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut keys = Vec::new();
        while let Some(entry) = reader.next_entry().await? {
            if entry
                .file_type()
                .await
                .map(|ft| ft.is_dir())
                .unwrap_or(false)
            {
                if let Some(name) = entry.file_name().to_str() {
                    keys.push(name.to_string());
                }
            }
        }
        keys.sort();
        Ok(keys)
    }

    /// Rebuild every derived artifact for a scope from the record documents.
    pub async fn rebuild_scope(
        &self,
        scope: LedgerScope,
        project_key: Option<&str>,
    ) -> io::Result<()> {
        let lock = self.scope_lock(scope, project_key);
        let _guard = lock.lock().await;
        self.refresh_scope_artifacts(scope, project_key).await
    }

    async fn load_record_at(&self, path: &PathBuf) -> io::Result<Option<LedgerRecordDocument>> {
        let raw = match fs::read_to_string(path).await {
            Ok(raw) => raw,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let (record, body) = parse_record_document(&raw)?;
        Ok(Some(LedgerRecordDocument {
            record,
            body,
            path: path.clone(),
        }))
    }

    async fn load_scope_records(
        &self,
        scope: LedgerScope,
        project_key: Option<&str>,
    ) -> io::Result<Vec<LedgerRecordDocument>> {
        let dir = self.resolver.records_dir(scope, project_key);
        let mut reader = match fs::read_dir(&dir).await {
            Ok(reader) => reader,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut docs = Vec::new();
        while let Some(entry) = reader.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("md") {
                continue;
            }
            match self.load_record_at(&path).await {
                Ok(Some(doc)) => docs.push(doc),
                Ok(None) => {}
                // A corrupt document must not take the whole ledger down; it
                // stays on disk for manual repair and is skipped here.
                Err(error) => {
                    tracing::warn!(path = %path.display(), error = %error, "skipping unreadable ledger record");
                }
            }
        }
        Ok(docs)
    }

    async fn refresh_scope_artifacts(
        &self,
        scope: LedgerScope,
        project_key: Option<&str>,
    ) -> io::Result<()> {
        let records: Vec<LedgerRecord> = self
            .load_scope_records(scope, project_key)
            .await?
            .into_iter()
            .map(|doc| doc.record)
            .collect();
        let now = Utc::now();

        let time_index = build_time_index(&records, now);
        let status_index = build_status_index(&records, now);
        let agenda = build_agenda_snapshot(&records, now, 7);

        let indexes_dir = self.resolver.indexes_dir(scope, project_key);
        let views_dir = self.resolver.views_dir(scope, project_key);
        atomic_write_batch(vec![
            (
                indexes_dir.join(BY_TIME_INDEX_FILE),
                serde_json::to_vec_pretty(&time_index)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
            ),
            (
                indexes_dir.join(BY_STATUS_INDEX_FILE),
                serde_json::to_vec_pretty(&status_index)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
            ),
            (
                views_dir.join(AGENDA_VIEW_FILE),
                build_agenda_markdown(&agenda).into_bytes(),
            ),
            (
                views_dir.join(TODO_VIEW_FILE),
                build_todo_markdown_view(&records).into_bytes(),
            ),
        ])
        .await
    }

    async fn append_audit(
        &self,
        scope: LedgerScope,
        project_key: Option<&str>,
        record_id: &str,
        action: &str,
        summary: &str,
    ) -> io::Result<()> {
        let entry = LedgerAuditEntry {
            timestamp: Utc::now().to_rfc3339(),
            action: action.to_string(),
            scope,
            project_key: project_key.map(ToOwned::to_owned),
            record_id: record_id.to_string(),
            summary: summary.chars().take(200).collect(),
        };
        let path = self
            .resolver
            .logs_dir(scope, project_key)
            .join(AUDIT_LOG_FILE);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut line = serde_json::to_string(&entry)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        line.push('\n');
        use tokio::io::AsyncWriteExt;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        file.write_all(line.as_bytes()).await?;
        file.sync_all().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::ledger::RecordKind;
    use chrono::TimeZone;
    use tempfile::tempdir;

    fn utc(y: i32, mo: u32, d: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, 0, 0).unwrap()
    }

    fn todo(id: &str, title: &str, due: Option<DateTime<Utc>>) -> LedgerRecord {
        let mut record = LedgerRecord::new(id, RecordKind::Todo, title);
        record.time.due_at = due;
        record
    }

    #[tokio::test]
    async fn write_get_transition_round_trip() {
        let dir = tempdir().unwrap();
        let store = LedgerStore::new(dir.path());

        let record = todo("rec_passport", "Renew passport", Some(utc(2026, 8, 1, 9)));
        let written = store
            .write_record(record, Some("Bring the old one.".to_string()))
            .await
            .unwrap();
        assert!(written
            .path
            .ends_with("scopes/global/records/rec_passport.md"));

        let fetched = store
            .get_record(LedgerScope::Global, None, "rec_passport")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.record.title, "Renew passport");
        assert_eq!(fetched.body, "Bring the old one.");

        let done = store
            .transition_record(
                LedgerScope::Global,
                None,
                "rec_passport",
                RecordStatus::Done,
                Some("picked it up"),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(done.record.status, RecordStatus::Done);
        assert_eq!(done.record.transitions.len(), 1);

        // No-op transition returns None and records nothing further.
        let noop = store
            .transition_record(
                LedgerScope::Global,
                None,
                "rec_passport",
                RecordStatus::Done,
                None,
            )
            .await
            .unwrap();
        assert!(noop.is_none());
    }

    #[tokio::test]
    async fn update_preserves_created_at_and_body() {
        let dir = tempdir().unwrap();
        let store = LedgerStore::new(dir.path());

        let first = store
            .write_record(
                todo("rec_1", "Original", None),
                Some("original body".to_string()),
            )
            .await
            .unwrap();

        let mut updated = first.record.clone();
        updated.title = "Renamed".to_string();
        let second = store.write_record(updated, None).await.unwrap();

        assert_eq!(second.record.created_at, first.record.created_at);
        assert_eq!(second.body, "original body");
        assert!(second.record.updated_at >= first.record.updated_at);
        assert_eq!(second.record.title, "Renamed");
    }

    #[tokio::test]
    async fn project_scope_requires_project_key_and_separates_records() {
        let dir = tempdir().unwrap();
        let store = LedgerStore::new(dir.path());

        let mut orphan = todo("rec_orphan", "No key", None);
        orphan.scope = LedgerScope::Project;
        assert!(store.write_record(orphan, None).await.is_err());

        let mut scoped = todo("rec_scoped", "Project item", None);
        scoped.scope = LedgerScope::Project;
        scoped.project_key = Some("proj-1".to_string());
        store.write_record(scoped, None).await.unwrap();

        assert!(store
            .get_record(LedgerScope::Global, None, "rec_scoped")
            .await
            .unwrap()
            .is_none());
        assert!(store
            .get_record(LedgerScope::Project, Some("proj-1"), "rec_scoped")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn list_records_filters_and_sorts_anchored_first() {
        let dir = tempdir().unwrap();
        let store = LedgerStore::new(dir.path());

        store
            .write_record(todo("rec_later", "Later", Some(utc(2026, 8, 2, 9))), None)
            .await
            .unwrap();
        store
            .write_record(todo("rec_soon", "Soon", Some(utc(2026, 7, 20, 9))), None)
            .await
            .unwrap();
        store
            .write_record(todo("rec_undated", "Undated", None), None)
            .await
            .unwrap();
        store
            .transition_record(
                LedgerScope::Global,
                None,
                "rec_later",
                RecordStatus::Cancelled,
                None,
            )
            .await
            .unwrap();

        let open = store
            .list_records(LedgerScope::Global, None, &RecordFilter::default())
            .await
            .unwrap();
        let ids: Vec<&str> = open.iter().map(|doc| doc.record.id.as_str()).collect();
        assert_eq!(ids, vec!["rec_soon", "rec_undated"]);

        let terminal_only = store
            .list_records(
                LedgerScope::Global,
                None,
                &RecordFilter {
                    statuses: Some(HashSet::from([RecordStatus::Cancelled])),
                    ..RecordFilter::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(terminal_only.len(), 1);
        assert_eq!(terminal_only[0].record.id, "rec_later");
    }

    #[tokio::test]
    async fn writes_refresh_indexes_views_and_audit_log() {
        let dir = tempdir().unwrap();
        let store = LedgerStore::new(dir.path());

        // Relative due date: the on-write artifact refresh buckets against the
        // real clock, so a fixed date would drift out of the 7-day window.
        store
            .write_record(
                todo("rec_1", "Send report", Some(Utc::now() + Duration::days(2))),
                None,
            )
            .await
            .unwrap();

        let scope_root = store.resolver().scope_root(LedgerScope::Global, None);
        let time_index: TimeIndex = serde_json::from_str(
            &std::fs::read_to_string(scope_root.join("indexes").join(BY_TIME_INDEX_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(time_index.items.len(), 1);
        assert_eq!(time_index.items[0].id, "rec_1");

        let agenda_view =
            std::fs::read_to_string(scope_root.join("views").join(AGENDA_VIEW_FILE)).unwrap();
        assert!(agenda_view.contains("Send report"));

        let audit = std::fs::read_to_string(scope_root.join("logs").join(AUDIT_LOG_FILE)).unwrap();
        assert!(audit.contains("\"action\":\"create\""));
    }

    #[tokio::test]
    async fn agenda_buckets_by_time_distance() {
        let dir = tempdir().unwrap();
        let store = LedgerStore::new(dir.path());
        let now = utc(2026, 7, 13, 12);

        store
            .write_record(
                todo("rec_overdue", "Yesterday", Some(utc(2026, 7, 12, 9))),
                None,
            )
            .await
            .unwrap();
        store
            .write_record(
                todo("rec_today", "Tonight", Some(utc(2026, 7, 13, 20))),
                None,
            )
            .await
            .unwrap();
        store
            .write_record(
                todo("rec_week", "This week", Some(utc(2026, 7, 16, 9))),
                None,
            )
            .await
            .unwrap();
        store
            .write_record(
                todo("rec_far", "Next month", Some(utc(2026, 8, 20, 9))),
                None,
            )
            .await
            .unwrap();
        store
            .write_record(todo("rec_undated", "Someday", None), None)
            .await
            .unwrap();

        let snapshot = store
            .agenda(&[(LedgerScope::Global, None)], now, 7)
            .await
            .unwrap();
        let ids =
            |items: &[AgendaItem]| items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();
        assert_eq!(ids(&snapshot.overdue), vec!["rec_overdue"]);
        assert_eq!(ids(&snapshot.today), vec!["rec_today"]);
        assert_eq!(ids(&snapshot.upcoming), vec!["rec_week"]);
        assert_eq!(ids(&snapshot.undated), vec!["rec_undated"]);

        let markdown = build_agenda_markdown(&snapshot);
        assert!(markdown.contains("## Overdue"));
        assert!(markdown.contains("rec_today"));
        assert!(!markdown.contains("rec_far"));
    }

    #[test]
    fn new_record_ids_are_unique_and_path_safe() {
        let a = new_record_id();
        let b = new_record_id();
        assert_ne!(a, b);
        assert!(validate_record_id(&a).is_ok());
    }
}
