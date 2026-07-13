//! `ledger` overlay tool — the agent-facing surface of the prospective-memory
//! record ledger (todos, events, reminders, habits).
//!
//! Mirrors [`crate::memory::MemoryTool`]'s shape (one action-dispatched tool)
//! so the model transfers its habits: `upsert`/`transition`/`decompose`/
//! `promote` mutate records, `get`/`query`/`agenda` read them. Reminder and
//! recurrence times are synced onto real `ScheduleSpec`s through the
//! [`LedgerScheduleBridge`] port, which the server implements over its
//! schedule store — this crate never sees the scheduler directly.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::json;

use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_domain::ledger::{
    LedgerRecord, LedgerScope, RecordActor, RecordKind, RecordStatus,
};
use bamboo_domain::schedule::ScheduleTrigger;
use bamboo_domain::{TaskItemStatus, TaskPriority};
use bamboo_memory::ledger_store::store::new_record_id;
use bamboo_memory::ledger_store::{LedgerRecordDocument, LedgerStore, RecordFilter};
use bamboo_memory::memory_store::project_key_from_path;
use bamboo_tools::tools::workspace_state;

#[cfg(test)]
mod tests;

const MAX_QUERY_LIMIT: usize = 50;
const DEFAULT_QUERY_LIMIT: usize = 20;
const MAX_DECOMPOSE_CHILDREN: usize = 20;
const DEFAULT_AGENDA_HORIZON_DAYS: i64 = 7;

// The schedule-bridge port lives beside the store so background maintenance
// (the engine's ledger gardener) can reconcile schedules through the same
// seam; re-exported here for existing callers.
pub use bamboo_memory::ledger_store::LedgerScheduleBridge;

#[derive(Clone)]
pub struct LedgerTool {
    session_repo: bamboo_engine::SessionRepository,
    store: LedgerStore,
    schedule_bridge: Option<Arc<dyn LedgerScheduleBridge>>,
}

#[derive(Debug, Deserialize)]
struct ChildSpec {
    title: String,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    due_at: Option<String>,
    #[serde(default)]
    priority: Option<String>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    tags: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Default)]
struct LedgerArgs {
    action: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    priority: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    project_key: Option<String>,
    #[serde(default)]
    due_at: Option<String>,
    #[serde(default)]
    starts_at: Option<String>,
    #[serde(default)]
    ends_at: Option<String>,
    #[serde(default)]
    remind_at: Option<Vec<String>>,
    #[serde(default)]
    recurrence: Option<serde_json::Value>,
    #[serde(default)]
    timezone: Option<String>,
    #[serde(default)]
    parent_id: Option<String>,
    #[serde(default)]
    depends_on: Option<Vec<String>>,
    #[serde(default)]
    related: Option<Vec<String>>,
    #[serde(default)]
    tags: Option<Vec<String>>,
    #[serde(default)]
    excerpt: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    statuses: Option<Vec<String>>,
    #[serde(default)]
    kinds: Option<Vec<String>>,
    #[serde(default)]
    due_before: Option<String>,
    #[serde(default)]
    due_after: Option<String>,
    #[serde(default)]
    include_terminal: Option<bool>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    horizon_days: Option<i64>,
    #[serde(default)]
    children: Option<Vec<ChildSpec>>,
    #[serde(default)]
    task_ids: Option<Vec<String>>,
}

fn parse_datetime(raw: &str, field: &str) -> Result<DateTime<Utc>, ToolError> {
    let trimmed = raw.trim();
    if let Ok(parsed) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(parsed.with_timezone(&Utc));
    }
    // Date-only convenience: midnight UTC.
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        if let Some(midnight) = date.and_hms_opt(0, 0, 0) {
            return Ok(DateTime::from_naive_utc_and_offset(midnight, Utc));
        }
    }
    Err(ToolError::InvalidArguments(format!(
        "{field} must be RFC3339 (e.g. 2026-07-20T09:00:00Z) or YYYY-MM-DD, got: {raw}"
    )))
}

fn parse_priority(raw: &str) -> Result<TaskPriority, ToolError> {
    serde_json::from_value(json!(raw.trim().to_ascii_lowercase())).map_err(|_| {
        ToolError::InvalidArguments(format!(
            "priority must be one of low|medium|high|critical, got: {raw}"
        ))
    })
}

fn parse_kind(raw: &str) -> Result<RecordKind, ToolError> {
    RecordKind::parse(raw)
        .ok_or_else(|| ToolError::InvalidArguments("kind cannot be empty".to_string()))
}

fn parse_status(raw: &str) -> Result<RecordStatus, ToolError> {
    RecordStatus::parse(raw).ok_or_else(|| {
        ToolError::InvalidArguments(format!(
            "status must be one of open|in_progress|blocked|done|cancelled|expired, got: {raw}"
        ))
    })
}

fn record_json(doc: &LedgerRecordDocument) -> serde_json::Value {
    json!({
        "record": doc.record,
        "body": doc.body,
    })
}

fn json_result(value: serde_json::Value) -> ToolResult {
    ToolResult {
        success: true,
        result: value.to_string(),
        display_preference: Some("json".to_string()),
        images: Vec::new(),
    }
}

impl LedgerTool {
    pub fn new(
        session_repo: bamboo_engine::SessionRepository,
        data_dir: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            session_repo,
            store: LedgerStore::new(data_dir),
            schedule_bridge: None,
        }
    }

    pub fn with_schedule_bridge(mut self, bridge: Arc<dyn LedgerScheduleBridge>) -> Self {
        self.schedule_bridge = Some(bridge);
        self
    }

    async fn resolve_project_key(
        &self,
        explicit: Option<&str>,
        session_id: &str,
    ) -> Option<String> {
        if let Some(explicit) = explicit
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
        {
            return Some(explicit);
        }
        workspace_state::get_workspace(session_id)
            .or_else(workspace_state::get_configured_default_workspace)
            .map(|path| project_key_from_path(&path))
    }

    /// Find a record by id: the explicitly requested scope first, otherwise
    /// global then the session's project scope.
    async fn locate_record(
        &self,
        id: &str,
        explicit_scope: Option<LedgerScope>,
        project_key: Option<&str>,
    ) -> Result<Option<LedgerRecordDocument>, ToolError> {
        let candidates: Vec<(LedgerScope, Option<&str>)> = match explicit_scope {
            Some(LedgerScope::Global) => vec![(LedgerScope::Global, None)],
            Some(LedgerScope::Project) => vec![(LedgerScope::Project, project_key)],
            None => vec![
                (LedgerScope::Global, None),
                (LedgerScope::Project, project_key),
            ],
        };
        for (scope, key) in candidates {
            if scope == LedgerScope::Project && key.is_none() {
                continue;
            }
            if let Some(doc) = self
                .store
                .get_record(scope, key, id)
                .await
                .map_err(|error| ToolError::Execution(format!("Failed to read record: {error}")))?
            {
                return Ok(Some(doc));
            }
        }
        Ok(None)
    }

    /// Reconcile managed schedules with the record's current state, persisting
    /// any change to the record's `schedule_ids`. Bridge failures degrade to a
    /// warning string instead of failing the mutation: the record write
    /// already succeeded, and losing it over a scheduler hiccup would be worse.
    async fn sync_schedules(&self, doc: LedgerRecordDocument) -> (LedgerRecordDocument, Vec<String>) {
        let Some(bridge) = &self.schedule_bridge else {
            return (doc, Vec::new());
        };
        let mut warnings = Vec::new();
        let record = &doc.record;
        let wants_schedules = !record.status.is_terminal()
            && (!record.time.remind_at.is_empty() || record.time.recurrence.is_some());

        let new_ids = if wants_schedules {
            match bridge.sync_record_schedules(record).await {
                Ok(ids) => ids,
                Err(error) => {
                    warnings.push(format!("schedule sync failed: {error}"));
                    return (doc, warnings);
                }
            }
        } else {
            if !record.schedule_ids.is_empty() {
                if let Err(error) = bridge.release_schedules(&record.schedule_ids).await {
                    warnings.push(format!("schedule release failed: {error}"));
                    return (doc, warnings);
                }
            }
            Vec::new()
        };

        if new_ids == record.schedule_ids {
            return (doc, warnings);
        }
        let mut updated = doc.record.clone();
        updated.schedule_ids = new_ids;
        match self.store.write_record(updated, Some(doc.body.clone())).await {
            Ok(rewritten) => (rewritten, warnings),
            Err(error) => {
                warnings.push(format!("failed to persist schedule ids: {error}"));
                (doc, warnings)
            }
        }
    }

    fn apply_time_args(record: &mut LedgerRecord, args: &LedgerArgs) -> Result<(), ToolError> {
        if let Some(raw) = &args.due_at {
            record.time.due_at = Some(parse_datetime(raw, "due_at")?);
        }
        if let Some(raw) = &args.starts_at {
            record.time.starts_at = Some(parse_datetime(raw, "starts_at")?);
        }
        if let Some(raw) = &args.ends_at {
            record.time.ends_at = Some(parse_datetime(raw, "ends_at")?);
        }
        if let Some(raws) = &args.remind_at {
            let mut parsed = Vec::with_capacity(raws.len());
            for raw in raws {
                parsed.push(parse_datetime(raw, "remind_at")?);
            }
            record.time.remind_at = parsed;
        }
        if let Some(raw) = &args.recurrence {
            if raw.is_null() {
                record.time.recurrence = None;
            } else {
                let trigger: ScheduleTrigger =
                    serde_json::from_value(raw.clone()).map_err(|error| {
                        ToolError::InvalidArguments(format!(
                            "recurrence must be a schedule trigger object \
                             (e.g. {{\"type\":\"daily\",\"hour\":9,\"minute\":0}}): {error}"
                        ))
                    })?;
                record.time.recurrence = Some(trigger);
            }
        }
        if let Some(tz) = &args.timezone {
            record.time.timezone = Some(tz.clone());
        }
        Ok(())
    }

    async fn handle_upsert(
        &self,
        args: &LedgerArgs,
        session_id: &str,
    ) -> Result<serde_json::Value, ToolError> {
        let explicit_scope = args
            .scope
            .as_deref()
            .map(|raw| {
                LedgerScope::parse(raw).ok_or_else(|| {
                    ToolError::InvalidArguments(format!(
                        "scope must be global or project, got: {raw}"
                    ))
                })
            })
            .transpose()?;
        let project_key = self
            .resolve_project_key(args.project_key.as_deref(), session_id)
            .await;

        let existing = match args.id.as_deref().map(str::trim).filter(|id| !id.is_empty()) {
            Some(id) => {
                self.locate_record(id, explicit_scope, project_key.as_deref())
                    .await?
            }
            None => None,
        };

        let mut record = match &existing {
            Some(doc) => doc.record.clone(),
            None => {
                let title = args
                    .title
                    .as_deref()
                    .map(str::trim)
                    .filter(|title| !title.is_empty())
                    .ok_or_else(|| {
                        ToolError::InvalidArguments(
                            "creating a record requires a title".to_string(),
                        )
                    })?;
                let id = args
                    .id
                    .as_deref()
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
                    .map(ToString::to_string)
                    .unwrap_or_else(new_record_id);
                let kind = match args.kind.as_deref() {
                    Some(raw) => parse_kind(raw)?,
                    None => RecordKind::Todo,
                };
                let mut record = LedgerRecord::new(id, kind, title);
                record.source.session_id = Some(session_id.to_string());
                record.source.created_by = RecordActor::Agent;
                match explicit_scope {
                    Some(LedgerScope::Project) => {
                        record.scope = LedgerScope::Project;
                        record.project_key = Some(project_key.clone().ok_or_else(|| {
                            ToolError::InvalidArguments(
                                "project scope requires a project_key (or a session workspace)"
                                    .to_string(),
                            )
                        })?);
                    }
                    _ => record.scope = LedgerScope::Global,
                }
                record
            }
        };

        // Field updates apply to both paths; on update, absent args leave the
        // existing value untouched.
        if let Some(title) = args.title.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
            record.title = title.to_string();
        }
        if existing.is_some() {
            if let Some(raw) = args.kind.as_deref() {
                record.kind = parse_kind(raw)?;
            }
        }
        if let Some(raw) = args.priority.as_deref() {
            record.priority = parse_priority(raw)?;
        }
        if let Some(raw) = args.status.as_deref() {
            let status = parse_status(raw)?;
            record.transition_to(status, args.reason.as_deref());
        }
        if let Some(parent_id) = &args.parent_id {
            record.relations.parent_id =
                Some(parent_id.trim().to_string()).filter(|value| !value.is_empty());
        }
        if let Some(depends_on) = &args.depends_on {
            record.relations.depends_on = depends_on.clone();
        }
        if let Some(related) = &args.related {
            record.relations.related = related.clone();
        }
        if let Some(tags) = &args.tags {
            record.tags = bamboo_memory::memory_store::normalize_tags(tags.iter());
        }
        if let Some(excerpt) = &args.excerpt {
            record.source.excerpt = Some(excerpt.clone());
        }
        Self::apply_time_args(&mut record, args)?;

        let body = args.body.clone();
        let action = if existing.is_some() { "update" } else { "create" };
        let doc = self
            .store
            .write_record(record, body)
            .await
            .map_err(|error| ToolError::Execution(format!("Failed to write record: {error}")))?;
        let (doc, warnings) = self.sync_schedules(doc).await;

        Ok(json!({
            "action": "upsert",
            "result": action,
            "data": record_json(&doc),
            "warnings": warnings,
        }))
    }

    async fn handle_transition(
        &self,
        args: &LedgerArgs,
        session_id: &str,
    ) -> Result<serde_json::Value, ToolError> {
        let id = args
            .id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| ToolError::InvalidArguments("transition requires an id".to_string()))?;
        let status = parse_status(args.status.as_deref().ok_or_else(|| {
            ToolError::InvalidArguments("transition requires a status".to_string())
        })?)?;
        let project_key = self
            .resolve_project_key(args.project_key.as_deref(), session_id)
            .await;
        let Some(located) = self.locate_record(id, None, project_key.as_deref()).await? else {
            return Err(ToolError::Execution(format!("record not found: {id}")));
        };

        let updated = self
            .store
            .transition_record(
                located.record.scope,
                located.record.project_key.as_deref(),
                id,
                status,
                args.reason.as_deref(),
            )
            .await
            .map_err(|error| ToolError::Execution(format!("Failed to transition: {error}")))?;
        let doc = match updated {
            Some(doc) => doc,
            // No-op transition: report current state rather than erroring.
            None => located,
        };
        let (doc, warnings) = self.sync_schedules(doc).await;

        Ok(json!({
            "action": "transition",
            "data": record_json(&doc),
            "warnings": warnings,
        }))
    }

    async fn handle_get(
        &self,
        args: &LedgerArgs,
        session_id: &str,
    ) -> Result<serde_json::Value, ToolError> {
        let id = args
            .id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| ToolError::InvalidArguments("get requires an id".to_string()))?;
        let project_key = self
            .resolve_project_key(args.project_key.as_deref(), session_id)
            .await;
        let Some(doc) = self.locate_record(id, None, project_key.as_deref()).await? else {
            return Err(ToolError::Execution(format!("record not found: {id}")));
        };

        let children = self
            .store
            .list_records(
                doc.record.scope,
                doc.record.project_key.as_deref(),
                &RecordFilter {
                    parent_id: Some(doc.record.id.clone()),
                    include_terminal: true,
                    ..RecordFilter::default()
                },
            )
            .await
            .map_err(|error| ToolError::Execution(format!("Failed to list children: {error}")))?;

        Ok(json!({
            "action": "get",
            "data": record_json(&doc),
            "children": children.iter().map(|child| &child.record).collect::<Vec<_>>(),
        }))
    }

    async fn handle_query(
        &self,
        args: &LedgerArgs,
        session_id: &str,
    ) -> Result<serde_json::Value, ToolError> {
        let project_key = self
            .resolve_project_key(args.project_key.as_deref(), session_id)
            .await;
        let scopes: Vec<(LedgerScope, Option<String>)> = match args.scope.as_deref() {
            Some("global") => vec![(LedgerScope::Global, None)],
            Some("project") => vec![(LedgerScope::Project, project_key.clone())],
            None | Some("all") => {
                let mut scopes = vec![(LedgerScope::Global, None)];
                if project_key.is_some() {
                    scopes.push((LedgerScope::Project, project_key.clone()));
                }
                scopes
            }
            Some(other) => {
                return Err(ToolError::InvalidArguments(format!(
                    "scope must be global, project, or all, got: {other}"
                )))
            }
        };

        let statuses = args
            .statuses
            .as_ref()
            .map(|raws| {
                raws.iter()
                    .map(|raw| parse_status(raw))
                    .collect::<Result<HashSet<_>, _>>()
            })
            .transpose()?;
        let kinds = args
            .kinds
            .as_ref()
            .map(|raws| {
                raws.iter()
                    .map(|raw| parse_kind(raw))
                    .collect::<Result<HashSet<_>, _>>()
            })
            .transpose()?;
        let filter = RecordFilter {
            statuses,
            kinds,
            tags: args.tags.clone().unwrap_or_default(),
            parent_id: args.parent_id.clone(),
            anchor_before: args
                .due_before
                .as_deref()
                .map(|raw| parse_datetime(raw, "due_before"))
                .transpose()?,
            anchor_after: args
                .due_after
                .as_deref()
                .map(|raw| parse_datetime(raw, "due_after"))
                .transpose()?,
            include_terminal: args.include_terminal.unwrap_or(false),
            limit: None,
        };
        let limit = args
            .limit
            .unwrap_or(DEFAULT_QUERY_LIMIT)
            .clamp(1, MAX_QUERY_LIMIT);

        let mut records = Vec::new();
        for (scope, key) in &scopes {
            if *scope == LedgerScope::Project && key.is_none() {
                return Err(ToolError::InvalidArguments(
                    "project scope requires a project_key (or a session workspace)".to_string(),
                ));
            }
            let docs = self
                .store
                .list_records(*scope, key.as_deref(), &filter)
                .await
                .map_err(|error| ToolError::Execution(format!("Failed to query: {error}")))?;
            records.extend(docs.into_iter().map(|doc| doc.record));
        }
        let total = records.len();
        records.truncate(limit);

        Ok(json!({
            "action": "query",
            "records": records,
            "returned": records.len(),
            "matched": total,
        }))
    }

    async fn handle_agenda(
        &self,
        args: &LedgerArgs,
        session_id: &str,
    ) -> Result<serde_json::Value, ToolError> {
        let project_key = self
            .resolve_project_key(args.project_key.as_deref(), session_id)
            .await;
        let mut scopes: Vec<(LedgerScope, Option<String>)> = vec![(LedgerScope::Global, None)];
        if project_key.is_some() {
            scopes.push((LedgerScope::Project, project_key));
        }
        let horizon = args
            .horizon_days
            .unwrap_or(DEFAULT_AGENDA_HORIZON_DAYS)
            .clamp(1, 31);
        let snapshot = self
            .store
            .agenda(&scopes, Utc::now(), horizon)
            .await
            .map_err(|error| ToolError::Execution(format!("Failed to build agenda: {error}")))?;
        Ok(json!({
            "action": "agenda",
            "horizon_days": horizon,
            "agenda": snapshot,
        }))
    }

    async fn handle_decompose(
        &self,
        args: &LedgerArgs,
        session_id: &str,
    ) -> Result<serde_json::Value, ToolError> {
        let parent_id = args
            .parent_id
            .as_deref()
            .or(args.id.as_deref())
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                ToolError::InvalidArguments("decompose requires a parent_id".to_string())
            })?;
        let children = args.children.as_ref().filter(|c| !c.is_empty()).ok_or_else(|| {
            ToolError::InvalidArguments(
                "decompose requires a non-empty children array".to_string(),
            )
        })?;
        if children.len() > MAX_DECOMPOSE_CHILDREN {
            return Err(ToolError::InvalidArguments(format!(
                "decompose supports at most {MAX_DECOMPOSE_CHILDREN} children per call"
            )));
        }
        let project_key = self
            .resolve_project_key(args.project_key.as_deref(), session_id)
            .await;
        let Some(parent) = self
            .locate_record(parent_id, None, project_key.as_deref())
            .await?
        else {
            return Err(ToolError::Execution(format!(
                "parent record not found: {parent_id}"
            )));
        };

        let mut created = Vec::with_capacity(children.len());
        for child in children {
            let kind = match child.kind.as_deref() {
                Some(raw) => parse_kind(raw)?,
                None => parent.record.kind.clone(),
            };
            let mut record = LedgerRecord::new(new_record_id(), kind, child.title.trim());
            record.scope = parent.record.scope;
            record.project_key = parent.record.project_key.clone();
            record.relations.parent_id = Some(parent.record.id.clone());
            record.source.session_id = Some(session_id.to_string());
            record.source.created_by = RecordActor::Agent;
            if let Some(raw) = child.due_at.as_deref() {
                record.time.due_at = Some(parse_datetime(raw, "children[].due_at")?);
            }
            if let Some(raw) = child.priority.as_deref() {
                record.priority = parse_priority(raw)?;
            }
            if let Some(tags) = &child.tags {
                record.tags = bamboo_memory::memory_store::normalize_tags(tags.iter());
            }
            let doc = self
                .store
                .write_record(record, child.body.clone())
                .await
                .map_err(|error| {
                    ToolError::Execution(format!("Failed to write child record: {error}"))
                })?;
            created.push(doc.record);
        }

        Ok(json!({
            "action": "decompose",
            "parent_id": parent.record.id,
            "created": created,
        }))
    }

    async fn handle_promote(
        &self,
        args: &LedgerArgs,
        session_id: &str,
    ) -> Result<serde_json::Value, ToolError> {
        let session = self
            .session_repo
            .load(session_id)
            .await
            .ok_or_else(|| ToolError::Execution(format!("session not found: {session_id}")))?;
        // The shared task list lives on the root session of the tree.
        let root = if session.root_session_id != session.id {
            self.session_repo
                .load(&session.root_session_id)
                .await
                .unwrap_or(session)
        } else {
            session
        };
        let Some(task_list) = root.task_list.as_ref().filter(|list| !list.items.is_empty())
        else {
            return Err(ToolError::Execution(
                "the session has no task list to promote".to_string(),
            ));
        };

        let selected: Vec<&bamboo_domain::TaskItem> = match &args.task_ids {
            Some(ids) => {
                let wanted: HashSet<&str> = ids.iter().map(String::as_str).collect();
                task_list
                    .items
                    .iter()
                    .filter(|item| wanted.contains(item.id.as_str()))
                    .collect()
            }
            None => task_list
                .items
                .iter()
                .filter(|item| item.status != TaskItemStatus::Completed)
                .collect(),
        };
        if selected.is_empty() {
            return Err(ToolError::Execution(
                "no matching task items to promote".to_string(),
            ));
        }

        // Task-item ids are session-local; co-promoted parents/dependencies are
        // remapped onto the new record ids, references to unpromoted items drop.
        let id_map: HashMap<&str, String> = selected
            .iter()
            .map(|item| (item.id.as_str(), new_record_id()))
            .collect();
        let mut created = Vec::with_capacity(selected.len());
        for item in &selected {
            let mut record = LedgerRecord::new(
                id_map[item.id.as_str()].clone(),
                RecordKind::Todo,
                item.description.trim(),
            );
            record.priority = item.priority.clone();
            record.status = match item.status {
                TaskItemStatus::InProgress => RecordStatus::InProgress,
                TaskItemStatus::Blocked => RecordStatus::Blocked,
                TaskItemStatus::Completed => RecordStatus::Done,
                TaskItemStatus::Pending => RecordStatus::Open,
            };
            record.relations.parent_id = item
                .parent_id
                .as_deref()
                .and_then(|parent| id_map.get(parent).cloned());
            record.relations.depends_on = item
                .depends_on
                .iter()
                .filter_map(|dep| id_map.get(dep.as_str()).cloned())
                .collect();
            record.source.session_id = Some(session_id.to_string());
            record.source.created_by = RecordActor::Agent;
            let body = (!item.notes.trim().is_empty()).then(|| item.notes.clone());
            let doc = self
                .store
                .write_record(record, body)
                .await
                .map_err(|error| {
                    ToolError::Execution(format!("Failed to promote task item: {error}"))
                })?;
            created.push(doc.record);
        }

        Ok(json!({
            "action": "promote",
            "created": created,
        }))
    }
}

#[async_trait]
impl Tool for LedgerTool {
    fn name(&self) -> &str {
        "ledger"
    }

    fn description(&self) -> &str {
        "Personal ledger of prospective records: todos, events, reminders, habits. \
         Use this — not the session Task list — whenever the user states a commitment, \
         deadline, appointment, or recurring routine, so it survives across sessions. \
         Actions: upsert (create/update a record; due_at/starts_at/remind_at accept \
         RFC3339 or YYYY-MM-DD; remind_at/recurrence become real fired reminders), \
         transition (done/cancel/block/reopen), get, query (by status/kind/time window), \
         agenda (overdue + next 24h + upcoming), decompose (split a record into child \
         records), promote (lift the current session's Task list into durable records)."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        // Built in two json! expansions: one deeply-nested literal blows the
        // macro recursion limit.
        let children_schema = json!({
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "kind": {"type": "string"},
                    "due_at": {"type": "string"},
                    "priority": {"type": "string"},
                    "body": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["title"]
            }
        });
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["upsert", "transition", "get", "query", "agenda", "decompose", "promote"]
                },
                "id": {"type": "string"},
                "kind": {"type": "string", "description": "todo | event | reminder | habit | custom kind"},
                "title": {"type": "string"},
                "body": {"type": "string", "description": "Free markdown notes for the record"},
                "status": {"type": "string", "enum": ["open", "in_progress", "blocked", "done", "cancelled", "expired"]},
                "priority": {"type": "string", "enum": ["low", "medium", "high", "critical"]},
                "scope": {"type": "string", "enum": ["global", "project", "all"], "description": "global = personal life (default); project = current workspace"},
                "project_key": {"type": "string"},
                "due_at": {"type": "string", "description": "RFC3339 or YYYY-MM-DD"},
                "starts_at": {"type": "string"},
                "ends_at": {"type": "string"},
                "remind_at": {"type": "array", "items": {"type": "string"}, "description": "Reminder times; each becomes a one-shot schedule that wakes the agent"},
                "recurrence": {"type": "object", "description": "Schedule trigger object, e.g. {\"type\":\"daily\",\"hour\":9,\"minute\":0}"},
                "timezone": {"type": "string"},
                "parent_id": {"type": "string"},
                "depends_on": {"type": "array", "items": {"type": "string"}},
                "related": {"type": "array", "items": {"type": "string"}},
                "tags": {"type": "array", "items": {"type": "string"}},
                "excerpt": {"type": "string", "description": "The user's sentence that spawned this record"},
                "reason": {"type": "string"},
                "statuses": {"type": "array", "items": {"type": "string"}},
                "kinds": {"type": "array", "items": {"type": "string"}},
                "due_before": {"type": "string"},
                "due_after": {"type": "string"},
                "include_terminal": {"type": "boolean"},
                "limit": {"type": "integer"},
                "horizon_days": {"type": "integer"},
                "children": children_schema,
                "task_ids": {"type": "array", "items": {"type": "string"}, "description": "promote: specific session task-item ids (default: all incomplete)"}
            },
            "required": ["action"]
        })
    }

    fn classify(&self, args: &serde_json::Value) -> ToolClass {
        let action = args
            .get("action")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        match action.as_str() {
            "get" | "query" | "agenda" => ToolClass::READONLY_PARALLEL,
            _ => ToolClass::MUTATING_SERIAL,
        }
    }

    async fn invoke(&self, args: serde_json::Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        let session_id = ctx.session_id().ok_or_else(|| {
            ToolError::Execution("ledger requires a session_id in tool context".to_string())
        })?;
        let parsed: LedgerArgs = serde_json::from_value(args)
            .map_err(|error| ToolError::InvalidArguments(format!("Invalid ledger args: {error}")))?;

        let value = match parsed.action.trim().to_ascii_lowercase().as_str() {
            "upsert" => self.handle_upsert(&parsed, session_id).await?,
            "transition" => self.handle_transition(&parsed, session_id).await?,
            "get" => self.handle_get(&parsed, session_id).await?,
            "query" => self.handle_query(&parsed, session_id).await?,
            "agenda" => self.handle_agenda(&parsed, session_id).await?,
            "decompose" => self.handle_decompose(&parsed, session_id).await?,
            "promote" => self.handle_promote(&parsed, session_id).await?,
            other => {
                return Err(ToolError::InvalidArguments(format!(
                    "unknown ledger action: {other}"
                )))
            }
        };
        Ok(ToolOutcome::Completed(json_result(value)))
    }
}
