use std::collections::HashSet;
use std::fmt::Display;

use actix_web::{web, Error, HttpResponse, Result};
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::json;

use bamboo_domain::ledger::{LedgerRecord, LedgerScope, RecordActor, RecordKind, RecordStatus};
use bamboo_domain::TaskPriority;
use bamboo_memory::ledger_store::store::new_record_id;
use bamboo_memory::ledger_store::{LedgerRecordDocument, LedgerStore, RecordFilter};

use crate::app_state::AppState;

use super::types::{
    AgendaQuery, ListRecordsQuery, LocateRecordQuery, PatchRecordRequest, UpsertRecordRequest,
};

const DEFAULT_LIST_LIMIT: usize = 20;
const MAX_LIST_LIMIT: usize = 50;
const DEFAULT_AGENDA_HORIZON_DAYS: i64 = 7;

// ── actix entry points ──────────────────────────────────────────────────────
//
// The store is constructed per request: `LedgerStore` is a stateless path
// resolver over `{app_data_dir}/ledger/v1` (the same pattern the memory
// metrics handlers use with `MemoryStore`).

fn ledger_store(state: &AppState) -> LedgerStore {
    LedgerStore::new(state.app_data_dir.clone())
}

/// `GET /api/v1/ledger/records`
pub async fn list_records(
    state: web::Data<AppState>,
    query: web::Query<ListRecordsQuery>,
) -> Result<HttpResponse> {
    list_records_core(&ledger_store(&state), query.into_inner()).await
}

/// `POST /api/v1/ledger/records`
pub async fn upsert_record(
    state: web::Data<AppState>,
    req: web::Json<UpsertRecordRequest>,
) -> Result<HttpResponse> {
    upsert_record_core(&ledger_store(&state), req.into_inner()).await
}

/// `PATCH /api/v1/ledger/records/{record_id}`
pub async fn patch_record(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<LocateRecordQuery>,
    req: web::Json<PatchRecordRequest>,
) -> Result<HttpResponse> {
    patch_record_core(
        &ledger_store(&state),
        &path.into_inner(),
        query.into_inner(),
        req.into_inner(),
    )
    .await
}

/// `DELETE /api/v1/ledger/records/{record_id}`
pub async fn delete_record(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<LocateRecordQuery>,
) -> Result<HttpResponse> {
    delete_record_core(
        &ledger_store(&state),
        &path.into_inner(),
        query.into_inner(),
    )
    .await
}

/// `GET /api/v1/ledger/agenda`
pub async fn agenda(
    state: web::Data<AppState>,
    query: web::Query<AgendaQuery>,
) -> Result<HttpResponse> {
    agenda_core(&ledger_store(&state), query.into_inner()).await
}

// ── response helpers (match the neighboring schedules handlers) ─────────────

fn internal_server_error(action: &str, error: impl Display) -> Error {
    crate::error::json_internal_server_error(format!("Failed to {action}: {error}"))
}

fn bad_request(message: impl Into<String>) -> HttpResponse {
    HttpResponse::BadRequest().json(json!({ "error": crate::error::error_value(message.into()) }))
}

fn record_not_found(record_id: &str) -> HttpResponse {
    HttpResponse::NotFound().json(json!({
        "error": crate::error::error_value("Record not found"),
        "record_id": record_id
    }))
}

// ── parsing helpers (same conventions as the `ledger` agent tool) ───────────

fn normalize_opt(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn parse_datetime(raw: &str, field: &str) -> std::result::Result<DateTime<Utc>, HttpResponse> {
    let trimmed = raw.trim();
    if let Ok(parsed) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(parsed.with_timezone(&Utc));
    }
    // Date-only convenience: midnight UTC (mirrors the ledger tool).
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        if let Some(midnight) = date.and_hms_opt(0, 0, 0) {
            return Ok(DateTime::from_naive_utc_and_offset(midnight, Utc));
        }
    }
    Err(bad_request(format!(
        "{field} must be RFC3339 (e.g. 2026-07-20T09:00:00Z) or YYYY-MM-DD, got: {raw}"
    )))
}

fn parse_priority(raw: &str) -> std::result::Result<TaskPriority, HttpResponse> {
    serde_json::from_value(json!(raw.trim().to_ascii_lowercase())).map_err(|_| {
        bad_request(format!(
            "priority must be one of low|medium|high|critical, got: {raw}"
        ))
    })
}

fn parse_kind(raw: &str) -> std::result::Result<RecordKind, HttpResponse> {
    RecordKind::parse(raw).ok_or_else(|| bad_request("kind cannot be empty"))
}

fn parse_status(raw: &str) -> std::result::Result<RecordStatus, HttpResponse> {
    RecordStatus::parse(raw).ok_or_else(|| {
        bad_request(format!(
            "status must be one of open|in_progress|blocked|done|cancelled|expired, got: {raw}"
        ))
    })
}

fn parse_status_csv(raw: &str) -> std::result::Result<Option<HashSet<RecordStatus>>, HttpResponse> {
    let mut statuses = HashSet::new();
    for token in raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        statuses.insert(parse_status(token)?);
    }
    Ok((!statuses.is_empty()).then_some(statuses))
}

fn parse_kind_csv(raw: &str) -> std::result::Result<Option<HashSet<RecordKind>>, HttpResponse> {
    let mut kinds = HashSet::new();
    for token in raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        kinds.insert(parse_kind(token)?);
    }
    Ok((!kinds.is_empty()).then_some(kinds))
}

/// Resolve the scopes a read spans. Forgiving by design: `project` (or `all`)
/// without a `project_key` degrades to the global scope instead of erroring,
/// so a UI can always render *something*.
fn resolve_scopes(
    scope: Option<&str>,
    project_key: Option<String>,
) -> std::result::Result<Vec<(LedgerScope, Option<String>)>, HttpResponse> {
    match scope.map(str::trim).filter(|value| !value.is_empty()) {
        Some("global") => Ok(vec![(LedgerScope::Global, None)]),
        Some("project") => match project_key {
            Some(key) => Ok(vec![(LedgerScope::Project, Some(key))]),
            None => Ok(vec![(LedgerScope::Global, None)]),
        },
        None | Some("all") => {
            let mut scopes = vec![(LedgerScope::Global, None)];
            if let Some(key) = project_key {
                scopes.push((LedgerScope::Project, Some(key)));
            }
            Ok(scopes)
        }
        Some(other) => Err(bad_request(format!(
            "scope must be global, project, or all, got: {other}"
        ))),
    }
}

/// Find a record by id: the explicitly requested scope if given, otherwise
/// the global scope first, then — when a `project_key` is available — that
/// project's scope (same lookup order as the ledger tool).
async fn locate_record(
    store: &LedgerStore,
    record_id: &str,
    explicit_scope: Option<LedgerScope>,
    project_key: Option<&str>,
) -> Result<Option<LedgerRecordDocument>> {
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
        if let Some(doc) = store
            .get_record(scope, key, record_id)
            .await
            .map_err(|error| internal_server_error("read ledger record", error))?
        {
            return Ok(Some(doc));
        }
    }
    Ok(None)
}

fn record_response(doc: &LedgerRecordDocument) -> serde_json::Value {
    json!({
        "record": doc.record,
        "body": doc.body,
    })
}

// ── core logic (unit-tested against a temp-dir store) ───────────────────────

pub(super) async fn list_records_core(
    store: &LedgerStore,
    query: ListRecordsQuery,
) -> Result<HttpResponse> {
    let project_key = normalize_opt(query.project_key.as_deref());
    let scopes = match resolve_scopes(query.scope.as_deref(), project_key) {
        Ok(scopes) => scopes,
        Err(response) => return Ok(response),
    };
    let statuses = match query.status.as_deref().map(parse_status_csv).transpose() {
        Ok(statuses) => statuses.flatten(),
        Err(response) => return Ok(response),
    };
    let kinds = match query.kind.as_deref().map(parse_kind_csv).transpose() {
        Ok(kinds) => kinds.flatten(),
        Err(response) => return Ok(response),
    };
    let filter = RecordFilter {
        statuses,
        kinds,
        parent_id: normalize_opt(query.parent_id.as_deref()),
        include_terminal: query.include_terminal.unwrap_or(false),
        ..RecordFilter::default()
    };
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);

    let mut records = Vec::new();
    for (scope, key) in &scopes {
        let docs = store
            .list_records(*scope, key.as_deref(), &filter)
            .await
            .map_err(|error| internal_server_error("list ledger records", error))?;
        records.extend(docs.into_iter().map(|doc| doc.record));
    }
    let matched = records.len();
    records.truncate(limit);

    Ok(HttpResponse::Ok().json(json!({
        "records": records,
        "returned": records.len(),
        "matched": matched,
    })))
}

pub(super) async fn upsert_record_core(
    store: &LedgerStore,
    req: UpsertRecordRequest,
) -> Result<HttpResponse> {
    let explicit_scope = match req
        .scope
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(raw) => match LedgerScope::parse(raw) {
            Some(scope) => Some(scope),
            None => {
                return Ok(bad_request(format!(
                    "scope must be global or project, got: {raw}"
                )))
            }
        },
        None => None,
    };
    let project_key = normalize_opt(req.project_key.as_deref());

    let existing = match normalize_opt(req.id.as_deref()) {
        Some(id) => locate_record(store, &id, explicit_scope, project_key.as_deref()).await?,
        None => None,
    };

    let mut record = match &existing {
        Some(doc) => doc.record.clone(),
        None => {
            let Some(title) = normalize_opt(req.title.as_deref()) else {
                return Ok(bad_request("creating a record requires a title"));
            };
            let id = normalize_opt(req.id.as_deref()).unwrap_or_else(new_record_id);
            let kind = match req.kind.as_deref() {
                Some(raw) => match parse_kind(raw) {
                    Ok(kind) => kind,
                    Err(response) => return Ok(response),
                },
                None => RecordKind::Todo,
            };
            let mut record = LedgerRecord::new(id, kind, title);
            // Records created over the HTTP API are user-authored (a person
            // clicking a UI), unlike the tool layer which writes as Agent.
            record.source.created_by = RecordActor::User;
            match explicit_scope {
                Some(LedgerScope::Project) => {
                    let Some(key) = project_key.clone() else {
                        return Ok(bad_request("project scope requires a project_key"));
                    };
                    record.scope = LedgerScope::Project;
                    record.project_key = Some(key);
                }
                _ => record.scope = LedgerScope::Global,
            }
            record
        }
    };

    // Field updates apply to both paths; on update, absent fields leave the
    // existing value untouched (mirrors the ledger tool's upsert).
    if let Some(title) = normalize_opt(req.title.as_deref()) {
        record.title = title;
    }
    if existing.is_some() {
        if let Some(raw) = req.kind.as_deref() {
            match parse_kind(raw) {
                Ok(kind) => record.kind = kind,
                Err(response) => return Ok(response),
            }
        }
    }
    if let Some(raw) = req.priority.as_deref() {
        match parse_priority(raw) {
            Ok(priority) => record.priority = priority,
            Err(response) => return Ok(response),
        }
    }
    if let Some(parent_id) = &req.parent_id {
        record.relations.parent_id = normalize_opt(Some(parent_id));
    }
    if let Some(tags) = &req.tags {
        record.tags = bamboo_memory::memory_store::normalize_tags(tags.iter());
    }
    if let Err(response) = apply_time_fields(
        &mut record,
        req.due_at.as_deref(),
        req.starts_at.as_deref(),
        req.ends_at.as_deref(),
        req.remind_at.as_deref(),
    ) {
        return Ok(response);
    }

    let result = if existing.is_some() {
        "update"
    } else {
        "create"
    };
    let doc = store
        .write_record(record, req.body)
        .await
        .map_err(|error| internal_server_error("write ledger record", error))?;

    // NOTE: the HTTP layer deliberately does NOT create/release schedules for
    // `remind_at`/recurrence times. The record<->schedule bridge is a
    // tool-layer concern (`LedgerTool::sync_schedules` over
    // `LedgerScheduleBridge`); reminders written here become live schedules
    // the next time the tool (or the ledger gardener) touches the record.

    Ok(HttpResponse::Ok().json(json!({
        "result": result,
        "record": doc.record,
        "body": doc.body,
    })))
}

pub(super) async fn patch_record_core(
    store: &LedgerStore,
    record_id: &str,
    query: LocateRecordQuery,
    req: PatchRecordRequest,
) -> Result<HttpResponse> {
    let project_key = normalize_opt(query.project_key.as_deref());
    let Some(existing) = locate_record(store, record_id, None, project_key.as_deref()).await?
    else {
        return Ok(record_not_found(record_id));
    };

    // Parse the status up front so an invalid one fails before any write.
    let status = match req.status.as_deref() {
        Some(raw) => match parse_status(raw) {
            Ok(status) => Some(status),
            Err(response) => return Ok(response),
        },
        None => None,
    };

    let mut record = existing.record.clone();
    let mut changed = false;
    if let Some(title) = normalize_opt(req.title.as_deref()) {
        record.title = title;
        changed = true;
    }
    if let Some(raw) = req.kind.as_deref() {
        match parse_kind(raw) {
            Ok(kind) => record.kind = kind,
            Err(response) => return Ok(response),
        }
        changed = true;
    }
    if let Some(raw) = req.priority.as_deref() {
        match parse_priority(raw) {
            Ok(priority) => record.priority = priority,
            Err(response) => return Ok(response),
        }
        changed = true;
    }
    if let Some(parent_id) = &req.parent_id {
        record.relations.parent_id = normalize_opt(Some(parent_id));
        changed = true;
    }
    if let Some(tags) = &req.tags {
        record.tags = bamboo_memory::memory_store::normalize_tags(tags.iter());
        changed = true;
    }
    let had_time_fields = req.due_at.is_some()
        || req.starts_at.is_some()
        || req.ends_at.is_some()
        || req.remind_at.is_some();
    if let Err(response) = apply_time_fields(
        &mut record,
        req.due_at.as_deref(),
        req.starts_at.as_deref(),
        req.ends_at.as_deref(),
        req.remind_at.as_deref(),
    ) {
        return Ok(response);
    }
    changed = changed || had_time_fields;

    let mut doc = existing;
    if changed || req.body.is_some() {
        doc = store
            .write_record(record, req.body.clone())
            .await
            .map_err(|error| internal_server_error("write ledger record", error))?;
    }

    // Status changes go through `transition_record` so the transition history
    // and audit log record them (not a blind field overwrite).
    if let Some(status) = status {
        if let Some(updated) = store
            .transition_record(
                doc.record.scope,
                doc.record.project_key.as_deref(),
                record_id,
                status,
                req.reason.as_deref(),
            )
            .await
            .map_err(|error| internal_server_error("transition ledger record", error))?
        {
            doc = updated;
        }
        // `None` = no-op transition (already in the target status): report the
        // current state rather than erroring, mirroring the ledger tool.
    }

    // See the schedule-bridge note in `upsert_record_core`: no schedule
    // syncing at the HTTP layer.

    Ok(HttpResponse::Ok().json(record_response(&doc)))
}

pub(super) async fn delete_record_core(
    store: &LedgerStore,
    record_id: &str,
    query: LocateRecordQuery,
) -> Result<HttpResponse> {
    let project_key = normalize_opt(query.project_key.as_deref());
    let Some(existing) = locate_record(store, record_id, None, project_key.as_deref()).await?
    else {
        return Ok(record_not_found(record_id));
    };

    // Ledger records are never hard-deleted: DELETE is a cancel transition, so
    // history/audit stay intact and the id keeps resolving.
    let doc = store
        .transition_record(
            existing.record.scope,
            existing.record.project_key.as_deref(),
            record_id,
            RecordStatus::Cancelled,
            Some("deleted via API"),
        )
        .await
        .map_err(|error| internal_server_error("cancel ledger record", error))?
        .unwrap_or(existing); // already cancelled: no-op is still success

    Ok(HttpResponse::Ok().json(json!({
        "success": true,
        "record": doc.record,
    })))
}

pub(super) async fn agenda_core(store: &LedgerStore, query: AgendaQuery) -> Result<HttpResponse> {
    let mut scopes: Vec<(LedgerScope, Option<String>)> = vec![(LedgerScope::Global, None)];
    if let Some(key) = normalize_opt(query.project_key.as_deref()) {
        scopes.push((LedgerScope::Project, Some(key)));
    }
    let horizon = query
        .horizon_days
        .unwrap_or(DEFAULT_AGENDA_HORIZON_DAYS)
        .clamp(1, 31);
    let snapshot = store
        .agenda(&scopes, Utc::now(), horizon)
        .await
        .map_err(|error| internal_server_error("build ledger agenda", error))?;
    Ok(HttpResponse::Ok().json(snapshot))
}

fn apply_time_fields(
    record: &mut LedgerRecord,
    due_at: Option<&str>,
    starts_at: Option<&str>,
    ends_at: Option<&str>,
    remind_at: Option<&[String]>,
) -> std::result::Result<(), HttpResponse> {
    if let Some(raw) = due_at {
        record.time.due_at = Some(parse_datetime(raw, "due_at")?);
    }
    if let Some(raw) = starts_at {
        record.time.starts_at = Some(parse_datetime(raw, "starts_at")?);
    }
    if let Some(raw) = ends_at {
        record.time.ends_at = Some(parse_datetime(raw, "ends_at")?);
    }
    if let Some(raws) = remind_at {
        let mut parsed = Vec::with_capacity(raws.len());
        for raw in raws {
            parsed.push(parse_datetime(raw, "remind_at")?);
        }
        record.time.remind_at = parsed;
    }
    Ok(())
}
