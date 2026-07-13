//! Ledger store — persistence for prospective-memory records.
//!
//! The ledger is the structured, future-facing counterpart to the durable
//! memory store: one markdown file (YAML frontmatter + body) per
//! [`LedgerRecord`], plus derived, rebuildable indexes (`by_time.json`,
//! `by_status.json`) and human-readable views (`AGENDA.md`, `TODO.md`), all
//! under the dedicated `{data_dir}/ledger/v1` area. It deliberately mirrors
//! `memory_store`'s conventions: atomic writes, per-scope write locks, an
//! append-only audit log, and indexes that are always regenerable from the
//! record documents.

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;

use bamboo_domain::ledger::{LedgerRecord, LedgerScope, RecordKind, RecordStatus};
use bamboo_domain::TaskPriority;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod paths;
pub mod store;

pub use paths::LedgerPathResolver;
pub use store::{AgendaItem, AgendaSnapshot, LedgerStore, RecordFilter};

pub const BY_TIME_INDEX_FILE: &str = "by_time.json";
pub const BY_STATUS_INDEX_FILE: &str = "by_status.json";
pub const AGENDA_VIEW_FILE: &str = "AGENDA.md";
pub const TODO_VIEW_FILE: &str = "TODO.md";
pub const AUDIT_LOG_FILE: &str = "audit.jsonl";

pub const MAX_RECORD_TITLE_LEN: usize = 200;
/// Terminal records kept per status bucket in `by_status.json`; the documents
/// themselves are never pruned, only the derived index is capped.
pub const MAX_TERMINAL_INDEX_ITEMS: usize = 100;

/// A ledger record document as stored on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerRecordDocument {
    pub record: LedgerRecord,
    pub body: String,
    pub path: PathBuf,
}

pub fn validate_record_id(record_id: &str) -> io::Result<&str> {
    let trimmed = record_id.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "record id cannot be empty",
        ));
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "record id must contain only alphanumeric, dash, or underscore characters",
        ));
    }
    Ok(trimmed)
}

pub fn validate_record_title(title: &str) -> io::Result<&str> {
    let trimmed = title.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "record title cannot be empty",
        ));
    }
    if trimmed.chars().count() > MAX_RECORD_TITLE_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("record title too long (max {} chars)", MAX_RECORD_TITLE_LEN),
        ));
    }
    Ok(trimmed)
}

pub fn parse_record_document(content: &str) -> io::Result<(LedgerRecord, String)> {
    let trimmed = content.trim_start_matches('\u{feff}');
    let Some(rest) = trimmed.strip_prefix("---\n") else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "missing frontmatter start marker",
        ));
    };
    let Some(end_idx) = rest.find("\n---\n") else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "missing frontmatter end marker",
        ));
    };
    let yaml = &rest[..end_idx];
    let body = &rest[end_idx + "\n---\n".len()..];
    let record: LedgerRecord = serde_yaml::from_str(yaml).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse ledger record frontmatter: {error}"),
        )
    })?;
    Ok((record, body.trim().to_string()))
}

pub fn render_record_document(record: &LedgerRecord, body: &str) -> io::Result<String> {
    let yaml = serde_yaml::to_string(record).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to serialize ledger record frontmatter: {error}"),
        )
    })?;
    Ok(format!("---\n{}---\n\n{}\n", yaml, body.trim()))
}

/// `by_time.json`: non-terminal records that have a time anchor, ascending by
/// anchor — the source for agenda queries and prompt injection.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TimeIndex {
    pub generated_at: String,
    pub items: Vec<TimeIndexItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeIndexItem {
    pub id: String,
    pub title: String,
    pub kind: RecordKind,
    pub status: RecordStatus,
    pub priority: TaskPriority,
    pub anchor_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub due_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starts_at: Option<DateTime<Utc>>,
}

/// `by_status.json`: records bucketed by status. Non-terminal buckets are
/// complete; terminal buckets keep only the most recent
/// [`MAX_TERMINAL_INDEX_ITEMS`] entries (counts stay exact).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatusIndex {
    pub generated_at: String,
    pub buckets: BTreeMap<String, Vec<StatusIndexItem>>,
    pub counts: BTreeMap<String, usize>,
    pub total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusIndexItem {
    pub id: String,
    pub title: String,
    pub kind: RecordKind,
    pub priority: TaskPriority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    pub updated_at: DateTime<Utc>,
}

/// Append-only audit entry, one JSONL line per ledger mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerAuditEntry {
    pub timestamp: String,
    pub action: String,
    pub scope: LedgerScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    pub record_id: String,
    pub summary: String,
}

pub fn build_time_index(records: &[LedgerRecord], generated_at: DateTime<Utc>) -> TimeIndex {
    let mut items: Vec<TimeIndexItem> = records
        .iter()
        .filter(|record| !record.status.is_terminal())
        .filter_map(|record| {
            record.time_anchor().map(|anchor_at| TimeIndexItem {
                id: record.id.clone(),
                title: record.title.clone(),
                kind: record.kind.clone(),
                status: record.status,
                priority: record.priority.clone(),
                anchor_at,
                due_at: record.time.due_at,
                starts_at: record.time.starts_at,
            })
        })
        .collect();
    items.sort_by(|left, right| {
        left.anchor_at
            .cmp(&right.anchor_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    TimeIndex {
        generated_at: generated_at.to_rfc3339(),
        items,
    }
}

pub fn build_status_index(records: &[LedgerRecord], generated_at: DateTime<Utc>) -> StatusIndex {
    let mut buckets: BTreeMap<String, Vec<StatusIndexItem>> = BTreeMap::new();
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for record in records {
        let key = record.status.as_str().to_string();
        *counts.entry(key.clone()).or_default() += 1;
        buckets.entry(key).or_default().push(StatusIndexItem {
            id: record.id.clone(),
            title: record.title.clone(),
            kind: record.kind.clone(),
            priority: record.priority.clone(),
            parent_id: record.relations.parent_id.clone(),
            updated_at: record.updated_at,
        });
    }
    let total = records.len();
    for (status_key, items) in buckets.iter_mut() {
        items.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        let is_terminal = RecordStatus::parse(status_key).is_some_and(RecordStatus::is_terminal);
        if is_terminal {
            items.truncate(MAX_TERMINAL_INDEX_ITEMS);
        }
    }
    StatusIndex {
        generated_at: generated_at.to_rfc3339(),
        buckets,
        counts,
        total,
    }
}

pub fn build_todo_markdown_view(records: &[LedgerRecord]) -> String {
    let mut out = String::from("# Ledger — Open Records\n\n");
    let open: Vec<&LedgerRecord> = records
        .iter()
        .filter(|record| !record.status.is_terminal())
        .collect();
    if open.is_empty() {
        out.push_str("_(empty)_\n");
        return out;
    }

    // Roots first (no parent, or parent not among the open set), children
    // indented beneath — a record tree with one root reads as a plan.
    let open_ids: std::collections::HashSet<&str> =
        open.iter().map(|record| record.id.as_str()).collect();
    let mut children: BTreeMap<&str, Vec<&LedgerRecord>> = BTreeMap::new();
    let mut roots: Vec<&LedgerRecord> = Vec::new();
    for record in &open {
        match record
            .relations
            .parent_id
            .as_deref()
            .filter(|parent| open_ids.contains(parent))
        {
            Some(parent) => children.entry(parent).or_default().push(record),
            None => roots.push(record),
        }
    }

    fn push_line(out: &mut String, record: &LedgerRecord, depth: usize) {
        let anchor = record
            .time_anchor()
            .map(|at| format!(" — {}", at.format("%Y-%m-%d %H:%M UTC")))
            .unwrap_or_default();
        out.push_str(&format!(
            "{}- `{}` [{}/{}] {}{}\n",
            "  ".repeat(depth),
            record.id,
            record.kind.as_str(),
            record.status.as_str(),
            record.title,
            anchor,
        ));
    }

    fn push_tree(
        out: &mut String,
        record: &LedgerRecord,
        children: &BTreeMap<&str, Vec<&LedgerRecord>>,
        depth: usize,
    ) {
        push_line(out, record, depth);
        // Depth cap guards against a pathological parent cycle in hand-edited files.
        if depth >= 8 {
            return;
        }
        if let Some(kids) = children.get(record.id.as_str()) {
            for kid in kids {
                push_tree(out, kid, children, depth + 1);
            }
        }
    }

    for root in roots {
        push_tree(&mut out, root, &children, 0);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn record(id: &str, status: RecordStatus, due_hour: Option<u32>) -> LedgerRecord {
        let mut record = LedgerRecord::new(id, RecordKind::Todo, format!("record {id}"));
        record.status = status;
        record.time.due_at =
            due_hour.map(|hour| Utc.with_ymd_and_hms(2026, 7, 20, hour, 0, 0).unwrap());
        record
    }

    #[test]
    fn record_document_round_trips() {
        let source = record("rec_1", RecordStatus::Open, Some(9));
        let rendered = render_record_document(&source, "Bring the old passport.\n").unwrap();
        let (parsed, body) = parse_record_document(&rendered).unwrap();
        assert_eq!(parsed, source);
        assert_eq!(body, "Bring the old passport.");
    }

    #[test]
    fn time_index_sorts_by_anchor_and_skips_terminal_and_undated() {
        let records = vec![
            record("rec_late", RecordStatus::Open, Some(15)),
            record("rec_early", RecordStatus::InProgress, Some(8)),
            record("rec_done", RecordStatus::Done, Some(6)),
            record("rec_undated", RecordStatus::Open, None),
        ];
        let index = build_time_index(&records, Utc::now());
        let ids: Vec<&str> = index.items.iter().map(|item| item.id.as_str()).collect();
        assert_eq!(ids, vec!["rec_early", "rec_late"]);
    }

    #[test]
    fn status_index_counts_stay_exact_when_terminal_bucket_is_capped() {
        let mut records: Vec<LedgerRecord> = (0..(MAX_TERMINAL_INDEX_ITEMS + 5))
            .map(|idx| record(&format!("rec_{idx}"), RecordStatus::Done, None))
            .collect();
        records.push(record("rec_open", RecordStatus::Open, None));

        let index = build_status_index(&records, Utc::now());
        assert_eq!(index.counts["done"], MAX_TERMINAL_INDEX_ITEMS + 5);
        assert_eq!(index.buckets["done"].len(), MAX_TERMINAL_INDEX_ITEMS);
        assert_eq!(index.buckets["open"].len(), 1);
        assert_eq!(index.total, MAX_TERMINAL_INDEX_ITEMS + 6);
    }

    #[test]
    fn todo_view_nests_children_under_open_parents() {
        let mut parent = record("rec_trip", RecordStatus::Open, None);
        parent.title = "Plan the trip".to_string();
        let mut child = record("rec_flight", RecordStatus::Open, Some(6));
        child.relations.parent_id = Some("rec_trip".to_string());
        let done = record("rec_done", RecordStatus::Done, None);

        let view = build_todo_markdown_view(&[parent, child, done]);
        assert!(view.contains("- `rec_trip`"));
        assert!(view.contains("  - `rec_flight`"));
        assert!(!view.contains("rec_done"));
    }

    #[test]
    fn validate_record_id_rejects_path_characters() {
        assert!(validate_record_id("rec_ok-1").is_ok());
        assert!(validate_record_id("../escape").is_err());
        assert!(validate_record_id("a/b").is_err());
        assert!(validate_record_id("  ").is_err());
    }
}
