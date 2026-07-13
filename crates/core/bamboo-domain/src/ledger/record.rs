//! Ledger record types: the prospective-memory counterpart to durable memory.
//!
//! Durable memory records what already happened (retrospective facts and
//! preferences); a [`LedgerRecord`] is a commitment about the *future* — a todo,
//! a calendar event, a reminder, a habit — with a status lifecycle, time
//! semantics, and decomposition/dependency relations. New assistant behaviors
//! are new [`RecordKind`]s over this one model, not new subsystems.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::schedule::ScheduleTrigger;
use crate::session::task::TaskPriority;

/// What kind of prospective record this is. `Custom` keeps the model open:
/// unknown kinds round-trip through persistence instead of failing to parse,
/// so a newer Bamboo (or a skill that invents a kind) never corrupts an older
/// store's reads.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RecordKind {
    Todo,
    Event,
    Reminder,
    Habit,
    Custom(String),
}

impl Default for RecordKind {
    fn default() -> Self {
        Self::Todo
    }
}

impl RecordKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Todo => "todo",
            Self::Event => "event",
            Self::Reminder => "reminder",
            Self::Habit => "habit",
            Self::Custom(kind) => kind.as_str(),
        }
    }

    /// Parse a case-insensitive kind token. Unknown non-empty tokens become
    /// `Custom`; empty input returns `None` so callers decide the fallback.
    pub fn parse(value: &str) -> Option<Self> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "" => None,
            "todo" => Some(Self::Todo),
            "event" => Some(Self::Event),
            "reminder" => Some(Self::Reminder),
            "habit" => Some(Self::Habit),
            _ => Some(Self::Custom(normalized)),
        }
    }
}

impl Serialize for RecordKind {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RecordKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw)
            .ok_or_else(|| serde::de::Error::custom("record kind cannot be empty"))
    }
}

/// Lifecycle status of a ledger record.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum RecordStatus {
    #[default]
    Open,
    InProgress,
    Blocked,
    Done,
    Cancelled,
    Expired,
}

impl RecordStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::InProgress => "in_progress",
            Self::Blocked => "blocked",
            Self::Done => "done",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "open" => Some(Self::Open),
            "in_progress" | "in-progress" => Some(Self::InProgress),
            "blocked" => Some(Self::Blocked),
            "done" | "completed" => Some(Self::Done),
            "cancelled" | "canceled" => Some(Self::Cancelled),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }

    /// Whether the record has reached a final state. Terminal records leave the
    /// agenda/status indexes and must release any linked schedules.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Cancelled | Self::Expired)
    }
}

/// Where a record lives. Personal-life records are `Global` (the default);
/// project work items are `Project` and carry a `project_key`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum LedgerScope {
    #[default]
    Global,
    Project,
}

impl LedgerScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Project => "project",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "global" => Some(Self::Global),
            "project" => Some(Self::Project),
            _ => None,
        }
    }
}

/// Time semantics of a record. All fields optional — a plain note-to-self has
/// none; a todo typically has `due_at`; an event `starts_at`/`ends_at`; a
/// reminder one or more `remind_at` points; a habit a `recurrence`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RecordTime {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub due_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starts_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ends_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remind_at: Vec<DateTime<Utc>>,
    /// Recurrence reuses the schedule trigger model so the schedule bridge can
    /// map it onto a `ScheduleSpec` without translation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recurrence: Option<ScheduleTrigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

impl RecordTime {
    pub fn is_empty(&self) -> bool {
        self.due_at.is_none()
            && self.starts_at.is_none()
            && self.ends_at.is_none()
            && self.remind_at.is_empty()
            && self.recurrence.is_none()
            && self.timezone.is_none()
    }

    /// The earliest actionable timestamp (due, start, or reminder) — the sort
    /// key for the by-time index and agenda views. `ends_at` alone never
    /// anchors: a record with only an end is not actionable by itself.
    pub fn anchor(&self) -> Option<DateTime<Utc>> {
        let mut anchor: Option<DateTime<Utc>> = None;
        let mut consider = |candidate: Option<DateTime<Utc>>| {
            if let Some(candidate) = candidate {
                anchor = Some(match anchor {
                    Some(current) => current.min(candidate),
                    None => candidate,
                });
            }
        };
        consider(self.due_at);
        consider(self.starts_at);
        consider(self.remind_at.iter().min().copied());
        anchor
    }
}

/// Decomposition and dependency edges. "Split this into steps" is the agent
/// writing child records under `parent_id`; a record tree with one root is a
/// plan that survives sessions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RecordRelations {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    /// Free-form references: durable memory ids, session ids, URLs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub related: Vec<String>,
}

impl RecordRelations {
    pub fn is_empty(&self) -> bool {
        self.parent_id.is_none() && self.depends_on.is_empty() && self.related.is_empty()
    }
}

/// Who created or last touched the record.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum RecordActor {
    #[default]
    User,
    /// Written by the agent during a conversation on the user's behalf.
    Agent,
    /// Proposed by a background extraction pass; treated as a suggestion until
    /// confirmed.
    Extractor,
    /// Written by system machinery (schedule bridge, gardener).
    System,
}

impl RecordActor {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Agent => "agent",
            Self::Extractor => "extractor",
            Self::System => "system",
        }
    }
}

/// Provenance: which conversation spawned the record and the sentence that did.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RecordSource {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default)]
    pub created_by: RecordActor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub excerpt: Option<String>,
}

impl RecordSource {
    pub fn is_empty(&self) -> bool {
        self.session_id.is_none()
            && self.created_by == RecordActor::default()
            && self.excerpt.is_none()
    }
}

/// History entry for record status changes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordTransition {
    pub from_status: RecordStatus,
    pub to_status: RecordStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub changed_at: DateTime<Utc>,
}

/// A prospective-memory record. Persisted as the YAML frontmatter of one
/// markdown file per record (the body carries free prose/checklists), mirroring
/// the durable-memory document format.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerRecord {
    pub id: String,
    #[serde(default)]
    pub kind: RecordKind,
    pub title: String,
    #[serde(default)]
    pub status: RecordStatus,
    #[serde(default)]
    pub priority: TaskPriority,
    #[serde(default)]
    pub scope: LedgerScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    #[serde(default, skip_serializing_if = "RecordTime::is_empty")]
    pub time: RecordTime,
    #[serde(default, skip_serializing_if = "RecordRelations::is_empty")]
    pub relations: RecordRelations,
    #[serde(default, skip_serializing_if = "RecordSource::is_empty")]
    pub source: RecordSource,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Ids of `ScheduleSpec`s the schedule bridge manages for this record's
    /// reminders/recurrence. Invariant: a terminal record has released them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schedule_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transitions: Vec<RecordTransition>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl LedgerRecord {
    /// A fresh open record with `now` timestamps.
    pub fn new(id: impl Into<String>, kind: RecordKind, title: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            kind,
            title: title.into(),
            status: RecordStatus::default(),
            priority: TaskPriority::default(),
            scope: LedgerScope::default(),
            project_key: None,
            time: RecordTime::default(),
            relations: RecordRelations::default(),
            source: RecordSource::default(),
            tags: Vec::new(),
            schedule_ids: Vec::new(),
            transitions: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Transition to a new status, recording history and bumping `updated_at`.
    /// Returns `false` (and records nothing) when the status is unchanged.
    pub fn transition_to(&mut self, status: RecordStatus, reason: Option<&str>) -> bool {
        if self.status == status {
            return false;
        }
        let now = Utc::now();
        self.transitions.push(RecordTransition {
            from_status: self.status,
            to_status: status,
            reason: reason
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
            changed_at: now,
        });
        self.status = status;
        self.updated_at = now;
        true
    }

    /// The earliest actionable timestamp, if any (see [`RecordTime::anchor`]).
    pub fn time_anchor(&self) -> Option<DateTime<Utc>> {
        self.time.anchor()
    }

    /// Whether the record is open work that is past its anchor time.
    pub fn is_overdue_at(&self, now: DateTime<Utc>) -> bool {
        !self.status.is_terminal()
            && self
                .time
                .due_at
                .or(self.time.starts_at)
                .is_some_and(|at| at < now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn utc(y: i32, mo: u32, d: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, 0, 0).unwrap()
    }

    #[test]
    fn record_kind_round_trips_including_custom() {
        for kind in [
            RecordKind::Todo,
            RecordKind::Event,
            RecordKind::Reminder,
            RecordKind::Habit,
            RecordKind::Custom("medication".to_string()),
        ] {
            let yaml = serde_yaml::to_string(&kind).unwrap();
            let back: RecordKind = serde_yaml::from_str(&yaml).unwrap();
            assert_eq!(back, kind);
        }
    }

    #[test]
    fn record_kind_parse_normalizes_and_rejects_empty() {
        assert_eq!(RecordKind::parse("  Todo "), Some(RecordKind::Todo));
        assert_eq!(
            RecordKind::parse("Follow-Up"),
            Some(RecordKind::Custom("follow-up".to_string()))
        );
        assert_eq!(RecordKind::parse("   "), None);
    }

    #[test]
    fn status_terminal_set_is_exactly_done_cancelled_expired() {
        use RecordStatus::*;
        for status in [Open, InProgress, Blocked] {
            assert!(!status.is_terminal(), "{status:?}");
        }
        for status in [Done, Cancelled, Expired] {
            assert!(status.is_terminal(), "{status:?}");
        }
    }

    #[test]
    fn time_anchor_picks_earliest_of_due_start_remind() {
        let time = RecordTime {
            due_at: Some(utc(2026, 8, 1, 12)),
            starts_at: Some(utc(2026, 7, 20, 9)),
            ends_at: Some(utc(2026, 7, 1, 0)), // ends_at never anchors
            remind_at: vec![utc(2026, 7, 25, 8), utc(2026, 7, 19, 8)],
            recurrence: None,
            timezone: None,
        };
        assert_eq!(time.anchor(), Some(utc(2026, 7, 19, 8)));
        assert_eq!(RecordTime::default().anchor(), None);
    }

    #[test]
    fn transition_records_history_and_skips_noop() {
        let mut record = LedgerRecord::new("rec_1", RecordKind::Todo, "Renew passport");
        assert!(!record.transition_to(RecordStatus::Open, Some("noop")));
        assert!(record.transitions.is_empty());

        assert!(record.transition_to(RecordStatus::Done, Some("renewed at the office")));
        assert_eq!(record.status, RecordStatus::Done);
        assert_eq!(record.transitions.len(), 1);
        assert_eq!(record.transitions[0].from_status, RecordStatus::Open);
        assert_eq!(record.transitions[0].to_status, RecordStatus::Done);
        assert_eq!(
            record.transitions[0].reason.as_deref(),
            Some("renewed at the office")
        );
    }

    #[test]
    fn overdue_requires_open_status_and_past_due_or_start() {
        let now = utc(2026, 7, 13, 12);
        let mut record = LedgerRecord::new("rec_1", RecordKind::Todo, "Send report");
        assert!(!record.is_overdue_at(now), "no time set");

        record.time.due_at = Some(utc(2026, 7, 10, 9));
        assert!(record.is_overdue_at(now));

        record.transition_to(RecordStatus::Done, None);
        assert!(!record.is_overdue_at(now), "terminal records are never overdue");
    }

    #[test]
    fn record_round_trips_through_yaml_frontmatter() {
        let mut record = LedgerRecord::new("rec_1", RecordKind::Event, "Flight to Munich");
        record.time.starts_at = Some(utc(2026, 7, 14, 6));
        record.time.remind_at = vec![utc(2026, 7, 13, 18)];
        record.time.recurrence = Some(ScheduleTrigger::Daily {
            hour: 9,
            minute: 0,
            second: 0,
        });
        record.relations.parent_id = Some("rec_trip".to_string());
        record.tags = vec!["travel".to_string()];
        record.schedule_ids = vec!["sched_1".to_string()];
        record.transition_to(RecordStatus::InProgress, Some("checked in"));

        let yaml = serde_yaml::to_string(&record).unwrap();
        let back: LedgerRecord = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(back, record);
    }

    /// A minimal frontmatter written by hand (or an older Bamboo) must parse
    /// with every optional field defaulting.
    #[test]
    fn minimal_frontmatter_parses_with_defaults() {
        let yaml = "id: rec_min\n\
                    title: Water the plants\n\
                    created_at: 2026-07-01T00:00:00Z\n\
                    updated_at: 2026-07-01T00:00:00Z\n";
        let record: LedgerRecord = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(record.kind, RecordKind::Todo);
        assert_eq!(record.status, RecordStatus::Open);
        assert_eq!(record.scope, LedgerScope::Global);
        assert!(record.time.is_empty());
        assert!(record.relations.is_empty());
    }
}
