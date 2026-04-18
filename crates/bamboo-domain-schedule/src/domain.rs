use bamboo_shared_types::ReasoningEffort;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Scheduler-owned schedule definition used by the redesigned scheduling domain.
///
/// This coexists with the legacy `ScheduleEntry` model during the transition away
/// from interval-only schedules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleSpec {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub trigger: ScheduleTrigger,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub misfire_policy: MisfirePolicy,
    #[serde(default)]
    pub overlap_policy: OverlapPolicy,
    #[serde(default)]
    pub run_config: ScheduleRunConfig,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ScheduleSpec {
    pub fn window(&self) -> ScheduleWindow {
        ScheduleWindow {
            start_at: self.start_at,
            end_at: self.end_at,
        }
    }
}

/// Canonical trigger definition for future schedule implementations.
///
/// Kept intentionally independent from any concrete recurrence library so Bamboo
/// can swap trigger engines without changing the persisted domain model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScheduleTrigger {
    Interval {
        every_seconds: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        anchor_at: Option<DateTime<Utc>>,
    },
    Daily {
        hour: u8,
        minute: u8,
        #[serde(default)]
        second: u8,
    },
    Weekly {
        weekdays: Vec<ScheduleWeekday>,
        hour: u8,
        minute: u8,
        #[serde(default)]
        second: u8,
    },
    Monthly {
        days: Vec<u8>,
        hour: u8,
        minute: u8,
        #[serde(default)]
        second: u8,
    },
    Cron {
        expr: String,
    },
}

impl ScheduleTrigger {
    pub fn legacy_interval(every_seconds: u64, anchor_at: Option<DateTime<Utc>>) -> Self {
        Self::Interval {
            every_seconds,
            anchor_at,
        }
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Interval { .. } => "interval",
            Self::Daily { .. } => "daily",
            Self::Weekly { .. } => "weekly",
            Self::Monthly { .. } => "monthly",
            Self::Cron { .. } => "cron",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleWeekday {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MisfirePolicy {
    #[default]
    RunOnce,
    Skip,
    CatchUpAll,
    CatchUpWindow {
        max_catch_up_runs: u32,
        max_lateness_seconds: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OverlapPolicy {
    Allow,
    Skip,
    #[default]
    QueueOne,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ScheduleWindow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ScheduleState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_fire_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_scheduled_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_finished_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub queued_run_count: u32,
    #[serde(default)]
    pub running_run_count: u32,
    #[serde(default)]
    pub consecutive_failures: u32,
    #[serde(default)]
    pub total_run_count: u64,
    #[serde(default)]
    pub total_success_count: u64,
    #[serde(default)]
    pub total_failure_count: u64,
    #[serde(default)]
    pub total_missed_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleRunStatus {
    Queued,
    Running,
    Success,
    Failed,
    Skipped,
    Missed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleRunRecord {
    pub run_id: String,
    pub schedule_id: String,
    pub scheduled_for: DateTime<Utc>,
    pub claimed_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    pub status: ScheduleRunStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_lag_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_duration_ms: Option<u64>,
    #[serde(default)]
    pub was_catch_up: bool,
}

/// Runtime configuration for schedule-executed sessions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ScheduleRunConfig {
    /// Optional system prompt override for new sessions created by this schedule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    /// Optional task message to add to the new session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_message: Option<String>,
    /// Model used when auto-executing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Optional reasoning effort override used when auto-executing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Optional workspace path context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
    /// Optional enhancement prompt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enhance_prompt: Option<String>,
    /// If true, immediately execute the new session (only meaningful if `task_message` exists).
    #[serde(default)]
    pub auto_execute: bool,
}
