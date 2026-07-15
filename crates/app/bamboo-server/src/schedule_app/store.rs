use std::cmp::Reverse;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use tokio::fs;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use super::trigger_engine::{default_trigger_engine, TriggerEngine};
use bamboo_domain::{
    MisFirePolicy, OverlapPolicy, ScheduleRunConfig, ScheduleRunRecord, ScheduleRunStatus,
    ScheduleSpec, ScheduleState, ScheduleTrigger, ScheduleWindow,
};

fn other_io_error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

async fn atomic_write_json(path: &Path, bytes: Vec<u8>) -> io::Result<()> {
    let tmp = path.with_extension(format!("json.tmp.{}", Uuid::new_v4()));

    // Write + fsync to ensure data is on disk before rename.
    {
        let mut file = fs::File::create(&tmp).await?;
        tokio::io::AsyncWriteExt::write_all(&mut file, &bytes).await?;
        file.sync_all().await?;
    }

    fs::rename(&tmp, path).await?;

    // fsync parent directory to persist the rename metadata.
    if let Some(parent) = path.parent() {
        if let Ok(dir) = fs::File::open(parent).await {
            let _ = dir.sync_all().await;
        }
    }

    Ok(())
}

/// Remove leftover `.tmp.*` files from a previous interrupted atomic write.
async fn cleanup_stale_tmp_files(dir: &Path, prefix: &str) {
    let mut entries = match fs::read_dir(dir).await {
        Ok(e) => e,
        Err(_) => return,
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        if let Some(name) = entry.file_name().to_str() {
            if name.starts_with(prefix) {
                tracing::info!("Removing stale temp file: {}", entry.path().display());
                let _ = fs::remove_file(entry.path()).await;
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ScheduleEntry {
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
    pub misfire_policy: MisFirePolicy,
    #[serde(default)]
    pub overlap_policy: OverlapPolicy,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub state: ScheduleState,
    #[serde(default)]
    pub run_config: ScheduleRunConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct ScheduleEntryCompat {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub interval_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger: Option<ScheduleTrigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub misfire_policy: MisFirePolicy,
    #[serde(default)]
    pub overlap_policy: OverlapPolicy,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub state: Option<ScheduleState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub queued_run_count: u32,
    #[serde(default)]
    pub running_run_count: u32,
    #[serde(default)]
    pub run_config: ScheduleRunConfig,
}

impl ScheduleEntry {
    fn from_compat(raw: ScheduleEntryCompat) -> Result<Self, String> {
        let trigger = match raw.trigger.clone() {
            Some(ScheduleTrigger::Interval {
                every_seconds,
                anchor_at,
            }) => {
                let every_seconds = raw.interval_seconds.unwrap_or(every_seconds);
                ScheduleTrigger::Interval {
                    every_seconds,
                    anchor_at: anchor_at
                        .or_else(|| derive_legacy_anchor_at(&raw, every_seconds))
                        .or(Some(raw.created_at)),
                }
            }
            Some(other) => other,
            None => {
                let every_seconds = raw.interval_seconds.ok_or_else(|| {
                    format!("schedule entry {} missing trigger definition", raw.id)
                })?;
                ScheduleTrigger::legacy_interval(
                    every_seconds,
                    derive_legacy_anchor_at(&raw, every_seconds).or(Some(raw.created_at)),
                )
            }
        };

        let mut state = raw.state.unwrap_or_else(|| ScheduleState {
            next_fire_at: raw.next_run_at,
            last_scheduled_at: raw.last_run_at,
            queued_run_count: raw.queued_run_count,
            running_run_count: raw.running_run_count,
            ..Default::default()
        });
        if state.next_fire_at.is_none() {
            state.next_fire_at = raw.next_run_at;
        }
        if state.last_scheduled_at.is_none() {
            state.last_scheduled_at = raw.last_run_at;
        }

        Ok(Self {
            id: raw.id,
            name: raw.name,
            enabled: raw.enabled,
            trigger,
            timezone: raw.timezone,
            start_at: raw.start_at,
            end_at: raw.end_at,
            misfire_policy: raw.misfire_policy,
            overlap_policy: raw.overlap_policy,
            created_at: raw.created_at,
            updated_at: raw.updated_at,
            state,
            run_config: raw.run_config,
        })
    }

    pub fn derived_anchor_at(&self) -> Option<DateTime<Utc>> {
        let every_seconds = interval_seconds_from_trigger(&self.trigger)?;
        if let ScheduleTrigger::Interval {
            anchor_at: Some(anchor_at),
            ..
        } = &self.trigger
        {
            return Some(*anchor_at);
        }
        self.state
            .last_scheduled_at
            .or_else(|| {
                self.state
                    .next_fire_at
                    .map(|next| next - Duration::seconds(every_seconds as i64))
            })
            .or(Some(self.created_at))
    }

    pub fn to_schedule_spec(&self) -> ScheduleSpec {
        ScheduleSpec {
            id: self.id.clone(),
            name: self.name.clone(),
            enabled: self.enabled,
            trigger: self.trigger.clone(),
            timezone: self.timezone.clone(),
            start_at: self.start_at,
            end_at: self.end_at,
            misfire_policy: self.misfire_policy,
            overlap_policy: self.overlap_policy,
            run_config: self.run_config.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }

    pub fn to_schedule_state(&self) -> ScheduleState {
        self.state.clone()
    }
}

impl<'de> Deserialize<'de> for ScheduleEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = ScheduleEntryCompat::deserialize(deserializer)?;
        ScheduleEntry::from_compat(raw).map_err(serde::de::Error::custom)
    }
}

fn derive_legacy_anchor_at(raw: &ScheduleEntryCompat, every_seconds: u64) -> Option<DateTime<Utc>> {
    if let Some(ScheduleTrigger::Interval {
        anchor_at: Some(anchor_at),
        ..
    }) = raw.trigger.as_ref()
    {
        return Some(*anchor_at);
    }

    raw.state
        .as_ref()
        .and_then(|state| state.last_scheduled_at)
        .or(raw.last_run_at)
        .or_else(|| {
            raw.state
                .as_ref()
                .and_then(|state| state.next_fire_at)
                .or(raw.next_run_at)
                .map(|next| next - Duration::seconds(every_seconds as i64))
        })
}

fn interval_seconds_from_trigger(trigger: &ScheduleTrigger) -> Option<u64> {
    match trigger {
        ScheduleTrigger::Interval { every_seconds, .. } => Some(*every_seconds),
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchedulesIndex {
    version: u32,
    updated_at: DateTime<Utc>,
    schedules: HashMap<String, ScheduleEntry>,
    #[serde(default)]
    run_records: HashMap<String, ScheduleRunRecord>,
}

impl SchedulesIndex {
    fn empty() -> Self {
        Self {
            version: 4,
            updated_at: Utc::now(),
            schedules: HashMap::new(),
            run_records: HashMap::new(),
        }
    }
}

/// Per-schedule cap on retained *terminal* run records. In-flight runs
/// (Queued/Running) are always kept; only completed history is bounded. Chosen
/// so a 60s schedule retains ~3.3h of history while keeping the persisted
/// `schedules.json` (and every rewrite of it) bounded rather than O(total
/// history) — see issue #233.
const MAX_TERMINAL_RUN_RECORDS_PER_SCHEDULE: usize = 200;

/// Drop the oldest terminal run records once a schedule exceeds
/// [`MAX_TERMINAL_RUN_RECORDS_PER_SCHEDULE`], keeping the most recent by
/// completion time. Non-terminal runs are never pruned. O(n) in the current
/// record count; called only at run-insert points, where the count is already
/// bounded by this same cap.
fn prune_run_records(run_records: &mut HashMap<String, ScheduleRunRecord>) {
    // Collect the run_ids to evict without holding a borrow across the removal.
    let mut evict: Vec<String> = Vec::new();
    {
        let mut terminal_by_schedule: HashMap<&str, Vec<&ScheduleRunRecord>> = HashMap::new();
        for record in run_records.values() {
            if record.status.is_terminal() {
                terminal_by_schedule
                    .entry(record.schedule_id.as_str())
                    .or_default()
                    .push(record);
            }
        }
        for records in terminal_by_schedule.values_mut() {
            if records.len() <= MAX_TERMINAL_RUN_RECORDS_PER_SCHEDULE {
                continue;
            }
            // Newest first by completion time (falling back to the scheduled
            // time for records without a completed_at).
            records.sort_by_key(|r| std::cmp::Reverse(r.completed_at.unwrap_or(r.scheduled_for)));
            for stale in records.iter().skip(MAX_TERMINAL_RUN_RECORDS_PER_SCHEDULE) {
                evict.push(stale.run_id.clone());
            }
        }
    }
    for run_id in evict {
        run_records.remove(&run_id);
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScheduleDefinitionChanges {
    pub trigger: Option<ScheduleTrigger>,
    pub timezone: Option<String>,
    pub start_at: Option<DateTime<Utc>>,
    pub end_at: Option<DateTime<Utc>>,
    pub misfire_policy: Option<MisFirePolicy>,
    pub overlap_policy: Option<OverlapPolicy>,
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn definition_window(definition: &ScheduleDefinitionChanges) -> ScheduleWindow {
    ScheduleWindow {
        start_at: definition.start_at,
        end_at: definition.end_at,
    }
}

fn compute_initial_next_run_at(
    trigger: &ScheduleTrigger,
    timezone: Option<&str>,
    window: &ScheduleWindow,
    now: DateTime<Utc>,
) -> io::Result<DateTime<Utc>> {
    let engine = default_trigger_engine();
    let runtime_timezone = match trigger {
        // Interval and Once are defined on absolute UTC instants; the engine
        // rejects a timezone for them, so never pass one through.
        ScheduleTrigger::Interval { .. } | ScheduleTrigger::Once { .. } => None,
        _ => timezone,
    };
    engine
        .next_after(trigger, runtime_timezone, now, window)
        .map_err(|error| other_io_error(format!("failed to compute initial next run: {error}")))?
        .ok_or_else(|| other_io_error("schedule has no next run within configured window"))
}

fn normalize_trigger_for_storage(
    trigger: ScheduleTrigger,
    anchor_at: DateTime<Utc>,
) -> ScheduleTrigger {
    match trigger {
        ScheduleTrigger::Interval {
            every_seconds,
            anchor_at: existing_anchor,
        } => ScheduleTrigger::Interval {
            every_seconds,
            anchor_at: existing_anchor.or(Some(anchor_at)),
        },
        other => other,
    }
}

fn normalize_loaded_index(mut index: SchedulesIndex) -> (SchedulesIndex, bool) {
    let mut changed = false;
    if index.version < 4 {
        index.version = 4;
        changed = true;
    }
    for entry in index.schedules.values_mut() {
        changed |= normalize_loaded_schedule_entry(entry);
    }
    if changed {
        index.updated_at = Utc::now();
    }
    (index, changed)
}

fn normalize_loaded_schedule_entry(entry: &mut ScheduleEntry) -> bool {
    let mut changed = false;

    let normalized_timezone = normalize_optional_string(entry.timezone.clone());
    if entry.timezone != normalized_timezone {
        entry.timezone = normalized_timezone;
        changed = true;
    }

    let derived_anchor_at = entry.derived_anchor_at();
    if let ScheduleTrigger::Interval {
        every_seconds,
        anchor_at,
    } = &mut entry.trigger
    {
        if anchor_at.is_none() {
            if let Some(derived_anchor_at) = derived_anchor_at {
                *anchor_at = Some(derived_anchor_at);
                changed = true;
            }
        }
        if *every_seconds == 0 {
            *every_seconds = 1;
            changed = true;
        }
    }

    // A `next_fire_at: None` on a one-shot trigger is terminal state (the
    // single occurrence was already claimed), not missing data — re-arming it
    // here would make a fired Once schedule fire a second time on reload.
    if entry.state.next_fire_at.is_none() && !matches!(entry.trigger, ScheduleTrigger::Once { .. })
    {
        entry.state.next_fire_at = Some(entry.created_at);
        changed = true;
    }

    changed
}

fn current_due_at(entry: &ScheduleEntry) -> Option<DateTime<Utc>> {
    let mut due_at = entry.state.next_fire_at?;
    if let Some(start_at) = entry.start_at {
        if due_at < start_at {
            due_at = start_at;
        }
    }
    if let Some(end_at) = entry.end_at {
        if due_at > end_at {
            return None;
        }
    }
    Some(due_at)
}

fn overlap_blocks_dispatch(entry: &ScheduleEntry) -> bool {
    match entry.overlap_policy {
        OverlapPolicy::Allow => false,
        OverlapPolicy::Skip => entry.state.running_run_count > 0,
        OverlapPolicy::QueueOne => entry.state.queued_run_count > 0,
    }
}

fn apply_overlap_dispatch_limit(entry: &ScheduleEntry, dispatch_count: u32) -> u32 {
    match entry.overlap_policy {
        OverlapPolicy::Allow | OverlapPolicy::Skip => dispatch_count,
        OverlapPolicy::QueueOne => dispatch_count.min(1),
    }
}

fn compute_misfire_dispatch_count(entry: &ScheduleEntry, now: DateTime<Utc>) -> u32 {
    let Some(next_fire_at) = entry.state.next_fire_at else {
        return 0;
    };
    let lateness_seconds = now.signed_duration_since(next_fire_at).num_seconds().max(0) as u64;
    let interval_seconds = interval_seconds_from_trigger(&entry.trigger).unwrap_or(0);

    match entry.misfire_policy {
        MisFirePolicy::RunOnce => 1,
        MisFirePolicy::Skip => 0,
        MisFirePolicy::CatchUpAll => lateness_seconds
            .checked_div(interval_seconds)
            .map(|q| q.saturating_add(1) as u32)
            .unwrap_or(1),
        MisFirePolicy::CatchUpWindow {
            max_catch_up_runs,
            max_lateness_seconds,
        } => {
            if lateness_seconds > max_lateness_seconds {
                0
            } else {
                lateness_seconds
                    .checked_div(interval_seconds)
                    .map(|q| (q.saturating_add(1) as u32).min(max_catch_up_runs.max(1)))
                    .unwrap_or(1)
            }
        }
    }
}

fn runtime_trigger(entry: &ScheduleEntry) -> ScheduleTrigger {
    match &entry.trigger {
        ScheduleTrigger::Interval { every_seconds, .. } => {
            ScheduleTrigger::legacy_interval(*every_seconds, None)
        }
        other => other.clone(),
    }
}

fn runtime_timezone<'a>(entry: &'a ScheduleEntry, trigger: &ScheduleTrigger) -> Option<&'a str> {
    match trigger {
        // Interval and Once are defined on absolute UTC instants; the engine
        // rejects a timezone for them, so never pass one through.
        ScheduleTrigger::Interval { .. } | ScheduleTrigger::Once { .. } => None,
        _ => entry.timezone.as_deref(),
    }
}

fn compute_next_run_at_with_engine(
    entry: &ScheduleEntry,
    now: DateTime<Utc>,
    engine: &dyn TriggerEngine,
) -> io::Result<Option<DateTime<Utc>>> {
    let trigger = runtime_trigger(entry);
    let window = ScheduleWindow {
        start_at: entry.start_at,
        end_at: entry.end_at,
    };
    engine
        .next_after(&trigger, runtime_timezone(entry, &trigger), now, &window)
        .map_err(|error| {
            other_io_error(format!(
                "failed to compute next fire for schedule {}: {}",
                entry.id, error
            ))
        })
}

fn record_missed_occurrence(entry: &mut ScheduleEntry) {
    entry.state.total_missed_count = entry.state.total_missed_count.saturating_add(1);
}

fn non_negative_duration_ms(from: DateTime<Utc>, to: DateTime<Utc>) -> u64 {
    to.signed_duration_since(from).num_milliseconds().max(0) as u64
}

fn make_queued_run_record(
    schedule_id: &str,
    scheduled_for: DateTime<Utc>,
    claimed_at: DateTime<Utc>,
    was_catch_up: bool,
) -> ScheduleRunRecord {
    ScheduleRunRecord {
        run_id: Uuid::new_v4().to_string(),
        schedule_id: schedule_id.to_string(),
        scheduled_for,
        claimed_at,
        started_at: None,
        completed_at: None,
        status: ScheduleRunStatus::Queued,
        outcome_reason: None,
        session_id: None,
        dispatch_lag_ms: None,
        execution_duration_ms: None,
        was_catch_up,
    }
}

fn make_fallback_run_record(
    run_id: &str,
    schedule_id: &str,
    now: DateTime<Utc>,
    status: ScheduleRunStatus,
) -> ScheduleRunRecord {
    ScheduleRunRecord {
        run_id: run_id.to_string(),
        schedule_id: schedule_id.to_string(),
        scheduled_for: now,
        claimed_at: now,
        started_at: matches!(status, ScheduleRunStatus::Running).then_some(now),
        completed_at: matches!(
            status,
            ScheduleRunStatus::Success
                | ScheduleRunStatus::Failed
                | ScheduleRunStatus::Skipped
                | ScheduleRunStatus::Missed
                | ScheduleRunStatus::Cancelled
        )
        .then_some(now),
        status,
        outcome_reason: None,
        session_id: None,
        dispatch_lag_ms: None,
        execution_duration_ms: None,
        was_catch_up: false,
    }
}

fn scheduled_for_dispatch(
    entry: &ScheduleEntry,
    due_at: DateTime<Utc>,
    dispatch_index: u32,
) -> DateTime<Utc> {
    match interval_seconds_from_trigger(&entry.trigger) {
        Some(interval_seconds) if dispatch_index > 0 => {
            due_at + Duration::seconds(interval_seconds as i64 * dispatch_index as i64)
        }
        _ => due_at,
    }
}

fn update_run_record_started(
    record: &mut ScheduleRunRecord,
    started_at: DateTime<Utc>,
    session_id: Option<&str>,
) {
    record.status = ScheduleRunStatus::Running;
    record.started_at = Some(started_at);
    record.dispatch_lag_ms = Some(non_negative_duration_ms(record.scheduled_for, started_at));
    if let Some(session_id) = session_id {
        record.session_id = Some(session_id.to_string());
    }
}

fn update_run_record_terminal(
    record: &mut ScheduleRunRecord,
    status: ScheduleRunStatus,
    completed_at: DateTime<Utc>,
    outcome_reason: Option<String>,
    session_id: Option<&str>,
) {
    record.status = status;
    record.completed_at = Some(completed_at);
    if record.dispatch_lag_ms.is_none() {
        record.dispatch_lag_ms = Some(non_negative_duration_ms(record.scheduled_for, completed_at));
    }
    if let Some(started_at) = record.started_at {
        record.execution_duration_ms = Some(non_negative_duration_ms(started_at, completed_at));
    }
    if let Some(outcome_reason) = normalize_optional_string(outcome_reason) {
        record.outcome_reason = Some(outcome_reason);
    }
    if let Some(session_id) = session_id {
        record.session_id = Some(session_id.to_string());
    }
}

fn apply_terminal_run_status(
    entry: &mut ScheduleEntry,
    status: ScheduleRunStatus,
    finished_at: DateTime<Utc>,
) -> io::Result<()> {
    entry.state.running_run_count = entry.state.running_run_count.saturating_sub(1);
    entry.state.last_finished_at = Some(finished_at);

    match status {
        ScheduleRunStatus::Success => {
            entry.state.last_success_at = Some(finished_at);
            entry.state.total_run_count = entry.state.total_run_count.saturating_add(1);
            entry.state.total_success_count = entry.state.total_success_count.saturating_add(1);
            entry.state.consecutive_failures = 0;
            Ok(())
        }
        ScheduleRunStatus::Failed | ScheduleRunStatus::Cancelled => {
            entry.state.last_failure_at = Some(finished_at);
            entry.state.total_run_count = entry.state.total_run_count.saturating_add(1);
            entry.state.total_failure_count = entry.state.total_failure_count.saturating_add(1);
            entry.state.consecutive_failures = entry.state.consecutive_failures.saturating_add(1);
            Ok(())
        }
        ScheduleRunStatus::Skipped => Ok(()),
        ScheduleRunStatus::Missed | ScheduleRunStatus::Queued | ScheduleRunStatus::Running => {
            Err(other_io_error(format!(
                "non-terminal or unsupported run status for lifecycle accounting: {:?}",
                status
            )))
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClaimedScheduleRun {
    pub run_id: String,
    pub schedule_id: String,
    pub schedule_name: String,
    pub run_config: ScheduleRunConfig,
    pub scheduled_for: DateTime<Utc>,
    pub claimed_at: DateTime<Utc>,
    pub was_catch_up: bool,
}

#[derive(Debug)]
pub struct ScheduleStore {
    index_path: PathBuf,
    index: RwLock<SchedulesIndex>,
    write_lock: Mutex<()>,
}

impl ScheduleStore {
    pub async fn new(bamboo_home_dir: PathBuf) -> io::Result<Self> {
        let index_path = bamboo_home_dir.join("schedules.json");

        let (index, needs_backfill_write) = if index_path.exists() {
            let raw = fs::read_to_string(&index_path).await?;
            match serde_json::from_str::<SchedulesIndex>(&raw) {
                Ok(parsed) => normalize_loaded_index(parsed),
                Err(e) => {
                    // Corrupted file (e.g. partial write before crash).
                    // Back up the broken file and start fresh so the app can boot.
                    let backup_path =
                        index_path.with_extension(format!("json.corrupted.{}", Uuid::new_v4()));
                    tracing::error!(
                        "schedules.json is corrupted ({}). Backing up to {} and resetting.",
                        e,
                        backup_path.display()
                    );
                    if let Err(rename_err) = fs::rename(&index_path, &backup_path).await {
                        tracing::warn!(
                            "Failed to back up corrupted schedules.json: {}",
                            rename_err
                        );
                    }
                    let fresh = SchedulesIndex::empty();
                    atomic_write_json(
                        &index_path,
                        serde_json::to_vec_pretty(&fresh)
                            .map_err(|e| other_io_error(e.to_string()))?,
                    )
                    .await?;
                    (fresh, false)
                }
            }
        } else {
            let index = SchedulesIndex::empty();
            atomic_write_json(
                &index_path,
                serde_json::to_vec_pretty(&index).map_err(|e| other_io_error(e.to_string()))?,
            )
            .await?;
            (index, false)
        };

        if needs_backfill_write {
            atomic_write_json(
                &index_path,
                serde_json::to_vec_pretty(&index).map_err(|e| other_io_error(e.to_string()))?,
            )
            .await?;
        }

        // Clean up stale temp files left behind by interrupted atomic writes.
        cleanup_stale_tmp_files(&bamboo_home_dir, "schedules.json.tmp.").await;

        Ok(Self {
            index_path,
            index: RwLock::new(index),
            write_lock: Mutex::new(()),
        })
    }

    pub fn index_path(&self) -> &Path {
        &self.index_path
    }

    async fn update_index<F, T>(&self, f: F) -> io::Result<T>
    where
        F: FnOnce(&mut SchedulesIndex) -> io::Result<T>,
    {
        let _guard = self.write_lock.lock().await;
        let mut index = self.index.write().await;
        let out = f(&mut index)?;
        index.updated_at = Utc::now();
        atomic_write_json(
            &self.index_path,
            serde_json::to_vec_pretty(&*index).map_err(|e| other_io_error(e.to_string()))?,
        )
        .await?;
        Ok(out)
    }

    pub async fn list_schedules(&self) -> Vec<ScheduleEntry> {
        let index = self.index.read().await;
        let mut items: Vec<_> = index.schedules.values().cloned().collect();
        items.sort_by_key(|e| Reverse(e.updated_at));
        items
    }

    pub async fn get_schedule(&self, id: &str) -> Option<ScheduleEntry> {
        let index = self.index.read().await;
        index.schedules.get(id).cloned()
    }

    pub async fn get_run_record(&self, run_id: &str) -> Option<ScheduleRunRecord> {
        let index = self.index.read().await;
        index.run_records.get(run_id).cloned()
    }

    pub async fn list_run_records_for_schedule(&self, schedule_id: &str) -> Vec<ScheduleRunRecord> {
        let index = self.index.read().await;
        let mut items = index
            .run_records
            .values()
            .filter(|record| record.schedule_id == schedule_id)
            .cloned()
            .collect::<Vec<_>>();
        items.sort_by_key(|r| Reverse(r.claimed_at));
        items
    }

    pub async fn create_schedule(
        &self,
        name: String,
        trigger: ScheduleTrigger,
        enabled: bool,
        run_config: ScheduleRunConfig,
    ) -> io::Result<ScheduleEntry> {
        self.create_schedule_with_definition(
            name,
            enabled,
            run_config,
            ScheduleDefinitionChanges {
                trigger: Some(trigger),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn create_schedule_with_definition(
        &self,
        name: String,
        enabled: bool,
        run_config: ScheduleRunConfig,
        definition: ScheduleDefinitionChanges,
    ) -> io::Result<ScheduleEntry> {
        let now = Utc::now();
        let id = Uuid::new_v4().to_string();
        let window = definition_window(&definition);
        let trigger = normalize_trigger_for_storage(
            definition
                .trigger
                .ok_or_else(|| other_io_error("schedule trigger is required"))?,
            now,
        );
        let timezone = normalize_optional_string(definition.timezone);
        let next_fire_at =
            compute_initial_next_run_at(&trigger, timezone.as_deref(), &window, now)?;
        let entry = ScheduleEntry {
            id: id.clone(),
            name,
            enabled,
            trigger,
            timezone,
            start_at: definition.start_at,
            end_at: definition.end_at,
            misfire_policy: definition.misfire_policy.unwrap_or_default(),
            overlap_policy: definition.overlap_policy.unwrap_or_default(),
            created_at: now,
            updated_at: now,
            state: ScheduleState {
                next_fire_at: Some(next_fire_at),
                ..Default::default()
            },
            run_config,
        };

        self.update_index(|index| {
            index.schedules.insert(id.clone(), entry.clone());
            Ok(entry.clone())
        })
        .await
    }

    pub async fn patch_schedule(
        &self,
        id: &str,
        name: Option<String>,
        enabled: Option<bool>,
        trigger: Option<ScheduleTrigger>,
        run_config: Option<ScheduleRunConfig>,
    ) -> io::Result<Option<ScheduleEntry>> {
        self.patch_schedule_with_definition(
            id,
            name,
            enabled,
            run_config,
            ScheduleDefinitionChanges {
                trigger,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn patch_schedule_with_definition(
        &self,
        id: &str,
        name: Option<String>,
        enabled: Option<bool>,
        run_config: Option<ScheduleRunConfig>,
        definition: ScheduleDefinitionChanges,
    ) -> io::Result<Option<ScheduleEntry>> {
        self.update_index(|index| {
            let Some(existing) = index.schedules.get_mut(id) else {
                return Ok(None);
            };
            let now = Utc::now();
            if let Some(name) = name {
                existing.name = name;
            }
            if let Some(enabled) = enabled {
                existing.enabled = enabled;
            }

            let trigger = normalize_trigger_for_storage(
                definition
                    .trigger
                    .clone()
                    .unwrap_or_else(|| existing.trigger.clone()),
                existing.derived_anchor_at().unwrap_or(now),
            );
            existing.trigger = trigger.clone();

            if let Some(timezone) = definition.timezone {
                existing.timezone = normalize_optional_string(Some(timezone));
            }
            if let Some(start_at) = definition.start_at {
                existing.start_at = Some(start_at);
            }
            if let Some(end_at) = definition.end_at {
                existing.end_at = Some(end_at);
            }
            if let Some(misfire_policy) = definition.misfire_policy {
                existing.misfire_policy = misfire_policy;
            }
            if let Some(overlap_policy) = definition.overlap_policy {
                existing.overlap_policy = overlap_policy;
            }
            if let Some(run_config) = run_config {
                existing.run_config = run_config;
            }

            let window = ScheduleWindow {
                start_at: existing.start_at,
                end_at: existing.end_at,
            };
            existing.state.next_fire_at = Some(compute_initial_next_run_at(
                &trigger,
                existing.timezone.as_deref(),
                &window,
                now,
            )?);
            existing.updated_at = now;
            Ok(Some(existing.clone()))
        })
        .await
    }

    pub async fn delete_schedule(&self, id: &str) -> io::Result<bool> {
        self.update_index(|index| {
            let deleted = index.schedules.remove(id).is_some();
            if deleted {
                index
                    .run_records
                    .retain(|_, record| record.schedule_id != id);
            }
            Ok(deleted)
        })
        .await
    }

    /// Claim all due schedules and advance their `next_run_at`.
    ///
    /// Returns a list of run descriptors to execute out-of-band.
    ///
    /// **Important**: only writes to disk when at least one schedule is actually
    /// due.  The ticker calls this every few seconds, so avoiding unnecessary
    /// writes is critical for disk health and crash-safety.
    pub async fn claim_due_runs(&self, now: DateTime<Utc>) -> io::Result<Vec<ClaimedScheduleRun>> {
        let engine = default_trigger_engine();
        self.claim_due_runs_with_engine(now, engine.as_ref()).await
    }

    pub async fn claim_due_runs_with_engine(
        &self,
        now: DateTime<Utc>,
        engine: &dyn TriggerEngine,
    ) -> io::Result<Vec<ClaimedScheduleRun>> {
        {
            let index = self.index.read().await;
            let any_due = index.schedules.values().any(|entry| {
                entry.enabled && current_due_at(entry).is_some_and(|due_at| due_at <= now)
            });
            if !any_due {
                return Ok(Vec::new());
            }
        }

        self.update_index(|index| {
            let mut out = Vec::new();
            let (schedules, run_records) = (&mut index.schedules, &mut index.run_records);
            for entry in schedules.values_mut() {
                if !entry.enabled {
                    continue;
                }
                let Some(due_at) = current_due_at(entry) else {
                    continue;
                };
                if due_at > now {
                    continue;
                }

                let dispatch_count = compute_misfire_dispatch_count(entry, now);
                if dispatch_count == 0 {
                    entry.state.last_scheduled_at = Some(now);
                    record_missed_occurrence(entry);
                    match compute_next_run_at_with_engine(entry, now, engine) {
                        Ok(Some(next_fire_at)) => entry.state.next_fire_at = Some(next_fire_at),
                        Ok(None) => {
                            entry.state.next_fire_at = None;
                            entry.enabled = false;
                        }
                        Err(error) => {
                            tracing::warn!(
                                "failed to compute next scheduled fire for {} after misfire skip: {}. falling back to legacy interval semantics",
                                entry.id,
                                error
                            );
                            let fallback = interval_seconds_from_trigger(&entry.trigger).unwrap_or(60);
                            entry.state.next_fire_at = Some(now + Duration::seconds(fallback as i64));
                        }
                    }
                    entry.updated_at = now;
                    continue;
                }

                let blocked = overlap_blocks_dispatch(entry);
                match entry.overlap_policy {
                    OverlapPolicy::Skip if blocked => {
                        entry.state.last_scheduled_at = Some(now);
                        record_missed_occurrence(entry);
                        match compute_next_run_at_with_engine(entry, now, engine) {
                            Ok(Some(next_fire_at)) => entry.state.next_fire_at = Some(next_fire_at),
                            Ok(None) => {
                                entry.state.next_fire_at = None;
                                entry.enabled = false;
                            }
                            Err(error) => {
                                tracing::warn!(
                                    "failed to compute next scheduled fire for {} after overlap skip: {}. falling back to legacy interval semantics",
                                    entry.id,
                                    error
                                );
                                let fallback = interval_seconds_from_trigger(&entry.trigger).unwrap_or(60);
                                entry.state.next_fire_at = Some(now + Duration::seconds(fallback as i64));
                            }
                        }
                        entry.updated_at = now;
                        continue;
                    }
                    OverlapPolicy::QueueOne if blocked => {
                        continue;
                    }
                    _ => {}
                }

                let dispatch_count = apply_overlap_dispatch_limit(entry, dispatch_count);
                entry.state.last_scheduled_at = Some(now);
                match compute_next_run_at_with_engine(entry, now, engine) {
                    Ok(Some(next_fire_at)) => {
                        entry.state.next_fire_at = Some(next_fire_at);
                    }
                    Ok(None) => {
                        entry.state.next_fire_at = None;
                        entry.enabled = false;
                    }
                    Err(error) => {
                        tracing::warn!(
                            "failed to compute next scheduled fire for {}: {}. falling back to legacy interval semantics",
                            entry.id,
                            error
                        );
                        let fallback = interval_seconds_from_trigger(&entry.trigger).unwrap_or(60);
                        entry.state.next_fire_at = Some(now + Duration::seconds(fallback as i64));
                    }
                }
                entry.state.queued_run_count = entry.state.queued_run_count.saturating_add(dispatch_count);
                entry.updated_at = now;
                for dispatch_index in 0..dispatch_count {
                    let scheduled_for = scheduled_for_dispatch(entry, due_at, dispatch_index);
                    let was_catch_up = scheduled_for < now;
                    let record = make_queued_run_record(&entry.id, scheduled_for, now, was_catch_up);
                    let run_id = record.run_id.clone();
                    run_records.insert(run_id.clone(), record);
                    out.push(ClaimedScheduleRun {
                        run_id,
                        schedule_id: entry.id.clone(),
                        schedule_name: entry.name.clone(),
                        run_config: entry.run_config.clone(),
                        scheduled_for,
                        claimed_at: now,
                        was_catch_up,
                    });
                }
            }
            prune_run_records(run_records);
            Ok(out)
        })
        .await
    }

    pub async fn mark_run_started(&self, schedule_id: &str, run_id: &str) -> io::Result<()> {
        self.update_index(|index| {
            let now = Utc::now();
            if let Some(entry) = index.schedules.get_mut(schedule_id) {
                entry.state.queued_run_count = entry.state.queued_run_count.saturating_sub(1);
                entry.state.running_run_count = entry.state.running_run_count.saturating_add(1);
                entry.state.last_started_at = Some(now);
                entry.updated_at = now;
            }
            let record = index
                .run_records
                .entry(run_id.to_string())
                .or_insert_with(|| {
                    make_fallback_run_record(run_id, schedule_id, now, ScheduleRunStatus::Queued)
                });
            update_run_record_started(record, now, None);
            Ok(())
        })
        .await
    }

    pub async fn bind_run_session(
        &self,
        schedule_id: &str,
        run_id: &str,
        session_id: &str,
    ) -> io::Result<()> {
        self.update_index(|index| {
            let now = Utc::now();
            let record = index
                .run_records
                .entry(run_id.to_string())
                .or_insert_with(|| {
                    make_fallback_run_record(run_id, schedule_id, now, ScheduleRunStatus::Running)
                });
            record.session_id = Some(session_id.to_string());
            Ok(())
        })
        .await
    }

    pub async fn mark_run_terminal(
        &self,
        schedule_id: &str,
        run_id: &str,
        status: ScheduleRunStatus,
        outcome_reason: Option<String>,
    ) -> io::Result<()> {
        self.update_index(|index| {
            let now = Utc::now();
            if let Some(entry) = index.schedules.get_mut(schedule_id) {
                apply_terminal_run_status(entry, status, now)?;
                entry.updated_at = now;
            }
            let record = index
                .run_records
                .entry(run_id.to_string())
                .or_insert_with(|| make_fallback_run_record(run_id, schedule_id, now, status));
            update_run_record_terminal(record, status, now, outcome_reason, None);
            Ok(())
        })
        .await
    }

    pub async fn mark_run_dequeued_without_start(
        &self,
        schedule_id: &str,
        run_id: &str,
        outcome_reason: Option<String>,
    ) -> io::Result<()> {
        self.update_index(|index| {
            let now = Utc::now();
            if let Some(entry) = index.schedules.get_mut(schedule_id) {
                entry.state.queued_run_count = entry.state.queued_run_count.saturating_sub(1);
                record_missed_occurrence(entry);
                entry.updated_at = now;
            }
            let record = index
                .run_records
                .entry(run_id.to_string())
                .or_insert_with(|| {
                    make_fallback_run_record(run_id, schedule_id, now, ScheduleRunStatus::Queued)
                });
            update_run_record_terminal(
                record,
                ScheduleRunStatus::Missed,
                now,
                outcome_reason,
                None,
            );
            Ok(())
        })
        .await
    }

    /// Create a run descriptor immediately (does not change the schedule cadence).
    pub async fn create_run_now(&self, id: &str) -> io::Result<Option<ClaimedScheduleRun>> {
        self.update_index(|index| {
            let Some(entry) = index.schedules.get(id).cloned() else {
                return Ok(None);
            };
            let now = Utc::now();
            let record = make_queued_run_record(&entry.id, now, now, false);
            let run_id = record.run_id.clone();
            index.run_records.insert(run_id.clone(), record);
            prune_run_records(&mut index.run_records);
            Ok(Some(ClaimedScheduleRun {
                run_id,
                schedule_id: entry.id,
                schedule_name: entry.name,
                run_config: entry.run_config,
                scheduled_for: now,
                claimed_at: now,
                was_catch_up: false,
            }))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn run_record(
        run_id: &str,
        schedule_id: &str,
        status: ScheduleRunStatus,
        completed_at: Option<DateTime<Utc>>,
    ) -> ScheduleRunRecord {
        let base = Utc::now();
        ScheduleRunRecord {
            run_id: run_id.to_string(),
            schedule_id: schedule_id.to_string(),
            scheduled_for: base,
            claimed_at: base,
            started_at: None,
            completed_at,
            status,
            outcome_reason: None,
            session_id: None,
            dispatch_lag_ms: None,
            execution_duration_ms: None,
            was_catch_up: false,
        }
    }

    #[test]
    fn prune_caps_terminal_records_keeps_newest_and_all_in_flight() {
        let mut records: HashMap<String, ScheduleRunRecord> = HashMap::new();
        let base = Utc::now();

        // MAX + 50 terminal records for schedule "s1", completion times ordered.
        let total_terminal = MAX_TERMINAL_RUN_RECORDS_PER_SCHEDULE + 50;
        for i in 0..total_terminal {
            let id = format!("s1-term-{i}");
            let completed = base + Duration::seconds(i as i64);
            records.insert(
                id.clone(),
                run_record(&id, "s1", ScheduleRunStatus::Success, Some(completed)),
            );
        }
        // In-flight runs must survive regardless of count.
        records.insert(
            "s1-queued".into(),
            run_record("s1-queued", "s1", ScheduleRunStatus::Queued, None),
        );
        records.insert(
            "s1-running".into(),
            run_record("s1-running", "s1", ScheduleRunStatus::Running, None),
        );
        // A different schedule under the cap is untouched.
        records.insert(
            "s2-term-0".into(),
            run_record("s2-term-0", "s2", ScheduleRunStatus::Failed, Some(base)),
        );

        prune_run_records(&mut records);

        let s1_terminal = records
            .values()
            .filter(|r| r.schedule_id == "s1" && r.status.is_terminal())
            .count();
        assert_eq!(s1_terminal, MAX_TERMINAL_RUN_RECORDS_PER_SCHEDULE);
        // Non-terminal runs kept.
        assert!(records.contains_key("s1-queued"));
        assert!(records.contains_key("s1-running"));
        // The newest terminal record is retained; the oldest is evicted.
        assert!(records.contains_key(&format!("s1-term-{}", total_terminal - 1)));
        assert!(!records.contains_key("s1-term-0"));
        // Other schedules untouched.
        assert!(records.contains_key("s2-term-0"));
    }

    #[tokio::test]
    async fn store_backfills_legacy_interval_trigger_on_load() {
        let dir = tempdir().unwrap();
        let now = DateTime::parse_from_rfc3339("2026-04-04T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let next_run_at = DateTime::parse_from_rfc3339("2026-04-04T11:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let raw = serde_json::json!({
            "version": 1,
            "updated_at": now,
            "schedules": {
                "legacy-1": {
                    "id": "legacy-1",
                    "name": "legacy",
                    "enabled": true,
                    "interval_seconds": 3600,
                    "created_at": now,
                    "updated_at": now,
                    "last_run_at": null,
                    "next_run_at": next_run_at,
                    "run_config": { "auto_execute": false }
                }
            }
        });
        tokio::fs::write(
            dir.path().join("schedules.json"),
            serde_json::to_vec_pretty(&raw).unwrap(),
        )
        .await
        .unwrap();

        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();
        let schedule = store.get_schedule("legacy-1").await.unwrap();
        assert!(matches!(
            schedule.trigger,
            ScheduleTrigger::Interval {
                every_seconds: 3600,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn create_schedule_with_definition_persists_interval_trigger_metadata() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();

        let created = store
            .create_schedule_with_definition(
                "interval".to_string(),
                true,
                ScheduleRunConfig::default(),
                ScheduleDefinitionChanges {
                    trigger: Some(ScheduleTrigger::Interval {
                        every_seconds: 300,
                        anchor_at: None,
                    }),
                    timezone: Some("Asia/Shanghai".to_string()),
                    misfire_policy: Some(MisFirePolicy::RunOnce),
                    overlap_policy: Some(OverlapPolicy::QueueOne),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(matches!(
            created.trigger,
            ScheduleTrigger::Interval {
                every_seconds: 300,
                ..
            }
        ));
        assert_eq!(created.timezone.as_deref(), Some("Asia/Shanghai"));
        assert_eq!(created.misfire_policy, MisFirePolicy::RunOnce);
        assert_eq!(created.overlap_policy, OverlapPolicy::QueueOne);
    }

    #[tokio::test]
    async fn patch_schedule_with_definition_updates_interval_trigger_metadata() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();

        let created = store
            .create_schedule(
                "interval".to_string(),
                ScheduleTrigger::Interval {
                    every_seconds: 300,
                    anchor_at: None,
                },
                true,
                ScheduleRunConfig::default(),
            )
            .await
            .unwrap();

        let patched = store
            .patch_schedule_with_definition(
                &created.id,
                None,
                None,
                None,
                ScheduleDefinitionChanges {
                    trigger: Some(ScheduleTrigger::Interval {
                        every_seconds: 600,
                        anchor_at: None,
                    }),
                    timezone: Some("UTC".to_string()),
                    misfire_policy: Some(MisFirePolicy::CatchUpAll),
                    overlap_policy: Some(OverlapPolicy::Skip),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(
            patched.trigger,
            ScheduleTrigger::Interval {
                every_seconds: 600,
                ..
            }
        ));
        assert_eq!(patched.timezone.as_deref(), Some("UTC"));
        assert_eq!(patched.misfire_policy, MisFirePolicy::CatchUpAll);
        assert_eq!(patched.overlap_policy, OverlapPolicy::Skip);
    }

    #[tokio::test]
    async fn claim_due_runs_with_engine_uses_runtime_adapter_for_interval_trigger() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();

        let now = DateTime::parse_from_rfc3339("2026-04-04T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let created = store
            .create_schedule_with_definition(
                "interval".to_string(),
                true,
                ScheduleRunConfig::default(),
                ScheduleDefinitionChanges {
                    trigger: Some(ScheduleTrigger::Interval {
                        every_seconds: 300,
                        anchor_at: Some(now - Duration::seconds(300)),
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        store
            .update_index(|index| {
                let entry = index.schedules.get_mut(&created.id).unwrap();
                entry.state.next_fire_at = Some(now);
                Ok(())
            })
            .await
            .unwrap();

        let engine = default_trigger_engine();
        let claimed = store
            .claim_due_runs_with_engine(now, engine.as_ref())
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);

        let updated = store.get_schedule(&created.id).await.unwrap();
        assert_eq!(updated.state.last_scheduled_at, Some(now));
        assert_eq!(
            updated.state.next_fire_at,
            Some(now + Duration::seconds(300))
        );
    }

    #[tokio::test]
    async fn create_schedule_with_definition_initializes_monthly_next_run() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();

        let created = store
            .create_schedule_with_definition(
                "monthly".to_string(),
                true,
                ScheduleRunConfig::default(),
                ScheduleDefinitionChanges {
                    trigger: Some(ScheduleTrigger::Monthly {
                        days: vec![1, 15],
                        hour: 9,
                        minute: 0,
                        second: 0,
                    }),
                    timezone: Some("UTC".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(matches!(
            created.trigger,
            ScheduleTrigger::Monthly {
                days,
                hour: 9,
                minute: 0,
                second: 0
            } if days == vec![1, 15]
        ));
        assert!(created
            .state
            .next_fire_at
            .is_some_and(|next| next > created.created_at));
    }

    #[tokio::test]
    async fn create_schedule_with_definition_initializes_once_next_run_at_trigger_instant() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();

        let at = Utc::now() + Duration::seconds(600);
        let created = store
            .create_schedule_with_definition(
                "once".to_string(),
                true,
                ScheduleRunConfig::default(),
                ScheduleDefinitionChanges {
                    trigger: Some(ScheduleTrigger::Once { at }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(matches!(created.trigger, ScheduleTrigger::Once { at: t } if t == at));
        assert_eq!(created.state.next_fire_at, Some(at));
    }

    #[tokio::test]
    async fn claimed_once_run_does_not_produce_a_second_due_run() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();

        let at = Utc::now() + Duration::seconds(600);
        let created = store
            .create_schedule_with_definition(
                "once".to_string(),
                true,
                ScheduleRunConfig::default(),
                ScheduleDefinitionChanges {
                    trigger: Some(ScheduleTrigger::Once { at }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(created.state.next_fire_at, Some(at));

        let engine = default_trigger_engine();

        // Not due yet: nothing is claimed before `at`.
        let claimed = store
            .claim_due_runs_with_engine(at - Duration::seconds(1), engine.as_ref())
            .await
            .unwrap();
        assert!(claimed.is_empty());

        // Due: the single occurrence is claimed exactly once, and the
        // schedule terminates (no next fire, disabled).
        let claimed = store
            .claim_due_runs_with_engine(at, engine.as_ref())
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].scheduled_for, at);

        let updated = store.get_schedule(&created.id).await.unwrap();
        assert_eq!(updated.state.next_fire_at, None);
        assert!(!updated.enabled);

        // Subsequent ticks never claim the fired one-shot again.
        let claimed = store
            .claim_due_runs_with_engine(at + Duration::seconds(60), engine.as_ref())
            .await
            .unwrap();
        assert!(claimed.is_empty());
        let claimed = store
            .claim_due_runs_with_engine(at + Duration::seconds(3600), engine.as_ref())
            .await
            .unwrap();
        assert!(claimed.is_empty());
    }

    #[tokio::test]
    async fn fired_once_schedule_is_not_rearmed_on_reload() {
        let dir = tempdir().unwrap();
        let at = Utc::now() + Duration::seconds(600);
        let created_id;
        {
            let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();
            let created = store
                .create_schedule_with_definition(
                    "once".to_string(),
                    true,
                    ScheduleRunConfig::default(),
                    ScheduleDefinitionChanges {
                        trigger: Some(ScheduleTrigger::Once { at }),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            created_id = created.id.clone();

            let engine = default_trigger_engine();
            let claimed = store
                .claim_due_runs_with_engine(at, engine.as_ref())
                .await
                .unwrap();
            assert_eq!(claimed.len(), 1);
        }

        // Reload from disk: the load-time backfill must not re-arm the fired
        // one-shot (`next_fire_at: None` is its terminal state).
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();
        let reloaded = store.get_schedule(&created_id).await.unwrap();
        assert_eq!(reloaded.state.next_fire_at, None);
        assert!(!reloaded.enabled);

        let engine = default_trigger_engine();
        let claimed = store
            .claim_due_runs_with_engine(at + Duration::seconds(60), engine.as_ref())
            .await
            .unwrap();
        assert!(claimed.is_empty());
    }

    #[tokio::test]
    async fn misfire_skip_does_not_dispatch_run() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();
        let now = DateTime::parse_from_rfc3339("2026-04-04T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let created = store
            .create_schedule_with_definition(
                "skip".to_string(),
                true,
                ScheduleRunConfig::default(),
                ScheduleDefinitionChanges {
                    trigger: Some(ScheduleTrigger::Interval {
                        every_seconds: 300,
                        anchor_at: None,
                    }),
                    misfire_policy: Some(MisFirePolicy::Skip),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        store
            .update_index(|index| {
                let entry = index.schedules.get_mut(&created.id).unwrap();
                entry.state.next_fire_at = Some(now - Duration::seconds(900));
                Ok(())
            })
            .await
            .unwrap();

        let engine = default_trigger_engine();
        let claimed = store
            .claim_due_runs_with_engine(now, engine.as_ref())
            .await
            .unwrap();
        assert!(claimed.is_empty());

        let updated = store.get_schedule(&created.id).await.unwrap();
        assert_eq!(updated.state.last_scheduled_at, Some(now));
        assert!(updated.state.next_fire_at.is_some_and(|next| next > now));
        assert_eq!(updated.state.queued_run_count, 0);
    }

    #[tokio::test]
    async fn overlap_skip_does_not_dispatch_when_running() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();
        let now = DateTime::parse_from_rfc3339("2026-04-04T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let created = store
            .create_schedule_with_definition(
                "skip-overlap".to_string(),
                true,
                ScheduleRunConfig::default(),
                ScheduleDefinitionChanges {
                    trigger: Some(ScheduleTrigger::Interval {
                        every_seconds: 300,
                        anchor_at: None,
                    }),
                    overlap_policy: Some(OverlapPolicy::Skip),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        store
            .update_index(|index| {
                let entry = index.schedules.get_mut(&created.id).unwrap();
                entry.state.next_fire_at = Some(now);
                entry.state.running_run_count = 1;
                Ok(())
            })
            .await
            .unwrap();

        let engine = default_trigger_engine();
        let claimed = store
            .claim_due_runs_with_engine(now, engine.as_ref())
            .await
            .unwrap();
        assert!(claimed.is_empty());

        let updated = store.get_schedule(&created.id).await.unwrap();
        assert_eq!(updated.state.running_run_count, 1);
        assert!(updated.state.next_fire_at.is_some_and(|next| next > now));
        assert_eq!(updated.state.queued_run_count, 0);
    }

    #[tokio::test]
    async fn overlap_queue_one_does_not_add_more_than_one_pending_run() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();
        let now = DateTime::parse_from_rfc3339("2026-04-04T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let created = store
            .create_schedule_with_definition(
                "queue-one".to_string(),
                true,
                ScheduleRunConfig::default(),
                ScheduleDefinitionChanges {
                    trigger: Some(ScheduleTrigger::Interval {
                        every_seconds: 300,
                        anchor_at: None,
                    }),
                    overlap_policy: Some(OverlapPolicy::QueueOne),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        store
            .update_index(|index| {
                let entry = index.schedules.get_mut(&created.id).unwrap();
                entry.state.next_fire_at = Some(now);
                entry.state.queued_run_count = 1;
                Ok(())
            })
            .await
            .unwrap();

        let engine = default_trigger_engine();
        let claimed = store
            .claim_due_runs_with_engine(now, engine.as_ref())
            .await
            .unwrap();
        assert!(claimed.is_empty());

        let updated = store.get_schedule(&created.id).await.unwrap();
        assert_eq!(updated.state.queued_run_count, 1);
    }

    #[tokio::test]
    async fn overlap_queue_one_limits_catch_up_to_single_pending_run() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();
        let now = DateTime::parse_from_rfc3339("2026-04-04T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let created = store
            .create_schedule_with_definition(
                "queue-one-catchup".to_string(),
                true,
                ScheduleRunConfig::default(),
                ScheduleDefinitionChanges {
                    trigger: Some(ScheduleTrigger::Interval {
                        every_seconds: 300,
                        anchor_at: None,
                    }),
                    misfire_policy: Some(MisFirePolicy::CatchUpAll),
                    overlap_policy: Some(OverlapPolicy::QueueOne),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        store
            .update_index(|index| {
                let entry = index.schedules.get_mut(&created.id).unwrap();
                entry.state.next_fire_at = Some(now - Duration::seconds(900));
                Ok(())
            })
            .await
            .unwrap();

        let engine = default_trigger_engine();
        let claimed = store
            .claim_due_runs_with_engine(now, engine.as_ref())
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);

        let updated = store.get_schedule(&created.id).await.unwrap();
        assert_eq!(updated.state.queued_run_count, 1);
        assert!(updated.state.next_fire_at.is_some_and(|next| next > now));
    }

    #[tokio::test]
    async fn mark_run_terminal_records_success_accounting() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();

        let created = store
            .create_schedule(
                "success".to_string(),
                ScheduleTrigger::Interval {
                    every_seconds: 60,
                    anchor_at: None,
                },
                true,
                ScheduleRunConfig::default(),
            )
            .await
            .unwrap();

        store
            .update_index(|index| {
                let entry = index.schedules.get_mut(&created.id).unwrap();
                entry.state.running_run_count = 1;
                entry.state.consecutive_failures = 2;
                Ok(())
            })
            .await
            .unwrap();

        store
            .mark_run_terminal(&created.id, "run-success", ScheduleRunStatus::Success, None)
            .await
            .unwrap();

        let updated = store.get_schedule(&created.id).await.unwrap();
        assert_eq!(updated.state.running_run_count, 0);
        assert!(updated.state.last_finished_at.is_some());
        assert!(updated.state.last_success_at.is_some());
        assert_eq!(updated.state.total_run_count, 1);
        assert_eq!(updated.state.total_success_count, 1);
        assert_eq!(updated.state.total_failure_count, 0);
        assert_eq!(updated.state.consecutive_failures, 0);
    }

    #[tokio::test]
    async fn mark_run_terminal_records_failure_accounting() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();

        let created = store
            .create_schedule(
                "failure".to_string(),
                ScheduleTrigger::Interval {
                    every_seconds: 60,
                    anchor_at: None,
                },
                true,
                ScheduleRunConfig::default(),
            )
            .await
            .unwrap();

        store
            .update_index(|index| {
                let entry = index.schedules.get_mut(&created.id).unwrap();
                entry.state.running_run_count = 1;
                Ok(())
            })
            .await
            .unwrap();

        store
            .mark_run_terminal(&created.id, "run-failed", ScheduleRunStatus::Failed, None)
            .await
            .unwrap();
        store
            .update_index(|index| {
                let entry = index.schedules.get_mut(&created.id).unwrap();
                entry.state.running_run_count = 1;
                Ok(())
            })
            .await
            .unwrap();
        store
            .mark_run_terminal(
                &created.id,
                "run-cancelled",
                ScheduleRunStatus::Cancelled,
                None,
            )
            .await
            .unwrap();

        let updated = store.get_schedule(&created.id).await.unwrap();
        assert_eq!(updated.state.running_run_count, 0);
        assert!(updated.state.last_finished_at.is_some());
        assert!(updated.state.last_failure_at.is_some());
        assert_eq!(updated.state.total_run_count, 2);
        assert_eq!(updated.state.total_success_count, 0);
        assert_eq!(updated.state.total_failure_count, 2);
        assert_eq!(updated.state.consecutive_failures, 2);
    }

    #[tokio::test]
    async fn mark_run_dequeued_without_start_counts_missed_occurrence() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();

        let created = store
            .create_schedule(
                "missed".to_string(),
                ScheduleTrigger::Interval {
                    every_seconds: 60,
                    anchor_at: None,
                },
                true,
                ScheduleRunConfig::default(),
            )
            .await
            .unwrap();

        store
            .update_index(|index| {
                let entry = index.schedules.get_mut(&created.id).unwrap();
                entry.state.queued_run_count = 1;
                Ok(())
            })
            .await
            .unwrap();

        store
            .mark_run_dequeued_without_start(&created.id, "run-missed", None)
            .await
            .unwrap();

        let updated = store.get_schedule(&created.id).await.unwrap();
        assert_eq!(updated.state.queued_run_count, 0);
        assert_eq!(updated.state.total_missed_count, 1);
        let record = store
            .get_run_record("run-missed")
            .await
            .expect("run record should be created for missed dequeue");
        assert_eq!(record.status, ScheduleRunStatus::Missed);
        assert!(record.completed_at.is_some());
    }

    #[tokio::test]
    async fn create_run_now_persists_queued_run_record() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();

        let created = store
            .create_schedule(
                "run-now".to_string(),
                ScheduleTrigger::Interval {
                    every_seconds: 60,
                    anchor_at: None,
                },
                true,
                ScheduleRunConfig::default(),
            )
            .await
            .unwrap();

        let claimed = store
            .create_run_now(&created.id)
            .await
            .unwrap()
            .expect("run descriptor should be created");

        let record = store
            .get_run_record(&claimed.run_id)
            .await
            .expect("queued run record should exist");
        assert_eq!(record.schedule_id, created.id);
        assert_eq!(record.status, ScheduleRunStatus::Queued);
        assert_eq!(record.claimed_at, claimed.claimed_at);
        assert_eq!(record.scheduled_for, claimed.scheduled_for);
        assert!(!claimed.was_catch_up);
    }

    #[tokio::test]
    async fn mark_run_started_and_terminal_updates_run_record_fields() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();

        let created = store
            .create_schedule(
                "run-record-lifecycle".to_string(),
                ScheduleTrigger::Interval {
                    every_seconds: 60,
                    anchor_at: None,
                },
                true,
                ScheduleRunConfig::default(),
            )
            .await
            .unwrap();

        let claimed = store
            .create_run_now(&created.id)
            .await
            .unwrap()
            .expect("run descriptor should be created");

        store
            .mark_run_started(&created.id, &claimed.run_id)
            .await
            .unwrap();
        store
            .bind_run_session(&created.id, &claimed.run_id, "session-1")
            .await
            .unwrap();
        store
            .mark_run_terminal(
                &created.id,
                &claimed.run_id,
                ScheduleRunStatus::Success,
                Some("ok".to_string()),
            )
            .await
            .unwrap();

        let record = store
            .get_run_record(&claimed.run_id)
            .await
            .expect("run record should exist");
        assert_eq!(record.status, ScheduleRunStatus::Success);
        assert!(record.started_at.is_some());
        assert!(record.completed_at.is_some());
        assert_eq!(record.session_id.as_deref(), Some("session-1"));
        assert!(record.dispatch_lag_ms.is_some());
        assert!(record.execution_duration_ms.is_some());
        assert_eq!(record.outcome_reason.as_deref(), Some("ok"));
    }

    #[tokio::test]
    async fn delete_schedule_removes_associated_run_records() {
        let dir = tempdir().unwrap();
        let store = ScheduleStore::new(dir.path().to_path_buf()).await.unwrap();

        let created = store
            .create_schedule(
                "cleanup-history".to_string(),
                ScheduleTrigger::Interval {
                    every_seconds: 60,
                    anchor_at: None,
                },
                true,
                ScheduleRunConfig::default(),
            )
            .await
            .unwrap();

        let claimed = store
            .create_run_now(&created.id)
            .await
            .unwrap()
            .expect("run descriptor should be created");
        assert!(store.get_run_record(&claimed.run_id).await.is_some());

        let deleted = store.delete_schedule(&created.id).await.unwrap();
        assert!(deleted);
        assert!(store.get_run_record(&claimed.run_id).await.is_none());
    }
}
