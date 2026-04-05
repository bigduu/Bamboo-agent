use std::sync::Arc;

use chrono::{
    DateTime, Datelike, Days, Duration, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, TimeZone,
    Utc, Weekday,
};
use chrono_tz::Tz;
use cron::Schedule as CronSchedule;
use thiserror::Error;

use super::domain::{ScheduleTrigger, ScheduleWeekday, ScheduleWindow};

pub type DynTriggerEngine = Arc<dyn TriggerEngine>;

/// Interface for computing schedule occurrences.
///
/// Bamboo owns schedule state, policies, and execution. Concrete trigger engines
/// only answer recurrence questions and can be swapped independently.
pub trait TriggerEngine: Send + Sync {
    fn kind(&self) -> TriggerEngineKind;

    fn next_after(
        &self,
        trigger: &ScheduleTrigger,
        timezone: Option<&str>,
        after: DateTime<Utc>,
        window: &ScheduleWindow,
    ) -> Result<Option<DateTime<Utc>>, TriggerComputationError>;

    fn due_between(
        &self,
        trigger: &ScheduleTrigger,
        timezone: Option<&str>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        window: &ScheduleWindow,
        limit: usize,
    ) -> Result<Vec<DateTime<Utc>>, TriggerComputationError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut current = from;
        let mut out = Vec::new();
        while out.len() < limit {
            let Some(next) = self.next_after(trigger, timezone, current, window)? else {
                break;
            };
            if next > to {
                break;
            }
            out.push(next);
            current = next;
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerEngineKind {
    Native,
    CronBacked,
    RRuleBacked,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TriggerComputationError {
    #[error("unsupported trigger kind for engine: {0}")]
    UnsupportedTrigger(&'static str),
    #[error("invalid trigger: {0}")]
    InvalidTrigger(String),
    #[error("timezone support is not available for engine kind {0:?}")]
    UnsupportedTimezone(TriggerEngineKind),
}

/// Minimal built-in trigger engine used as the default interface implementation.
///
/// It owns Bamboo's native recurrence logic for interval/daily/weekly/monthly.
#[derive(Debug, Default)]
pub struct NativeTriggerEngine;

#[derive(Debug, Default)]
pub struct CronBackedTriggerEngine;

#[derive(Debug, Default)]
pub struct CompositeTriggerEngine {
    native: NativeTriggerEngine,
    cron: CronBackedTriggerEngine,
}

impl NativeTriggerEngine {
    pub fn new() -> Self {
        Self
    }
}

impl CronBackedTriggerEngine {
    pub fn new() -> Self {
        Self
    }
}

impl CompositeTriggerEngine {
    pub fn new() -> Self {
        Self {
            native: NativeTriggerEngine::new(),
            cron: CronBackedTriggerEngine::new(),
        }
    }
}

impl TriggerEngine for NativeTriggerEngine {
    fn kind(&self) -> TriggerEngineKind {
        TriggerEngineKind::Native
    }

    fn next_after(
        &self,
        trigger: &ScheduleTrigger,
        timezone: Option<&str>,
        after: DateTime<Utc>,
        window: &ScheduleWindow,
    ) -> Result<Option<DateTime<Utc>>, TriggerComputationError> {
        let next = match trigger {
            ScheduleTrigger::Interval {
                every_seconds,
                anchor_at,
            } => {
                if timezone.is_some() {
                    return Err(TriggerComputationError::UnsupportedTimezone(self.kind()));
                }
                compute_interval_next(*every_seconds, *anchor_at, after)?
            }
            ScheduleTrigger::Daily {
                hour,
                minute,
                second,
            } => compute_daily_next(*hour, *minute, *second, timezone, after)?,
            ScheduleTrigger::Weekly {
                weekdays,
                hour,
                minute,
                second,
            } => compute_weekly_next(weekdays, *hour, *minute, *second, timezone, after)?,
            ScheduleTrigger::Monthly {
                days,
                hour,
                minute,
                second,
            } => compute_monthly_next(days, *hour, *minute, *second, timezone, after)?,
            ScheduleTrigger::Cron { .. } => {
                return Err(TriggerComputationError::UnsupportedTrigger("cron"));
            }
        };

        Ok(apply_window(next, window))
    }
}

fn parse_timezone(timezone: Option<&str>) -> Result<Tz, TriggerComputationError> {
    match timezone {
        Some(value) => value.parse::<Tz>().map_err(|_| {
            TriggerComputationError::InvalidTrigger(format!("invalid timezone: {value}"))
        }),
        None => Ok(chrono_tz::UTC),
    }
}

fn resolve_local_datetime(
    timezone: Tz,
    local: NaiveDateTime,
) -> Result<DateTime<Utc>, TriggerComputationError> {
    match timezone.from_local_datetime(&local) {
        LocalResult::Single(dt) => Ok(dt.with_timezone(&Utc)),
        LocalResult::Ambiguous(first, second) => Ok(first.min(second).with_timezone(&Utc)),
        LocalResult::None => Err(TriggerComputationError::InvalidTrigger(format!(
            "local datetime does not exist in timezone {}: {}",
            timezone, local
        ))),
    }
}

fn local_candidate_utc(
    timezone: Tz,
    date: NaiveDate,
    hour: u8,
    minute: u8,
    second: u8,
) -> Result<DateTime<Utc>, TriggerComputationError> {
    let time =
        NaiveTime::from_hms_opt(hour as u32, minute as u32, second as u32).ok_or_else(|| {
            TriggerComputationError::InvalidTrigger(format!(
                "invalid time components: {:02}:{:02}:{:02}",
                hour, minute, second
            ))
        })?;
    resolve_local_datetime(timezone, NaiveDateTime::new(date, time))
}

fn compute_daily_next(
    hour: u8,
    minute: u8,
    second: u8,
    timezone: Option<&str>,
    after: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, TriggerComputationError> {
    let timezone = parse_timezone(timezone)?;
    let after_local = after.with_timezone(&timezone);
    let mut date = after_local.date_naive();

    for _ in 0..370 {
        let candidate = local_candidate_utc(timezone, date, hour, minute, second)?;
        if candidate > after {
            return Ok(Some(candidate));
        }
        date = date.checked_add_days(Days::new(1)).ok_or_else(|| {
            TriggerComputationError::InvalidTrigger(
                "daily schedule overflowed date range".to_string(),
            )
        })?;
    }

    Err(TriggerComputationError::InvalidTrigger(
        "daily schedule failed to produce next occurrence".to_string(),
    ))
}

fn to_chrono_weekday(weekday: ScheduleWeekday) -> Weekday {
    match weekday {
        ScheduleWeekday::Mon => Weekday::Mon,
        ScheduleWeekday::Tue => Weekday::Tue,
        ScheduleWeekday::Wed => Weekday::Wed,
        ScheduleWeekday::Thu => Weekday::Thu,
        ScheduleWeekday::Fri => Weekday::Fri,
        ScheduleWeekday::Sat => Weekday::Sat,
        ScheduleWeekday::Sun => Weekday::Sun,
    }
}

fn compute_weekly_next(
    weekdays: &[ScheduleWeekday],
    hour: u8,
    minute: u8,
    second: u8,
    timezone: Option<&str>,
    after: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, TriggerComputationError> {
    if weekdays.is_empty() {
        return Err(TriggerComputationError::InvalidTrigger(
            "weekly.weekdays must not be empty".to_string(),
        ));
    }

    let timezone = parse_timezone(timezone)?;
    let after_local = after.with_timezone(&timezone);
    let mut date = after_local.date_naive();
    let allowed: Vec<Weekday> = weekdays.iter().copied().map(to_chrono_weekday).collect();

    for _ in 0..370 {
        if allowed.contains(&date.weekday()) {
            let candidate = local_candidate_utc(timezone, date, hour, minute, second)?;
            if candidate > after {
                return Ok(Some(candidate));
            }
        }
        date = date.checked_add_days(Days::new(1)).ok_or_else(|| {
            TriggerComputationError::InvalidTrigger(
                "weekly schedule overflowed date range".to_string(),
            )
        })?;
    }

    Err(TriggerComputationError::InvalidTrigger(
        "weekly schedule failed to produce next occurrence".to_string(),
    ))
}

fn days_in_month(year: i32, month: u32) -> Result<u32, TriggerComputationError> {
    let first = NaiveDate::from_ymd_opt(year, month, 1).ok_or_else(|| {
        TriggerComputationError::InvalidTrigger(format!("invalid year-month: {year}-{month}"))
    })?;
    let next_month = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
    }
    .ok_or_else(|| {
        TriggerComputationError::InvalidTrigger(format!(
            "invalid next year-month from {year}-{month}"
        ))
    })?;
    Ok(next_month.signed_duration_since(first).num_days() as u32)
}

fn compute_monthly_next(
    days: &[u8],
    hour: u8,
    minute: u8,
    second: u8,
    timezone: Option<&str>,
    after: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, TriggerComputationError> {
    if days.is_empty() {
        return Err(TriggerComputationError::InvalidTrigger(
            "monthly.days must not be empty".to_string(),
        ));
    }
    if days.iter().any(|day| *day == 0 || *day > 31) {
        return Err(TriggerComputationError::InvalidTrigger(
            "monthly.days values must be between 1 and 31".to_string(),
        ));
    }

    let timezone = parse_timezone(timezone)?;
    let after_local = after.with_timezone(&timezone);
    let mut year = after_local.year();
    let mut month = after_local.month();
    let mut sorted_days: Vec<u32> = days.iter().copied().map(u32::from).collect();
    sorted_days.sort_unstable();
    sorted_days.dedup();

    for _ in 0..1200 {
        let max_day = days_in_month(year, month)?;
        for day in &sorted_days {
            if *day > max_day {
                continue;
            }
            let date = NaiveDate::from_ymd_opt(year, month, *day).ok_or_else(|| {
                TriggerComputationError::InvalidTrigger(format!(
                    "invalid monthly occurrence date: {year}-{month}-{day}"
                ))
            })?;
            let candidate = local_candidate_utc(timezone, date, hour, minute, second)?;
            if candidate > after {
                return Ok(Some(candidate));
            }
        }

        if month == 12 {
            year += 1;
            month = 1;
        } else {
            month += 1;
        }
    }

    Err(TriggerComputationError::InvalidTrigger(
        "monthly schedule failed to produce next occurrence".to_string(),
    ))
}

fn compute_interval_next(
    every_seconds: u64,
    anchor_at: Option<DateTime<Utc>>,
    after: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, TriggerComputationError> {
    if every_seconds == 0 {
        return Err(TriggerComputationError::InvalidTrigger(
            "interval.every_seconds must be > 0".to_string(),
        ));
    }

    let duration = Duration::seconds(every_seconds as i64);
    let next = match anchor_at {
        Some(anchor) if anchor > after => anchor,
        Some(anchor) => {
            let elapsed = after.signed_duration_since(anchor).num_seconds();
            let step = every_seconds as i64;
            let intervals_elapsed = elapsed.div_euclid(step) + 1;
            anchor + Duration::seconds(intervals_elapsed * step)
        }
        None => after + duration,
    };

    Ok(Some(next))
}

fn apply_window(
    candidate: Option<DateTime<Utc>>,
    window: &ScheduleWindow,
) -> Option<DateTime<Utc>> {
    let candidate = candidate?;
    if let Some(start_at) = window.start_at {
        if candidate < start_at {
            return Some(start_at);
        }
    }
    if let Some(end_at) = window.end_at {
        if candidate > end_at {
            return None;
        }
    }
    Some(candidate)
}

impl TriggerEngine for CronBackedTriggerEngine {
    fn kind(&self) -> TriggerEngineKind {
        TriggerEngineKind::CronBacked
    }

    fn next_after(
        &self,
        trigger: &ScheduleTrigger,
        timezone: Option<&str>,
        after: DateTime<Utc>,
        window: &ScheduleWindow,
    ) -> Result<Option<DateTime<Utc>>, TriggerComputationError> {
        let ScheduleTrigger::Cron { expr } = trigger else {
            return Err(TriggerComputationError::UnsupportedTrigger(
                trigger.kind_name(),
            ));
        };

        let schedule = expr.parse::<CronSchedule>().map_err(|error| {
            TriggerComputationError::InvalidTrigger(format!(
                "invalid cron expression '{}': {}",
                expr, error
            ))
        })?;

        let timezone = parse_timezone(timezone)?;
        let after_local = after.with_timezone(&timezone);
        let next_local = schedule.after(&after_local).next();
        Ok(apply_window(
            next_local.map(|candidate| candidate.with_timezone(&Utc)),
            window,
        ))
    }
}

impl TriggerEngine for CompositeTriggerEngine {
    fn kind(&self) -> TriggerEngineKind {
        TriggerEngineKind::CronBacked
    }

    fn next_after(
        &self,
        trigger: &ScheduleTrigger,
        timezone: Option<&str>,
        after: DateTime<Utc>,
        window: &ScheduleWindow,
    ) -> Result<Option<DateTime<Utc>>, TriggerComputationError> {
        match trigger {
            ScheduleTrigger::Cron { .. } => self.cron.next_after(trigger, timezone, after, window),
            _ => self.native.next_after(trigger, timezone, after, window),
        }
    }
}

pub fn default_trigger_engine() -> DynTriggerEngine {
    Arc::new(CompositeTriggerEngine::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::schedules::domain::{ScheduleTrigger, ScheduleWeekday};

    #[test]
    fn interval_next_uses_anchor_alignment() {
        let engine = NativeTriggerEngine::new();
        let anchor = DateTime::parse_from_rfc3339("2026-04-04T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let after = DateTime::parse_from_rfc3339("2026-04-04T00:01:01Z")
            .unwrap()
            .with_timezone(&Utc);
        let next = engine
            .next_after(
                &ScheduleTrigger::legacy_interval(60, Some(anchor)),
                None,
                after,
                &ScheduleWindow::default(),
            )
            .unwrap();
        assert_eq!(next, Some(anchor + Duration::seconds(120)));
    }

    #[test]
    fn due_between_returns_bounded_sequence() {
        let engine = NativeTriggerEngine::new();
        let anchor = DateTime::parse_from_rfc3339("2026-04-04T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let from = anchor;
        let to = anchor + Duration::seconds(181);
        let due = engine
            .due_between(
                &ScheduleTrigger::legacy_interval(60, Some(anchor)),
                None,
                from,
                to,
                &ScheduleWindow::default(),
                10,
            )
            .unwrap();
        assert_eq!(due.len(), 3);
        assert_eq!(due[0], anchor + Duration::seconds(60));
        assert_eq!(due[1], anchor + Duration::seconds(120));
        assert_eq!(due[2], anchor + Duration::seconds(180));
    }

    #[test]
    fn daily_next_works_in_utc() {
        let engine = NativeTriggerEngine::new();
        let after = DateTime::parse_from_rfc3339("2026-04-04T09:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let next = engine
            .next_after(
                &ScheduleTrigger::Daily {
                    hour: 10,
                    minute: 0,
                    second: 0,
                },
                None,
                after,
                &ScheduleWindow::default(),
            )
            .unwrap();
        assert_eq!(
            next,
            Some(
                DateTime::parse_from_rfc3339("2026-04-04T10:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
    }

    #[test]
    fn weekly_next_works_in_utc() {
        let engine = NativeTriggerEngine::new();
        let after = DateTime::parse_from_rfc3339("2026-04-04T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let next = engine
            .next_after(
                &ScheduleTrigger::Weekly {
                    weekdays: vec![ScheduleWeekday::Mon],
                    hour: 9,
                    minute: 0,
                    second: 0,
                },
                None,
                after,
                &ScheduleWindow::default(),
            )
            .unwrap();
        assert_eq!(
            next,
            Some(
                DateTime::parse_from_rfc3339("2026-04-06T09:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
    }

    #[test]
    fn daily_next_respects_timezone() {
        let engine = NativeTriggerEngine::new();
        let after = DateTime::parse_from_rfc3339("2026-04-04T00:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let next = engine
            .next_after(
                &ScheduleTrigger::Daily {
                    hour: 9,
                    minute: 0,
                    second: 0,
                },
                Some("Asia/Shanghai"),
                after,
                &ScheduleWindow::default(),
            )
            .unwrap();
        assert_eq!(
            next,
            Some(
                DateTime::parse_from_rfc3339("2026-04-04T01:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
    }

    #[test]
    fn monthly_next_uses_same_month_when_valid_day_remains() {
        let engine = NativeTriggerEngine::new();
        let after = DateTime::parse_from_rfc3339("2026-04-04T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let next = engine
            .next_after(
                &ScheduleTrigger::Monthly {
                    days: vec![10, 20],
                    hour: 9,
                    minute: 0,
                    second: 0,
                },
                None,
                after,
                &ScheduleWindow::default(),
            )
            .unwrap();
        assert_eq!(
            next,
            Some(
                DateTime::parse_from_rfc3339("2026-04-10T09:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
    }

    #[test]
    fn monthly_next_skips_short_month_for_missing_day() {
        let engine = NativeTriggerEngine::new();
        let after = DateTime::parse_from_rfc3339("2026-04-30T23:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let next = engine
            .next_after(
                &ScheduleTrigger::Monthly {
                    days: vec![31],
                    hour: 9,
                    minute: 0,
                    second: 0,
                },
                None,
                after,
                &ScheduleWindow::default(),
            )
            .unwrap();
        assert_eq!(
            next,
            Some(
                DateTime::parse_from_rfc3339("2026-05-31T09:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
    }

    #[test]
    fn monthly_next_respects_timezone() {
        let engine = NativeTriggerEngine::new();
        let after = DateTime::parse_from_rfc3339("2026-04-30T15:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let next = engine
            .next_after(
                &ScheduleTrigger::Monthly {
                    days: vec![1],
                    hour: 9,
                    minute: 0,
                    second: 0,
                },
                Some("Asia/Shanghai"),
                after,
                &ScheduleWindow::default(),
            )
            .unwrap();
        assert_eq!(
            next,
            Some(
                DateTime::parse_from_rfc3339("2026-05-01T01:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
    }

    #[test]
    fn cron_backed_next_works_in_utc() {
        let engine = CronBackedTriggerEngine::new();
        let after = DateTime::parse_from_rfc3339("2026-04-04T10:15:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let next = engine
            .next_after(
                &ScheduleTrigger::Cron {
                    expr: "0 */30 * * * * *".to_string(),
                },
                Some("UTC"),
                after,
                &ScheduleWindow::default(),
            )
            .unwrap();
        assert_eq!(
            next,
            Some(
                DateTime::parse_from_rfc3339("2026-04-04T10:30:00Z")
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
    }

    #[test]
    fn cron_backed_next_respects_timezone() {
        let engine = CronBackedTriggerEngine::new();
        let after = DateTime::parse_from_rfc3339("2026-04-04T00:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let next = engine
            .next_after(
                &ScheduleTrigger::Cron {
                    expr: "0 0 9 * * * *".to_string(),
                },
                Some("Asia/Shanghai"),
                after,
                &ScheduleWindow::default(),
            )
            .unwrap();
        assert_eq!(
            next,
            Some(
                DateTime::parse_from_rfc3339("2026-04-04T01:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
    }
}
