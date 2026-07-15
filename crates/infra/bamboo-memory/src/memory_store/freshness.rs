use chrono::Utc;

use super::parse_rfc3339;
use super::TemporalGranularity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreshnessKind {
    Index,
    RecalledMemory,
    StateLikeClaim,
}

/// Age (days, based on `updated_at`) after which a `day`-granularity memory is
/// conservatively marked Stale by the freshness gardener (issue #61 phase 2).
/// Day-level memories are meant to capture today's working context, so roughly a
/// week without a touch is a reasonable, conservative staleness signal — long
/// enough to survive a normal weekend gap, short enough that genuinely stale
/// day-level context doesn't linger in recall for weeks.
pub const DAY_GRANULARITY_STALE_AFTER_DAYS: i64 = 7;

/// Age (days, based on `updated_at`) after which a `week`-granularity memory is
/// conservatively marked Stale. Sprint-level content is expected to stay relevant
/// roughly a month, so this is looser than the day threshold — weekly memories
/// shouldn't be churned as aggressively as daily ones.
pub const WEEK_GRANULARITY_STALE_AFTER_DAYS: i64 = 30;

/// Whether a memory with the given (optional) granularity has crossed its
/// conservative auto-staleness window, based on `updated_at` age. Only `day` and
/// `week` — the two granularities the issue's Prefix Cache Friendly constraint
/// calls "high churn" (see [`TemporalGranularity::is_high_churn`]) — ever expire
/// here; `None` and month/quarter/year never do, since those are meant to be
/// long-lived by design and the freshness gardener must stay conservative (no
/// silent reclassification of memories that were never tagged with a temporal
/// dimension). Callers are expected to additionally gate on
/// `frontmatter.status == Active` before acting — this is purely the time-based
/// predicate.
pub fn granularity_expired(granularity: Option<TemporalGranularity>, updated_at: &str) -> bool {
    let Some(age_days) = memory_age_days(updated_at) else {
        return false;
    };
    match granularity {
        Some(TemporalGranularity::Day) => age_days > DAY_GRANULARITY_STALE_AFTER_DAYS,
        Some(TemporalGranularity::Week) => age_days > WEEK_GRANULARITY_STALE_AFTER_DAYS,
        _ => false,
    }
}

pub fn memory_age_days(updated_at: &str) -> Option<i64> {
    let updated_at = parse_rfc3339(updated_at)?;
    let now = Utc::now();
    let clamped = if updated_at > now { now } else { updated_at };
    Some((now - clamped).num_days())
}

pub fn memory_age_label(updated_at: &str) -> Option<String> {
    let days = memory_age_days(updated_at)?;
    Some(match days {
        0 => "today".to_string(),
        1 => "1 day old".to_string(),
        _ => format!("{days} days old"),
    })
}

pub fn memory_freshness_text(updated_at: &str, kind: FreshnessKind) -> Option<String> {
    let days = memory_age_days(updated_at)?;
    if days <= 1 {
        return None;
    }

    let age_label = memory_age_label(updated_at)?;
    let text = match kind {
        FreshnessKind::Index => {
            if days <= 7 {
                format!(
                    "Historical memory index entry ({age_label}); verify against current tools/files before treating it as live project state."
                )
            } else {
                format!(
                    "Older memory index entry ({age_label}); verify against current tools/files before treating it as current project truth."
                )
            }
        }
        FreshnessKind::RecalledMemory => {
            if days <= 7 {
                format!(
                    "Historical memory ({age_label}); verify against current task context before treating it as still current."
                )
            } else {
                format!(
                    "Older historical memory ({age_label}); verify against current tools/files before relying on it."
                )
            }
        }
        FreshnessKind::StateLikeClaim => {
            if days <= 7 {
                format!(
                    "Historical state-like memory ({age_label}); verify against current code/config before asserting it as fact."
                )
            } else {
                format!(
                    "Older state-like memory ({age_label}); verify against current code/config before asserting it as fact."
                )
            }
        }
    };

    Some(text)
}

pub fn render_memory_freshness_note(updated_at: &str, kind: FreshnessKind) -> Option<String> {
    memory_freshness_text(updated_at, kind).map(|text| format!("Note: {text}"))
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::*;

    #[test]
    fn same_day_timestamp_has_no_warning() {
        let timestamp = Utc::now().to_rfc3339();
        assert_eq!(memory_age_days(&timestamp), Some(0));
        assert_eq!(memory_age_label(&timestamp).as_deref(), Some("today"));
        assert!(memory_freshness_text(&timestamp, FreshnessKind::Index).is_none());
    }

    #[test]
    fn multi_day_timestamp_gets_warning_and_age_label() {
        let timestamp = (Utc::now() - Duration::days(3)).to_rfc3339();
        assert_eq!(memory_age_days(&timestamp), Some(3));
        assert_eq!(memory_age_label(&timestamp).as_deref(), Some("3 days old"));
        let text = memory_freshness_text(&timestamp, FreshnessKind::RecalledMemory)
            .expect("warning expected");
        assert!(text.contains("3 days old"));
        assert!(text.contains("Historical memory"));
    }

    #[test]
    fn invalid_timestamp_degrades_safely() {
        assert!(memory_age_days("not-a-timestamp").is_none());
        assert!(memory_age_label("not-a-timestamp").is_none());
        assert!(memory_freshness_text("not-a-timestamp", FreshnessKind::Index).is_none());
        assert!(render_memory_freshness_note("not-a-timestamp", FreshnessKind::Index).is_none());
    }

    #[test]
    fn future_timestamp_is_clamped_to_zero_age() {
        let timestamp = (Utc::now() + Duration::days(5)).to_rfc3339();
        assert_eq!(memory_age_days(&timestamp), Some(0));
        assert_eq!(memory_age_label(&timestamp).as_deref(), Some("today"));
        assert!(memory_freshness_text(&timestamp, FreshnessKind::Index).is_none());
    }

    #[test]
    fn day_granularity_expires_after_its_window_not_before() {
        let just_under =
            (Utc::now() - Duration::days(DAY_GRANULARITY_STALE_AFTER_DAYS)).to_rfc3339();
        let just_over =
            (Utc::now() - Duration::days(DAY_GRANULARITY_STALE_AFTER_DAYS + 1)).to_rfc3339();
        assert!(!granularity_expired(
            Some(TemporalGranularity::Day),
            &just_under
        ));
        assert!(granularity_expired(
            Some(TemporalGranularity::Day),
            &just_over
        ));
    }

    #[test]
    fn week_granularity_uses_a_longer_window_than_day() {
        let mid_window =
            (Utc::now() - Duration::days(DAY_GRANULARITY_STALE_AFTER_DAYS + 1)).to_rfc3339();
        // Past the day threshold but still inside the week threshold.
        assert!(!granularity_expired(
            Some(TemporalGranularity::Week),
            &mid_window
        ));
        let past_week_window =
            (Utc::now() - Duration::days(WEEK_GRANULARITY_STALE_AFTER_DAYS + 1)).to_rfc3339();
        assert!(granularity_expired(
            Some(TemporalGranularity::Week),
            &past_week_window
        ));
    }

    #[test]
    fn coarse_and_unset_granularity_never_expire() {
        let ancient = (Utc::now() - Duration::days(10_000)).to_rfc3339();
        assert!(!granularity_expired(None, &ancient));
        assert!(!granularity_expired(
            Some(TemporalGranularity::Month),
            &ancient
        ));
        assert!(!granularity_expired(
            Some(TemporalGranularity::Quarter),
            &ancient
        ));
        assert!(!granularity_expired(
            Some(TemporalGranularity::Year),
            &ancient
        ));
    }

    #[test]
    fn invalid_timestamp_never_expires() {
        assert!(!granularity_expired(
            Some(TemporalGranularity::Day),
            "not-a-timestamp"
        ));
    }

    #[test]
    fn state_like_claim_uses_stronger_wording_than_index() {
        let timestamp = (Utc::now() - Duration::days(10)).to_rfc3339();
        let index_text = memory_freshness_text(&timestamp, FreshnessKind::Index)
            .expect("index warning expected");
        let state_text = memory_freshness_text(&timestamp, FreshnessKind::StateLikeClaim)
            .expect("state-like warning expected");
        assert!(index_text.contains("current project truth"));
        assert!(state_text.contains("current code/config"));
        assert_ne!(index_text, state_text);
    }
}
