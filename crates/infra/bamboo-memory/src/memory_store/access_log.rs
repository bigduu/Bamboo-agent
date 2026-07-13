//! L5 cheap access-frequency signal (#264, follow-up to #263's `memory_value`).
//!
//! The RFC's intended L5 keep-value signal is `recency × access × confidence`,
//! but the access term was omitted in #263 because it wasn't tracked:
//! `DurableMemoryRetrieval::last_accessed_at` exists but writing it back onto the
//! recalled document on the read path would force a full per-scope index rebuild
//! on every recall (the store rebuilds all per-scope indexes on every document
//! write) — unacceptable on a hot path.
//!
//! Instead, recall appends a tiny `{id, ts}` line to a per-scope append-only
//! `access_log.jsonl` (see [`super::store::MemoryStore::record_memory_accesses`]):
//! O(ids) appends, no document rewrite, no index rebuild. The capacity gardener
//! reads/aggregates that log lazily (only when it actually runs an eviction pass —
//! not on the hot path) into an [`AccessStats`] per memory id, and
//! [`access_multiplier`] turns that into the multiplier `memory_value` documents
//! as its seam.
//!
//! This module is pure logic (parsing, aggregation, the multiplier formula,
//! compaction policy) with no I/O — [`super::store::MemoryStore`] owns reading,
//! writing, locking, and compacting the actual file.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::parse_rfc3339;

/// File name of the per-scope access log, alongside the existing audit logs in
/// the scope's `logs/` dir (see `MemoryPathResolver::logs_dir`).
pub(crate) const ACCESS_LOG_FILE: &str = "access_log.jsonl";

/// Append-triggered compaction threshold: once the log exceeds this many bytes,
/// the next append rewrites it via [`compact_entries`] instead of just appending.
/// ~40-60 bytes/line (short id + rfc3339 ts) → 256 KiB is on the order of a few
/// thousand entries per scope, generous for a hot scope while keeping the rewrite
/// (a full read + filter + atomic write) cheap and infrequent — amortized O(1)
/// per append, same idea as a growable vector's doubling.
pub(crate) const ACCESS_LOG_COMPACT_TRIGGER_BYTES: u64 = 256 * 1024;

/// Aging window for compaction: entries older than this are dropped. Matches the
/// horizon over which "recently accessed" is a meaningful signal for keep-value
/// scoring — an access from a year ago says nothing about whether a memory is
/// worth keeping today.
const ACCESS_LOG_AGING_WINDOW_DAYS: i64 = 90;

/// Hard cap on retained entries after compaction (oldest dropped first, entries
/// are stored in append/chronological order) — bounds the log's size even for a
/// scope so hot that it fills the aging window with more than this many accesses
/// before the byte trigger next fires.
const ACCESS_LOG_MAX_RETAINED_ENTRIES: usize = 20_000;

/// Half-life (days) used to decay an individual access event's contribution to
/// [`AccessStats::score`]. Deliberately the same 30-day scale `memory_value` uses
/// for `recency(updated_at)`, so the access signal ages on a comparable clock to
/// the signal it multiplies.
const ACCESS_DECAY_HALF_LIFE_DAYS: f64 = 30.0;

/// Upper bound on how much a fully "hot" access history can multiply
/// `memory_value` by. `access_multiplier` ranges over `[1.0, 1.0 + WEIGHT]`:
/// `1.0` (neutral, no change) when a memory has no access-log data, rising
/// monotonically with more/more-recent accesses but never dropping below `1.0`
/// — so access data can only ever help a memory survive eviction, never newly
/// penalize one for lacking it (many memories legitimately have none: this
/// signal is new, and callers that don't wire the log at all keep scoring
/// unchanged). Kept modest relative to the ~[0.125, 1.0] range of
/// `confidence × stale_penalty` so access nudges close rankings rather than
/// overriding strong recency/confidence/staleness signals.
const ACCESS_MULTIPLIER_WEIGHT: f64 = 1.0;

/// Saturation constant for the score → multiplier curve (see
/// [`access_multiplier`]): the decayed-access score at which the multiplier has
/// closed half the gap to its `1.0 + ACCESS_MULTIPLIER_WEIGHT` ceiling. Roughly
/// "3 same-day accesses" (score ≈ 3) gets a memory halfway to the cap; a single
/// stale access barely moves it.
const ACCESS_SATURATION_K: f64 = 3.0;

/// One `{id, ts}` line in a scope's `access_log.jsonl` — `ts` is UTC rfc3339.
/// Deliberately minimal (no query text, no scope) so an append is as cheap as
/// possible; the file's own path already carries the scope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AccessLogEntry {
    pub id: String,
    pub ts: String,
}

/// Aggregated access history for one memory id, as of the aggregation time.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct AccessStats {
    /// Raw count of log lines for this id (undecayed; informational / testable,
    /// not itself used by [`access_multiplier`]).
    pub count: usize,
    /// Recency-decayed sum of access events — see [`ACCESS_DECAY_HALF_LIFE_DAYS`].
    /// Always `>= 0.0`.
    pub score: f64,
}

/// Parse a raw `access_log.jsonl` file's contents into entries, silently
/// skipping blank and malformed lines — an access log is a best-effort signal,
/// not a durability-critical artifact, so a torn or corrupt line should degrade
/// the signal, not fail the read.
pub(crate) fn parse_access_log(raw: &str) -> Vec<AccessLogEntry> {
    raw.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return None;
            }
            serde_json::from_str::<AccessLogEntry>(trimmed).ok()
        })
        .collect()
}

/// Aggregate parsed log entries into per-id [`AccessStats`] as of `now`. Entries
/// with an empty id or an unparseable timestamp are skipped (best-effort).
pub(crate) fn aggregate_access_stats(
    entries: &[AccessLogEntry],
    now: DateTime<Utc>,
) -> HashMap<String, AccessStats> {
    let mut stats: HashMap<String, AccessStats> = HashMap::new();
    for entry in entries {
        let id = entry.id.trim();
        if id.is_empty() {
            continue;
        }
        let Some(ts) = parse_rfc3339(&entry.ts) else {
            continue;
        };
        let age_days = (now - ts).num_days().max(0) as f64;
        let weight = 0.5_f64.powf(age_days / ACCESS_DECAY_HALF_LIFE_DAYS);
        let entry_stats = stats.entry(id.to_string()).or_default();
        entry_stats.count += 1;
        entry_stats.score += weight;
    }
    stats
}

/// The `memory_value` access-multiplier: `1.0` (neutral) with no data, rising
/// monotonically toward `1.0 + ACCESS_MULTIPLIER_WEIGHT` as `stats.score` grows,
/// via a hyperbolic saturation curve (`score / (score + K)`) that is
/// self-bounding to `[0, 1)` by construction — no separate clamp needed to
/// guarantee the multiplier never drops below `1.0` or exceeds the cap.
pub(crate) fn access_multiplier(stats: Option<&AccessStats>) -> f64 {
    let score = stats.map(|value| value.score).unwrap_or(0.0);
    if score <= 0.0 {
        return 1.0;
    }
    let normalized = score / (score + ACCESS_SATURATION_K);
    1.0 + ACCESS_MULTIPLIER_WEIGHT * normalized
}

/// Compaction policy applied when the log exceeds
/// [`ACCESS_LOG_COMPACT_TRIGGER_BYTES`]: drop entries older than
/// [`ACCESS_LOG_AGING_WINDOW_DAYS`], then — if still over
/// [`ACCESS_LOG_MAX_RETAINED_ENTRIES`] — drop the oldest until it fits. `entries`
/// must be in append (chronological) order, which is how they're read back from
/// the file.
pub(crate) fn compact_entries(
    mut entries: Vec<AccessLogEntry>,
    now: DateTime<Utc>,
) -> Vec<AccessLogEntry> {
    entries.retain(|entry| {
        parse_rfc3339(&entry.ts)
            .map(|ts| (now - ts).num_days() <= ACCESS_LOG_AGING_WINDOW_DAYS)
            // Malformed timestamp: can't tell its age, so don't keep it around
            // forever — drop it on compaction.
            .unwrap_or(false)
    });
    if entries.len() > ACCESS_LOG_MAX_RETAINED_ENTRIES {
        let drop_count = entries.len() - ACCESS_LOG_MAX_RETAINED_ENTRIES;
        entries.drain(0..drop_count);
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, ts: DateTime<Utc>) -> AccessLogEntry {
        AccessLogEntry {
            id: id.to_string(),
            ts: ts.to_rfc3339(),
        }
    }

    #[test]
    fn multiplier_is_neutral_with_no_data() {
        assert_eq!(access_multiplier(None), 1.0);
        assert_eq!(access_multiplier(Some(&AccessStats::default())), 1.0);
    }

    #[test]
    fn multiplier_increases_monotonically_with_score_and_stays_bounded() {
        let low = access_multiplier(Some(&AccessStats {
            count: 1,
            score: 0.5,
        }));
        let mid = access_multiplier(Some(&AccessStats {
            count: 5,
            score: 3.0,
        }));
        let high = access_multiplier(Some(&AccessStats {
            count: 500,
            score: 10_000.0,
        }));
        assert!(low > 1.0, "any positive score beats the 1.0 baseline");
        assert!(mid > low);
        assert!(high > mid);
        assert!(
            high < 1.0 + ACCESS_MULTIPLIER_WEIGHT,
            "even an extreme score stays strictly below the cap"
        );
    }

    #[test]
    fn aggregate_counts_and_decays_by_age() {
        let now = Utc::now();
        let entries = vec![
            entry("mem-a", now),
            entry("mem-a", now - chrono::Duration::days(30)),
            entry("mem-b", now - chrono::Duration::days(365)),
        ];
        let stats = aggregate_access_stats(&entries, now);
        let a = stats.get("mem-a").unwrap();
        assert_eq!(a.count, 2);
        // today's access weighs 1.0, the 30-day-old one weighs 0.5 (one half-life).
        assert!((a.score - 1.5).abs() < 0.01, "score = {}", a.score);

        let b = stats.get("mem-b").unwrap();
        assert_eq!(b.count, 1);
        assert!(b.score > 0.0 && b.score < 0.01, "score = {}", b.score);
    }

    #[test]
    fn aggregate_skips_malformed_entries() {
        let now = Utc::now();
        let entries = vec![
            AccessLogEntry {
                id: "".to_string(),
                ts: now.to_rfc3339(),
            },
            AccessLogEntry {
                id: "mem-c".to_string(),
                ts: "not-a-timestamp".to_string(),
            },
        ];
        let stats = aggregate_access_stats(&entries, now);
        assert!(stats.is_empty());
    }

    #[test]
    fn parse_access_log_skips_blank_and_malformed_lines() {
        let raw = "{\"id\":\"a\",\"ts\":\"2026-01-01T00:00:00Z\"}\n\nnot json\n{\"id\":\"b\",\"ts\":\"2026-01-02T00:00:00Z\"}\n";
        let entries = parse_access_log(raw);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, "a");
        assert_eq!(entries[1].id, "b");
    }

    #[test]
    fn compact_entries_drops_stale_and_bounds_retained_count() {
        let now = Utc::now();
        let mut entries = vec![entry("old", now - chrono::Duration::days(200))];
        for i in 0..(ACCESS_LOG_MAX_RETAINED_ENTRIES + 500) {
            entries.push(entry(&format!("mem-{i}"), now));
        }
        let compacted = compact_entries(entries, now);
        assert!(!compacted.iter().any(|e| e.id == "old"), "aged out");
        assert_eq!(compacted.len(), ACCESS_LOG_MAX_RETAINED_ENTRIES);
        // Oldest-first drop: the earliest-pushed "recent" entries are the ones cut.
        assert!(!compacted.iter().any(|e| e.id == "mem-0"));
        assert!(compacted
            .iter()
            .any(|e| e.id == format!("mem-{}", ACCESS_LOG_MAX_RETAINED_ENTRIES + 499)));
    }
}
