//! Numeric timings and counts on existing tool and model spans.

use std::time::Duration;

use opentelemetry::trace::TraceContextExt;
use opentelemetry::{Context, KeyValue};

use super::Observation;

const GREP_QUEUE: &str = "bamboo.perf.grep.queue_us";
const GREP_SCAN: &str = "bamboo.perf.grep.scan_us";
const GREP_FILES: &str = "bamboo.perf.grep.files_scanned";
const GREP_BYTES: &str = "bamboo.perf.grep.bytes_scanned";
const GREP_MATCHES: &str = "bamboo.perf.grep.matches";
const RIPGREP_FILES: &str = "bamboo.perf.grep.ripgrep_files";
const CACHE_HITS: &str = "bamboo.perf.token_cache.process_hits";
const CACHE_MISSES: &str = "bamboo.perf.token_cache.process_misses";
const CACHE_ENTRIES: &str = "bamboo.perf.token_cache.process_entries";
const FIRST_TEXT: &str = "bamboo.perf.model.first_text_us";
const GEMINI_TYPED: &str = "bamboo.perf.gemini.typed_text_frames";
const GEMINI_JSON: &str = "bamboo.perf.gemini.full_json_frames";

pub(super) const LOCAL_FIELDS: &[(&str, &str)] = &[
    (GREP_QUEUE, "grep_queue_us"),
    (GREP_SCAN, "grep_scan_us"),
    (GREP_FILES, "grep_files_scanned"),
    (GREP_BYTES, "grep_bytes_scanned"),
    (GREP_MATCHES, "grep_matches"),
    (RIPGREP_FILES, "grep_ripgrep_files"),
    (CACHE_HITS, "token_cache_process_hits"),
    (CACHE_MISSES, "token_cache_process_misses"),
    (CACHE_ENTRIES, "token_cache_process_entries"),
    (FIRST_TEXT, "model_first_text_us"),
    (GEMINI_TYPED, "gemini_typed_text_frames"),
    (GEMINI_JSON, "gemini_full_json_frames"),
];

#[derive(Default)]
pub struct GrepPerformance {
    pub queue_time: Duration,
    pub scan_time: Duration,
    pub files_scanned: u64,
    pub bytes_scanned: u64,
    pub matches: u64,
    pub ripgrep_files: u64,
}

fn micros(value: Duration) -> u64 {
    u64::try_from(value.as_micros()).unwrap_or(u64::MAX)
}

fn record(context: &Context, fields: &[(&'static str, u64)]) {
    let span = context.span();
    if span.is_recording() {
        for &(key, value) in fields {
            span.set_attribute(KeyValue::new(key, i64::try_from(value).unwrap_or(i64::MAX)));
        }
    }
}

/// Attach worker timing and counts to the current Grep tool observation.
pub fn record_grep_performance(stats: &GrepPerformance) {
    record(
        &Context::current(),
        &[
            (GREP_QUEUE, micros(stats.queue_time)),
            (GREP_SCAN, micros(stats.scan_time)),
            (GREP_FILES, stats.files_scanned),
            (GREP_BYTES, stats.bytes_scanned),
            (GREP_MATCHES, stats.matches),
            (RIPGREP_FILES, stats.ripgrep_files),
        ],
    );
}

/// Record upstream text arrival on the current request, including silent streams.
pub fn record_first_model_text(elapsed: Duration) {
    record(&Context::current(), &[(FIRST_TEXT, micros(elapsed))]);
}

impl Observation {
    pub fn is_recording(&self) -> bool {
        self.context.span().is_recording()
    }

    pub fn gemini_decode_counts(&self, typed_text: u64, full_json: u64) {
        record(
            &self.context,
            &[(GEMINI_TYPED, typed_text), (GEMINI_JSON, full_json)],
        );
    }

    /// These snapshots are process-wide, including calls in concurrent sessions.
    pub fn token_cache_snapshot(&self, hits: u64, misses: u64, entries: usize) {
        record(
            &self.context,
            &[
                (CACHE_HITS, hits),
                (CACHE_MISSES, misses),
                (CACHE_ENTRIES, entries as u64),
            ],
        );
    }
}
