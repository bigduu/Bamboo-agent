//! Print a token-usage / prompt-cache report for a bamboo home dir or a glob.
//!
//!   cargo run -p bamboo-analytics --example report -- ~/.bamboo
//!   cargo run -p bamboo-analytics --example report -- '/path/sessions/**/token-usage.jsonl'

use std::path::Path;
use std::process::ExitCode;

use bamboo_analytics::{TokenUsageDb, DEFAULT_TTL_SECONDS};

fn main() -> ExitCode {
    let Some(arg) = std::env::args().nth(1) else {
        eprintln!("usage: report <bamboo_home_dir | jsonl_glob>");
        return ExitCode::from(2);
    };

    let db = if arg.contains('*') {
        TokenUsageDb::open_glob(&arg)
    } else {
        TokenUsageDb::open_home(Path::new(&arg))
    };
    let db = match db {
        Ok(db) => db,
        Err(error) => {
            eprintln!("error: {error}");
            return ExitCode::FAILURE;
        }
    };

    println!("== Per-session summary ==");
    println!(
        "{:<28} {:>6} {:>12} {:>12} {:>10} {:>6} {:>14}",
        "session", "calls", "cache_read", "cache_create", "avg_hit", "compac", "savings(tok)"
    );
    match db.session_summary() {
        Ok(rows) => {
            for r in &rows {
                println!(
                    "{:<28} {:>6} {:>12} {:>12} {:>9.1}% {:>6} {:>14.0}",
                    truncate(&r.session_id, 28),
                    r.calls,
                    r.total_cache_read,
                    r.total_cache_creation,
                    r.avg_cached_fraction * 100.0,
                    r.compactions,
                    r.est_savings_vs_uncached,
                );
            }
            if rows.is_empty() {
                println!("(no records)");
            }
        }
        Err(error) => eprintln!("session_summary failed: {error}"),
    }

    println!("\n== Compaction events (cold round after) ==");
    match db.compaction_events() {
        Ok(rows) if !rows.is_empty() => {
            for r in &rows {
                println!(
                    "{}  {}  removed={} read_this={} read_next={} create_next={}",
                    truncate(&r.session_id, 20),
                    r.ts,
                    r.segments_removed,
                    r.cache_read_this_round,
                    r.cache_read_next_round,
                    r.cache_creation_next_round,
                );
            }
        }
        Ok(_) => println!("(none)"),
        Err(error) => eprintln!("compaction_events failed: {error}"),
    }

    println!(
        "\n== Pauses > {:.0}s (cache survival across gaps) ==",
        DEFAULT_TTL_SECONDS
    );
    match db.pause_survival(DEFAULT_TTL_SECONDS) {
        Ok(rows) if !rows.is_empty() => {
            for r in &rows {
                let verdict = if r.cache_read_after > 0 {
                    "SURVIVED"
                } else {
                    "COLD"
                };
                println!(
                    "{}  {}  gap={:.0}s  read_after={:>10}  [{}]",
                    truncate(&r.session_id, 20),
                    r.ts,
                    r.gap_seconds,
                    r.cache_read_after,
                    verdict,
                );
            }
        }
        Ok(_) => println!("(no pauses over the TTL)"),
        Err(error) => eprintln!("pause_survival failed: {error}"),
    }

    ExitCode::SUCCESS
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}
