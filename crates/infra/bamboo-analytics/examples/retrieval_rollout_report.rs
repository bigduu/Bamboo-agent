//! Render the versioned retrieval-window rollout report from aggregate JSONL.
//!
//!   cargo run -p bamboo-analytics --example retrieval_rollout_report -- \
//!     crates/infra/bamboo-analytics/fixtures/retrieval-rollout-v1.jsonl
//!
//! Pass an existing Bamboo `metrics.db` as the optional second argument to add
//! the current `session_history_current` call/success/latency aggregate. The
//! database is opened read-only and no tool arguments or results are selected.

use std::path::Path;
use std::process::ExitCode;

use bamboo_analytics::{
    render_retrieval_rollout_report, session_history_tool_metrics, HistoryToolMetrics,
};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(evidence_path) = args.next() else {
        eprintln!("usage: retrieval_rollout_report <aggregate-jsonl> [metrics.db]");
        return ExitCode::from(2);
    };
    let tool_metrics: Option<HistoryToolMetrics> = match args.next() {
        Some(path) => match session_history_tool_metrics(Path::new(&path)) {
            Ok(metrics) => Some(metrics),
            Err(error) => {
                eprintln!("failed to read metrics.db: {error}");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };
    if args.next().is_some() {
        eprintln!("usage: retrieval_rollout_report <aggregate-jsonl> [metrics.db]");
        return ExitCode::from(2);
    }

    match render_retrieval_rollout_report(Path::new(&evidence_path), tool_metrics.as_ref()) {
        Ok(report) => {
            print!("{report}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("failed to render rollout report: {error}");
            ExitCode::FAILURE
        }
    }
}
