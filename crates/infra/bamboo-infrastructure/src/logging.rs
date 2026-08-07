//! Centralized logging/tracing initialization.
//!
//! This lives in the infrastructure layer (next to `config::paths`) so every
//! consumer shares one logging policy: the standalone `bamboo serve` binary, the
//! CLI/TUI, and embedded hosts such as the Bodhi Tauri app. The policy fixes the
//! problems the old per-app setups had:
//!
//! - **Logs survive restarts.** Output goes to a date-stamped file under
//!   `{home}/logs` that is appended to, not truncated, so a restart on the same
//!   day continues the same file and earlier days are left intact.
//! - **Rotation is by date, not size.** Files roll once per day (`Rotation::DAILY`),
//!   so a single run's logs are never split mid-stream on a byte threshold.
//!   Count and byte limits are enforced at process startup; a process that runs
//!   across later UTC rollovers may temporarily exceed them until its next start.
//! - **Old files are purged.** Startup pruning keeps at most
//!   [`DEFAULT_MAX_LOG_FILES`] strictly matching dated files and bounds their
//!   total size by [`DEFAULT_MAX_LOG_BYTES`]. The captured UTC-day file and any
//!   future-dated files are never removed.
//! - **Server logs are quiet by default.** `bamboo serve` defaults to `info` in
//!   every build profile. Explicit `RUST_LOG` directives still override that
//!   policy, while embedding APIs may opt into a build-profile-derived level.
//! - **File and stdout filters are independent.** Embedded debug builds can keep
//!   Bamboo `debug` output on stdout while files remain `info` by default.
//!   Dependency-specific noise defaults keep frame-level traces out of both
//!   sinks unless an operator explicitly overrides the matching target.
//!
//! All initializers are best-effort and idempotent: they use `try_init`, so a
//! second call (or a call after some other subscriber is installed) is a no-op
//! rather than a panic. Because the global subscriber is a process-wide side
//! effect, call these once from a binary's entry point — not from library code.
//!
//! `tracing-subscriber`'s default `tracing-log` feature installs a `log` →
//! `tracing` bridge as part of `try_init`, so existing `log::info!`-style calls
//! (which Bodhi uses heavily) are captured without any code changes.

use std::io;
use std::path::{Path, PathBuf};

use chrono::{NaiveDate, Utc};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Number of strictly matching dated log files to retain at process startup.
/// With daily rotation this is roughly two weeks of history. A value of zero in
/// [`LogOptions::max_files`] disables count pruning.
pub const DEFAULT_MAX_LOG_FILES: usize = 14;

/// Total byte budget for date-stamped logs sharing one prefix.
///
/// The active UTC-day file is exempt because unlinking an open file would not
/// reclaim its space and would make the current process's logs disappear from
/// the directory. File-level `info` defaults keep that active-file exception
/// small under normal operation; the budget bounds retained history at startup.
pub const DEFAULT_MAX_LOG_BYTES: u64 = 128 * 1024 * 1024;

const DEFAULT_FILE_LOG_LEVEL: &str = "info";

const NOISY_DEPENDENCY_DEFAULTS: &[(&str, &str)] = &[
    ("h2", "warn"),
    ("hyper", "info"),
    ("hyper_util", "info"),
    ("tungstenite", "info"),
    ("rustls", "info"),
];

/// Tuning knobs for [`init_logging_with_options`].
#[derive(Debug, Clone)]
pub struct LogOptions {
    /// Directory the log files are written to (created if missing).
    pub dir: PathBuf,
    /// Filename prefix; the date and a `.log` suffix are appended by the appender
    /// (e.g. `bamboo.2026-05-31.log`). Lets co-located apps keep separate files.
    pub file_name_prefix: String,
    /// Maximum number of strictly matching dated files to keep at startup.
    /// A value of zero disables count pruning; byte-budget pruning remains active.
    pub max_files: usize,
    /// Stdout level used when `RUST_LOG` is not set (e.g. `"info"` or `"debug"`).
    pub default_level: String,
}

impl LogOptions {
    /// Options writing to `dir` with the shared defaults (`bamboo` prefix,
    /// count- and byte-bounded retention, and `info` for both sinks).
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            file_name_prefix: "bamboo".to_string(),
            max_files: DEFAULT_MAX_LOG_FILES,
            default_level: "info".to_string(),
        }
    }
}

/// Initialize file + stdout logging for a process rooted at `home`.
///
/// Logs are written under `{home}/logs`. Pass `debug = true` (typically
/// `cfg!(debug_assertions)`) to default stdout to `debug`; otherwise stdout is
/// `info`. Files default to `info` in either case. This is the entry point both
/// the `bamboo` binary and the Bodhi app call.
pub fn init_logging_with_home(home: &Path, debug: bool) {
    init_logging_with_options(options_for_home(home, debug));
}

/// Initialize file + stdout logging for the standalone `bamboo serve` process.
///
/// Server operation defaults to `info` in every build profile. A debug binary is
/// an implementation detail of the development toolchain, not an operator
/// request for dependency-wide debug logs. Callers can still opt in explicitly
/// through `RUST_LOG`, `--log-level`, or `-v` before this initializer runs.
pub fn init_server_logging_with_home(home: &Path) {
    init_logging_with_options(options_for_server_home(home, cfg!(debug_assertions)));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogContext {
    Server,
    BuildProfile,
}

/// Build the [`LogOptions`] used by [`init_logging_with_home`]: logs under
/// `{home}/logs`, stdout level by build profile, shared defaults otherwise.
///
/// Split out from the initializer so the path/level composition can be unit
/// tested without installing a process-global subscriber.
fn options_for_home(home: &Path, debug: bool) -> LogOptions {
    let mut opts = LogOptions::new(home.join("logs"));
    opts.default_level = level_for(LogContext::BuildProfile, debug).to_string();
    opts
}

/// Build server options without installing a process-global subscriber.
/// Keeping the build-profile input explicit makes the invariant directly
/// testable: both debug and release servers must resolve to `info`.
fn options_for_server_home(home: &Path, debug_build: bool) -> LogOptions {
    let mut opts = LogOptions::new(home.join("logs"));
    opts.default_level = level_for(LogContext::Server, debug_build).to_string();
    opts
}

/// Create the log directory and a daily-rotating file appender for `opts`.
///
/// Separated from [`init_logging_with_options`] so the file-side behavior
/// (directory creation, filename shape, rotation/retention config) is testable
/// without touching the global subscriber, which can only be set once per process.
fn build_appender(
    opts: &LogOptions,
) -> Result<RollingFileAppender, tracing_appender::rolling::InitError> {
    // Best-effort: a missing directory shouldn't abort startup. If creation
    // fails we still try the appender (and the caller falls back to stdout).
    if let Err(e) = std::fs::create_dir_all(&opts.dir) {
        eprintln!(
            "warning: could not create log directory {}: {e}",
            opts.dir.display()
        );
    }

    // tracing-appender's daily rotation uses UTC. Use the same date boundary
    // so startup pruning can never unlink the file this appender will open.
    if let Err(error) = prune_logs_at_startup(opts, Utc::now().date_naive(), DEFAULT_MAX_LOG_BYTES)
    {
        eprintln!(
            "warning: could not prune log directory {}: {error}",
            opts.dir.display()
        );
    }

    RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix(&opts.file_name_prefix)
        .filename_suffix("log")
        .build(&opts.dir)
}

#[derive(Debug)]
struct DatedLogFile {
    date: NaiveDate,
    path: PathBuf,
    bytes: u64,
}

/// Prune matching historical logs oldest-first until both startup limits hold.
///
/// Only regular files named exactly `<prefix>.<YYYY-MM-DD>.log` participate.
/// Symlinks, directories, non-UTF-8 names, and co-located unrelated files are
/// ignored. The captured active date and future dates are protected. Per-entry
/// metadata and deletion failures are best-effort so a stale unreadable file
/// cannot disable logging at startup.
fn prune_logs_at_startup(
    opts: &LogOptions,
    active_date: NaiveDate,
    max_total_bytes: u64,
) -> io::Result<usize> {
    let entries = std::fs::read_dir(&opts.dir)?;
    let mut matching = Vec::new();

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                eprintln!(
                    "warning: could not inspect an entry in {}: {error}",
                    opts.dir.display()
                );
                continue;
            }
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) => {
                eprintln!(
                    "warning: could not inspect log candidate {}: {error}",
                    entry.path().display()
                );
                continue;
            }
        };
        if !file_type.is_file() || file_type.is_symlink() {
            continue;
        }

        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(date) = dated_log_name(&opts.file_name_prefix, &name) else {
            continue;
        };
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                eprintln!(
                    "warning: could not measure log candidate {}: {error}",
                    entry.path().display()
                );
                continue;
            }
        };
        matching.push(DatedLogFile {
            date,
            path: entry.path(),
            bytes: metadata.len(),
        });
    }

    let active_exists = matching.iter().any(|file| file.date == active_date);
    let reserved_active_slot = usize::from(!active_exists);
    let mut matching_count = matching.len();
    let mut total_bytes = total_log_bytes(&matching);
    if !startup_limits_exceeded(
        opts.max_files,
        max_total_bytes,
        matching_count,
        reserved_active_slot,
        total_bytes,
    ) {
        return Ok(0);
    }

    matching.sort_by(|left, right| {
        left.date
            .cmp(&right.date)
            .then_with(|| left.path.cmp(&right.path))
    });

    let mut deleted = 0;
    for file in matching {
        if !startup_limits_exceeded(
            opts.max_files,
            max_total_bytes,
            matching_count,
            reserved_active_slot,
            total_bytes,
        ) {
            break;
        }
        if file.date >= active_date {
            continue;
        }

        // Recheck without following links immediately before deletion. A
        // concurrent replacement can therefore at worst make deletion fail or
        // remove the replacement directory entry, never follow a symlink target.
        let metadata = match std::fs::symlink_metadata(&file.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                total_bytes = total_bytes.saturating_sub(file.bytes);
                matching_count = matching_count.saturating_sub(1);
                continue;
            }
            Err(error) => {
                eprintln!(
                    "warning: could not recheck historical log {}: {error}",
                    file.path.display()
                );
                continue;
            }
        };
        if !metadata.file_type().is_file() {
            total_bytes = total_bytes.saturating_sub(file.bytes);
            matching_count = matching_count.saturating_sub(1);
            continue;
        }
        match std::fs::remove_file(&file.path) {
            Ok(()) => {
                total_bytes = total_bytes.saturating_sub(file.bytes);
                matching_count = matching_count.saturating_sub(1);
                deleted += 1;
            }
            Err(error) => eprintln!(
                "warning: could not prune historical log {}: {error}",
                file.path.display()
            ),
        }
    }

    Ok(deleted)
}

fn startup_limits_exceeded(
    max_files: usize,
    max_total_bytes: u64,
    matching_count: usize,
    reserved_active_slot: usize,
    total_bytes: u64,
) -> bool {
    let exceeds_count =
        max_files != 0 && matching_count.saturating_add(reserved_active_slot) > max_files;
    exceeds_count || total_bytes > max_total_bytes
}

fn total_log_bytes(files: &[DatedLogFile]) -> u64 {
    files
        .iter()
        .fold(0_u64, |total, file| total.saturating_add(file.bytes))
}

fn dated_log_name(prefix: &str, name: &str) -> Option<NaiveDate> {
    let date = name
        .strip_prefix(prefix)?
        .strip_prefix('.')?
        .strip_suffix(".log")?;
    if date.len() != "YYYY-MM-DD".len() {
        return None;
    }
    let parsed = NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?;
    (parsed.format("%Y-%m-%d").to_string() == date).then_some(parsed)
}

fn level_verbosity(level: &str) -> Option<u8> {
    if level.eq_ignore_ascii_case("off") {
        Some(0)
    } else if level.eq_ignore_ascii_case("error") {
        Some(1)
    } else if level.eq_ignore_ascii_case("warn") {
        Some(2)
    } else if level.eq_ignore_ascii_case("info") {
        Some(3)
    } else if level.eq_ignore_ascii_case("debug") {
        Some(4)
    } else if level.eq_ignore_ascii_case("trace") {
        Some(5)
    } else {
        None
    }
}

fn effective_root_verbosity(default_level: &str, explicit: Option<&str>) -> Option<u8> {
    explicit
        .and_then(|directives| {
            directives
                .split(',')
                .filter_map(|directive| level_verbosity(directive.trim()))
                .next_back()
        })
        .or_else(|| level_verbosity(default_level.trim()))
}

fn filter_directives(default_level: &str, explicit: Option<&str>) -> String {
    // Always seed a root directive for this sink. A target-only RUST_LOG such
    // as `h2=debug` raises that dependency without accidentally changing the
    // default for every other target (stdout and file may seed different roots).
    let mut directives = String::from(default_level);
    let effective_root = effective_root_verbosity(default_level, explicit);
    for (target, level) in NOISY_DEPENDENCY_DEFAULTS {
        // Target defaults exist only to lower noisy dependencies. A more
        // restrictive explicit root such as `error` or `off` must never be
        // widened by a more specific default target directive.
        if !level_verbosity(level)
            .zip(effective_root)
            .is_some_and(|(target_level, root_level)| target_level <= root_level)
        {
            continue;
        }
        directives.push(',');
        directives.push_str(target);
        directives.push('=');
        directives.push_str(level);
    }
    if let Some(explicit) = explicit.filter(|value| !value.trim().is_empty()) {
        directives.push(',');
        // Explicit directives come last. EnvFilter replaces a prior directive
        // with the same target/matcher, so `h2=debug` overrides our `h2=warn`.
        directives.push_str(explicit);
    }
    directives
}

fn make_filter(default_level: &str, explicit: Option<&str>) -> EnvFilter {
    let defaults = filter_directives(default_level, None);
    EnvFilter::try_new(filter_directives(default_level, explicit))
        .unwrap_or_else(|_| EnvFilter::new(defaults))
}

/// Initialize file + stdout logging from explicit [`LogOptions`].
pub fn init_logging_with_options(opts: LogOptions) {
    let explicit = std::env::var(EnvFilter::DEFAULT_ENV).ok();

    match build_appender(&opts) {
        Ok(file_writer) => {
            // `RollingFileAppender` implements `MakeWriter`, so it drives the file
            // layer directly — no background worker, hence no guard to keep alive.
            let stdout_layer = fmt::layer().with_target(true);
            let file_layer = fmt::layer()
                .with_target(true)
                .with_ansi(false)
                .with_writer(file_writer);
            let _ = tracing_subscriber::registry()
                .with(
                    stdout_layer.with_filter(make_filter(&opts.default_level, explicit.as_deref())),
                )
                .with(
                    file_layer
                        .with_filter(make_filter(DEFAULT_FILE_LOG_LEVEL, explicit.as_deref())),
                )
                .try_init();
        }
        Err(e) => {
            eprintln!("warning: file logging disabled ({e}); using stdout only");
            let _ = fmt()
                .with_target(true)
                .with_env_filter(make_filter(&opts.default_level, explicit.as_deref()))
                .try_init();
        }
    }
}

/// Initialize stdout-only logging.
///
/// For contexts without a stable data directory (e.g. the `bamboo config`
/// subcommand). Prefer [`init_logging_with_home`] when a `{home}/logs` dir exists.
pub fn init_logging(debug: bool) {
    let explicit = std::env::var(EnvFilter::DEFAULT_ENV).ok();
    let _ = fmt()
        .with_target(true)
        .with_env_filter(make_filter(
            level_for(LogContext::BuildProfile, debug),
            explicit.as_deref(),
        ))
        .try_init();
}

/// Default level string for a logging context when `RUST_LOG` is unset.
fn level_for(context: LogContext, debug_build: bool) -> &'static str {
    match (context, debug_build) {
        (LogContext::Server, _) | (LogContext::BuildProfile, false) => "info",
        (LogContext::BuildProfile, true) => "debug",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tempfile::tempdir;
    use tracing::{Event, Subscriber};
    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::layer::{Context, Layer};

    #[derive(Clone)]
    struct EventCounter(Arc<AtomicUsize>);

    impl<S> Layer<S> for EventCounter
    where
        S: Subscriber,
    {
        fn on_event(&self, _event: &Event<'_>, _ctx: Context<'_, S>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn dated_file(dir: &Path, name: &str, bytes: usize) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, vec![b'x'; bytes]).expect("write dated log fixture");
        path
    }

    #[test]
    fn level_for_maps_build_profile_for_embedding_apis() {
        assert_eq!(level_for(LogContext::BuildProfile, true), "debug");
        assert_eq!(level_for(LogContext::BuildProfile, false), "info");
    }

    #[test]
    fn server_level_is_info_in_debug_and_release_builds() {
        assert_eq!(level_for(LogContext::Server, true), "info");
        assert_eq!(level_for(LogContext::Server, false), "info");
    }

    #[test]
    fn log_options_new_uses_shared_defaults() {
        let opts = LogOptions::new("/tmp/example");
        assert_eq!(opts.dir, PathBuf::from("/tmp/example"));
        assert_eq!(opts.file_name_prefix, "bamboo");
        assert_eq!(opts.max_files, DEFAULT_MAX_LOG_FILES);
        assert_eq!(opts.default_level, "info");
    }

    #[test]
    fn options_for_home_places_logs_under_home_and_sets_level() {
        let debug = options_for_home(Path::new("/srv/data"), true);
        assert_eq!(debug.dir, PathBuf::from("/srv/data/logs"));
        assert_eq!(debug.default_level, "debug");

        let release = options_for_home(Path::new("/srv/data"), false);
        assert_eq!(release.default_level, "info");
    }

    #[test]
    fn server_options_are_info_in_debug_and_release_builds() {
        for debug_build in [true, false] {
            let opts = options_for_server_home(Path::new("/srv/data"), debug_build);
            assert_eq!(opts.dir, PathBuf::from("/srv/data/logs"));
            assert_eq!(opts.default_level, "info");
        }
    }

    #[test]
    fn stdout_and_file_filters_have_independent_defaults() {
        let stdout_events = Arc::new(AtomicUsize::new(0));
        let file_events = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry()
            .with(EventCounter(stdout_events.clone()).with_filter(make_filter("debug", None)))
            .with(EventCounter(file_events.clone()).with_filter(make_filter("info", None)));

        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!(target: "bamboo_filter_default_test", "stdout only");
            tracing::info!(target: "bamboo_filter_default_test", "both sinks");
        });

        assert_eq!(stdout_events.load(Ordering::Relaxed), 2);
        assert_eq!(file_events.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn noisy_targets_default_low_but_explicit_target_directive_wins() {
        let defaults = make_filter("debug", None).to_string();
        for expected in [
            "h2=warn",
            "hyper=info",
            "hyper_util=info",
            "tungstenite=info",
            "rustls=info",
        ] {
            assert!(
                defaults.split(',').any(|directive| directive == expected),
                "missing {expected} in {defaults}"
            );
        }

        let default_events = Arc::new(AtomicUsize::new(0));
        let override_events = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry()
            .with(EventCounter(default_events.clone()).with_filter(make_filter("debug", None)))
            .with(
                EventCounter(override_events.clone())
                    .with_filter(make_filter("info", Some("bamboo_engine=trace,h2=debug"))),
            );

        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!(target: "h2", "explicitly requested frame detail");
        });

        assert_eq!(default_events.load(Ordering::Relaxed), 0);
        assert_eq!(override_events.load(Ordering::Relaxed), 1);
        let overridden = make_filter("info", Some("h2=debug")).to_string();
        assert!(overridden
            .split(',')
            .any(|directive| directive == "h2=debug"));
        assert!(!overridden
            .split(',')
            .any(|directive| directive == "h2=warn"));
    }

    #[test]
    fn target_only_explicit_filter_preserves_each_sink_root_default() {
        let stdout_events = Arc::new(AtomicUsize::new(0));
        let file_events = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry()
            .with(
                EventCounter(stdout_events.clone())
                    .with_filter(make_filter("debug", Some("h2=trace"))),
            )
            .with(
                EventCounter(file_events.clone())
                    .with_filter(make_filter("info", Some("h2=trace"))),
            );

        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!(target: "bamboo_target_only_root_test", "stdout root");
            tracing::info!(target: "bamboo_target_only_root_test", "both roots");
        });

        assert_eq!(stdout_events.load(Ordering::Relaxed), 2);
        assert_eq!(file_events.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn explicit_root_directive_raises_the_file_filter() {
        let file_events = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(
            EventCounter(file_events.clone()).with_filter(make_filter("info", Some("debug"))),
        );

        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!(target: "bamboo_explicit_root_test", "explicit file debug");
        });

        assert_eq!(file_events.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn restrictive_explicit_roots_are_never_widened_by_noise_defaults() {
        for root in ["error", "off"] {
            let events = Arc::new(AtomicUsize::new(0));
            let subscriber = tracing_subscriber::registry()
                .with(EventCounter(events.clone()).with_filter(make_filter("debug", Some(root))));

            tracing::subscriber::with_default(subscriber, || {
                tracing::warn!(target: "h2", "must remain blocked by restrictive root");
                tracing::info!(target: "hyper", "must remain blocked by restrictive root");
            });

            assert_eq!(events.load(Ordering::Relaxed), 0, "root={root}");
            let rendered = make_filter("debug", Some(root)).to_string();
            assert!(!rendered.split(',').any(|directive| directive == "h2=warn"));
            assert!(!rendered
                .split(',')
                .any(|directive| directive == "hyper=info"));
        }
        assert_eq!(
            effective_root_verbosity("debug", Some("off,trace,error")),
            level_verbosity("error")
        );
    }

    #[test]
    fn restrictive_root_still_allows_an_explicit_target_override() {
        let events = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(
            EventCounter(events.clone()).with_filter(make_filter("info", Some("error,h2=debug"))),
        );

        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!(target: "h2", "explicit target override");
            tracing::info!(target: "hyper", "still blocked by root error");
        });

        assert_eq!(events.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn invalid_explicit_filter_falls_back_to_safe_defaults() {
        let filter = make_filter("info", Some("h2=debug,[broken"));
        let rendered = filter.to_string();
        assert!(rendered.split(',').any(|directive| directive == "info"));
        assert!(rendered.split(',').any(|directive| directive == "h2=warn"));
        assert!(!rendered.split(',').any(|directive| directive == "h2=debug"));
    }

    #[test]
    fn dated_log_name_requires_exact_prefix_date_and_suffix() {
        assert_eq!(
            dated_log_name("bamboo", "bamboo.2026-08-07.log"),
            NaiveDate::from_ymd_opt(2026, 8, 7)
        );
        for unrelated in [
            "other.2026-08-07.log",
            "bamboo-extra.2026-08-07.log",
            "bamboo.2026-8-7.log",
            "bamboo.2026-08-07.log.bak",
            "bamboo.latest.log",
        ] {
            assert_eq!(dated_log_name("bamboo", unrelated), None, "{unrelated}");
        }
    }

    #[test]
    fn byte_pruning_is_oldest_first_and_counts_active_file() {
        let tmp = tempdir().expect("tempdir");
        let oldest = dated_file(tmp.path(), "bamboo.2026-08-01.log", 4);
        let newer = dated_file(tmp.path(), "bamboo.2026-08-02.log", 5);
        let active = dated_file(tmp.path(), "bamboo.2026-08-07.log", 6);
        let mut opts = LogOptions::new(tmp.path());
        opts.max_files = 0;

        let deleted = prune_logs_at_startup(
            &opts,
            NaiveDate::from_ymd_opt(2026, 8, 7).expect("valid date"),
            11,
        )
        .expect("prune succeeds");

        assert_eq!(deleted, 1);
        assert!(!oldest.exists());
        assert!(newer.exists());
        assert!(active.exists());
    }

    #[test]
    fn byte_pruning_never_removes_active_file_even_when_it_exceeds_budget() {
        let tmp = tempdir().expect("tempdir");
        let active = dated_file(tmp.path(), "bamboo.2026-08-07.log", 32);
        let mut opts = LogOptions::new(tmp.path());
        opts.max_files = 0;

        let deleted = prune_logs_at_startup(
            &opts,
            NaiveDate::from_ymd_opt(2026, 8, 7).expect("valid date"),
            0,
        )
        .expect("prune succeeds");

        assert_eq!(deleted, 0);
        assert!(active.exists());
    }

    #[test]
    fn startup_pruning_leaves_unrelated_and_non_file_entries_untouched() {
        let tmp = tempdir().expect("tempdir");
        let matching = dated_file(tmp.path(), "bamboo.2026-08-01.log", 4);
        let unrelated = [
            dated_file(tmp.path(), "other.2026-08-01.log", 8),
            dated_file(tmp.path(), "bamboo-extra.2026-08-01.log", 8),
            dated_file(tmp.path(), "bamboo.2026-08-01.log.bak", 8),
            dated_file(tmp.path(), "bamboo.latest.log", 8),
        ];
        let matching_directory = tmp.path().join("bamboo.2026-08-02.log");
        std::fs::create_dir(&matching_directory).expect("matching-name directory");
        let opts = LogOptions::new(tmp.path());

        let deleted = prune_logs_at_startup(
            &opts,
            NaiveDate::from_ymd_opt(2026, 8, 7).expect("valid date"),
            0,
        )
        .expect("prune succeeds");

        assert_eq!(deleted, 1);
        assert!(!matching.exists());
        assert!(unrelated.iter().all(|path| path.exists()));
        assert!(matching_directory.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn startup_pruning_never_follows_or_deletes_symlinks() {
        use std::os::unix::fs::symlink;

        let tmp = tempdir().expect("tempdir");
        let target = dated_file(tmp.path(), "outside.log", 16);
        let link = tmp.path().join("bamboo.2026-08-01.log");
        symlink(&target, &link).expect("symlink fixture");
        let opts = LogOptions::new(tmp.path());

        let deleted = prune_logs_at_startup(
            &opts,
            NaiveDate::from_ymd_opt(2026, 8, 7).expect("valid date"),
            0,
        )
        .expect("prune succeeds");

        assert_eq!(deleted, 0);
        assert!(link.symlink_metadata().is_ok());
        assert_eq!(std::fs::read(&target).expect("target remains").len(), 16);
    }

    #[test]
    fn appender_never_count_prunes_lookalike_files_at_or_above_the_threshold() {
        let tmp = tempdir().expect("tempdir");
        let lookalikes = [
            dated_file(tmp.path(), "bamboo-not-a-date.log", 1),
            dated_file(tmp.path(), "bamboo.2026-8-1.log", 1),
            dated_file(tmp.path(), "bamboo.2026-08-01-extra.log", 1),
        ];
        let mut opts = LogOptions::new(tmp.path());
        opts.max_files = 2;

        let appender = build_appender(&opts).expect("appender builds");
        drop(appender);

        assert!(lookalikes.iter().all(|path| path.exists()));
    }

    #[test]
    fn count_pruning_never_removes_the_active_file_even_when_created_first() {
        let tmp = tempdir().expect("tempdir");
        // Create the active file first to ensure filesystem creation order is
        // irrelevant; retention is based only on strict parsed dates.
        let active = dated_file(tmp.path(), "bamboo.2026-08-07.log", 1);
        let oldest = dated_file(tmp.path(), "bamboo.2026-08-01.log", 1);
        let newer = dated_file(tmp.path(), "bamboo.2026-08-02.log", 1);
        let mut opts = LogOptions::new(tmp.path());
        opts.max_files = 2;

        let deleted = prune_logs_at_startup(
            &opts,
            NaiveDate::from_ymd_opt(2026, 8, 7).expect("valid date"),
            u64::MAX,
        )
        .expect("prune succeeds");

        assert_eq!(deleted, 1);
        assert!(!oldest.exists());
        assert!(newer.exists());
        assert!(active.exists());
    }

    #[test]
    fn count_pruning_reserves_a_slot_when_the_active_file_does_not_exist_yet() {
        let tmp = tempdir().expect("tempdir");
        let oldest = dated_file(tmp.path(), "bamboo.2026-08-01.log", 1);
        let newer = dated_file(tmp.path(), "bamboo.2026-08-02.log", 1);
        let mut opts = LogOptions::new(tmp.path());
        opts.max_files = 2;

        let deleted = prune_logs_at_startup(
            &opts,
            NaiveDate::from_ymd_opt(2026, 8, 7).expect("valid date"),
            u64::MAX,
        )
        .expect("prune succeeds");

        assert_eq!(deleted, 1);
        assert!(!oldest.exists());
        assert!(newer.exists());
    }

    #[test]
    fn zero_max_files_disables_count_pruning_but_not_byte_pruning() {
        let tmp = tempdir().expect("tempdir");
        let oldest = dated_file(tmp.path(), "bamboo.2026-08-01.log", 4);
        let newer = dated_file(tmp.path(), "bamboo.2026-08-02.log", 4);
        let mut opts = LogOptions::new(tmp.path());
        opts.max_files = 0;

        let count_only = prune_logs_at_startup(
            &opts,
            NaiveDate::from_ymd_opt(2026, 8, 7).expect("valid date"),
            u64::MAX,
        )
        .expect("count pruning disabled");
        assert_eq!(count_only, 0);
        assert!(oldest.exists() && newer.exists());

        let byte_limited = prune_logs_at_startup(
            &opts,
            NaiveDate::from_ymd_opt(2026, 8, 7).expect("valid date"),
            4,
        )
        .expect("byte pruning remains active");
        assert_eq!(byte_limited, 1);
        assert!(!oldest.exists());
        assert!(newer.exists());
    }

    #[test]
    fn count_and_byte_limits_are_enforced_together_oldest_first() {
        let tmp = tempdir().expect("tempdir");
        let oldest = dated_file(tmp.path(), "bamboo.2026-08-01.log", 4);
        let middle = dated_file(tmp.path(), "bamboo.2026-08-02.log", 4);
        let newest = dated_file(tmp.path(), "bamboo.2026-08-03.log", 4);
        let active = dated_file(tmp.path(), "bamboo.2026-08-07.log", 4);
        let mut opts = LogOptions::new(tmp.path());
        opts.max_files = 3;

        let deleted = prune_logs_at_startup(
            &opts,
            NaiveDate::from_ymd_opt(2026, 8, 7).expect("valid date"),
            8,
        )
        .expect("prune succeeds");

        assert_eq!(deleted, 2);
        assert!(!oldest.exists());
        assert!(!middle.exists());
        assert!(newest.exists());
        assert!(active.exists());
    }

    #[test]
    fn startup_pruning_never_removes_future_dated_files() {
        let tmp = tempdir().expect("tempdir");
        let historical = dated_file(tmp.path(), "bamboo.2026-08-01.log", 4);
        let future = dated_file(tmp.path(), "bamboo.2026-08-08.log", 4);
        let mut opts = LogOptions::new(tmp.path());
        opts.max_files = 1;

        let deleted = prune_logs_at_startup(
            &opts,
            NaiveDate::from_ymd_opt(2026, 8, 7).expect("valid date"),
            0,
        )
        .expect("prune succeeds");

        assert_eq!(deleted, 1);
        assert!(!historical.exists());
        assert!(future.exists());
    }

    #[test]
    fn total_log_bytes_saturates_on_overflow() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 1).expect("valid date");
        let files = [
            DatedLogFile {
                date,
                path: PathBuf::from("first"),
                bytes: u64::MAX,
            },
            DatedLogFile {
                date,
                path: PathBuf::from("second"),
                bytes: 1,
            },
        ];
        assert_eq!(total_log_bytes(&files), u64::MAX);
    }

    #[test]
    fn build_appender_creates_dir_and_writes_dated_file() {
        let tmp = tempdir().expect("tempdir");
        // Nested path that does not exist yet, to prove directories are created.
        let dir = tmp.path().join("nested").join("logs");
        let opts = LogOptions {
            dir: dir.clone(),
            file_name_prefix: "unit-test".to_string(),
            max_files: 5,
            default_level: "info".to_string(),
        };

        let appender = build_appender(&opts).expect("appender builds");
        assert!(dir.exists(), "log directory should be created");

        // Write through the appender the same way the fmt layer does.
        {
            let mut writer = appender.make_writer();
            writeln!(writer, "hello-from-test").expect("write line");
            writer.flush().expect("flush");
        }
        drop(appender); // ensure the file handle is released before reading

        let entries: Vec<_> = std::fs::read_dir(&dir)
            .expect("read log dir")
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();

        assert_eq!(entries.len(), 1, "exactly one log file, got {entries:?}");
        let name = &entries[0];
        assert!(
            name.starts_with("unit-test.") && name.ends_with(".log"),
            "filename should be `<prefix>.<date>.log`, got {name}"
        );

        let contents =
            std::fs::read_to_string(dir.join(name)).expect("read back log file contents");
        assert!(
            contents.contains("hello-from-test"),
            "log file should contain the written line, got: {contents:?}"
        );
    }

    #[test]
    fn init_logging_with_options_creates_dir_and_is_idempotent() {
        // Exercises the real entry point. The global subscriber can only be set
        // once per test binary, so we assert only on the deterministic side
        // effect (directory creation) and that a repeat call does not panic.
        let tmp = tempdir().expect("tempdir");
        let dir = tmp.path().join("logs");
        let opts = LogOptions {
            dir: dir.clone(),
            file_name_prefix: "idem".to_string(),
            max_files: 2,
            default_level: "info".to_string(),
        };

        init_logging_with_options(opts.clone());
        init_logging_with_options(opts); // must be a no-op, not a panic

        assert!(dir.exists());
    }
}
