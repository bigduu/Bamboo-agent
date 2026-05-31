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
//! - **Old files are purged.** At most [`DEFAULT_MAX_LOG_FILES`] dated files are
//!   kept; the appender deletes the oldest beyond that on rollover.
//! - **Level matches the build profile.** Debug builds default to `debug`, release
//!   builds to `info`, and `RUST_LOG` always overrides both.
//!
//! All initializers are best-effort and idempotent: they use `try_init`, so a
//! second call (or a call after some other subscriber is installed) is a no-op
//! rather than a panic. Because the global subscriber is a process-wide side
//! effect, call these once from a binary's entry point — not from library code.
//!
//! `tracing-subscriber`'s default `tracing-log` feature installs a `log` →
//! `tracing` bridge as part of `try_init`, so existing `log::info!`-style calls
//! (which Bodhi uses heavily) are captured without any code changes.

use std::path::{Path, PathBuf};

use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Number of dated log files to retain before the oldest are purged on rollover.
/// With daily rotation this is roughly two weeks of history.
pub const DEFAULT_MAX_LOG_FILES: usize = 14;

/// Tuning knobs for [`init_logging_with_options`].
#[derive(Debug, Clone)]
pub struct LogOptions {
    /// Directory the log files are written to (created if missing).
    pub dir: PathBuf,
    /// Filename prefix; the date and a `.log` suffix are appended by the appender
    /// (e.g. `bamboo.2026-05-31.log`). Lets co-located apps keep separate files.
    pub file_name_prefix: String,
    /// Maximum number of dated files to keep; older ones are deleted on rollover.
    pub max_files: usize,
    /// Level filter used when `RUST_LOG` is not set (e.g. `"info"` or `"debug"`).
    pub default_level: String,
}

impl LogOptions {
    /// Options writing to `dir` with the shared defaults (`bamboo` prefix,
    /// [`DEFAULT_MAX_LOG_FILES`] retention, `info` level).
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
/// `cfg!(debug_assertions)`) to default to the `debug` level; otherwise `info`.
/// This is the entry point both the `bamboo` binary and the Bodhi app call.
pub fn init_logging_with_home(home: &Path, debug: bool) {
    init_logging_with_options(options_for_home(home, debug));
}

/// Build the [`LogOptions`] used by [`init_logging_with_home`]: logs under
/// `{home}/logs`, level by build profile, shared defaults otherwise.
///
/// Split out from the initializer so the path/level composition can be unit
/// tested without installing a process-global subscriber.
fn options_for_home(home: &Path, debug: bool) -> LogOptions {
    let mut opts = LogOptions::new(home.join("logs"));
    opts.default_level = level_for(debug).to_string();
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

    RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix(&opts.file_name_prefix)
        .filename_suffix("log")
        .max_log_files(opts.max_files)
        .build(&opts.dir)
}

/// Initialize file + stdout logging from explicit [`LogOptions`].
pub fn init_logging_with_options(opts: LogOptions) {
    // EnvFilter is not `Clone`, so build a fresh one wherever it's needed.
    let default_level = opts.default_level.clone();
    let make_filter = move || {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level.clone()))
    };

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
                .with(make_filter())
                .with(stdout_layer)
                .with(file_layer)
                .try_init();
        }
        Err(e) => {
            eprintln!("warning: file logging disabled ({e}); using stdout only");
            let _ = fmt()
                .with_target(true)
                .with_env_filter(make_filter())
                .try_init();
        }
    }
}

/// Initialize stdout-only logging.
///
/// For contexts without a stable data directory (e.g. the `bamboo config`
/// subcommand). Prefer [`init_logging_with_home`] when a `{home}/logs` dir exists.
pub fn init_logging(debug: bool) {
    let _ = fmt()
        .with_target(true)
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(level_for(debug))),
        )
        .try_init();
}

/// Default level string for a build profile when `RUST_LOG` is unset.
fn level_for(debug: bool) -> &'static str {
    if debug {
        "debug"
    } else {
        "info"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;
    use tracing_subscriber::fmt::MakeWriter;

    #[test]
    fn level_for_maps_build_profile() {
        assert_eq!(level_for(true), "debug");
        assert_eq!(level_for(false), "info");
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
