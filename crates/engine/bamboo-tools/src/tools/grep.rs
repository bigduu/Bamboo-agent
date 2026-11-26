use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_infrastructure::telemetry::{
    is_current_span_recording, record_grep_performance, record_grep_resources, GrepPerformance,
};
use globset::{GlobBuilder, GlobSet};
use grep_regex::{RegexMatcher, RegexMatcherBuilder};
use grep_searcher::{sinks::UTF8, SearcherBuilder};
use regex::{Regex, RegexBuilder};
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::{parameter_schema, search_traversal, workspace_state};

mod thread_cpu;

const DEFAULT_HEAD_LIMIT: usize = 200;
const MAX_RESULT_BYTES: usize = 256 * 1024;
const MAX_MATCHES: usize = 2_000;
const MAX_SCANNED_FILES: usize = 50_000;
const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024;
static GREP_WORKERS: Semaphore = Semaphore::const_new(4);
const SEARCH_SCOPE_TOO_BROAD_ERROR: &str =
    "Search scope too broad. Add path/glob/type or reduce pattern.";
const MULTILINE_REQUIRES_NARROWED_PATH_ERROR: &str = "Multiline grep requires narrowed path.";
const RESULT_TOO_LARGE_ERROR: &str = "Result too large; refine query and retry.";

#[derive(Debug, PartialEq, Eq)]
enum SearchTextRead {
    Complete,
    TooLarge,
    Unreadable,
}

#[derive(Default)]
struct SearchMetrics {
    performance: GrepPerformance,
    record_resources: bool,
    read_buffer_peak_capacity_bytes: usize,
}

#[derive(Default)]
struct SearchCoverage {
    scan_limited: bool,
    traversal_errors: usize,
    oversized_files: usize,
    unreadable_files: usize,
    non_text_files: usize,
}

impl SearchCoverage {
    fn notice(&self, scan_limit: usize) -> Option<String> {
        let mut reasons = Vec::new();
        if self.scan_limited {
            reasons.push(format!(
                "directory discovery stopped at {scan_limit} candidate files"
            ));
        }
        if self.traversal_errors != 0 {
            reasons.push(format!(
                "{} directory traversal error(s)",
                self.traversal_errors
            ));
        }
        if self.oversized_files != 0 {
            reasons.push(format!(
                "{} file(s) exceeded the {MAX_FILE_BYTES}-byte per-file limit",
                self.oversized_files
            ));
        }
        if self.unreadable_files != 0 {
            reasons.push(format!(
                "{} unreadable or invalid UTF-8 file(s) skipped",
                self.unreadable_files
            ));
        }
        if self.non_text_files != 0 {
            reasons.push(format!(
                "{} file(s) containing NUL bytes skipped",
                self.non_text_files
            ));
        }
        (!reasons.is_empty()).then(|| {
            format!(
                "[PARTIAL] Search coverage is incomplete: {}. Narrow path/glob/type or check skipped files separately.",
                reasons.join("; ")
            )
        })
    }
}

#[derive(Debug, Deserialize, Clone, Copy, Default, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(inline)]
enum OutputMode {
    Content,
    #[default]
    FilesWithMatches,
    Count,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[schemars(deny_unknown_fields)]
struct GrepArgs {
    /// Regex pattern
    pattern: String,
    /// File or directory to search. An explicit file bypasses ignore rules. Narrow this for expensive or multiline searches.
    #[serde(default)]
    #[schemars(with = "String", skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    /// Glob file filter used to limit candidate files
    #[serde(default)]
    #[schemars(with = "String", skip_serializing_if = "Option::is_none")]
    glob: Option<String>,
    /// Output mode. Prefer files_with_matches for broad discovery, then refine with Read or content mode.
    #[serde(default)]
    #[schemars(with = "OutputMode", skip_serializing_if = "Option::is_none")]
    output_mode: Option<OutputMode>,
    /// Lines before match
    #[serde(rename = "-B", default)]
    #[schemars(
        schema_with = "parameter_schema::number",
        skip_serializing_if = "Option::is_none"
    )]
    before: Option<usize>,
    /// Lines after match
    #[serde(rename = "-A", default)]
    #[schemars(
        schema_with = "parameter_schema::number",
        skip_serializing_if = "Option::is_none"
    )]
    after: Option<usize>,
    /// Lines before and after match
    #[serde(rename = "-C", default)]
    #[schemars(
        schema_with = "parameter_schema::number",
        skip_serializing_if = "Option::is_none"
    )]
    context: Option<usize>,
    /// Show line numbers
    #[serde(rename = "-n", default)]
    #[schemars(with = "bool", skip_serializing_if = "Option::is_none")]
    line_numbers: Option<bool>,
    /// Case insensitive
    #[serde(rename = "-i", default)]
    #[schemars(with = "bool", skip_serializing_if = "Option::is_none")]
    case_insensitive: Option<bool>,
    /// File type filter (for example rust, js, ts, py)
    #[serde(default)]
    #[schemars(with = "String", skip_serializing_if = "Option::is_none")]
    r#type: Option<String>,
    /// Limit output entries. Reaching the limit may stop the search before remaining files are checked.
    #[serde(default)]
    #[schemars(
        schema_with = "parameter_schema::number",
        skip_serializing_if = "Option::is_none"
    )]
    head_limit: Option<usize>,
    /// Enable multiline regex. Requires a narrowed path.
    #[serde(default)]
    #[schemars(with = "bool", skip_serializing_if = "Option::is_none")]
    multiline: Option<bool>,
    /// Include gitignored files. Requires an explicit path; scan/result limits and fixed directory exclusions still apply.
    #[serde(default)]
    include_ignored: bool,
}

pub struct GrepTool;

impl GrepTool {
    pub fn new() -> Self {
        Self
    }

    fn extension_map() -> HashMap<&'static str, &'static [&'static str]> {
        HashMap::from([
            ("js", &["js", "mjs", "cjs"] as &[_]),
            ("ts", &["ts", "tsx"]),
            ("py", &["py"]),
            ("rust", &["rs"]),
            ("go", &["go"]),
            ("java", &["java"]),
            ("cpp", &["cc", "cpp", "cxx", "hpp", "h"]),
            ("c", &["c", "h"]),
            ("json", &["json"]),
            ("yaml", &["yaml", "yml"]),
            ("toml", &["toml"]),
            ("md", &["md", "markdown"]),
        ])
    }

    fn collect_files(
        base: &Path,
        type_filter: Option<&str>,
        include_ignored: bool,
        cancel: &CancellationToken,
        scan_limit: usize,
        coverage: &mut SearchCoverage,
    ) -> Vec<PathBuf> {
        #[cfg(test)]
        tests::pause_directory_discovery(base);

        let ext_map = Self::extension_map();
        let allowed_ext = type_filter.and_then(|name| ext_map.get(name).copied());

        let mut files = Vec::new();
        for entry in search_traversal::walk(base, include_ignored) {
            if cancel.is_cancelled() {
                break;
            }
            if files.len() >= scan_limit {
                coverage.scan_limited = true;
                break;
            }
            let Ok(entry) = entry else {
                coverage.traversal_errors += 1;
                continue;
            };
            if !entry.file_type().is_some_and(|kind| kind.is_file()) {
                continue;
            }
            let path = entry.path();

            if let Some(extensions) = allowed_ext {
                let ext = path
                    .extension()
                    .and_then(|v| v.to_str())
                    .unwrap_or_default();
                if !extensions.iter().any(|candidate| candidate == &ext) {
                    continue;
                }
            }

            files.push(path.to_path_buf());
        }

        files
    }

    fn read_search_text(reader: impl Read, content: &mut String) -> SearchTextRead {
        // A file can grow after stat. Check one extra byte and skip the
        // entire file if it is oversized or contains invalid UTF-8.
        content.clear();
        let mut reader = reader.take(MAX_FILE_BYTES + 1);
        let result = reader.read_to_string(content);
        if reader.limit() == 0 {
            SearchTextRead::TooLarge
        } else if result.is_ok() {
            SearchTextRead::Complete
        } else {
            SearchTextRead::Unreadable
        }
    }

    fn exact_file_size_limit_error() -> ToolError {
        ToolError::Execution(format!(
            "Grep did not search the specified file because it exceeds the per-file search limit of {MAX_FILE_BYTES} bytes."
        ))
    }

    fn literal_matcher(
        pattern: &str,
        case_insensitive: bool,
        multiline: bool,
    ) -> Option<RegexMatcher> {
        // Only accelerate patterns whose per-line match semantics are
        // identical. The original regex handles anchors, escapes and flags.
        if case_insensitive
            || multiline
            || pattern.is_empty()
            || pattern.contains(['\r', '\n'])
            || regex::escape(pattern) != pattern
        {
            return None;
        }
        RegexMatcherBuilder::new()
            .line_terminator(Some(b'\n'))
            .build(pattern)
            .ok()
    }

    fn compile_glob(glob: Option<&str>) -> Result<Option<GlobSet>, ToolError> {
        let Some(pattern) = glob else {
            return Ok(None);
        };

        let mut builder = globset::GlobSetBuilder::new();
        let glob = GlobBuilder::new(pattern)
            .literal_separator(false)
            .build()
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid glob pattern: {}", e)))?;
        builder.add(glob);
        builder
            .build()
            .map(Some)
            .map_err(|e| ToolError::Execution(format!("Failed to compile glob: {}", e)))
    }

    fn compile_regex(
        pattern: &str,
        case_insensitive: bool,
        multiline: bool,
    ) -> Result<Regex, ToolError> {
        let mut builder = RegexBuilder::new(pattern);
        builder.case_insensitive(case_insensitive);
        builder.dot_matches_new_line(multiline);
        builder.multi_line(multiline);
        builder
            .build()
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid regex pattern: {}", e)))
    }

    fn byte_to_line(line_starts: &[usize], byte: usize) -> usize {
        match line_starts.binary_search(&byte) {
            Ok(idx) => idx,
            Err(idx) => idx.saturating_sub(1),
        }
    }

    fn format_content_hits(
        path: &Path,
        content: &str,
        regex: &Regex,
        multiline: bool,
        before: usize,
        after: usize,
        line_numbers: bool,
    ) -> Vec<String> {
        let lines: Vec<&str> = content.lines().collect();
        if lines.is_empty() {
            return Vec::new();
        }

        let mut selected_lines = BTreeSet::new();

        if multiline {
            let mut line_starts = vec![0usize];
            for (idx, byte) in content.bytes().enumerate() {
                if byte == b'\n' {
                    line_starts.push(idx + 1);
                }
            }

            for mat in regex.find_iter(content) {
                let start_line = Self::byte_to_line(&line_starts, mat.start());
                let end_line = Self::byte_to_line(&line_starts, mat.end().saturating_sub(1));
                let range_start = start_line.saturating_sub(before);
                let range_end = (end_line + after).min(lines.len().saturating_sub(1));
                for line_idx in range_start..=range_end {
                    selected_lines.insert(line_idx);
                }
            }
        } else {
            for (idx, line) in lines.iter().enumerate() {
                if regex.is_match(line) {
                    let range_start = idx.saturating_sub(before);
                    let range_end = (idx + after).min(lines.len().saturating_sub(1));
                    for line_idx in range_start..=range_end {
                        selected_lines.insert(line_idx);
                    }
                }
            }
        }

        selected_lines
            .into_iter()
            .map(|idx| {
                let display_path = bamboo_config::paths::path_to_display_string(path);
                if line_numbers {
                    format!("{}:{}:{}", display_path, idx + 1, lines[idx])
                } else {
                    format!("{}:{}", display_path, lines[idx])
                }
            })
            .collect()
    }

    fn resolve_search_root(path: Option<&str>, cwd: &Path) -> PathBuf {
        match path {
            Some(path) => {
                let candidate = PathBuf::from(path);
                if candidate.is_absolute() {
                    candidate
                } else {
                    cwd.join(candidate)
                }
            }
            None => cwd.to_path_buf(),
        }
    }

    fn validate_scope(
        args: &GrepArgs,
        output_mode: OutputMode,
        multiline: bool,
        cwd: &Path,
    ) -> Result<(), ToolError> {
        if args.include_ignored && args.path.is_none() {
            return Err(ToolError::InvalidArguments(
                "include_ignored requires an explicit path.".to_string(),
            ));
        }
        if matches!(output_mode, OutputMode::Content)
            && args.path.is_none()
            && args.glob.is_none()
            && args.r#type.is_none()
        {
            return Err(ToolError::InvalidArguments(
                SEARCH_SCOPE_TOO_BROAD_ERROR.to_string(),
            ));
        }

        if multiline {
            let Some(path) = args.path.as_deref() else {
                return Err(ToolError::InvalidArguments(
                    MULTILINE_REQUIRES_NARROWED_PATH_ERROR.to_string(),
                ));
            };

            let resolved = Self::resolve_search_root(Some(path), cwd);
            if resolved.is_dir() {
                if let (Ok(resolved_canonical), Ok(cwd_canonical)) =
                    (resolved.canonicalize(), cwd.canonicalize())
                {
                    if resolved_canonical == cwd_canonical {
                        return Err(ToolError::InvalidArguments(
                            MULTILINE_REQUIRES_NARROWED_PATH_ERROR.to_string(),
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

impl Default for GrepTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for GrepTool {
    fn name(&self) -> &str {
        "Grep"
    }

    fn description(&self) -> &str {
        "Search file contents using ripgrep-style regex parameters. Directory searches respect repository .gitignore rules, including parent rules up to the Git root; non-repository searches do not apply ignore rules. Hidden files remain visible; global Git ignores, .ignore and .git/info/exclude are not applied. Directory results flag incomplete coverage from skipped files or the scan limit separately from output limits. Searching one oversized file reports the per-file limit. Start with files_with_matches or a narrowed path/glob/type before using content or multiline mode."
    }

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
    }

    fn parameters_schema(&self) -> serde_json::Value {
        parameter_schema::for_arguments::<GrepArgs>()
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parsed: GrepArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Grep args: {}", e)))?;

        let cwd = workspace_state::workspace_or_process_cwd(ctx.session_id());
        let root = Self::resolve_search_root(parsed.path.as_deref(), &cwd);

        let multiline = parsed.multiline.unwrap_or(false);
        let case_insensitive = parsed.case_insensitive.unwrap_or(false);
        Self::validate_scope(
            &parsed,
            parsed.output_mode.unwrap_or_default(),
            multiline,
            &cwd,
        )?;
        let regex = Self::compile_regex(&parsed.pattern, case_insensitive, multiline)?;
        let glob_filter = Self::compile_glob(parsed.glob.as_deref())?;

        let queued_at = Instant::now();
        let permit = GREP_WORKERS.acquire().await.expect("static Grep semaphore");
        let record_resources = is_current_span_recording();
        let cancel = CancellationToken::new();
        let _cancel_on_drop = cancel.clone().drop_guard();
        // Keep both filesystem and matching work off the Actix worker. One
        // search uses one worker and reuses its read buffer between files.
        let (result, metrics, scan_cpu_time) = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let started_at = Instant::now();
            let mut metrics = SearchMetrics {
                performance: GrepPerformance {
                    queue_time: started_at.duration_since(queued_at),
                    ..GrepPerformance::default()
                },
                record_resources,
                read_buffer_peak_capacity_bytes: 0,
            };
            let cpu_started_at = record_resources.then(thread_cpu::time).flatten();
            let result = Self::search(
                &parsed,
                &root,
                &regex,
                glob_filter.as_ref(),
                &cancel,
                &mut metrics,
                MAX_SCANNED_FILES,
            );
            let scan_cpu_time =
                cpu_started_at.and_then(|start| thread_cpu::time()?.checked_sub(start));
            metrics.performance.scan_time = started_at.elapsed();
            (result, metrics, scan_cpu_time)
        })
        .await
        .map_err(|error| ToolError::Execution(format!("Grep worker failed: {error}")))?;
        record_grep_performance(&metrics.performance);
        if record_resources {
            record_grep_resources(scan_cpu_time, metrics.read_buffer_peak_capacity_bytes);
        }
        result
    }
}

impl GrepTool {
    fn search(
        parsed: &GrepArgs,
        root: &Path,
        regex: &Regex,
        glob_filter: Option<&GlobSet>,
        cancel: &CancellationToken,
        metrics: &mut SearchMetrics,
        scan_limit: usize,
    ) -> Result<ToolOutcome, ToolError> {
        let record_resources = metrics.record_resources;
        let performance = &mut metrics.performance;
        let output_mode = parsed.output_mode.unwrap_or_default();
        let context = parsed.context.unwrap_or(0);
        let before = parsed.before.unwrap_or(context);
        let after = parsed.after.unwrap_or(context);
        let line_numbers = parsed.line_numbers.unwrap_or(false);
        let multiline = parsed.multiline.unwrap_or(false);
        let head_limit = parsed.head_limit.unwrap_or(DEFAULT_HEAD_LIMIT);
        let literal = Self::literal_matcher(
            &parsed.pattern,
            parsed.case_insensitive.unwrap_or(false),
            multiline,
        );
        let mut searcher = literal
            .as_ref()
            .map(|_| SearcherBuilder::new().bom_sniffing(false).build());

        let mut coverage = SearchCoverage::default();
        let exact_file = root.is_file();
        let files = if exact_file {
            vec![root.to_path_buf()]
        } else if root.is_dir() {
            Self::collect_files(
                root,
                parsed.r#type.as_deref(),
                parsed.include_ignored,
                cancel,
                scan_limit,
                &mut coverage,
            )
        } else {
            return Err(ToolError::Execution(format!(
                "Path does not exist: {}",
                root.display()
            )));
        };
        let mut read_buffer = String::new();
        let mut result_lines = Vec::new();
        let mut output_limit = None;
        let mut files = files.into_iter();

        while let Some(file) = files.next() {
            if cancel.is_cancelled() {
                return Err(ToolError::Execution("Grep cancelled".to_owned()));
            }
            if let Some(filter) = glob_filter {
                let relative = file.strip_prefix(root).unwrap_or(&file);
                if !filter.is_match(relative) && !filter.is_match(&file) {
                    continue;
                }
            }

            let Ok(metadata) = std::fs::metadata(&file) else {
                coverage.unreadable_files += 1;
                continue;
            };
            if !metadata.is_file() {
                coverage.unreadable_files += 1;
                continue;
            }
            if metadata.len() > MAX_FILE_BYTES {
                if exact_file {
                    return Err(Self::exact_file_size_limit_error());
                }
                coverage.oversized_files += 1;
                continue;
            }

            let Ok(reader) = std::fs::File::open(&file) else {
                coverage.unreadable_files += 1;
                continue;
            };
            let read = Self::read_search_text(reader, &mut read_buffer);
            if record_resources {
                metrics.read_buffer_peak_capacity_bytes = metrics
                    .read_buffer_peak_capacity_bytes
                    .max(read_buffer.capacity());
            }
            match read {
                SearchTextRead::Complete if !read_buffer.contains('\0') => {}
                SearchTextRead::TooLarge if exact_file => {
                    return Err(Self::exact_file_size_limit_error());
                }
                SearchTextRead::TooLarge => {
                    coverage.oversized_files += 1;
                    continue;
                }
                SearchTextRead::Unreadable => {
                    coverage.unreadable_files += 1;
                    continue;
                }
                SearchTextRead::Complete => {
                    coverage.non_text_files += 1;
                    continue;
                }
            }
            let content = read_buffer.as_str();
            performance.files_scanned += 1;
            performance.bytes_scanned += content.len() as u64;

            let fast_count =
                searcher
                    .as_mut()
                    .zip(literal.as_ref())
                    .and_then(|(searcher, matcher)| {
                        let mut count = 0;
                        searcher
                            .search_slice(
                                matcher,
                                content.as_bytes(),
                                UTF8(|_, _| {
                                    count += 1;
                                    Ok(true)
                                }),
                            )
                            .ok()?;
                        performance.ripgrep_files += 1;
                        Some(count)
                    });
            let match_count = fast_count.unwrap_or_else(|| {
                if multiline {
                    regex.find_iter(content).count()
                } else {
                    content.lines().filter(|line| regex.is_match(line)).count()
                }
            });
            if match_count == 0 {
                continue;
            }

            performance.matches += match_count as u64;
            if performance.matches > MAX_MATCHES as u64 {
                return Err(ToolError::Execution(RESULT_TOO_LARGE_ERROR.to_string()));
            }

            match output_mode {
                OutputMode::FilesWithMatches => {
                    result_lines.push(bamboo_config::paths::path_to_display_string(&file));
                }
                OutputMode::Count => {
                    let path = bamboo_config::paths::path_to_display_string(&file);
                    result_lines.push(format!("{path}:{match_count}"));
                }
                OutputMode::Content => {
                    result_lines.extend(Self::format_content_hits(
                        &file,
                        content,
                        regex,
                        multiline,
                        before,
                        after,
                        line_numbers,
                    ));
                }
            }
            if result_lines.len() >= head_limit {
                output_limit = match (
                    result_lines.len() > head_limit,
                    !files.as_slice().is_empty(),
                ) {
                    (true, true) => Some(
                        "Entries were truncated and remaining candidate files were not searched.",
                    ),
                    (true, false) => Some("Entries were truncated."),
                    (false, true) => Some("Remaining candidate files were not searched."),
                    (false, false) => None,
                };
                break;
            }
        }

        result_lines.truncate(head_limit);
        if let Some(detail) = output_limit {
            result_lines.insert(0, format!(
                "[PARTIAL] Output limited by head_limit={head_limit}. {detail} Narrow path/pattern or raise head_limit."
            ));
        }
        if let Some(notice) = coverage.notice(scan_limit) {
            result_lines.insert(0, notice);
        }

        let result = result_lines.join("\n");
        if result.len() > MAX_RESULT_BYTES {
            return Err(ToolError::Execution(RESULT_TOO_LARGE_ERROR.to_string()));
        }

        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result,
            display_preference: Some("Collapsible".to_string()),
            images: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{mpsc, Mutex, OnceLock};
    use std::time::Duration;
    use tokio::sync::oneshot;

    struct DiscoveryPause {
        started: oneshot::Sender<()>,
        release: mpsc::Receiver<()>,
    }

    fn discovery_pauses() -> &'static Mutex<HashMap<PathBuf, DiscoveryPause>> {
        static PAUSES: OnceLock<Mutex<HashMap<PathBuf, DiscoveryPause>>> = OnceLock::new();
        PAUSES.get_or_init(Mutex::default)
    }

    pub(super) fn pause_directory_discovery(path: &Path) {
        let pause = discovery_pauses().lock().unwrap().remove(path);
        if let Some(pause) = pause {
            pause.started.send(()).unwrap();
            // A watchdog makes a regression fail instead of deadlocking the
            // current-thread test runtime; this is not a traversal benchmark.
            pause
                .release
                .recv_timeout(Duration::from_secs(5))
                .expect("directory discovery blocked the async runtime");
        }
    }

    fn pause_next_discovery(path: &Path) -> (oneshot::Receiver<()>, mpsc::Sender<()>) {
        let (started, observed) = oneshot::channel();
        let (release, wait) = mpsc::channel();
        assert!(discovery_pauses()
            .lock()
            .unwrap()
            .insert(
                path.to_owned(),
                DiscoveryPause {
                    started,
                    release: wait,
                },
            )
            .is_none());
        (observed, release)
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn grep_directory_discovery_keeps_async_heartbeat_running() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("match.rs"), "needle\n").unwrap();
        let mut heartbeat_timer = tokio::time::interval(Duration::from_secs(5));
        heartbeat_timer.tick().await;
        let (started, release) = pause_next_discovery(dir.path());
        let args = json!({"pattern": "needle", "path": dir.path()});
        let search = tokio::spawn(async move { run(&GrepTool::new(), args).await });

        started.await.unwrap();
        let heartbeat = tokio::spawn(async move { heartbeat_timer.tick().await });
        tokio::time::advance(Duration::from_secs(5)).await;
        heartbeat.await.unwrap();
        assert!(
            !search.is_finished(),
            "heartbeat must run while directory discovery is still blocked"
        );
        release.send(()).unwrap();

        let result = search.await.unwrap().unwrap();
        assert!(result.success);
        assert!(result.result.contains("match.rs"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn grep_can_be_cancelled_while_directory_discovery_is_blocked() {
        let dir = tempfile::tempdir().unwrap();
        let (started, release) = pause_next_discovery(dir.path());
        let args = json!({"pattern": "needle", "path": dir.path()});
        let search = tokio::spawn(async move { run(&GrepTool::new(), args).await });

        started.await.unwrap();
        search.abort();
        let cancelled = search.await.unwrap_err().is_cancelled();
        release.send(()).unwrap();
        assert!(cancelled);
    }

    #[tokio::test]
    async fn grep_missing_path_preserves_execution_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        let error = run(
            &GrepTool::new(),
            json!({"pattern": "needle", "path": missing}),
        )
        .await
        .unwrap_err();

        match error {
            ToolError::Execution(message) => {
                assert_eq!(
                    message,
                    format!("Path does not exist: {}", missing.display())
                );
            }
            other => panic!("expected execution error, got {other}"),
        }
    }

    #[tokio::test]
    async fn grep_resource_fields_follow_each_search_when_scans_overlap() {
        use bamboo_infrastructure::telemetry::{
            test_support::LocalTraceTest, Observation, Outcome,
        };

        let large = tempfile::tempdir().unwrap();
        let small = tempfile::tempdir().unwrap();
        let empty = tempfile::tempdir().unwrap();
        let mut rejected_contents = vec![b'x'; 128 * 1024];
        rejected_contents[0] = 0;
        std::fs::write(large.path().join("binary.txt"), &rejected_contents).unwrap();
        let small_file = small.path().join("small.txt");
        std::fs::write(&small_file, "needle\n").unwrap();

        let local = LocalTraceTest::isolated();
        let birth = chrono::Utc::now();
        let tool = GrepTool::new();
        let scan = async |session_id: &str, path: &Path| {
            let observation = Observation::tool("Grep", "call", session_id, "round")
                .bind_session_lifetime(session_id, birth);
            let (trace_id, _) = observation.trace_ids().unwrap();
            let result = observation
                .scope(run(&tool, json!({"pattern": "needle", "path": path})))
                .await
                .unwrap();
            assert!(result.success);
            observation.finish(Outcome::Success);
            let detail = local.store.detail(session_id, birth, &trace_id).unwrap();
            let performance = &detail.spans[0].performance;
            if cfg!(any(target_os = "macos", target_os = "linux", windows)) {
                assert!(performance.contains_key("grep_scan_cpu_us"));
            }
            (
                performance["grep_files_scanned"],
                performance["grep_read_buffer_peak_capacity_bytes"],
            )
        };
        let (started, release) = pause_next_discovery(large.path());
        let (large, (small, empty)) = local
            .scope(async {
                tokio::join!(scan("large", large.path()), async {
                    started.await.unwrap();
                    let measurements =
                        tokio::join!(scan("small", &small_file), scan("empty", empty.path()));
                    release.send(()).unwrap();
                    measurements
                })
            })
            .await;

        assert_eq!(large.0, 0, "the binary file was read and rejected");
        assert!(large.1 >= rejected_contents.len() as u64);
        assert_eq!(small.0, 1);
        assert!(small.1 >= 7 && small.1 < large.1);
        assert_eq!(empty, (0, 0));
    }

    #[test]
    fn grep_args_preserve_flag_names_and_all_values() {
        let parsed: GrepArgs = serde_json::from_value(json!({
            "pattern": "needle",
            "path": "src",
            "glob": "**/*.rs",
            "output_mode": "content",
            "-B": 1,
            "-A": 2,
            "-C": 3,
            "-n": true,
            "-i": false,
            "type": "rust",
            "head_limit": 0,
            "multiline": false,
            "include_ignored": true,
            "before": 99,
            "after": 99,
            "context": 99,
            "line_numbers": false,
            "case_insensitive": true
        }))
        .unwrap();

        assert_eq!(parsed.pattern, "needle");
        assert_eq!(parsed.path.as_deref(), Some("src"));
        assert_eq!(parsed.glob.as_deref(), Some("**/*.rs"));
        assert!(matches!(parsed.output_mode, Some(OutputMode::Content)));
        assert_eq!(parsed.before, Some(1));
        assert_eq!(parsed.after, Some(2));
        assert_eq!(parsed.context, Some(3));
        assert_eq!(parsed.line_numbers, Some(true));
        assert_eq!(parsed.case_insensitive, Some(false));
        assert_eq!(parsed.r#type.as_deref(), Some("rust"));
        assert_eq!(parsed.head_limit, Some(0));
        assert_eq!(parsed.multiline, Some(false));
        assert!(parsed.include_ignored);
    }

    #[test]
    fn grep_args_preserve_defaults_nulls_and_unknown_fields() {
        for value in [
            json!({"pattern": "needle"}),
            json!({
                "pattern": "needle", "before": 1, "after": 1, "context": 1,
                "line_numbers": true, "case_insensitive": true, "unknown": true
            }),
            json!({
                "pattern": "needle", "path": null, "glob": null,
                "output_mode": null, "-B": null, "-A": null, "-C": null,
                "-n": null, "-i": null, "type": null, "head_limit": null,
                "multiline": null
            }),
        ] {
            let parsed: GrepArgs = serde_json::from_value(value).unwrap();
            assert_eq!(parsed.pattern, "needle");
            assert!(parsed.path.is_none());
            assert!(parsed.glob.is_none());
            assert!(parsed.output_mode.is_none());
            assert!(parsed.before.is_none());
            assert!(parsed.after.is_none());
            assert!(parsed.context.is_none());
            assert!(parsed.line_numbers.is_none());
            assert!(parsed.case_insensitive.is_none());
            assert!(parsed.r#type.is_none());
            assert!(parsed.head_limit.is_none());
            assert!(parsed.multiline.is_none());
            assert!(!parsed.include_ignored);
            assert!(matches!(
                parsed.output_mode.unwrap_or_default(),
                OutputMode::FilesWithMatches
            ));
        }
    }

    #[test]
    fn grep_args_preserve_output_mode_spellings() {
        for (name, expected) in [
            ("content", OutputMode::Content),
            ("files_with_matches", OutputMode::FilesWithMatches),
            ("count", OutputMode::Count),
        ] {
            let parsed: GrepArgs =
                serde_json::from_value(json!({"pattern": "needle", "output_mode": name})).unwrap();
            assert_eq!(
                std::mem::discriminant(&parsed.output_mode.unwrap()),
                std::mem::discriminant(&expected)
            );
        }
    }

    #[test]
    fn grep_args_preserve_invalid_argument_rejection() {
        for value in [
            json!({}),
            json!({"pattern": null}),
            json!({"pattern": 1}),
            json!({"pattern": "needle", "include_ignored": null}),
            json!({"pattern": "needle", "output_mode": "FilesWithMatches"}),
            json!({"pattern": "needle", "output_mode": "unknown"}),
            json!({"pattern": "needle", "output_mode": 1}),
        ] {
            assert!(serde_json::from_value::<GrepArgs>(value).is_err());
        }
        for key in ["-B", "-A", "-C", "head_limit"] {
            for invalid in [json!(-1), json!(1.5), json!("1")] {
                let mut value = json!({"pattern": "needle"});
                value[key] = invalid;
                assert!(serde_json::from_value::<GrepArgs>(value).is_err(), "{key}");
            }
        }
        for key in ["-n", "-i", "multiline", "include_ignored"] {
            let mut value = json!({"pattern": "needle"});
            value[key] = json!(1);
            assert!(serde_json::from_value::<GrepArgs>(value).is_err(), "{key}");
        }
    }

    async fn run(tool: &GrepTool, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        match tool.invoke(args, ToolCtx::none("t")).await? {
            ToolOutcome::Completed(r) => Ok(r),
            _ => panic!("expected Completed"),
        }
    }

    fn result_lines(result: &ToolResult) -> Vec<&str> {
        result
            .result
            .lines()
            .filter(|line| !line.is_empty())
            .collect()
    }

    fn non_partial_lines(result: &ToolResult) -> Vec<&str> {
        result_lines(result)
            .into_iter()
            .filter(|line| !line.starts_with("[PARTIAL]"))
            .collect()
    }

    #[tokio::test]
    async fn grep_distinguishes_complete_no_match_from_skipped_files() {
        let dir = tempfile::tempdir().unwrap();
        let query = json!({"pattern": "needle", "path": dir.path()});
        let tool = GrepTool::new();
        tokio::fs::write(dir.path().join("searched.txt"), "other\n")
            .await
            .unwrap();
        let complete = run(&tool, query.clone()).await.unwrap();
        assert!(complete.success);
        assert!(complete.result.is_empty());

        let mut oversized = vec![b'x'; MAX_FILE_BYTES as usize + 1];
        oversized[..7].copy_from_slice(b"needle\n");
        tokio::fs::write(dir.path().join("too-large.txt"), oversized)
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("invalid.txt"), [0xff])
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("binary.txt"), [0])
            .await
            .unwrap();

        let incomplete = run(&tool, query).await.unwrap();
        assert!(incomplete.success);
        assert!(non_partial_lines(&incomplete).is_empty());
        assert!(incomplete
            .result
            .starts_with("[PARTIAL] Search coverage is incomplete"));
        assert!(incomplete
            .result
            .contains("1 file(s) exceeded the 2097152-byte per-file limit"));
        assert!(incomplete
            .result
            .contains("1 unreadable or invalid UTF-8 file(s) skipped"));
        assert!(incomplete
            .result
            .contains("1 file(s) containing NUL bytes skipped"));
        assert!(!incomplete.result.contains("Output limited"));
    }

    #[test]
    fn grep_reports_a_directory_scan_limit_only_when_traversal_stops_early() {
        let dir = tempfile::tempdir().unwrap();
        for file in ["first.txt", "second.txt"] {
            std::fs::write(dir.path().join(file), "other\n").unwrap();
        }
        let args: GrepArgs = serde_json::from_value(json!({"pattern": "needle"})).unwrap();
        let regex = Regex::new(&args.pattern).unwrap();
        let search = |scan_limit| {
            let ToolOutcome::Completed(result) = GrepTool::search(
                &args,
                dir.path(),
                &regex,
                None,
                &CancellationToken::new(),
                &mut SearchMetrics::default(),
                scan_limit,
            )
            .unwrap() else {
                panic!("expected Complete");
            };
            assert!(result.success);
            result.result
        };
        assert!(search(2).is_empty());
        let partial = search(1);
        assert!(partial.starts_with("[PARTIAL] Search coverage is incomplete"));
        assert!(partial.contains("directory discovery stopped at 1 candidate files"));
        assert!(!partial.contains("Output limited"));
    }

    #[tokio::test]
    async fn grep_output_limit_does_not_imply_skipped_file_coverage() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("matches.txt");
        tokio::fs::write(&file, "needle\nneedle\n").await.unwrap();
        let tool = GrepTool::new();
        let query = |limit| {
            json!({
                "pattern": "needle",
                "path": &file,
                "output_mode": "content",
                "head_limit": limit
            })
        };
        let partial = run(&tool, query(1)).await.unwrap();
        assert_eq!(non_partial_lines(&partial).len(), 1);
        assert!(partial
            .result
            .contains("[PARTIAL] Output limited by head_limit=1"));
        assert!(partial.result.contains("Entries were truncated."));
        assert!(!partial.result.contains("Search coverage is incomplete"));
        let exact = run(&tool, query(2)).await.unwrap();
        assert_eq!(result_lines(&exact).len(), 2);
        assert!(!exact.result.contains("[PARTIAL]"));
    }

    #[tokio::test]
    async fn grep_defaults_to_files_with_matches() {
        let dir = tempfile::tempdir().unwrap();
        let file_hit = dir.path().join("match.rs");
        let file_miss = dir.path().join("miss.txt");
        tokio::fs::write(&file_hit, "let value = 1;\nneedle\n")
            .await
            .unwrap();
        tokio::fs::write(&file_miss, "nothing to see\n")
            .await
            .unwrap();

        let tool = GrepTool::new();
        let result = run(
            &tool,
            json!({
                "pattern": "needle",
                "path": dir.path()
            }),
        )
        .await
        .unwrap();

        assert!(result.success);
        let lines = result_lines(&result);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("match.rs"));
    }

    #[tokio::test]
    async fn literal_search_preserves_unicode_crlf_and_counts_lines_not_occurrences() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("mixed.txt");
        std::fs::write(&file, "\u{feff}中文\r\nnone\r\n中文 中文\n中文").unwrap();

        for pattern in ["中文", "\u{feff}中文"] {
            assert!(GrepTool::literal_matcher(pattern, false, false).is_some());
            let result = run(
                &GrepTool::new(),
                json!({"pattern": pattern, "path": file, "output_mode": "count"}),
            )
            .await
            .unwrap();
            let expected = if pattern == "中文" { ":3" } else { ":1" };
            assert!(result.result.ends_with(expected), "{}", result.result);
        }
    }

    #[tokio::test]
    async fn grep_skips_project_worktree_checkout() {
        let dir = tempfile::tempdir().unwrap();
        let kept = dir.path().join("src/kept.txt");
        let duplicate = dir.path().join(".bamboo/worktree/child/duplicate.txt");
        tokio::fs::create_dir_all(kept.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::create_dir_all(duplicate.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&kept, "needle").await.unwrap();
        tokio::fs::write(&duplicate, "needle").await.unwrap();

        let result = run(
            &GrepTool::new(),
            json!({"pattern": "needle", "path": dir.path()}),
        )
        .await
        .unwrap();
        assert!(result.result.contains("kept.txt"));
        assert!(!result.result.contains("duplicate.txt"));
    }

    #[tokio::test]
    async fn grep_content_mode_supports_context_and_line_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("content.txt");
        tokio::fs::write(&file, "one\ntwo\nneedle\nfour\nfive\n")
            .await
            .unwrap();

        let tool = GrepTool::new();
        let result = run(
            &tool,
            json!({
                "pattern": "needle",
                "path": file,
                "output_mode": "content",
                "-C": 1,
                "-n": true
            }),
        )
        .await
        .unwrap();

        let output = result.result;
        assert!(output.contains(":2:two"));
        assert!(output.contains(":3:needle"));
        assert!(output.contains(":4:four"));
        assert!(!output.contains(":1:one"));
        assert!(!output.contains(":5:five"));
    }

    #[tokio::test]
    async fn grep_content_flags_preserve_context_override_and_case_matching() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("flags.txt");
        tokio::fs::write(&file, "one\ntwo\nNEEDLE\nfour\nfive\n")
            .await
            .unwrap();

        let result = run(
            &GrepTool::new(),
            json!({
                "pattern": "needle", "path": file, "output_mode": "content",
                "-C": 1, "-B": 2, "-A": 0, "-n": true, "-i": true
            }),
        )
        .await
        .unwrap();

        let lines = result_lines(&result);
        assert_eq!(lines.len(), 3);
        assert!(lines[0].ends_with(":1:one"));
        assert!(lines[1].ends_with(":2:two"));
        assert!(lines[2].ends_with(":3:NEEDLE"));
        assert!(!result.result.contains(":4:four"));
    }

    #[tokio::test]
    async fn grep_count_mode_respects_type_filter_and_head_limit() {
        let dir = tempfile::tempdir().unwrap();
        let file_rs_a = dir.path().join("a.rs");
        let file_rs_b = dir.path().join("b.rs");
        let file_txt = dir.path().join("c.txt");
        tokio::fs::write(&file_rs_a, "foo\nfoo\n").await.unwrap();
        tokio::fs::write(&file_rs_b, "foo\n").await.unwrap();
        tokio::fs::write(&file_txt, "foo\n").await.unwrap();

        let tool = GrepTool::new();
        let result = run(
            &tool,
            json!({
                "pattern": "foo",
                "path": dir.path(),
                "output_mode": "count",
                "type": "rust",
                "head_limit": 1
            }),
        )
        .await
        .unwrap();

        let lines = non_partial_lines(&result);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains(".rs:"));
        assert!(!lines[0].contains("c.txt"));
        assert!(result.result.contains("[PARTIAL]"));
    }

    #[tokio::test]
    async fn grep_multiline_and_case_insensitive_work_with_glob_filter() {
        let dir = tempfile::tempdir().unwrap();
        let file_one = dir.path().join("one.rs");
        let file_two = dir.path().join("two.rs");
        tokio::fs::write(&file_one, "Hello\nWORLD\n").await.unwrap();
        tokio::fs::write(&file_two, "Hello\nplanet\n")
            .await
            .unwrap();

        let tool = GrepTool::new();
        let result = run(
            &tool,
            json!({
                "pattern": "hello\\s+world",
                "path": dir.path(),
                "glob": "**/one.rs",
                "-i": true,
                "multiline": true
            }),
        )
        .await
        .unwrap();

        let output = result.result;
        assert!(output.contains("one.rs"));
        assert!(!output.contains("two.rs"));
    }

    #[tokio::test]
    async fn grep_content_mode_requires_scope_hint() {
        let tool = GrepTool::new();
        let error = run(
            &tool,
            json!({
                "pattern": "needle",
                "output_mode": "content"
            }),
        )
        .await
        .expect_err("content mode without scope should fail");

        assert!(matches!(error, ToolError::InvalidArguments(_)));
        assert!(error.to_string().contains(SEARCH_SCOPE_TOO_BROAD_ERROR));
    }

    #[tokio::test]
    async fn grep_multiline_requires_explicit_narrowed_path() {
        let tool = GrepTool::new();
        let error = run(
            &tool,
            json!({
                "pattern": "a\\s+b",
                "multiline": true
            }),
        )
        .await
        .expect_err("multiline without path should fail");
        assert!(matches!(error, ToolError::InvalidArguments(_)));
        assert!(error
            .to_string()
            .contains(MULTILINE_REQUIRES_NARROWED_PATH_ERROR));

        let cwd = std::env::current_dir().unwrap();
        let error = run(
            &tool,
            json!({
                "pattern": "a\\s+b",
                "multiline": true,
                "path": cwd
            }),
        )
        .await
        .expect_err("multiline at workspace root should fail");
        assert!(matches!(error, ToolError::InvalidArguments(_)));
        assert!(error
            .to_string()
            .contains(MULTILINE_REQUIRES_NARROWED_PATH_ERROR));
    }

    #[tokio::test]
    async fn grep_defaults_head_limit_to_200() {
        let dir = tempfile::tempdir().unwrap();
        for idx in 0..260 {
            let file = dir.path().join(format!("file-{idx}.txt"));
            tokio::fs::write(&file, "needle\n").await.unwrap();
        }

        let tool = GrepTool::new();
        let result = run(
            &tool,
            json!({
                "pattern": "needle",
                "path": dir.path()
            }),
        )
        .await
        .unwrap();

        let lines = non_partial_lines(&result);
        assert_eq!(lines.len(), 200);
        assert!(result.result.contains("[PARTIAL]"));
    }

    #[tokio::test]
    async fn grep_rejects_excessive_match_volume() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("huge.txt");
        let mut content = String::new();
        for _ in 0..(MAX_MATCHES + 1) {
            content.push_str("needle\n");
        }
        tokio::fs::write(&file, content).await.unwrap();

        let tool = GrepTool::new();
        let error = run(
            &tool,
            json!({
                "pattern": "needle",
                "path": file
            }),
        )
        .await
        .expect_err("should reject oversized results");

        assert!(matches!(error, ToolError::Execution(_)));
        assert!(error.to_string().contains(RESULT_TOO_LARGE_ERROR));
    }

    #[test]
    fn grep_bounds_reads_independently_of_file_metadata() {
        let bytes = vec![b'x'; MAX_FILE_BYTES as usize + 32];
        let at_limit = &bytes[..MAX_FILE_BYTES as usize];
        let mut buffer = String::new();
        assert_eq!(
            GrepTool::read_search_text(at_limit, &mut buffer),
            SearchTextRead::Complete
        );
        assert_eq!(buffer.len() as u64, MAX_FILE_BYTES);

        let mut reader = std::io::Cursor::new(bytes);
        assert_eq!(
            GrepTool::read_search_text(&mut reader, &mut buffer),
            SearchTextRead::TooLarge
        );
        assert_eq!(reader.position(), MAX_FILE_BYTES + 1);
        assert_eq!(
            GrepTool::read_search_text(&[0xFF][..], &mut buffer),
            SearchTextRead::Unreadable
        );
        assert_eq!(
            GrepTool::read_search_text(&b"hello"[..], &mut buffer),
            SearchTextRead::Complete
        );
        assert_eq!(buffer, "hello");
        assert!(buffer.capacity() >= MAX_FILE_BYTES as usize);
    }

    #[tokio::test]
    async fn grep_reports_oversized_explicit_file_without_returning_a_no_match_result() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut bytes = vec![b'x'; MAX_FILE_BYTES as usize + 1];
        bytes[..7].copy_from_slice(b"needle\n");
        let tool = GrepTool::new();
        let args = json!({"pattern": "needle", "path": file.path()});

        tokio::fs::write(file.path(), &bytes[..MAX_FILE_BYTES as usize])
            .await
            .unwrap();
        let at_limit = run(&tool, args.clone()).await.unwrap();
        assert!(at_limit.success);
        assert_eq!(result_lines(&at_limit).len(), 1);

        tokio::fs::write(file.path(), &bytes).await.unwrap();
        let error = run(&tool, args.clone())
            .await
            .expect_err("an oversized exact file was not searched");
        assert!(matches!(error, ToolError::Execution(_)));
        let message = error.to_string();
        assert!(message.contains("did not search the specified file"));
        assert!(message.contains(&MAX_FILE_BYTES.to_string()));

        tokio::fs::write(file.path(), "unrelated\n").await.unwrap();
        let no_match = run(&tool, args).await.unwrap();
        assert!(no_match.success);
        assert!(no_match.result.is_empty());
    }

    #[tokio::test]
    async fn grep_ignored_file_opt_out_keeps_type_glob_and_file_size_limits() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        tokio::fs::write(dir.path().join(".gitignore"), "*\n")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("small.rs"), "needle\n")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("other.rs"), "needle\n")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("small.txt"), "needle\n")
            .await
            .unwrap();
        let mut oversized = vec![b'x'; MAX_FILE_BYTES as usize + 1];
        oversized[..6].copy_from_slice(b"needle");
        tokio::fs::write(dir.path().join("large.rs"), oversized)
            .await
            .unwrap();

        let result = run(
            &GrepTool::new(),
            json!({
                "pattern": "needle",
                "path": dir.path(),
                "include_ignored": true,
                "type": "rust",
                "glob": "**/{small,large}.rs"
            }),
        )
        .await
        .unwrap();
        let lines = non_partial_lines(&result);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("small.rs"));
        assert!(result.result.contains("1 file(s) exceeded"));
    }
}
