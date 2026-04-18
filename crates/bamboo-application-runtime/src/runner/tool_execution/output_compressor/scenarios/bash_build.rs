//! Compression for build/lint output: `cargo check`, `cargo clippy`, `tsc`, `eslint`, etc.
//!
//! **Strategy:**
//! - Zero errors/warnings → short-circuit: `"✅ No errors"`
//! - Errors present → keep error/warning lines with context, drop notes/help/info noise
//! - Group by severity: errors first, then warnings

use regex::Regex;

use crate::runner::tool_execution::output_compressor::filters;
use crate::runner::tool_execution::output_compressor::CompressionResult;

/// Minimum result length (chars) before compression kicks in.
const MIN_COMPRESS_LEN: usize = 1500;

/// Max lines to keep after compression.
const MAX_COMPRESSED_LINES: usize = 150;

lazy_static::lazy_static! {
    // ── Rust (cargo check / clippy) ──

    /// Matches: `error[E0308]: mismatched types` or `warning: unused variable`
    static ref RUST_DIAG_RE: Regex = Regex::new(
        r"(?m)^(error|warning)(?:\[E\d+\])?:\s+(.+)"
    ).expect("RUST_DIAG_RE must compile");

    /// Matches the location line: `  --> src/main.rs:42:5`
    static ref RUST_LOCATION_RE: Regex = Regex::new(
        r"(?m)^\s+-->\s+(.+:\d+:\d+)"
    ).expect("RUST_LOCATION_RE must compile");

    /// Matches: `error: could not compile` or `error: aborting due to`
    static ref RUST_FATAL_RE: Regex = Regex::new(
        r"(?m)^error(?:\[E\d+\])?: (?:could not compile|aborting due to)"
    ).expect("RUST_FATAL_RE must compile");

    /// Summary: `warning: `foo` (lib) generated 3 warnings`
    static ref RUST_SUMMARY_RE: Regex = Regex::new(
        r"(?m)^(?:warning|error):.+generated \d+ warning"
    ).expect("RUST_SUMMARY_RE must compile");

    // ── TypeScript (tsc) ──

    /// Matches: `src/index.ts(42,5): error TS2304: Cannot find name 'foo'.`
    static ref TSC_ERROR_RE: Regex = Regex::new(
        r"(?m)^(.+)\((\d+),(\d+)\):\s+(error)\s+(TS\d+):\s+(.+)"
    ).expect("TSC_ERROR_RE must compile");

    /// Matches: `Found N errors.` or `Found N errors in N files.`
    static ref TSC_SUMMARY_RE: Regex = Regex::new(
        r"(?m)^Found (\d+) errors?"
    ).expect("TSC_SUMMARY_RE must compile");

    // ── ESLint ──

    /// Matches eslint problem lines: `  42:5  error  Missing semicolon  semi`
    static ref ESLINT_PROBLEM_RE: Regex = Regex::new(
        r"(?m)^\s+(\d+):(\d+)\s+(error|warning)\s+(.+)"
    ).expect("ESLINT_PROBLEM_RE must compile");

    /// Matches eslint summary: `✖ 5 problems (3 errors, 2 warnings)`
    static ref ESLINT_SUMMARY_RE: Regex = Regex::new(
        r"[✖✗]\s+(\d+) problems?\s+\((\d+) errors?,\s*(\d+) warnings?\)"
    ).expect("ESLINT_SUMMARY_RE must compile");

    // ── Maven ──

    /// Matches: `[INFO] BUILD SUCCESS` or `[INFO] BUILD FAILURE`
    static ref MVN_BUILD_RESULT_RE: Regex = Regex::new(
        r"(?m)^\[INFO\]\s+BUILD (SUCCESS|FAILURE)"
    ).expect("MVN_BUILD_RESULT_RE must compile");

    /// Matches Maven timing: `[INFO] Total time:  12.345 s`
    static ref MVN_TIME_RE: Regex = Regex::new(
        r"(?m)^\[INFO\]\s+Total time:\s+(.+)"
    ).expect("MVN_TIME_RE must compile");

    /// Matches: `[INFO] Downloading from central:` or `[INFO] Downloaded from`
    static ref MVN_DOWNLOAD_RE: Regex = Regex::new(
        r"(?m)^\[INFO\]\s+(Downloading|Downloaded)\s+from\s+"
    ).expect("MVN_DOWNLOAD_RE must compile");

    /// Matches: `[ERROR] ...` lines
    static ref MVN_ERROR_RE: Regex = Regex::new(
        r"(?m)^\[ERROR\]\s+(.+)"
    ).expect("MVN_ERROR_RE must compile");

    /// Matches: `[WARNING] ...` lines
    static ref MVN_WARNING_RE: Regex = Regex::new(
        r"(?m)^\[WARNING\]\s+(.+)"
    ).expect("MVN_WARNING_RE must compile");

    /// Matches Surefire/Failsafe test summary:
    /// `Tests run: 42, Failures: 0, Errors: 0, Skipped: 2`
    static ref MVN_TEST_SUMMARY_RE: Regex = Regex::new(
        r"(?m)Tests run:\s+(\d+),\s+Failures:\s+(\d+),\s+Errors:\s+(\d+),\s+Skipped:\s+(\d+)"
    ).expect("MVN_TEST_SUMMARY_RE must compile");

    /// Matches Maven module header:
    /// `[INFO] --- maven-compiler-plugin:3.11.0:compile (default-compile) @ my-module ---`
    /// or `[INFO] Building my-module 1.0.0`
    static ref MVN_MODULE_RE: Regex = Regex::new(
        r"(?m)^\[INFO\]\s+(?:---|Building)\s+(.+)"
    ).expect("MVN_MODULE_RE must compile");

    /// Matches `[INFO] Compiling N source files` or `[INFO] Nothing to compile`
    static ref MVN_COMPILE_RE: Regex = Regex::new(
        r"(?m)^\[INFO\]\s+(Compiling \d+ source files?|Nothing to compile)"
    ).expect("MVN_COMPILE_RE must compile");

    // ── Gradle ──

    /// Matches: `BUILD SUCCESSFUL in 12s` or `BUILD FAILED`
    static ref GRADLE_RESULT_RE: Regex = Regex::new(
        r"(?m)^BUILD (SUCCESSFUL|FAILED)\b(?: in (\S+))?"
    ).expect("GRADLE_RESULT_RE must compile");

    /// Matches: `> Task :compileJava` or `> Task :app:compileKotlin`
    static ref GRADLE_TASK_RE: Regex = Regex::new(
        r"(?m)^> Task ([:\w]+)"
    ).expect("GRADLE_TASK_RE must compile");

    /// Matches Gradle download lines:
    /// `> https://repo.maven.apache.org/...`
    /// or `Downloading https://...` or `Download https://...`
    static ref GRADLE_DOWNLOAD_RE: Regex = Regex::new(
        r"(?m)^(?:> |Downloading |Download )https?://"
    ).expect("GRADLE_DOWNLOAD_RE must compile");

    /// Matches Gradle error/warning output
    static ref GRADLE_ERROR_RE: Regex = Regex::new(
        r"(?m)^(?:e:|FAILURE:|> .+Exception|> .+Error|> Could not)"
    ).expect("GRADLE_ERROR_RE must compile");

    /// Matches Gradle "N actionable tasks: N executed"
    static ref GRADLE_ACTIONABLE_RE: Regex = Regex::new(
        r"(?m)^(\d+) actionable tasks?:\s+(.+)"
    ).expect("GRADLE_ACTIONABLE_RE must compile");

    // ── Docker ──

    /// Matches Docker build step: `Step 3/12 : RUN apt-get update`
    /// or BuildKit format: `#5 [2/8] RUN apt-get update`
    static ref DOCKER_STEP_RE: Regex = Regex::new(
        r"(?m)^(?:Step (\d+)/(\d+)|#\d+\s+\[(\d+)/(\d+)\])\s*:?\s*(.*)"
    ).expect("DOCKER_STEP_RE must compile");

    /// Matches Docker layer cache: `---> Using cache` or `CACHED`
    static ref DOCKER_CACHE_RE: Regex = Regex::new(
        r"(?m)^(?:--->|#\d+)\s+(?:Using cache|CACHED)"
    ).expect("DOCKER_CACHE_RE must compile");

    /// Matches Docker hash line: `---> abc123def456`
    static ref DOCKER_HASH_RE: Regex = Regex::new(
        r"(?m)^--->\s+[0-9a-f]{12}"
    ).expect("DOCKER_HASH_RE must compile");

    /// Matches Docker "Successfully built" or "Successfully tagged"
    static ref DOCKER_SUCCESS_RE: Regex = Regex::new(
        r"(?m)^(?:Successfully (?:built|tagged)|naming to|exporting to image)"
    ).expect("DOCKER_SUCCESS_RE must compile");

    /// Matches Docker download progress: `Downloading [====>  ] 12.3MB/45.6MB`
    /// or `Extracting [====>  ]`
    static ref DOCKER_PROGRESS_RE: Regex = Regex::new(
        r"(?m)^(?:Downloading|Extracting|Waiting|Pulling fs layer|Verifying Checksum|Download complete|Pull complete)\b"
    ).expect("DOCKER_PROGRESS_RE must compile");

    // ── .NET (dotnet build) ──

    /// Matches: `Build succeeded.` or `Build FAILED.`
    static ref DOTNET_RESULT_RE: Regex = Regex::new(
        r"(?m)^Build (succeeded|FAILED)\."
    ).expect("DOTNET_RESULT_RE must compile");

    /// Matches: `  Restored /path/to/project.csproj (in 1.23 s).`
    static ref DOTNET_RESTORE_RE: Regex = Regex::new(
        r"(?m)^\s+Restored .+\.csproj"
    ).expect("DOTNET_RESTORE_RE must compile");

    /// Matches: `  Determining projects to restore...`
    static ref DOTNET_DETERMINING_RE: Regex = Regex::new(
        r"(?m)^\s+Determining projects to restore"
    ).expect("DOTNET_DETERMINING_RE must compile");

    /// Matches: `0 Warning(s)` and `0 Error(s)` in dotnet output
    static ref DOTNET_COUNTS_RE: Regex = Regex::new(
        r"(?m)^\s+(\d+) (Warning|Error)\(s\)"
    ).expect("DOTNET_COUNTS_RE must compile");

    // ── Terraform ──

    /// Matches: `Plan: 3 to add, 1 to change, 0 to destroy.`
    static ref TF_PLAN_SUMMARY_RE: Regex = Regex::new(
        r"(?m)^Plan:\s+(\d+)\s+to add,\s+(\d+)\s+to change,\s+(\d+)\s+to destroy\."
    ).expect("TF_PLAN_SUMMARY_RE must compile");

    /// Matches: `Apply complete! Resources: 3 added, 1 changed, 0 destroyed.`
    static ref TF_APPLY_SUMMARY_RE: Regex = Regex::new(
        r"(?m)^Apply complete! Resources:\s+(\d+)\s+added,\s+(\d+)\s+changed,\s+(\d+)\s+destroyed\."
    ).expect("TF_APPLY_SUMMARY_RE must compile");

    /// Matches terraform resource lines: `  # aws_instance.web will be created`
    static ref TF_RESOURCE_RE: Regex = Regex::new(
        r"(?m)^\s+#\s+(\S+)\s+will be (created|destroyed|updated|replaced)"
    ).expect("TF_RESOURCE_RE must compile");

    /// Matches terraform detail lines: `+ resource ...` or `~ resource ...` or `- resource ...`
    static ref TF_DETAIL_RE: Regex = Regex::new(
        r"(?m)^\s+[+~-]\s+"
    ).expect("TF_DETAIL_RE must compile");

    /// Matches: `Terraform will perform the following actions:`
    static ref TF_ACTIONS_HEADER_RE: Regex = Regex::new(
        r"(?m)^Terraform will perform the following actions:"
    ).expect("TF_ACTIONS_HEADER_RE must compile");
}

// ── Public Entry Point ─────────────────────────────────────────────────────

pub(crate) fn compress(raw_result: &str) -> CompressionResult {
    if raw_result.len() < MIN_COMPRESS_LEN {
        return CompressionResult {
            compressed: raw_result.to_string(),
            was_compressed: false,
        };
    }

    // Parse the Bash JSON envelope
    let parsed: serde_json::Value = match serde_json::from_str(raw_result) {
        Ok(v) => v,
        Err(_) => return compress_plain_build_text(raw_result),
    };

    let stdout = parsed.get("stdout").and_then(|v| v.as_str()).unwrap_or("");
    let stderr = parsed.get("stderr").and_then(|v| v.as_str()).unwrap_or("");
    let exit_code = parsed
        .get("exit_code")
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);

    let clean_stdout = filters::strip_ansi(stdout);
    let clean_stderr = filters::strip_ansi(stderr);
    let command = parsed.get("command").and_then(|v| v.as_str()).unwrap_or("");

    // Detect which build tool we're dealing with.
    // Maven must be checked before Rust because both contain "Compiling" text.
    if command.contains("mvn") {
        if let Some(result) = try_maven_compress(&clean_stdout, &clean_stderr, exit_code, &parsed) {
            return result;
        }
    }
    if let Some(result) = try_rust_build_compress(&clean_stdout, &clean_stderr, exit_code, &parsed)
    {
        return result;
    }
    if let Some(result) = try_tsc_compress(&clean_stdout, &clean_stderr, exit_code, &parsed) {
        return result;
    }
    if let Some(result) = try_eslint_compress(&clean_stdout, &clean_stderr, exit_code, &parsed) {
        return result;
    }
    // Also try Maven for auto-detected (non-mvn command) Maven output
    if let Some(result) = try_maven_compress(&clean_stdout, &clean_stderr, exit_code, &parsed) {
        return result;
    }
    if let Some(result) = try_gradle_compress(&clean_stdout, &clean_stderr, exit_code, &parsed) {
        return result;
    }
    if let Some(result) = try_docker_compress(&clean_stdout, &clean_stderr, exit_code, &parsed) {
        return result;
    }
    if let Some(result) = try_dotnet_compress(&clean_stdout, &clean_stderr, exit_code, &parsed) {
        return result;
    }
    if let Some(result) = try_terraform_compress(&clean_stdout, &clean_stderr, exit_code, &parsed) {
        return result;
    }

    // Fallback: use generic compression
    super::bash_generic::compress(raw_result)
}

// ── Rust Build (cargo check / clippy) ───────────────────────────────────────

fn try_rust_build_compress(
    stdout: &str,
    stderr: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    // Detect Rust output: look for "error[E" or "warning:" or "Compiling" in stderr
    let combined = format!("{}\n{}", stdout, stderr);
    if !combined.contains("Compiling")
        && !combined.contains("Checking")
        && !RUST_DIAG_RE.is_match(&combined)
    {
        return None;
    }

    let error_count = RUST_DIAG_RE
        .captures_iter(&combined)
        .filter(|c| &c[1] == "error")
        .count();
    let warning_count = RUST_DIAG_RE
        .captures_iter(&combined)
        .filter(|c| &c[1] == "warning")
        .count();

    // Zero errors + zero warnings + exit_code == 0 → clean build
    if error_count == 0 && exit_code == 0 {
        let mut summary = "✅ Build clean".to_string();
        if warning_count > 0 {
            summary = format!("⚠️ Build succeeded with {} warnings", warning_count);
            // Keep warning lines
            let warnings = extract_rust_diagnostics(&combined, "warning");
            let new_stderr = if warnings.is_empty() {
                String::new()
            } else {
                warnings
            };
            return Some(rebuild_json(original, &summary, &new_stderr));
        }
        return Some(rebuild_json(original, &summary, ""));
    }

    // Errors present → keep errors (full context), drop noise
    let errors = extract_rust_diagnostics(&combined, "error");
    let warnings_brief = count_rust_warnings(&combined);

    let mut new_stderr = format!("❌ {} errors", error_count);
    if warning_count > 0 {
        new_stderr.push_str(&format!(", {} warnings", warning_count));
    }
    new_stderr.push_str("\n\n");
    new_stderr.push_str(&errors);
    if !warnings_brief.is_empty() {
        new_stderr.push('\n');
        new_stderr.push_str(&warnings_brief);
    }

    let (capped, _) = filters::cap_lines(&new_stderr, MAX_COMPRESSED_LINES);
    Some(rebuild_json(original, "", &capped))
}

/// Extract diagnostic blocks (error or warning) with their location context.
fn extract_rust_diagnostics(text: &str, severity: &str) -> String {
    let mut result = String::new();
    let lines: Vec<&str> = text.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        if let Some(cap) = RUST_DIAG_RE.captures(lines[i]) {
            if &cap[1] == severity {
                // Keep the diagnostic line
                result.push_str(lines[i]);
                result.push('\n');

                // Keep following context: location line + code snippet (up to 5 lines)
                let context_end = (i + 6).min(lines.len());
                for line in &lines[i + 1..context_end] {
                    let line = *line;
                    // Stop at next diagnostic or blank separator
                    if RUST_DIAG_RE.is_match(line) || RUST_FATAL_RE.is_match(line) {
                        break;
                    }
                    // Keep location and code lines, skip "help:" and "note:" lines
                    if RUST_LOCATION_RE.is_match(line)
                        || line.starts_with("  |")
                        || line.trim().is_empty()
                    {
                        result.push_str(line);
                        result.push('\n');
                    }
                }
                result.push('\n');
            }
        }
        i += 1;
    }

    result
}

/// Produce a brief summary of warnings (just counts, not full text).
fn count_rust_warnings(text: &str) -> String {
    // Collect unique warning messages
    let warnings: Vec<String> = RUST_DIAG_RE
        .captures_iter(text)
        .filter(|c| &c[1] == "warning")
        .map(|c| c[2].to_string())
        .collect();

    if warnings.is_empty() {
        return String::new();
    }

    if warnings.len() <= 5 {
        let mut result = String::from("Warnings:\n");
        for w in &warnings {
            result.push_str(&format!("  ⚠️ {}\n", w));
        }
        result
    } else {
        format!(
            "Warnings: {} total (showing first 5)\n{}",
            warnings.len(),
            warnings[..5]
                .iter()
                .map(|w| format!("  ⚠️ {}", w))
                .collect::<Vec<_>>()
                .join("\n")
        )
    }
}

// ── TypeScript (tsc) ────────────────────────────────────────────────────────

fn try_tsc_compress(
    stdout: &str,
    stderr: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    let combined = format!("{}\n{}", stdout, stderr);

    // Detect tsc output
    if !TSC_ERROR_RE.is_match(&combined) && !TSC_SUMMARY_RE.is_match(&combined) {
        return None;
    }

    // Collect errors grouped by file
    let errors: Vec<String> = TSC_ERROR_RE
        .captures_iter(&combined)
        .map(|cap| {
            format!(
                "{}({}:{}): {} {}: {}",
                &cap[1], &cap[2], &cap[3], &cap[4], &cap[5], &cap[6]
            )
        })
        .collect();

    if errors.is_empty() && exit_code == 0 {
        return Some(rebuild_json(original, "✅ tsc: no errors", ""));
    }

    let mut compressed = format!("❌ tsc: {} errors\n\n", errors.len());
    for error in &errors {
        compressed.push_str(error);
        compressed.push('\n');
    }

    // Append summary if present
    for line in combined.lines() {
        if TSC_SUMMARY_RE.is_match(line) {
            compressed.push('\n');
            compressed.push_str(line);
            compressed.push('\n');
        }
    }

    let (capped, _) = filters::cap_lines(&compressed, MAX_COMPRESSED_LINES);
    Some(rebuild_json(original, &capped, ""))
}

// ── ESLint ──────────────────────────────────────────────────────────────────

fn try_eslint_compress(
    stdout: &str,
    stderr: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    let combined = format!("{}\n{}", stdout, stderr);

    // Detect eslint output
    if !ESLINT_PROBLEM_RE.is_match(&combined) && !ESLINT_SUMMARY_RE.is_match(&combined) {
        return None;
    }

    if exit_code == 0 && !ESLINT_PROBLEM_RE.is_match(&combined) {
        return Some(rebuild_json(original, "✅ eslint: no problems", ""));
    }

    // Keep file headers + problem lines + summary
    let mut result = String::new();
    let mut current_file = String::new();

    for line in combined.lines() {
        // File header (non-indented, contains a path)
        if !line.starts_with(' ') && !line.starts_with('\t') && line.contains('/') {
            current_file = line.to_string();
            result.push('\n');
            result.push_str(line);
            result.push('\n');
            continue;
        }

        // Problem lines
        if ESLINT_PROBLEM_RE.is_match(line) {
            if !current_file.is_empty() {
                // File header already emitted above
            }
            result.push_str(line);
            result.push('\n');
            continue;
        }

        // Summary line
        if ESLINT_SUMMARY_RE.is_match(line) {
            result.push('\n');
            result.push_str(line);
            result.push('\n');
        }
    }

    let (capped, _) = filters::cap_lines(&result, MAX_COMPRESSED_LINES);
    Some(rebuild_json(original, &capped, ""))
}

// ── Maven (mvn compile / package / install / verify) ────────────────────────

fn try_maven_compress(
    stdout: &str,
    stderr: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    let combined = format!("{}\n{}", stdout, stderr);

    // Detect Maven output
    if !MVN_BUILD_RESULT_RE.is_match(&combined)
        && !MVN_DOWNLOAD_RE.is_match(&combined)
        && !MVN_MODULE_RE.is_match(&combined)
    {
        return None;
    }

    // Count download noise lines
    let download_count = MVN_DOWNLOAD_RE.captures_iter(&combined).count();

    // Extract build result
    let build_result = MVN_BUILD_RESULT_RE
        .captures(&combined)
        .map(|c| c[1].to_string())
        .unwrap_or_else(|| "UNKNOWN".to_string());

    // Extract timing
    let timing = MVN_TIME_RE
        .captures(&combined)
        .map(|c| c[1].trim().to_string())
        .unwrap_or_default();

    // Extract error lines
    let errors: Vec<String> = MVN_ERROR_RE
        .captures_iter(&combined)
        .map(|c| c[1].to_string())
        .collect();

    // Extract warning lines
    let warnings: Vec<String> = MVN_WARNING_RE
        .captures_iter(&combined)
        .map(|c| c[1].to_string())
        .collect();

    // Extract test summaries (from surefire/failsafe)
    let test_summaries: Vec<MavenTestSummary> = MVN_TEST_SUMMARY_RE
        .captures_iter(&combined)
        .filter_map(|c| {
            Some(MavenTestSummary {
                run: c.get(1)?.as_str().parse().ok()?,
                failures: c.get(2)?.as_str().parse().ok()?,
                errors: c.get(3)?.as_str().parse().ok()?,
                skipped: c.get(4)?.as_str().parse().ok()?,
            })
        })
        .collect();

    // Build compressed output
    let mut result = String::new();

    // BUILD SUCCESS / FAILURE header
    if build_result == "SUCCESS" && exit_code == 0 {
        result.push_str("✅ Maven BUILD SUCCESS");
    } else {
        result.push_str("❌ Maven BUILD FAILURE");
    }
    if !timing.is_empty() {
        result.push_str(&format!(" ({})", timing));
    }
    result.push('\n');

    // Download noise summary
    if download_count > 0 {
        result.push_str(&format!(
            "[{} dependency download lines stripped]\n",
            download_count
        ));
    }

    // Test summary (aggregate)
    if !test_summaries.is_empty() {
        let total_run: u32 = test_summaries.iter().map(|s| s.run).sum();
        let total_fail: u32 = test_summaries.iter().map(|s| s.failures).sum();
        let total_err: u32 = test_summaries.iter().map(|s| s.errors).sum();
        let total_skip: u32 = test_summaries.iter().map(|s| s.skipped).sum();

        result.push_str(&format!(
            "Tests: {} run, {} failures, {} errors, {} skipped\n",
            total_run, total_fail, total_err, total_skip
        ));
    }

    // Compilation info
    let compile_lines: Vec<String> = MVN_COMPILE_RE
        .captures_iter(&combined)
        .map(|c| c[1].to_string())
        .collect();
    if !compile_lines.is_empty() {
        for cl in &compile_lines {
            result.push_str(&format!("[INFO] {}\n", cl));
        }
    }

    // Warnings (capped)
    if !warnings.is_empty() {
        let show = warnings.len().min(10);
        result.push_str(&format!("\n⚠️ {} warnings", warnings.len()));
        if warnings.len() > show {
            result.push_str(&format!(" (showing first {})", show));
        }
        result.push('\n');
        for w in warnings.iter().take(show) {
            result.push_str(&format!("  [WARNING] {}\n", w));
        }
    }

    // Errors (always keep all)
    if !errors.is_empty() {
        result.push_str(&format!("\n❌ {} errors:\n", errors.len()));
        for e in &errors {
            result.push_str(&format!("  [ERROR] {}\n", e));
        }
    }

    let (capped, _) = filters::cap_lines(&result, MAX_COMPRESSED_LINES);
    Some(rebuild_json(original, &capped, ""))
}

struct MavenTestSummary {
    run: u32,
    failures: u32,
    errors: u32,
    skipped: u32,
}

// ── Gradle Build ────────────────────────────────────────────────────────────

fn try_gradle_compress(
    stdout: &str,
    stderr: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    let combined = format!("{}\n{}", stdout, stderr);

    // Detect Gradle output
    if !GRADLE_RESULT_RE.is_match(&combined) && !GRADLE_TASK_RE.is_match(&combined) {
        return None;
    }

    // Count task and download lines
    let task_count = GRADLE_TASK_RE.captures_iter(&combined).count();
    let download_count = GRADLE_DOWNLOAD_RE.captures_iter(&combined).count();

    // Extract build result
    let (build_result, timing) = GRADLE_RESULT_RE
        .captures(&combined)
        .map(|c| {
            (
                c.get(1).map(|m| m.as_str().to_string()).unwrap_or_default(),
                c.get(2).map(|m| m.as_str().to_string()).unwrap_or_default(),
            )
        })
        .unwrap_or_default();

    // Extract actionable tasks summary
    let actionable = GRADLE_ACTIONABLE_RE.captures(&combined).map(|c| {
        format!(
            "{} actionable tasks: {}",
            c.get(1).map(|m| m.as_str()).unwrap_or("?"),
            c.get(2).map(|m| m.as_str()).unwrap_or("?"),
        )
    });

    // Extract error lines
    let errors: Vec<String> = GRADLE_ERROR_RE
        .captures_iter(&combined)
        .map(|c| c[0].to_string())
        .collect();

    // Build compressed output
    let mut result = String::new();

    if build_result == "SUCCESSFUL" && exit_code == 0 {
        result.push_str("✅ Gradle BUILD SUCCESSFUL");
    } else if build_result == "FAILED" || exit_code != 0 {
        result.push_str("❌ Gradle BUILD FAILED");
    } else {
        result.push_str("Gradle build");
    }
    if !timing.is_empty() {
        result.push_str(&format!(" ({})", timing));
    }
    result.push('\n');

    if task_count > 0 {
        result.push_str(&format!("[{} tasks executed]\n", task_count));
    }
    if download_count > 0 {
        result.push_str(&format!(
            "[{} dependency download lines stripped]\n",
            download_count
        ));
    }
    if let Some(actionable_str) = actionable {
        result.push_str(&format!("{}\n", actionable_str));
    }

    // Errors (always keep)
    if !errors.is_empty() {
        result.push_str(&format!("\n❌ {} error lines:\n", errors.len()));
        for (i, e) in errors.iter().enumerate() {
            if i >= 20 {
                result.push_str(&format!("  ... and {} more\n", errors.len() - 20));
                break;
            }
            result.push_str(&format!("  {}\n", e));
        }
    }

    let (capped, _) = filters::cap_lines(&result, MAX_COMPRESSED_LINES);
    Some(rebuild_json(original, &capped, ""))
}

// ── Docker Build ────────────────────────────────────────────────────────────

fn try_docker_compress(
    stdout: &str,
    stderr: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    let combined = format!("{}\n{}", stdout, stderr);

    // Detect Docker build output
    if !DOCKER_STEP_RE.is_match(&combined) && !DOCKER_SUCCESS_RE.is_match(&combined) {
        return None;
    }

    // Extract steps: collect (step_num, total, instruction)
    let steps: Vec<(String, String)> = DOCKER_STEP_RE
        .captures_iter(&combined)
        .map(|c| {
            let step = c
                .get(1)
                .or_else(|| c.get(3))
                .map(|m| m.as_str().to_string())
                .unwrap_or_default();
            let total = c
                .get(2)
                .or_else(|| c.get(4))
                .map(|m| m.as_str().to_string())
                .unwrap_or_default();
            (step, total)
        })
        .collect();

    let total_steps = steps.first().map(|(_, t)| t.as_str()).unwrap_or("?");

    // Count noise lines
    let cache_count = DOCKER_CACHE_RE.captures_iter(&combined).count();
    let hash_count = DOCKER_HASH_RE.captures_iter(&combined).count();
    let progress_count = DOCKER_PROGRESS_RE.captures_iter(&combined).count();
    let noise_count = cache_count + hash_count + progress_count;

    // Check success
    let is_success = DOCKER_SUCCESS_RE.is_match(&combined);

    // Build compressed output
    let mut result = String::new();

    if is_success && exit_code == 0 {
        result.push_str(&format!(
            "✅ Docker build: {} steps completed\n",
            total_steps
        ));
    } else {
        result.push_str(&format!(
            "❌ Docker build failed at step {}/{}\n",
            steps.last().map(|(s, _)| s.as_str()).unwrap_or("?"),
            total_steps,
        ));
    }

    if noise_count > 0 {
        result.push_str(&format!(
            "[{} cache/hash/progress lines stripped]\n",
            noise_count
        ));
    }

    // Keep Step headlines (compact: one line per step)
    result.push_str("\nSteps:\n");
    for line in combined.lines() {
        if let Some(cap) = DOCKER_STEP_RE.captures(line) {
            let instr = cap.get(5).map(|m| m.as_str()).unwrap_or("");
            let step_n = cap
                .get(1)
                .or_else(|| cap.get(3))
                .map(|m| m.as_str())
                .unwrap_or("?");
            result.push_str(&format!("  {}/{} {}\n", step_n, total_steps, instr));
        }
    }

    // On failure: keep error lines from stderr or lines after last step
    if !is_success || exit_code != 0 {
        let mut error_lines: Vec<&str> = Vec::new();
        let mut past_last_step = false;
        for line in combined.lines() {
            if DOCKER_STEP_RE.is_match(line) {
                past_last_step = true;
                error_lines.clear();
                continue;
            }
            if past_last_step
                && !DOCKER_CACHE_RE.is_match(line)
                && !DOCKER_HASH_RE.is_match(line)
                && !DOCKER_PROGRESS_RE.is_match(line)
                && !line.trim().is_empty()
            {
                error_lines.push(line);
            }
        }
        if !error_lines.is_empty() {
            result.push_str("\nError output:\n");
            for (i, e) in error_lines.iter().enumerate() {
                if i >= 30 {
                    result.push_str(&format!(
                        "  ... and {} more lines\n",
                        error_lines.len() - 30
                    ));
                    break;
                }
                result.push_str(&format!("  {}\n", e));
            }
        }
    }

    // Extract "Successfully tagged" info
    for line in combined.lines() {
        if DOCKER_SUCCESS_RE.is_match(line) {
            result.push_str(&format!("\n{}\n", line.trim()));
        }
    }

    let (capped, _) = filters::cap_lines(&result, MAX_COMPRESSED_LINES);
    Some(rebuild_json(original, &capped, ""))
}

// ── .NET (dotnet build) ─────────────────────────────────────────────────────

fn try_dotnet_compress(
    stdout: &str,
    stderr: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    let combined = format!("{}\n{}", stdout, stderr);

    // Detect dotnet output
    if !DOTNET_RESULT_RE.is_match(&combined) {
        return None;
    }

    let build_result = DOTNET_RESULT_RE
        .captures(&combined)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
        .unwrap_or_default();

    // Count restore/noise lines
    let restore_count = DOTNET_RESTORE_RE.captures_iter(&combined).count();
    let determining_count = DOTNET_DETERMINING_RE.captures_iter(&combined).count();

    // Extract warning/error counts
    let mut warnings: u32 = 0;
    let mut errors: u32 = 0;
    for cap in DOTNET_COUNTS_RE.captures_iter(&combined) {
        let count: u32 = cap
            .get(1)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);
        match cap.get(2).map(|m| m.as_str()) {
            Some("Warning") => warnings += count,
            Some("Error") => errors += count,
            _ => {}
        }
    }

    let mut result = String::new();

    if build_result == "succeeded" && exit_code == 0 {
        result.push_str("✅ dotnet build succeeded");
    } else {
        result.push_str("❌ dotnet build FAILED");
    }
    if warnings > 0 {
        result.push_str(&format!(" ({} warnings)", warnings));
    }
    if errors > 0 {
        result.push_str(&format!(" ({} errors)", errors));
    }
    result.push('\n');

    if restore_count + determining_count > 0 {
        result.push_str(&format!(
            "[{} NuGet restore lines stripped]\n",
            restore_count + determining_count
        ));
    }

    // Keep error/warning lines
    for line in combined.lines() {
        let trimmed = line.trim();
        if trimmed.contains(": error ") || trimmed.contains(": warning ") {
            result.push_str(line);
            result.push('\n');
        }
    }

    let (capped, _) = filters::cap_lines(&result, MAX_COMPRESSED_LINES);
    Some(rebuild_json(original, &capped, ""))
}

// ── Terraform ───────────────────────────────────────────────────────────────

fn try_terraform_compress(
    stdout: &str,
    stderr: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    let combined = format!("{}\n{}", stdout, stderr);

    // Detect Terraform output by summary or actions header
    let has_plan = TF_PLAN_SUMMARY_RE.is_match(&combined);
    let has_apply = TF_APPLY_SUMMARY_RE.is_match(&combined);
    let has_actions = TF_ACTIONS_HEADER_RE.is_match(&combined);

    if !has_plan && !has_apply && !has_actions {
        return None;
    }

    let mut result = String::new();

    // Extract plan summary
    if let Some(cap) = TF_PLAN_SUMMARY_RE.captures(&combined) {
        let add: u32 = cap
            .get(1)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);
        let change: u32 = cap
            .get(2)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);
        let destroy: u32 = cap
            .get(3)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);

        if add == 0 && change == 0 && destroy == 0 {
            result.push_str("✅ Terraform plan: no changes\n");
        } else {
            result.push_str(&format!(
                "📋 Terraform plan: +{} add, ~{} change, -{} destroy\n",
                add, change, destroy
            ));
        }
    }

    // Extract apply summary
    if let Some(cap) = TF_APPLY_SUMMARY_RE.captures(&combined) {
        let add: u32 = cap
            .get(1)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);
        let change: u32 = cap
            .get(2)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);
        let destroy: u32 = cap
            .get(3)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);
        result.push_str(&format!(
            "✅ Terraform apply: {} added, {} changed, {} destroyed\n",
            add, change, destroy
        ));
    }

    // Keep resource action lines (compact)
    let resources: Vec<String> = TF_RESOURCE_RE
        .captures_iter(&combined)
        .map(|c| {
            format!(
                "  {} will be {}",
                c.get(1).map(|m| m.as_str()).unwrap_or("?"),
                c.get(2).map(|m| m.as_str()).unwrap_or("?"),
            )
        })
        .collect();

    if !resources.is_empty() {
        result.push_str(&format!("\nResources ({}):\n", resources.len()));
        for r in &resources {
            result.push_str(r);
            result.push('\n');
        }
    }

    // Count stripped detail lines
    let detail_count = TF_DETAIL_RE.captures_iter(&combined).count();
    if detail_count > 0 {
        result.push_str(&format!(
            "\n[{} resource detail lines stripped]\n",
            detail_count
        ));
    }

    // On failure, keep error lines
    if exit_code != 0 {
        result.push('\n');
        for line in combined.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("Error:")
                || trimmed.starts_with("│ Error:")
                || trimmed.starts_with("╷")
                || trimmed.starts_with("│")
                || trimmed.starts_with("╵")
            {
                result.push_str(line);
                result.push('\n');
            }
        }
    }

    let (capped, _) = filters::cap_lines(&result, MAX_COMPRESSED_LINES);
    Some(rebuild_json(original, &capped, ""))
}

// ── JSON Rebuild Helper ─────────────────────────────────────────────────────

fn rebuild_json(
    original: &serde_json::Value,
    new_stdout: &str,
    new_stderr: &str,
) -> CompressionResult {
    let mut obj = original.clone();
    if let Some(map) = obj.as_object_mut() {
        map.insert(
            "stdout".to_string(),
            serde_json::Value::String(new_stdout.to_string()),
        );
        map.insert(
            "stderr".to_string(),
            serde_json::Value::String(new_stderr.to_string()),
        );
    }

    let compressed = serde_json::to_string(&obj).unwrap_or_else(|_| new_stdout.to_string());

    CompressionResult {
        compressed,
        was_compressed: true,
    }
}

/// Fallback for non-JSON build output.
fn compress_plain_build_text(text: &str) -> CompressionResult {
    let clean = filters::strip_ansi(text);
    let collapsed = filters::collapse_blank_lines(&clean);
    let (capped, was_capped) = filters::cap_lines(&collapsed, MAX_COMPRESSED_LINES);
    let (final_text, was_byte_capped) = filters::cap_bytes(&capped, filters::DEFAULT_MAX_BYTES);

    let any_change = was_capped || was_byte_capped || final_text.len() != text.len();

    CompressionResult {
        compressed: final_text,
        was_compressed: any_change,
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bash_json(stdout: &str, stderr: &str, exit_code: i64) -> String {
        make_bash_json_cmd("cargo check", stdout, stderr, exit_code)
    }

    fn make_bash_json_cmd(cmd: &str, stdout: &str, stderr: &str, exit_code: i64) -> String {
        serde_json::json!({
            "command": cmd,
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": exit_code,
            "timed_out": false,
            "stdout_truncated": false,
            "stderr_truncated": false,
        })
        .to_string()
    }

    fn pad(s: &str) -> String {
        format!("{}{}", s, "x".repeat(MIN_COMPRESS_LEN))
    }

    // ── Rust build ──

    #[test]
    fn rust_clean_build() {
        let stderr = &pad("   Compiling foo v0.1.0\n   Compiling bar v0.2.0\n    Checking baz v0.3.0\n    Finished dev [unoptimized + debuginfo] target(s) in 2.34s\n");
        let input = make_bash_json("", stderr, 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ Build clean"));
    }

    #[test]
    fn rust_build_with_warnings() {
        let stderr = &pad("\
   Compiling foo v0.1.0
warning: unused variable `x`
 --> src/main.rs:10:9
  |
10 |     let x = 1;
  |         ^
  = help: consider prefixing with an underscore

    Finished dev [unoptimized + debuginfo] target(s)
");
        let input = make_bash_json("", stderr, 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("⚠️"));
        assert!(result.compressed.contains("warning"));
    }

    #[test]
    fn rust_build_errors() {
        let stderr = &pad("\
   Compiling foo v0.1.0
error[E0308]: mismatched types
 --> src/main.rs:5:12
  |
5 |     let x: u32 = \"hello\";
  |            ---   ^^^^^^^ expected `u32`, found `&str`

error: could not compile `foo` (bin \"foo\") due to 1 previous error
");
        let input = make_bash_json("", stderr, 1);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("❌"));
        assert!(result.compressed.contains("error"));
        assert!(result.compressed.contains("E0308"));
    }

    // ── tsc ──

    #[test]
    fn tsc_no_errors() {
        // tsc with no errors typically has no output and exit_code 0
        // But if there's enough volume to trigger compression:
        let stdout = &pad("");
        let input = make_bash_json(stdout, "", 0);
        let result = compress(&input);
        // Below threshold or no tsc patterns → falls through to generic
        // This is expected behavior
        assert!(!result.compressed.is_empty());
    }

    #[test]
    fn tsc_errors() {
        let stdout = &pad("\
src/index.ts(10,5): error TS2304: Cannot find name 'foo'.
src/index.ts(15,8): error TS2345: Argument of type 'string' is not assignable to parameter of type 'number'.
src/utils.ts(3,1): error TS1005: ';' expected.

Found 3 errors in 2 files.
");
        let input = make_bash_json(stdout, "", 1);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("❌ tsc: 3 errors"));
        assert!(result.compressed.contains("TS2304"));
        assert!(result.compressed.contains("TS2345"));
    }

    // ── ESLint ──

    #[test]
    fn eslint_problems() {
        let stdout = &pad("\
/src/App.tsx
  10:5   error  Unexpected var, use let or const instead  no-var
  15:1   warning  Missing semicolon                        semi

/src/utils.ts
  3:10   error  'foo' is not defined                      no-undef

✖ 3 problems (2 errors, 1 warning)
");
        let input = make_bash_json(stdout, "", 1);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("error"));
        assert!(result.compressed.contains("no-var") || result.compressed.contains("problems"));
    }

    // ── Fallback ──

    #[test]
    fn short_output_not_compressed() {
        let input = make_bash_json("ok", "", 0);
        let result = compress(&input);
        assert!(!result.was_compressed);
    }

    #[test]
    fn plain_text_fallback() {
        let big = "error line\n".repeat(300);
        let result = compress(&big);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("lines omitted"));
    }

    // ── Maven ──

    #[test]
    fn maven_build_success() {
        let stdout = &pad("\
[INFO] Scanning for projects...
[INFO] 
[INFO] ----------------------< com.example:myapp >-----------------------
[INFO] Building myapp 1.0.0-SNAPSHOT
[INFO] --------------------------------[ jar ]---------------------------------
[INFO] Downloading from central: https://repo.maven.apache.org/maven2/org/apache/maven/plugins/maven-resources-plugin/3.3.1/maven-resources-plugin-3.3.1.pom
[INFO] Downloaded from central: https://repo.maven.apache.org/maven2/org/apache/maven/plugins/maven-resources-plugin/3.3.1/maven-resources-plugin-3.3.1.pom (8.2 kB at 45 kB/s)
[INFO] Downloading from central: https://repo.maven.apache.org/maven2/org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar
[INFO] Downloaded from central: https://repo.maven.apache.org/maven2/org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar (587 kB at 1.2 MB/s)
[INFO] Downloading from central: https://repo.maven.apache.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.0/jackson-databind-2.15.0.jar
[INFO] Downloaded from central: https://repo.maven.apache.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.0/jackson-databind-2.15.0.jar (1.6 MB at 3.2 MB/s)
[INFO] 
[INFO] --- maven-resources-plugin:3.3.1:resources (default-resources) @ myapp ---
[INFO] --- maven-compiler-plugin:3.11.0:compile (default-compile) @ myapp ---
[INFO] Compiling 42 source files to /target/classes
[INFO] 
[INFO] --- maven-surefire-plugin:3.1.2:test (default-test) @ myapp ---
[INFO] Tests run: 15, Failures: 0, Errors: 0, Skipped: 2
[INFO] 
[INFO] --- maven-jar-plugin:3.3.0:jar (default-jar) @ myapp ---
[INFO] Building jar: /target/myapp-1.0.0-SNAPSHOT.jar
[INFO] 
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  12.345 s
[INFO] Finished at: 2024-01-15T10:30:00Z
[INFO] ------------------------------------------------------------------------
");
        let input = make_bash_json_cmd("mvn clean install", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed, "Maven output should be compressed");
        assert!(result.compressed.contains("✅ Maven BUILD SUCCESS"));
        assert!(result.compressed.contains("12.345 s"));
        // Download noise should be summarized, not shown line-by-line
        assert!(result.compressed.contains("download lines stripped"));
        assert!(!result.compressed.contains("repo.maven.apache.org"));
        // Test summary should be present
        assert!(result.compressed.contains("15 run"));
        // Compilation info
        assert!(result.compressed.contains("42 source files"));
    }

    #[test]
    fn maven_build_failure_with_errors() {
        let stdout = &pad("\
[INFO] Scanning for projects...
[INFO] Building myapp 1.0.0-SNAPSHOT
[INFO] --- maven-compiler-plugin:3.11.0:compile (default-compile) @ myapp ---
[INFO] Compiling 10 source files to /target/classes
[ERROR] /src/main/java/com/example/App.java:[15,20] error: cannot find symbol
[ERROR]   symbol:   variable foo
[ERROR]   location: class com.example.App
[ERROR] /src/main/java/com/example/Service.java:[42,10] error: incompatible types
[WARNING] Some deprecated API usage in module X
[WARNING] Another warning about something
[INFO] ------------------------------------------------------------------------
[INFO] BUILD FAILURE
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  3.456 s
");
        let input = make_bash_json_cmd("mvn compile", stdout, "", 1);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("❌ Maven BUILD FAILURE"));
        assert!(result.compressed.contains("3.456 s"));
        // Errors should be kept
        assert!(result.compressed.contains("cannot find symbol"));
        assert!(result.compressed.contains("incompatible types"));
        // Warnings summary
        assert!(result.compressed.contains("warning"));
    }

    #[test]
    fn maven_heavy_download_noise_stripped() {
        // Simulate 200+ download lines (common for fresh builds)
        let mut stdout = String::new();
        stdout.push_str("[INFO] Scanning for projects...\n");
        stdout.push_str("[INFO] Building myapp 1.0.0\n");
        for i in 0..100 {
            stdout.push_str(&format!(
                "[INFO] Downloading from central: https://repo.maven.apache.org/artifact-{}.jar\n",
                i
            ));
            stdout.push_str(&format!(
                "[INFO] Downloaded from central: https://repo.maven.apache.org/artifact-{}.jar (100 kB)\n",
                i
            ));
        }
        stdout.push_str("[INFO] --- maven-compiler-plugin:3.11.0:compile ---\n");
        stdout.push_str("[INFO] Compiling 5 source files\n");
        stdout.push_str("[INFO] BUILD SUCCESS\n");
        stdout.push_str("[INFO] Total time:  45.678 s\n");

        let input = make_bash_json_cmd("mvn clean install", &stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ Maven BUILD SUCCESS"));
        assert!(result
            .compressed
            .contains("200 dependency download lines stripped"));
        // Original was ~200 lines of downloads, compressed should be much shorter
        assert!(result.compressed.len() < input.len() / 2);
    }

    #[test]
    fn maven_test_with_surefire_summary() {
        let stdout = &pad("\
[INFO] --- maven-surefire-plugin:3.1.2:test (default-test) @ module-a ---
Tests run: 20, Failures: 0, Errors: 0, Skipped: 1
[INFO] --- maven-surefire-plugin:3.1.2:test (default-test) @ module-b ---
Tests run: 35, Failures: 2, Errors: 1, Skipped: 0
[INFO] BUILD FAILURE
[INFO] Total time:  8.123 s
[ERROR] There are test failures.
");
        let input = make_bash_json_cmd("mvn test", stdout, "", 1);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("❌ Maven BUILD FAILURE"));
        // Aggregate test counts: 20+35=55 run, 0+2=2 failures, 0+1=1 errors, 1+0=1 skipped
        assert!(result.compressed.contains("55 run"));
        assert!(result.compressed.contains("2 failures"));
        assert!(result.compressed.contains("1 errors"));
    }

    // ── Gradle ──

    #[test]
    fn gradle_build_success() {
        let stdout = &pad("\
> Task :compileJava
> Task :processResources
> Task :classes
> Task :jar
> Task :assemble
> Task :compileTestJava
> Task :testClasses
> Task :test
> Task :check
> Task :build

BUILD SUCCESSFUL in 12s
8 actionable tasks: 8 executed
");
        let input = make_bash_json_cmd("gradle build", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ Gradle BUILD SUCCESSFUL"));
        assert!(result.compressed.contains("12s"));
        assert!(result.compressed.contains("8 actionable tasks"));
    }

    #[test]
    fn gradle_build_failure() {
        let stdout = &pad("\
> Task :compileJava
> Task :compileKotlin FAILED

FAILURE: Build failed with an exception.

> Could not resolve all files for configuration ':compileClasspath'.
> Could not find com.example:missing-lib:1.0.0.

BUILD FAILED in 5s
2 actionable tasks: 1 executed, 1 failed
");
        let input = make_bash_json_cmd("./gradlew build", stdout, "", 1);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("❌ Gradle BUILD FAILED"));
        // Errors should be kept
        assert!(result.compressed.contains("Could not"));
    }

    #[test]
    fn gradle_download_noise() {
        let mut stdout = String::new();
        for i in 0..50 {
            stdout.push_str(&format!(
                "> https://repo.maven.apache.org/maven2/artifact-{}.jar\n",
                i
            ));
        }
        stdout.push_str("> Task :compileJava\n");
        stdout.push_str("> Task :build\n");
        stdout.push_str("BUILD SUCCESSFUL in 20s\n");
        stdout.push_str("2 actionable tasks: 2 executed\n");

        let input = make_bash_json_cmd("gradle build", &stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ Gradle BUILD SUCCESSFUL"));
        assert!(result
            .compressed
            .contains("50 dependency download lines stripped"));
    }

    // ── Docker ──

    #[test]
    fn docker_build_success() {
        let stdout = &pad("\
Sending build context to Docker daemon  15.36kB
Step 1/8 : FROM node:18-alpine
---> abc123def456
Step 2/8 : WORKDIR /app
---> Using cache
---> def456abc789
Step 3/8 : COPY package*.json ./
---> Using cache
---> 789abc123def
Step 4/8 : RUN npm install
---> Using cache
---> aaa111bbb222
Step 5/8 : COPY . .
---> abc111222333
Step 6/8 : RUN npm run build
---> ccc333ddd444
Step 7/8 : EXPOSE 3000
---> ddd444eee555
Step 8/8 : CMD [\"node\", \"dist/index.js\"]
---> eee555fff666
Successfully built eee555fff666
Successfully tagged myapp:latest
");
        let input = make_bash_json_cmd("docker build -t myapp:latest .", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result
            .compressed
            .contains("✅ Docker build: 8 steps completed"));
        // Hash/cache noise should be stripped
        assert!(result
            .compressed
            .contains("cache/hash/progress lines stripped"));
        assert!(!result.compressed.contains("abc123def456"));
        // Should keep step summaries
        assert!(result.compressed.contains("npm install"));
        // Should keep success tag
        assert!(result
            .compressed
            .contains("Successfully tagged myapp:latest"));
    }

    #[test]
    fn docker_build_failure() {
        let stdout = &pad("\
Sending build context to Docker daemon  15.36kB
Step 1/5 : FROM node:18-alpine
---> abc123def456
Step 2/5 : WORKDIR /app
---> def456abc789
Step 3/5 : RUN apt-get install -y missing-package
The command '/bin/sh -c apt-get install -y missing-package' returned a non-zero code: 100
");
        let input = make_bash_json_cmd("docker build .", stdout, "", 1);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("❌ Docker build failed"));
        // Error details should be kept
        assert!(result.compressed.contains("non-zero code"));
    }

    // ── dotnet ──

    #[test]
    fn dotnet_build_success() {
        let stdout = &pad("\
Microsoft (R) Build Engine version 17.0.0+c9eb9dd64 for .NET
  Determining projects to restore...
  Restored /src/MyApp/MyApp.csproj (in 1.23 s).
  Restored /src/MyLib/MyLib.csproj (in 0.45 s).
  MyLib -> /src/MyLib/bin/Debug/net8.0/MyLib.dll
  MyApp -> /src/MyApp/bin/Debug/net8.0/MyApp.dll

Build succeeded.
    0 Warning(s)
    0 Error(s)

Time Elapsed 00:00:03.45
");
        let input = make_bash_json_cmd("dotnet build", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ dotnet build succeeded"));
        assert!(result.compressed.contains("NuGet restore lines stripped"));
    }

    #[test]
    fn dotnet_build_failure() {
        let stdout = &pad("\
Microsoft (R) Build Engine version 17.0.0+c9eb9dd64 for .NET
  Determining projects to restore...
  Restored /src/MyApp/MyApp.csproj (in 0.5 s).
  /src/MyApp/Program.cs(10,20): error CS0103: The name 'foo' does not exist in the current context
  /src/MyApp/Program.cs(15,5): warning CS0168: The variable 'x' is declared but never used

Build FAILED.
    1 Warning(s)
    1 Error(s)
");
        let input = make_bash_json_cmd("dotnet build", stdout, "", 1);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("❌ dotnet build FAILED"));
        assert!(result.compressed.contains("1 warnings"));
        assert!(result.compressed.contains("1 errors"));
        // Should keep the error line
        assert!(result.compressed.contains("error CS0103"));
    }

    // ── Terraform ──

    #[test]
    fn terraform_plan_changes() {
        let stdout = &pad("\
Terraform used the selected providers to generate the following execution plan.

Terraform will perform the following actions:

  # aws_instance.web will be created
  + resource \"aws_instance\" \"web\" {
      + ami           = \"ami-12345678\"
      + instance_type = \"t2.micro\"
      + tags          = {
          + \"Name\" = \"web\"
        }
    }

  # aws_s3_bucket.data will be created
  + resource \"aws_s3_bucket\" \"data\" {
      + bucket = \"my-data-bucket\"
    }

  # aws_security_group.web will be updated in-place
  ~ resource \"aws_security_group\" \"web\" {
      ~ ingress = [
          + { from_port = 443 }
        ]
    }

Plan: 2 to add, 1 to change, 0 to destroy.
");
        let input = make_bash_json_cmd("terraform plan", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result
            .compressed
            .contains("Terraform plan: +2 add, ~1 change, -0 destroy"));
        // Should list resources
        assert!(result
            .compressed
            .contains("aws_instance.web will be created"));
        assert!(result
            .compressed
            .contains("aws_s3_bucket.data will be created"));
        // Should strip detail lines
        assert!(result.compressed.contains("resource detail lines stripped"));
        // Should NOT contain full resource blocks
        assert!(!result.compressed.contains("ami-12345678"));
    }

    #[test]
    fn terraform_apply_success() {
        let stdout = &pad("\
aws_instance.web: Creating...
aws_instance.web: Still creating... [10s elapsed]
aws_instance.web: Creation complete after 15s [id=i-1234567890abcdef0]
aws_s3_bucket.data: Creating...
aws_s3_bucket.data: Creation complete after 2s [id=my-data-bucket]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
");
        let input = make_bash_json_cmd("terraform apply -auto-approve", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result
            .compressed
            .contains("✅ Terraform apply: 2 added, 0 changed, 0 destroyed"));
    }

    #[test]
    fn terraform_plan_no_changes() {
        let stdout = &pad("\
No changes. Your infrastructure matches the configuration.

Plan: 0 to add, 0 to change, 0 to destroy.
");
        let input = make_bash_json_cmd("terraform plan", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ Terraform plan: no changes"));
    }
}
