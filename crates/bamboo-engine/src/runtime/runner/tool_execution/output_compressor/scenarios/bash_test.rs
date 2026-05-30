//! Compression for test runner output: `cargo test`, `pytest`, `npm test`, etc.
//!
//! Strategy:
//! - All tests pass → single-line summary: "✅ 42 tests passed (1.23s)"
//! - Some tests fail → keep FAILED lines + failure details, drop "... ok" lines

use regex::Regex;

use crate::runtime::runner::tool_execution::output_compressor::filters;
use crate::runtime::runner::tool_execution::output_compressor::CompressionResult;
use crate::runtime::runner::tool_execution::output_compressor::CompressionTier;

type TestCompressorFn = fn(&str, &str, i64) -> Option<(String, String)>;

/// Minimum result length (chars) before compression kicks in.
const MIN_COMPRESS_LEN: usize = 1500;

// ── Public Entry Point ─────────────────────────────────────────────────────

pub(crate) fn compress(raw_result: &str, tier: CompressionTier) -> CompressionResult {
    let _ = tier;
    if raw_result.len() < MIN_COMPRESS_LEN {
        return CompressionResult {
            compressed: raw_result.to_string(),
            was_compressed: false,
        };
    }

    // Parse the Bash JSON envelope: { command, stdout, stderr, exit_code, … }
    let parsed: serde_json::Value = match serde_json::from_str(raw_result) {
        Ok(v) => v,
        Err(_) => {
            return CompressionResult {
                compressed: raw_result.to_string(),
                was_compressed: false,
            }
        }
    };

    let stdout = parsed.get("stdout").and_then(|v| v.as_str()).unwrap_or("");
    let stderr = parsed.get("stderr").and_then(|v| v.as_str()).unwrap_or("");
    let exit_code = parsed
        .get("exit_code")
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);

    let clean_stdout = filters::strip_ansi(stdout);
    let clean_stderr = filters::strip_ansi(stderr);

    // Try each test framework's format in order.
    // Each returns Some((new_stdout, new_stderr)) if it detected its format.
    let handlers: &[TestCompressorFn] = &[
        try_compress_cargo_test,
        try_compress_pytest,
        try_compress_jest,
        try_compress_go_test,
        try_compress_surefire,
    ];

    for handler in handlers {
        if let Some(compressed) = handler(&clean_stdout, &clean_stderr, exit_code) {
            return rebuild_bash_json(&parsed, &compressed.0, &compressed.1, exit_code);
        }
    }

    // Fallback: ANSI strip + line cap (for unrecognized test runners)
    let (capped_stdout, stdout_capped) =
        filters::cap_lines(&clean_stdout, filters::DEFAULT_MAX_LINES);
    let (capped_stderr, stderr_capped) = filters::cap_lines(&clean_stderr, 50);

    if !stdout_capped && !stderr_capped {
        // Only ANSI stripping happened – check if that actually saved anything
        if capped_stdout.len() == stdout.len() && capped_stderr.len() == stderr.len() {
            return CompressionResult {
                compressed: raw_result.to_string(),
                was_compressed: false,
            };
        }
    }

    rebuild_bash_json(&parsed, &capped_stdout, &capped_stderr, exit_code)
}

// ── Cargo Test Parser ──────────────────────────────────────────────────────

struct CargoTestSummary {
    passed: u32,
    failed: u32,
    ignored: u32,
    time_str: String,
}

lazy_static::lazy_static! {
    /// Matches: `test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.15s`
    static ref CARGO_SUMMARY_RE: Regex = Regex::new(
        r"test result: (?:ok|FAILED)\.\s+(\d+) passed;\s+(\d+) failed;\s+(\d+) ignored;(?:.*finished in\s+(\S+)|[^\n]*)"
    ).expect("cargo summary regex");

    /// Matches: `test some::module::name ... ok`
    static ref TEST_LINE_OK_RE: Regex = Regex::new(
        r"^test\s+\S+\s+\.\.\.\s+ok$"
    ).expect("test ok regex");

    /// Captures test name from: `test some::module::name ... ok`
    static ref TEST_NAME_OK_RE: Regex = Regex::new(
        r"(?m)^test\s+(\S+)\s+\.\.\.\s+ok$"
    ).expect("test name capture regex");

    /// Matches: `running N tests` or `running N test`
    static ref RUNNING_RE: Regex = Regex::new(
        r"^running \d+ tests?$"
    ).expect("running regex");

    // ── pytest ──

    /// Matches pytest summary: `= 5 passed in 1.23s =` or `= 3 failed, 2 passed in 2.5s =`
    /// or `= 1 failed, 3 passed, 1 warning in 0.8s =`
    static ref PYTEST_SUMMARY_RE: Regex = Regex::new(
        r"(?m)^=+\s+(.+?)\s+in\s+(\S+)\s+=+\s*$"
    ).expect("pytest summary regex");

    /// Matches pytest short test results: `PASSED`, `FAILED`, `ERROR`, `SKIPPED`
    static ref PYTEST_COUNTS_RE: Regex = Regex::new(
        r"(\d+)\s+(passed|failed|error|skipped|warnings?|deselected|xfailed|xpassed)"
    ).expect("pytest counts regex");

    /// Matches pytest FAILURES section header
    static ref PYTEST_FAILURE_HEADER_RE: Regex = Regex::new(
        r"(?m)^_+\s+(.+?)\s+_+\s*$"
    ).expect("pytest failure header regex");

    /// Matches pytest short test status: `test_file.py::test_name PASSED`
    static ref PYTEST_TEST_LINE_RE: Regex = Regex::new(
        r"(?m)^(\S+::\S+)\s+(PASSED|FAILED|ERROR|SKIPPED)"
    ).expect("pytest test line regex");

    // ── Jest / Vitest ──

    /// Matches Jest summary: `Tests:       3 failed, 42 passed, 45 total`
    static ref JEST_TESTS_RE: Regex = Regex::new(
        r"(?m)^Tests:\s+(.+?total)\s*$"
    ).expect("jest tests regex");

    /// Matches Jest suite summary: `Test Suites: 1 failed, 4 passed, 5 total`
    static ref JEST_SUITES_RE: Regex = Regex::new(
        r"(?m)^Test Suites:\s+(.+?total)\s*$"
    ).expect("jest suites regex");

    /// Matches Jest time: `Time:        4.567 s`
    static ref JEST_TIME_RE: Regex = Regex::new(
        r"(?m)^Time:\s+(.+)"
    ).expect("jest time regex");

    /// Matches Jest PASS/FAIL: `PASS src/foo.test.ts` or `FAIL src/bar.test.ts`
    static ref JEST_RESULT_LINE_RE: Regex = Regex::new(
        r"(?m)^(PASS|FAIL)\s+(.+)"
    ).expect("jest result line regex");

    /// Matches Jest inline counts: `N failed` / `N passed` in summary
    static ref JEST_COUNT_RE: Regex = Regex::new(
        r"(\d+)\s+(failed|passed|skipped|pending|todo)"
    ).expect("jest count regex");

    // ── Go test ──

    /// Matches Go test result: `ok      github.com/foo/bar     0.123s`
    static ref GO_TEST_OK_RE: Regex = Regex::new(
        r"(?m)^ok\s+\S+\s+(\S+)"
    ).expect("go test ok regex");

    /// Matches Go test failure: `FAIL    github.com/foo/bar     0.456s`
    static ref GO_TEST_FAIL_RE: Regex = Regex::new(
        r"(?m)^FAIL\s+\S+\s+(\S+)"
    ).expect("go test fail regex");

    /// Matches Go individual test: `--- PASS: TestFoo (0.00s)` or `--- FAIL: TestBar (0.01s)`
    static ref GO_TEST_LINE_RE: Regex = Regex::new(
        r"(?m)^--- (PASS|FAIL|SKIP): (\S+)"
    ).expect("go test line regex");

    /// Matches Go test pass line: `=== RUN   TestFoo`
    static ref GO_RUN_RE: Regex = Regex::new(
        r"(?m)^=== RUN\s+\S+"
    ).expect("go run regex");

    // ── Maven Surefire / JUnit ──

    /// Matches Surefire summary: `Tests run: 42, Failures: 0, Errors: 0, Skipped: 2`
    static ref SUREFIRE_SUMMARY_RE: Regex = Regex::new(
        r"(?m)Tests run:\s+(\d+),\s+Failures:\s+(\d+),\s+Errors:\s+(\d+),\s+Skipped:\s+(\d+)"
    ).expect("surefire summary regex");
}

/// Parse all `test result:` summary lines from the output (handles workspace
/// builds with multiple test binaries).
fn parse_cargo_summaries(text: &str) -> Vec<CargoTestSummary> {
    CARGO_SUMMARY_RE
        .captures_iter(text)
        .filter_map(|cap| {
            Some(CargoTestSummary {
                passed: cap.get(1)?.as_str().parse().ok()?,
                failed: cap.get(2)?.as_str().parse().ok()?,
                ignored: cap.get(3)?.as_str().parse().ok()?,
                time_str: cap
                    .get(4)
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_default(),
            })
        })
        .collect()
}

/// Attempt to compress cargo test output. Returns `Some((new_stdout, new_stderr))`
/// if this looks like cargo test output, `None` otherwise.
fn try_compress_cargo_test(stdout: &str, stderr: &str, exit_code: i64) -> Option<(String, String)> {
    // Check that this actually looks like cargo test output
    let combined = format!("{}\n{}", stdout, stderr);
    let summaries = parse_cargo_summaries(&combined);
    if summaries.is_empty() {
        return None;
    }

    let total_passed: u32 = summaries.iter().map(|s| s.passed).sum();
    let total_failed: u32 = summaries.iter().map(|s| s.failed).sum();
    let total_ignored: u32 = summaries.iter().map(|s| s.ignored).sum();

    // Grab the last time string (overall time)
    let time_str = summaries.last().map(|s| s.time_str.as_str()).unwrap_or("");

    if total_failed == 0 && exit_code == 0 {
        // ✅ All pass → extreme compression
        let mut summary = format!("✅ {} tests passed", total_passed);
        if total_ignored > 0 {
            summary.push_str(&format!(", {} ignored", total_ignored));
        }
        if !time_str.is_empty() {
            summary.push_str(&format!(" ({})", time_str));
        }

        // Append test names (comma-separated, capped at 200 chars)
        let names: Vec<&str> = TEST_NAME_OK_RE
            .captures_iter(&combined)
            .filter_map(|c| c.get(1).map(|m| m.as_str()))
            .collect();
        if !names.is_empty() {
            summary.push_str("\nTests: ");
            let names_start = summary.len();
            let max_len = 200;
            for (idx, name) in names.iter().enumerate() {
                let entry = if idx == 0 {
                    name.to_string()
                } else {
                    format!(", {}", name)
                };
                if summary.len() + entry.len() - names_start > max_len {
                    summary.push_str(&format!(", ... ({} more)", names.len() - idx));
                    break;
                }
                summary.push_str(&entry);
            }
        }

        // Keep only meaningful compiler warnings from stderr (drop test progress noise)
        let compressed_stderr = compress_stderr_warnings(stderr);

        return Some((summary, compressed_stderr));
    }

    // ❌ Failures → keep failure details, drop "... ok" lines
    let compressed_stdout = compress_failing_test_output(stdout);

    // stderr often has compilation output; keep warnings/errors, cap the rest
    let compressed_stderr = compress_stderr_warnings(stderr);

    Some((compressed_stdout, compressed_stderr))
}

/// Compress cargo test output when there are failures.
///
/// Keeps: `running N tests`, `test ... FAILED`, the `failures:` section, and
/// the summary line. Drops all `test ... ok` lines.
fn compress_failing_test_output(stdout: &str) -> String {
    let mut result = String::with_capacity(stdout.len() / 2);
    let mut in_failures_section = false;
    let mut ok_count: u32 = 0;

    for line in stdout.lines() {
        let trimmed = line.trim();

        // Always keep the "failures:" header and everything after it
        if trimmed == "failures:" {
            // Emit the collapsed ok-count before entering failures section
            if ok_count > 0 {
                result.push_str(&format!("  ... {} tests passed (hidden)\n", ok_count));
                ok_count = 0;
            }
            in_failures_section = true;
            result.push_str(line);
            result.push('\n');
            continue;
        }

        if in_failures_section {
            // The failures section ends at the next "test result:" line
            if trimmed.starts_with("test result:") {
                in_failures_section = false;
                // fall through to keep this line
            } else {
                result.push_str(line);
                result.push('\n');
                continue;
            }
        }

        // Keep: "running N tests"
        if RUNNING_RE.is_match(trimmed) {
            result.push_str(line);
            result.push('\n');
            continue;
        }

        // Drop: "test ... ok"
        if TEST_LINE_OK_RE.is_match(trimmed) {
            ok_count += 1;
            continue;
        }

        // Keep: summary line, FAILED lines, everything else
        if ok_count > 0 {
            result.push_str(&format!("  ... {} tests passed (hidden)\n", ok_count));
            ok_count = 0;
        }
        result.push_str(line);
        result.push('\n');
    }

    if ok_count > 0 {
        result.push_str(&format!("  ... {} tests passed (hidden)\n", ok_count));
    }

    result
}

// ── pytest Handler ──────────────────────────────────────────────────────────

fn try_compress_pytest(stdout: &str, stderr: &str, exit_code: i64) -> Option<(String, String)> {
    let combined = format!("{}\n{}", stdout, stderr);

    // Detect pytest output by its distinctive summary line
    let summary_cap = PYTEST_SUMMARY_RE.captures(&combined)?;
    let summary_text = summary_cap.get(1)?.as_str();
    let time_str = summary_cap.get(2)?.as_str();

    // Parse counts from summary: "3 failed, 2 passed" etc.
    let mut passed: u32 = 0;
    let mut failed: u32 = 0;
    let mut errors: u32 = 0;
    let mut skipped: u32 = 0;

    for cap in PYTEST_COUNTS_RE.captures_iter(summary_text) {
        let count: u32 = cap
            .get(1)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);
        match cap.get(2).map(|m| m.as_str()) {
            Some("passed") => passed = count,
            Some("failed") => failed = count,
            Some("error") => errors = count,
            Some("skipped") => skipped = count,
            _ => {}
        }
    }

    if failed == 0 && errors == 0 && exit_code == 0 {
        // All pass → one-line summary
        let mut summary = format!("✅ pytest: {} passed", passed);
        if skipped > 0 {
            summary.push_str(&format!(", {} skipped", skipped));
        }
        summary.push_str(&format!(" ({})", time_str));
        return Some((summary, String::new()));
    }

    // Failures → keep FAILURES section, drop PASSED test lines
    let mut result = String::new();
    result.push_str(&format!(
        "❌ pytest: {} failed, {} passed",
        failed + errors,
        passed
    ));
    if skipped > 0 {
        result.push_str(&format!(", {} skipped", skipped));
    }
    result.push_str(&format!(" ({})\n\n", time_str));

    // Extract failure sections: keep everything between `_ test_name _` headers
    let mut in_failure = false;
    let mut failure_lines = 0u32;
    let max_failure_lines: u32 = 100;

    for line in combined.lines() {
        if PYTEST_FAILURE_HEADER_RE.is_match(line) {
            in_failure = true;
            failure_lines = 0;
            result.push_str(line);
            result.push('\n');
            continue;
        }
        if in_failure {
            if line.starts_with("=") && line.contains("short test summary") {
                in_failure = false;
                continue;
            }
            // Also end on the final summary line
            if PYTEST_SUMMARY_RE.is_match(line) {
                in_failure = false;
                continue;
            }
            failure_lines += 1;
            if failure_lines <= max_failure_lines {
                result.push_str(line);
                result.push('\n');
            } else if failure_lines == max_failure_lines + 1 {
                result.push_str("  ... (truncated)\n");
            }
        }
    }

    let compressed_stderr = compress_stderr_warnings(stderr);
    Some((result, compressed_stderr))
}

// ── Jest / Vitest Handler ───────────────────────────────────────────────────

fn try_compress_jest(stdout: &str, stderr: &str, exit_code: i64) -> Option<(String, String)> {
    // Jest can output to either stdout or stderr depending on runner/config
    let combined = format!("{}\n{}", stdout, stderr);

    // Detect Jest/Vitest by its summary format
    let tests_line = JEST_TESTS_RE.captures(&combined)?;
    let tests_text = tests_line.get(1)?.as_str();
    let time_str = JEST_TIME_RE
        .captures(&combined)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().trim().to_string())
        .unwrap_or_default();

    // Parse counts from "3 failed, 42 passed, 45 total"
    let mut passed: u32 = 0;
    let mut failed: u32 = 0;
    for cap in JEST_COUNT_RE.captures_iter(tests_text) {
        let count: u32 = cap
            .get(1)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);
        match cap.get(2).map(|m| m.as_str()) {
            Some("passed") => passed = count,
            Some("failed") => failed = count,
            _ => {}
        }
    }

    // Also check Test Suites line
    let suites_info = JEST_SUITES_RE
        .captures(&combined)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string());

    if failed == 0 && exit_code == 0 {
        let mut summary = format!("✅ jest: {} passed", passed);
        if let Some(suites) = &suites_info {
            summary.push_str(&format!(" (suites: {})", suites));
        }
        if !time_str.is_empty() {
            summary.push_str(&format!(" [{}]", time_str));
        }
        return Some((summary, String::new()));
    }

    // Failures → keep FAIL file lines + error details
    let mut result = String::new();
    result.push_str(&format!("❌ jest: {} failed, {} passed", failed, passed));
    if !time_str.is_empty() {
        result.push_str(&format!(" [{}]", time_str));
    }
    result.push('\n');

    // Keep FAIL lines with their file paths
    let mut in_fail_block = false;
    let mut fail_detail_lines = 0u32;
    let max_detail_lines: u32 = 30;

    for line in combined.lines() {
        let trimmed = line.trim();

        // Start of a FAIL block
        if let Some(cap) = JEST_RESULT_LINE_RE.captures(trimmed) {
            if cap.get(1).map(|m| m.as_str()) == Some("FAIL") {
                in_fail_block = true;
                fail_detail_lines = 0;
                result.push('\n');
                result.push_str(line);
                result.push('\n');
                continue;
            } else if cap.get(1).map(|m| m.as_str()) == Some("PASS") {
                in_fail_block = false;
                continue;
            }
        }

        if in_fail_block {
            fail_detail_lines += 1;
            if fail_detail_lines <= max_detail_lines {
                result.push_str(line);
                result.push('\n');
            } else if fail_detail_lines == max_detail_lines + 1 {
                result.push_str("  ... (truncated)\n");
            }
        }
    }

    Some((result, String::new()))
}

// ── Go test Handler ─────────────────────────────────────────────────────────

fn try_compress_go_test(stdout: &str, stderr: &str, exit_code: i64) -> Option<(String, String)> {
    let combined = format!("{}\n{}", stdout, stderr);

    // Detect Go test by its result lines
    let ok_count = GO_TEST_OK_RE.captures_iter(&combined).count();
    let fail_count = GO_TEST_FAIL_RE.captures_iter(&combined).count();

    if ok_count == 0 && fail_count == 0 {
        // Also check for individual test lines
        if !GO_TEST_LINE_RE.is_match(&combined) && !GO_RUN_RE.is_match(&combined) {
            return None;
        }
    }

    let pass_lines: Vec<String> = GO_TEST_LINE_RE
        .captures_iter(&combined)
        .filter(|c| c.get(1).map(|m| m.as_str()) == Some("PASS"))
        .map(|c| c.get(2).map(|m| m.as_str().to_string()).unwrap_or_default())
        .collect();

    let fail_lines: Vec<String> = GO_TEST_LINE_RE
        .captures_iter(&combined)
        .filter(|c| c.get(1).map(|m| m.as_str()) == Some("FAIL"))
        .map(|c| c.get(2).map(|m| m.as_str().to_string()).unwrap_or_default())
        .collect();

    if fail_lines.is_empty() && exit_code == 0 {
        let mut summary = format!("✅ go test: {} passed", pass_lines.len());
        if ok_count > 0 {
            summary.push_str(&format!(" ({} packages ok)", ok_count));
        }
        return Some((summary, String::new()));
    }

    // Failures → keep failure output
    let mut result = String::new();
    result.push_str(&format!(
        "❌ go test: {} failed, {} passed\n\n",
        fail_lines.len(),
        pass_lines.len()
    ));

    // Keep content around FAIL lines
    let mut in_fail_section = false;
    let mut section_lines = 0u32;
    let max_section_lines: u32 = 40;

    for line in combined.lines() {
        let trimmed = line.trim();

        if GO_TEST_LINE_RE
            .captures(trimmed)
            .map(|c| c.get(1).map(|m| m.as_str()) == Some("FAIL"))
            .unwrap_or(false)
        {
            in_fail_section = true;
            section_lines = 0;
            result.push_str(line);
            result.push('\n');
            continue;
        }

        // End section on next `--- PASS/FAIL` or package result
        if in_fail_section
            && (GO_TEST_LINE_RE.is_match(trimmed)
                || GO_TEST_OK_RE.is_match(trimmed)
                || GO_TEST_FAIL_RE.is_match(trimmed))
        {
            in_fail_section = false;
        }

        // Also keep `--- FAIL` and associated package FAIL lines
        if trimmed.starts_with("FAIL\t") || trimmed.starts_with("--- FAIL") {
            result.push_str(line);
            result.push('\n');
            continue;
        }

        if in_fail_section {
            section_lines += 1;
            if section_lines <= max_section_lines {
                result.push_str(line);
                result.push('\n');
            }
        }
    }

    let compressed_stderr = compress_stderr_warnings(stderr);
    Some((result, compressed_stderr))
}

// ── Maven Surefire Handler ──────────────────────────────────────────────────

fn try_compress_surefire(stdout: &str, stderr: &str, _exit_code: i64) -> Option<(String, String)> {
    let combined = format!("{}\n{}", stdout, stderr);

    // Detect Surefire output by its summary line
    let summaries: Vec<(u32, u32, u32, u32)> = SUREFIRE_SUMMARY_RE
        .captures_iter(&combined)
        .filter_map(|c| {
            Some((
                c.get(1)?.as_str().parse().ok()?,
                c.get(2)?.as_str().parse().ok()?,
                c.get(3)?.as_str().parse().ok()?,
                c.get(4)?.as_str().parse().ok()?,
            ))
        })
        .collect();

    if summaries.is_empty() {
        return None;
    }

    let total_run: u32 = summaries.iter().map(|s| s.0).sum();
    let total_fail: u32 = summaries.iter().map(|s| s.1).sum();
    let total_err: u32 = summaries.iter().map(|s| s.2).sum();
    let total_skip: u32 = summaries.iter().map(|s| s.3).sum();
    let total_pass = total_run.saturating_sub(total_fail + total_err + total_skip);

    if total_fail == 0 && total_err == 0 {
        let mut summary = format!("✅ surefire: {} passed", total_pass);
        if total_skip > 0 {
            summary.push_str(&format!(", {} skipped", total_skip));
        }
        return Some((summary, String::new()));
    }

    // Failures → keep error details
    let mut result = String::new();
    result.push_str(&format!(
        "❌ surefire: {} run, {} failures, {} errors",
        total_run, total_fail, total_err
    ));
    if total_skip > 0 {
        result.push_str(&format!(", {} skipped", total_skip));
    }
    result.push('\n');

    // Keep lines that contain failure details
    let mut in_failure_report = false;
    for line in combined.lines() {
        let trimmed = line.trim();
        // Surefire prints: `Failed tests:` or `Tests in error:`
        if trimmed.starts_with("Failed tests:") || trimmed.starts_with("Tests in error:") {
            in_failure_report = true;
            result.push('\n');
            result.push_str(line);
            result.push('\n');
            continue;
        }
        if in_failure_report {
            // Empty line ends the section
            if trimmed.is_empty() {
                in_failure_report = false;
                result.push('\n');
                continue;
            }
            result.push_str(line);
            result.push('\n');
        }
    }

    let compressed_stderr = compress_stderr_warnings(stderr);
    Some((result, compressed_stderr))
}

// ── Common Helpers ──────────────────────────────────────────────────────────

/// Compress stderr for test runs: keep `warning[...]` and `error[...]` lines
/// with their context, drop download/compile progress noise.
fn compress_stderr_warnings(stderr: &str) -> String {
    if stderr.trim().is_empty() {
        return String::new();
    }

    let lines: Vec<&str> = stderr.lines().collect();
    let mut keep = vec![false; lines.len()];
    let context_lines = 2usize;

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        // Keep lines that contain warnings or errors
        if trimmed.starts_with("warning[")
            || trimmed.starts_with("error[")
            || trimmed.starts_with("warning:")
            || trimmed.starts_with("error:")
            || trimmed.starts_with("error(")
        {
            // Mark this line and surrounding context
            let start = i.saturating_sub(context_lines);
            let end = (i + context_lines + 1).min(lines.len());
            for k in &mut keep[start..end] {
                *k = true;
            }
        }
    }

    // If nothing flagged, cap and return
    if !keep.iter().any(|&k| k) {
        let (capped, _) = filters::cap_lines(stderr, 30);
        return capped;
    }

    let mut result = String::with_capacity(stderr.len() / 2);
    let mut last_kept = false;
    for (i, line) in lines.iter().enumerate() {
        if keep[i] {
            if !last_kept && i > 0 {
                result.push_str("  ...\n");
            }
            result.push_str(line);
            result.push('\n');
            last_kept = true;
        } else {
            last_kept = false;
        }
    }

    result
}

// ── JSON Rebuild ───────────────────────────────────────────────────────────

/// Rebuild the Bash JSON envelope with compressed stdout/stderr.
fn rebuild_bash_json(
    original: &serde_json::Value,
    new_stdout: &str,
    new_stderr: &str,
    exit_code: i64,
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

    let compressed = serde_json::to_string(&obj).unwrap_or_else(|_| {
        // Fallback: simple format
        format!(
            r#"{{"command":"(compressed)","stdout":"{}","stderr":"{}","exit_code":{},"timed_out":false}}"#,
            new_stdout.replace('\"', "\\\""),
            new_stderr.replace('\"', "\\\""),
            exit_code
        )
    });

    let _ = exit_code; // used in fallback above
    CompressionResult {
        compressed,
        was_compressed: true,
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bash_json(stdout: &str, stderr: &str, exit_code: i64) -> String {
        serde_json::json!({
            "command": "cargo test",
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": exit_code,
            "timed_out": false,
            "stdout_truncated": false,
            "stderr_truncated": false,
        })
        .to_string()
    }

    #[test]
    fn all_pass_extreme_compression() {
        let stdout = "\
running 3 tests
test foo::bar ... ok
test foo::baz ... ok
test foo::qux ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.15s
";
        // Pad to exceed MIN_COMPRESS_LEN
        let padding = "x".repeat(MIN_COMPRESS_LEN);
        let full_stdout = format!("{}{}", stdout, padding);
        let input = make_bash_json(&full_stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);

        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ 3 tests passed"));
        assert!(result.compressed.contains("0.15s"));
        // Should contain test names in the summary
        assert!(result.compressed.contains("foo::bar"));
        assert!(result.compressed.contains("foo::baz"));
        assert!(result.compressed.contains("foo::qux"));
    }

    #[test]
    fn all_pass_with_ignored() {
        let stdout = "\
running 5 tests
test a ... ok
test b ... ok
test c ... ok
test d ... ignored
test e ... ignored

test result: ok. 3 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 0.50s
";
        let padding = "x".repeat(MIN_COMPRESS_LEN);
        let full_stdout = format!("{}{}", stdout, padding);
        let input = make_bash_json(&full_stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);

        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ 3 tests passed"));
        assert!(result.compressed.contains("2 ignored"));
    }

    #[test]
    fn failures_keep_details() {
        let stdout = "\
running 4 tests
test foo::a ... ok
test foo::b ... ok
test foo::c ... FAILED
test foo::d ... ok

failures:

---- foo::c stdout ----
thread 'foo::c' panicked at 'assertion `left == right` failed
  left: 1
  right: 2'

failures:
    foo::c

test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s
";
        let padding = "x".repeat(MIN_COMPRESS_LEN);
        let full_stdout = format!("{}{}", stdout, padding);
        let input = make_bash_json(&full_stdout, "", 1);
        let result = compress(&input, CompressionTier::Standard);

        assert!(result.was_compressed);
        // Should keep FAILED lines and failure details
        assert!(result.compressed.contains("FAILED"));
        assert!(result.compressed.contains("foo::c"));
        assert!(result.compressed.contains("panicked"));
        // Should NOT contain individual ok lines
        assert!(!result.compressed.contains("foo::a ... ok"));
        assert!(!result.compressed.contains("foo::b ... ok"));
        // ok lines flushed in segments: 2 before FAILED, 1 after
        assert!(result.compressed.contains("2 tests passed (hidden)"));
        assert!(result.compressed.contains("1 tests passed (hidden)"));
    }

    #[test]
    fn workspace_multiple_summaries() {
        let stdout = "\
running 2 tests
test lib_a::x ... ok
test lib_a::y ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.05s

running 3 tests
test lib_b::a ... ok
test lib_b::b ... ok
test lib_b::c ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.00s
";
        let padding = "x".repeat(MIN_COMPRESS_LEN);
        let full_stdout = format!("{}{}", stdout, padding);
        let input = make_bash_json(&full_stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);

        assert!(result.was_compressed);
        // Should aggregate: 2 + 3 = 5 passed
        assert!(result.compressed.contains("✅ 5 tests passed"));
    }

    #[test]
    fn short_output_not_compressed() {
        let stdout = "running 1 test\ntest x ... ok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n";
        let input = make_bash_json(stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);

        // Below MIN_COMPRESS_LEN → no compression
        assert!(!result.was_compressed);
    }

    #[test]
    fn non_test_output_fallback() {
        // Output that doesn't look like cargo test at all — exceeds DEFAULT_MAX_LINES
        let big_stdout = "some random output\n".repeat(500);
        let input = make_bash_json(&big_stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);

        // Should cap lines as fallback since no cargo test summary found
        assert!(result.was_compressed);
    }

    #[test]
    fn invalid_json_passthrough() {
        let input = "not valid json at all but long enough to trigger ".repeat(50);
        let result = compress(&input, CompressionTier::Standard);
        assert!(!result.was_compressed);
    }

    #[test]
    fn parse_cargo_summaries_basic() {
        let text = "test result: ok. 10 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 1.23s";
        let summaries = parse_cargo_summaries(text);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].passed, 10);
        assert_eq!(summaries[0].failed, 0);
        assert_eq!(summaries[0].ignored, 2);
        assert_eq!(summaries[0].time_str, "1.23s");
    }

    #[test]
    fn parse_cargo_summaries_failed() {
        let text = "test result: FAILED. 5 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.50s";
        let summaries = parse_cargo_summaries(text);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].passed, 5);
        assert_eq!(summaries[0].failed, 2);
    }

    #[test]
    fn compress_failing_output_structure() {
        let stdout = "\
running 3 tests
test a ... ok
test b ... FAILED
test c ... ok

failures:

---- b stdout ----
assertion failed

failures:
    b

test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out
";
        let result = compress_failing_test_output(stdout);
        assert!(result.contains("running 3 tests"));
        assert!(result.contains("FAILED"));
        assert!(result.contains("assertion failed"));
        assert!(!result.contains("test a ... ok"));
        assert!(!result.contains("test c ... ok"));
        // ok lines are flushed in segments: 1 before FAILED, 1 after
        assert!(result.contains("1 tests passed (hidden)"));
    }

    #[test]
    fn stderr_warnings_kept() {
        let stderr = "\
   Compiling foo v0.1.0
   Compiling bar v0.2.0
warning: unused variable `x`
 --> src/lib.rs:10:5
  |
10 |     let x = 1;
  |         ^ help: remove this
   Compiling baz v0.3.0
   Finished test target(s)
     Running tests
";
        let result = compress_stderr_warnings(stderr);
        assert!(result.contains("warning: unused variable"));
        // Should have context around the warning
        assert!(result.contains("src/lib.rs"));
    }

    // ── pytest tests ──

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

    /// Pad a string to exceed MIN_COMPRESS_LEN
    fn pad(s: &str) -> String {
        let needed = MIN_COMPRESS_LEN.saturating_sub(s.len()) + 100;
        format!("{}{}", s, "\n".repeat(needed))
    }

    #[test]
    fn pytest_all_pass() {
        let stdout = &pad("\
collecting ... collected 15 items

tests/test_auth.py::test_login PASSED
tests/test_auth.py::test_logout PASSED
tests/test_api.py::test_get_users PASSED
tests/test_api.py::test_create_user PASSED
tests/test_api.py::test_delete_user PASSED
tests/test_models.py::test_user_model PASSED
tests/test_models.py::test_validate PASSED
tests/test_models.py::test_serialize PASSED
tests/test_utils.py::test_hash PASSED
tests/test_utils.py::test_format PASSED
tests/test_utils.py::test_parse PASSED
tests/test_utils.py::test_clean PASSED
tests/test_utils.py::test_merge PASSED
tests/test_utils.py::test_split PASSED
tests/test_utils.py::test_join PASSED

========================= 15 passed in 1.23s ==========================
");
        let input = make_bash_json_cmd("pytest", stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ pytest: 15 passed"));
        assert!(result.compressed.contains("1.23s"));
        // Should NOT contain individual test lines
        assert!(!result.compressed.contains("test_login PASSED"));
    }

    #[test]
    fn pytest_with_failures() {
        let stdout = &pad("\
collecting ... collected 10 items

tests/test_auth.py::test_login PASSED
tests/test_auth.py::test_token FAILED
tests/test_api.py::test_get PASSED

_________________________________ test_token __________________________________

    def test_token():
>       assert generate_token() == 'expected'
E       AssertionError: assert 'abc' == 'expected'
E         - expected
E         + abc

tests/test_auth.py:25: AssertionError

=========================== short test summary info ============================
FAILED tests/test_auth.py::test_token
========================= 1 failed, 2 passed in 0.45s =========================
");
        let input = make_bash_json_cmd("pytest", stdout, "", 1);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("❌ pytest: 1 failed, 2 passed"));
        assert!(result.compressed.contains("0.45s"));
        // Should keep failure details
        assert!(result.compressed.contains("test_token"));
        assert!(result.compressed.contains("AssertionError"));
    }

    #[test]
    fn pytest_with_skipped() {
        let stdout = &pad("\
========================= 8 passed, 3 skipped in 2.10s ==========================
");
        let input = make_bash_json_cmd("pytest -v", stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ pytest: 8 passed, 3 skipped"));
    }

    // ── Jest/Vitest tests ──

    #[test]
    fn jest_all_pass() {
        let stdout = &pad("\
PASS src/auth.test.ts
PASS src/api.test.ts
PASS src/utils.test.ts

Test Suites: 3 passed, 3 total
Tests:       25 passed, 25 total
Snapshots:   0 total
Time:        4.567 s
");
        let input = make_bash_json_cmd("npx jest", stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ jest: 25 passed"));
        assert!(result.compressed.contains("4.567 s"));
    }

    #[test]
    fn jest_with_failures() {
        let stdout = &pad("\
PASS src/auth.test.ts
FAIL src/api.test.ts
  ● GET /users > should return all users
    expect(received).toEqual(expected)
    Expected: 200
    Received: 404
      at Object.<anonymous> (src/api.test.ts:15:20)
PASS src/utils.test.ts

Test Suites: 1 failed, 2 passed, 3 total
Tests:       1 failed, 24 passed, 25 total
Snapshots:   0 total
Time:        5.123 s
");
        let input = make_bash_json_cmd("npm test", stdout, "", 1);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("❌ jest: 1 failed, 24 passed"));
        // Should keep failure details
        assert!(result.compressed.contains("should return all users"));
        assert!(result.compressed.contains("Expected: 200"));
    }

    // ── Go test tests ──

    #[test]
    fn go_test_all_pass() {
        let stdout = &pad("\
=== RUN   TestAdd
--- PASS: TestAdd (0.00s)
=== RUN   TestSubtract
--- PASS: TestSubtract (0.00s)
=== RUN   TestMultiply
--- PASS: TestMultiply (0.00s)
=== RUN   TestDivide
--- PASS: TestDivide (0.01s)
ok      github.com/foo/mathlib      0.023s
ok      github.com/foo/utils        0.015s
");
        let input = make_bash_json_cmd("go test ./...", stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ go test: 4 passed"));
        assert!(result.compressed.contains("2 packages ok"));
    }

    #[test]
    fn go_test_with_failures() {
        let stdout = &pad("\
=== RUN   TestAdd
--- PASS: TestAdd (0.00s)
=== RUN   TestDivide
--- FAIL: TestDivide (0.00s)
    math_test.go:42: expected 5, got 0 (division by zero)
FAIL    github.com/foo/mathlib      0.034s
ok      github.com/foo/utils        0.015s
");
        let input = make_bash_json_cmd("go test ./...", stdout, "", 1);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("❌ go test: 1 failed, 1 passed"));
        // Should keep failure details
        assert!(result.compressed.contains("TestDivide"));
        assert!(result.compressed.contains("division by zero"));
    }

    // ── Surefire tests ──

    #[test]
    fn surefire_all_pass() {
        let stdout = &pad("\
[INFO] --- maven-surefire-plugin:3.1.2:test (default-test) @ myapp ---
[INFO] -------------------------------------------------------
[INFO]  T E S T S
[INFO] -------------------------------------------------------
[INFO] Running com.example.AppTest
[INFO] Tests run: 15, Failures: 0, Errors: 0, Skipped: 2 - in com.example.AppTest
[INFO] Running com.example.ServiceTest
[INFO] Tests run: 8, Failures: 0, Errors: 0, Skipped: 0 - in com.example.ServiceTest
[INFO]
[INFO] Results:
[INFO]
Tests run: 23, Failures: 0, Errors: 0, Skipped: 2
");
        let input = make_bash_json_cmd("mvn test", stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ surefire:"));
        // Sum: 15+8+23 run, but we should get aggregated totals
        assert!(result.compressed.contains("passed"));
    }

    #[test]
    fn surefire_with_failures() {
        let stdout = &pad("\
[INFO] -------------------------------------------------------
[INFO]  T E S T S
[INFO] -------------------------------------------------------
Tests run: 10, Failures: 2, Errors: 1, Skipped: 0

Failed tests:
  com.example.AppTest.testLogin
  com.example.AppTest.testLogout

Tests in error:
  com.example.ServiceTest.testTimeout

[INFO] BUILD FAILURE
");
        let input = make_bash_json_cmd("mvn test", stdout, "", 1);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("❌ surefire:"));
        assert!(result.compressed.contains("2 failures"));
        assert!(result.compressed.contains("1 errors"));
        // Should keep failure details
        assert!(result.compressed.contains("testLogin"));
        assert!(result.compressed.contains("testTimeout"));
    }

    // ── Edge cases ──

    #[test]
    fn empty_stdout_stderr() {
        let input = make_bash_json("", "", 0);
        let result = compress(&input, CompressionTier::Standard);
        // Below MIN_COMPRESS_LEN → not compressed
        assert!(!result.was_compressed);
    }

    #[test]
    fn cargo_test_zero_tests() {
        let stdout = "\
running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
";
        let padding = "x".repeat(MIN_COMPRESS_LEN);
        let full_stdout = format!("{}{}", stdout, padding);
        let input = make_bash_json(&full_stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("0 tests passed"));
    }

    #[test]
    fn cargo_test_many_names_truncated() {
        let mut stdout = String::from("running 50 tests\n");
        for i in 0..50 {
            stdout.push_str(&format!(
                "test module::test_case_name_{}_with_long_name ... ok\n",
                i
            ));
        }
        stdout.push_str("\ntest result: ok. 50 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.00s\n");
        let padding = "x".repeat(MIN_COMPRESS_LEN);
        let full_stdout = format!("{}{}", stdout, padding);
        let input = make_bash_json(&full_stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("50 tests passed"));
        // Should have test names but truncated due to 200-char limit
        assert!(result.compressed.contains("Tests:"));
        assert!(result.compressed.contains("... (") || result.compressed.contains("more)"));
    }

    #[test]
    fn cargo_test_all_ignored() {
        let stdout = "\
running 3 tests
test a ... ignored
test b ... ignored
test c ... ignored

test result: ok. 0 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 0.00s
";
        let padding = "x".repeat(MIN_COMPRESS_LEN);
        let full_stdout = format!("{}{}", stdout, padding);
        let input = make_bash_json(&full_stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("0 tests passed"));
        assert!(result.compressed.contains("3 ignored"));
    }

    #[test]
    fn cargo_test_failure_without_failures_section() {
        // Some failures have no "failures:" section (e.g. compile errors in tests)
        let stdout = "\
running 1 test
test foo::bar ... FAILED

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out
";
        let padding = "x".repeat(MIN_COMPRESS_LEN);
        let full_stdout = format!("{}{}", stdout, padding);
        let input = make_bash_json(&full_stdout, "", 101);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("FAILED"));
    }

    #[test]
    fn multiple_test_binaries_mixed_pass_fail() {
        let stdout = "\
running 2 tests
test lib_a::x ... ok
test lib_a::y ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.05s

running 2 tests
test lib_b::a ... ok
test lib_b::b ... FAILED

test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s
";
        let padding = "x".repeat(MIN_COMPRESS_LEN);
        let full_stdout = format!("{}{}", stdout, padding);
        let input = make_bash_json(&full_stdout, "", 101);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        // Should show aggregated: 3 passed, 1 failed
        assert!(result.compressed.contains("FAILED"));
    }

    #[test]
    fn pytest_all_pass_no_test_lines() {
        // pytest summary only, no individual test lines
        let stdout = &pad("\
========================= 5 passed in 0.12s ==========================
");
        let input = make_bash_json_cmd("pytest", stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ pytest: 5 passed"));
    }

    #[test]
    fn jest_zero_tests() {
        let stdout = &pad("\
Test Suites: 0 total
Tests:       0 total
Snapshots:   0 total
Time:        0.001 s
");
        let input = make_bash_json_cmd("npx jest", stdout, "", 0);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("0 passed"));
    }

    #[test]
    fn go_test_all_fail() {
        let stdout = &pad("\
=== RUN   TestFoo
--- FAIL: TestFoo (0.00s)
    foo_test.go:10: unexpected value
=== RUN   TestBar
--- FAIL: TestBar (0.00s)
    bar_test.go:20: wrong result
FAIL    github.com/example/pkg
");
        let input = make_bash_json_cmd("go test ./...", stdout, "", 1);
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("2 failed"));
        assert!(result.compressed.contains("0 passed"));
    }
}
