//! Compression for git command output: `git status`, `git diff`, `git log`, etc.
//!
//! **Strategy:**
//! - `git status` clean → `"✅ Working tree clean"`
//! - `git status` dirty → compact summary: `"main | 3M 1? 1A"`
//! - `git diff` → keep diff stats + hunks, drop redundant context lines
//! - `git log` → cap entries, keep summary

use regex::Regex;

use crate::agent::loop_module::runner::tool_execution::output_compressor::filters;
use crate::agent::loop_module::runner::tool_execution::output_compressor::CompressionResult;

/// Minimum result length (chars) before compression kicks in.
const MIN_COMPRESS_LEN: usize = 1500;

/// Max lines for compressed git diff output.
const MAX_DIFF_LINES: usize = 200;

/// Max lines for compressed git log output.
const MAX_LOG_LINES: usize = 100;

lazy_static::lazy_static! {
    /// Matches: `On branch main` or `On branch feature/foo`
    static ref GIT_BRANCH_RE: Regex = Regex::new(
        r"On branch\s+(\S+)"
    ).expect("GIT_BRANCH_RE must compile");

    /// Matches: `nothing to commit, working tree clean`
    static ref GIT_CLEAN_RE: Regex = Regex::new(
        r"nothing to commit"
    ).expect("GIT_CLEAN_RE must compile");

    /// Matches git status short format: `M  src/main.rs` or `?? new_file.txt`
    static ref GIT_STATUS_SHORT_RE: Regex = Regex::new(
        r"^\s*([MADRCU?! ]{1,2})\s+(.+)"
    ).expect("GIT_STATUS_SHORT_RE must compile");

    /// Matches diff stat lines: ` src/main.rs | 42 +++---`
    static ref DIFF_STAT_RE: Regex = Regex::new(
        r"^\s+\S+.*\|\s+\d+"
    ).expect("DIFF_STAT_RE must compile");

    /// Matches diff summary: `3 files changed, 10 insertions(+), 5 deletions(-)`
    static ref DIFF_SUMMARY_RE: Regex = Regex::new(
        r"\d+ files? changed"
    ).expect("DIFF_SUMMARY_RE must compile");

    /// Matches diff hunk header: `@@ -10,5 +10,7 @@ fn something`
    static ref DIFF_HUNK_RE: Regex = Regex::new(
        r"^@@\s+[-+]\d+"
    ).expect("DIFF_HUNK_RE must compile");

    /// Matches diff file header: `diff --git a/file b/file`
    static ref DIFF_FILE_HEADER_RE: Regex = Regex::new(
        r"^diff --git"
    ).expect("DIFF_FILE_HEADER_RE must compile");

    /// Matches git log commit line: `commit abc123...`
    static ref LOG_COMMIT_RE: Regex = Regex::new(
        r"^commit [0-9a-f]{7,40}"
    ).expect("LOG_COMMIT_RE must compile");
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
        Err(_) => return compress_plain_git_text(raw_result),
    };

    let stdout = parsed.get("stdout").and_then(|v| v.as_str()).unwrap_or("");
    let stderr = parsed.get("stderr").and_then(|v| v.as_str()).unwrap_or("");
    let command = parsed.get("command").and_then(|v| v.as_str()).unwrap_or("");

    let clean_stdout = filters::strip_ansi(stdout);
    let clean_stderr = filters::strip_ansi(stderr);

    // Dispatch by git subcommand
    if command.contains("git status") {
        if let Some(result) = compress_git_status(&clean_stdout, &parsed) {
            return result;
        }
    }
    if command.contains("git diff") {
        if let Some(result) = compress_git_diff(&clean_stdout, &parsed) {
            return result;
        }
    }
    if command.contains("git log") || command.contains("git show") {
        if let Some(result) = compress_git_log(&clean_stdout, &parsed) {
            return result;
        }
    }

    // Also auto-detect from output content if command wasn't parsed
    if GIT_BRANCH_RE.is_match(&clean_stdout) && clean_stdout.contains("Changes") {
        if let Some(result) = compress_git_status(&clean_stdout, &parsed) {
            return result;
        }
    }
    if DIFF_FILE_HEADER_RE.is_match(&clean_stdout) {
        if let Some(result) = compress_git_diff(&clean_stdout, &parsed) {
            return result;
        }
    }

    // Fallback: generic compression
    let combined = format!("{}\n{}", clean_stdout, clean_stderr);
    let (capped, was_capped) = filters::cap_lines(&combined, MAX_DIFF_LINES);
    if !was_capped {
        return CompressionResult {
            compressed: raw_result.to_string(),
            was_compressed: false,
        };
    }

    rebuild_json(&parsed, &capped, "")
}

// ── Git Status ──────────────────────────────────────────────────────────────

fn compress_git_status(stdout: &str, original: &serde_json::Value) -> Option<CompressionResult> {
    let branch = GIT_BRANCH_RE
        .captures(stdout)
        .map(|c| c[1].to_string())
        .unwrap_or_else(|| "?".to_string());

    // Clean working tree
    if GIT_CLEAN_RE.is_match(stdout) {
        return Some(rebuild_json(
            original,
            &format!("✅ On branch {} — working tree clean", branch),
            "",
        ));
    }

    // Dirty: count file statuses
    let mut modified = 0u32;
    let mut added = 0u32;
    let mut deleted = 0u32;
    let mut untracked = 0u32;
    let mut renamed = 0u32;
    let mut files: Vec<String> = Vec::new();

    for line in stdout.lines() {
        if let Some(cap) = GIT_STATUS_SHORT_RE.captures(line) {
            let status = cap[1].trim();
            let file = &cap[2];

            match status {
                s if s.contains('M') => {
                    modified += 1;
                    files.push(format!("  M  {}", file));
                }
                s if s.contains('A') => {
                    added += 1;
                    files.push(format!("  A  {}", file));
                }
                s if s.contains('D') => {
                    deleted += 1;
                    files.push(format!("  D  {}", file));
                }
                s if s.contains('R') => {
                    renamed += 1;
                    files.push(format!("  R  {}", file));
                }
                s if s.contains('?') => {
                    untracked += 1;
                    files.push(format!("  ?  {}", file));
                }
                _ => {
                    files.push(format!("  {}  {}", status, file));
                }
            }
        }
    }

    // Build compact summary
    let mut summary = format!("On branch {}", branch);
    let mut parts = Vec::new();
    if modified > 0 {
        parts.push(format!("{}M", modified));
    }
    if added > 0 {
        parts.push(format!("{}A", added));
    }
    if deleted > 0 {
        parts.push(format!("{}D", deleted));
    }
    if renamed > 0 {
        parts.push(format!("{}R", renamed));
    }
    if untracked > 0 {
        parts.push(format!("{}?", untracked));
    }

    if !parts.is_empty() {
        summary.push_str(" | ");
        summary.push_str(&parts.join(" "));
    }
    summary.push('\n');

    // List files (capped)
    let total_files = files.len();
    if total_files <= 30 {
        for f in &files {
            summary.push_str(f);
            summary.push('\n');
        }
    } else {
        for f in &files[..15] {
            summary.push_str(f);
            summary.push('\n');
        }
        summary.push_str(&format!("  ... ({} more files)\n", total_files - 30));
        for f in &files[total_files - 15..] {
            summary.push_str(f);
            summary.push('\n');
        }
    }

    Some(rebuild_json(original, &summary, ""))
}

// ── Git Diff ────────────────────────────────────────────────────────────────

fn compress_git_diff(stdout: &str, original: &serde_json::Value) -> Option<CompressionResult> {
    let lines: Vec<&str> = stdout.lines().collect();
    if lines.is_empty() {
        return None;
    }

    // If output has --stat format, keep it as-is (already compact)
    if DIFF_STAT_RE.is_match(lines.first().unwrap_or(&"")) || DIFF_SUMMARY_RE.is_match(stdout) {
        let (capped, was_capped) = filters::cap_lines(stdout, MAX_DIFF_LINES);
        if !was_capped && stdout.len() < MIN_COMPRESS_LEN {
            return None;
        }
        return Some(rebuild_json(original, &capped, ""));
    }

    // Full diff: keep file headers, hunk headers, and +/- lines; reduce context
    let mut result = String::with_capacity(stdout.len() / 2);
    let mut context_count = 0u32;
    let max_context = 3u32; // Keep max 3 context lines around changes

    for line in &lines {
        if DIFF_FILE_HEADER_RE.is_match(line) {
            result.push_str(line);
            result.push('\n');
            context_count = 0;
            continue;
        }

        // Always keep: +++ / --- headers, hunk headers
        if line.starts_with("+++") || line.starts_with("---") || DIFF_HUNK_RE.is_match(line) {
            result.push_str(line);
            result.push('\n');
            context_count = 0;
            continue;
        }

        // Keep change lines
        if line.starts_with('+') || line.starts_with('-') {
            result.push_str(line);
            result.push('\n');
            context_count = 0;
            continue;
        }

        // Context lines: keep up to max_context
        if line.starts_with(' ') {
            context_count += 1;
            if context_count <= max_context {
                result.push_str(line);
                result.push('\n');
            } else if context_count == max_context + 1 {
                result.push_str("  ...\n");
            }
            continue;
        }

        // Summary lines (diff --stat at end)
        if DIFF_SUMMARY_RE.is_match(line) || DIFF_STAT_RE.is_match(line) {
            result.push_str(line);
            result.push('\n');
        }
    }

    let (capped, _) = filters::cap_lines(&result, MAX_DIFF_LINES);
    Some(rebuild_json(original, &capped, ""))
}

// ── Git Log ─────────────────────────────────────────────────────────────────

fn compress_git_log(stdout: &str, original: &serde_json::Value) -> Option<CompressionResult> {
    let (capped, was_capped) = filters::cap_lines(stdout, MAX_LOG_LINES);

    if !was_capped {
        return None; // Already small enough
    }

    // Count commits
    let commit_count = stdout.lines().filter(|l| LOG_COMMIT_RE.is_match(l)).count();
    let shown = capped.lines().filter(|l| LOG_COMMIT_RE.is_match(l)).count();

    let mut result = capped;
    if commit_count > shown {
        result.push_str(&format!(
            "\n... (showing {} of {} commits)\n",
            shown, commit_count
        ));
    }

    Some(rebuild_json(original, &result, ""))
}

// ── Helpers ─────────────────────────────────────────────────────────────────

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

fn compress_plain_git_text(text: &str) -> CompressionResult {
    let clean = filters::strip_ansi(text);
    let (capped, was_capped) = filters::cap_lines(&clean, MAX_DIFF_LINES);
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

    fn make_bash_json(cmd: &str, stdout: &str, stderr: &str, exit_code: i64) -> String {
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

    // ── git status ──

    #[test]
    fn git_status_clean() {
        let stdout = &pad("On branch main\nnothing to commit, working tree clean\n");
        let input = make_bash_json("git status", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅"));
        assert!(result.compressed.contains("main"));
        assert!(result.compressed.contains("clean"));
    }

    #[test]
    fn git_status_dirty() {
        let stdout = &pad("\
On branch feature/foo
Changes not staged for commit:
  (use \"git add <file>...\" to update what will be committed)

 M src/main.rs
 M src/lib.rs

Untracked files:
  (use \"git add <file>...\" to include in what will be committed)

?? new_file.txt

");
        let input = make_bash_json("git status", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("feature/foo"));
        // Should have compact counts
        assert!(result.compressed.contains("2M") || result.compressed.contains("M"));
    }

    // ── git diff ──

    #[test]
    fn git_diff_stat() {
        let stdout = &pad("\
 src/main.rs | 10 ++++------
 src/lib.rs  |  5 ++---
 2 files changed, 6 insertions(+), 9 deletions(-)
");
        let input = make_bash_json("git diff --stat", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("files changed"));
    }

    #[test]
    fn git_diff_full_reduces_context() {
        let mut lines = Vec::new();
        lines.push("diff --git a/src/main.rs b/src/main.rs".to_string());
        lines.push("--- a/src/main.rs".to_string());
        lines.push("+++ b/src/main.rs".to_string());
        lines.push("@@ -1,20 +1,20 @@".to_string());
        // Add 10 context lines, then a change, then 10 more context
        for i in 0..10 {
            lines.push(format!(" context line {}", i));
        }
        lines.push("+added line".to_string());
        lines.push("-removed line".to_string());
        for i in 0..10 {
            lines.push(format!(" more context {}", i));
        }

        let stdout = &pad(&lines.join("\n"));
        let input = make_bash_json("git diff", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        // Should still contain the changes
        assert!(result.compressed.contains("+added line") || result.compressed.contains("added"));
    }

    // ── git log ──

    #[test]
    fn git_log_capped() {
        let mut log = String::new();
        for i in 0..50 {
            log.push_str(&format!(
                "commit {0:07x}{0:07x}{0:07x}{0:07x}{0:07x}{0:05x}\nAuthor: Test <test@test.com>\nDate:   Mon Jan 1 00:00:00 2024\n\n    commit message {1}\n\n",
                i, i
            ));
        }
        let stdout = &pad(&log);
        let input = make_bash_json("git log", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
    }

    // ── Fallback ──

    #[test]
    fn short_output_not_compressed() {
        let input = make_bash_json("git status", "On branch main\nclean", "", 0);
        let result = compress(&input);
        assert!(!result.was_compressed);
    }
}
