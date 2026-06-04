//! Compression for `Grep` tool output.
//!
//! **Strategy:**
//! - Cap total matches to avoid overwhelming context with grep results.
//! - Collapse duplicate file paths when many matches in the same file.
//! - Preserve enough context for the LLM to understand where matches are.

use regex::Regex;
use std::sync::LazyLock;

use crate::runtime::runner::tool_execution::output_compressor::filters;
use crate::runtime::runner::tool_execution::output_compressor::CompressionResult;
use crate::runtime::runner::tool_execution::output_compressor::CompressionTier;

/// Minimum result length (chars) before compression kicks in.
const MIN_COMPRESS_LEN: usize = 4_000;

/// Maximum lines in compressed grep output.
const MAX_LINES: usize = 200;

/// Maximum matches per file before summarizing.
const MAX_MATCHES_PER_FILE: usize = 15;

/// Matches grep output with file path: `path/to/file.rs:42:matched text`
/// or `path/to/file.rs-42-context line`
static GREP_FILE_LINE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(\S+?)[:]\d+[:-]").expect("grep file line regex"));

/// Extracts the line number from grep output: `path/to/file.rs:42:matched`
static GREP_LINE_NUM_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\S+?[:](\d+)[:-]").expect("grep line number regex"));

// ── Public Entry Point ─────────────────────────────────────────────────────

pub(crate) fn compress(raw_result: &str, tier: CompressionTier) -> CompressionResult {
    let _ = tier;
    if raw_result.len() < MIN_COMPRESS_LEN {
        return CompressionResult {
            compressed: raw_result.to_string(),
            was_compressed: false,
        };
    }

    let compressed = compress_grep_output(raw_result);
    let (capped, was_capped) = filters::cap_lines(&compressed, MAX_LINES);

    let was_compressed = was_capped || compressed.len() < raw_result.len();

    if !was_compressed {
        return CompressionResult {
            compressed: raw_result.to_string(),
            was_compressed: false,
        };
    }

    CompressionResult {
        compressed: capped,
        was_compressed: true,
    }
}

// ── Grep Output Compression ──────────────────────────────────────────────────

fn compress_grep_output(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let mut result = String::with_capacity(text.len());

    let mut current_file: Option<String> = None;
    let mut file_match_count: usize = 0;
    let mut hidden_count: usize = 0;
    let mut hidden_first_line: Option<usize> = None;
    let mut hidden_last_line: Option<usize> = None;

    let flush_hidden =
        |result: &mut String, count: usize, first: Option<usize>, last: Option<usize>| {
            let range_hint = match (first, last) {
                (Some(f), Some(l)) if f != l => format!(" at lines {}-{}", f, l),
                (Some(f), _) => format!(" at line {}", f),
                _ => String::new(),
            };
            result.push_str(&format!(
                "  ... ({} more matches hidden{})\n",
                count, range_hint
            ));
        };

    for line in &lines {
        // Try to extract file path from the line
        if let Some(cap) = GREP_FILE_LINE_RE.captures(line) {
            let file = cap
                .get(1)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default();

            if Some(&file) != current_file.as_ref() {
                // Flush hidden count for previous file
                if hidden_count > 0 {
                    flush_hidden(
                        &mut result,
                        hidden_count,
                        hidden_first_line,
                        hidden_last_line,
                    );
                    hidden_count = 0;
                    hidden_first_line = None;
                    hidden_last_line = None;
                }
                current_file = Some(file);
                file_match_count = 0;
            }

            file_match_count += 1;

            if file_match_count <= MAX_MATCHES_PER_FILE {
                result.push_str(line);
                result.push('\n');
            } else {
                hidden_count += 1;
                let line_num = GREP_LINE_NUM_RE
                    .captures(line)
                    .and_then(|c| c.get(1))
                    .and_then(|m| m.as_str().parse::<usize>().ok());
                if hidden_first_line.is_none() {
                    hidden_first_line = line_num;
                }
                hidden_last_line = line_num;
            }
        } else {
            // Non-match lines (e.g., context separators `--`)
            if hidden_count > 0 {
                flush_hidden(
                    &mut result,
                    hidden_count,
                    hidden_first_line,
                    hidden_last_line,
                );
                hidden_count = 0;
                hidden_first_line = None;
                hidden_last_line = None;
            }
            result.push_str(line);
            result.push('\n');
        }
    }

    // Final flush
    if hidden_count > 0 {
        flush_hidden(
            &mut result,
            hidden_count,
            hidden_first_line,
            hidden_last_line,
        );
    }

    result
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn pad(s: &str) -> String {
        let needed = MIN_COMPRESS_LEN.saturating_sub(s.len()) + 100;
        format!("{}{}", s, "\n".repeat(needed))
    }

    #[test]
    fn short_output_not_compressed() {
        let input = "file.rs:1:match\nfile.rs:2:match\n";
        let result = compress(input, CompressionTier::Standard);
        assert!(!result.was_compressed);
    }

    #[test]
    fn many_matches_per_file_capped() {
        let mut input = String::new();
        for i in 0..30 {
            input.push_str(&format!("src/big_file.rs:{}:let x = {};\n", i, i));
        }
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("more matches hidden"));
        // First 15 should be kept
        assert!(result.compressed.contains("src/big_file.rs:0:"));
        assert!(result.compressed.contains("src/big_file.rs:14:"));
        // Hidden matches should show line range
        assert!(result.compressed.contains("lines 15-29"));
    }

    #[test]
    fn different_files_separate_counts() {
        let mut input = String::new();
        for i in 0..20 {
            input.push_str(&format!("src/file_a.rs:{}:match a {}\n", i, i));
        }
        for i in 0..20 {
            input.push_str(&format!("src/file_b.rs:{}:match b {}\n", i, i));
        }
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        assert!(result.was_compressed);
        // Both files should have their own cap
        assert!(result.compressed.contains("src/file_a.rs:0:"));
        assert!(result.compressed.contains("src/file_b.rs:0:"));
    }

    #[test]
    fn total_lines_capped() {
        let input: String = (0..500)
            .map(|i| format!("file_{}.rs:1:some match text here line {}\n", i, i))
            .collect();
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        let line_count = result.compressed.lines().count();
        assert!(line_count <= MAX_LINES + 5);
    }

    // ── Edge cases ──

    #[test]
    fn empty_input_not_compressed() {
        let result = compress("", CompressionTier::Standard);
        assert!(!result.was_compressed);
    }

    #[test]
    fn single_match_not_compressed() {
        let input = "src/main.rs:42:fn main() {\n";
        let result = compress(input, CompressionTier::Standard);
        assert!(!result.was_compressed);
    }

    #[test]
    fn hidden_matches_single_line_range() {
        // Exactly 16 matches in one file → 1 hidden, same line → "at line N"
        let mut input = String::new();
        for i in 0..16 {
            input.push_str(&format!("src/file.rs:50:match text {}\n", i));
        }
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("at line 50"));
    }

    #[test]
    fn context_separator_preserved() {
        let mut input = String::new();
        for i in 0..15 {
            input.push_str(&format!("src/a.rs:{}:match\n", i));
        }
        input.push_str("--\n");
        for i in 0..15 {
            input.push_str(&format!("src/b.rs:{}:match\n", i));
        }
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        assert!(result.was_compressed);
        // Context separator should be preserved
        assert!(result.compressed.contains("--"));
    }

    #[test]
    fn grep_with_dash_separator_format() {
        // Some grep output uses `-` instead of `:` for context lines
        let mut input = String::new();
        for i in 0..20 {
            input.push_str(&format!("src/file.rs-{}-context line\n", i));
        }
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        assert!(result.was_compressed);
    }
}
