//! Compression for `Grep` tool output.
//!
//! **Strategy:**
//! - Cap total matches to avoid overwhelming context with grep results.
//! - Collapse duplicate file paths when many matches in the same file.
//! - Preserve enough context for the LLM to understand where matches are.

use regex::Regex;

use crate::agent::loop_module::runner::tool_execution::output_compressor::filters;
use crate::agent::loop_module::runner::tool_execution::output_compressor::CompressionResult;

/// Minimum result length (chars) before compression kicks in.
const MIN_COMPRESS_LEN: usize = 4_000;

/// Maximum lines in compressed grep output.
const MAX_LINES: usize = 200;

/// Maximum matches per file before summarizing.
const MAX_MATCHES_PER_FILE: usize = 15;

lazy_static::lazy_static! {
    /// Matches grep output with file path: `path/to/file.rs:42:matched text`
    /// or `path/to/file.rs-42-context line`
    static ref GREP_FILE_LINE_RE: Regex = Regex::new(
        r"^(\S+?)[:]\d+[:-]"
    ).expect("grep file line regex");
}

// ── Public Entry Point ─────────────────────────────────────────────────────

pub(crate) fn compress(raw_result: &str) -> CompressionResult {
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

    for line in &lines {
        // Try to extract file path from the line
        if let Some(cap) = GREP_FILE_LINE_RE.captures(line) {
            let file = cap.get(1).map(|m| m.as_str().to_string()).unwrap_or_default();

            if Some(&file) != current_file.as_ref() {
                // Flush hidden count for previous file
                if hidden_count > 0 {
                    result.push_str(&format!(
                        "  ... ({} more matches hidden)\n",
                        hidden_count
                    ));
                    hidden_count = 0;
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
            }
        } else {
            // Non-match lines (e.g., context separators `--`)
            if hidden_count > 0 {
                result.push_str(&format!(
                    "  ... ({} more matches hidden)\n",
                    hidden_count
                ));
                hidden_count = 0;
            }
            result.push_str(line);
            result.push('\n');
        }
    }

    // Final flush
    if hidden_count > 0 {
        result.push_str(&format!(
            "  ... ({} more matches hidden)\n",
            hidden_count
        ));
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
        let result = compress(input);
        assert!(!result.was_compressed);
    }

    #[test]
    fn many_matches_per_file_capped() {
        let mut input = String::new();
        for i in 0..30 {
            input.push_str(&format!("src/big_file.rs:{}:let x = {};\n", i, i));
        }
        let padded = pad(&input);
        let result = compress(&padded);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("more matches hidden"));
        // First 15 should be kept
        assert!(result.compressed.contains("src/big_file.rs:0:"));
        assert!(result.compressed.contains("src/big_file.rs:14:"));
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
        let result = compress(&padded);
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
        let result = compress(&input);
        assert!(result.was_compressed);
        let line_count = result.compressed.lines().count();
        assert!(line_count <= MAX_LINES + 5);
    }
}
