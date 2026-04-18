//! Generic Bash output compression: ANSI strip + blank-line collapse + line/byte cap.
//!
//! Applied to any Bash command that doesn't match a more specific scenario.

use crate::runner::tool_execution::output_compressor::filters;
use crate::runner::tool_execution::output_compressor::CompressionResult;

/// Minimum result length (chars) before compression kicks in.
const MIN_COMPRESS_LEN: usize = 1500;

/// Max lines for stdout in generic mode.
const MAX_STDOUT_LINES: usize = 200;

/// Max lines for stderr in generic mode.
const MAX_STDERR_LINES: usize = 80;

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
        Err(_) => {
            // Not valid JSON — apply text-level compression
            return compress_plain_text(raw_result);
        }
    };

    let stdout = parsed.get("stdout").and_then(|v| v.as_str()).unwrap_or("");
    let stderr = parsed.get("stderr").and_then(|v| v.as_str()).unwrap_or("");

    // Stage 1: Strip ANSI
    let clean_stdout = filters::strip_ansi(stdout);
    let clean_stderr = filters::strip_ansi(stderr);

    // Stage 2: Collapse blank lines
    let collapsed_stdout = filters::collapse_blank_lines(&clean_stdout);
    let collapsed_stderr = filters::collapse_blank_lines(&clean_stderr);

    // Stage 3: Cap lines
    let (capped_stdout, stdout_capped) = filters::cap_lines(&collapsed_stdout, MAX_STDOUT_LINES);
    let (capped_stderr, stderr_capped) = filters::cap_lines(&collapsed_stderr, MAX_STDERR_LINES);

    // Stage 4: Cap bytes (safety net for extremely long lines)
    let (final_stdout, stdout_byte_capped) =
        filters::cap_bytes(&capped_stdout, filters::DEFAULT_MAX_BYTES);
    let (final_stderr, stderr_byte_capped) =
        filters::cap_bytes(&capped_stderr, filters::DEFAULT_MAX_BYTES);

    let any_change = stdout_capped
        || stderr_capped
        || stdout_byte_capped
        || stderr_byte_capped
        || final_stdout.len() != stdout.len()
        || final_stderr.len() != stderr.len();

    if !any_change {
        return CompressionResult {
            compressed: raw_result.to_string(),
            was_compressed: false,
        };
    }

    // Rebuild JSON with compressed fields
    let mut obj = parsed.clone();
    if let Some(map) = obj.as_object_mut() {
        map.insert(
            "stdout".to_string(),
            serde_json::Value::String(final_stdout),
        );
        map.insert(
            "stderr".to_string(),
            serde_json::Value::String(final_stderr),
        );
    }

    let compressed = serde_json::to_string(&obj).unwrap_or_else(|_| raw_result.to_string());

    CompressionResult {
        compressed,
        was_compressed: true,
    }
}

/// Fallback for non-JSON Bash-like output (e.g. from BashOutput).
fn compress_plain_text(text: &str) -> CompressionResult {
    let clean = filters::strip_ansi(text);
    let collapsed = filters::collapse_blank_lines(&clean);
    let (capped, line_capped) = filters::cap_lines(&collapsed, MAX_STDOUT_LINES);
    let (final_text, byte_capped) = filters::cap_bytes(&capped, filters::DEFAULT_MAX_BYTES);

    let any_change = line_capped || byte_capped || final_text.len() != text.len();

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
        serde_json::json!({
            "command": "ls -la",
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
    fn short_output_not_compressed() {
        let input = make_bash_json("hello world", "", 0);
        let result = compress(&input);
        assert!(!result.was_compressed);
    }

    #[test]
    fn ansi_stripped() {
        // Must exceed MIN_COMPRESS_LEN when serialized as JSON
        let noisy = format!(
            "\x1b[32m{}\x1b[0m",
            "a]long-line-of-text-with-color\n".repeat(100)
        );
        let input = make_bash_json(&noisy, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(!result.compressed.contains("\x1b["));
    }

    #[test]
    fn lines_capped() {
        let big_stdout = (0..500)
            .map(|i| format!("line {}", i))
            .collect::<Vec<_>>()
            .join("\n");
        let input = make_bash_json(&big_stdout, "", 0);
        let result = compress(&input);

        assert!(result.was_compressed);
        assert!(result.compressed.contains("lines omitted"));
    }

    #[test]
    fn blank_lines_collapsed() {
        // Must exceed MIN_COMPRESS_LEN when serialized as JSON
        let with_blanks = format!(
            "start-of-a-long-output-buffer\n{}\nend-of-long-output-buffer\n{}",
            "\n".repeat(20),
            "this-is-some-content-line-x\n".repeat(100)
        );
        let input = make_bash_json(&with_blanks, "", 0);
        let result = compress(&input);

        assert!(result.was_compressed);
        // Consecutive blanks should be collapsed
        assert!(!result.compressed.contains("\n\n\n\n"));
    }

    #[test]
    fn plain_text_fallback() {
        let big_text = "line\n".repeat(500);
        let result = compress(&big_text);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("lines omitted"));
    }

    #[test]
    fn byte_cap_safety() {
        // Create a line that's very long (no newlines)
        let long_line = "x".repeat(100_000);
        let input = make_bash_json(&long_line, "", 0);
        let result = compress(&input);

        assert!(result.was_compressed);
        assert!(result.compressed.len() < input.len());
    }

    #[test]
    fn stderr_also_capped() {
        let big_stderr = (0..200)
            .map(|i| format!("warning {}", i))
            .collect::<Vec<_>>()
            .join("\n");
        let input = make_bash_json("ok", &big_stderr, 0);
        let result = compress(&input);

        assert!(result.was_compressed);
        assert!(result.compressed.contains("lines omitted"));
    }
}
