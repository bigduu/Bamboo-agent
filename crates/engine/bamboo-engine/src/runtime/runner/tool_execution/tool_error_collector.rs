//! Collects tool execution errors (both hard errors and soft failures) into a
//! JSONL file for offline analysis and tool improvement.
//!
//! The file is written to `${BAMBOO_DATA_DIR}/tool_errors.jsonl`, one JSON
//! record per line. It is designed for append-only, fire-and-forget writes so
//! that it never blocks or fails the main agent loop.

use chrono::Utc;
use serde::Serialize;
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

/// Classifies the severity of a tool error.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolErrorKind {
    /// The tool executor threw an error (NotFound / Execution / InvalidArgs).
    Hard,
    /// The tool executed but returned `ToolResult { success: false, .. }`.
    Soft,
}

/// A single error record to be persisted.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ToolErrorRecord {
    /// ISO-8601 timestamp
    pub ts: String,
    /// Session that triggered the error
    pub session_id: String,
    /// Agent loop round number
    pub round: usize,
    /// Name of the tool that failed
    pub tool_name: String,
    /// Tool call ID (correlates with ToolStart / ToolError events)
    pub tool_call_id: String,
    /// Truncated preview of arguments (max 512 chars)
    pub args_preview: String,
    /// Hard error vs soft failure
    pub error_kind: ToolErrorKind,
    /// The error / failure message
    pub error_message: String,
    /// First 512 chars of the tool result (only for soft failures)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_snippet: Option<String>,
}

/// Maximum characters to keep for argument previews and result snippets.
const PREVIEW_MAX_CHARS: usize = 512;

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max).collect();
        format!("{truncated}...")
    }
}

fn error_preview(tool_name: &str, value: &str) -> String {
    let canonical = bamboo_domain::canonical_tool_name(tool_name);
    if canonical.eq_ignore_ascii_case("browser") || canonical.eq_ignore_ascii_case("browser_eval") {
        // Browser input, page scripts and failure text can contain page
        // data. Keep call metadata for correlation, not those payloads.
        "[redacted]".to_string()
    } else {
        truncate(value, PREVIEW_MAX_CHARS)
    }
}

/// Returns the path `${BAMBOO_DATA_DIR}/tool_errors.jsonl`.
fn error_log_path() -> PathBuf {
    bamboo_config::paths::bamboo_dir().join("tool_errors.jsonl")
}

/// Append a tool error record to the JSONL file.
///
/// This is fire-and-forget: if the write fails, it only logs a tracing warning
/// and never propagates the error back to the caller.
pub(crate) async fn append_tool_error(record: ToolErrorRecord) {
    let path = error_log_path();

    let line = match serde_json::to_string(&record) {
        Ok(json) => format!("{json}\n"),
        Err(e) => {
            tracing::warn!("Failed to serialize tool error record: {e}");
            return;
        }
    };

    match OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
    {
        Ok(mut file) => {
            if let Err(e) = file.write_all(line.as_bytes()).await {
                tracing::warn!(
                    "Failed to write tool error record to {}: {e}",
                    path.display()
                );
            }
        }
        Err(e) => {
            tracing::warn!("Failed to open tool error log {}: {e}", path.display());
        }
    }
}

/// Convenience builder for a hard error record.
pub(crate) fn hard_error_record(
    session_id: &str,
    round: usize,
    tool_name: &str,
    tool_call_id: &str,
    arguments: &str,
    error_message: &str,
) -> ToolErrorRecord {
    ToolErrorRecord {
        ts: Utc::now().to_rfc3339(),
        session_id: session_id.to_string(),
        round,
        tool_name: tool_name.to_string(),
        tool_call_id: tool_call_id.to_string(),
        args_preview: error_preview(tool_name, arguments),
        error_kind: ToolErrorKind::Hard,
        error_message: error_preview(tool_name, error_message),
        result_snippet: None,
    }
}

/// Convenience builder for a soft failure record.
pub(crate) fn soft_failure_record(
    session_id: &str,
    round: usize,
    tool_name: &str,
    tool_call_id: &str,
    arguments: &str,
    result_text: &str,
) -> ToolErrorRecord {
    ToolErrorRecord {
        ts: Utc::now().to_rfc3339(),
        session_id: session_id.to_string(),
        round,
        tool_name: tool_name.to_string(),
        tool_call_id: tool_call_id.to_string(),
        args_preview: error_preview(tool_name, arguments),
        error_kind: ToolErrorKind::Soft,
        error_message: error_preview(tool_name, result_text),
        result_snippet: Some(error_preview(tool_name, result_text)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_hard_and_soft_errors_keep_metadata_without_input_or_result_text() {
        for (tool_name, args) in [
            ("browser", r#"{"action":"type","text":"private input"}"#),
            (
                "browser_eval",
                r#"{"code":"private source","expected_url":"https://example.com/?token=private-query"}"#,
            ),
            (
                "default::browser_eval",
                r#"{"code":"private source","expected_url":"https://example.com/?token=private-query"}"#,
            ),
        ] {
            let hard = hard_error_record("session", 3, tool_name, "call", args, "private failure");
            let soft = soft_failure_record("session", 3, tool_name, "call", args, "private result");
            assert_eq!(soft.result_snippet.as_deref(), Some("[redacted]"));
            for record in [hard, soft] {
                let serialized = serde_json::to_string(&record).expect("JSON record");
                assert_eq!(record.tool_name, tool_name);
                assert_eq!(record.tool_call_id, "call");
                assert_eq!(record.args_preview, "[redacted]");
                assert_eq!(record.error_message, "[redacted]");
                assert!(!serialized.contains("private"));
            }
        }
        let ordinary = soft_failure_record("session", 3, "Bash", "call", "echo ok", "failed");
        assert_eq!(ordinary.args_preview, "echo ok");
        assert_eq!(ordinary.error_message, "failed");
        assert_eq!(ordinary.result_snippet.as_deref(), Some("failed"));
    }
}
