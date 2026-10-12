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
    if canonical.eq_ignore_ascii_case("browser")
        || canonical.eq_ignore_ascii_case("browser_eval")
        || canonical.eq_ignore_ascii_case("session_message")
    {
        // Browser and peer arguments, results and failure text can contain
        // private content. Keep correlation metadata, not those payloads.
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
    append_tool_error_to_path(record, error_log_path()).await;
}

async fn append_tool_error_to_path(record: ToolErrorRecord, path: PathBuf) {
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
            // write_all may only hand bytes to Tokio's blocking filesystem
            // worker. A completed Agent must not leave this write outstanding.
            if let Err(e) = file.flush().await {
                tracing::warn!("Failed to flush tool error log {}: {e}", path.display());
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

    #[tokio::test]
    async fn peer_hard_and_soft_error_jsonl_redacts_message_and_routing() {
        let home = tempfile::tempdir().expect("isolated collector directory");
        let path = home.path().join("tool_errors.jsonl");
        let body = "synthetic-peer-private-body";
        let target = "synthetic-peer-private-target";
        let reply_to = "synthetic-peer-private-reply";
        let operation = "synthetic-peer-private-operation";
        let failure = format!("synthetic-peer-private-failure {body} {target}");
        let cases = [
            ("session_message", "send", false),
            ("session_message", "reply", false),
            ("SESSION_MESSAGE", "send", false),
            ("default::session_message", "reply", false),
            ("default::SESSION_MESSAGE", "send", true),
            ("session_message", "reply", true),
        ];
        for (tool_name, action, malformed) in cases {
            let mut arguments = serde_json::json!({
                "action": action,
                "operation_id": operation,
                "message": body,
            });
            if action == "send" {
                arguments["target_session_id"] = target.into();
            } else {
                arguments["reply_to"] = reply_to.into();
            }
            let mut arguments = arguments.to_string();
            if malformed {
                arguments.pop();
                assert!(serde_json::from_str::<serde_json::Value>(&arguments).is_err());
            }
            for record in [
                hard_error_record(
                    "collector-session",
                    3,
                    tool_name,
                    "collector-call",
                    &arguments,
                    &failure,
                ),
                soft_failure_record(
                    "collector-session",
                    3,
                    tool_name,
                    "collector-call",
                    &arguments,
                    &failure,
                ),
            ] {
                append_tool_error_to_path(record, path.clone()).await;
            }
        }

        let serialized = tokio::fs::read_to_string(&path)
            .await
            .expect("actual JSONL records");
        for sentinel in [
            body,
            target,
            reply_to,
            operation,
            "synthetic-peer-private-failure",
        ] {
            assert!(
                !serialized.contains(sentinel),
                "collector persisted private marker {sentinel}"
            );
        }
        let records: Vec<serde_json::Value> = serialized
            .lines()
            .map(|line| serde_json::from_str(line).expect("serialized collector record"))
            .collect();
        assert_eq!(records.len(), cases.len() * 2);
        for (index, record) in records.iter().enumerate() {
            assert_eq!(record["session_id"], "collector-session");
            assert_eq!(record["round"], 3);
            assert_eq!(record["tool_name"], cases[index / 2].0);
            assert_eq!(record["tool_call_id"], "collector-call");
            chrono::DateTime::parse_from_rfc3339(record["ts"].as_str().expect("record timestamp"))
                .expect("existing timestamp format");
            assert_eq!(record["args_preview"], "[redacted]");
            assert_eq!(record["error_message"], "[redacted]");
            if index % 2 == 0 {
                assert_eq!(record["error_kind"], "hard");
                assert!(record.get("result_snippet").is_none());
            } else {
                assert_eq!(record["error_kind"], "soft");
                assert_eq!(record["result_snippet"], "[redacted]");
            }
        }
    }

    #[tokio::test]
    async fn ordinary_and_browser_error_jsonl_keep_existing_policy() {
        let home = tempfile::tempdir().expect("isolated collector directory");
        let path = home.path().join("tool_errors.jsonl");
        let arguments = "界".repeat(PREVIEW_MAX_CHARS + 1);
        let expected_preview = format!("{}...", "界".repeat(PREVIEW_MAX_CHARS));
        for record in [
            hard_error_record("session", 2, "Bash", "call", &arguments, "ordinary failure"),
            soft_failure_record("session", 2, "Bash", "call", &arguments, "ordinary failure"),
        ] {
            append_tool_error_to_path(record, path.clone()).await;
        }
        for tool_name in ["browser", "BROWSER", "default::browser_eval"] {
            for record in [
                hard_error_record(
                    "session",
                    2,
                    tool_name,
                    "call",
                    "private browser input",
                    "private browser error",
                ),
                soft_failure_record(
                    "session",
                    2,
                    tool_name,
                    "call",
                    "private browser input",
                    "private browser result",
                ),
            ] {
                append_tool_error_to_path(record, path.clone()).await;
            }
        }
        let serialized = tokio::fs::read_to_string(&path)
            .await
            .expect("actual JSONL records");
        assert!(!serialized.contains("private browser"));
        let records: Vec<serde_json::Value> = serialized
            .lines()
            .map(|line| serde_json::from_str(line).expect("serialized collector record"))
            .collect();
        assert_eq!(records.len(), 8);
        for (index, record) in records.iter().enumerate() {
            assert_eq!(
                record["error_kind"],
                if index % 2 == 0 { "hard" } else { "soft" }
            );
            if index < 2 {
                assert_eq!(record["args_preview"], expected_preview);
                assert_eq!(record["error_message"], "ordinary failure");
                assert_eq!(
                    record
                        .get("result_snippet")
                        .and_then(serde_json::Value::as_str),
                    (index == 1).then_some("ordinary failure")
                );
            } else {
                assert_eq!(record["args_preview"], "[redacted]");
                assert_eq!(record["error_message"], "[redacted]");
                assert_eq!(
                    record
                        .get("result_snippet")
                        .and_then(serde_json::Value::as_str),
                    (index % 2 == 1).then_some("[redacted]")
                );
            }
        }
    }

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
