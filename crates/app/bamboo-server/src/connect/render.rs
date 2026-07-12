//! Renders a session's live [`AgentEvent`] stream into platform messages.
//!
//! MVP scope (issue #452): tool-use one-liners (`⚙ Bash: cargo test…`,
//! truncated) as they happen, plus the final assistant text once the run
//! reaches a terminal state. Streaming edit-in-place (capability-gated on
//! [`crate::connect::platform::Capabilities::edit_message`]) is a later
//! phase.
//!
//! [`stream_execution`] runs until the underlying broadcast stream reaches a
//! terminal event (`Complete`/`Cancelled`/`Error`) or is closed. The bridge
//! awaits it inline (not detached) so the run's completion doubles as this
//! task's completion — see `bridge::ConnectBridge::run_prompt`.

use std::sync::Arc;

use tokio::sync::broadcast;

use bamboo_agent_core::AgentEvent;

use super::platform::{OutboundMessage, Platform, ReplyCtx};

/// Telegram's hard message-length limit (in UTF-16 characters, but ASCII/most
/// text is 1 UTF-8 char == 1 unit; treating it as a char-count chunk size is a
/// safe, conservative approximation other adapters can share too).
pub const MAX_MESSAGE_CHARS: usize = 4096;

/// Longest a single tool-use one-liner is allowed to be before truncation,
/// well under [`MAX_MESSAGE_CHARS`] so a chatty tool call never dominates the
/// stream of updates.
const TOOL_LINE_MAX_CHARS: usize = 300;

/// Split `text` into chunks of at most `limit` **characters** (not bytes), so
/// a multi-byte UTF-8 sequence is never split mid-codepoint. Returns an empty
/// vec for empty input (callers should skip sending in that case).
pub fn chunk_message(text: &str, limit: usize) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    let chars: Vec<char> = text.chars().collect();
    chars
        .chunks(limit.max(1))
        .map(|chunk| chunk.iter().collect())
        .collect()
}

/// Truncate `text` to at most `max` characters, appending an ellipsis when cut.
fn truncate_chars(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max).collect();
    out.push('…');
    out
}

/// Best-effort one-line human summary of a tool call's arguments, used to
/// keep the one-liner scannable (`⚙ Bash: cargo test…`) instead of dumping
/// raw JSON.
fn summarize_arguments(arguments: &serde_json::Value) -> String {
    let Some(obj) = arguments.as_object() else {
        return arguments.to_string();
    };
    for key in ["command", "file_path", "path", "query", "pattern", "url"] {
        if let Some(value) = obj.get(key).and_then(|v| v.as_str()) {
            return value.to_string();
        }
    }
    serde_json::to_string(arguments).unwrap_or_default()
}

/// Formats a `ToolStart` event as a one-liner, truncated to
/// [`TOOL_LINE_MAX_CHARS`].
fn format_tool_line(tool_name: &str, arguments: &serde_json::Value) -> String {
    let summary = summarize_arguments(arguments);
    truncate_chars(&format!("⚙ {tool_name}: {summary}"), TOOL_LINE_MAX_CHARS)
}

async fn send_chunks(platform: &Arc<dyn Platform>, ctx: &ReplyCtx, text: &str) {
    for chunk in chunk_message(text, MAX_MESSAGE_CHARS) {
        if let Err(error) = platform.reply(ctx, OutboundMessage::text(chunk)).await {
            tracing::warn!("connect: failed to deliver reply: {error}");
        }
    }
}

/// Consume `rx` until a terminal `AgentEvent`, sending tool one-liners as
/// they arrive and the final assistant text (or error/cancellation note) once
/// the run ends. Returns once the stream reaches a terminal state or closes.
pub async fn stream_execution(
    platform: Arc<dyn Platform>,
    reply_ctx: ReplyCtx,
    mut rx: broadcast::Receiver<AgentEvent>,
) {
    let mut final_text = String::new();
    let mut terminal_note: Option<String> = None;

    loop {
        match rx.recv().await {
            Ok(AgentEvent::ToolStart {
                tool_name,
                arguments,
                ..
            }) => {
                let line = format_tool_line(&tool_name, &arguments);
                send_chunks(&platform, &reply_ctx, &line).await;
            }
            Ok(AgentEvent::Token { content }) => final_text.push_str(&content),
            Ok(AgentEvent::Complete { .. }) => break,
            Ok(AgentEvent::Cancelled { message }) => {
                terminal_note = Some(message.unwrap_or_else(|| "Cancelled.".to_string()));
                break;
            }
            Ok(AgentEvent::Error { message }) => {
                terminal_note = Some(format!("Error: {message}"));
                break;
            }
            Ok(_) => continue,
            // A slow reader missed some events; the run is still live, keep going.
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            // Sender dropped without a terminal event (should not normally
            // happen — treat as done rather than hanging forever).
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }

    let body = terminal_note.unwrap_or(final_text);
    if !body.trim().is_empty() {
        send_chunks(&platform, &reply_ctx, &body).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_message_splits_on_char_boundaries_not_bytes() {
        // Every char is 3 bytes in UTF-8 but 1 char; a byte-based chunker
        // would split mid-codepoint at a limit of 2.
        let text = "\u{4e2d}\u{6587}\u{6d4b}\u{8bd5}"; // 4 CJK chars, 12 bytes
        let chunks = chunk_message(text, 2);
        assert_eq!(chunks, vec!["\u{4e2d}\u{6587}", "\u{6d4b}\u{8bd5}"]);
    }

    #[test]
    fn chunk_message_respects_the_4096_limit() {
        let text = "a".repeat(10_000);
        let chunks = chunk_message(&text, MAX_MESSAGE_CHARS);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].chars().count(), MAX_MESSAGE_CHARS);
        assert_eq!(chunks[1].chars().count(), MAX_MESSAGE_CHARS);
        assert_eq!(chunks[2].chars().count(), 10_000 - 2 * MAX_MESSAGE_CHARS);
    }

    #[test]
    fn chunk_message_empty_text_yields_no_chunks() {
        assert!(chunk_message("", MAX_MESSAGE_CHARS).is_empty());
    }

    #[test]
    fn format_tool_line_prefers_command_field_and_truncates() {
        let args = serde_json::json!({ "command": "cargo test --workspace" });
        let line = format_tool_line("Bash", &args);
        assert_eq!(line, "⚙ Bash: cargo test --workspace");
    }

    #[test]
    fn format_tool_line_truncates_long_summaries() {
        let long_command = "x".repeat(1000);
        let args = serde_json::json!({ "command": long_command });
        let line = format_tool_line("Bash", &args);
        assert!(line.chars().count() <= TOOL_LINE_MAX_CHARS + 1);
        assert!(line.ends_with('…'));
    }

    #[test]
    fn format_tool_line_falls_back_to_json_for_unknown_shape() {
        let args = serde_json::json!({ "foo": "bar" });
        let line = format_tool_line("CustomTool", &args);
        assert!(line.starts_with("⚙ CustomTool: "));
        assert!(line.contains("foo"));
    }

    struct RecordingPlatform {
        sent: tokio::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Platform for RecordingPlatform {
        fn name(&self) -> &str {
            "recording"
        }
        fn capabilities(&self) -> super::super::platform::Capabilities {
            Default::default()
        }
        async fn start(
            &self,
            _inbound: tokio::sync::mpsc::Sender<super::super::platform::InboundMessage>,
        ) -> super::super::platform::PlatformResult<()> {
            Ok(())
        }
        async fn reply(
            &self,
            _ctx: &ReplyCtx,
            msg: OutboundMessage,
        ) -> super::super::platform::PlatformResult<super::super::platform::MessageRef> {
            self.sent.lock().await.push(msg.text);
            Ok(super::super::platform::MessageRef(serde_json::Value::Null))
        }
        async fn edit(
            &self,
            _msg_ref: &super::super::platform::MessageRef,
            _new: OutboundMessage,
        ) -> super::super::platform::PlatformResult<()> {
            Ok(())
        }
        async fn stop(&self) -> super::super::platform::PlatformResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn stream_execution_renders_tool_lines_and_final_text() {
        let (tx, rx) = broadcast::channel(16);
        let platform = Arc::new(RecordingPlatform {
            sent: tokio::sync::Mutex::new(Vec::new()),
        });
        let ctx = ReplyCtx(serde_json::json!({"chat_id": "1"}));

        tx.send(AgentEvent::ToolStart {
            tool_call_id: "1".to_string(),
            tool_name: "Bash".to_string(),
            arguments: serde_json::json!({ "command": "cargo test" }),
        })
        .unwrap();
        tx.send(AgentEvent::Token {
            content: "Hello ".to_string(),
        })
        .unwrap();
        tx.send(AgentEvent::Token {
            content: "world.".to_string(),
        })
        .unwrap();
        tx.send(AgentEvent::Complete {
            usage: bamboo_agent_core::TokenUsage {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: 2,
            },
        })
        .unwrap();

        stream_execution(platform.clone(), ctx, rx).await;

        let sent = platform.sent.lock().await;
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0], "⚙ Bash: cargo test");
        assert_eq!(sent[1], "Hello world.");
    }

    #[tokio::test]
    async fn stream_execution_renders_error_note_instead_of_partial_text() {
        let (tx, rx) = broadcast::channel(16);
        let platform = Arc::new(RecordingPlatform {
            sent: tokio::sync::Mutex::new(Vec::new()),
        });
        let ctx = ReplyCtx(serde_json::json!({"chat_id": "1"}));

        tx.send(AgentEvent::Token {
            content: "partial".to_string(),
        })
        .unwrap();
        tx.send(AgentEvent::Error {
            message: "boom".to_string(),
        })
        .unwrap();

        stream_execution(platform.clone(), ctx, rx).await;

        let sent = platform.sent.lock().await;
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0], "Error: boom");
    }

    #[tokio::test]
    async fn stream_execution_returns_when_channel_closes_without_terminal_event() {
        let (tx, rx) = broadcast::channel(16);
        let platform = Arc::new(RecordingPlatform {
            sent: tokio::sync::Mutex::new(Vec::new()),
        });
        let ctx = ReplyCtx(serde_json::json!({"chat_id": "1"}));
        drop(tx);

        // Must return promptly, not hang, when the sender is dropped.
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream_execution(platform.clone(), ctx, rx),
        )
        .await
        .expect("stream_execution must not hang on a closed channel");

        assert!(platform.sent.lock().await.is_empty());
    }
}
