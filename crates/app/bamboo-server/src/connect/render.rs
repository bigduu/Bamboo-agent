//! Renders a session's live [`AgentEvent`] stream into platform messages.
//!
//! Two rendering modes, chosen from `platform.capabilities().edit_message`:
//! - **Legacy** (issue #452 MVP): tool-use one-liners and the final assistant
//!   text are each sent as separate messages.
//! - **Streaming edit-in-place** (issue #458 phase 2): one status message per
//!   run, throttled-edited as tool lines/tokens arrive, replaced by a ✅/❌/⏹
//!   final edit (or a short "done" edit + chunked follow-up when the final
//!   text doesn't fit a single message).
//!
//! Both modes stop at the same three terminal `AgentEvent`s
//! (`Complete`/`Cancelled`/`Error`) — the phase-1 termination contract — and
//! BOTH now also stop at `AgentEvent::NeedClarification`, returning
//! [`RunOutcome::Paused`] instead of continuing to wait for a terminal event
//! that will never come while the run is genuinely suspended on a pending
//! question. The bridge (`bridge::ConnectBridge::render_until_settled`) is
//! responsible for turning a `Paused` outcome into a rendered ask
//! (`connect::approvals`) and, once answered, calling `stream_execution`
//! again on the resumed run's stream.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::time::Instant;

use bamboo_agent_core::AgentEvent;

use super::platform::{MessageRef, OutboundMessage, Platform, ReplyCtx};

/// Telegram's hard message-length limit (in UTF-16 characters, but ASCII/most
/// text is 1 UTF-8 char == 1 unit; treating it as a char-count chunk size is a
/// safe, conservative approximation other adapters can share too).
pub const MAX_MESSAGE_CHARS: usize = 4096;

/// Longest a single tool-use one-liner is allowed to be before truncation,
/// well under [`MAX_MESSAGE_CHARS`] so a chatty tool call never dominates the
/// stream of updates.
const TOOL_LINE_MAX_CHARS: usize = 300;

/// Minimum time between throttled status-message edits (cc-connect-tuned,
/// issue #458).
const EDIT_MIN_INTERVAL: Duration = Duration::from_millis(1500);
/// Minimum new characters accumulated before a throttled edit fires, ANDed
/// with [`EDIT_MIN_INTERVAL`] (issue #458).
const EDIT_MIN_NEW_CHARS: usize = 30;

/// What a live run's event stream settled into.
#[derive(Debug)]
pub enum RunOutcome {
    /// Reached a terminal `AgentEvent` (or the stream closed) — nothing more
    /// to render for this run.
    Terminal,
    /// Paused on `AgentEvent::NeedClarification` — a human decision is now
    /// required before the run can continue.
    Paused(PendingAsk),
}

/// The pause-worthy subset of `AgentEvent::NeedClarification`'s fields,
/// decoupled from the wire event so `connect::approvals` doesn't need to
/// match on `AgentEvent` itself.
#[derive(Debug, Clone)]
pub struct PendingAsk {
    pub tool_call_id: String,
    pub tool_name: String,
    pub question: String,
    pub options: Vec<String>,
    pub allow_custom: bool,
}

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

/// Keep the LAST `max` characters of `text` (a "tail-keep" truncation, as
/// opposed to [`truncate_chars`]'s head-keep) — used for the rolling
/// streaming-edit body so a long run's most RECENT progress stays visible
/// instead of getting stuck showing only the earliest lines.
fn tail_chars(text: &str, max: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max {
        return text.to_string();
    }
    let start = chars.len() - max;
    chars[start..].iter().collect()
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

fn pending_ask_from_event(
    question: String,
    options: Option<Vec<String>>,
    tool_call_id: Option<String>,
    tool_name: Option<String>,
    allow_custom: bool,
) -> PendingAsk {
    PendingAsk {
        tool_call_id: tool_call_id.unwrap_or_default(),
        tool_name: tool_name.unwrap_or_default(),
        question,
        options: options.unwrap_or_default(),
        allow_custom,
    }
}

/// Consume `rx` until a terminal `AgentEvent` or a pause, rendering into
/// `platform` as it goes. Dispatches to the streaming edit-in-place mode when
/// `platform.capabilities().edit_message`, else the legacy per-message mode
/// (issue #452's original behavior, preserved verbatim for adapters that
/// can't edit).
pub async fn stream_execution(
    platform: Arc<dyn Platform>,
    reply_ctx: ReplyCtx,
    rx: broadcast::Receiver<AgentEvent>,
) -> RunOutcome {
    if platform.capabilities().edit_message {
        stream_execution_streaming(platform, reply_ctx, rx).await
    } else {
        stream_execution_legacy(platform, reply_ctx, rx).await
    }
}

/// Legacy (issue #452) rendering: each tool one-liner and the final text (or
/// error/cancellation note) is sent as its own message. Returns
/// [`RunOutcome::Paused`] on `NeedClarification` instead of the old
/// (pre-#458) behavior of silently ignoring it and waiting forever.
async fn stream_execution_legacy(
    platform: Arc<dyn Platform>,
    reply_ctx: ReplyCtx,
    mut rx: broadcast::Receiver<AgentEvent>,
) -> RunOutcome {
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
            Ok(AgentEvent::NeedClarification {
                question,
                options,
                tool_call_id,
                tool_name,
                allow_custom,
            }) => {
                return RunOutcome::Paused(pending_ask_from_event(
                    question,
                    options,
                    tool_call_id,
                    tool_name,
                    allow_custom,
                ));
            }
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
    RunOutcome::Terminal
}

/// Accumulated state for the streaming edit-in-place renderer, plus the
/// throttle/edit-degrade machinery (issue #458 §B).
struct StreamingRenderer {
    platform: Arc<dyn Platform>,
    reply_ctx: ReplyCtx,
    tool_lines: Vec<String>,
    assistant_text: String,
    status_ref: Option<MessageRef>,
    last_edit_at: Option<Instant>,
    chars_since_edit: usize,
}

impl StreamingRenderer {
    fn new(platform: Arc<dyn Platform>, reply_ctx: ReplyCtx) -> Self {
        Self {
            platform,
            reply_ctx,
            tool_lines: Vec::new(),
            assistant_text: String::new(),
            status_ref: None,
            last_edit_at: None,
            chars_since_edit: 0,
        }
    }

    async fn send_initial(&mut self) {
        match self
            .platform
            .reply(&self.reply_ctx, OutboundMessage::text("⏳ Working…"))
            .await
        {
            Ok(msg_ref) => self.status_ref = Some(msg_ref),
            Err(error) => {
                tracing::warn!("connect: failed to send initial status message: {error}")
            }
        }
    }

    /// Full body (tool lines + assistant text), untruncated — used for the
    /// final "does it fit in one message" check.
    fn full_body(&self) -> String {
        let mut body = String::new();
        for line in &self.tool_lines {
            body.push_str(line);
            body.push('\n');
        }
        if !self.assistant_text.is_empty() {
            if !body.is_empty() {
                body.push('\n');
            }
            body.push_str(&self.assistant_text);
        }
        body
    }

    /// Rolling display body, tail-truncated to [`MAX_MESSAGE_CHARS`] so the
    /// most recent progress always stays visible in the status message.
    fn display_tail(&self) -> String {
        tail_chars(&self.full_body(), MAX_MESSAGE_CHARS)
    }

    /// Record `added_chars` of new content and fire a throttled edit if both
    /// the interval and char-count thresholds are met.
    async fn note_growth(&mut self, added_chars: usize) {
        self.chars_since_edit += added_chars;
        let now = Instant::now();
        let interval_ok = self
            .last_edit_at
            .map(|at| now.duration_since(at) >= EDIT_MIN_INTERVAL)
            .unwrap_or(true);
        if !interval_ok || self.chars_since_edit < EDIT_MIN_NEW_CHARS {
            return;
        }
        let text = self.display_tail();
        self.apply_edit(text).await;
        self.last_edit_at = Some(now);
        self.chars_since_edit = 0;
    }

    /// Apply an edit unconditionally (bypassing the throttle) — used for
    /// terminal/pause renders where the final content must land regardless of
    /// timing. Degrades to a fresh `reply()` when there's no status message
    /// yet, or when the edit itself fails (message too old / unchanged
    /// content / any other 400) — an edit failure must never fail the run.
    async fn apply_edit(&mut self, text: String) {
        if text.trim().is_empty() {
            return;
        }
        let Some(msg_ref) = self.status_ref.clone() else {
            match self
                .platform
                .reply(&self.reply_ctx, OutboundMessage::text(text))
                .await
            {
                Ok(new_ref) => self.status_ref = Some(new_ref),
                Err(error) => {
                    tracing::warn!("connect: failed to send status message: {error}")
                }
            }
            return;
        };
        if let Err(error) = self
            .platform
            .edit(&msg_ref, OutboundMessage::text(text.clone()))
            .await
        {
            tracing::warn!("connect: status edit failed, degrading to a fresh message: {error}");
            match self
                .platform
                .reply(&self.reply_ctx, OutboundMessage::text(text))
                .await
            {
                Ok(new_ref) => self.status_ref = Some(new_ref),
                Err(error) => tracing::warn!("connect: fallback send also failed: {error}"),
            }
        }
    }

    /// Final render on success: the completed status message becomes "✅ " +
    /// the full text when it fits in one message; otherwise a short "✅ done"
    /// edit plus the full result sent as fresh chunked messages (issue #458
    /// §B point 5).
    async fn finalize_success(&mut self) {
        let full = self.full_body();
        if full.trim().is_empty() {
            self.apply_edit("✅ Done.".to_string()).await;
            return;
        }
        if full.chars().count() <= MAX_MESSAGE_CHARS {
            self.apply_edit(format!("✅ {full}")).await;
        } else {
            self.apply_edit("✅ done".to_string()).await;
            send_chunks(&self.platform, &self.reply_ctx, &full).await;
        }
    }

    /// Final render on error/cancel: `icon` + `note`, replacing whatever
    /// partial progress the status message was showing.
    async fn finalize_terminal_note(&mut self, icon: &str, note: &str) {
        self.apply_edit(format!("{icon} {note}")).await;
    }

    /// Courtesy edit marking the status message as paused, before the ask
    /// itself is rendered as a separate message by `connect::approvals`.
    async fn finalize_paused(&mut self) {
        let mut text = self.display_tail();
        if !text.is_empty() {
            text.push_str("\n\n");
        }
        text.push_str("⏸ Waiting for your input…");
        self.apply_edit(text).await;
    }
}

/// Streaming edit-in-place (issue #458 §B) rendering mode.
async fn stream_execution_streaming(
    platform: Arc<dyn Platform>,
    reply_ctx: ReplyCtx,
    mut rx: broadcast::Receiver<AgentEvent>,
) -> RunOutcome {
    let mut renderer = StreamingRenderer::new(platform, reply_ctx);
    renderer.send_initial().await;

    loop {
        match rx.recv().await {
            Ok(AgentEvent::ToolStart {
                tool_name,
                arguments,
                ..
            }) => {
                let line = format_tool_line(&tool_name, &arguments);
                let added = line.chars().count();
                renderer.tool_lines.push(line);
                renderer.note_growth(added).await;
            }
            Ok(AgentEvent::Token { content }) => {
                let added = content.chars().count();
                renderer.assistant_text.push_str(&content);
                renderer.note_growth(added).await;
            }
            Ok(AgentEvent::NeedClarification {
                question,
                options,
                tool_call_id,
                tool_name,
                allow_custom,
            }) => {
                renderer.finalize_paused().await;
                return RunOutcome::Paused(pending_ask_from_event(
                    question,
                    options,
                    tool_call_id,
                    tool_name,
                    allow_custom,
                ));
            }
            Ok(AgentEvent::Complete { .. }) => {
                renderer.finalize_success().await;
                return RunOutcome::Terminal;
            }
            Ok(AgentEvent::Cancelled { message }) => {
                renderer
                    .finalize_terminal_note(
                        "⏹",
                        &message.unwrap_or_else(|| "Cancelled.".to_string()),
                    )
                    .await;
                return RunOutcome::Terminal;
            }
            Ok(AgentEvent::Error { message }) => {
                renderer.finalize_terminal_note("❌ Error:", &message).await;
                return RunOutcome::Terminal;
            }
            Ok(_) => continue,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            // Sender dropped without a terminal event — matches the legacy
            // mode's "treat as done, never hang" contract. Leave whatever
            // partial status message is showing rather than editing it (no
            // reliable terminal state to report).
            Err(broadcast::error::RecvError::Closed) => return RunOutcome::Terminal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::platform::{Capabilities, InboundMessage};

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
    fn tail_chars_keeps_the_last_n_characters() {
        let text = "0123456789";
        assert_eq!(tail_chars(text, 4), "6789");
        assert_eq!(tail_chars(text, 100), text);
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

    /// Records every `reply()`/`edit()` call. `edit_message` capability is
    /// controlled by a constructor flag so the same fake drives both render
    /// modes' tests.
    struct RecordingPlatform {
        edit_message: bool,
        sent: tokio::sync::Mutex<Vec<String>>,
        edits: tokio::sync::Mutex<Vec<String>>,
        edit_should_fail: std::sync::atomic::AtomicBool,
    }

    impl RecordingPlatform {
        fn new(edit_message: bool) -> Arc<Self> {
            Arc::new(Self {
                edit_message,
                sent: tokio::sync::Mutex::new(Vec::new()),
                edits: tokio::sync::Mutex::new(Vec::new()),
                edit_should_fail: std::sync::atomic::AtomicBool::new(false),
            })
        }
    }

    #[async_trait::async_trait]
    impl Platform for RecordingPlatform {
        fn name(&self) -> &str {
            "recording"
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities {
                buttons: false,
                edit_message: self.edit_message,
                images: false,
                files: false,
            }
        }
        async fn start(
            &self,
            _inbound: tokio::sync::mpsc::Sender<super::super::platform::Inbound>,
        ) -> super::super::platform::PlatformResult<()> {
            Ok(())
        }
        async fn reply(
            &self,
            _ctx: &ReplyCtx,
            msg: OutboundMessage,
        ) -> super::super::platform::PlatformResult<super::super::platform::MessageRef> {
            self.sent.lock().await.push(msg.text);
            Ok(super::super::platform::MessageRef(serde_json::json!({
                "id": self.sent.lock().await.len()
            })))
        }
        async fn edit(
            &self,
            _msg_ref: &super::super::platform::MessageRef,
            new: OutboundMessage,
        ) -> super::super::platform::PlatformResult<()> {
            if self
                .edit_should_fail
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(super::super::platform::PlatformError::other("edit failed"));
            }
            self.edits.lock().await.push(new.text);
            Ok(())
        }
        async fn stop(&self) -> super::super::platform::PlatformResult<()> {
            Ok(())
        }
    }

    fn ask_event(question: &str, options: Vec<&str>, allow_custom: bool) -> AgentEvent {
        AgentEvent::NeedClarification {
            question: question.to_string(),
            options: Some(options.into_iter().map(str::to_string).collect()),
            tool_call_id: Some("call-1".to_string()),
            tool_name: Some("conclusion_with_options".to_string()),
            allow_custom,
        }
    }

    // ---- Legacy mode (edit_message = false) ----

    #[tokio::test]
    async fn stream_execution_renders_tool_lines_and_final_text() {
        let (tx, rx) = broadcast::channel(16);
        let platform = RecordingPlatform::new(false);
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

        let outcome = stream_execution(platform.clone() as Arc<dyn Platform>, ctx, rx).await;
        assert!(matches!(outcome, RunOutcome::Terminal));

        let sent = platform.sent.lock().await;
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0], "⚙ Bash: cargo test");
        assert_eq!(sent[1], "Hello world.");
    }

    #[tokio::test]
    async fn stream_execution_renders_error_note_instead_of_partial_text() {
        let (tx, rx) = broadcast::channel(16);
        let platform = RecordingPlatform::new(false);
        let ctx = ReplyCtx(serde_json::json!({"chat_id": "1"}));

        tx.send(AgentEvent::Token {
            content: "partial".to_string(),
        })
        .unwrap();
        tx.send(AgentEvent::Error {
            message: "boom".to_string(),
        })
        .unwrap();

        stream_execution(platform.clone() as Arc<dyn Platform>, ctx, rx).await;

        let sent = platform.sent.lock().await;
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0], "Error: boom");
    }

    #[tokio::test]
    async fn stream_execution_returns_when_channel_closes_without_terminal_event() {
        let (tx, rx) = broadcast::channel(16);
        let platform = RecordingPlatform::new(false);
        let ctx = ReplyCtx(serde_json::json!({"chat_id": "1"}));
        drop(tx);

        // Must return promptly, not hang, when the sender is dropped.
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream_execution(platform.clone() as Arc<dyn Platform>, ctx, rx),
        )
        .await
        .expect("stream_execution must not hang on a closed channel");

        assert!(platform.sent.lock().await.is_empty());
    }

    #[tokio::test]
    async fn stream_execution_legacy_pauses_on_need_clarification() {
        let (tx, rx) = broadcast::channel(16);
        let platform = RecordingPlatform::new(false);
        let ctx = ReplyCtx(serde_json::json!({"chat_id": "1"}));

        tx.send(ask_event("Pick one", vec!["A", "B"], false))
            .unwrap();

        let outcome = stream_execution(platform.clone() as Arc<dyn Platform>, ctx, rx).await;
        match outcome {
            RunOutcome::Paused(ask) => {
                assert_eq!(ask.question, "Pick one");
                assert_eq!(ask.options, vec!["A".to_string(), "B".to_string()]);
                assert_eq!(ask.tool_call_id, "call-1");
                assert!(!ask.allow_custom);
            }
            RunOutcome::Terminal => panic!("expected Paused"),
        }
        // No terminal note sent — the ask itself is rendered by the bridge,
        // not by render.rs.
        assert!(platform.sent.lock().await.is_empty());
    }

    // ---- Streaming edit-in-place mode (edit_message = true) ----

    #[tokio::test]
    async fn streaming_mode_sends_one_initial_status_message() {
        let (_tx, rx) = broadcast::channel(16);
        let platform = RecordingPlatform::new(true);
        let ctx = ReplyCtx(serde_json::json!({"chat_id": "1"}));
        drop(_tx);

        stream_execution(platform.clone() as Arc<dyn Platform>, ctx, rx).await;

        let sent = platform.sent.lock().await;
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0], "⏳ Working…");
    }

    #[tokio::test]
    async fn streaming_mode_final_success_edits_status_with_checkmark() {
        let (tx, rx) = broadcast::channel(16);
        let platform = RecordingPlatform::new(true);
        let ctx = ReplyCtx(serde_json::json!({"chat_id": "1"}));

        tx.send(AgentEvent::Token {
            content: "All done.".to_string(),
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

        let outcome = stream_execution(platform.clone() as Arc<dyn Platform>, ctx, rx).await;
        assert!(matches!(outcome, RunOutcome::Terminal));

        // The 9-char token is below the 30-char throttle, so no mid-run edit
        // fires — only the unconditional final edit.
        let edits = platform.edits.lock().await;
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0], "✅ All done.");
        // Never sent as a separate chunked message (it fit in the edit).
        assert_eq!(platform.sent.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn streaming_mode_final_error_edits_status_with_cross() {
        let (tx, rx) = broadcast::channel(16);
        let platform = RecordingPlatform::new(true);
        let ctx = ReplyCtx(serde_json::json!({"chat_id": "1"}));

        tx.send(AgentEvent::Error {
            message: "boom".to_string(),
        })
        .unwrap();

        stream_execution(platform.clone() as Arc<dyn Platform>, ctx, rx).await;

        let edits = platform.edits.lock().await;
        assert_eq!(edits.last().unwrap(), "❌ Error: boom");
    }

    #[tokio::test]
    async fn streaming_mode_final_cancel_edits_status_with_stop_icon() {
        let (tx, rx) = broadcast::channel(16);
        let platform = RecordingPlatform::new(true);
        let ctx = ReplyCtx(serde_json::json!({"chat_id": "1"}));

        tx.send(AgentEvent::Cancelled {
            message: Some("user requested /stop".to_string()),
        })
        .unwrap();

        stream_execution(platform.clone() as Arc<dyn Platform>, ctx, rx).await;

        let edits = platform.edits.lock().await;
        assert_eq!(edits.last().unwrap(), "⏹ user requested /stop");
    }

    #[tokio::test]
    async fn streaming_mode_pauses_on_need_clarification_with_courtesy_edit() {
        let (tx, rx) = broadcast::channel(16);
        let platform = RecordingPlatform::new(true);
        let ctx = ReplyCtx(serde_json::json!({"chat_id": "1"}));

        tx.send(AgentEvent::Token {
            content: "Working on it".to_string(),
        })
        .unwrap();
        tx.send(ask_event("Approve?", vec!["Approve", "Deny"], false))
            .unwrap();

        let outcome = stream_execution(platform.clone() as Arc<dyn Platform>, ctx, rx).await;
        match outcome {
            RunOutcome::Paused(ask) => assert_eq!(ask.question, "Approve?"),
            RunOutcome::Terminal => panic!("expected Paused"),
        }

        let edits = platform.edits.lock().await;
        assert!(edits.last().unwrap().contains("Waiting for your input"));
    }

    #[tokio::test]
    async fn streaming_mode_throttle_skips_edits_below_the_char_threshold() {
        let (tx, rx) = broadcast::channel(16);
        let platform = RecordingPlatform::new(true);
        let ctx = ReplyCtx(serde_json::json!({"chat_id": "1"}));

        // Each token is well under the 30-char threshold; none should trigger
        // a mid-run edit before the terminal event's unconditional edit.
        for i in 0..5 {
            tx.send(AgentEvent::Token {
                content: format!("t{i} "),
            })
            .unwrap();
        }
        tx.send(AgentEvent::Complete {
            usage: bamboo_agent_core::TokenUsage {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: 2,
            },
        })
        .unwrap();

        stream_execution(platform.clone() as Arc<dyn Platform>, ctx, rx).await;

        // Exactly one edit: the final one.
        assert_eq!(platform.edits.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn streaming_mode_long_final_text_chunks_instead_of_editing_in_full() {
        let (tx, rx) = broadcast::channel(16);
        let platform = RecordingPlatform::new(true);
        let ctx = ReplyCtx(serde_json::json!({"chat_id": "1"}));

        let long_text = "a".repeat(5000);
        tx.send(AgentEvent::Token {
            content: long_text.clone(),
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

        stream_execution(platform.clone() as Arc<dyn Platform>, ctx, rx).await;

        // Final edit is the short "done" marker, not the full 5000 chars.
        let edits = platform.edits.lock().await;
        assert_eq!(edits.last().unwrap(), "✅ done");
        // The full text was chunk-sent as fresh messages instead (2 chunks at
        // 4096 + initial status message = 3 sends total).
        let sent = platform.sent.lock().await;
        assert_eq!(sent.len(), 3);
    }

    #[tokio::test]
    async fn streaming_mode_edit_failure_degrades_to_a_fresh_send() {
        let (tx, rx) = broadcast::channel(16);
        let platform = RecordingPlatform::new(true);
        platform
            .edit_should_fail
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let ctx = ReplyCtx(serde_json::json!({"chat_id": "1"}));

        tx.send(AgentEvent::Complete {
            usage: bamboo_agent_core::TokenUsage {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: 2,
            },
        })
        .unwrap();

        let outcome = stream_execution(platform.clone() as Arc<dyn Platform>, ctx, rx).await;
        assert!(matches!(outcome, RunOutcome::Terminal));

        // No successful edits recorded (they all failed) — but the run never
        // errors out; it degrades to sending a fresh message instead.
        assert!(platform.edits.lock().await.is_empty());
        // Initial status + degraded final send.
        assert_eq!(platform.sent.lock().await.len(), 2);
    }

    // Sanity: `InboundMessage` remains constructible with the same shape used
    // elsewhere in the module (guards against an accidental field drift when
    // `Inbound`/`CallbackQuery` were added alongside it).
    #[test]
    fn inbound_message_is_still_constructible() {
        let _ = InboundMessage {
            platform: "telegram".to_string(),
            chat_id: "1".to_string(),
            user_id: "1".to_string(),
            message_id: "1".to_string(),
            sent_at: chrono::Utc::now(),
            text: "hi".to_string(),
            reply_ctx: ReplyCtx(serde_json::Value::Null),
        };
    }
}
