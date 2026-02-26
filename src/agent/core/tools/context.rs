//! Execution context for tool calls.
//!
//! Tools normally return a single `ToolResult` after completion. Some tools
//! (for example, long-running CLIs) may want to stream intermediate progress
//! to clients. The agent loop passes a `ToolExecutionContext` that allows tools
//! to emit `AgentEvent`s while they run.

use tokio::sync::mpsc;

use crate::agent::core::AgentEvent;

/// Context passed to tools during execution.
///
/// All fields are optional and should be treated as best-effort hints.
#[derive(Clone, Copy, Debug)]
pub struct ToolExecutionContext<'a> {
    /// Bamboo session id that is executing the tool.
    pub session_id: Option<&'a str>,
    /// Tool call id from the model (`ToolCall.id`).
    pub tool_call_id: &'a str,
    /// Event sender for streaming progress to clients (agent SSE stream).
    pub event_tx: Option<&'a mpsc::Sender<AgentEvent>>,
}

impl<'a> ToolExecutionContext<'a> {
    pub fn none(tool_call_id: &'a str) -> Self {
        Self {
            session_id: None,
            tool_call_id,
            event_tx: None,
        }
    }

    /// Clone the sender (when present) for use in spawned tasks.
    pub fn cloned_sender(&self) -> Option<mpsc::Sender<AgentEvent>> {
        self.event_tx.cloned()
    }

    /// Best-effort emit of an event (ignored if no sender).
    pub async fn emit(&self, event: AgentEvent) {
        if let Some(tx) = self.event_tx {
            // Tools sometimes want to stream incremental output. Historically they emitted
            // `AgentEvent::Token`, but that mixes tool output into the assistant stream.
            // When emitting from a tool context, treat `Token` as tool-scoped output.
            let event = match event {
                AgentEvent::Token { content } => AgentEvent::ToolToken {
                    tool_call_id: self.tool_call_id.to_string(),
                    content,
                },
                other => other,
            };
            let _ = tx.send(event).await;
        }
    }

    /// Convenience helper for streaming tool-scoped output.
    pub async fn emit_tool_token(&self, content: impl Into<String>) {
        self.emit(AgentEvent::ToolToken {
            tool_call_id: self.tool_call_id.to_string(),
            content: content.into(),
        })
        .await;
    }
}
