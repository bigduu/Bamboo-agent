//! Execution context for tool calls.
//!
//! Tools normally return a single `ToolResult` after completion. Some tools
//! (for example, long-running CLIs) may want to stream intermediate progress
//! to clients. The agent loop passes a `ToolExecutionContext` that allows tools
//! to emit `AgentEvent`s while they run.

use tokio::sync::mpsc;

use crate::tools::ToolSchema;
use crate::{AgentEvent, Session};

/// Per-session flags that flow into every tool call's [`ToolExecutionContext`].
///
/// These are derived ONCE from the executing [`Session`] (via
/// [`ToolExecutionSessionFlags::from_session`]) and copied into the context. To
/// add a new per-session execution flag, add a field here, derive it in
/// `from_session`, and map it in [`ToolExecutionContext::for_dispatch`]. Because
/// both agent loops build their context through `for_dispatch`, a new flag
/// reaches every dispatch path automatically — it can't be wired into one loop
/// and silently skipped in the other.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ToolExecutionSessionFlags {
    /// When `true`, the session is in "bypass permissions" mode and tool
    /// permission checks are skipped. Sourced from the session's runtime state.
    pub bypass_permissions: bool,
}

impl ToolExecutionSessionFlags {
    /// Derive the per-session tool-execution flags from a session's runtime
    /// state. This is the single source of truth for both agent loops.
    pub fn from_session(session: &Session) -> Self {
        Self {
            bypass_permissions: session
                .agent_runtime_state
                .as_ref()
                .is_some_and(|state| state.bypass_permissions),
        }
    }
}

/// Context passed to tools during execution.
///
/// All fields are optional and should be treated as best-effort hints.
///
/// ⚠️ Real tool dispatch must build this via [`ToolExecutionContext::for_dispatch`]
/// (both agent loops do), NOT a struct literal — that routes every per-session
/// flag through [`ToolExecutionSessionFlags`] so a new flag can't be wired into
/// one loop and silently skipped in the other. Struct literals are for tests
/// and tools that synthesize a child context.
#[derive(Clone, Copy, Debug)]
pub struct ToolExecutionContext<'a> {
    /// Bamboo session id that is executing the tool.
    pub session_id: Option<&'a str>,
    /// Tool call id from the model (`ToolCall.id`).
    pub tool_call_id: &'a str,
    /// Event sender for streaming progress to clients (agent SSE stream).
    pub event_tx: Option<&'a mpsc::Sender<AgentEvent>>,
    /// Snapshot of tools currently available to the executing session.
    pub available_tool_schemas: Option<&'a [ToolSchema]>,
    /// When `true`, the executing session is in "bypass permissions" mode, so
    /// tool permission checks are skipped. Sourced per-session from the
    /// session's runtime state (`runtime.json`), not the global checker.
    pub bypass_permissions: bool,
}

impl<'a> ToolExecutionContext<'a> {
    pub fn none(tool_call_id: &'a str) -> Self {
        Self {
            session_id: None,
            tool_call_id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
        }
    }

    /// Build a context for a real tool dispatch, applying every per-session flag
    /// from [`ToolExecutionSessionFlags`]. This is the SINGLE place that maps
    /// session flags onto the context, and the only constructor the agent loops
    /// use — keep both loops (`per_call.rs`, `result_handler.rs`) on it so a new
    /// per-session field reaches all dispatch paths without per-site edits.
    pub fn for_dispatch(
        session_id: &'a str,
        tool_call_id: &'a str,
        event_tx: &'a mpsc::Sender<AgentEvent>,
        available_tool_schemas: &'a [ToolSchema],
        flags: ToolExecutionSessionFlags,
    ) -> Self {
        Self {
            session_id: Some(session_id),
            tool_call_id,
            event_tx: Some(event_tx),
            available_tool_schemas: Some(available_tool_schemas),
            bypass_permissions: flags.bypass_permissions,
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
            let _ = tx.try_send(event);
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

#[cfg(test)]
mod session_flags_tests {
    use super::*;
    use bamboo_domain::AgentRuntimeState;

    #[test]
    fn from_session_defaults_false_without_runtime_state() {
        let session = Session::new("s-none", "test-model");
        assert_eq!(
            ToolExecutionSessionFlags::from_session(&session),
            ToolExecutionSessionFlags { bypass_permissions: false }
        );
    }

    #[test]
    fn from_session_reads_bypass_from_runtime_state() {
        let mut session = Session::new("s-bypass", "test-model");
        let mut runtime = AgentRuntimeState::new("run-1");
        runtime.bypass_permissions = true;
        session.agent_runtime_state = Some(runtime);
        assert!(ToolExecutionSessionFlags::from_session(&session).bypass_permissions);
    }

    #[test]
    fn for_dispatch_maps_flags_onto_context() {
        let (tx, _rx) = mpsc::channel(1);
        let ctx = ToolExecutionContext::for_dispatch(
            "s1",
            "call-1",
            &tx,
            &[],
            ToolExecutionSessionFlags { bypass_permissions: true },
        );
        assert_eq!(ctx.session_id, Some("s1"));
        assert!(ctx.bypass_permissions);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn emit_does_not_block_when_channel_is_full() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(AgentEvent::Token {
            content: "full".to_string(),
        })
        .await
        .unwrap();
        let ctx = ToolExecutionContext {
            session_id: Some("session_1"),
            tool_call_id: "call_1",
            event_tx: Some(&tx),
            available_tool_schemas: None,
            bypass_permissions: false,
        };

        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            ctx.emit(AgentEvent::Token {
                content: "next".to_string(),
            }),
        )
        .await
        .expect("emit should not block on full channel");

        let first = rx.recv().await.unwrap();
        match first {
            AgentEvent::Token { content } => assert_eq!(content, "full"),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn emit_converts_token_to_tool_token() {
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = ToolExecutionContext {
            session_id: Some("session_1"),
            tool_call_id: "call_123",
            event_tx: Some(&tx),
            available_tool_schemas: None,
            bypass_permissions: false,
        };

        ctx.emit(AgentEvent::Token {
            content: "test content".to_string(),
        })
        .await;

        let event = rx.recv().await.unwrap();
        match event {
            AgentEvent::ToolToken {
                tool_call_id,
                content,
            } => {
                assert_eq!(tool_call_id, "call_123");
                assert_eq!(content, "test content");
            }
            other => panic!("Expected ToolToken, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn emit_passes_through_non_token_events() {
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = ToolExecutionContext {
            session_id: Some("session_1"),
            tool_call_id: "call_456",
            event_tx: Some(&tx),
            available_tool_schemas: None,
            bypass_permissions: false,
        };

        // Test with various non-Token events
        ctx.emit(AgentEvent::ToolToken {
            tool_call_id: "other".to_string(),
            content: "direct tool token".to_string(),
        })
        .await;

        let event = rx.recv().await.unwrap();
        match event {
            AgentEvent::ToolToken { content, .. } => {
                assert_eq!(content, "direct tool token");
            }
            other => panic!("Expected ToolToken, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn emit_does_nothing_when_no_sender() {
        let ctx = ToolExecutionContext::none("call_789");

        // Should not panic or block
        ctx.emit(AgentEvent::Token {
            content: "test".to_string(),
        })
        .await;

        // Success if we get here
    }

    #[tokio::test]
    async fn emit_tool_token_convenience_method() {
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = ToolExecutionContext {
            session_id: None,
            tool_call_id: "call_abc",
            event_tx: Some(&tx),
            available_tool_schemas: None,
            bypass_permissions: false,
        };

        ctx.emit_tool_token("convenient output").await;

        let event = rx.recv().await.unwrap();
        match event {
            AgentEvent::ToolToken {
                tool_call_id,
                content,
            } => {
                assert_eq!(tool_call_id, "call_abc");
                assert_eq!(content, "convenient output");
            }
            other => panic!("Expected ToolToken, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn emit_tool_token_with_no_sender_does_nothing() {
        let ctx = ToolExecutionContext::none("call_def");

        // Should not panic or block
        ctx.emit_tool_token("test").await;

        // Success if we get here
    }

    #[test]
    fn none_creates_context_with_no_optional_fields() {
        let ctx = ToolExecutionContext::none("call_xyz");

        assert_eq!(ctx.session_id, None);
        assert_eq!(ctx.tool_call_id, "call_xyz");
        assert!(ctx.event_tx.is_none());
    }

    #[test]
    fn cloned_sender_returns_none_when_no_sender() {
        let ctx = ToolExecutionContext::none("call_test");
        assert!(ctx.cloned_sender().is_none());
    }

    #[tokio::test]
    async fn cloned_sender_returns_clone_when_sender_present() {
        let (tx, _rx) = mpsc::channel(10);
        let ctx = ToolExecutionContext {
            session_id: None,
            tool_call_id: "call_clone",
            event_tx: Some(&tx),
            available_tool_schemas: None,
            bypass_permissions: false,
        };

        let cloned = ctx.cloned_sender();
        assert!(cloned.is_some());

        // Can use cloned sender
        cloned
            .unwrap()
            .send(AgentEvent::Token {
                content: "test".to_string(),
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn emit_handles_multiple_sequential_calls() {
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = ToolExecutionContext {
            session_id: Some("session_multi"),
            tool_call_id: "call_multi",
            event_tx: Some(&tx),
            available_tool_schemas: None,
            bypass_permissions: false,
        };

        for i in 0..5 {
            ctx.emit(AgentEvent::Token {
                content: format!("message {}", i),
            })
            .await;
        }

        for i in 0..5 {
            let event = rx.recv().await.unwrap();
            match event {
                AgentEvent::ToolToken { content, .. } => {
                    assert_eq!(content, format!("message {}", i));
                }
                other => panic!("Expected ToolToken, got: {other:?}"),
            }
        }
    }

    #[test]
    fn context_is_clone_and_copy() {
        let (tx, _rx) = mpsc::channel(10);
        let ctx = ToolExecutionContext {
            session_id: Some("session_copy"),
            tool_call_id: "call_copy",
            event_tx: Some(&tx),
            available_tool_schemas: None,
            bypass_permissions: false,
        };

        // Can clone (Copy implies Clone)
        let _cloned = ctx;

        // Can copy
        let copied = ctx;

        // Both are valid
        assert_eq!(copied.tool_call_id, "call_copy");
    }

    #[test]
    fn context_is_debug() {
        let ctx = ToolExecutionContext::none("call_debug");
        let debug_str = format!("{:?}", ctx);
        assert!(debug_str.contains("call_debug"));
    }

    #[tokio::test]
    async fn emit_with_empty_tool_call_id() {
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = ToolExecutionContext {
            session_id: None,
            tool_call_id: "",
            event_tx: Some(&tx),
            available_tool_schemas: None,
            bypass_permissions: false,
        };

        ctx.emit(AgentEvent::Token {
            content: "test".to_string(),
        })
        .await;

        let event = rx.recv().await.unwrap();
        match event {
            AgentEvent::ToolToken { tool_call_id, .. } => {
                assert_eq!(tool_call_id, "");
            }
            other => panic!("Expected ToolToken, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn emit_with_unicode_content() {
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = ToolExecutionContext {
            session_id: Some("会话"),
            tool_call_id: "调用_123",
            event_tx: Some(&tx),
            available_tool_schemas: None,
            bypass_permissions: false,
        };

        ctx.emit(AgentEvent::Token {
            content: "测试内容 🎯".to_string(),
        })
        .await;

        let event = rx.recv().await.unwrap();
        match event {
            AgentEvent::ToolToken {
                tool_call_id,
                content,
            } => {
                assert_eq!(tool_call_id, "调用_123");
                assert_eq!(content, "测试内容 🎯");
            }
            other => panic!("Expected ToolToken, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn emit_with_special_characters_in_tool_call_id() {
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = ToolExecutionContext {
            session_id: None,
            tool_call_id: "call-with_special.chars:123",
            event_tx: Some(&tx),
            available_tool_schemas: None,
            bypass_permissions: false,
        };

        ctx.emit(AgentEvent::Token {
            content: "test".to_string(),
        })
        .await;

        let event = rx.recv().await.unwrap();
        match event {
            AgentEvent::ToolToken { tool_call_id, .. } => {
                assert_eq!(tool_call_id, "call-with_special.chars:123");
            }
            other => panic!("Expected ToolToken, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn emit_tool_token_with_string_content() {
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = ToolExecutionContext {
            session_id: None,
            tool_call_id: "call_string",
            event_tx: Some(&tx),
            available_tool_schemas: None,
            bypass_permissions: false,
        };

        let content = String::from("owned string");
        ctx.emit_tool_token(content).await;

        let event = rx.recv().await.unwrap();
        match event {
            AgentEvent::ToolToken { content, .. } => {
                assert_eq!(content, "owned string");
            }
            other => panic!("Expected ToolToken, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn emit_tool_token_with_str_content() {
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = ToolExecutionContext {
            session_id: None,
            tool_call_id: "call_str",
            event_tx: Some(&tx),
            available_tool_schemas: None,
            bypass_permissions: false,
        };

        ctx.emit_tool_token("string slice").await;

        let event = rx.recv().await.unwrap();
        match event {
            AgentEvent::ToolToken { content, .. } => {
                assert_eq!(content, "string slice");
            }
            other => panic!("Expected ToolToken, got: {other:?}"),
        }
    }
}
