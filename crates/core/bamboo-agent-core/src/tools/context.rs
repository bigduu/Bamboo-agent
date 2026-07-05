//! Execution context for tool calls.
//!
//! Tools normally return a single `ToolResult` after completion. Some tools
//! (for example, long-running CLIs) may want to stream intermediate progress
//! to clients. The agent loop passes a `ToolExecutionContext` that allows tools
//! to emit `AgentEvent`s while they run.

use std::sync::Arc;

use tokio::sync::mpsc;

use serde_json::Value;

use crate::tools::{BashCompletionSink, ToolSchema};
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
#[derive(Clone, Copy)]
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
    /// When `true`, the executing agent loop can suspend the current turn for a
    /// backgrounded shell and self-resume once it finishes (i.e. a
    /// `bash_resume_hook` AND persistence are wired). The Bash tool uses this to
    /// decide whether its auto path (`run_in_background` omitted) may promote a
    /// long command to background: when `false`, the auto path stays purely
    /// synchronous so the command's output is never orphaned on a loop that
    /// can't resume it (issue #84, phase 2d). Derived from the loop config at
    /// the dispatch site — NOT session-derived — so it is a direct
    /// `for_dispatch` parameter rather than a `ToolExecutionSessionFlags` field.
    pub can_async_resume: bool,
    /// Loop-facing sink invoked once when a background Bash shell owned by this
    /// session completes (issue #84 Phase 2b follow-up). When wired, the Bash
    /// tool hands it to the background completion-poll task so the shell's result
    /// is pushed into the loop (injected at the next round boundary while it is
    /// actively looping, or via a resume when it is idle) — instead of the model
    /// having to poll `BashOutput`. Borrowed like `event_tx` (kept `Copy`) and
    /// cloned into the spawned task via [`Self::cloned_bash_completion_sink`].
    /// Derived from the loop config at the dispatch site — NOT session-derived —
    /// so it is a direct `for_dispatch` parameter, not a session flag. `None`
    /// leaves the push inert (the durable end-of-turn poll backstop still runs).
    pub bash_completion_sink: Option<&'a Arc<dyn BashCompletionSink>>,
    /// The tool call's `function.arguments` JSON string, already parsed once by
    /// the dispatching agent loop (which also parses it to populate the
    /// `ToolStart` event). When `Some`, downstream executors should reuse this
    /// instead of calling `parse_tool_args_best_effort` on the raw string a
    /// second time — the value here is the *exact* output of that same parser on
    /// the same input, so reuse is behavior-preserving (issue #106, deferred B1
    /// from #17). When `None` (e.g. `none()` contexts, tests, or executors that
    /// synthesize a child call), executors parse the raw string themselves,
    /// preserving the original single-parse-per-consumer behavior.
    pub pre_parsed_args: Option<&'a Value>,
}

// Hand-written so implementors of `BashCompletionSink` (a trait object stored
// here) don't have to be `Debug`. The sink is rendered as a presence flag.
impl std::fmt::Debug for ToolExecutionContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolExecutionContext")
            .field("session_id", &self.session_id)
            .field("tool_call_id", &self.tool_call_id)
            .field("event_tx", &self.event_tx)
            .field("available_tool_schemas", &self.available_tool_schemas)
            .field("bypass_permissions", &self.bypass_permissions)
            .field("can_async_resume", &self.can_async_resume)
            .field("bash_completion_sink", &self.bash_completion_sink.is_some())
            .field("pre_parsed_args", &self.pre_parsed_args)
            .finish()
    }
}

impl<'a> ToolExecutionContext<'a> {
    pub fn none(tool_call_id: &'a str) -> Self {
        Self {
            session_id: None,
            tool_call_id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
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
        // Whether the executing loop can suspend for and self-resume a
        // backgrounded bash shell (`bash_resume_hook` + persistence wired).
        // When `false`, the Bash auto path stays synchronous (issue #84,
        // phase 2d). NOT session-derived — set by the dispatch site.
        can_async_resume: bool,
        // Loop-facing sink for background-Bash completion (issue #84 Phase 2b
        // follow-up). Set by the dispatch site from the loop config; `None` on
        // loops without the engine suspend/resume machinery so the push stays
        // inert. NOT session-derived.
        bash_completion_sink: Option<&'a Arc<dyn BashCompletionSink>>,
        // The call's arguments, already parsed once at the dispatch site (to
        // populate the `ToolStart` event). Threaded down so the executor reuses
        // it instead of re-parsing the raw JSON string (issue #106). Only pass
        // `Some` when the value was produced by `parse_tool_args_best_effort`
        // (the executor's own parser) so reuse is byte-for-byte equivalent; a
        // dispatch site that parses with a different/stricter parser must pass
        // `None` so the executor re-parses leniently and behavior is preserved.
        pre_parsed_args: Option<&'a Value>,
    ) -> Self {
        Self {
            session_id: Some(session_id),
            tool_call_id,
            event_tx: Some(event_tx),
            available_tool_schemas: Some(available_tool_schemas),
            bypass_permissions: flags.bypass_permissions,
            can_async_resume,
            bash_completion_sink,
            pre_parsed_args,
        }
    }

    /// Clone the sender (when present) for use in spawned tasks.
    pub fn cloned_sender(&self) -> Option<mpsc::Sender<AgentEvent>> {
        self.event_tx.cloned()
    }

    /// Clone the background-Bash completion sink (when present) into an owned
    /// handle for a spawned task — mirrors [`Self::cloned_sender`]. Returns an
    /// owned `Arc` so the shell's detached completion-poll task can outlive the
    /// borrowed dispatch context.
    pub fn cloned_bash_completion_sink(&self) -> Option<Arc<dyn BashCompletionSink>> {
        self.bash_completion_sink.map(Arc::clone)
    }

    /// TRANSITIONAL bridge to the owned [`ToolCtx`](crate::tools::ToolCtx) that the
    /// rewritten `Tool::invoke` takes. Clones this borrowed dispatch context into
    /// owned/`Arc` form at the concrete-executor seam, so the trait + dispatch
    /// path keep using `ToolExecutionContext` (no wide ripple) while tools run on
    /// `ToolCtx`. Removed in Phase B when the dispatch path adopts `ToolCtx`
    /// directly.
    pub fn to_tool_ctx(&self) -> crate::tools::ToolCtx {
        crate::tools::ToolCtx {
            session_id: self.session_id.map(Arc::from),
            tool_call_id: Arc::from(self.tool_call_id),
            event_tx: self.event_tx.cloned(),
            available_tool_schemas: self
                .available_tool_schemas
                .map(Arc::from)
                .unwrap_or_else(|| Arc::from(Vec::new())),
            bypass_permissions: self.bypass_permissions,
            can_async_resume: self.can_async_resume,
            async_completion_sink: None,
            bash_completion_sink: self.bash_completion_sink.map(Arc::clone),
        }
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
            ToolExecutionSessionFlags {
                bypass_permissions: false
            }
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
            ToolExecutionSessionFlags {
                bypass_permissions: true,
            },
            true,
            None,
            None,
        );
        assert_eq!(ctx.session_id, Some("s1"));
        assert!(ctx.bypass_permissions);
        assert!(ctx.can_async_resume);
        assert!(ctx.pre_parsed_args.is_none());
    }

    #[test]
    fn for_dispatch_threads_pre_parsed_args() {
        let (tx, _rx) = mpsc::channel(1);
        let parsed = serde_json::json!({"v": "x"});
        let ctx = ToolExecutionContext::for_dispatch(
            "s1",
            "call-1",
            &tx,
            &[],
            ToolExecutionSessionFlags::default(),
            false,
            None,
            Some(&parsed),
        );
        assert_eq!(ctx.pre_parsed_args, Some(&parsed));
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
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
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
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
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
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
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
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
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
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
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
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
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
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
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
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
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
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
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
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
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
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
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
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
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
