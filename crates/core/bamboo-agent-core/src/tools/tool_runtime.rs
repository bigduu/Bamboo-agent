//! Runtime types for the (rewritten) [`Tool`](crate::tools::Tool) trait: the
//! per-call [`ToolOutcome`], the scheduling/gating [`ToolClass`], the owned
//! [`ToolCtx`], and the detached-work ([`RunningHandle`]) + async-completion
//! machinery. See `zenith/docs/tool-v2.md`.
//!
//! A tool declares its **per-call disposition as its return value**
//! ([`ToolOutcome`]) instead of the loop inferring async/interactive intent after
//! the fact by sniffing string markers on a plain `ToolResult`. Sync / detached /
//! interactive are the three [`ToolOutcome`] variants, decided per call; a slow
//! [`ToolOutcome::Completed`] can be runtime-promoted to [`ToolOutcome::Running`].

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::tools::{BashCompletionSink, ToolMutability, ToolResult, ToolSchema};
use crate::{AgentEvent, PendingQuestion};

/// The per-call disposition of a tool — the value the loop reads directly,
/// replacing the post-hoc string-marker inference channel (`ToolHandlingOutcome`,
/// the `should_handle_user_question_tool` sniff, and the running/`runtime_control`
/// result markers).
pub enum ToolOutcome {
    /// Terminal in-round. Append this [`ToolResult`] as the paired `tool_result`
    /// now (today's success path). Loop control == Continue.
    Completed(ToolResult),

    /// The work detaches and continues after `invoke` returns. The executor emits
    /// a synthetic `{handle, status:"running"}` paired `tool_result` now, and the
    /// real result re-enters the loop later via the async-completion sink →
    /// `pending_injected_messages` boundary. Loop control == Continue.
    Running(RunningHandle),

    /// Suspend the turn for a human decision. Carries the structured
    /// [`PendingQuestion`] (drives the pending-question state, no marker sniff)
    /// AND the tool's `result` — the rich display payload (conclusion / plan /
    /// permission data) that becomes the paired `tool_result` in the transcript
    /// and the `ToolComplete` UI event. Loop control == Break.
    NeedsHuman {
        question: PendingQuestion,
        result: ToolResult,
    },
}

// Hand-written: `RunningHandle` holds non-`Debug` fields (a `Box<dyn FnOnce>` and
// a boxed future), so the enum can't derive `Debug`; the detached/human variants
// print a placeholder. Lets tests assert on / `unwrap_err` a `ToolOutcome`.
impl std::fmt::Debug for ToolOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ToolOutcome::Completed(result) => f.debug_tuple("Completed").field(result).finish(),
            ToolOutcome::Running(handle) => {
                write!(f, "Running(tool_call_id={:?})", handle.tool_call_id)
            }
            ToolOutcome::NeedsHuman { question, .. } => {
                write!(f, "NeedsHuman(tool_call_id={:?})", question.tool_call_id)
            }
        }
    }
}

impl ToolOutcome {
    /// TRANSITIONAL: collapse an outcome back to a plain [`ToolResult`] for call
    /// sites that are not yet outcome-aware. `Completed` yields its result;
    /// `Running` yields its synthetic ack (itself a `ToolResult`, preserving the
    /// running-placeholder the loop saw before); `NeedsHuman` yields a failure
    /// result (no tool produces it on these paths during the transition). Phase B
    /// makes every consumer outcome-aware and removes this.
    pub fn into_tool_result(self) -> ToolResult {
        match self {
            ToolOutcome::Completed(result) => result,
            ToolOutcome::Running(handle) => handle.ack,
            ToolOutcome::NeedsHuman { result, .. } => result,
        }
    }
}

/// Scheduling + permission-gating class for a tool call. Folds the old
/// `{mutability, concurrency_safe}` pair plus the promotion opt-in into one
/// args-aware value (returned by [`Tool::classify`](crate::tools::Tool::classify)).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ToolClass {
    /// Approval / permission gating axis.
    pub mutability: ToolMutability,
    /// May join the contiguous read-only parallel batch (old `concurrency_safe`).
    pub parallel_safe: bool,
    /// May be latency-promoted from `Completed` to `Running` by the executor.
    /// Opt-in for network / remote / idempotent tools; NEVER for local
    /// non-idempotent writes (Edit/Write/NotebookEdit).
    pub promotable: bool,
}

impl ToolClass {
    /// The conservative default: mutating, serial, not promotable.
    pub const MUTATING_SERIAL: Self = Self {
        mutability: ToolMutability::Mutating,
        parallel_safe: false,
        promotable: false,
    };

    /// A read-only tool that may run in the concurrent read-only batch.
    pub const READONLY_PARALLEL: Self = Self {
        mutability: ToolMutability::ReadOnly,
        parallel_safe: true,
        promotable: false,
    };

    /// Builder: mark this class latency-promotable.
    pub const fn promotable(mut self) -> Self {
        self.promotable = true;
        self
    }
}

impl Default for ToolClass {
    fn default() -> Self {
        Self::MUTATING_SERIAL
    }
}

/// Which durable-wait bucket + terminal-gate `suspend_reason` a [`Running`]
/// outcome belongs to — the distinct discriminants the finalize match keys on.
/// Do NOT overload them.
///
/// [`Running`]: ToolOutcome::Running
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AsyncWaitKind {
    /// Background Bash + executor-driven promoted tools → `"waiting_for_async_tools"`.
    AsyncTools,
    /// SubAgent create/wait → `"waiting_for_children"` (distinct merge path).
    Children,
}

/// Boxed `'static` future yielding the real result of a promoted / driven tool.
pub type ToolResultFuture = Pin<Box<dyn Future<Output = ToolResult> + Send + 'static>>;

/// How the real result of a [`ToolOutcome::Running`] re-enters the loop. Both
/// variants terminate at the same `pending_injected_messages` → merge boundary.
pub enum RunningCompletion {
    /// The tool already wired its own out-of-band delivery (background Bash clones
    /// the completion sink into its background registry). The executor only
    /// records the durable wait as the idle backstop; nothing to drive.
    Detached,
    /// Executor-driven: the executor awaits this future, then feeds the
    /// [`ToolResult`] to the completion sink. The vehicle for latency-adaptive
    /// promotion — wrap any slow `Completed` future here with no per-tool code.
    Driven(ToolResultFuture),
}

/// The detached-work handle carried by [`ToolOutcome::Running`].
pub struct RunningHandle {
    /// The model's `tool_call_id` — pairs the synthetic ack now with the real
    /// result later.
    pub tool_call_id: String,
    /// The synthetic paired `tool_result` shown now (`{handle, status:"running"}`).
    pub ack: ToolResult,
    /// How the real result re-enters the loop.
    pub completion: RunningCompletion,
    /// Which durable-wait bucket + terminal-gate `suspend_reason` this belongs to.
    pub wait_kind: AsyncWaitKind,
    /// Cooperative kill (reap the process / abort the drive task).
    pub kill: Box<dyn FnOnce() + Send>,
}

/// Owned, `Arc`-based execution context for a tool call.
///
/// Owned (not `Copy`-over-borrows like the former `ToolExecutionContext`) so an
/// `invoke` future is `'static`-capable and can be moved into the executor's
/// detached drive task for latency-adaptive promotion. Tool execution still never
/// borrows `&mut session` (only the apply phase mutates), so concurrent execution
/// stays sound.
#[derive(Clone)]
pub struct ToolCtx {
    /// Bamboo session id executing the tool.
    pub session_id: Option<Arc<str>>,
    /// The model's tool call id.
    pub tool_call_id: Arc<str>,
    /// Streaming progress channel (owned clone of the former `&mpsc::Sender`).
    pub event_tx: Option<mpsc::Sender<AgentEvent>>,
    /// Snapshot of tools available to the session.
    pub available_tool_schemas: Arc<[ToolSchema]>,
    /// Per-session bypass-permissions flag.
    pub bypass_permissions: bool,
    /// Whether the executing loop can suspend and self-resume for detached work.
    pub can_async_resume: bool,
    /// Sink that pushes a detached tool's real result into the owning loop.
    pub async_completion_sink: Option<Arc<dyn AsyncToolCompletionSink>>,
    /// TRANSITIONAL: the bash-specific completion sink, carried through the
    /// rewrite so background Bash keeps its existing delivery. Phase B folds this
    /// into `async_completion_sink` (bash → `ToolOutcome::Running`) and removes it.
    pub bash_completion_sink: Option<Arc<dyn BashCompletionSink>>,
}

impl ToolCtx {
    /// A context with no session / channel / capabilities — for tests and tools
    /// that synthesize a child call. Peer of the former `ToolExecutionContext::none`.
    pub fn none(tool_call_id: impl Into<Arc<str>>) -> Self {
        Self {
            session_id: None,
            tool_call_id: tool_call_id.into(),
            event_tx: None,
            available_tool_schemas: Arc::from(Vec::new()),
            bypass_permissions: false,
            can_async_resume: false,
            async_completion_sink: None,
            bash_completion_sink: None,
        }
    }

    /// Clone the bash-specific completion sink (transitional; see the field).
    pub fn cloned_bash_completion_sink(&self) -> Option<Arc<dyn BashCompletionSink>> {
        self.bash_completion_sink.clone()
    }

    /// The session id as `&str`, if present.
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// Clone the event sender for a spawned task (peer of the former
    /// `cloned_sender`).
    pub fn cloned_sender(&self) -> Option<mpsc::Sender<AgentEvent>> {
        self.event_tx.clone()
    }

    /// Clone the async-completion sink for a spawned task.
    pub fn cloned_async_completion_sink(&self) -> Option<Arc<dyn AsyncToolCompletionSink>> {
        self.async_completion_sink.clone()
    }

    /// Best-effort emit of an event (ignored when no sender). A bare
    /// [`AgentEvent::Token`] is remapped to a tool-scoped `ToolToken`, matching
    /// the former `ToolExecutionContext::emit`.
    pub async fn emit(&self, event: AgentEvent) {
        if let Some(tx) = &self.event_tx {
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

/// Payload delivered to an [`AsyncToolCompletionSink`] when a [`Running`] tool's
/// detached work finishes. Carries any tool's [`ToolResult`] plus the identity
/// needed to render it.
///
/// [`Running`]: ToolOutcome::Running
pub struct AsyncToolCompletionInfo {
    /// Owning session id — the loop to notify.
    pub session_id: String,
    /// The originating tool call id (already acked in-round with a running
    /// placeholder).
    pub tool_call_id: String,
    /// The real result to deliver. It is rendered into an appended user message,
    /// NOT a second paired `tool_result` for `tool_call_id` (the synthetic ack
    /// already closed that id, and a late second result would break same-turn
    /// pairing).
    pub result: ToolResult,
}

/// Loop-facing sink invoked exactly once, off the loop, when a [`Running`] tool's
/// detached work finishes. Generalizes the former bash-specific
/// `BashCompletionSink`.
///
/// Implementations MUST be cheap / non-blocking (hand off to a detached task) and
/// idempotent with the durable poll backstop.
///
/// [`Running`]: ToolOutcome::Running
pub trait AsyncToolCompletionSink: Send + Sync {
    /// Deliver a completed tool's result into its owning session's loop.
    fn on_tool_completed(&self, info: AsyncToolCompletionInfo);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_class_defaults_and_builders() {
        assert_eq!(ToolClass::default(), ToolClass::MUTATING_SERIAL);
        assert_eq!(
            ToolClass::MUTATING_SERIAL.mutability,
            ToolMutability::Mutating
        );
        assert!(!ToolClass::MUTATING_SERIAL.parallel_safe);
        assert!(!ToolClass::MUTATING_SERIAL.promotable);
        assert_eq!(
            ToolClass::READONLY_PARALLEL.mutability,
            ToolMutability::ReadOnly
        );
        assert!(ToolClass::READONLY_PARALLEL.parallel_safe);
        assert!(ToolClass::READONLY_PARALLEL.promotable().promotable);
    }

    #[test]
    fn tool_ctx_none_and_accessors() {
        let ctx = ToolCtx::none("call-1");
        assert_eq!(ctx.tool_call_id.as_ref(), "call-1");
        assert!(ctx.session_id().is_none());
        assert!(ctx.cloned_sender().is_none());
        assert!(ctx.cloned_async_completion_sink().is_none());
    }
}
