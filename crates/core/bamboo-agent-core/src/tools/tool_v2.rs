//! Tool v2 — clean-slate async-native tool trait with a per-call [`ToolOutcome`].
//!
//! **Phase 0 (this file): types only, alongside v1, nothing wired.** These are
//! the buildable target shapes from `zenith/docs/tool-v2.md`. No executor or loop
//! consumes them yet; the v1 [`Tool`](crate::tools::Tool) trait remains the live
//! path. Subsequent phases add a v1→v2 adapter, stand up the v2 executor behind a
//! flag, port tools in batches with golden-parity tests, switch the loop, and
//! finally delete v1.
//!
//! The core idea: a tool declares its **per-call disposition as its return
//! value** ([`ToolOutcome`]), instead of the loop inferring async/interactive
//! intent after the fact by sniffing string markers on a plain `ToolResult`. The
//! async-tools RFC's static Sync/AsyncNotify/InteractiveSuspend taxonomy becomes
//! the three [`ToolOutcome`] variants, decided per call; a slow
//! [`ToolOutcome::Completed`] can be runtime-promoted to [`ToolOutcome::Running`].

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::tools::{FunctionSchema, ToolMutability, ToolResult, ToolSchema};
use crate::{AgentEvent, PendingQuestion};

/// The per-call disposition of a v2 tool — the value the loop reads directly,
/// replacing the post-hoc string-marker inference channel (`ToolHandlingOutcome`,
/// the `should_handle_user_question_tool` sniff, and the running/`runtime_control`
/// result markers).
pub enum ToolOutcome {
    /// Terminal in-round. Append this [`ToolResult`] as the paired `tool_result`
    /// now (today's success/error path). Loop control == Continue.
    Completed(ToolResult),

    /// The work detaches and continues after `invoke` returns. The executor emits
    /// a synthetic `{handle, status:"running"}` paired `tool_result` now, and the
    /// real result re-enters the loop later via the async-completion sink →
    /// `pending_injected_messages` boundary. Loop control == Continue (never
    /// breaks — remaining same-round calls still run), mirroring background Bash.
    Running(RunningHandle),

    /// Suspend the turn for a human decision, carrying a fully-formed
    /// [`PendingQuestion`] directly instead of encoding intent in result JSON.
    /// Loop control == Break.
    NeedsHuman(PendingQuestion),
}

/// Scheduling + permission-gating class for a v2 tool call. Folds v1's
/// `{mutability, concurrency_safe}` pair plus the new promotion opt-in into one
/// args-aware value (returned by [`ToolV2::classify`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ToolClass {
    /// Approval / permission gating axis (== v1 [`ToolMutability`]).
    pub mutability: ToolMutability,
    /// May join the contiguous read-only parallel batch (== v1 `concurrency_safe`).
    pub parallel_safe: bool,
    /// May be latency-promoted from `Completed` to `Running` by the executor.
    /// Opt-in for network / remote / idempotent tools; NEVER for local
    /// non-idempotent writes (Edit/Write/NotebookEdit).
    pub promotable: bool,
}

impl ToolClass {
    /// The conservative default: mutating, serial, not promotable — matches the
    /// v1 `Tool::mutability` default.
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
}

impl Default for ToolClass {
    fn default() -> Self {
        Self::MUTATING_SERIAL
    }
}

/// Which durable-wait bucket + terminal-gate `suspend_reason` a [`Running`]
/// outcome belongs to. These are the distinct discriminants the finalize match
/// keys on — do NOT overload them (`suspend_reason` is a finalize-merge
/// discriminant).
///
/// [`Running`]: ToolOutcome::Running
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AsyncWaitKind {
    /// Background Bash + all executor-driven promoted tools. Terminal-gate
    /// reason: `"waiting_for_async_tools"` (the generalized rename of
    /// `"waiting_for_bash"`).
    AsyncTools,
    /// SubAgent create/wait. Terminal-gate reason: `"waiting_for_children"`
    /// (unchanged — a distinct merge path).
    Children,
}

/// Boxed `'static` future yielding the real result of a promoted / driven tool.
pub type ToolResultFuture = Pin<Box<dyn Future<Output = ToolResult> + Send + 'static>>;

/// How the real result of a [`ToolOutcome::Running`] re-enters the loop. Both
/// variants terminate at the same `pending_injected_messages` → merge boundary;
/// they differ only in who drives the wait.
pub enum RunningCompletion {
    /// The tool already wired its own out-of-band delivery (e.g. background Bash
    /// clones the completion sink into its background registry). The executor
    /// only records the durable wait as the idle backstop; nothing to drive.
    Detached,
    /// Executor-driven: the executor awaits this future, then feeds the
    /// [`ToolResult`] to the completion sink (which lands it in
    /// `pending_injected_messages`). This is the vehicle for latency-adaptive
    /// promotion — wrap any slow `Completed` future here with no per-tool code.
    Driven(ToolResultFuture),
}

/// The detached-work handle carried by [`ToolOutcome::Running`].
pub struct RunningHandle {
    /// The model's `tool_call_id` — pairs the synthetic ack emitted now with the
    /// real result delivered later (protocol: same-turn paired `tool_result`).
    pub tool_call_id: String,
    /// The synthetic paired `tool_result` shown now: `success == true`, body is
    /// the `{handle, status:"running"}` shape.
    pub ack: ToolResult,
    /// How the real result re-enters the loop.
    pub completion: RunningCompletion,
    /// Which durable-wait bucket + terminal-gate `suspend_reason` this belongs to.
    pub wait_kind: AsyncWaitKind,
    /// Cooperative kill: Bash reaps via `kill_on_drop`; a promoted future is
    /// aborted by dropping its drive-task handle.
    pub kill: Box<dyn FnOnce() + Send>,
}

/// Owned, `Arc`-based execution context for a v2 tool call.
///
/// Unlike v1's `Copy`-over-borrows `ToolExecutionContext`, this is **owned** so
/// every `invoke` future is `'static`-capable and can be moved into the
/// executor's detached drive task for latency-adaptive promotion. It preserves
/// the load-bearing invariant that tool execution never borrows `&mut session`
/// (only the apply phase mutates), so concurrent execution stays sound.
#[derive(Clone)]
pub struct ToolCtx {
    /// Bamboo session id executing the tool.
    pub session_id: Option<Arc<str>>,
    /// The model's tool call id.
    pub tool_call_id: Arc<str>,
    /// Streaming progress channel (owned clone of the v1 `&'a mpsc::Sender`).
    pub event_tx: Option<mpsc::Sender<AgentEvent>>,
    /// Snapshot of tools available to the session (owned clone of `&'a [ToolSchema]`).
    pub available_tool_schemas: Arc<[ToolSchema]>,
    /// Per-session bypass-permissions flag.
    pub bypass_permissions: bool,
    /// Whether the executing loop can suspend and self-resume for detached work
    /// (a resume hook + persistence are wired).
    pub can_async_resume: bool,
    /// Generalizes v1's bash-specific `bash_completion_sink` to all async tools.
    pub async_completion_sink: Option<Arc<dyn AsyncToolCompletionSink>>,
}

/// Payload delivered to an [`AsyncToolCompletionSink`] when a [`Running`] tool's
/// detached work finishes. Generalizes the bash-specific `BashCompletionInfo` to
/// carry any tool's [`ToolResult`] plus the identity needed to render it.
///
/// [`Running`]: ToolOutcome::Running
pub struct AsyncToolCompletionInfo {
    /// Owning session id — the loop to notify.
    pub session_id: String,
    /// The originating tool call id (already acked in-round with a running
    /// placeholder).
    pub tool_call_id: String,
    /// The real result to deliver. It is rendered into an appended user message,
    /// NOT a second paired `tool_result` for `tool_call_id` — the synthetic ack
    /// already closed that id, and a late second result would break same-turn
    /// pairing.
    pub result: ToolResult,
}

/// Loop-facing sink invoked exactly once, off the loop, when a [`Running`] tool's
/// detached work finishes. The generalized peer of the bash-specific
/// `BashCompletionSink`.
///
/// Implementations MUST be cheap / non-blocking (hand off to a detached task) and
/// idempotent with the durable poll backstop — the same completion may also be
/// observed by that backstop, so delivery must not double-apply.
///
/// [`Running`]: ToolOutcome::Running
pub trait AsyncToolCompletionSink: Send + Sync {
    /// Deliver a completed tool's result into its owning session's loop.
    fn on_tool_completed(&self, info: AsyncToolCompletionInfo);
}

/// The v2 tool trait: one async-native entry ([`invoke`](ToolV2::invoke))
/// returning a per-call [`ToolOutcome`]. Collapses v1's `execute` /
/// `execute_with_context` split (every tool is context-aware now) and its four
/// classification hooks (into one args-aware [`classify`](ToolV2::classify)).
///
/// `invoke` takes `&self` (tools are stored `Arc`-shared, so by-value `self` is
/// impossible) and an **owned** [`ToolCtx`] + owned parsed `args`.
#[async_trait]
pub trait ToolV2: Send + Sync {
    /// Stable tool name (unchanged from v1).
    fn name(&self) -> &str;

    /// Model-facing description (unchanged from v1).
    fn description(&self) -> &str;

    /// JSON Schema for the tool's arguments (unchanged from v1).
    fn parameters_schema(&self) -> serde_json::Value;

    /// Per-call scheduling + gating class. Args-aware (folds v1's four hooks).
    /// Defaults to the conservative [`ToolClass::MUTATING_SERIAL`], matching v1's
    /// `mutability`/`concurrency_safe` defaults.
    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::MUTATING_SERIAL
    }

    /// The sole execution entry. Returns the call's [`ToolOutcome`].
    async fn invoke(&self, args: serde_json::Value, ctx: ToolCtx) -> ToolOutcome;

    /// Derived tool schema. Identical shape to v1 `Tool::to_schema`.
    fn to_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: self.name().to_string(),
                description: self.description().to_string(),
                parameters: self.parameters_schema(),
            },
        }
    }
}

/// Reference-counted pointer to a v2 tool (peer of v1 `SharedTool`).
pub type SharedToolV2 = Arc<dyn ToolV2>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_class_defaults_match_v1() {
        assert_eq!(ToolClass::default(), ToolClass::MUTATING_SERIAL);
        assert_eq!(ToolClass::MUTATING_SERIAL.mutability, ToolMutability::Mutating);
        assert!(!ToolClass::MUTATING_SERIAL.parallel_safe);
        assert!(!ToolClass::MUTATING_SERIAL.promotable);
        assert_eq!(ToolClass::READONLY_PARALLEL.mutability, ToolMutability::ReadOnly);
        assert!(ToolClass::READONLY_PARALLEL.parallel_safe);
    }

    /// A minimal v2 tool proves the trait is object-safe (`dyn ToolV2`) and that
    /// the default `classify`/`to_schema` provided methods compile + behave.
    struct EchoTool;

    #[async_trait]
    impl ToolV2 for EchoTool {
        fn name(&self) -> &str {
            "Echo"
        }
        fn description(&self) -> &str {
            "echo the args back"
        }
        fn parameters_schema(&self) -> serde_json::Value {
            serde_json::json!({ "type": "object" })
        }
        async fn invoke(&self, args: serde_json::Value, _ctx: ToolCtx) -> ToolOutcome {
            ToolOutcome::Completed(ToolResult {
                success: true,
                result: args.to_string(),
                display_preference: None,
                images: Vec::new(),
            })
        }
    }

    #[tokio::test]
    async fn echo_tool_is_object_safe_and_completes() {
        let tool: SharedToolV2 = Arc::new(EchoTool);
        assert_eq!(tool.name(), "Echo");
        // default classify == conservative
        assert_eq!(tool.classify(&serde_json::json!({})), ToolClass::MUTATING_SERIAL);
        // default to_schema mirrors v1 shape
        let schema = tool.to_schema();
        assert_eq!(schema.schema_type, "function");
        assert_eq!(schema.function.name, "Echo");

        let ctx = ToolCtx {
            session_id: None,
            tool_call_id: Arc::from("call-1"),
            event_tx: None,
            available_tool_schemas: Arc::from(Vec::new()),
            bypass_permissions: false,
            can_async_resume: false,
            async_completion_sink: None,
        };
        match tool.invoke(serde_json::json!({ "x": 1 }), ctx).await {
            ToolOutcome::Completed(r) => {
                assert!(r.success);
                assert!(r.result.contains("\"x\":1"));
            }
            _ => panic!("expected Completed"),
        }
    }
}
