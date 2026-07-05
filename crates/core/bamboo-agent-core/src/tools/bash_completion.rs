//! Loop-facing completion signal for background Bash shells (issue #84 Phase 2b
//! follow-up: push completion into the loop instead of only polling at turn end).
//!
//! A `run_in_background` shell (or an auto-promoted long command) runs detached
//! from the agent loop. When it finishes, the loop should learn the result the
//! same way it learns a sub-agent finished — pushed in, not polled.
//! [`BashCompletionSink`] is that push: the background completion-poll task
//! invokes it once per shell on exit, and the engine's implementation delivers
//! the result into the owning session's loop (injected at the next round
//! boundary when the loop is actively iterating; via a resume when it is idle).
//!
//! The trait lives in `bamboo-agent-core` — NOT `bamboo-engine` — because the
//! producer (`bamboo_tools::tools::bash_runtime::spawn_completion_poll`) is in
//! the tools crate, which depends on this crate but not on the engine. This is
//! the one deliberate wiring divergence from the sibling `ChildCompletionHandler`
//! (which can live in the engine because its own producer does too).

/// Data delivered to a [`BashCompletionSink`] when a background shell exits.
#[derive(Debug, Clone)]
pub struct BashCompletionInfo {
    /// Bamboo session id that owns the shell — i.e. the loop to notify. The sink
    /// no-ops for a shell with no owning session (`None` at spawn time), so this
    /// is always a concrete id by the time it reaches a sink.
    pub session_id: String,
    /// Background shell id (the `bash_id` returned to the model; the value to
    /// pass to `BashOutput`).
    pub bash_id: String,
    /// The command line that was executed.
    pub command: String,
    /// Process exit code, when available (`None` for signal / kill termination).
    pub exit_code: Option<i32>,
    /// Terminal status: one of `"completed"`, `"killed"`, or `"error"`.
    pub status: String,
    /// The last lines of captured output, so the model can see the result
    /// without a mandatory `BashOutput` round-trip. May be empty (no output).
    /// Byte-bounded by the producer.
    pub output_tail: String,
}

/// Loop-facing sink invoked exactly once when a background Bash shell completes.
///
/// Implementations MUST be cheap and non-blocking on the calling task: the
/// producer invokes this from the shell's completion-poll task, so the
/// implementation should hand the delivery to a detached task (mirroring the
/// sibling `BashResumeHook`) rather than doing I/O inline.
///
/// Delivery MUST stay idempotent with the durable end-of-turn suspend/poll
/// backstop: the same completion can also be observed by that poll, so an
/// implementation must not double-deliver (e.g. guard on the persisted bash
/// wait) and must treat the push purely as a latency optimization over the poll.
pub trait BashCompletionSink: Send + Sync {
    /// Called once, off the agent loop, when the shell described by `info` exits.
    fn on_bash_completed(&self, info: BashCompletionInfo);
}
