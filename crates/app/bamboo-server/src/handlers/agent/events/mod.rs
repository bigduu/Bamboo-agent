//! Server-Sent Events (SSE) handler for real-time agent event streaming.

mod handler;
mod stream;
mod terminal;

pub use handler::handler;
// Reused by the v2 WS multiplex (`agent.{sid}` channel):
// - `MAX_BATCH_MS` clamps the untrusted `?batch_ms=` query the same way the v1
//   SSE handler does.
// - `Coalescer` is the exact same token-merge buffer the v1 SSE path uses; the
//   WS forwarder reuses it rather than reimplementing coalescing.
pub(crate) use handler::MAX_BATCH_MS;
pub(crate) use stream::Coalescer;
// Reused by the v2 WS `agent.{sid}` forwarder to keep the channel open while
// child sub-agents are still running (parity with the v1 SSE stream, which does
// not close on the parent terminal while descendants survive).
pub(crate) use terminal::{
    begin_execute_startup, clear_pending_turn, has_running_child, mark_pending_turn,
    reconcile_abandoned_startup, startup_reconcile_delay, startup_work_id, terminal_event_if_ready,
    transition_startup_failure_if_owned, ExecuteStartupGuard, StartupFailureTarget,
};

#[cfg(test)]
mod tests;
