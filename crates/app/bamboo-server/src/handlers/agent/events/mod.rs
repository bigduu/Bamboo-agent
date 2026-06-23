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

#[cfg(test)]
mod tests;
