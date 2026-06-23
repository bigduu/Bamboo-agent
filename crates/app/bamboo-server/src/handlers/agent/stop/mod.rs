//! Agent execution cancellation API handler.

mod handler;
mod types;

pub use handler::handler;
// Reused by the v2 WS multiplex `control` channel (`{"type":"stop"}`).
pub(crate) use handler::cancel_session;

#[cfg(test)]
mod tests;
