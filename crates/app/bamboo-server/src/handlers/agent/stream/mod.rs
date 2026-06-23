//! Account-scoped change-feed SSE endpoint.

mod handler;
mod response;

pub use handler::handler;
// Reused by the v2 WS multiplex (`feed` channel) so the WS path runs the exact
// same journal-replay + cursor handoff discipline as the v1 SSE feed instead of
// reimplementing it.
pub(crate) use response::{plan_replay, ReplayPlan};

#[cfg(test)]
mod tests;
