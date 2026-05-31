//! Server event infrastructure.
//!
//! - [`replayable`] — the canonical per-session replayable-event helper.
//! - [`change_feed`] / [`journal`] / [`account_sink`] — the durable,
//!   account-scoped change feed powering `GET /api/v1/stream`.

pub mod account_sink;
pub mod change_feed;
pub mod journal;
pub mod replayable;

pub use account_sink::AccountEventSink;
pub use change_feed::ChangeEvent;
pub use replayable::publish_replayable_session_event;
