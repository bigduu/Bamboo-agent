//! Server-side re-export shim for the engine event infrastructure.
//!
//! The event cluster (account change feed + replayable-event helper) now lives
//! in `bamboo_engine::events`. These re-exports preserve the historical
//! `crate::events::…` paths used throughout the server crate.

pub use bamboo_engine::events::{
    account_sink, change_feed, journal, publish_replayable_session_event, replayable,
    AccountEventSink, ChangeEvent,
};

#[cfg(test)]
mod replayable_tests;
