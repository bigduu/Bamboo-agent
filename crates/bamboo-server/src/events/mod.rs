//! Server-side regression tests for the engine event infrastructure.
//!
//! The event cluster (account change feed + replayable-event helper) lives in
//! `bamboo_engine::events`; callers reference it directly. This module only
//! retains server-level tests that exercise that infrastructure through the
//! server `AppState`.

#[cfg(test)]
mod replayable_tests;
