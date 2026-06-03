//! Ergonomic SDK surface over the canonical child-spawn path.
//!
//! # Anti-fork invariant
//!
//! There is exactly **one** implementation of the spawn/execute/finalize logic:
//! [`spawn::run_child_spawn`]. Both the background [`SpawnScheduler`] (via
//! `run_spawn_job`) and the ergonomic [`runner::ProfileRunner`] funnel into it.
//! No caller may duplicate the event ordering, heartbeat/watchdog, runner
//! reservation, `ExecuteRequest` construction, or terminal status strings.
//!
//! [`SpawnScheduler`]: crate::runtime::execution::SpawnScheduler

pub mod runner;
pub mod spawn;

#[cfg(test)]
mod tests;
