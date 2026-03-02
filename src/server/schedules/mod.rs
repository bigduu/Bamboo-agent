//! Schedule system (timed tasks that create/run sessions).
//!
//! A schedule is persisted in `~/.bamboo/schedules.json` and can periodically create new
//! root sessions (optionally auto-executing a task message).

pub mod manager;
pub mod store;

pub use manager::{ScheduleManager, ScheduleRunJob};
pub use store::{ScheduleEntry, ScheduleRunConfig, ScheduleStore};

