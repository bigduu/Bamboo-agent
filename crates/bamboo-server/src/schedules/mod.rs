//! Schedule system (timed tasks that create/run sessions).
//!
//! A schedule is persisted in the Bamboo data directory's `schedules.json` and can periodically create new
//! root sessions (optionally auto-executing a task message).
//!
//! Implementation lives in `bamboo-application-schedule`. This module provides
//! backward-compatible re-exports and adapter helpers.

pub mod manager;
pub mod session_factory;
pub mod store;
pub mod trigger_engine;

pub use bamboo_domain_schedule::{
    MisfirePolicy, OverlapPolicy, ScheduleRunConfig, ScheduleRunRecord, ScheduleRunStatus,
    ScheduleSpec, ScheduleState, ScheduleTrigger, ScheduleWeekday, ScheduleWindow,
};
pub use manager::{ScheduleManager, ScheduleRunJob};
pub use store::{ScheduleEntry, ScheduleStore};
pub use trigger_engine::{
    default_trigger_engine, DynTriggerEngine, NativeTriggerEngine, TriggerComputationError,
    TriggerEngine, TriggerEngineKind,
};
