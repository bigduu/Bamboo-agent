//! Schedule system (timed tasks that create/run sessions).
//!
//! A schedule is persisted in the Bamboo data directory's `schedules.json` and can periodically create new
//! root sessions (optionally auto-executing a task message).

pub mod domain;
pub mod manager;
pub mod store;
pub mod trigger_engine;

pub use domain::{
    MisfirePolicy, OverlapPolicy, ScheduleRunRecord, ScheduleRunStatus, ScheduleSpec,
    ScheduleState, ScheduleTrigger, ScheduleWeekday, ScheduleWindow,
};
pub use manager::{ScheduleManager, ScheduleRunJob};
pub use store::{ScheduleEntry, ScheduleRunConfig, ScheduleStore};
pub use trigger_engine::{
    default_trigger_engine, DynTriggerEngine, NativeTriggerEngine, TriggerComputationError,
    TriggerEngine, TriggerEngineKind,
};
