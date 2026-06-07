//! Schedule management application logic.
//!
//! Self-contained subsystem: trigger evaluation, run store, manager, the
//! session factory, and the LLM-facing `scheduler` tool ([`ScheduleTasksTool`])
//! that fronts all of it. The tool lives here (not in the generic
//! `bamboo-server-tools` crate) because it is a facade over this subsystem, not
//! a subsystem-independent capability.

pub mod manager;
pub mod scheduler_tool;
pub mod session_factory;
pub mod store;
pub mod trigger_engine;

pub use bamboo_domain::{
    MisFirePolicy, OverlapPolicy, ScheduleRunConfig, ScheduleRunRecord, ScheduleRunStatus,
    ScheduleSpec, ScheduleState, ScheduleTrigger, ScheduleWeekday, ScheduleWindow,
};
pub use manager::{
    build_schedule_context, ResolvedRunConfig, ScheduleContext, ScheduleManager, ScheduleRunJob,
};
pub use scheduler_tool::ScheduleTasksTool;
pub use store::{ScheduleEntry, ScheduleStore};
pub use trigger_engine::{
    default_trigger_engine, DynTriggerEngine, NativeTriggerEngine, TriggerComputationError,
    TriggerEngine, TriggerEngineKind,
};
