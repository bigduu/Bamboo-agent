//! Bamboo schedule application layer.
//!
//! Provides trigger computation, schedule persistence, session factory,
//! and background execution orchestration for scheduled tasks.

pub mod trigger_engine;
pub mod store;
pub mod session_factory;
pub mod manager;

pub use manager::{ResolvedRunConfig, ScheduleContext, ScheduleManager, ScheduleRunJob};
