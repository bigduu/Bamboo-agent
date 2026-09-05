//! Execution orchestration for background agent runs.
//!
//! This module provides the shared execution infrastructure used by all
//! background execution paths: HTTP execute handler, spawn scheduler,
//! and schedule manager.
//!
//! # Key types
//!
//! - [`AgentRunner`] / [`AgentStatus`] — lifecycle state of an agent run
//! - [`try_reserve_runner`] — atomically reserve a runner slot
//! - [`finalize_runner`] — update runner terminal status
//! - [`create_event_forwarder`] — MPSC → broadcast channel forwarder
//! - [`get_or_create_event_sender`] — session-scoped event broadcaster
//! - [`spawn_session_execution`] — spawn agent execution with full orchestration

pub mod agent_spawn;
pub mod child_completion;
pub mod event_forwarder;
pub mod event_publication;
pub mod runner_lifecycle;
pub mod runner_state;
pub mod session_events;
pub mod spawn;

pub use agent_spawn::{
    log_base_system_prompt_snapshot, reserve_session_execution, spawn_session_execution,
    SessionCompletionHook, SessionExecutionArgs, SessionExecutionOutcome,
    SessionExecutionReservation, SessionExecutionReserveOutcome,
};
pub use child_completion::{ChildCompletion, ChildCompletionHandler};
pub use event_forwarder::{create_event_forwarder, AccountFeedInbox};
pub use runner_lifecycle::{
    finalize_runner, finalize_runner_exact, reserve_runner_core, status_from_execution_result,
    try_reserve_runner, ReserveOutcome, RunnerReservation,
};
pub use runner_state::{AgentRunner, AgentStatus};
pub use session_events::{get_or_create_event_sender, SESSION_EVENT_CHANNEL_CAPACITY};
pub use spawn::{
    ChildRunLaunchHook, ExternalChildRunner, SessionInboxRuntimeBinding, SpawnContext, SpawnJob,
    SpawnScheduler,
};
