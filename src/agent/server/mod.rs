// Deprecated: These modules have been moved to crate::server
// Re-exports provided for backward compatibility

#[deprecated(
    since = "0.2.0",
    note = "Use `crate::server` instead. See migration guide in README.md"
)]
pub use crate::server::handlers;

#[deprecated(
    since = "0.2.0",
    note = "Use `crate::server::logging` instead. See migration guide in README.md"
)]
pub use crate::server::logging;

#[deprecated(
    since = "0.2.0",
    note = "Use `crate::server::metrics_service` instead. See migration guide in README.md"
)]
pub use crate::server::metrics_service;

#[allow(clippy::module_inception)]
pub mod server;
pub mod state;

#[deprecated(
    since = "0.2.0",
    note = "Use `crate::server::workflow` instead. See migration guide in README.md"
)]
pub use crate::server::workflow;

pub use crate::agent::loop_module::{run_agent_loop, run_agent_loop_with_config, AgentLoopConfig};
pub use server::{
    run_server, run_server_with_config, run_server_with_config_and_mode, start_server_in_thread,
};

#[deprecated(
    since = "0.2.0",
    note = "Use `crate::server::workflow` instead. See migration guide in README.md"
)]
pub use crate::server::workflow::{WorkflowDefinition, WorkflowLoadError, WorkflowLoader};

#[deprecated(
    since = "0.2.0",
    note = "Use `crate::server::metrics_service::MetricsService` instead. See migration guide in README.md"
)]
pub use crate::server::metrics_service::MetricsService;
