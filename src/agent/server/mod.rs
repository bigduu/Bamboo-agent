// Deprecated: These modules have been moved to crate::server
// Re-exports provided for backward compatibility

pub use crate::server::handlers;
pub use crate::server::logging;
pub use crate::server::metrics_service;
#[allow(clippy::module_inception)]
pub mod server;
pub mod state;
pub use crate::server::workflow;

pub use crate::agent::loop_module::{run_agent_loop, run_agent_loop_with_config, AgentLoopConfig};
pub use server::{
    run_server, run_server_with_config, run_server_with_config_and_mode, start_server_in_thread,
};

pub use crate::server::workflow::{WorkflowDefinition, WorkflowLoadError, WorkflowLoader};

pub use crate::server::metrics_service::MetricsService;
