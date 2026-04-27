pub mod a2a_adapter;
pub mod config;
pub mod mapping;
pub mod runtime;

pub use a2a_adapter::A2AExternalChildRunner;
pub use config::{
    parse_external_agents, parse_subagent_routing, resolve_runtime_metadata, ExternalAgentProfile,
    ExternalAgentProtocol, SubagentRouting,
};
pub use runtime::build_external_child_runner;
