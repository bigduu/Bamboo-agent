pub mod a2a_adapter;
pub mod actor_adapter;
pub mod approval_registry;
pub mod config;
pub mod live;
pub mod mapping;
pub mod runtime;

pub use a2a_adapter::A2AExternalChildRunner;
pub use actor_adapter::{
    child_approval_reviewer, set_child_approval_reviewer, ActorChildRunner, ChildApprovalReviewer,
};
pub use config::{
    parse_external_agents, parse_subagent_routing, resolve_runtime_metadata, ExternalAgentProfile,
    ExternalAgentProtocol, SubagentRouting,
};
pub use runtime::{
    build_external_child_runner, build_external_child_runner_with_registry,
    build_external_child_runner_with_registry_and_reviewer,
};
