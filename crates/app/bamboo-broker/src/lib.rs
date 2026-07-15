//! `bamboo-broker` — a standalone network message broker for sub-agent
//! ask/reply (and future task/handoff) traffic.
//!
//! The broker fronts durable per-session [`Mailbox`](bamboo_subagent::Mailbox)
//! queues with a WebSocket bus, so a central orchestrator and its workers —
//! local subprocesses, local Docker containers, or SSH/remote hosts — exchange
//! messages over the network with durability and at-least-once delivery, without
//! sharing a filesystem.
//!
//! Topology: a single central broker (hub-and-spoke). It is a *pure message
//! bus* — it routes messages, it does not spawn actors or coordinate with other
//! brokers. Spawning the PUSH-side workers lives in [`deploy`] (the `Deployer`
//! family: local / docker / ssh / russh). (The `bamboo_subagent::WorkerLauncher`
//! trait that earlier docs referenced was never wired and has been removed; the
//! PULL-side actor runner dispatches placement inline.)
//!
//! Layers:
//! - [`proto`] — the client↔broker wire frames.
//! - [`core`] — [`BrokerCore`], the transport-agnostic routing engine (tested in-process).
//! - the WebSocket server + auth + `bamboo broker serve` wiring layer on top (added next).

pub mod ask;
pub mod child_link;
pub mod client;
pub mod core;
pub mod deploy;
pub mod deploy_russh;
pub mod mcp;
pub mod mux;
pub mod proto;
pub mod serve;
pub mod server;

mod error;

/// The orchestrator's broker mailbox id — where workers send MCP proxy requests
/// and where `serve_mcp_proxy` listens. A fixed well-known id (single MCP host).
pub const ORCHESTRATOR_ID: &str = "bamboo-orchestrator";

pub use crate::ask::{ask_agent, ask_over, request_over};
pub use crate::child_link::BrokerChildLink;
pub use crate::client::BrokerClient;
pub use crate::core::{BrokerCore, DEFAULT_MAX_PENDING_PER_MAILBOX};
pub use crate::deploy::{
    AgentDeployment, DeployedAgent, Deployer, DockerDeployer, LocalProcessDeployer,
    RemoteDeployment, SshDeployer, UploadSpec,
};
pub use crate::deploy_russh::{RusshAuth, RusshDeployer};
pub use crate::error::{BrokerError, BrokerResult};
pub use crate::mcp::{
    serve_mcp_proxy, serve_mcp_proxy_supervised, McpProxyExecutor, McpReply, McpRequest,
    ProxiedResult, RoleToolAllowlist,
};
pub use crate::mux::MultiplexedClient;
pub use crate::proto::{BrokerFrame, ClientFrame};
pub use crate::serve::{serve_executor, serve_loop, serve_mailbox, serve_with, Handled};
pub use crate::server::{BrokerLimits, BrokerServer};
pub use bamboo_subagent::AgentRef;
