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
//! brokers. Placement/spawning lives behind `bamboo_subagent::WorkerLauncher`.
//!
//! Layers:
//! - [`proto`] — the client↔broker wire frames.
//! - [`core`] — [`BrokerCore`], the transport-agnostic routing engine (tested in-process).
//! - the WebSocket server + auth + `bamboo broker serve` wiring layer on top (added next).

pub mod client;
pub mod core;
pub mod proto;
pub mod server;

mod error;

pub use crate::client::BrokerClient;
pub use crate::core::BrokerCore;
pub use crate::error::{BrokerError, BrokerResult};
pub use crate::proto::{BrokerFrame, ClientFrame};
pub use crate::server::BrokerServer;
