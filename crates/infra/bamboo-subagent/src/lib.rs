//! `bamboo-subagent` — sub-agent fleet runtime.
//!
//! Slice 1 (this build): pure filesystem logic, no runtime dependency.
//!
//! - [`store`] — project-keyed session store + denormalized indices (rebuildable cache).
//! - [`mailbox`] — Maildir-style persistent inbox (multi-writer / single-reader, crash-safe).
//!
//! See `docs/subagent-actor-runtime-design.md` (§3.4, §5) and
//! `docs/subagent-store-mailbox-interface.md` for the design these implement.
//!
//! The session payload is kept opaque (generic `T: Serialize`/`DeserializeOwned`) so this
//! crate stays a leaf and is fully unit-testable with a tempdir; higher layers pass the
//! domain `Session`. Index rebuild is decoupled via the [`store::MetaExtractor`] seam.

pub mod codex_discovery;
pub mod discovery;
pub mod error;
pub mod executor;
pub mod executor_util;
pub mod fleet;
pub mod mailbox;
pub(crate) mod poison;
pub mod proto;
pub mod provision;
pub mod store;
pub mod transport;

pub use discovery::{Discovery, Fabric, FileFabric};
pub use error::{Result, StoreError};
pub use executor::{ChildExecutor, ChildOutcome, EchoExecutor, EventSink, HostBridge, SteerInbox};
pub use fleet::{spawn_worker, spawn_worker_on_bus, SpawnedChild};
pub use mailbox::{
    AdmittedSet, AgentRef, AskBody, AskMode, Delivered, InboxKind, InboxMessage, Mailbox, MsgId,
    ReplyBody, ADMITTED_SET_CAPACITY,
};
pub use proto::{AgentRecord, ChildFrame, ParentFrame, RunSpec, TerminalStatus};
pub use provision::{
    BusEndpoint, Capabilities, ChildIdentity, ExecutorSpec, Limits, McpProxyConfig, ModelRefSpec,
    Placement, ProvisionSpec, ScopedCredential, SecretsEnvelope, PROVISION_VERSION,
};
pub use store::{
    ChildEntry, ChildFields, ChildStatus, ChildrenIndex, MetaExtractor, ProjectIndex, ProjectKey,
    RootEntry, RootFields, SessionLoc, SubagentStore,
};
pub use transport::{ChildClient, ChildLink, TransportError, TransportResult, WsServer};
