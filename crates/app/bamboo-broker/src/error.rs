//! Broker error type. Wraps the underlying mailbox store errors and adds the
//! transport/auth failures the WS layer needs.

/// Errors from broker operations.
#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    /// A durable mailbox (maildir) operation failed.
    #[error("store: {0}")]
    Store(#[from] bamboo_subagent::StoreError),
    /// Handshake authentication failed (bad/missing token).
    #[error("auth: {0}")]
    Auth(String),
    /// A malformed or out-of-sequence frame (e.g. a request before `Hello`).
    #[error("protocol: {0}")]
    Protocol(String),
    /// WebSocket / IO transport failure.
    #[error("transport: {0}")]
    Transport(String),
    /// `deliver` refused: the target session's mailbox already holds
    /// `limit` pending (undelivered-or-unacked) messages — a backlog cap
    /// against a flood aimed at an offline/never-draining mailbox filling
    /// disk (#53). The sender should back off; this is not a transport or
    /// auth failure.
    #[error("mailbox '{session}' is full ({limit} pending messages)")]
    MailboxFull { session: String, limit: usize },
}

pub type BrokerResult<T> = Result<T, BrokerError>;
