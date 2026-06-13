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
}

pub type BrokerResult<T> = Result<T, BrokerError>;
