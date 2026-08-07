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
    /// TLS configuration / handshake failure — bad/missing cert or key
    /// (server `wss://` listener, #48), a bad custom CA (client-side
    /// self-signed trust), or the TLS handshake itself failing. Kept distinct
    /// from [`Self::Transport`] so callers/logs can tell "the TLS layer
    /// rejected this" apart from a plain socket/WS-protocol failure.
    #[error("tls: {0}")]
    Tls(String),
    /// `deliver` refused: the target session's mailbox already holds
    /// `limit` pending (undelivered-or-unacked) messages — a backlog cap
    /// against a flood aimed at an offline/never-draining mailbox filling
    /// disk (#53). The sender should back off; this is not a transport or
    /// auth failure.
    #[error("mailbox '{session}' is full ({limit} pending messages)")]
    MailboxFull { session: String, limit: usize },
    /// The broker explicitly REJECTED a request (e.g. [`Self::MailboxFull`])
    /// and told the caller so via a correlated [`crate::proto::BrokerFrame::Error`]
    /// — as opposed to [`Self::Transport`], which means the RPC never got a
    /// definitive answer at all (network stall, broker crash, timeout). This
    /// distinction is the point: `BrokerClient::deliver` returns this the
    /// instant the broker's rejection arrives, instead of the caller burning
    /// the full receipt timeout to learn the same thing. `reason` is the
    /// broker's stringified error (client-side; the original `BrokerError`
    /// variant on the broker doesn't cross the wire, only its `Display`).
    #[error("broker rejected the request: {0}")]
    Rejected(String),
    /// The broker connection disappeared while handlers were still running,
    /// and at least one handler ignored cancellation past the bounded drain
    /// deadline. The serve loop aborts and joins the listed handlers before
    /// returning this error, so callers fail non-successfully without leaving
    /// detached work behind.
    #[error(
        "connection-loss drain timed out after {timeout_ms} ms; stuck handlers: {stuck_ids:?}"
    )]
    ConnectionDrainTimeout {
        timeout_ms: u64,
        stuck_ids: Vec<String>,
    },
}

pub type BrokerResult<T> = Result<T, BrokerError>;
