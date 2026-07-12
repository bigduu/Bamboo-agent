//! The `Platform` trait + shared inbound/outbound message types.
//!
//! Per epic #447: cc-connect's proven 5-method core, with capabilities
//! EXPLICIT (not type-asserted) so the bridge/render layer never guesses what
//! an adapter can do. `ReplyCtx` is platform-opaque (`serde_json::Value`),
//! carried on every [`InboundMessage`] and handed back to
//! `Platform::reply`/`Platform::edit` unmodified — mirrors cc-connect's
//! `replyCtx any` pattern.

use tokio::sync::mpsc;

/// What a platform adapter supports. MVP adapters (Telegram, phase 1) advertise
/// everything `false` except plain text — buttons/streaming-edit are phase 2.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Capabilities {
    /// Inline approval/action buttons on a message.
    pub buttons: bool,
    /// In-place message editing (`Platform::edit`) — used for streaming
    /// edit-in-place in a later phase.
    pub edit_message: bool,
    /// Sending image attachments.
    pub images: bool,
    /// Sending file attachments.
    pub files: bool,
}

/// Platform-opaque context handed back unmodified to `Platform::reply`/`edit`.
/// Concrete shape is decided by each adapter (e.g. Telegram stores `chat_id`).
#[derive(Debug, Clone, PartialEq)]
pub struct ReplyCtx(pub serde_json::Value);

/// A reference to a previously-sent message, for `Platform::edit`
/// (capability-gated on [`Capabilities::edit_message`]).
#[derive(Debug, Clone, PartialEq)]
pub struct MessageRef(pub serde_json::Value);

/// A message received from a platform, normalized to the shape the bridge
/// understands. Fields beyond `text`/`reply_ctx` exist to let the bridge
/// enforce security policy (allow-list, dedup) generically across every
/// platform adapter, without knowing platform-specific wire formats.
#[derive(Debug, Clone)]
pub struct InboundMessage {
    /// Platform name (matches `Platform::name`), e.g. `"telegram"`.
    pub platform: String,
    /// Platform-scoped chat identifier.
    pub chat_id: String,
    /// Platform-scoped sender identifier (checked against `allow_from`).
    pub user_id: String,
    /// Platform-scoped, per-platform-unique message id used for dedup (e.g.
    /// Telegram's `update_id`, stringified).
    pub message_id: String,
    /// When the platform says the message was sent — used to drop stale
    /// backlog delivered right after a restart (older than process start).
    pub sent_at: chrono::DateTime<chrono::Utc>,
    /// Message text.
    pub text: String,
    /// Opaque context to hand back to `Platform::reply`/`edit`.
    pub reply_ctx: ReplyCtx,
}

/// A message to send to a platform.
#[derive(Debug, Clone)]
pub struct OutboundMessage {
    pub text: String,
}

impl OutboundMessage {
    pub fn text(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }
}

/// Error returned by a `Platform` adapter operation.
#[derive(Debug, thiserror::Error)]
pub enum PlatformError {
    #[error("{0}")]
    Other(String),
}

impl PlatformError {
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}

pub type PlatformResult<T> = std::result::Result<T, PlatformError>;

/// An IM-platform adapter. cc-connect's proven 5-method core (`start`,
/// `reply`, `edit`, `stop`, plus `capabilities`/`name`) — see epic #447.
#[async_trait::async_trait]
pub trait Platform: Send + Sync {
    /// Short identifier, e.g. `"telegram"`. Matches [`InboundMessage::platform`].
    fn name(&self) -> &str;

    /// What this adapter supports.
    fn capabilities(&self) -> Capabilities;

    /// Start receiving inbound messages, sending each onto `inbound`.
    /// Runs for the adapter's lifetime (a long-poll loop, a WS connection,
    /// …); returns only on an unrecoverable error or `stop()`.
    async fn start(&self, inbound: mpsc::Sender<InboundMessage>) -> PlatformResult<()>;

    /// Send a message in reply to `ctx`.
    async fn reply(&self, ctx: &ReplyCtx, msg: OutboundMessage) -> PlatformResult<MessageRef>;

    /// Edit a previously-sent message in place. Capability-gated on
    /// [`Capabilities::edit_message`]; adapters that don't support it may
    /// return an error — callers must check the capability first.
    async fn edit(&self, msg_ref: &MessageRef, new: OutboundMessage) -> PlatformResult<()>;

    /// Stop the adapter (best-effort; graceful shutdown).
    async fn stop(&self) -> PlatformResult<()>;
}
