//! Notification policy for the Bamboo agent framework.
//!
//! This crate owns the *decision* of whether a given [`AgentEvent`] should
//! surface to the user as a notification. It is deliberately a self-contained
//! infra crate with no I/O beyond reading/writing a small preferences file and
//! no async runtime: it classifies events, gates them against user
//! [`NotificationPreferences`], and coalesces duplicates within a short window.
//!
//! The pipeline is three layers:
//!
//! - [`preferences`] — the user's opt-in/opt-out flags, persisted as JSON.
//! - [`policy`] — a pure [`classify`](policy::classify) function mapping an
//!   [`AgentEvent`] + preferences into an optional
//!   [`ClassifiedNotification`](policy::ClassifiedNotification).
//! - [`service`] — a `Send + Sync` [`NotificationService`] that wraps policy
//!   with a dedup window, preference persistence, and per-session relay
//!   idempotency for the server to hang off an `Arc`.
//!
//! The server constructs the user-facing
//! [`AgentEvent::Notification`](bamboo_agent_core::AgentEvent::Notification);
//! the client merely delivers it after its own presence checks.
//!
//! [`AgentEvent`]: bamboo_agent_core::AgentEvent

pub mod policy;
pub mod preferences;
pub mod service;

pub use policy::{NotificationCategory, NotificationPriority};
pub use preferences::NotificationPreferences;
pub use service::NotificationService;
