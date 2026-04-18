//! Persistent storage port definitions.
//!
//! This module provides abstract storage interfaces for persisting agent data.
//! Concrete implementations live in the `bamboo-agent-storage` crate.

pub mod port;

pub use port::{AttachmentReader, Storage};
