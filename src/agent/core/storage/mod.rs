//! Persistent storage backends
//!
//! This module provides storage implementations for persisting agent data,
//! such as conversation history, session state, and other artifacts.
//!
//! # Available Backends
//!
//! - **JsonlStorage**: JSON Lines format for append-only logs
//!
//! # Usage
//!
//! ```rust,ignore
//! use bamboo_agent::agent::core::storage::{Storage, JsonlStorage};
//!
//! let storage = JsonlStorage::new("sessions.jsonl")?;
//! storage.append(session_id, &data).await?;
//! let data = storage.read(session_id).await?;
//! ```

pub mod jsonl;

pub use jsonl::{JsonlStorage, Storage};
