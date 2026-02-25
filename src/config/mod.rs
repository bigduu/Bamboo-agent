//! Configuration management for Bamboo
//!
//! This module provides configuration loading and saving.
//!
//! **DEPRECATED**: This entire module is deprecated. Use `bamboo_agent::core::Config` instead.
//! The configuration has been unified into the core module.

pub mod bamboo_config;
pub mod paths;

#[deprecated(
    since = "0.2.6",
    note = "Use `bamboo_agent::core::Config` instead. This module will be removed in a future version."
)]
pub use bamboo_config::{BambooConfig, ServerConfig};
pub use paths::*;
