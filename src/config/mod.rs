//! Configuration management for Bamboo
//!
//! This module provides configuration loading and saving.

pub mod bamboo_config;
pub mod paths;

pub use bamboo_config::{BambooConfig, ServerConfig};
pub use paths::*;
