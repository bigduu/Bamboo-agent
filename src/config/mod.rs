//! Configuration management for Bamboo
//!
//! This module provides XDG-compliant configuration loading and saving.

pub mod xdg_paths;
pub mod bamboo_config;

pub use bamboo_config::{BambooConfig, ServerConfig};
pub use xdg_paths::*;
