//! Web service layer - HTTP controllers and services
//!
//! DEPRECATED: This module has been consolidated into crate::server
//! All functionality is now available in the unified server module.
//!
//! # Migration Guide
//!
//! If you were using:
//! ```ignore
//! use bamboo_agent::web_service::WebService;
//! use bamboo_agent::web_service::controllers::*;
//! ```
//!
//! Change to:
//! ```ignore
//! use bamboo_agent::server::WebService;
//! use bamboo_agent::server::controllers::*;
//! ```
//!
//! All other imports follow the same pattern:
//! - `bamboo_agent::web_service::X` → `bamboo_agent::server::X`

// Deprecated: These modules have been moved to crate::server
// Re-exports provided for backward compatibility
#[deprecated(
    since = "0.2.0",
    note = "Use `crate::server::controllers` instead. See migration guide in README.md"
)]
pub use crate::server::controllers;

#[deprecated(
    since = "0.2.0",
    note = "Use `crate::server::error` instead. See migration guide in README.md"
)]
pub use crate::server::error;

#[deprecated(
    since = "0.2.0",
    note = "Use `crate::server::model_config_helper` instead. See migration guide in README.md"
)]
pub use crate::server::model_config_helper;

pub mod server;

#[deprecated(
    since = "0.2.0",
    note = "Use `crate::server::services` instead. See migration guide in README.md"
)]
pub use crate::server::services;

use std::sync::Arc;
use tokio::sync::Mutex;

#[deprecated(
    since = "0.2.0",
    note = "Use `crate::server::WebService` instead. See migration guide in README.md"
)]
pub use server::WebService;

#[deprecated(
    since = "0.2.0",
    note = "Use `crate::server::WebService` instead. See migration guide in README.md"
)]
pub type WebServiceState = Arc<Mutex<WebService>>;
