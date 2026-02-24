//! Web service layer - HTTP controllers and services
//!
//! DEPRECATED: This module has been consolidated into crate::server
//! All functionality is now available in the unified server module.
//!
//! Use `crate::server` instead.

// Deprecated: These modules have been moved to crate::server
// Re-exports provided for backward compatibility
pub use crate::server::controllers;
pub use crate::server::error;
pub use crate::server::model_config_helper;
pub mod server;
pub use crate::server::services;

use std::sync::Arc;
use tokio::sync::Mutex;

pub use server::WebService;

pub type WebServiceState = Arc<Mutex<WebService>>;
