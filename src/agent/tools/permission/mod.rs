//! Permission management system for tool execution.
//!
//! This module provides a comprehensive permission system for controlling access
//! to potentially dangerous operations like file writes, command execution,
//! HTTP requests, and more.
//!
//! # Key Components
//!
//! - [`PermissionConfig`](config::PermissionConfig): Configuration for permissions including
//!   whitelist rules and session grants
//! - [`PermissionChecker`](checker::PermissionChecker): Trait for checking and requesting permissions
//! - [`PermissionType`](config::PermissionType): Types of permissions (WriteFile, ExecuteCommand, etc.)
//!
//! # Usage
//!
//! ```rust
//! use std::sync::Arc;
//! use agent_tools::permission::{PermissionConfig, PermissionChecker, PermissionType};
//!
//! // Create a permission configuration
//! let config = Arc::new(PermissionConfig::new());
//!
//! // Check if permission is needed
//! if config.needs_confirmation(PermissionType::WriteFile, "/tmp/test.txt") {
//!     // Request user confirmation...
//! }
//!
//! // Grant session permission
//! config.grant_session_permission(PermissionType::WriteFile, "/tmp/*");
//! ```

pub mod config;
pub mod checker;
pub mod storage;
pub mod tool_permissions;

// Re-export commonly used types
pub use config::{
    PermissionConfig, PermissionRule, PermissionType, RiskLevel, SerializablePermissionConfig,
    SessionGrant,
};
pub use checker::{
    AllowAllPermissionChecker, ConfigPermissionChecker, DenyDangerousPermissionChecker,
    LoggingPermissionChecker, PermissionChecker, PermissionCheckerExt, PermissionContext,
    PermissionError, PermissionResult,
};
pub use storage::PermissionStorage;
pub use tool_permissions::{check_permissions, is_delete_command};
