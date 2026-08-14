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
//! ```ignore
//! use std::sync::Arc;
//! use crate::{PermissionConfig, PermissionChecker, PermissionType};
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

pub mod approval_store;
pub mod bash_security;
pub mod checker;
pub mod config;
pub mod hierarchy;
pub mod policy;
mod replay;
pub mod rule_parser;
pub mod storage;
pub mod tool_permissions;

// Re-export commonly used types
pub use approval_store::{with_cached_approval, ApprovalDecision, ApprovalStore};
pub use checker::{
    is_read_only_command, is_safe_edit_command, AllowAllPermissionChecker, ConfigPermissionChecker,
    DenyDangerousPermissionChecker, GuardianReadOnlyChecker, LoggingPermissionChecker,
    ModeAwarePermissionChecker, PermissionChecker, PermissionCheckerExt, PermissionContext,
    PermissionError, PermissionResult,
};
pub use config::{
    explicit_deny_policy_reason, PermissionConfig, PermissionMode, PermissionRule, PermissionType,
    RiskLevel, SerializablePermissionConfig, SessionGrant, TemporaryPermissionGrant,
    TemporaryPermissionGrantEffect, TemporaryPermissionGrantScope,
};
pub use hierarchy::PermissionRuleSet;
pub use policy::{
    conservative_matchers, DurablePermissionRule, EffectivePermissionPolicy, PermissionDecision,
    PermissionDecisionKind, PermissionDecisionReceipt, PermissionDecisionSource,
    PermissionDenyReason, PermissionEvaluation, PermissionMatcher, PermissionMatcherKind,
    PermissionOutcome, PermissionReasonCode, PermissionRequest, PermissionRuleEffect,
    PermissionRuleRef, PermissionRuleScope, PermissionRuleSource,
};
pub use replay::{current_permission_replay_generation, with_permission_replay_generation};
pub use rule_parser::ParsedRule;
pub use storage::{
    default_permission_document, PermissionSection, PermissionStorage, PermissionStorageError,
};
pub use tool_permissions::{
    check_permissions, check_tool_rules, is_delete_command, MAX_PROACTIVE_PERMISSION_BATCH,
};
