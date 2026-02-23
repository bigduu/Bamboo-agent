//! Permission checker trait and implementations.
//!
//! This module provides the [`PermissionChecker`] trait that defines how tools
//! check for permission before executing potentially dangerous operations.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::config::{PermissionConfig, PermissionType, RiskLevel};

/// Context for a permission request
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PermissionContext {
    /// The type of permission being requested
    pub permission_type: PermissionType,
    /// The resource being accessed (e.g., file path, URL, command)
    pub resource: String,
    /// Human-readable description of the operation
    pub operation_description: String,
    /// Additional details about the operation
    #[serde(skip_serializing_if = "Option::is_none")]
pub details: Option<serde_json::Value>,
}

impl PermissionContext {
    /// Create a new permission context
    pub fn new(
        permission_type: PermissionType,
        resource: impl Into<String>,
        operation_description: impl Into<String>,
    ) -> Self {
        Self {
            permission_type,
            resource: resource.into(),
            operation_description: operation_description.into(),
            details: None,
        }
    }

    /// Add details to the permission context
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    /// Get the risk level for this permission type
    pub fn risk_level(&self) -> RiskLevel {
        self.permission_type.risk_level()
    }

    /// Generate a human-readable message describing this permission request
    pub fn format_request_message(&self) -> String {
        let risk_label = self.risk_level().label();
        format!(
            "{} - {}\n\nResource: {}\nOperation: {}",
            risk_label,
            self.permission_type.description(),
            self.resource,
            self.operation_description
        )
    }
}

/// Result of a permission check
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionResult {
    /// Permission is granted, proceed with operation
    Granted,
    /// Permission is denied, do not proceed
    Denied,
    /// Permission requires user confirmation
    RequiresConfirmation(PermissionContext),
}

/// Error type for permission operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum PermissionError {
    #[error("Permission denied: {0}")]
    Denied(String),

    #[error("Permission check failed: {0}")]
    CheckFailed(String),

    #[error("Confirmation required for {permission_type:?} on {resource}")]
    ConfirmationRequired {
        permission_type: PermissionType,
        resource: String,
    },
}

impl PermissionError {
    /// Create a confirmation required error
    pub fn confirmation_required(context: PermissionContext) -> Self {
        Self::ConfirmationRequired {
            permission_type: context.permission_type,
            resource: context.resource,
        }
    }
}

/// Trait for checking and requesting permissions
///
/// This trait is implemented by types that can check if a permission is allowed
/// and request user confirmation when needed.
#[async_trait]
pub trait PermissionChecker: Send + Sync {
    /// Check if a permission needs confirmation
    ///
    /// Returns `true` if the operation requires user confirmation before proceeding.
    async fn needs_confirmation(&self, perm_type: PermissionType, resource: &str) -> bool;

    /// Check if a permission is granted (without requesting confirmation)
    ///
    /// This method checks the whitelist and session grants but does not
    /// prompt the user for confirmation.
    async fn is_granted(&self, perm_type: PermissionType, resource: &str) -> bool {
        !self.needs_confirmation(perm_type, resource).await
    }

    /// Request user confirmation for a permission
    ///
    /// This method should prompt the user for confirmation (e.g., via Tauri event
    /// to the frontend) and return the user's decision.
    ///
    /// Returns `true` if the user grants permission.
    async fn request_confirmation(&self, ctx: PermissionContext) -> Result<bool, PermissionError>;

    /// Grant a permission for the current session
    ///
    /// After granting, subsequent calls to `needs_confirmation` for the same
    /// permission type and matching resources will return `false`.
    fn grant_session_permission(&self, perm_type: PermissionType, resource: String);

    /// Check permission and either grant or request confirmation
    ///
    /// This is a convenience method that:
    /// 1. Checks if permission is already granted
    /// 2. If not, requests user confirmation
    /// 3. Returns true if permission is granted (either pre-authorized or confirmed)
    async fn check_or_request(&self, ctx: PermissionContext) -> Result<bool, PermissionError> {
        // First check if already granted
        if self.is_granted(ctx.permission_type, &ctx.resource).await {
            return Ok(true);
        }

        // Request confirmation from user
        self.request_confirmation(ctx).await
    }
}

/// A permission checker that uses a [`PermissionConfig`] for checks
///
/// This is the standard implementation that checks the configuration
/// but does not implement user confirmation (which requires frontend integration).
///
/// For a full implementation with user confirmation, use [`InteractivePermissionChecker`]
/// or implement the trait for your own type.
#[derive(Debug)]
pub struct ConfigPermissionChecker {
    config: Arc<PermissionConfig>,
}

impl ConfigPermissionChecker {
    /// Create a new config-based permission checker
    pub fn new(config: Arc<PermissionConfig>) -> Self {
        Self { config }
    }

    /// Get the underlying config
    pub fn config(&self) -> &PermissionConfig {
        &self.config
    }
}

#[async_trait]
impl PermissionChecker for ConfigPermissionChecker {
    async fn needs_confirmation(&self, perm_type: PermissionType, resource: &str) -> bool {
        self.config.needs_confirmation(perm_type, resource)
    }

    async fn request_confirmation(&self, _ctx: PermissionContext) -> Result<bool, PermissionError> {
        // This implementation doesn't support interactive confirmation
        // It always returns an error indicating confirmation is required
        Err(PermissionError::confirmation_required(_ctx))
    }

    fn grant_session_permission(&self, perm_type: PermissionType, resource: String) {
        self.config.grant_session_permission(perm_type, resource);
    }
}

/// A permission checker that wraps another checker and logs all permission checks
#[derive(Debug)]
pub struct LoggingPermissionChecker<T: PermissionChecker> {
    inner: T,
}

impl<T: PermissionChecker> LoggingPermissionChecker<T> {
    /// Create a new logging permission checker
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<T: PermissionChecker> PermissionChecker for LoggingPermissionChecker<T> {
    async fn needs_confirmation(&self, perm_type: PermissionType, resource: &str) -> bool {
        let needs = self.inner.needs_confirmation(perm_type, resource).await;
        log::debug!(
            "Permission check: {:?} for '{}' - needs_confirmation: {}",
            perm_type,
            resource,
            needs
        );
        needs
    }

    async fn request_confirmation(&self, ctx: PermissionContext) -> Result<bool, PermissionError> {
        log::info!(
            "Requesting user confirmation: {:?} for '{}'",
            ctx.permission_type,
            ctx.resource
        );
        let result = self.inner.request_confirmation(ctx).await;
        log::debug!("User confirmation result: {:?}", result);
        result
    }

    fn grant_session_permission(&self, perm_type: PermissionType, resource: String) {
        log::info!("Granting session permission: {:?} for '{}'", perm_type, resource);
        self.inner.grant_session_permission(perm_type, resource);
    }
}

/// A permission checker that always allows all operations
///
/// This is useful for testing or in trusted environments.
#[derive(Debug, Clone)]
pub struct AllowAllPermissionChecker;

#[async_trait]
impl PermissionChecker for AllowAllPermissionChecker {
    async fn needs_confirmation(&self, _perm_type: PermissionType, _resource: &str) -> bool {
        false
    }

    async fn request_confirmation(&self, _ctx: PermissionContext) -> Result<bool, PermissionError> {
        Ok(true)
    }

    fn grant_session_permission(&self, _perm_type: PermissionType, _resource: String) {
        // No-op since everything is allowed
    }
}

/// A permission checker that always denies dangerous operations
///
/// This is useful for read-only or highly restricted environments.
#[derive(Debug, Clone)]
pub struct DenyDangerousPermissionChecker;

#[async_trait]
impl PermissionChecker for DenyDangerousPermissionChecker {
    async fn needs_confirmation(&self, perm_type: PermissionType, _resource: &str) -> bool {
        // Only allow read operations (no confirmation needed for low-risk)
        matches!(perm_type.risk_level(), RiskLevel::High | RiskLevel::Medium)
    }

    async fn request_confirmation(&self, ctx: PermissionContext) -> Result<bool, PermissionError> {
        // Always deny
        Err(PermissionError::Denied(format!(
            "{} operation denied: {}",
            ctx.permission_type.description(),
            ctx.resource
        )))
    }

    fn grant_session_permission(&self, _perm_type: PermissionType, _resource: String) {
        // No-op since we don't allow grants
    }
}

/// Extension trait for PermissionChecker with convenience methods
#[async_trait]
pub trait PermissionCheckerExt: PermissionChecker {
    /// Check if file write is allowed
    async fn check_write_file(&self, path: &str) -> Result<(), PermissionError> {
        let ctx = PermissionContext::new(
            PermissionType::WriteFile,
            path,
            format!("Write file: {}", path),
        );

        if self.check_or_request(ctx).await? {
            Ok(())
        } else {
            Err(PermissionError::Denied(format!(
                "Write permission denied for: {}",
                path
            )))
        }
    }

    /// Check if command execution is allowed
    async fn check_execute_command(&self, command: &str) -> Result<(), PermissionError> {
        let ctx = PermissionContext::new(
            PermissionType::ExecuteCommand,
            command,
            format!("Execute command: {}", command),
        );

        if self.check_or_request(ctx).await? {
            Ok(())
        } else {
            Err(PermissionError::Denied(format!(
                "Command execution denied for: {}",
                command
            )))
        }
    }

    /// Check if HTTP request is allowed
    async fn check_http_request(&self, url: &str) -> Result<(), PermissionError> {
        let ctx = PermissionContext::new(
            PermissionType::HttpRequest,
            url,
            format!("HTTP request to: {}", url),
        );

        if self.check_or_request(ctx).await? {
            Ok(())
        } else {
            Err(PermissionError::Denied(format!(
                "HTTP request denied for: {}",
                url
            )))
        }
    }

    /// Check if delete operation is allowed
    async fn check_delete(&self, path: &str) -> Result<(), PermissionError> {
        let ctx = PermissionContext::new(
            PermissionType::DeleteOperation,
            path,
            format!("Delete: {}", path),
        );

        if self.check_or_request(ctx).await? {
            Ok(())
        } else {
            Err(PermissionError::Denied(format!(
                "Delete permission denied for: {}",
                path
            )))
        }
    }

    /// Check if Git write operation is allowed
    async fn check_git_write(&self, operation: &str) -> Result<(), PermissionError> {
        let ctx = PermissionContext::new(
            PermissionType::GitWrite,
            operation,
            format!("Git operation: {}", operation),
        );

        if self.check_or_request(ctx).await? {
            Ok(())
        } else {
            Err(PermissionError::Denied(format!(
                "Git write denied for: {}",
                operation
            )))
        }
    }

    /// Check if terminal session is allowed
    async fn check_terminal_session(&self, command: &str) -> Result<(), PermissionError> {
        let ctx = PermissionContext::new(
            PermissionType::TerminalSession,
            command,
            format!("Terminal session: {}", command),
        );

        if self.check_or_request(ctx).await? {
            Ok(())
        } else {
            Err(PermissionError::Denied(format!(
                "Terminal session denied for: {}",
                command
            )))
        }
    }
}

#[async_trait]
impl<T: PermissionChecker + ?Sized> PermissionCheckerExt for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_allow_all_checker() {
        let checker = AllowAllPermissionChecker;

        assert!(!checker.needs_confirmation(PermissionType::WriteFile, "/tmp/test").await);
        assert!(!checker.needs_confirmation(PermissionType::ExecuteCommand, "rm -rf /").await);

        let ctx = PermissionContext::new(PermissionType::WriteFile, "/tmp/test", "test");
        assert!(checker.request_confirmation(ctx).await.unwrap());
    }

    #[tokio::test]
    async fn test_deny_dangerous_checker() {
        let checker = DenyDangerousPermissionChecker;

        assert!(checker.needs_confirmation(PermissionType::WriteFile, "/tmp/test").await);
        assert!(checker.needs_confirmation(PermissionType::ExecuteCommand, "ls").await);
    }

    #[tokio::test]
    async fn test_config_checker() {
        let config = Arc::new(PermissionConfig::new());
        let checker = ConfigPermissionChecker::new(config);

        // By default, should need confirmation
        assert!(checker.needs_confirmation(PermissionType::WriteFile, "/tmp/test").await);

        // After granting session permission, should not need confirmation
        checker.grant_session_permission(PermissionType::WriteFile, "/tmp/*".to_string());
        assert!(!checker.needs_confirmation(PermissionType::WriteFile, "/tmp/test").await);
    }

    #[test]
    fn test_permission_context() {
        let ctx = PermissionContext::new(
            PermissionType::WriteFile,
            "/tmp/test.txt",
            "Write configuration file",
        );

        assert_eq!(ctx.permission_type, PermissionType::WriteFile);
        assert_eq!(ctx.resource, "/tmp/test.txt");
        assert!(ctx.operation_description.contains("Write configuration"));
        assert_eq!(ctx.risk_level(), RiskLevel::Medium);

        let message = ctx.format_request_message();
        assert!(message.contains("Medium Risk"));
        assert!(message.contains("/tmp/test.txt"));
    }
}
