//! Permission checker trait and implementations.
//!
//! This module provides the [`PermissionChecker`] trait that defines how tools
//! check for permission before executing potentially dangerous operations.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::config::{PermissionConfig, PermissionMode, PermissionType, RiskLevel};

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
        tracing::debug!(
            "Permission check: {:?} for '{}' - needs_confirmation: {}",
            perm_type,
            resource,
            needs
        );
        needs
    }

    async fn request_confirmation(&self, ctx: PermissionContext) -> Result<bool, PermissionError> {
        tracing::info!(
            "Requesting user confirmation: {:?} for '{}'",
            ctx.permission_type,
            ctx.resource
        );
        let result = self.inner.request_confirmation(ctx).await;
        tracing::debug!("User confirmation result: {:?}", result);
        result
    }

    fn grant_session_permission(&self, perm_type: PermissionType, resource: String) {
        tracing::info!(
            "Granting session permission: {:?} for '{}'",
            perm_type,
            resource
        );
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

/// Shell commands that are considered safe for auto-approval in AcceptEdits mode.
const SAFE_EDIT_COMMANDS: &[&str] = &[
    "mkdir", "touch", "cp", "mv", "ls", "cat", "echo", "pwd", "chmod", "chown",
    "git status", "git diff", "git log", "git add", "git commit",
    "cargo check", "cargo build", "cargo test", "cargo clippy",
    "npm run", "npm test", "npm install",
];

/// Command wrappers stripped before checking the base command.
const COMMAND_WRAPPERS: &[&str] = &["time", "nohup", "timeout", "nice", "env"];

/// Check if a command is safe for auto-approval in AcceptEdits mode.
///
/// Strips wrappers (time, nohup, timeout, nice, env), then checks against
/// `SAFE_EDIT_COMMANDS` with prefix matching (e.g., `"git add"` matches `"git add file.txt"`).
pub fn is_safe_edit_command(command: &str) -> bool {
    let trimmed = command.trim();
    if trimmed.is_empty() {
        return false;
    }

    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    if tokens.is_empty() {
        return false;
    }

    // Strip wrappers and their arguments
    let mut idx = 0;
    while idx < tokens.len() {
        let token = tokens[idx];
        if !COMMAND_WRAPPERS.contains(&token) {
            break;
        }
        idx += 1;
        while idx < tokens.len() {
            let next = tokens[idx];
            if next.starts_with('-') {
                idx += 1;
                continue;
            }
            if COMMAND_WRAPPERS.contains(&next) {
                break;
            }
            if ["timeout", "nice", "env"].contains(&token) {
                idx += 1;
            }
            break;
        }
    }

    if idx >= tokens.len() {
        return false;
    }

    let cmd = tokens[idx..].join(" ");

    for &safe_cmd in SAFE_EDIT_COMMANDS {
        if cmd == safe_cmd {
            return true;
        }
        if cmd.starts_with(safe_cmd) {
            let after = &cmd[safe_cmd.len()..];
            if after.is_empty() || after.starts_with(' ') {
                return true;
            }
        }
    }

    false
}

/// A permission checker that applies mode-specific logic on top of an inner checker.
///
/// The active `PermissionMode` is read from the shared `PermissionConfig` at check time,
/// so mode changes take effect immediately.
pub struct ModeAwarePermissionChecker {
    inner: Arc<dyn PermissionChecker>,
    config: Arc<PermissionConfig>,
}

impl std::fmt::Debug for ModeAwarePermissionChecker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModeAwarePermissionChecker")
            .field("mode", &self.config.mode())
            .finish()
    }
}

impl ModeAwarePermissionChecker {
    /// Create a new mode-aware checker wrapping `inner`, reading mode from `config`.
    pub fn new(inner: Arc<dyn PermissionChecker>, config: Arc<PermissionConfig>) -> Self {
        Self { inner, config }
    }
}

#[async_trait]
impl PermissionChecker for ModeAwarePermissionChecker {
    async fn needs_confirmation(&self, perm_type: PermissionType, resource: &str) -> bool {
        match self.config.mode() {
            PermissionMode::BypassPermissions => false,
            PermissionMode::Plan => {
                // In plan mode, all non-low-risk operations require confirmation (= are blocked)
                perm_type.risk_level() != RiskLevel::Low
            }
            PermissionMode::AcceptEdits => {
                // Auto-approve file writes
                if perm_type == PermissionType::WriteFile {
                    return false;
                }
                // Auto-approve safe edit commands
                if perm_type == PermissionType::ExecuteCommand
                    && is_safe_edit_command(resource)
                {
                    return false;
                }
                self.inner.needs_confirmation(perm_type, resource).await
            }
            PermissionMode::DontAsk => {
                // Only allow if explicitly whitelisted; otherwise deny (needs_confirmation=true)
                match self.config.is_whitelist_allowed(perm_type, resource) {
                    Some(true) => false,
                    _ => true,
                }
            }
            PermissionMode::Default => {
                self.inner.needs_confirmation(perm_type, resource).await
            }
        }
    }

    async fn request_confirmation(&self, ctx: PermissionContext) -> Result<bool, PermissionError> {
        match self.config.mode() {
            PermissionMode::BypassPermissions => Ok(true),
            PermissionMode::Plan => Err(PermissionError::Denied(format!(
                "Plan mode: {} operation blocked for '{}'",
                ctx.permission_type.description(),
                ctx.resource
            ))),
            PermissionMode::DontAsk => Err(PermissionError::Denied(format!(
                "Permission denied (dontAsk mode): {} on '{}'",
                ctx.permission_type.description(),
                ctx.resource
            ))),
            PermissionMode::AcceptEdits => {
                if ctx.permission_type == PermissionType::WriteFile {
                    Ok(true)
                } else if ctx.permission_type == PermissionType::ExecuteCommand
                    && is_safe_edit_command(&ctx.resource)
                {
                    Ok(true)
                } else {
                    self.inner.request_confirmation(ctx).await
                }
            }
            PermissionMode::Default => self.inner.request_confirmation(ctx).await,
        }
    }

    fn grant_session_permission(&self, perm_type: PermissionType, resource: String) {
        self.inner.grant_session_permission(perm_type, resource);
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
    use crate::permission::PermissionRule;

    #[tokio::test]
    async fn test_allow_all_checker() {
        let checker = AllowAllPermissionChecker;

        assert!(
            !checker
                .needs_confirmation(PermissionType::WriteFile, "/tmp/test")
                .await
        );
        assert!(
            !checker
                .needs_confirmation(PermissionType::ExecuteCommand, "rm -rf /")
                .await
        );

        let ctx = PermissionContext::new(PermissionType::WriteFile, "/tmp/test", "test");
        assert!(checker.request_confirmation(ctx).await.unwrap());
    }

    #[tokio::test]
    async fn test_deny_dangerous_checker() {
        let checker = DenyDangerousPermissionChecker;

        assert!(
            checker
                .needs_confirmation(PermissionType::WriteFile, "/tmp/test")
                .await
        );
        assert!(
            checker
                .needs_confirmation(PermissionType::ExecuteCommand, "ls")
                .await
        );
    }

    #[tokio::test]
    async fn test_config_checker() {
        let config = Arc::new(PermissionConfig::new());
        let checker = ConfigPermissionChecker::new(config);

        // By default, should need confirmation
        assert!(
            checker
                .needs_confirmation(PermissionType::WriteFile, "/tmp/test")
                .await
        );

        // After granting session permission, should not need confirmation
        checker.grant_session_permission(PermissionType::WriteFile, "/tmp/*".to_string());
        assert!(
            !checker
                .needs_confirmation(PermissionType::WriteFile, "/tmp/test")
                .await
        );
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

    // --- ModeAwarePermissionChecker tests ---

    fn mode_aware_setup(mode: PermissionMode) -> ModeAwarePermissionChecker {
        let config = Arc::new(PermissionConfig::new());
        config.set_mode(mode);
        let inner: Arc<dyn PermissionChecker> = Arc::new(ConfigPermissionChecker::new(config.clone()));
        ModeAwarePermissionChecker::new(inner, config)
    }

    #[tokio::test]
    async fn test_mode_default_delegates_to_inner() {
        let checker = mode_aware_setup(PermissionMode::Default);
        // Default mode: no whitelist rules, so everything needs confirmation
        assert!(checker.needs_confirmation(PermissionType::WriteFile, "/tmp/test").await);
        assert!(checker.needs_confirmation(PermissionType::ExecuteCommand, "ls").await);
    }

    #[tokio::test]
    async fn test_mode_bypass_allows_everything() {
        let checker = mode_aware_setup(PermissionMode::BypassPermissions);
        assert!(!checker.needs_confirmation(PermissionType::WriteFile, "/tmp/test").await);
        assert!(!checker.needs_confirmation(PermissionType::ExecuteCommand, "rm -rf /").await);
        assert!(!checker.needs_confirmation(PermissionType::DeleteOperation, "/etc/passwd").await);

        // request_confirmation should also succeed
        let ctx = PermissionContext::new(PermissionType::ExecuteCommand, "rm -rf /", "dangerous");
        assert!(checker.request_confirmation(ctx).await.is_ok());
    }

    #[tokio::test]
    async fn test_mode_plan_blocks_mutating() {
        let checker = mode_aware_setup(PermissionMode::Plan);
        // Plan mode: high/medium risk operations need confirmation (blocked)
        assert!(checker.needs_confirmation(PermissionType::WriteFile, "/tmp/test").await);
        assert!(checker.needs_confirmation(PermissionType::ExecuteCommand, "ls").await);
        assert!(checker.needs_confirmation(PermissionType::DeleteOperation, "/tmp/file").await);

        // request_confirmation should deny with Plan mode message
        let ctx = PermissionContext::new(PermissionType::WriteFile, "/tmp/test", "write");
        let result = checker.request_confirmation(ctx).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Plan mode"));
    }

    #[tokio::test]
    async fn test_mode_accept_edits_auto_approves_writes() {
        let checker = mode_aware_setup(PermissionMode::AcceptEdits);
        // WriteFile should be auto-approved
        assert!(!checker.needs_confirmation(PermissionType::WriteFile, "/tmp/test").await);
        // ExecuteCommand should still need confirmation (delegated to inner)
        assert!(checker.needs_confirmation(PermissionType::ExecuteCommand, "rm -rf /").await);
    }

    #[tokio::test]
    async fn test_mode_dont_ask_denies_unless_whitelisted() {
        let config = Arc::new(PermissionConfig::new());
        config.set_mode(PermissionMode::DontAsk);
        // Add a whitelist allow rule
        config.add_rule(PermissionRule::new(PermissionType::WriteFile, "/safe/*", true));

        let inner: Arc<dyn PermissionChecker> = Arc::new(ConfigPermissionChecker::new(config.clone()));
        let checker = ModeAwarePermissionChecker::new(inner, config);

        // Whitelisted path: allowed
        assert!(!checker.needs_confirmation(PermissionType::WriteFile, "/safe/file.rs").await);
        // Non-whitelisted path: denied (needs_confirmation=true)
        assert!(checker.needs_confirmation(PermissionType::WriteFile, "/unsafe/file.rs").await);

        // request_confirmation should deny
        let ctx = PermissionContext::new(PermissionType::WriteFile, "/unsafe/file.rs", "write");
        let result = checker.request_confirmation(ctx).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("dontAsk"));
    }

    #[tokio::test]
    async fn test_mode_switches_at_runtime() {
        let config = Arc::new(PermissionConfig::new());
        let inner: Arc<dyn PermissionChecker> = Arc::new(ConfigPermissionChecker::new(config.clone()));
        let checker = ModeAwarePermissionChecker::new(inner, config.clone());

        // Start in Default mode
        assert!(checker.needs_confirmation(PermissionType::WriteFile, "/tmp/test").await);

        // Switch to Bypass
        config.set_mode(PermissionMode::BypassPermissions);
        assert!(!checker.needs_confirmation(PermissionType::WriteFile, "/tmp/test").await);

        // Switch to Plan
        config.set_mode(PermissionMode::Plan);
        assert!(checker.needs_confirmation(PermissionType::WriteFile, "/tmp/test").await);
    }
}
