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

    /// Override the active permission mode at runtime.
    ///
    /// Used by headless entrypoints (e.g. `bamboo -p --permission-mode=bypass`)
    /// that have no interactive approver, so a tool-using run is not stranded at
    /// the first gated tool. The default is a no-op; mode-aware implementations
    /// apply it to their shared config so it takes effect for subsequent checks.
    fn set_permission_mode(&self, _mode: PermissionMode) {}

    /// Access the underlying mutable [`PermissionConfig`] when the implementation
    /// is config-backed. Used by settings/admin endpoints to read and update
    /// persisted rules (e.g. the "always ask" patterns). The default returns
    /// `None`; config-backed implementations return their shared config.
    fn permission_config(&self) -> Option<Arc<PermissionConfig>> {
        None
    }

    /// Whether this tool call matches an "always ask" rule (configured pattern
    /// or built-in dangerous-command detection) and must therefore force a user
    /// confirmation REGARDLESS of the active permission mode — including
    /// `BypassPermissions`. The default returns `false`; config-backed
    /// implementations consult their [`PermissionConfig`].
    fn requires_forced_confirmation(&self, _tool_name: &str, _args: &serde_json::Value) -> bool {
        false
    }

    /// Like [`check_or_request`](Self::check_or_request) but IGNORES the active
    /// permission mode/bypass. Used to enforce "always ask" rules even under
    /// bypass. The default delegates to `check_or_request`, which is correct for
    /// mode-unaware implementations; mode-aware wrappers override this to route
    /// through their inner (mode-unaware) checker.
    async fn check_or_request_forced(
        &self,
        ctx: PermissionContext,
    ) -> Result<bool, PermissionError> {
        self.check_or_request(ctx).await
    }

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

    fn requires_forced_confirmation(&self, tool_name: &str, args: &serde_json::Value) -> bool {
        self.config.requires_forced_confirmation(tool_name, args)
    }

    fn permission_config(&self) -> Option<Arc<PermissionConfig>> {
        Some(self.config.clone())
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

    fn requires_forced_confirmation(&self, tool_name: &str, args: &serde_json::Value) -> bool {
        self.inner.requires_forced_confirmation(tool_name, args)
    }

    async fn check_or_request_forced(
        &self,
        ctx: PermissionContext,
    ) -> Result<bool, PermissionError> {
        self.inner.check_or_request_forced(ctx).await
    }

    fn permission_config(&self) -> Option<Arc<PermissionConfig>> {
        self.inner.permission_config()
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

/// A permission checker for a READ-ONLY Guardian reviewer's Bash.
///
/// The Guardian reviewer is given a tool DENYLIST (`guardian_read_only_disabled_tools`)
/// that strips every mutating tool but keeps `Bash` so the reviewer can fetch the
/// diff and run tests. That left `Bash` unrestricted — a "read-only" reviewer
/// could still `rm -rf`, `git push --force`, `curl … | sh`, or `> file`. This
/// checker closes that gap: it wraps an inner checker (the worker's High-threshold
/// [`ConfigPermissionChecker`]) and, for `ExecuteCommand`, allows ONLY commands in
/// the read-only allowlist ([`is_read_only_command`]) — without gating them (so
/// `cargo test` runs freely) — and DENIES everything else, failing closed (the
/// reviewer has no human to approve). Every other permission type delegates to the
/// inner checker; the reviewer's other mutating tools are already removed by the
/// denylist, so they never reach here.
pub struct GuardianReadOnlyChecker {
    inner: Arc<dyn PermissionChecker>,
}

impl std::fmt::Debug for GuardianReadOnlyChecker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GuardianReadOnlyChecker").finish()
    }
}

impl GuardianReadOnlyChecker {
    /// Wrap `inner`, enforcing the read-only Bash allowlist on top of it.
    pub fn new(inner: Arc<dyn PermissionChecker>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl PermissionChecker for GuardianReadOnlyChecker {
    async fn needs_confirmation(&self, perm_type: PermissionType, resource: &str) -> bool {
        if perm_type == PermissionType::ExecuteCommand {
            // A read-only command runs WITHOUT a gate (so `cargo test` is free);
            // anything else "needs confirmation" — which, with no human, denies.
            return !is_read_only_command(resource);
        }
        // Other tools are removed by the reviewer's denylist; delegate safely.
        self.inner.needs_confirmation(perm_type, resource).await
    }

    async fn request_confirmation(&self, ctx: PermissionContext) -> Result<bool, PermissionError> {
        // Fail closed: a non-read-only command is DENIED outright (not routed to
        // an approver / model-reviewer), so the read-only guarantee is hard — a
        // reviewer's Bash can NEVER run a mutating command, period.
        if ctx.permission_type == PermissionType::ExecuteCommand
            && !is_read_only_command(&ctx.resource)
        {
            return Err(PermissionError::Denied(format!(
                "Guardian reviewer is read-only: command not allowed: {}",
                ctx.resource
            )));
        }
        self.inner.request_confirmation(ctx).await
    }

    async fn check_or_request(&self, ctx: PermissionContext) -> Result<bool, PermissionError> {
        // Read-only commands are auto-allowed; non-read-only ones are hard-denied.
        if ctx.permission_type == PermissionType::ExecuteCommand {
            return if is_read_only_command(&ctx.resource) {
                Ok(true)
            } else {
                Err(PermissionError::Denied(format!(
                    "Guardian reviewer is read-only: command not allowed: {}",
                    ctx.resource
                )))
            };
        }
        self.inner.check_or_request(ctx).await
    }

    async fn check_or_request_forced(
        &self,
        ctx: PermissionContext,
    ) -> Result<bool, PermissionError> {
        // A forced (always-ask) ExecuteCommand is held to the SAME hard rule, so a
        // dangerous-command pattern can't slip past the read-only guarantee.
        if ctx.permission_type == PermissionType::ExecuteCommand {
            return if is_read_only_command(&ctx.resource) {
                Ok(true)
            } else {
                Err(PermissionError::Denied(format!(
                    "Guardian reviewer is read-only: command not allowed: {}",
                    ctx.resource
                )))
            };
        }
        self.inner.check_or_request_forced(ctx).await
    }

    fn grant_session_permission(&self, perm_type: PermissionType, resource: String) {
        self.inner.grant_session_permission(perm_type, resource);
    }

    fn set_permission_mode(&self, mode: PermissionMode) {
        self.inner.set_permission_mode(mode);
    }

    fn requires_forced_confirmation(&self, tool_name: &str, args: &serde_json::Value) -> bool {
        self.inner.requires_forced_confirmation(tool_name, args)
    }

    fn permission_config(&self) -> Option<Arc<PermissionConfig>> {
        self.inner.permission_config()
    }
}

/// Shell commands that are considered safe for auto-approval in AcceptEdits mode.
const SAFE_EDIT_COMMANDS: &[&str] = &[
    "mkdir",
    "touch",
    "cp",
    "mv",
    "ls",
    "cat",
    "echo",
    "pwd",
    "chmod",
    "chown",
    "git status",
    "git diff",
    "git log",
    "git add",
    "git commit",
    "cargo check",
    "cargo build",
    "cargo test",
    "cargo clippy",
    "npm run",
    "npm test",
    "npm install",
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

    // Strip wrappers (time, nohup, timeout, nice, env) and their arguments.
    let stripped = strip_command_wrappers(trimmed);
    if stripped.is_empty() {
        return false;
    }

    let cmd = stripped.join(" ");

    for &safe_cmd in SAFE_EDIT_COMMANDS {
        if cmd == safe_cmd {
            return true;
        }
        if let Some(after) = cmd.strip_prefix(safe_cmd) {
            if after.is_empty() || after.starts_with(' ') {
                return true;
            }
        }
    }

    false
}

/// Read-only `git` subcommands the Guardian reviewer may run (inspection only —
/// NO add/commit/push/pull/checkout/reset/rebase/merge/stash/clean/rm/mv/tag).
const GUARDIAN_GIT_SUBCOMMANDS: &[&str] = &[
    // Read-only inspection only. NOT `branch` (its -d/-D/-m mutate refs), and NOT
    // add/commit/push/pull/checkout/reset/rebase/merge/stash/clean/rm/mv/tag.
    "status",
    "diff",
    "log",
    "show",
    "blame",
    "rev-parse",
    "ls-files",
    "diff-tree",
    "cat-file",
    "describe",
];

/// Read-only / test / build `cargo` subcommands the Guardian reviewer may run
/// (NO run/publish/install/clean — those execute or mutate; NO `fmt` — it
/// rewrites source files in place unless `--check`, which an allowlist can't
/// require cheaply).
const GUARDIAN_CARGO_SUBCOMMANDS: &[&str] = &[
    "check", "build", "test", "clippy", "tree", "metadata", "nextest",
];

/// Plain read-only/inspection tools the Guardian reviewer may run directly. These
/// neither mutate the filesystem nor reach the network. (`echo`/`true` are inert;
/// they only matter as the tail of a pipe.) Deliberately EXCLUDES tools whose
/// flags can write/exec: `sort`/`tree` (`-o` writes), `uniq` (positional output
/// arg). `find`/`fd`/`rg` are included but their exec/delete escape-hatch flags
/// are rejected in `segment_is_read_only`.
const GUARDIAN_READ_ONLY_COMMANDS: &[&str] = &[
    "ls", "cat", "head", "tail", "wc", "grep", "rg", "find", "fd", "file", "stat", "pwd", "echo",
    "true", "cut", "nl", "column", "diff", "du", "df", "basename", "dirname", "realpath",
    "readlink", "which", "type", "uname", "hostname",
];

/// Per-command flags that turn an otherwise-read-only tool into an
/// arbitrary-execution or delete/write surface. Rejected before the allowlist
/// check so e.g. `find … -exec rm {} +` / `fd -x rm` / `rg --pre sh` don't slip
/// through on the base-command name alone.
fn segment_has_dangerous_flag(base: &str, args: &[&str]) -> bool {
    match base {
        "find" => args.iter().any(|a| {
            matches!(
                *a,
                "-exec"
                    | "-execdir"
                    | "-ok"
                    | "-okdir"
                    | "-delete"
                    | "-fprint"
                    | "-fprintf"
                    | "-fls"
            )
        }),
        "fd" => args
            .iter()
            .any(|a| *a == "-x" || *a == "-X" || a.starts_with("--exec")),
        "rg" => args
            .iter()
            .any(|a| a.starts_with("--pre") || *a == "--hostname-bin"),
        _ => false,
    }
}

/// Whether a single (already wrapper-stripped) command segment's base command is
/// in the strict read-only allowlist. `git`/`cargo` additionally require their
/// subcommand (token 1) to be in the respective read-only subcommand list.
fn segment_is_read_only(segment: &str) -> bool {
    let tokens: Vec<&str> = strip_command_wrappers(segment);
    let Some(&base) = tokens.first() else {
        return false;
    };
    let args = &tokens[1..];
    // Reject exec/delete escape-hatch flags on otherwise-read-only tools.
    if segment_has_dangerous_flag(base, args) {
        return false;
    }
    match base {
        "git" => tokens
            .get(1)
            .is_some_and(|sub| GUARDIAN_GIT_SUBCOMMANDS.contains(sub)),
        "cargo" => tokens
            .get(1)
            .is_some_and(|sub| GUARDIAN_CARGO_SUBCOMMANDS.contains(sub)),
        other => GUARDIAN_READ_ONLY_COMMANDS.contains(&other),
    }
}

/// Whether `command` is a read-only command the Guardian reviewer may run.
///
/// The Guardian reviewer keeps an unrestricted-looking `Bash` so it can fetch the
/// diff and run tests, but a true read-only guarantee means its shell must NOT be
/// able to mutate the workspace, push, exfiltrate, or run arbitrary interpreters.
/// This is the allowlist that closes that gap (see `guardian_read_only_disabled_tools`).
///
/// Rules:
/// 1. Reject any command containing shell chaining/redirection that could hide a
///    mutation: `;`, `&&`, `||`, `&`, `>`, `<`, backtick, `$(`, `${`, or a
///    newline. The ONE exception is `|` pipes — allowed, but then EVERY pipe
///    segment's base command must independently be in the allowlist.
/// 2. After stripping wrappers (time/nohup/timeout/nice/env), the base command
///    (token 0; for `git`/`cargo` also the subcommand) must be in the strict
///    read-only allowlist ([`GUARDIAN_GIT_SUBCOMMANDS`] /
///    [`GUARDIAN_CARGO_SUBCOMMANDS`] / [`GUARDIAN_READ_ONLY_COMMANDS`]).
///
/// Everything else (rm/mv/cp/mkdir/touch/chmod/chown/ln/dd/tee/sed/awk/curl/wget/
/// ssh/nc/python/node/sh/bash/zsh/eval/npm/pip/make/…) returns `false`.
pub fn is_read_only_command(command: &str) -> bool {
    let trimmed = command.trim();
    if trimmed.is_empty() {
        return false;
    }

    // Rule 1: reject shell metacharacters that could hide a mutation. `|` is the
    // sole exception (handled below by per-segment validation); but `||` (logical
    // OR) and `&` (background / `&&`) are rejected, so scan with that nuance.
    let bytes = trimmed.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b';' | b'<' | b'>' | b'&' | b'`' => return false,
            b'\n' | b'\r' => return false,
            b'$' => {
                // `$(` command substitution and `${` parameter expansion are both
                // mutation/exfiltration vectors.
                if matches!(bytes.get(i + 1), Some(b'(') | Some(b'{')) {
                    return false;
                }
            }
            b'|' => {
                // A doubled `|` is logical OR (chaining) → reject; a single `|`
                // is a pipe → allowed, validated per-segment below.
                if bytes.get(i + 1) == Some(&b'|') {
                    return false;
                }
            }
            _ => {}
        }
        i += 1;
    }

    // Rule 2: split on single `|` pipes and require EVERY segment to be a read-only
    // base command. (A command with no pipe is a single segment.)
    trimmed
        .split('|')
        .all(|segment| segment_is_read_only(segment))
}

/// Split a command into tokens with leading wrappers (time/nohup/timeout/nice/env)
/// stripped, returning the remaining tokens (base command first). Shared by
/// [`is_safe_edit_command`] and [`is_read_only_command`].
fn strip_command_wrappers(command: &str) -> Vec<&str> {
    let tokens: Vec<&str> = command.split_whitespace().collect();
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
    tokens[idx..].to_vec()
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
                if perm_type == PermissionType::ExecuteCommand && is_safe_edit_command(resource) {
                    return false;
                }
                self.inner.needs_confirmation(perm_type, resource).await
            }
            PermissionMode::DontAsk => {
                // Only allow if explicitly whitelisted; otherwise deny (needs_confirmation=true)
                !matches!(
                    self.config.is_whitelist_allowed(perm_type, resource),
                    Some(true)
                )
            }
            PermissionMode::Default => self.inner.needs_confirmation(perm_type, resource).await,
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
                if ctx.permission_type == PermissionType::WriteFile
                    || (ctx.permission_type == PermissionType::ExecuteCommand
                        && is_safe_edit_command(&ctx.resource))
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

    fn set_permission_mode(&self, mode: PermissionMode) {
        // Shared `Arc<PermissionConfig>` with `inner`, and `mode()` is read per
        // check, so this takes effect immediately for subsequent gating.
        self.config.set_mode(mode);
    }

    fn requires_forced_confirmation(&self, tool_name: &str, args: &serde_json::Value) -> bool {
        self.config.requires_forced_confirmation(tool_name, args)
    }

    async fn check_or_request_forced(
        &self,
        ctx: PermissionContext,
    ) -> Result<bool, PermissionError> {
        // Route through the inner (mode-unaware) checker so the active mode —
        // including BypassPermissions — does NOT suppress the forced prompt.
        // Session grants still short-circuit, so a re-attempt after approval
        // passes.
        self.inner.check_or_request(ctx).await
    }

    fn permission_config(&self) -> Option<Arc<PermissionConfig>> {
        Some(self.config.clone())
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
    use crate::PermissionRule;

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

    // --- is_read_only_command tests ---

    #[test]
    fn read_only_command_allows_read_test_build() {
        // Plain read tools.
        assert!(is_read_only_command("ls"));
        assert!(is_read_only_command("ls -la src/"));
        assert!(is_read_only_command("cat Cargo.toml"));
        assert!(is_read_only_command("rg foo src/"));
        assert!(is_read_only_command("grep -rn foo ."));
        assert!(is_read_only_command("find . -name '*.rs'"));
        assert!(is_read_only_command("pwd"));
        // git read-only subcommands.
        assert!(is_read_only_command("git status"));
        assert!(is_read_only_command("git diff"));
        assert!(is_read_only_command("git diff HEAD~1"));
        assert!(is_read_only_command("git log --oneline -20"));
        assert!(is_read_only_command("git show HEAD"));
        // cargo read-only / test / build subcommands.
        assert!(is_read_only_command("cargo test"));
        assert!(is_read_only_command("cargo test --workspace"));
        assert!(is_read_only_command("cargo check"));
        assert!(is_read_only_command("cargo clippy --all"));
        assert!(is_read_only_command("cargo nextest run"));
        // Allowed wrappers stripped before the base command is checked. (NOTE:
        // the shared `timeout`/`nice`/`env` stripping consumes ONE following token
        // as the wrapper's own argument — `timeout 60 cmd`, `env A=1 cmd` — so a
        // separating arg must be present; `nohup` consumes none.)
        assert!(is_read_only_command("timeout 60 cargo test"));
        assert!(is_read_only_command("nohup cargo build"));
        // Pipe: allowed when EVERY segment is read-only.
        assert!(is_read_only_command("git diff | head -50"));
        assert!(is_read_only_command("cat f.txt | grep foo | wc -l"));
        assert!(is_read_only_command("rg foo src/ | head -20"));
        // Read-only find usage (no exec/delete) is still allowed.
        assert!(is_read_only_command("find . -name '*.rs' -type f"));
    }

    #[test]
    fn read_only_command_denies_mutation_and_escapes() {
        // Empty / mutation.
        assert!(!is_read_only_command(""));
        assert!(!is_read_only_command("   "));
        assert!(!is_read_only_command("rm -rf x"));
        assert!(!is_read_only_command("mv a b"));
        assert!(!is_read_only_command("cp a b"));
        assert!(!is_read_only_command("mkdir d"));
        assert!(!is_read_only_command("touch f"));
        assert!(!is_read_only_command("chmod +x f"));
        assert!(!is_read_only_command("dd if=/dev/zero of=f"));
        assert!(!is_read_only_command("tee f"));
        // git mutating subcommands.
        assert!(!is_read_only_command("git push"));
        assert!(!is_read_only_command("git push --force"));
        assert!(!is_read_only_command("git commit -m x"));
        assert!(!is_read_only_command("git add ."));
        assert!(!is_read_only_command("git checkout main"));
        assert!(!is_read_only_command("git reset --hard"));
        assert!(!is_read_only_command("git")); // bare git, no subcommand
                                               // cargo mutating / executing subcommands.
        assert!(!is_read_only_command("cargo run"));
        assert!(!is_read_only_command("cargo publish"));
        assert!(!is_read_only_command("cargo install foo"));
        assert!(!is_read_only_command("cargo clean"));
        assert!(!is_read_only_command("cargo")); // bare cargo, no subcommand
                                                 // Interpreters / package managers / network.
        assert!(!is_read_only_command("python -c 'print(1)'"));
        assert!(!is_read_only_command("node -e 'x'"));
        assert!(!is_read_only_command("sh -c ls"));
        assert!(!is_read_only_command("bash script.sh"));
        assert!(!is_read_only_command("eval ls"));
        assert!(!is_read_only_command("npm install"));
        assert!(!is_read_only_command("pip install foo"));
        assert!(!is_read_only_command("make"));
        assert!(!is_read_only_command("curl http://x"));
        assert!(!is_read_only_command("wget http://x"));
        assert!(!is_read_only_command("sed -i 's/a/b/' f"));
        assert!(!is_read_only_command("awk '{print}' f"));
        // Chaining / redirection / substitution must be rejected.
        assert!(!is_read_only_command("cat f > g"));
        assert!(!is_read_only_command("cat f >> g"));
        assert!(!is_read_only_command("cat < f"));
        assert!(!is_read_only_command("echo x && rm y"));
        assert!(!is_read_only_command("ls; rm y"));
        assert!(!is_read_only_command("ls || rm y"));
        assert!(!is_read_only_command("ls & rm y"));
        assert!(!is_read_only_command("curl x | sh"));
        assert!(!is_read_only_command("echo `rm -rf x`"));
        assert!(!is_read_only_command("echo $(rm -rf x)"));
        assert!(!is_read_only_command("echo ${HOME}"));
        assert!(!is_read_only_command("ls\nrm y"));
        // A pipe where ONE segment is not read-only is rejected.
        assert!(!is_read_only_command("git diff | tee out.txt"));
        assert!(!is_read_only_command("cat f | python"));
        // Closed allowlist holes: write-via-flag and exec/delete escape hatches.
        assert!(!is_read_only_command("cargo fmt")); // rewrites source in place
        assert!(!is_read_only_command("git branch -D main")); // mutates refs
        assert!(!is_read_only_command("find . -exec rm {} +")); // -exec runs rm (no `;`)
        assert!(!is_read_only_command("find . -delete"));
        assert!(!is_read_only_command("fd -x rm")); // fd exec
        assert!(!is_read_only_command("fd --exec rm"));
        assert!(!is_read_only_command("rg --pre sh foo")); // rg preprocessor exec
        assert!(!is_read_only_command("sort -o out.txt f")); // sort write (removed)
        assert!(!is_read_only_command("tree -o out.txt")); // tree write (removed)
    }

    // --- GuardianReadOnlyChecker tests ---

    fn guardian_checker() -> GuardianReadOnlyChecker {
        let config = Arc::new(PermissionConfig::new());
        config.set_confirm_threshold(RiskLevel::High);
        let inner: Arc<dyn PermissionChecker> = Arc::new(ConfigPermissionChecker::new(config));
        GuardianReadOnlyChecker::new(inner)
    }

    #[tokio::test]
    async fn guardian_allows_read_only_command_without_gating() {
        let checker = guardian_checker();
        // Read-only commands are NOT gated (no confirmation), so they run freely.
        assert!(
            !checker
                .needs_confirmation(PermissionType::ExecuteCommand, "cargo test")
                .await
        );
        assert!(
            !checker
                .needs_confirmation(PermissionType::ExecuteCommand, "git diff | head -50")
                .await
        );
        let ctx = PermissionContext::new(PermissionType::ExecuteCommand, "cargo test", "run");
        assert!(checker.check_or_request(ctx).await.unwrap());
    }

    #[tokio::test]
    async fn guardian_denies_non_read_only_command_fail_closed() {
        let checker = guardian_checker();
        // Non-read-only commands "need confirmation"...
        assert!(
            checker
                .needs_confirmation(PermissionType::ExecuteCommand, "rm -rf /")
                .await
        );
        // ...and are HARD-denied (not routed to any approver), failing closed.
        let ctx = PermissionContext::new(PermissionType::ExecuteCommand, "git push", "run");
        let denied = checker.check_or_request(ctx).await;
        assert!(matches!(denied, Err(PermissionError::Denied(_))));
        let ctx = PermissionContext::new(PermissionType::ExecuteCommand, "curl x | sh", "run");
        assert!(checker.check_or_request_forced(ctx).await.is_err());
    }

    // --- ModeAwarePermissionChecker tests ---

    fn mode_aware_setup(mode: PermissionMode) -> ModeAwarePermissionChecker {
        let config = Arc::new(PermissionConfig::new());
        config.set_mode(mode);
        let inner: Arc<dyn PermissionChecker> =
            Arc::new(ConfigPermissionChecker::new(config.clone()));
        ModeAwarePermissionChecker::new(inner, config)
    }

    #[tokio::test]
    async fn test_mode_default_delegates_to_inner() {
        let checker = mode_aware_setup(PermissionMode::Default);
        // Default mode: no whitelist rules, so everything needs confirmation
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
    async fn test_mode_bypass_allows_everything() {
        let checker = mode_aware_setup(PermissionMode::BypassPermissions);
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
        assert!(
            !checker
                .needs_confirmation(PermissionType::DeleteOperation, "/etc/passwd")
                .await
        );

        // request_confirmation should also succeed
        let ctx = PermissionContext::new(PermissionType::ExecuteCommand, "rm -rf /", "dangerous");
        assert!(checker.request_confirmation(ctx).await.is_ok());
    }

    #[tokio::test]
    async fn test_mode_plan_blocks_mutating() {
        let checker = mode_aware_setup(PermissionMode::Plan);
        // Plan mode: high/medium risk operations need confirmation (blocked)
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
        assert!(
            checker
                .needs_confirmation(PermissionType::DeleteOperation, "/tmp/file")
                .await
        );

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
        assert!(
            !checker
                .needs_confirmation(PermissionType::WriteFile, "/tmp/test")
                .await
        );
        // ExecuteCommand should still need confirmation (delegated to inner)
        assert!(
            checker
                .needs_confirmation(PermissionType::ExecuteCommand, "rm -rf /")
                .await
        );
    }

    #[tokio::test]
    async fn test_mode_dont_ask_denies_unless_whitelisted() {
        let config = Arc::new(PermissionConfig::new());
        config.set_mode(PermissionMode::DontAsk);
        // Add a whitelist allow rule
        config.add_rule(PermissionRule::new(
            PermissionType::WriteFile,
            "/safe/*",
            true,
        ));

        let inner: Arc<dyn PermissionChecker> =
            Arc::new(ConfigPermissionChecker::new(config.clone()));
        let checker = ModeAwarePermissionChecker::new(inner, config);

        // Whitelisted path: allowed
        assert!(
            !checker
                .needs_confirmation(PermissionType::WriteFile, "/safe/file.rs")
                .await
        );
        // Non-whitelisted path: denied (needs_confirmation=true)
        assert!(
            checker
                .needs_confirmation(PermissionType::WriteFile, "/unsafe/file.rs")
                .await
        );

        // request_confirmation should deny
        let ctx = PermissionContext::new(PermissionType::WriteFile, "/unsafe/file.rs", "write");
        let result = checker.request_confirmation(ctx).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("dontAsk"));
    }

    #[tokio::test]
    async fn test_mode_switches_at_runtime() {
        let config = Arc::new(PermissionConfig::new());
        let inner: Arc<dyn PermissionChecker> =
            Arc::new(ConfigPermissionChecker::new(config.clone()));
        let checker = ModeAwarePermissionChecker::new(inner, config.clone());

        // Start in Default mode
        assert!(
            checker
                .needs_confirmation(PermissionType::WriteFile, "/tmp/test")
                .await
        );

        // Switch to Bypass
        config.set_mode(PermissionMode::BypassPermissions);
        assert!(
            !checker
                .needs_confirmation(PermissionType::WriteFile, "/tmp/test")
                .await
        );

        // Switch to Plan
        config.set_mode(PermissionMode::Plan);
        assert!(
            checker
                .needs_confirmation(PermissionType::WriteFile, "/tmp/test")
                .await
        );
    }
}
