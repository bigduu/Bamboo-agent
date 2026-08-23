//! Permission configuration for tool execution.
//!
//! This module provides a flexible permission system for controlling access to
//! potentially dangerous operations like file writes, command execution, and HTTP requests.

use std::collections::HashMap;
use std::path::{Component, Path};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use bamboo_domain::poison::PoisonRecover;
use tracing::warn;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

// Re-export PermissionMode from the shared location in bamboo-infrastructure
pub use bamboo_config::settings::PermissionMode;

use crate::policy::{
    conservative_matchers, DurablePermissionRule, EffectivePermissionPolicy, PermissionDecision,
    PermissionDecisionReceipt, PermissionDecisionSource, PermissionDenyReason,
    PermissionEvaluation, PermissionMatcher, PermissionMatcherKind, PermissionOutcome,
    PermissionReasonCode, PermissionRequest, PermissionRuleEffect, PermissionRuleRef,
    PermissionRuleScope, PermissionRuleSource,
};
use crate::rule_parser::ParsedRule;

/// Types of permissions that can be granted
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionType {
    /// Permission to write files
    WriteFile,
    /// Permission to execute shell commands
    ExecuteCommand,
    /// Permission to perform Git write operations (commit, push, etc.)
    GitWrite,
    /// Permission to make HTTP requests
    HttpRequest,
    /// Permission to perform delete operations
    DeleteOperation,
    /// Permission for terminal sessions (long-running interactive commands)
    TerminalSession,
}

impl PermissionType {
    /// Get a human-readable description of this permission type
    pub fn description(&self) -> &'static str {
        match self {
            PermissionType::WriteFile => "Write files to disk",
            PermissionType::ExecuteCommand => "Execute shell commands",
            PermissionType::GitWrite => "Perform Git write operations (commit, push, etc.)",
            PermissionType::HttpRequest => "Make HTTP requests to external services",
            PermissionType::DeleteOperation => "Delete files or directories",
            PermissionType::TerminalSession => "Run interactive terminal sessions",
        }
    }

    /// Get the risk level of this permission type
    pub fn risk_level(&self) -> RiskLevel {
        match self {
            PermissionType::WriteFile => RiskLevel::Medium,
            PermissionType::ExecuteCommand => RiskLevel::High,
            PermissionType::GitWrite => RiskLevel::High,
            PermissionType::HttpRequest => RiskLevel::Medium,
            PermissionType::DeleteOperation => RiskLevel::High,
            PermissionType::TerminalSession => RiskLevel::High,
        }
    }
}

fn is_path_permission(perm_type: PermissionType) -> bool {
    matches!(
        perm_type,
        PermissionType::WriteFile | PermissionType::DeleteOperation
    )
}

fn should_normalize_path(perm_type: PermissionType, resource: &str) -> bool {
    is_path_permission(perm_type)
        && (perm_type == PermissionType::WriteFile || Path::new(resource).is_absolute())
}

/// Risk level for permission types
///
/// Ordered `Low < Medium < High` (derived from declaration order), so a risk
/// level can be compared against a confirmation threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
}

impl RiskLevel {
    /// Get a human-readable label for this risk level
    pub fn label(&self) -> &'static str {
        match self {
            RiskLevel::Low => "Low Risk",
            RiskLevel::Medium => "Medium Risk",
            RiskLevel::High => "High Risk",
        }
    }
}

/// A rule in the permission whitelist
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionRule {
    /// The type of permission this rule applies to
    pub tool_type: PermissionType,
    /// Pattern to match resources (e.g., "/Users/bigduu/project/*" or "*.rs")
    pub resource_pattern: String,
    /// Whether this rule allows or denies access
    pub allowed: bool,
    /// Optional expiration time for this rule
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl PermissionRule {
    /// Create a new permission rule
    pub fn new(
        tool_type: PermissionType,
        resource_pattern: impl Into<String>,
        allowed: bool,
    ) -> Self {
        Self {
            tool_type,
            resource_pattern: resource_pattern.into(),
            allowed,
            expires_at: None,
        }
    }

    /// Set an expiration time for this rule
    pub fn with_expiration(mut self, expires_at: chrono::DateTime<chrono::Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Check if this rule has expired
    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|exp| chrono::Utc::now() > exp)
            .unwrap_or(false)
    }

    /// Check if this rule matches the given permission type and resource
    pub fn matches(&self, perm_type: PermissionType, resource: &str) -> bool {
        if self.tool_type != perm_type {
            return false;
        }
        if self.is_expired() {
            return false;
        }

        // For file-related permissions, normalize the path
        // For other permissions (HTTP, commands, etc.), match directly
        let normalized_resource = if should_normalize_path(perm_type, resource) {
            canonicalize_path_for_matching(resource)
        } else {
            Some(resource.to_string())
        };

        let normalized_resource = match normalized_resource {
            Some(r) => r,
            None => return false,
        };

        // Use globset for proper glob matching
        let normalized_pattern = if should_normalize_path(perm_type, &self.resource_pattern) {
            canonicalize_path_pattern_for_matching(&self.resource_pattern)
        } else {
            Some(self.resource_pattern.clone())
        };
        normalized_pattern
            .as_deref()
            .is_some_and(|pattern| match_glob_pattern(pattern, &normalized_resource))
    }
}

/// Session-granted permission entry with expiration
#[derive(Debug, Clone)]
pub struct SessionGrant {
    /// When this grant was created
    pub granted_at: Instant,
    /// When this grant expires
    pub expires_at: Instant,
    /// The resource pattern this grant applies to
    pub resource_pattern: String,
}

impl SessionGrant {
    /// Create a new session grant with the given duration
    pub fn new(resource_pattern: impl Into<String>, duration: Duration) -> Self {
        let now = Instant::now();
        Self {
            granted_at: now,
            expires_at: now + duration,
            resource_pattern: resource_pattern.into(),
        }
    }

    /// Check if this grant has expired
    pub fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }

    /// Check if this grant matches the given resource
    ///
    /// # Arguments
    ///
    /// * `perm_type` - The type of permission being checked
    /// * `resource` - The resource to match against (path, URL, command, etc.)
    ///
    /// # Returns
    ///
    /// `true` if the grant matches and has not expired, `false` otherwise.
    pub fn matches(&self, perm_type: PermissionType, resource: &str) -> bool {
        if self.is_expired() {
            return false;
        }

        // For file-related permissions, normalize the path
        // For other permissions (HTTP, commands, etc.), match directly
        let normalized_resource = if should_normalize_path(perm_type, resource) {
            canonicalize_path_for_matching(resource)
        } else {
            Some(resource.to_string())
        };

        let normalized_resource = match normalized_resource {
            Some(r) => r,
            None => return false,
        };

        let normalized_pattern = if should_normalize_path(perm_type, &self.resource_pattern) {
            canonicalize_path_pattern_for_matching(&self.resource_pattern)
        } else {
            Some(self.resource_pattern.clone())
        };
        normalized_pattern
            .as_deref()
            .is_some_and(|pattern| match_glob_pattern(pattern, &normalized_resource))
    }
}

/// Session-scoped grant that preserves the server-issued typed matcher.
#[derive(Debug, Clone)]
struct TypedSessionGrant {
    granted_at: Instant,
    expires_at: Instant,
    matcher: PermissionMatcher,
}

impl TypedSessionGrant {
    fn new(matcher: PermissionMatcher, duration: Duration) -> Self {
        let now = Instant::now();
        Self {
            granted_at: now,
            expires_at: now + duration,
            matcher,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }

    fn matches(&self, permission_type: PermissionType, resource: &str) -> bool {
        !self.is_expired() && self.matcher.matches(permission_type, resource)
    }
}

/// Runtime lifetime of a temporary permission grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporaryPermissionGrantScope {
    /// Legacy process-wide session grant without a stable session id.
    UnscopedSession,
    /// Grant or deny remembered for one stable session until expiry.
    Session,
    /// Allow receipt bound to one request and consumed exactly once.
    OneShot,
}

/// Effective decision carried by a temporary permission grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporaryPermissionGrantEffect {
    Allow,
    Deny,
}

/// Read-only, non-durable projection of one active runtime permission grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporaryPermissionGrant {
    pub scope: TemporaryPermissionGrantScope,
    pub effect: TemporaryPermissionGrantEffect,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub permission_type: PermissionType,
    /// The matcher used by runtime enforcement. No tool arguments or credential
    /// payloads are added to this projection.
    pub matcher: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub granted_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Canonicalize the static prefix of a file matcher without changing its glob
/// suffix. This makes `/tmp/**` and `/private/tmp/**` the same matcher on macOS
/// while preserving component boundaries and never widening an invalid rule.
pub(crate) fn canonicalize_path_pattern_for_matching(pattern: &str) -> Option<String> {
    let normalized = normalize_path_separators(pattern.trim());
    if normalized.is_empty() || has_path_traversal(&normalized) {
        return None;
    }

    let first_glob = normalized.find(['*', '?', '[', '{']);
    let Some(first_glob) = first_glob else {
        // Keep legacy filename-only patterns such as `*.rs`; path-like exact
        // matchers must be absolute and canonical.
        return if Path::new(&normalized).is_absolute() {
            canonicalize_path_for_matching(&normalized)
        } else {
            Some(normalized)
        };
    };

    // A filename-only glob has no filesystem prefix to canonicalize.
    let Some(separator) = normalized[..first_glob].rfind('/') else {
        return Some(normalized);
    };
    let prefix = if separator == 0 {
        "/"
    } else {
        &normalized[..separator]
    };
    if !Path::new(prefix).is_absolute() {
        // Legacy relative rules have no workspace identity. Preserve their
        // exact historical semantics (without widening); traversal was already
        // rejected above.
        return Some(normalized);
    }
    let canonical_prefix = canonicalize_path_for_matching(prefix)?;
    let suffix = &normalized[separator..];
    if canonical_prefix.ends_with('/') && suffix.starts_with('/') {
        Some(format!("{}{}", canonical_prefix, &suffix[1..]))
    } else {
        Some(format!("{canonical_prefix}{suffix}"))
    }
}

/// Canonicalize a resource path before permission matching.
///
/// This function:
/// 1. Resolves symlinks using `std::fs::canonicalize()` to prevent symlink bypass attacks
/// 2. Normalizes path separators and removes `.` and `..` components
/// 3. Supports both Unix and Windows paths
/// 4. For non-existent paths, resolves the parent directory and appends the filename
/// 5. Falls back to basic normalization if filesystem operations fail
///
/// Returns `None` when:
/// - The path is not absolute
/// - The path contains parent directory traversal (`..`) in the original string
///
/// # Security
///
/// Always use this function to resolve paths before permission checking to prevent
/// symlink-based bypass attacks where an attacker creates a symlink in an allowed
/// directory pointing to a sensitive file.
///
/// The function attempts to resolve symlinks for maximum security, but falls back
/// to basic normalization if the path doesn't exist or cannot be accessed.
pub fn canonicalize_path_for_matching(path: &str) -> Option<String> {
    let path_obj = Path::new(path);

    // Require absolute paths
    if !path_obj.is_absolute() {
        warn!("Permission check rejected non-absolute path: {}", path);
        return None;
    }

    // Quick rejection: if the original path contains "..", reject it immediately
    // This prevents basic traversal attempts even if filesystem operations fail
    if has_path_traversal(path) {
        warn!("Permission check rejected path with traversal: {}", path);
        return None;
    }

    // Try to canonicalize the full path first (resolves symlinks for existing paths)
    if let Ok(canonical) = std::fs::canonicalize(path_obj) {
        // On Windows, canonicalize may return UNC paths like \\?\C:\foo\bar
        // We need to normalize this for pattern matching
        let canonical_str = canonical.to_str()?.to_string();

        #[cfg(windows)]
        {
            // Remove the \\?\ prefix if present (UNC path prefix)
            let normalized = if canonical_str.starts_with(r"\\?\") {
                &canonical_str[4..]
            } else {
                &canonical_str
            };
            // Convert backslashes to forward slashes for consistent pattern matching
            return Some(normalized.replace('\\', "/"));
        }

        #[cfg(not(windows))]
        {
            return Some(canonical_str);
        }
    }

    // Path doesn't exist - try to canonicalize parent directory
    if let Some(parent) = path_obj.parent() {
        if let Some(file_name) = path_obj.file_name() {
            // Canonicalize the parent directory (resolves symlinks)
            if let Ok(canonical_parent) = std::fs::canonicalize(parent) {
                // Reconstruct the path: canonical_parent + file_name
                let mut result = canonical_parent;
                result.push(file_name);

                // On Windows, normalize UNC paths for pattern matching
                #[cfg(windows)]
                {
                    let result_str = result.to_str()?.to_string();
                    let normalized = if result_str.starts_with(r"\\?\") {
                        &result_str[4..]
                    } else {
                        &result_str
                    };
                    return Some(normalized.replace('\\', "/"));
                }

                #[cfg(not(windows))]
                {
                    return Some(result.to_str()?.to_string());
                }
            }
        }
    }

    // Fallback: basic normalization without filesystem access
    // This handles test environments and unusual error conditions
    let normalized = normalize_path_basic(path);
    Some(normalized)
}

/// Basic path normalization without filesystem access.
///
/// This function:
/// - Removes redundant slashes
/// - Removes `.` components
/// - Rejects `..` components (already checked by caller)
/// - Normalizes to forward slashes for cross-platform pattern matching
///
/// This is a fallback when `canonicalize_path_for_matching` cannot access
/// the filesystem. It does NOT resolve symlinks, so it's less secure than
/// the full canonicalization.
///
/// # Platform-specific behavior
///
/// On Windows, backslashes are converted to forward slashes for consistent
/// pattern matching. On Unix, the path is left as-is.
fn normalize_path_basic(path: &str) -> String {
    // Always replace backslashes with forward slashes for cross-platform consistency
    // This allows Windows paths to be tested on Unix systems
    let path = path.replace('\\', "/");

    let components: Vec<&str> = path
        .split('/')
        .filter(|s| !s.is_empty() && *s != ".")
        .collect();

    // Handle Windows paths with drive letters (e.g., "C:/Users/foo")
    // The drive letter will be the first component after splitting
    if !components.is_empty() && components[0].ends_with(':') {
        // Windows path with drive letter: C: is already in components
        // Just join them with forward slashes
        return components.join("/");
    }

    "/".to_string() + &components.join("/")
}

/// Check if a path contains parent directory traversal components.
///
/// This is a lightweight check for paths that haven't been canonicalized yet.
/// It rejects paths containing `..` components which could be used for directory traversal.
///
/// # Security Note
///
/// This check alone is NOT sufficient for security - always use `canonicalize_path_for_matching`
/// before permission checks to fully resolve symlinks and normalize paths.
pub fn has_path_traversal(path: &str) -> bool {
    Path::new(path)
        .components()
        .any(|c| matches!(c, Component::ParentDir))
}

/// Open a file safely with O_NOFOLLOW to prevent TOCTOU symlink attacks.
///
/// This function opens a file while ensuring that:
/// 1. If the file exists, it's opened with O_NOFOLLOW (fails if it's a symlink)
/// 2. If the file doesn't exist, we verify the parent directory exists and is not a symlink
///
/// This prevents the TOCTOU (Time-of-Check to Time-of-Use) race condition where:
/// - Attacker creates a file in allowed location
/// - We check permissions on the file
/// - Attacker replaces the file with a symlink to a sensitive location
/// - We open the symlink (now pointing to sensitive location)
///
/// # Platform Notes
///
/// - Unix: Uses `O_NOFOLLOW` flag directly
/// - Windows: Uses `FILE_FLAG_OPEN_REPARSE_POINT` to avoid following symlinks
pub fn open_file_no_follow(path: &Path) -> Result<std::fs::File, std::io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;

        // On Windows, use FILE_FLAG_OPEN_REPARSE_POINT to avoid following reparse points
        // This is similar to O_NOFOLLOW on Unix
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x00200000;

        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .attributes(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)
    }

    #[cfg(not(any(unix, windows)))]
    {
        // Fallback for other platforms - still try to avoid following symlinks
        // by checking the file type before opening
        if let Ok(metadata) = std::fs::symlink_metadata(path) {
            if metadata.file_type().is_symlink() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "Path is a symbolic link",
                ));
            }
        }
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)
    }
}

/// Open a file for writing safely, handling both new and existing files securely.
///
/// This function handles the case where we need to create a new file:
/// 1. Verifies the parent directory exists and is canonical (not a symlink)
/// 2. Creates the file with restrictive permissions
///
/// For existing files, uses `open_file_no_follow` to ensure it's not a symlink.
pub fn open_file_for_write_secure(path: &Path) -> Result<std::fs::File, std::io::Error> {
    // First, check if the file exists
    if path.exists() {
        // File exists - use O_NOFOLLOW to prevent symlink attacks
        return open_file_no_follow(path);
    }

    // File doesn't exist - we need to check the parent directory
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Path has no parent directory",
        )
    })?;

    // Canonicalize parent to resolve any symlinks in the path
    // This ensures we're creating the file in the intended location
    let canonical_parent = std::fs::canonicalize(parent).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Parent directory cannot be resolved: {}", e),
        )
    })?;

    // Verify the parent is actually a directory
    let parent_metadata = std::fs::metadata(&canonical_parent)?;
    if !parent_metadata.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Parent path is not a directory",
        ));
    }

    // Reconstruct the full path with canonical parent
    let file_name = path.file_name().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "Path has no file name")
    })?;

    let canonical_path = canonical_parent.join(file_name);

    // Check again if the file exists now (possible race condition)
    if canonical_path.exists() {
        return open_file_no_follow(&canonical_path);
    }

    // Create the new file
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644) // Restrictive permissions for new files
            .open(&canonical_path)
    }

    #[cfg(not(unix))]
    {
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&canonical_path)
    }
}

/// Normalize a path for pattern matching by converting backslashes to forward slashes.
/// This allows patterns to use forward slashes and match against Windows paths.
fn normalize_path_separators(path: &str) -> String {
    path.replace('\\', "/")
}

/// Match a glob pattern against a resource path
///
/// Supports:
/// - `*` - matches everything
/// - `**/*` - matches everything (recursive)
/// - `*.ext` - matches files with extension
/// - `/path/*` - matches direct children of /path
/// - `/path/**` - matches all descendants of /path
/// - Exact string matches
///
/// # Platform-specific behavior
///
/// On macOS, `/tmp` is a symlink to `/private/tmp`. This function handles
/// both the original and canonicalized paths for common symlinks like `/tmp`.
///
/// On Windows, backslashes are normalized to forward slashes for pattern matching,
/// allowing patterns like `C:/Users/*` to match `C:\Users\file.txt`.
pub(crate) fn match_glob_pattern(pattern: &str, resource: &str) -> bool {
    // Normalize path separators for cross-platform matching
    let resource = normalize_path_separators(resource);

    // Universal wildcards
    if pattern == "*" || pattern == "**/*" {
        return true;
    }

    // File extension pattern: *.rs
    if pattern.starts_with("*.") && !pattern.contains('/') {
        let suffix = &pattern[1..]; // .rs
        return resource.ends_with(suffix);
    }

    // Try matching with the resource as-is first
    if match_pattern_internal(pattern, &resource) {
        return true;
    }

    // On macOS and some systems, /tmp is a symlink to /private/tmp
    // Handle common symlink patterns by checking both directions
    if resource.starts_with("/private/tmp/") && pattern.starts_with("/tmp/") {
        let alt_resource = resource.replacen("/private/tmp/", "/tmp/", 1);
        if match_pattern_internal(pattern, &alt_resource) {
            return true;
        }
    }

    if resource.starts_with("/tmp/") && pattern.starts_with("/private/tmp/") {
        let alt_resource = resource.replacen("/tmp/", "/private/tmp/", 1);
        if match_pattern_internal(pattern, &alt_resource) {
            return true;
        }
    }

    false
}

/// Internal pattern matching logic
fn match_pattern_internal(pattern: &str, resource: &str) -> bool {
    // Directory prefix patterns need careful handling
    // /tmp/* should match /tmp/file.txt but NOT /tmpx/file.txt
    if pattern.ends_with("/*") && !pattern.contains("**") {
        let prefix = &pattern[..pattern.len() - 1]; // /tmp/
        return resource.starts_with(prefix) && !resource[prefix.len()..].contains('/');
    }

    // Recursive directory pattern: /tmp/**
    if let Some(prefix) = pattern.strip_suffix("/**") {
        // pattern is like "/tmp/**", remove the "/**" to get "/tmp"
        // Remove "**" and the preceding "/"
        return resource.starts_with(prefix)
            && (resource.len() == prefix.len() || resource[prefix.len()..].starts_with('/'));
    }

    // Exact match
    resource == pattern
}

type TypedSessionGrantMatchers = HashMap<(PermissionMatcherKind, String), TypedSessionGrant>;
type TypedScopedSessionGrants = DashMap<String, DashMap<PermissionType, TypedSessionGrantMatchers>>;

/// Global permission configuration
///
/// This struct manages both persistent whitelist rules and session-level grants.
/// It is designed to be shared across threads using Arc.
#[derive(Debug)]
pub struct PermissionConfig {
    /// Persistent whitelist rules (loaded from/saved to config file)
    whitelist: DashMap<String, PermissionRule>,
    /// Session-granted permissions that expire after a timeout.
    ///
    /// Keyed by `PermissionType`, then by the grant's resource-pattern string.
    /// Using a `HashMap` (rather than a `Vec`) makes dedup and re-grant O(1) and
    /// bounds growth: re-granting the same pattern replaces/refreshes the existing
    /// entry instead of appending a duplicate. The per-pattern lookup cannot be
    /// used to resolve a *match*, however, because matching is glob-based — see
    /// [`PermissionConfig::is_session_granted`].
    session_grants: DashMap<PermissionType, HashMap<String, SessionGrant>>,
    /// Grants keyed by stable session id. New interactive approvals use this
    /// map; the unscoped map above remains only for API compatibility.
    scoped_session_grants: DashMap<String, DashMap<PermissionType, HashMap<String, SessionGrant>>>,
    scoped_session_denies: DashMap<String, DashMap<PermissionType, HashMap<String, SessionGrant>>>,
    typed_scoped_session_grants: TypedScopedSessionGrants,
    typed_scoped_session_denies: TypedScopedSessionGrants,
    /// Compatibility grants created by legacy boolean response paths.
    one_shot_grants: DashMap<(String, String), Vec<(PermissionType, String)>>,
    /// Typed one-shot grants are additionally bound to the server generation.
    typed_one_shot_grants: DashMap<(String, String, String), Vec<(PermissionType, String)>>,
    /// Default session grant duration (default: 30 minutes)
    session_grant_duration: Duration,
    /// Whether permission checks are enabled
    enabled: AtomicBool,
    /// Active permission mode controlling auto-approval behavior
    mode: RwLock<PermissionMode>,
    /// Minimum risk level that requires confirmation when no explicit rule or
    /// session grant matches. Operations below this threshold are auto-allowed.
    /// Defaults to `Low` (confirm everything) to preserve legacy behavior; the
    /// server sets it to `High` for the "ask on high-risk" posture.
    confirm_threshold: RwLock<RiskLevel>,
    /// "Always ask" rules: tool-call patterns (e.g. `Bash(rm -rf *)`,
    /// `Bash(git push *)`) that force a user confirmation EVEN under bypass or
    /// other permissive modes. Built-in dangerous-command detection
    /// (`bash_security` Deny verdict) is layered on top of these. See
    /// [`PermissionConfig::requires_forced_confirmation`].
    ask_rules: RwLock<Vec<ParsedRule>>,
    /// Versioned durable rules used by the typed evaluator. Legacy whitelist and
    /// ask-rule strings remain readable during migration but new scoped writes
    /// land here.
    durable_rules: DashMap<String, DurablePermissionRule>,
    /// Revision of the durable permission section that produced this live
    /// policy. Included in every outcome/request and updated only after commit.
    policy_revision: AtomicU64,
    /// Pending typed requests and immutable decision receipts. Receipt keys
    /// include the server generation because provider tool-call ids may be
    /// reused within one session.
    pending_requests: DashMap<(String, String), PermissionRequest>,
    decision_receipts: DashMap<(String, String, String), PermissionDecisionReceipt>,
    /// Stable workspace identity registered by the execution boundary. This is
    /// non-durable runtime context, keyed by session to prevent workspace rules
    /// from leaking across sessions when tool arguments omit `cwd`.
    session_workspaces: DashMap<String, String>,
}

impl Default for PermissionConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl PermissionConfig {
    /// Create a new permission config with default settings
    pub fn new() -> Self {
        Self {
            whitelist: DashMap::new(),
            session_grants: DashMap::new(),
            scoped_session_grants: DashMap::new(),
            scoped_session_denies: DashMap::new(),
            typed_scoped_session_grants: DashMap::new(),
            typed_scoped_session_denies: DashMap::new(),
            one_shot_grants: DashMap::new(),
            typed_one_shot_grants: DashMap::new(),
            session_grant_duration: Duration::from_secs(30 * 60), // 30 minutes
            enabled: AtomicBool::new(true),
            mode: RwLock::new(PermissionMode::Default),
            confirm_threshold: RwLock::new(RiskLevel::Low),
            ask_rules: RwLock::new(Vec::new()),
            durable_rules: DashMap::new(),
            policy_revision: AtomicU64::new(0),
            pending_requests: DashMap::new(),
            decision_receipts: DashMap::new(),
            session_workspaces: DashMap::new(),
        }
    }

    /// Create a new permission config with specific settings
    pub fn with_settings(enabled: bool, session_duration: Duration) -> Self {
        Self {
            whitelist: DashMap::new(),
            session_grants: DashMap::new(),
            scoped_session_grants: DashMap::new(),
            scoped_session_denies: DashMap::new(),
            typed_scoped_session_grants: DashMap::new(),
            typed_scoped_session_denies: DashMap::new(),
            one_shot_grants: DashMap::new(),
            typed_one_shot_grants: DashMap::new(),
            session_grant_duration: session_duration,
            enabled: AtomicBool::new(enabled),
            mode: RwLock::new(PermissionMode::Default),
            confirm_threshold: RwLock::new(RiskLevel::Low),
            ask_rules: RwLock::new(Vec::new()),
            durable_rules: DashMap::new(),
            policy_revision: AtomicU64::new(0),
            pending_requests: DashMap::new(),
            decision_receipts: DashMap::new(),
            session_workspaces: DashMap::new(),
        }
    }

    /// Check if permission checks are enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Enable or disable permission checks
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// Get the current permission mode
    pub fn mode(&self) -> PermissionMode {
        *self.mode.read().recover_poison()
    }

    /// Set the permission mode
    pub fn set_mode(&self, mode: PermissionMode) {
        *self.mode.write().recover_poison() = mode;
    }

    /// Get the minimum risk level that requires confirmation.
    pub fn confirm_threshold(&self) -> RiskLevel {
        *self.confirm_threshold.read().recover_poison()
    }

    /// Set the minimum risk level that requires confirmation.
    ///
    /// Operations whose risk level is below `threshold` are auto-allowed when no
    /// explicit rule or session grant matches. For example, `High` means only
    /// high-risk operations (execute command, delete, git write, terminal) ask.
    pub fn set_confirm_threshold(&self, threshold: RiskLevel) {
        *self.confirm_threshold.write().recover_poison() = threshold;
    }

    pub fn policy_revision(&self) -> u64 {
        self.policy_revision.load(Ordering::Acquire)
    }

    pub fn register_session_workspace(
        &self,
        session_id: impl Into<String>,
        workspace_path: impl Into<String>,
    ) {
        self.set_session_workspace(session_id, Some(workspace_path.into()));
    }

    /// Replace the authoritative workspace identity for one session.
    /// `None` (or a blank value) explicitly removes stale runtime state when a
    /// session is unbound; callers must invoke this at every execution boundary.
    pub fn set_session_workspace(
        &self,
        session_id: impl Into<String>,
        workspace_path: Option<String>,
    ) {
        let session_id = session_id.into();
        match workspace_path
            .map(|workspace| workspace.trim().to_string())
            .filter(|workspace| !workspace.is_empty())
        {
            Some(workspace) => {
                self.session_workspaces.insert(session_id, workspace);
            }
            None => {
                self.session_workspaces.remove(&session_id);
            }
        }
    }

    pub fn session_workspace(&self, session_id: &str) -> Option<String> {
        self.session_workspaces
            .get(session_id)
            .map(|entry| entry.value().clone())
    }

    pub fn set_policy_revision(&self, revision: u64) {
        self.policy_revision.store(revision, Ordering::Release);
    }

    pub fn register_pending_request(&self, request: PermissionRequest) {
        self.register_pending_request_inner(request, || {});
    }

    fn register_pending_request_inner<F>(&self, request: PermissionRequest, before_insert: F)
    where
        F: FnOnce(),
    {
        let key = (request.session_id.clone(), request.request_id.clone());
        let request_generation = request.request_generation.clone();
        let receipt_key = (
            request.session_id.clone(),
            request.request_id.clone(),
            request_generation.clone(),
        );
        if request_generation.trim().is_empty() {
            return;
        }
        if self.decision_receipts.contains_key(&receipt_key) {
            self.pending_requests.remove_if(&key, |_, pending| {
                pending.request_generation == request_generation
            });
            return;
        }
        // A request id may be reused by a provider in a later round. Any
        // unconsumed one-shot grant belongs to the previous parked operation
        // and must not survive registration of an unresolved generation. Keep
        // the current generation: a concurrent decision writes its grant
        // immediately before its receipt, and reconnect hydration must never
        // revoke that already-authorized operation in the intervening window.
        self.one_shot_grants.remove(&key);
        self.typed_one_shot_grants
            .retain(|(session_id, request_id, generation), _| {
                session_id != &request.session_id
                    || request_id != &request.request_id
                    || generation == &request_generation
            });
        before_insert();
        self.pending_requests.insert(key.clone(), request);
        // Close the opposite interleaving: a decision receipt may have landed
        // after the first check but before this insert. Remove only this exact
        // generation so a newer provider-reused request id remains intact.
        if self.decision_receipts.contains_key(&receipt_key) {
            self.pending_requests.remove_if(&key, |_, pending| {
                pending.request_generation == request_generation
            });
        }
    }

    pub fn pending_request(&self, session_id: &str, request_id: &str) -> Option<PermissionRequest> {
        self.pending_requests
            .get(&(session_id.to_string(), request_id.to_string()))
            .map(|entry| entry.value().clone())
    }

    pub fn decision_receipt(
        &self,
        session_id: &str,
        request_id: &str,
        request_generation: &str,
    ) -> Option<PermissionDecisionReceipt> {
        self.decision_receipts
            .get(&(
                session_id.to_string(),
                request_id.to_string(),
                request_generation.to_string(),
            ))
            .map(|entry| entry.value().clone())
    }

    /// Record a decision idempotently. `Ok(true)` is a replay of the same
    /// decision, `Ok(false)` is the first application, and a different replay
    /// fails closed.
    pub fn record_decision(
        &self,
        session_id: &str,
        decision: PermissionDecision,
    ) -> Result<bool, String> {
        self.record_decision_receipt(PermissionDecisionReceipt {
            session_id: session_id.to_string(),
            decision,
            decided_at: chrono::Utc::now(),
        })
    }

    /// Restore an exact durable receipt after process restart. The timestamp is
    /// preserved while idempotency remains keyed by session, request, and the
    /// server-issued operation generation.
    pub fn record_decision_receipt(
        &self,
        receipt: PermissionDecisionReceipt,
    ) -> Result<bool, String> {
        let key = (
            receipt.session_id.clone(),
            receipt.decision.request_id.clone(),
            receipt.decision.request_generation.clone(),
        );
        if receipt.decision.request_generation.trim().is_empty() {
            return Err("permission decision generation must not be blank".to_string());
        }
        match self.decision_receipts.entry(key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(existing) => {
                if existing.get().decision == receipt.decision {
                    Ok(true)
                } else {
                    Err("request already resolved with a different decision".to_string())
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let pending_key = (
                    receipt.session_id.clone(),
                    receipt.decision.request_id.clone(),
                );
                let receipt_generation = receipt.decision.request_generation.clone();
                entry.insert(receipt);
                self.pending_requests.remove_if(&pending_key, |_, request| {
                    request.request_generation == receipt_generation
                });
                Ok(false)
            }
        }
    }

    pub fn durable_rules(&self) -> Vec<DurablePermissionRule> {
        let mut rules: Vec<_> = self
            .durable_rules
            .iter()
            .map(|entry| entry.value().clone())
            .filter(|rule| !rule.is_expired())
            .collect();
        rules.sort_by(|left, right| left.id.cmp(&right.id));
        rules
    }

    pub fn replace_durable_rules(&self, rules: impl IntoIterator<Item = DurablePermissionRule>) {
        self.durable_rules.clear();
        for rule in rules {
            self.durable_rules.insert(rule.id.clone(), rule);
        }
    }

    pub fn add_durable_rule(&self, rule: DurablePermissionRule) -> Result<(), String> {
        rule.validate()?;
        self.durable_rules.insert(rule.id.clone(), rule);
        Ok(())
    }

    pub fn remove_durable_rule(&self, id: &str) -> bool {
        self.durable_rules.remove(id).is_some()
    }

    /// Publish an already-durable snapshot to the live checker without touching
    /// temporary once/session grants.
    pub fn publish_persistent_policy(
        &self,
        revision: u64,
        candidate: &SerializablePermissionConfig,
    ) {
        self.clear_rules();
        for rule in &candidate.whitelist {
            self.add_rule(rule.clone());
        }
        self.set_enabled(candidate.enabled);
        self.set_mode(candidate.mode.unwrap_or_default());
        self.set_confirm_threshold(candidate.confirm_threshold.unwrap_or(RiskLevel::Low));
        self.set_ask_rules(candidate.ask_rules.clone());
        self.replace_durable_rules(candidate.durable_rules.clone());
        self.set_policy_revision(revision);
    }

    /// Replace the "always ask" rules from a list of pattern strings (e.g.
    /// `["Bash(rm -rf *)", "Bash(git push *)"]`). Unparseable entries degrade to
    /// a bare tool-name match per [`ParsedRule::parse`].
    pub fn set_ask_rules(&self, patterns: impl IntoIterator<Item = String>) {
        let parsed = patterns
            .into_iter()
            .map(|p| ParsedRule::parse(&p))
            .collect();
        *self.ask_rules.write().recover_poison() = parsed;
    }

    /// The configured "always ask" rules rendered back to pattern strings, for
    /// persistence (round-trips `Tool` and `Tool(pattern)` forms).
    pub fn ask_rule_patterns(&self) -> Vec<String> {
        self.ask_rules
            .read()
            .recover_poison()
            .iter()
            .map(|rule| match &rule.pattern {
                Some(pattern) => format!("{}({})", rule.tool_name, pattern),
                None => rule.tool_name.clone(),
            })
            .collect()
    }

    /// Whether this tool call must force a user confirmation regardless of the
    /// active permission mode (including `BypassPermissions`). True when:
    /// - the command is a built-in hard-dangerous shell command (a
    ///   `bash_security` `Deny` verdict), or
    /// - the command is a super-dangerous archetype the verdict downgrades to
    ///   `Allow`/`Safe` — privilege escalation, raw-device write, recursive
    ///   force-delete of a protected root, or remote pipe-to-shell, or
    /// - it matches a configured "always ask" rule.
    pub fn requires_forced_confirmation(&self, tool_name: &str, args: &serde_json::Value) -> bool {
        // Built-in backstop: hard-dangerous shell commands always ask.
        if tool_name.eq_ignore_ascii_case("Bash") {
            if let Some(command) = args.get("command").and_then(|v| v.as_str()) {
                if crate::bash_security::analyze_command(command).verdict
                    == crate::bash_security::BashVerdict::Deny
                {
                    return true;
                }
                // Catastrophic archetypes that the verdict leaves as Allow/Safe
                // (sudo, dd of=/dev/…, rm -rf /, curl … | sh) must still prompt.
                if crate::bash_security::super_dangerous_reason(command).is_some() {
                    return true;
                }
            }
        }

        // Built-in backstop: js_repl (and any future eval-style tool) executes
        // arbitrary, non-statically-analyzable code — a Node program can shell
        // out via `require('child_process')`, so it is every bit as dangerous as
        // a super-dangerous shell command. It is not covered by the Bash command
        // analysis above (the permission layer only sees "node"), so it must
        // force a confirmation like one — INCLUDING under BypassPermissions.
        if tool_name.eq_ignore_ascii_case("js_repl") {
            return true;
        }

        // Configured "always ask" rules.
        self.ask_rules
            .read()
            .recover_poison()
            .iter()
            .any(|rule| rule.matches_tool_call(tool_name, args))
    }

    /// Whether the forced confirmation came from Bamboo's non-configurable
    /// hard-dangerous backstop rather than a user configured ask rule.
    pub fn is_hard_dangerous(&self, tool_name: &str, args: &serde_json::Value) -> bool {
        if tool_name.eq_ignore_ascii_case("js_repl") {
            return true;
        }
        tool_name.eq_ignore_ascii_case("Bash")
            && args
                .get("command")
                .and_then(|value| value.as_str())
                .is_some_and(|command| {
                    crate::bash_security::analyze_command(command).verdict
                        == crate::bash_security::BashVerdict::Deny
                        || crate::bash_security::super_dangerous_reason(command).is_some()
                })
    }

    /// Get the session grant duration
    pub fn session_grant_duration(&self) -> Duration {
        self.session_grant_duration
    }

    /// Set the session grant duration
    pub fn set_session_grant_duration(&mut self, duration: Duration) {
        self.session_grant_duration = duration;
    }

    /// Add a rule to the whitelist
    pub fn add_rule(&self, rule: PermissionRule) {
        let key = format!("{:?}:{}", rule.tool_type, rule.resource_pattern);
        self.whitelist.insert(key, rule);
    }

    /// Remove a rule from the whitelist
    pub fn remove_rule(&self, tool_type: PermissionType, resource_pattern: &str) -> bool {
        let key = format!("{:?}:{}", tool_type, resource_pattern);
        self.whitelist.remove(&key).is_some()
    }

    /// Get all whitelist rules
    pub fn get_rules(&self) -> Vec<PermissionRule> {
        self.whitelist
            .iter()
            .map(|entry| entry.value().clone())
            .filter(|rule| !rule.is_expired())
            .collect()
    }

    /// Clear all whitelist rules
    pub fn clear_rules(&self) {
        self.whitelist.clear();
    }

    /// Grant a permission for the current session.
    ///
    /// Grants are keyed by their resource pattern, so re-granting the **same**
    /// pattern replaces the existing entry (refreshing its expiry) instead of
    /// appending a duplicate — this bounds the per-permission grant set to one
    /// entry per distinct pattern. Expired grants are also opportunistically
    /// pruned on every call, so the set does not grow without an explicit
    /// [`PermissionConfig::cleanup_expired_grants`] call.
    pub fn grant_session_permission(
        &self,
        perm_type: PermissionType,
        resource_pattern: impl Into<String>,
    ) {
        let grant = SessionGrant::new(resource_pattern, self.session_grant_duration);
        let pattern = grant.resource_pattern.clone();

        let mut grants = self.session_grants.entry(perm_type).or_default();

        // Opportunistic auto-cleanup: drop expired grants on every insert.
        grants.retain(|_, g| !g.is_expired());

        // Dedup: re-granting the same pattern replaces/refreshes the entry.
        grants.insert(pattern, grant);
    }

    /// Check if a permission is granted for the current session.
    ///
    /// Grant matching is **glob-based** (a single resource such as `/tmp/x.rs`
    /// can match several patterns, e.g. `/tmp/*` and `*.rs`), so the match
    /// cannot be resolved by a single O(1) hash lookup of the resource — each
    /// pattern must be evaluated against it. The set scanned here is, however,
    /// deduped and (via [`PermissionConfig::grant_session_permission`]) cleaned
    /// of expired entries, so it never grows without bound. Expired grants are
    /// still skipped defensively here in case the set hasn't been pruned yet.
    pub fn is_session_granted(&self, perm_type: PermissionType, resource: &str) -> bool {
        if let Some(grants) = self.session_grants.get(&perm_type) {
            return grants
                .values()
                .any(|grant| !grant.is_expired() && grant.matches(perm_type, resource));
        }
        false
    }

    pub fn grant_scoped_session_permission(
        &self,
        session_id: &str,
        perm_type: PermissionType,
        resource_pattern: impl Into<String>,
    ) {
        let grant = SessionGrant::new(resource_pattern, self.session_grant_duration);
        let pattern = grant.resource_pattern.clone();
        let session = self
            .scoped_session_grants
            .entry(session_id.to_string())
            .or_default();
        let mut grants = session.entry(perm_type).or_default();
        grants.retain(|_, grant| !grant.is_expired());
        grants.insert(pattern, grant);
    }

    /// Remember a session allow with the exact typed matcher selected by the
    /// user. Unlike legacy session grants, matcher values are never interpreted
    /// as globs.
    pub fn grant_typed_scoped_session_permission(
        &self,
        session_id: &str,
        perm_type: PermissionType,
        matcher: PermissionMatcher,
    ) -> Result<(), String> {
        matcher.validate(perm_type)?;
        let key = (matcher.kind, matcher.value.clone());
        let grant = TypedSessionGrant::new(matcher, self.session_grant_duration);
        let session = self
            .typed_scoped_session_grants
            .entry(session_id.to_string())
            .or_default();
        let mut grants = session.entry(perm_type).or_default();
        grants.retain(|_, grant| !grant.is_expired());
        grants.insert(key, grant);
        Ok(())
    }

    pub fn is_scoped_session_granted(
        &self,
        session_id: &str,
        perm_type: PermissionType,
        resource: &str,
    ) -> bool {
        let typed = self
            .typed_scoped_session_grants
            .get(session_id)
            .and_then(|session| session.get(&perm_type).map(|grants| grants.clone()))
            .is_some_and(|grants| {
                grants
                    .values()
                    .any(|grant| grant.matches(perm_type, resource))
            });
        typed
            || self
                .scoped_session_grants
                .get(session_id)
                .and_then(|session| session.get(&perm_type).map(|grants| grants.clone()))
                .is_some_and(|grants| {
                    grants
                        .values()
                        .any(|grant| !grant.is_expired() && grant.matches(perm_type, resource))
                })
    }

    pub fn deny_scoped_session_permission(
        &self,
        session_id: &str,
        perm_type: PermissionType,
        resource_pattern: impl Into<String>,
    ) {
        let grant = SessionGrant::new(resource_pattern, self.session_grant_duration);
        let pattern = grant.resource_pattern.clone();
        let session = self
            .scoped_session_denies
            .entry(session_id.to_string())
            .or_default();
        let mut denies = session.entry(perm_type).or_default();
        denies.retain(|_, deny| !deny.is_expired());
        denies.insert(pattern, grant);
    }

    /// Remember a session deny with the exact typed matcher selected by the
    /// user. Matcher kind is retained for enforcement and observability.
    pub fn deny_typed_scoped_session_permission(
        &self,
        session_id: &str,
        perm_type: PermissionType,
        matcher: PermissionMatcher,
    ) -> Result<(), String> {
        matcher.validate(perm_type)?;
        let key = (matcher.kind, matcher.value.clone());
        let deny = TypedSessionGrant::new(matcher, self.session_grant_duration);
        let session = self
            .typed_scoped_session_denies
            .entry(session_id.to_string())
            .or_default();
        let mut denies = session.entry(perm_type).or_default();
        denies.retain(|_, deny| !deny.is_expired());
        denies.insert(key, deny);
        Ok(())
    }

    pub fn is_scoped_session_denied(
        &self,
        session_id: &str,
        perm_type: PermissionType,
        resource: &str,
    ) -> bool {
        let typed = self
            .typed_scoped_session_denies
            .get(session_id)
            .and_then(|session| session.get(&perm_type).map(|denies| denies.clone()))
            .is_some_and(|denies| {
                denies
                    .values()
                    .any(|deny| deny.matches(perm_type, resource))
            });
        typed
            || self
                .scoped_session_denies
                .get(session_id)
                .and_then(|session| session.get(&perm_type).map(|denies| denies.clone()))
                .is_some_and(|denies| {
                    denies
                        .values()
                        .any(|deny| !deny.is_expired() && deny.matches(perm_type, resource))
                })
    }

    /// Consume a one-shot grant bound to one session. Legacy `Approve` maps to
    /// this path and therefore authorizes only the parked re-execution.
    pub fn consume_scoped_session_grant(
        &self,
        session_id: &str,
        perm_type: PermissionType,
        resource: &str,
    ) -> bool {
        let Some(session) = self.scoped_session_grants.get(session_id) else {
            return false;
        };
        let Some(mut grants) = session.get_mut(&perm_type) else {
            return false;
        };
        let matched = grants.iter().find_map(|(pattern, grant)| {
            (!grant.is_expired() && grant.matches(perm_type, resource)).then(|| pattern.clone())
        });
        matched.is_some_and(|pattern| grants.remove(&pattern).is_some())
    }

    pub fn grant_once(
        &self,
        session_id: &str,
        request_id: &str,
        perm_type: PermissionType,
        resource: String,
    ) {
        let mut grants = self
            .one_shot_grants
            .entry((session_id.to_string(), request_id.to_string()))
            .or_default();
        if !grants
            .iter()
            .any(|grant| grant.0 == perm_type && grant.1 == resource)
        {
            grants.push((perm_type, resource));
        }
    }

    pub fn grant_once_for_generation(
        &self,
        session_id: &str,
        request_id: &str,
        request_generation: &str,
        perm_type: PermissionType,
        resource: String,
    ) -> Result<(), String> {
        if request_generation.trim().is_empty() {
            return Err("permission grant generation must not be blank".to_string());
        }
        let mut grants = self
            .typed_one_shot_grants
            .entry((
                session_id.to_string(),
                request_id.to_string(),
                request_generation.to_string(),
            ))
            .or_default();
        if !grants
            .iter()
            .any(|grant| grant.0 == perm_type && grant.1 == resource)
        {
            grants.push((perm_type, resource));
        }
        Ok(())
    }

    pub fn consume_once(
        &self,
        session_id: &str,
        request_id: &str,
        perm_type: PermissionType,
        resource: &str,
    ) -> bool {
        let key = (session_id.to_string(), request_id.to_string());
        let Some(mut grants) = self.one_shot_grants.get_mut(&key) else {
            return false;
        };
        let Some(index) = grants
            .iter()
            .position(|grant| grant.0 == perm_type && grant.1 == resource)
        else {
            return false;
        };
        grants.swap_remove(index);
        let empty = grants.is_empty();
        drop(grants);
        if empty {
            self.one_shot_grants
                .remove_if(&key, |_, grants| grants.is_empty());
        }
        true
    }

    pub fn consume_once_for_generation(
        &self,
        session_id: &str,
        request_id: &str,
        request_generation: &str,
        perm_type: PermissionType,
        resource: &str,
    ) -> bool {
        if request_generation.trim().is_empty() {
            return false;
        }
        let key = (
            session_id.to_string(),
            request_id.to_string(),
            request_generation.to_string(),
        );
        let Some(mut grants) = self.typed_one_shot_grants.get_mut(&key) else {
            return false;
        };
        let Some(index) = grants
            .iter()
            .position(|grant| grant.0 == perm_type && grant.1 == resource)
        else {
            return false;
        };
        grants.swap_remove(index);
        let empty = grants.is_empty();
        drop(grants);
        if empty {
            self.typed_one_shot_grants
                .remove_if(&key, |_, grants| grants.is_empty());
        }
        true
    }

    /// Clear all session grants
    pub fn clear_session_grants(&self) {
        self.session_grants.clear();
        self.scoped_session_grants.clear();
        self.scoped_session_denies.clear();
        self.typed_scoped_session_grants.clear();
        self.typed_scoped_session_denies.clear();
        self.one_shot_grants.clear();
        self.typed_one_shot_grants.clear();
    }

    /// Return a deterministic read-only snapshot of active runtime grants.
    ///
    /// Expired entries are omitted. One-shot receipts intentionally have no
    /// timestamp because their lifetime is consumption-bound rather than
    /// duration-bound.
    pub fn temporary_grants(&self) -> Vec<TemporaryPermissionGrant> {
        let now_instant = Instant::now();
        let now_utc = chrono::Utc::now();
        let mut result = Vec::new();

        for permission_entry in self.session_grants.iter() {
            for grant in permission_entry.value().values() {
                if grant.expires_at < now_instant {
                    continue;
                }
                result.push(temporary_session_grant(
                    TemporaryPermissionGrantScope::UnscopedSession,
                    TemporaryPermissionGrantEffect::Allow,
                    None,
                    *permission_entry.key(),
                    grant,
                    now_instant,
                    now_utc,
                ));
            }
        }

        for session_entry in self.scoped_session_grants.iter() {
            for permission_entry in session_entry.value().iter() {
                for grant in permission_entry.value().values() {
                    if grant.expires_at < now_instant {
                        continue;
                    }
                    result.push(temporary_session_grant(
                        TemporaryPermissionGrantScope::Session,
                        TemporaryPermissionGrantEffect::Allow,
                        Some(session_entry.key().clone()),
                        *permission_entry.key(),
                        grant,
                        now_instant,
                        now_utc,
                    ));
                }
            }
        }

        for session_entry in self.scoped_session_denies.iter() {
            for permission_entry in session_entry.value().iter() {
                for grant in permission_entry.value().values() {
                    if grant.expires_at < now_instant {
                        continue;
                    }
                    result.push(temporary_session_grant(
                        TemporaryPermissionGrantScope::Session,
                        TemporaryPermissionGrantEffect::Deny,
                        Some(session_entry.key().clone()),
                        *permission_entry.key(),
                        grant,
                        now_instant,
                        now_utc,
                    ));
                }
            }
        }

        for session_entry in self.typed_scoped_session_grants.iter() {
            for permission_entry in session_entry.value().iter() {
                for grant in permission_entry.value().values() {
                    if grant.expires_at < now_instant {
                        continue;
                    }
                    result.push(temporary_typed_session_grant(
                        TemporaryPermissionGrantEffect::Allow,
                        session_entry.key().clone(),
                        *permission_entry.key(),
                        grant,
                        now_instant,
                        now_utc,
                    ));
                }
            }
        }

        for session_entry in self.typed_scoped_session_denies.iter() {
            for permission_entry in session_entry.value().iter() {
                for grant in permission_entry.value().values() {
                    if grant.expires_at < now_instant {
                        continue;
                    }
                    result.push(temporary_typed_session_grant(
                        TemporaryPermissionGrantEffect::Deny,
                        session_entry.key().clone(),
                        *permission_entry.key(),
                        grant,
                        now_instant,
                        now_utc,
                    ));
                }
            }
        }

        for receipt_entry in self.one_shot_grants.iter() {
            let (session_id, request_id) = receipt_entry.key();
            for (permission_type, matcher) in receipt_entry.value() {
                result.push(TemporaryPermissionGrant {
                    scope: TemporaryPermissionGrantScope::OneShot,
                    effect: TemporaryPermissionGrantEffect::Allow,
                    session_id: Some(session_id.clone()),
                    request_id: Some(request_id.clone()),
                    permission_type: *permission_type,
                    matcher: matcher.clone(),
                    granted_at: None,
                    expires_at: None,
                });
            }
        }

        for receipt_entry in self.typed_one_shot_grants.iter() {
            let (session_id, request_id, _) = receipt_entry.key();
            for (permission_type, matcher) in receipt_entry.value() {
                result.push(TemporaryPermissionGrant {
                    scope: TemporaryPermissionGrantScope::OneShot,
                    effect: TemporaryPermissionGrantEffect::Allow,
                    session_id: Some(session_id.clone()),
                    request_id: Some(request_id.clone()),
                    permission_type: *permission_type,
                    matcher: matcher.clone(),
                    granted_at: None,
                    expires_at: None,
                });
            }
        }

        result.sort_by(|left, right| {
            left.session_id
                .cmp(&right.session_id)
                .then_with(|| left.request_id.cmp(&right.request_id))
                .then_with(|| left.scope.cmp(&right.scope))
                .then_with(|| left.effect.cmp(&right.effect))
                .then_with(|| left.permission_type.cmp(&right.permission_type))
                .then_with(|| left.matcher.cmp(&right.matcher))
        });
        result
    }

    /// Clean up expired session grants.
    ///
    /// Note that expired grants are *also* removed opportunistically by
    /// [`PermissionConfig::grant_session_permission`]; this explicit call is
    /// still available for callers that want to prune without granting.
    pub fn cleanup_expired_grants(&self) {
        for mut entry in self.session_grants.iter_mut() {
            entry.value_mut().retain(|_, g| !g.is_expired());
        }
        for session in self.typed_scoped_session_grants.iter() {
            for mut permission in session.value().iter_mut() {
                permission
                    .value_mut()
                    .retain(|_, grant| !grant.is_expired());
            }
        }
        for session in self.typed_scoped_session_denies.iter() {
            for mut permission in session.value().iter_mut() {
                permission.value_mut().retain(|_, deny| !deny.is_expired());
            }
        }
    }

    /// Check if a permission is allowed by the whitelist
    pub fn is_whitelist_allowed(&self, perm_type: PermissionType, resource: &str) -> Option<bool> {
        // Check for explicit denies first, then explicit allows
        let mut allowed = None;

        for entry in self.whitelist.iter() {
            let rule = entry.value();
            if rule.matches(perm_type, resource) {
                if rule.allowed {
                    allowed = Some(true);
                } else {
                    // Explicit deny takes precedence
                    return Some(false);
                }
            }
        }

        allowed
    }

    /// Evaluate one operation through the single typed precedence chain.
    ///
    /// A request-bound one-shot authorization is checked after hard/explicit
    /// deny but before forced confirmation. It is the receipt for the exact
    /// parked invocation that was already confirmed; unrelated remembered
    /// grants are intentionally evaluated only after hard-dangerous and
    /// always-ask rules.
    pub fn evaluate(&self, input: PermissionEvaluation) -> PermissionOutcome {
        let configured_mode = self.mode();
        // Normalize legacy callers that accidentally supplied both booleans:
        // Auto is a no-prompt posture, never an alias for Bypass or its wider
        // sandbox semantics. Plan remains a hard read-only overlay.
        let requested = if input.auto_approve_requested {
            bamboo_domain::SessionPermissionMode::Auto
        } else if input.bypass_requested {
            bamboo_domain::SessionPermissionMode::Bypass
        } else {
            bamboo_domain::SessionPermissionMode::Default
        };
        let resolution = bamboo_domain::resolve_permission_mode(requested, configured_mode);
        let effective_mode = resolution.effective;
        let effective_policy = EffectivePermissionPolicy {
            revision: self.policy_revision(),
            mode: effective_mode,
            bypass_requested: resolution.bypass_permissions(),
            auto_approve_requested: requested == bamboo_domain::SessionPermissionMode::Auto,
        };

        let deny = |code, message: String, matched_rule| PermissionOutcome::Deny {
            reason: PermissionDenyReason {
                code,
                message,
                matched_rule,
            },
            effective_policy: effective_policy.clone(),
        };

        if let Some(message) = input.platform_hard_deny.clone() {
            return deny(PermissionReasonCode::PlatformHardDeny, message, None);
        }

        if let Some(rule) = self.matching_durable_rule(&input, PermissionRuleEffect::Deny) {
            return deny(
                PermissionReasonCode::ExplicitDeny,
                format!("permission denied by durable rule '{}'", rule.id),
                Some(PermissionRuleRef::from(&rule)),
            );
        }
        if self.is_scoped_session_denied(&input.session_id, input.permission_type, &input.resource)
        {
            return deny(
                PermissionReasonCode::ExplicitDeny,
                "permission denied by a remembered session decision".to_string(),
                None,
            );
        }
        if self.is_whitelist_allowed(input.permission_type, &input.resource) == Some(false) {
            return deny(
                PermissionReasonCode::ExplicitDeny,
                "permission denied by explicit policy (legacy rule)".to_string(),
                Some(legacy_rule_ref("legacy-deny", PermissionRuleEffect::Deny)),
            );
        }

        // Plan/read-only is an authorization boundary rather than an approval
        // preference. It precedes receipts, grants, disabled-checks, Bypass and
        // Auto so none of those mechanisms can turn a mutating operation into
        // an allow while the gate is active.
        if effective_mode == PermissionMode::Plan && input.risk_level != RiskLevel::Low {
            return deny(
                PermissionReasonCode::ModeDenied,
                "operation denied by plan mode".to_string(),
                None,
            );
        }

        let replay_generation = input.consume_once.then(|| {
            crate::current_permission_replay_generation(&input.session_id, &input.request_id)
        });
        let consumed_once = match replay_generation.flatten() {
            Some(generation) => self.consume_once_for_generation(
                &input.session_id,
                &input.request_id,
                &generation,
                input.permission_type,
                &input.resource,
            ),
            None if input.consume_once => self.consume_once(
                &input.session_id,
                &input.request_id,
                input.permission_type,
                &input.resource,
            ),
            None => false,
        };
        if consumed_once {
            return PermissionOutcome::Allow {
                source: PermissionDecisionSource::OneShot,
                effective_policy,
            };
        }

        // Auto is deliberately ordered after all hard/explicit denials and the
        // exact one-shot receipt, but before every source of an approval
        // request. It therefore produces zero prompts without weakening
        // platform, durable, session, or legacy deny rules.
        if resolution.suppress_approval_prompts() {
            return PermissionOutcome::Allow {
                source: PermissionDecisionSource::Auto,
                effective_policy,
            };
        }

        let hard_dangerous = self.is_hard_dangerous(&input.tool_name, &input.tool_args);
        let typed_always_ask = self
            .matching_durable_rule(&input, PermissionRuleEffect::AlwaysAsk)
            .map(|rule| PermissionRuleRef::from(&rule));
        let configured_always_ask = typed_always_ask.is_some()
            || self
                .ask_rules
                .read()
                .recover_poison()
                .iter()
                .any(|rule| rule.matches_tool_call(&input.tool_name, &input.tool_args));
        if hard_dangerous || configured_always_ask {
            let reason_code = if hard_dangerous {
                PermissionReasonCode::HardDangerous
            } else {
                PermissionReasonCode::ConfiguredAlwaysAsk
            };
            return PermissionOutcome::Ask(self.build_request(
                &input,
                effective_mode,
                reason_code,
                typed_always_ask.or_else(|| {
                    configured_always_ask.then(|| {
                        legacy_rule_ref("legacy-always-ask", PermissionRuleEffect::AlwaysAsk)
                    })
                }),
                true,
            ));
        }

        if self.is_scoped_session_granted(&input.session_id, input.permission_type, &input.resource)
        {
            return PermissionOutcome::Allow {
                source: PermissionDecisionSource::RememberedSession,
                effective_policy,
            };
        }
        if let Some(rule) = self.matching_durable_rule(&input, PermissionRuleEffect::Allow) {
            return PermissionOutcome::Allow {
                source: PermissionDecisionSource::RememberedRule {
                    rule: PermissionRuleRef::from(&rule),
                },
                effective_policy,
            };
        }
        if self.is_whitelist_allowed(input.permission_type, &input.resource) == Some(true) {
            return PermissionOutcome::Allow {
                source: PermissionDecisionSource::RememberedRule {
                    rule: legacy_rule_ref("legacy-allow", PermissionRuleEffect::Allow),
                },
                effective_policy,
            };
        }

        if !self.is_enabled() {
            return PermissionOutcome::Allow {
                source: PermissionDecisionSource::PermissionChecksDisabled,
                effective_policy,
            };
        }

        if effective_mode == PermissionMode::BypassPermissions {
            return PermissionOutcome::Allow {
                source: PermissionDecisionSource::Bypass,
                effective_policy,
            };
        }

        match effective_mode {
            PermissionMode::DontAsk => {
                return deny(
                    PermissionReasonCode::ModeDenied,
                    "operation denied by dont-ask mode without an explicit allow".to_string(),
                    None,
                );
            }
            PermissionMode::AcceptEdits
                if input.permission_type == PermissionType::WriteFile
                    || (input.permission_type == PermissionType::ExecuteCommand
                        && crate::checker::is_safe_edit_command(&input.resource)) =>
            {
                return PermissionOutcome::Allow {
                    source: PermissionDecisionSource::Mode,
                    effective_policy,
                };
            }
            PermissionMode::Auto | PermissionMode::BypassPermissions => {}
            _ => {}
        }

        if input.risk_level < self.confirm_threshold() {
            PermissionOutcome::Allow {
                source: PermissionDecisionSource::BelowRiskThreshold,
                effective_policy,
            }
        } else {
            PermissionOutcome::Ask(self.build_request(
                &input,
                effective_mode,
                PermissionReasonCode::RiskThreshold,
                None,
                false,
            ))
        }
    }

    fn matching_durable_rule(
        &self,
        input: &PermissionEvaluation,
        effect: PermissionRuleEffect,
    ) -> Option<DurablePermissionRule> {
        let mut matches: Vec<_> = self
            .durable_rules
            .iter()
            .map(|entry| entry.value().clone())
            .filter(|rule| {
                rule.effect == effect
                    && rule.matches(
                        input.permission_type,
                        &input.resource,
                        input.workspace_path.as_deref(),
                    )
            })
            .collect();
        // Workspace is narrower than global. A stable id tie-break makes
        // diagnostics deterministic without changing effect precedence.
        matches.sort_by_key(|rule| {
            (
                match rule.scope {
                    PermissionRuleScope::Workspace => 0,
                    PermissionRuleScope::Global => 1,
                },
                rule.id.clone(),
            )
        });
        matches.into_iter().next()
    }

    fn build_request(
        &self,
        input: &PermissionEvaluation,
        effective_mode: PermissionMode,
        reason_code: PermissionReasonCode,
        matched_rule: Option<PermissionRuleRef>,
        forced: bool,
    ) -> PermissionRequest {
        let offered = if forced {
            PermissionRequest::forced_decisions()
        } else {
            PermissionRequest::ordinary_decisions(input.workspace_path.is_some())
        };
        let allowed_decisions = offered
            .into_iter()
            .filter(|decision| input.supported_decisions.contains(decision))
            .collect();
        PermissionRequest {
            request_id: input.request_id.clone(),
            request_generation: PermissionRequest::fresh_generation(),
            session_id: input.session_id.clone(),
            workspace_path: input.workspace_path.clone(),
            tool_name: input.tool_name.clone(),
            permission_type: input.permission_type,
            resource: input.resource.clone(),
            operation_summary: input.operation_summary.clone(),
            risk_level: input.risk_level,
            reason_code,
            effective_mode,
            bypass_requested: input.bypass_requested,
            auto_approve_requested: input.auto_approve_requested,
            policy_revision: self.policy_revision(),
            matched_rule,
            allowed_decisions,
            suggested_matchers: conservative_matchers(input.permission_type, &input.resource),
        }
    }

    /// Check if permission is required for an operation
    ///
    /// Returns true if the operation requires user confirmation
    pub fn needs_confirmation(&self, perm_type: PermissionType, resource: &str) -> bool {
        if !self.is_enabled() {
            return false;
        }

        // Check session grants first (fast path)
        if self.is_session_granted(perm_type, resource) {
            return false;
        }

        // Check whitelist
        match self.is_whitelist_allowed(perm_type, resource) {
            Some(true) => false, // Explicitly allowed
            Some(false) => true, // Explicitly denied (requires override)
            // No rule found: require confirmation only when the operation's risk
            // level meets the configured threshold; lower-risk ops auto-allow.
            None => perm_type.risk_level() >= self.confirm_threshold(),
        }
    }

    /// Convert to serializable format for persistence
    pub fn to_serializable(&self) -> SerializablePermissionConfig {
        SerializablePermissionConfig {
            whitelist: self.get_rules(),
            enabled: self.is_enabled(),
            session_grant_duration_secs: self.session_grant_duration.as_secs(),
            mode: Some(self.mode()),
            confirm_threshold: Some(self.confirm_threshold()),
            ask_rules: self.ask_rule_patterns(),
            durable_rules: self.durable_rules(),
        }
    }

    /// Load from serializable format
    pub fn from_serializable(config: SerializablePermissionConfig) -> Self {
        let whitelist = DashMap::new();
        for rule in config.whitelist {
            let key = format!("{:?}:{}", rule.tool_type, rule.resource_pattern);
            whitelist.insert(key, rule);
        }

        let mode = config.mode.unwrap_or_default();
        let confirm_threshold = config.confirm_threshold.unwrap_or(RiskLevel::Low);
        let ask_rules = config
            .ask_rules
            .iter()
            .map(|p| ParsedRule::parse(p))
            .collect();
        let durable_rules = DashMap::new();
        for rule in config.durable_rules {
            durable_rules.insert(rule.id.clone(), rule);
        }

        Self {
            whitelist,
            session_grants: DashMap::new(),
            scoped_session_grants: DashMap::new(),
            scoped_session_denies: DashMap::new(),
            typed_scoped_session_grants: DashMap::new(),
            typed_scoped_session_denies: DashMap::new(),
            one_shot_grants: DashMap::new(),
            typed_one_shot_grants: DashMap::new(),
            session_grant_duration: Duration::from_secs(config.session_grant_duration_secs),
            enabled: AtomicBool::new(config.enabled),
            mode: RwLock::new(mode),
            confirm_threshold: RwLock::new(confirm_threshold),
            ask_rules: RwLock::new(ask_rules),
            durable_rules,
            policy_revision: AtomicU64::new(0),
            pending_requests: DashMap::new(),
            decision_receipts: DashMap::new(),
            session_workspaces: DashMap::new(),
        }
    }

    /// Merge `other` into this config, returning a new `PermissionConfig`.
    ///
    /// `other` has higher priority: its whitelist rules replace conflicting ones from `self`,
    /// its mode overrides `self`'s, and its enabled flag takes precedence.
    /// Both allow and deny rules from both configs are preserved (deduplicated).
    pub fn merge(&self, other: &PermissionConfig) -> Self {
        let merged = Self::new();

        // Copy all rules from self (lower priority)
        for rule in self.get_rules() {
            let key = format!("{:?}:{}", rule.tool_type, rule.resource_pattern);
            merged.whitelist.insert(key, rule);
        }

        // Override/add rules from other (higher priority)
        for rule in other.get_rules() {
            let key = format!("{:?}:{}", rule.tool_type, rule.resource_pattern);
            merged.whitelist.insert(key, rule);
        }

        // Session grants from other (higher priority source)
        for entry in other.session_grants.iter() {
            let perm_type = entry.key();
            let grants = entry.value();
            merged.session_grants.insert(*perm_type, grants.clone());
        }

        // Mode from other takes precedence
        merged.set_mode(other.mode());

        // Confirmation threshold from other takes precedence
        merged.set_confirm_threshold(other.confirm_threshold());

        // Enabled flag from other takes precedence
        merged.set_enabled(other.is_enabled());

        // "Always ask" rules: union of both, other's appended after self's.
        let mut ask_rules = self.ask_rules.read().recover_poison().clone();
        ask_rules.extend(other.ask_rules.read().recover_poison().iter().cloned());
        *merged.ask_rules.write().recover_poison() = ask_rules;

        for rule in self.durable_rules() {
            merged.durable_rules.insert(rule.id.clone(), rule);
        }
        for rule in other.durable_rules() {
            merged.durable_rules.insert(rule.id.clone(), rule);
        }
        for entry in self.session_workspaces.iter() {
            merged
                .session_workspaces
                .insert(entry.key().clone(), entry.value().clone());
        }
        for entry in other.session_workspaces.iter() {
            merged
                .session_workspaces
                .insert(entry.key().clone(), entry.value().clone());
        }
        merged.set_policy_revision(other.policy_revision());

        merged
    }
}

fn temporary_session_grant(
    scope: TemporaryPermissionGrantScope,
    effect: TemporaryPermissionGrantEffect,
    session_id: Option<String>,
    permission_type: PermissionType,
    grant: &SessionGrant,
    now_instant: Instant,
    now_utc: chrono::DateTime<chrono::Utc>,
) -> TemporaryPermissionGrant {
    TemporaryPermissionGrant {
        scope,
        effect,
        session_id,
        request_id: None,
        permission_type,
        matcher: grant.resource_pattern.clone(),
        granted_at: instant_to_utc(grant.granted_at, now_instant, now_utc),
        expires_at: instant_to_utc(grant.expires_at, now_instant, now_utc),
    }
}

fn temporary_typed_session_grant(
    effect: TemporaryPermissionGrantEffect,
    session_id: String,
    permission_type: PermissionType,
    grant: &TypedSessionGrant,
    now_instant: Instant,
    now_utc: chrono::DateTime<chrono::Utc>,
) -> TemporaryPermissionGrant {
    TemporaryPermissionGrant {
        scope: TemporaryPermissionGrantScope::Session,
        effect,
        session_id: Some(session_id),
        request_id: None,
        permission_type,
        matcher: serde_json::to_string(&grant.matcher)
            .unwrap_or_else(|_| grant.matcher.value.clone()),
        granted_at: instant_to_utc(grant.granted_at, now_instant, now_utc),
        expires_at: instant_to_utc(grant.expires_at, now_instant, now_utc),
    }
}

fn instant_to_utc(
    instant: Instant,
    anchor_instant: Instant,
    anchor_utc: chrono::DateTime<chrono::Utc>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Some(duration) = instant.checked_duration_since(anchor_instant) {
        chrono::Duration::from_std(duration)
            .ok()
            .and_then(|duration| anchor_utc.checked_add_signed(duration))
    } else {
        let duration = anchor_instant.checked_duration_since(instant)?;
        chrono::Duration::from_std(duration)
            .ok()
            .and_then(|duration| anchor_utc.checked_sub_signed(duration))
    }
}

fn legacy_rule_ref(id: &str, effect: PermissionRuleEffect) -> PermissionRuleRef {
    PermissionRuleRef {
        id: id.to_string(),
        effect,
        scope: PermissionRuleScope::Global,
        source: PermissionRuleSource::Legacy,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializablePermissionConfig {
    pub whitelist: Vec<PermissionRule>,
    pub enabled: bool,
    pub session_grant_duration_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<PermissionMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm_threshold: Option<RiskLevel>,
    /// "Always ask" rule patterns (e.g. `Bash(rm -rf *)`) that force a prompt
    /// even under bypass. See [`PermissionConfig::requires_forced_confirmation`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ask_rules: Vec<String>,
    /// Versioned typed rules. Legacy whitelist/ask-rule fields remain readable
    /// and are evaluated without broadening during the migration window.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub durable_rules: Vec<DurablePermissionRule>,
}

/// Conservative external-runner preflight for explicit Bamboo deny policy.
///
/// Vendor native no-prompt modes do not expose a reliable per-tool callback.
/// Any live explicit deny therefore makes such an activation fail closed before
/// process spawn; workspace/session applicability that cannot be proven at this
/// boundary is deliberately treated as applicable.
pub fn explicit_deny_policy_reason(policy: &SerializablePermissionConfig) -> Option<String> {
    let legacy_denies = policy
        .whitelist
        .iter()
        .filter(|rule| !rule.allowed && !rule.is_expired())
        .count();
    let durable_denies = policy
        .durable_rules
        .iter()
        .filter(|rule| rule.effect == PermissionRuleEffect::Deny && !rule.is_expired())
        .count();
    let total = legacy_denies.saturating_add(durable_denies);
    (total > 0).then(|| {
        format!(
            "external no-prompt executor cannot safely enforce {total} explicit Bamboo deny rule(s)"
        )
    })
}

impl Default for SerializablePermissionConfig {
    fn default() -> Self {
        Self {
            whitelist: Vec::new(),
            enabled: true,
            session_grant_duration_secs: 30 * 60, // 30 minutes
            mode: None,
            confirm_threshold: None,
            ask_rules: Vec::new(),
            durable_rules: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permission_type_description() {
        assert!(PermissionType::WriteFile
            .description()
            .contains("Write files"));
        assert!(PermissionType::ExecuteCommand
            .description()
            .contains("Execute"));
    }

    #[test]
    fn test_risk_level() {
        assert_eq!(PermissionType::WriteFile.risk_level(), RiskLevel::Medium);
        assert_eq!(PermissionType::ExecuteCommand.risk_level(), RiskLevel::High);
    }

    #[test]
    fn test_session_grant_with_real_paths() {
        let grant = SessionGrant::new("/tmp/*", Duration::from_secs(3600));
        // Use /tmp which exists on most systems
        assert!(grant.matches(PermissionType::WriteFile, "/tmp/test.txt"));
        assert!(!grant.matches(PermissionType::WriteFile, "/var/test.txt"));
    }

    #[test]
    fn test_permission_rule_matches() {
        // Test with paths that should exist
        let rule = PermissionRule::new(PermissionType::WriteFile, "*.rs", true);
        assert!(rule.matches(PermissionType::WriteFile, "/tmp/test.rs"));
        assert!(!rule.matches(PermissionType::WriteFile, "/tmp/test.txt"));
        assert!(!rule.matches(PermissionType::ExecuteCommand, "/tmp/test.rs"));
    }

    #[test]
    fn test_permission_rule_directory_pattern() {
        let rule = PermissionRule::new(PermissionType::WriteFile, "/tmp/*", true);
        assert!(rule.matches(PermissionType::WriteFile, "/tmp/test.txt"));
        assert!(!rule.matches(PermissionType::WriteFile, "/var/test.txt"));
    }

    #[test]
    fn test_session_grant_matches() {
        let grant = SessionGrant::new("/tmp/*", Duration::from_secs(3600));
        // Test with /tmp which should exist
        assert!(grant.matches(PermissionType::WriteFile, "/tmp/test.txt"));
        assert!(!grant.matches(PermissionType::WriteFile, "/var/test.txt"));
    }

    #[test]
    fn test_permission_rule_rejects_traversal() {
        let rule = PermissionRule::new(PermissionType::WriteFile, "/safe/**", true);
        assert!(!rule.matches(PermissionType::WriteFile, "/safe/../etc/passwd"));
    }

    #[test]
    fn test_session_grant_rejects_traversal() {
        let grant = SessionGrant::new("/safe/**", Duration::from_secs(3600));
        assert!(!grant.matches(PermissionType::WriteFile, "/safe/../etc/passwd"));
    }

    #[test]
    fn test_permission_rule_normalizes_slashes() {
        let rule = PermissionRule::new(PermissionType::WriteFile, "/tmp/*", true);
        assert!(rule.matches(PermissionType::WriteFile, "/tmp//file.txt"));
    }

    #[test]
    fn test_permission_rule_rejects_relative_resource() {
        let rule = PermissionRule::new(PermissionType::WriteFile, "*.rs", true);
        assert!(!rule.matches(PermissionType::WriteFile, "test.rs"));
    }

    #[test]
    fn test_config_needs_confirmation() {
        let config = PermissionConfig::new();

        // By default, should require confirmation
        assert!(config.needs_confirmation(PermissionType::WriteFile, "/tmp/test.txt"));

        // After granting session permission, should not require confirmation
        config.grant_session_permission(PermissionType::WriteFile, "/tmp/*");
        assert!(!config.needs_confirmation(PermissionType::WriteFile, "/tmp/test.txt"));
        assert!(config.needs_confirmation(PermissionType::WriteFile, "/var/test.txt"));
    }

    #[test]
    fn test_whitelist_allowed() {
        let config = PermissionConfig::new();
        config.add_rule(PermissionRule::new(PermissionType::WriteFile, "*.rs", true));

        assert_eq!(
            config.is_whitelist_allowed(PermissionType::WriteFile, "/tmp/test.rs"),
            Some(true)
        );
        assert_eq!(
            config.is_whitelist_allowed(PermissionType::WriteFile, "/tmp/test.txt"),
            None
        );
    }

    #[test]
    fn test_whitelist_denial() {
        let config = PermissionConfig::new();
        config.add_rule(PermissionRule::new(
            PermissionType::WriteFile,
            "*.txt",
            false,
        ));

        assert_eq!(
            config.is_whitelist_allowed(PermissionType::WriteFile, "/tmp/test.txt"),
            Some(false)
        );
    }

    #[test]
    fn test_glob_pattern_exact_match() {
        assert!(match_glob_pattern("/tmp/test.txt", "/tmp/test.txt"));
        assert!(!match_glob_pattern("/tmp/test.txt", "/tmp/other.txt"));
    }

    #[test]
    fn test_glob_pattern_wildcard() {
        assert!(match_glob_pattern("*", "/any/path"));
        assert!(match_glob_pattern("**/*", "/any/path"));
    }

    #[test]
    fn test_glob_pattern_extension() {
        assert!(match_glob_pattern("*.rs", "test.rs"));
        assert!(match_glob_pattern("*.rs", "/path/to/test.rs"));
        assert!(!match_glob_pattern("*.rs", "test.txt"));
        assert!(!match_glob_pattern("*.rs", "/path/to/test.rs.txt"));
    }

    #[test]
    fn test_glob_pattern_directory_children() {
        // /tmp/* should match /tmp/file.txt but NOT /tmp/subdir/file.txt
        let rule = PermissionRule::new(PermissionType::WriteFile, "/tmp/*", true);
        assert!(rule.matches(PermissionType::WriteFile, "/tmp/test.txt"));
        assert!(rule.matches(PermissionType::WriteFile, "/tmp/file.rs"));
        assert!(!rule.matches(PermissionType::WriteFile, "/tmp/subdir/file.txt"));
        assert!(!rule.matches(PermissionType::WriteFile, "/tmpx/file.txt"));
    }

    #[test]
    fn test_glob_pattern_recursive() {
        // /tmp/** should match all descendants
        assert!(match_glob_pattern("/tmp/**", "/tmp/file.txt"));
        assert!(match_glob_pattern("/tmp/**", "/tmp/subdir/file.txt"));
        assert!(match_glob_pattern("/tmp/**", "/tmp/a/b/c/d.txt"));
        assert!(!match_glob_pattern("/tmp/**", "/tmpx/file.txt"));
    }

    #[test]
    fn test_glob_pattern_edge_cases() {
        // Ensure /tmp/* does NOT match /tmpx/ (boundary check)
        assert!(!match_glob_pattern("/tmp/*", "/tmpx/file.txt"));

        // Ensure directory patterns work correctly
        assert!(match_glob_pattern("/home/user/*", "/home/user/file.txt"));
        assert!(!match_glob_pattern("/home/user/*", "/home/user2/file.txt"));
    }

    #[test]
    fn test_non_path_resources_http_domains() {
        // HTTP domain permissions should match domains in URLs
        let rule = PermissionRule::new(PermissionType::HttpRequest, "api.example.com", true);
        // Exact match should work
        assert!(rule.matches(PermissionType::HttpRequest, "api.example.com"));
        // Different domain should not match
        assert!(!rule.matches(PermissionType::HttpRequest, "other.example.com"));
        // Note: Subdomain matching and full URL extraction are handled at the call site
        // in tool_permissions.rs using extract_domain_from_url
    }

    #[test]
    fn test_non_path_resources_commands() {
        // Command permissions should match command prefix
        let rule = PermissionRule::new(PermissionType::ExecuteCommand, "npm", true);
        assert!(rule.matches(PermissionType::ExecuteCommand, "npm"));
        // Different command should not match
        assert!(!rule.matches(PermissionType::ExecuteCommand, "yarn"));
        // Note: "npm install" matching would need prefix matching, which is current behavior
        // but the test expectation was wrong
    }

    #[test]
    fn test_non_path_resources_session_ids() {
        // Session ID permissions should match exactly
        let grant = SessionGrant::new("session_abc123", Duration::from_secs(3600));
        assert!(grant.matches(PermissionType::TerminalSession, "session_abc123"));
        assert!(!grant.matches(PermissionType::TerminalSession, "session_xyz789"));
    }

    #[test]
    fn test_permission_rule_expiration() {
        let rule = PermissionRule::new(PermissionType::WriteFile, "/tmp/*", true)
            .with_expiration(chrono::Utc::now() - chrono::Duration::seconds(1)); // Expired

        assert!(!rule.matches(PermissionType::WriteFile, "/tmp/test.txt"));
    }

    #[test]
    fn test_session_grant_expiration() {
        let grant = SessionGrant::new("/tmp/*", Duration::from_secs(0)); // Immediately expired

        // Wait a bit to ensure expiration
        std::thread::sleep(std::time::Duration::from_millis(10));

        assert!(!grant.matches(PermissionType::WriteFile, "/tmp/test.txt"));
    }

    #[test]
    fn test_session_grant_dedup_same_pattern_replaces() {
        // (a) Re-granting the same pattern must NOT append a duplicate — it
        // replaces/refreshes the existing entry, so the grant set is bounded.
        let config = PermissionConfig::new();

        config.grant_session_permission(PermissionType::WriteFile, "/tmp/*");
        config.grant_session_permission(PermissionType::WriteFile, "/tmp/*");
        config.grant_session_permission(PermissionType::WriteFile, "/tmp/*");

        let count = config
            .session_grants
            .get(&PermissionType::WriteFile)
            .map(|m| m.len())
            .unwrap_or(0);
        assert_eq!(
            count, 1,
            "re-granting the same pattern must dedup to a single entry"
        );

        // A distinct pattern under the same perm_type is a separate entry.
        config.grant_session_permission(PermissionType::WriteFile, "/home/*");
        let count = config
            .session_grants
            .get(&PermissionType::WriteFile)
            .map(|m| m.len())
            .unwrap_or(0);
        assert_eq!(count, 2, "two distinct patterns must yield two entries");
    }

    #[test]
    fn test_session_grant_expired_auto_removed_on_grant() {
        // (b) An expired grant is removed (not merely skipped) by a subsequent
        // grant — opportunistic auto-cleanup inside grant_session_permission.
        let config = PermissionConfig::new();

        // Inject an already-expired grant directly (grant_session_permission
        // always uses the 30-min default, so we plant a 0-duration one here).
        {
            let mut grants = config
                .session_grants
                .entry(PermissionType::WriteFile)
                .or_default();
            grants.insert(
                "/var/*".to_string(),
                SessionGrant::new("/var/*", Duration::from_secs(0)),
            );
        }
        // Ensure the 0-duration grant has actually expired.
        std::thread::sleep(std::time::Duration::from_millis(10));

        let before = config
            .session_grants
            .get(&PermissionType::WriteFile)
            .map(|m| m.len())
            .unwrap_or(0);
        assert_eq!(before, 1, "precondition: expired grant is present");

        // Granting a new (distinct) pattern must prune the expired one.
        config.grant_session_permission(PermissionType::WriteFile, "/tmp/*");

        let grants = config
            .session_grants
            .get(&PermissionType::WriteFile)
            .expect("WriteFile grants bucket must exist");
        assert_eq!(grants.len(), 1, "expired grant must be auto-removed");
        assert!(grants.contains_key("/tmp/*"), "new grant must remain");
        assert!(!grants.contains_key("/var/*"), "expired grant must be gone");
    }

    #[test]
    fn test_session_grant_expired_removed_by_explicit_cleanup() {
        // (b, cont.) The explicit cleanup_expired_grants() still removes
        // expired entries too (behavior preserved from the old Vec impl).
        let config = PermissionConfig::new();
        {
            let mut grants = config
                .session_grants
                .entry(PermissionType::ExecuteCommand)
                .or_default();
            grants.insert(
                "npm".to_string(),
                SessionGrant::new("npm", Duration::from_secs(0)),
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(config
            .session_grants
            .get(&PermissionType::ExecuteCommand)
            .map(|m| m.contains_key("npm"))
            .unwrap_or(false));

        config.cleanup_expired_grants();

        let grants = config
            .session_grants
            .get(&PermissionType::ExecuteCommand)
            .expect("bucket must still exist after cleanup");
        assert!(grants.is_empty(), "expired grant must be removed");
    }

    #[test]
    fn test_is_session_granted_allow_deny_expired() {
        // (c) is_session_granted returns the correct allow/deny for matching,
        // non-matching, and expired patterns.
        let config = PermissionConfig::new();
        config.grant_session_permission(PermissionType::HttpRequest, "api.example.com");

        // Matching pattern → granted.
        assert!(config.is_session_granted(PermissionType::HttpRequest, "api.example.com"));
        // Non-matching resource → not granted.
        assert!(!config.is_session_granted(PermissionType::HttpRequest, "other.example.com"));
        // Different perm_type → not granted.
        assert!(!config.is_session_granted(PermissionType::ExecuteCommand, "api.example.com"));

        // Expired grant → not granted (deny).
        {
            let mut grants = config
                .session_grants
                .entry(PermissionType::TerminalSession)
                .or_default();
            grants.insert(
                "session_1".to_string(),
                SessionGrant::new("session_1", Duration::from_secs(0)),
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(!config.is_session_granted(PermissionType::TerminalSession, "session_1"));
    }

    #[test]
    fn test_is_session_granted_glob_match() {
        // (d) Matching is glob-based: a wildcard pattern matches a concrete
        // resource that is NOT identical to the pattern string. This is exactly
        // why the match itself cannot be a single O(1) hashed lookup.
        let config = PermissionConfig::new();
        config.grant_session_permission(PermissionType::WriteFile, "/tmp/*");

        // /tmp/foo.rs matches the /tmp/* glob even though it isn't the key.
        assert!(config.is_session_granted(PermissionType::WriteFile, "/tmp/foo.rs"));
        // A path outside the glob does not.
        assert!(!config.is_session_granted(PermissionType::WriteFile, "/var/foo.rs"));
    }

    #[test]
    fn test_empty_strings() {
        // Empty pattern should not match anything
        let rule = PermissionRule::new(PermissionType::WriteFile, "", true);
        assert!(!rule.matches(PermissionType::WriteFile, "/tmp/test.txt"));

        // Non-empty pattern should not match empty resource
        assert!(!rule.matches(PermissionType::WriteFile, ""));
    }

    #[test]
    fn test_special_characters_in_paths() {
        // Paths with special characters should be handled correctly
        let rule = PermissionRule::new(PermissionType::WriteFile, "/tmp/*", true);
        assert!(rule.matches(PermissionType::WriteFile, "/tmp/file-with-dash.txt"));
        assert!(rule.matches(PermissionType::WriteFile, "/tmp/file_with_underscore.txt"));
        assert!(rule.matches(PermissionType::WriteFile, "/tmp/file.with.dots.txt"));
    }

    #[test]
    fn test_traversal_variants() {
        let rule = PermissionRule::new(PermissionType::WriteFile, "/safe/*", true);

        // All these should be rejected
        assert!(!rule.matches(PermissionType::WriteFile, "/safe/../etc/passwd"));
        assert!(!rule.matches(PermissionType::WriteFile, "/safe/./etc/passwd"));
        assert!(!rule.matches(PermissionType::WriteFile, "/safe/subdir/../../etc/passwd"));
        assert!(!rule.matches(PermissionType::WriteFile, "/safe//etc/passwd")); // Double slash
    }

    #[test]
    fn test_has_path_traversal() {
        // Test the helper function directly
        assert!(has_path_traversal("../etc/passwd"));
        assert!(has_path_traversal("/safe/../etc/passwd"));
        // Note: "./" is CurrentDir, not ParentDir, so it's not considered traversal
        assert!(!has_path_traversal("/safe/./etc/passwd"));
        assert!(!has_path_traversal("/safe/etc/passwd"));
    }

    #[test]
    fn test_wildcard_matches_anything() {
        // Wildcard patterns should match any resource
        assert!(match_glob_pattern("*", "anything"));
        assert!(match_glob_pattern("*", "/any/path"));
        assert!(match_glob_pattern("**/*", "/any/deep/path"));
        assert!(match_glob_pattern("*", "api.example.com"));
        assert!(match_glob_pattern("*", "C:/Windows/file.txt"));
    }

    #[test]
    fn test_windows_paths() {
        // Windows-style paths should work with basic normalization
        // On Unix, we test the normalization logic directly
        let normalized = normalize_path_basic("C:/Users/file.txt");
        assert_eq!(normalized, "C:/Users/file.txt");

        let normalized = normalize_path_basic("C:\\Users\\file.txt");
        assert_eq!(normalized, "C:/Users/file.txt");

        // Test that drive letter is preserved
        assert!(normalized.contains(':'));
        assert!(normalized.starts_with("C:/"));
    }

    #[test]
    fn test_permission_type_mismatch() {
        let rule = PermissionRule::new(PermissionType::WriteFile, "/tmp/*", true);

        // Should not match if permission types don't match
        assert!(!rule.matches(PermissionType::ExecuteCommand, "/tmp/test.txt"));
        assert!(!rule.matches(PermissionType::HttpRequest, "/tmp/test.txt"));
    }

    #[test]
    fn test_config_enabled_disabled() {
        let config = PermissionConfig::new();

        // By default, enabled
        assert!(config.is_enabled());

        // Should require confirmation when enabled
        assert!(config.needs_confirmation(PermissionType::WriteFile, "/tmp/test.txt"));

        // Disable checks
        config.set_enabled(false);
        assert!(!config.is_enabled());

        // Should not require confirmation when disabled
        assert!(!config.needs_confirmation(PermissionType::WriteFile, "/tmp/test.txt"));
    }

    // TOCTOU Protection Tests
    #[test]
    fn test_path_symlink_switch_blocked() {
        use std::io::Write;

        // Create a temporary directory for testing
        let temp_dir = std::env::temp_dir().join(format!("toctou_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create allowed directory
        let allowed_dir = temp_dir.join("allowed");
        std::fs::create_dir_all(&allowed_dir).unwrap();

        // Create a file in the allowed directory
        let test_file = allowed_dir.join("test.txt");
        {
            let mut file = std::fs::File::create(&test_file).unwrap();
            file.write_all(b"original content").unwrap();
        }

        // Verify we can open the real file
        assert!(open_file_no_follow(&test_file).is_ok());

        // Create a symlink pointing outside the allowed directory
        let symlink_file = allowed_dir.join("symlink.txt");
        let outside_file = temp_dir.join("outside.txt");
        {
            let mut file = std::fs::File::create(&outside_file).unwrap();
            file.write_all(b"sensitive content").unwrap();
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            symlink(&outside_file, &symlink_file).unwrap();

            // Attempting to open the symlink should fail
            let result = open_file_no_follow(&symlink_file);
            assert!(result.is_err(), "Should block opening symlink");

            // Verify we can't read the symlink target's content
            if let Err(e) = result {
                // On macOS (errno 62: Too many levels of symbolic links)
                // On Linux (errno 40: Too many symbolic links)
                // Both indicate the symlink was blocked
                let is_blocked = e.kind() == std::io::ErrorKind::PermissionDenied
                    || e.kind() == std::io::ErrorKind::InvalidInput
                    || e.kind() == std::io::ErrorKind::Other
                    || e.raw_os_error() == Some(62)  // macOS ELOOP
                    || e.raw_os_error() == Some(40); // Linux ELOOP
                assert!(is_blocked, "Expected symlink to be blocked, got: {:?}", e);
            }
        }

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_path_traversal_blocked() {
        // Test path traversal in various forms
        let test_cases = vec![
            "/safe/../etc/passwd",
            "/safe/subdir/../../etc/passwd",
            "/safe/./../etc/passwd",
        ];

        for path in test_cases {
            let config = PermissionConfig::new();
            config.add_rule(PermissionRule::new(
                PermissionType::WriteFile,
                "/safe/*",
                true,
            ));

            // All traversal attempts should be rejected
            assert!(
                config.needs_confirmation(PermissionType::WriteFile, path),
                "Path traversal should require confirmation (be blocked by default): {}",
                path
            );
        }
    }

    #[test]
    fn test_path_within_allowed_directory() {
        let config = PermissionConfig::new();
        // Use ** for recursive matching to include subdirectories
        config.add_rule(PermissionRule::new(
            PermissionType::WriteFile,
            "/tmp/allowed/**",
            true,
        ));

        // Files within allowed directory should be allowed
        assert!(!config.needs_confirmation(PermissionType::WriteFile, "/tmp/allowed/file.txt"));
        assert!(
            !config.needs_confirmation(PermissionType::WriteFile, "/tmp/allowed/subdir/file.txt")
        );

        // Files outside should require confirmation
        assert!(config.needs_confirmation(PermissionType::WriteFile, "/tmp/other/file.txt"));
        assert!(config.needs_confirmation(PermissionType::WriteFile, "/etc/passwd"));
    }

    #[test]
    fn test_secure_file_create_parent_validation() {
        use std::io::Write;

        let temp_dir =
            std::env::temp_dir().join(format!("secure_create_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let allowed_dir = temp_dir.join("allowed");
        std::fs::create_dir_all(&allowed_dir).unwrap();

        // Creating a file in an allowed directory should work
        let new_file = allowed_dir.join("new_file.txt");
        let result = open_file_for_write_secure(&new_file);
        assert!(
            result.is_ok(),
            "Should be able to create file in allowed directory"
        );

        if let Ok(mut file) = result {
            file.write_all(b"test content").unwrap();
            drop(file);

            // Verify file was created
            assert!(new_file.exists());
            let content = std::fs::read_to_string(&new_file).unwrap();
            assert_eq!(content, "test content");
        }

        // Creating in non-existent directory should fail
        let bad_path = temp_dir.join("nonexistent_dir").join("file.txt");
        let result = open_file_for_write_secure(&bad_path);
        assert!(result.is_err(), "Should fail when parent doesn't exist");

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_whitelist_with_session_grants() {
        let config = PermissionConfig::new();

        // Add whitelist rule
        config.add_rule(PermissionRule::new(
            PermissionType::WriteFile,
            "/tmp/*",
            true,
        ));

        // Whitelist allows, should not require confirmation
        assert!(!config.needs_confirmation(PermissionType::WriteFile, "/tmp/test.txt"));

        // Outside whitelist, requires confirmation
        assert!(config.needs_confirmation(PermissionType::WriteFile, "/home/test.txt"));

        // Grant session permission for different path
        config.grant_session_permission(PermissionType::WriteFile, "/home/*");

        // Now /home should not require confirmation (if /home exists)
        // Note: This depends on whether /home exists on the system
    }

    #[test]
    fn test_multiple_session_grants() {
        let config = PermissionConfig::new();

        // Grant multiple session permissions
        config.grant_session_permission(PermissionType::WriteFile, "/tmp/*");
        config.grant_session_permission(PermissionType::WriteFile, "/home/*");

        // Both should work if paths exist
        assert!(!config.needs_confirmation(PermissionType::WriteFile, "/tmp/test.txt"));
        // Note: /home may not exist on all systems
    }

    #[test]
    fn test_deny_overrides_allow() {
        let config = PermissionConfig::new();

        // Add allow rule
        config.add_rule(PermissionRule::new(
            PermissionType::WriteFile,
            "/tmp/*",
            true,
        ));

        // Add deny rule (should override allow)
        config.add_rule(PermissionRule::new(
            PermissionType::WriteFile,
            "/tmp/sensitive.txt",
            false,
        ));

        // Normal files in /tmp should be allowed
        assert_eq!(
            config.is_whitelist_allowed(PermissionType::WriteFile, "/tmp/test.txt"),
            Some(true)
        );

        // Sensitive file should be denied
        assert_eq!(
            config.is_whitelist_allowed(PermissionType::WriteFile, "/tmp/sensitive.txt"),
            Some(false)
        );
    }

    #[test]
    fn test_non_path_permissions_integration() {
        let config = PermissionConfig::new();

        // HTTP domain permission
        config.grant_session_permission(PermissionType::HttpRequest, "api.example.com");
        assert!(!config.needs_confirmation(PermissionType::HttpRequest, "api.example.com"));

        // Command permission
        config.grant_session_permission(PermissionType::ExecuteCommand, "npm");
        assert!(!config.needs_confirmation(PermissionType::ExecuteCommand, "npm"));
    }

    #[test]
    fn test_permission_mode_default_is_default() {
        let config = PermissionConfig::new();
        assert_eq!(config.mode(), PermissionMode::Default);
    }

    #[test]
    fn test_permission_mode_set_and_get() {
        let config = PermissionConfig::new();
        config.set_mode(PermissionMode::Plan);
        assert_eq!(config.mode(), PermissionMode::Plan);
        config.set_mode(PermissionMode::BypassPermissions);
        assert_eq!(config.mode(), PermissionMode::BypassPermissions);
    }

    #[test]
    fn test_permission_mode_serialize_roundtrip() {
        let mut serializable = SerializablePermissionConfig::default();
        assert!(serializable.mode.is_none());

        serializable.mode = Some(PermissionMode::Plan);
        let json = serde_json::to_string(&serializable).unwrap();
        assert!(json.contains("plan"));

        let deserialized: SerializablePermissionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.mode, Some(PermissionMode::Plan));
    }

    #[test]
    fn test_permission_mode_backward_compat_no_mode() {
        // Old serialized format without mode field should deserialize to Default
        let json = r#"{"whitelist":[],"enabled":true,"session_grant_duration_secs":1800}"#;
        let deserialized: SerializablePermissionConfig = serde_json::from_str(json).unwrap();
        assert!(deserialized.mode.is_none());

        let config = PermissionConfig::from_serializable(deserialized);
        assert_eq!(config.mode(), PermissionMode::Default);
    }

    #[test]
    fn test_permission_config_merge() {
        let user = PermissionConfig::new();
        user.add_rule(PermissionRule::new(
            PermissionType::WriteFile,
            "/tmp/user/*",
            true,
        ));
        user.set_mode(PermissionMode::Default);

        let project = PermissionConfig::new();
        project.add_rule(PermissionRule::new(
            PermissionType::WriteFile,
            "/tmp/project/*",
            true,
        ));
        project.add_rule(PermissionRule::new(
            PermissionType::WriteFile,
            "/tmp/project/secret",
            false,
        ));
        project.set_mode(PermissionMode::AcceptEdits);

        let merged = user.merge(&project);

        // Project mode takes precedence
        assert_eq!(merged.mode(), PermissionMode::AcceptEdits);

        // Both user and project allow rules are present
        assert!(!merged.needs_confirmation(PermissionType::WriteFile, "/tmp/user/code.rs"));
        assert!(!merged.needs_confirmation(PermissionType::WriteFile, "/tmp/project/code.rs"));

        // Project deny rule overrides user allow
        assert!(merged.needs_confirmation(PermissionType::WriteFile, "/tmp/project/secret"));
    }

    #[test]
    fn test_permission_mode_description() {
        assert!(!PermissionMode::Default.description().is_empty());
        assert!(!PermissionMode::Plan.description().is_empty());
        assert!(!PermissionMode::AcceptEdits.description().is_empty());
        assert!(!PermissionMode::DontAsk.description().is_empty());
        assert!(!PermissionMode::BypassPermissions.description().is_empty());
        assert!(!PermissionMode::Auto.description().is_empty());
    }

    #[test]
    fn test_confirm_threshold_defaults_to_low() {
        let config = PermissionConfig::new();
        assert_eq!(config.confirm_threshold(), RiskLevel::Low);
        // With the default Low threshold, every (medium/high) op needs confirmation.
        assert!(config.needs_confirmation(PermissionType::WriteFile, "/tmp/x.txt"));
        assert!(config.needs_confirmation(PermissionType::ExecuteCommand, "ls"));
    }

    #[test]
    fn test_confirm_threshold_high_auto_allows_below_high() {
        let config = PermissionConfig::new();
        config.set_confirm_threshold(RiskLevel::High);
        // Medium-risk ops auto-allow (no confirmation) ...
        assert!(!config.needs_confirmation(PermissionType::WriteFile, "/tmp/x.txt"));
        assert!(!config.needs_confirmation(PermissionType::HttpRequest, "api.example.com"));
        // ... high-risk ops still require confirmation.
        assert!(config.needs_confirmation(PermissionType::ExecuteCommand, "rm -rf /"));
        assert!(config.needs_confirmation(PermissionType::DeleteOperation, "/etc/passwd"));
        assert!(config.needs_confirmation(PermissionType::GitWrite, "push"));
        assert!(config.needs_confirmation(PermissionType::TerminalSession, "sh"));
    }

    #[test]
    fn test_confirm_threshold_explicit_deny_overrides_auto_allow() {
        let config = PermissionConfig::new();
        config.set_confirm_threshold(RiskLevel::High);
        // An explicit deny rule still forces confirmation for a medium-risk op.
        config.add_rule(PermissionRule::new(
            PermissionType::WriteFile,
            "/secret/*",
            false,
        ));
        assert!(config.needs_confirmation(PermissionType::WriteFile, "/secret/x.txt"));
        // Other medium-risk writes still auto-allow.
        assert!(!config.needs_confirmation(PermissionType::WriteFile, "/tmp/x.txt"));
    }

    #[test]
    fn test_confirm_threshold_serialize_roundtrip() {
        let config = PermissionConfig::new();
        config.set_confirm_threshold(RiskLevel::High);
        let serializable = config.to_serializable();
        assert_eq!(serializable.confirm_threshold, Some(RiskLevel::High));
        let json = serde_json::to_string(&serializable).unwrap();
        assert!(json.contains("high"));
        let restored = PermissionConfig::from_serializable(serde_json::from_str(&json).unwrap());
        assert_eq!(restored.confirm_threshold(), RiskLevel::High);
    }

    #[test]
    fn test_confirm_threshold_backward_compat_defaults_low() {
        // Old serialized config without confirm_threshold → defaults to Low.
        let json = r#"{"whitelist":[],"enabled":true,"session_grant_duration_secs":1800}"#;
        let restored = PermissionConfig::from_serializable(serde_json::from_str(json).unwrap());
        assert_eq!(restored.confirm_threshold(), RiskLevel::Low);
    }

    #[test]
    fn forced_confirmation_builtin_dangerous_command() {
        let config = PermissionConfig::new();
        // Built-in detection: a hard-dangerous (Deny) shell command forces a
        // prompt even with no configured ask rules.
        assert!(config.requires_forced_confirmation(
            "Bash",
            &serde_json::json!({ "command": "eval 'cat /etc/passwd'" }),
        ));
        // A benign command is not forced.
        assert!(!config
            .requires_forced_confirmation("Bash", &serde_json::json!({ "command": "ls -la" }),));
    }

    #[test]
    fn forced_confirmation_js_repl_always_asks() {
        let config = PermissionConfig::new();
        // js_repl runs arbitrary code (it can shell out via `child_process`), so
        // it must force a prompt even with no configured ask rules — the
        // equivalent of a super-dangerous Bash command, including under
        // BypassPermissions where non-forced tools are otherwise skipped.
        assert!(config.requires_forced_confirmation(
            "js_repl",
            &serde_json::json!({ "code": "require('child_process').execSync('id')" }),
        ));
        // Case-insensitive, and even a benign-looking snippet forces — the code
        // body is not statically analyzable at this layer.
        assert!(config
            .requires_forced_confirmation("JS_REPL", &serde_json::json!({ "code": "1 + 1" }),));
        // A non-eval tool with no matching ask rule is still not forced.
        assert!(!config
            .requires_forced_confirmation("Read", &serde_json::json!({ "file_path": "/tmp/x" }),));
    }

    #[test]
    fn forced_confirmation_configured_ask_rule() {
        let config = PermissionConfig::new();
        config.set_ask_rules(["Bash(rm -rf *)".to_string(), "Write(/etc/**)".to_string()]);

        assert!(config.requires_forced_confirmation(
            "Bash",
            &serde_json::json!({ "command": "rm -rf /tmp/data" }),
        ));
        assert!(config.requires_forced_confirmation(
            "Write",
            &serde_json::json!({ "file_path": "/etc/hosts" }),
        ));
        // Non-matching call is not forced.
        assert!(
            !config.requires_forced_confirmation(
                "Bash",
                &serde_json::json!({ "command": "git status" }),
            )
        );
    }

    #[test]
    fn ask_rules_round_trip_through_serialization() {
        let config = PermissionConfig::new();
        config.set_ask_rules(["Bash(rm -rf *)".to_string(), "Read".to_string()]);
        let json = serde_json::to_string(&config.to_serializable()).unwrap();
        let restored = PermissionConfig::from_serializable(serde_json::from_str(&json).unwrap());
        assert_eq!(
            restored.ask_rule_patterns(),
            vec!["Bash(rm -rf *)".to_string(), "Read".to_string()]
        );
    }

    #[test]
    fn temporary_grant_snapshot_is_complete_deterministic_and_omits_expired_entries() {
        let config = PermissionConfig::new();
        config.grant_session_permission(PermissionType::HttpRequest, "api.example.com");
        config.grant_scoped_session_permission(
            "session-a",
            PermissionType::ExecuteCommand,
            "cargo test",
        );
        config.deny_scoped_session_permission(
            "session-a",
            PermissionType::WriteFile,
            "/private/**",
        );
        config.grant_once(
            "session-a",
            "request-1",
            PermissionType::GitWrite,
            "git push".to_string(),
        );

        let session = config
            .scoped_session_grants
            .entry("session-expired".to_string())
            .or_default();
        session
            .entry(PermissionType::ExecuteCommand)
            .or_default()
            .insert(
                "expired".to_string(),
                SessionGrant {
                    granted_at: Instant::now(),
                    expires_at: Instant::now(),
                    resource_pattern: "expired".to_string(),
                },
            );
        drop(session);

        let snapshot = config.temporary_grants();
        assert_eq!(snapshot.len(), 4);
        assert_eq!(
            snapshot
                .iter()
                .map(|grant| grant.matcher.as_str())
                .collect::<Vec<_>>(),
            vec!["api.example.com", "cargo test", "/private/**", "git push"]
        );
        assert!(!snapshot.iter().any(|grant| grant.matcher == "expired"));

        let session_allow = snapshot
            .iter()
            .find(|grant| grant.matcher == "cargo test")
            .unwrap();
        assert_eq!(session_allow.scope, TemporaryPermissionGrantScope::Session);
        assert_eq!(session_allow.effect, TemporaryPermissionGrantEffect::Allow);
        assert_eq!(session_allow.session_id.as_deref(), Some("session-a"));
        assert!(session_allow.granted_at.is_some());
        assert!(session_allow.expires_at.is_some());

        let session_deny = snapshot
            .iter()
            .find(|grant| grant.matcher == "/private/**")
            .unwrap();
        assert_eq!(session_deny.effect, TemporaryPermissionGrantEffect::Deny);

        let one_shot = snapshot
            .iter()
            .find(|grant| grant.scope == TemporaryPermissionGrantScope::OneShot)
            .unwrap();
        assert_eq!(one_shot.request_id.as_deref(), Some("request-1"));
        assert!(one_shot.granted_at.is_none());
        assert!(one_shot.expires_at.is_none());

        assert!(config.consume_once(
            "session-a",
            "request-1",
            PermissionType::GitWrite,
            "git push"
        ));
        assert!(!config
            .temporary_grants()
            .iter()
            .any(|grant| grant.scope == TemporaryPermissionGrantScope::OneShot));
    }

    #[test]
    fn scoped_session_grants_do_not_leak_between_sessions() {
        let config = PermissionConfig::new();
        config.grant_scoped_session_permission(
            "session-a",
            PermissionType::ExecuteCommand,
            "git status",
        );
        assert!(config.is_scoped_session_granted(
            "session-a",
            PermissionType::ExecuteCommand,
            "git status"
        ));
        assert!(!config.is_scoped_session_granted(
            "session-b",
            PermissionType::ExecuteCommand,
            "git status"
        ));
        assert!(config.consume_scoped_session_grant(
            "session-a",
            PermissionType::ExecuteCommand,
            "git status"
        ));
        assert!(!config.consume_scoped_session_grant(
            "session-a",
            PermissionType::ExecuteCommand,
            "git status"
        ));

        config.grant_once(
            "session-a",
            "call-1",
            PermissionType::ExecuteCommand,
            "git status".into(),
        );
        assert!(!config.consume_once(
            "session-b",
            "call-1",
            PermissionType::ExecuteCommand,
            "git status"
        ));
        assert!(!config.consume_once(
            "session-a",
            "call-2",
            PermissionType::ExecuteCommand,
            "git status"
        ));

        config.grant_once(
            "session-a",
            "multi",
            PermissionType::WriteFile,
            "/tmp/a".into(),
        );
        config.grant_once(
            "session-a",
            "multi",
            PermissionType::ExecuteCommand,
            "cargo test".into(),
        );
        assert!(!config.consume_once(
            "session-a",
            "multi",
            PermissionType::WriteFile,
            "/tmp/not-a"
        ));
        assert!(config.consume_once(
            "session-a",
            "multi",
            PermissionType::ExecuteCommand,
            "cargo test"
        ));
        assert!(config.consume_once("session-a", "multi", PermissionType::WriteFile, "/tmp/a"));

        config.grant_once(
            "session-a",
            "duplicate",
            PermissionType::ExecuteCommand,
            "cargo test".into(),
        );
        config.grant_once(
            "session-a",
            "duplicate",
            PermissionType::ExecuteCommand,
            "cargo test".into(),
        );
        assert!(config.consume_once(
            "session-a",
            "duplicate",
            PermissionType::ExecuteCommand,
            "cargo test"
        ));
        assert!(!config.consume_once(
            "session-a",
            "duplicate",
            PermissionType::ExecuteCommand,
            "cargo test"
        ));

        let config = std::sync::Arc::new(config);
        config.grant_once(
            "session-a",
            "concurrent",
            PermissionType::ExecuteCommand,
            "cargo test".into(),
        );
        let successes = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let config = config.clone();
                    scope.spawn(move || {
                        config.consume_once(
                            "session-a",
                            "concurrent",
                            PermissionType::ExecuteCommand,
                            "cargo test",
                        )
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("consumer"))
                .filter(|consumed| *consumed)
                .count()
        });
        assert_eq!(successes, 1, "a one-shot grant must be consumed once");
        assert!(config.consume_once(
            "session-a",
            "call-1",
            PermissionType::ExecuteCommand,
            "git status"
        ));
        assert!(!config.consume_once(
            "session-a",
            "call-1",
            PermissionType::ExecuteCommand,
            "git status"
        ));
    }

    #[test]
    fn typed_session_exact_resource_never_becomes_a_glob() {
        let config = PermissionConfig::new();
        config
            .grant_typed_scoped_session_permission(
                "session-a",
                PermissionType::ExecuteCommand,
                PermissionMatcher {
                    id: "exact_resource".to_string(),
                    kind: PermissionMatcherKind::ExactResource,
                    value: "rm *.log".to_string(),
                },
            )
            .expect("valid exact matcher");

        assert!(config.is_scoped_session_granted(
            "session-a",
            PermissionType::ExecuteCommand,
            "rm *.log"
        ));
        assert!(!config.is_scoped_session_granted(
            "session-a",
            PermissionType::ExecuteCommand,
            "rm audit.log"
        ));
        assert!(!config.is_scoped_session_granted(
            "session-b",
            PermissionType::ExecuteCommand,
            "rm *.log"
        ));
    }

    #[test]
    fn typed_session_matcher_kinds_retain_their_server_semantics() {
        let config = PermissionConfig::new();
        config
            .grant_typed_scoped_session_permission(
                "session-a",
                PermissionType::WriteFile,
                PermissionMatcher {
                    id: "workspace_subtree".to_string(),
                    kind: PermissionMatcherKind::PathSubtree,
                    value: "/tmp/workspace".to_string(),
                },
            )
            .expect("valid subtree matcher");
        config
            .grant_typed_scoped_session_permission(
                "session-a",
                PermissionType::ExecuteCommand,
                PermissionMatcher {
                    id: "command_prefix".to_string(),
                    kind: PermissionMatcherKind::CommandPrefix,
                    value: "cargo test".to_string(),
                },
            )
            .expect("valid command prefix");
        config
            .deny_typed_scoped_session_permission(
                "session-a",
                PermissionType::ExecuteCommand,
                PermissionMatcher {
                    id: "deny_prefix".to_string(),
                    kind: PermissionMatcherKind::CommandPrefix,
                    value: "git push".to_string(),
                },
            )
            .expect("valid command prefix deny");

        assert!(config.is_scoped_session_granted(
            "session-a",
            PermissionType::WriteFile,
            "/tmp/workspace/src/lib.rs"
        ));
        assert!(!config.is_scoped_session_granted(
            "session-a",
            PermissionType::WriteFile,
            "/tmp/workspace-sibling/src/lib.rs"
        ));
        assert!(config.is_scoped_session_granted(
            "session-a",
            PermissionType::ExecuteCommand,
            "cargo test -p bamboo-permission"
        ));
        assert!(!config.is_scoped_session_granted(
            "session-a",
            PermissionType::ExecuteCommand,
            "cargo nextest run"
        ));
        assert!(config.is_scoped_session_denied(
            "session-a",
            PermissionType::ExecuteCommand,
            "git push origin dev"
        ));
        assert!(!config.is_scoped_session_denied(
            "session-a",
            PermissionType::ExecuteCommand,
            "git push origin dev && rm -rf /"
        ));
        assert!(!config.is_scoped_session_denied(
            "session-a",
            PermissionType::ExecuteCommand,
            "git status"
        ));
    }

    #[test]
    fn typed_session_matchers_expire_and_cleanup() {
        let config = PermissionConfig::with_settings(true, Duration::ZERO);
        config
            .grant_typed_scoped_session_permission(
                "session-a",
                PermissionType::ExecuteCommand,
                PermissionMatcher {
                    id: "exact_resource".to_string(),
                    kind: PermissionMatcherKind::ExactResource,
                    value: "cargo test".to_string(),
                },
            )
            .expect("valid exact matcher");
        config
            .deny_typed_scoped_session_permission(
                "session-a",
                PermissionType::ExecuteCommand,
                PermissionMatcher {
                    id: "command_prefix".to_string(),
                    kind: PermissionMatcherKind::CommandPrefix,
                    value: "git push".to_string(),
                },
            )
            .expect("valid command prefix");

        assert!(!config.is_scoped_session_granted(
            "session-a",
            PermissionType::ExecuteCommand,
            "cargo test"
        ));
        assert!(!config.is_scoped_session_denied(
            "session-a",
            PermissionType::ExecuteCommand,
            "git push origin dev"
        ));
        config.cleanup_expired_grants();
        assert!(config.temporary_grants().is_empty());
    }

    #[test]
    fn file_matchers_canonicalize_static_prefixes_without_widening() {
        let temp = tempfile::tempdir().expect("tempdir");
        let alias_path = temp.path().join("future.txt");
        let canonical_path = std::fs::canonicalize(temp.path())
            .expect("canonical temp")
            .join("future.txt");
        let alias = alias_path.to_string_lossy().to_string();
        let canonical = canonical_path.to_string_lossy().to_string();

        let config = PermissionConfig::new();
        config.grant_session_permission(PermissionType::WriteFile, alias.clone());
        assert!(config.is_session_granted(PermissionType::WriteFile, &canonical));

        config.grant_session_permission(PermissionType::DeleteOperation, alias.clone());
        assert!(config.is_session_granted(PermissionType::DeleteOperation, &canonical));

        let delete_deny =
            PermissionRule::new(PermissionType::DeleteOperation, alias.clone(), false);
        assert!(delete_deny.matches(PermissionType::DeleteOperation, &canonical));

        let command_deny =
            PermissionRule::new(PermissionType::DeleteOperation, "rm ./relative-file", false);
        assert!(command_deny.matches(PermissionType::DeleteOperation, "rm ./relative-file"));

        config.set_ask_rules([format!("Write({alias})")]);
        assert!(config
            .requires_forced_confirmation("Write", &serde_json::json!({"file_path": canonical})));

        let traversal = format!("{}/../escape.txt", temp.path().display());
        let deny = PermissionRule::new(PermissionType::WriteFile, traversal, false);
        assert!(
            !deny.matches(PermissionType::WriteFile, &alias),
            "a traversal matcher must fail closed rather than normalize broader"
        );
        #[cfg(unix)]
        assert_eq!(
            canonicalize_path_pattern_for_matching("/**").as_deref(),
            Some("/**")
        );
    }

    fn evaluation(
        session_id: &str,
        request_id: &str,
        resource: &str,
        bypass_requested: bool,
    ) -> PermissionEvaluation {
        PermissionEvaluation {
            request_id: request_id.to_string(),
            session_id: session_id.to_string(),
            workspace_path: None,
            tool_name: "Bash".to_string(),
            tool_args: serde_json::json!({"command": resource}),
            permission_type: PermissionType::ExecuteCommand,
            resource: resource.to_string(),
            operation_summary: format!("execute {resource}"),
            risk_level: RiskLevel::High,
            bypass_requested,
            auto_approve_requested: false,
            platform_hard_deny: None,
            consume_once: true,
            supported_decisions: crate::policy::PermissionDecisionKind::all_supported(),
        }
    }

    #[test]
    fn typed_precedence_deny_and_forced_ask_beat_bypass_and_remembered_allow() {
        let config = PermissionConfig::new();
        config.grant_scoped_session_permission(
            "a",
            PermissionType::ExecuteCommand,
            "sudo rm -rf /tmp/target",
        );
        let hard = evaluation("a", "hard", "sudo rm -rf /tmp/target", true);
        assert!(matches!(
            config.evaluate(hard),
            PermissionOutcome::Ask(PermissionRequest {
                reason_code: PermissionReasonCode::HardDangerous,
                ..
            })
        ));

        config.add_rule(PermissionRule::new(
            PermissionType::ExecuteCommand,
            "sudo rm -rf /tmp/target",
            false,
        ));
        assert!(matches!(
            config.evaluate(evaluation("a", "denied", "sudo rm -rf /tmp/target", true)),
            PermissionOutcome::Deny {
                reason: PermissionDenyReason {
                    code: PermissionReasonCode::ExplicitDeny,
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn auto_skips_forced_asks_but_never_overrides_hard_denials() {
        let config = PermissionConfig::new();
        config.set_ask_rules(["Bash(eval *)".to_string()]);

        let mut auto = evaluation("auto", "hard", "eval 'echo allowed'", false);
        auto.auto_approve_requested = true;
        assert!(matches!(
            config.evaluate(auto.clone()),
            PermissionOutcome::Allow {
                source: PermissionDecisionSource::Auto,
                effective_policy: EffectivePermissionPolicy {
                    mode: PermissionMode::Auto,
                    auto_approve_requested: true,
                    ..
                }
            }
        ));

        auto.platform_hard_deny = Some("sandbox policy rejected operation".to_string());
        assert!(matches!(
            config.evaluate(auto.clone()),
            PermissionOutcome::Deny {
                reason: PermissionDenyReason {
                    code: PermissionReasonCode::PlatformHardDeny,
                    ..
                },
                ..
            }
        ));

        auto.platform_hard_deny = None;
        config.add_rule(PermissionRule::new(
            PermissionType::ExecuteCommand,
            "eval 'echo allowed'",
            false,
        ));
        assert!(matches!(
            config.evaluate(auto),
            PermissionOutcome::Deny {
                reason: PermissionDenyReason {
                    code: PermissionReasonCode::ExplicitDeny,
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn configured_auto_mode_uses_the_same_zero_prompt_precedence() {
        let config = PermissionConfig::new();
        config.set_mode(PermissionMode::Auto);

        assert!(matches!(
            config.evaluate(evaluation(
                "auto",
                "configured",
                "eval 'echo configured'",
                false
            )),
            PermissionOutcome::Allow {
                source: PermissionDecisionSource::Auto,
                effective_policy: EffectivePermissionPolicy {
                    mode: PermissionMode::Auto,
                    ..
                }
            }
        ));
    }

    #[test]
    fn explicit_bypass_overrides_global_auto_without_skipping_forced_confirmation() {
        let config = PermissionConfig::new();
        config.set_mode(PermissionMode::Auto);

        assert!(matches!(
            config.evaluate(evaluation(
                "legacy-bypass",
                "configured-auto",
                "sudo rm -rf /tmp/still-confirmed",
                true
            )),
            PermissionOutcome::Ask(PermissionRequest {
                reason_code: PermissionReasonCode::HardDangerous,
                effective_mode: PermissionMode::BypassPermissions,
                bypass_requested: true,
                auto_approve_requested: false,
                ..
            })
        ));
    }

    #[test]
    fn exact_one_shot_authorizes_only_the_parked_forced_invocation() {
        let config = PermissionConfig::new();
        config.grant_once(
            "a",
            "call-1",
            PermissionType::ExecuteCommand,
            "rm -rf /tmp/target".to_string(),
        );
        assert!(matches!(
            config.evaluate(evaluation("a", "call-1", "rm -rf /tmp/target", false)),
            PermissionOutcome::Allow {
                source: PermissionDecisionSource::OneShot,
                ..
            }
        ));
        assert!(matches!(
            config.evaluate(evaluation("a", "call-2", "rm -rf /tmp/target", false)),
            PermissionOutcome::Ask(_)
        ));
    }

    #[test]
    fn remembered_session_allow_isolated_from_sibling_session() {
        let config = PermissionConfig::new();
        config.grant_scoped_session_permission("a", PermissionType::ExecuteCommand, "cargo test");
        assert!(matches!(
            config.evaluate(evaluation("a", "a-1", "cargo test", false)),
            PermissionOutcome::Allow {
                source: PermissionDecisionSource::RememberedSession,
                ..
            }
        ));
        assert!(matches!(
            config.evaluate(evaluation("b", "b-1", "cargo test", false)),
            PermissionOutcome::Ask(_)
        ));
    }

    #[test]
    fn workspace_rule_requires_same_canonical_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        let sibling = tempfile::tempdir().unwrap();
        let workspace_path = workspace.path().to_string_lossy().to_string();
        let rule = DurablePermissionRule {
            id: "workspace-cargo".to_string(),
            permission_type: PermissionType::ExecuteCommand,
            effect: PermissionRuleEffect::Allow,
            scope: PermissionRuleScope::Workspace,
            workspace_path: Some(workspace_path.clone()),
            matcher: crate::policy::PermissionMatcher {
                id: "cargo-test".to_string(),
                kind: crate::policy::PermissionMatcherKind::CommandPrefix,
                value: "cargo test".to_string(),
            },
            source: PermissionRuleSource::User,
            expires_at: None,
        };
        let config = PermissionConfig::new();
        config.add_durable_rule(rule).unwrap();
        let mut own = evaluation("a", "own", "cargo test -p bamboo", false);
        own.workspace_path = Some(workspace_path);
        assert!(matches!(
            config.evaluate(own),
            PermissionOutcome::Allow {
                source: PermissionDecisionSource::RememberedRule { .. },
                ..
            }
        ));
        let mut other = evaluation("b", "other", "cargo test -p bamboo", false);
        other.workspace_path = Some(sibling.path().to_string_lossy().to_string());
        assert!(matches!(config.evaluate(other), PermissionOutcome::Ask(_)));
    }

    #[test]
    fn decision_receipt_is_idempotent_and_conflicting_replay_fails() {
        let config = PermissionConfig::new();
        let allow = PermissionDecision {
            request_id: "req".to_string(),
            request_generation: "generation-1".to_string(),
            decision: crate::policy::PermissionDecisionKind::AllowOnce,
            matcher_id: None,
            expected_policy_revision: None,
            confirm_global: false,
        };
        assert_eq!(config.record_decision("a", allow.clone()), Ok(false));
        assert_eq!(config.record_decision("a", allow), Ok(true));
        let deny = PermissionDecision {
            request_id: "req".to_string(),
            request_generation: "generation-1".to_string(),
            decision: crate::policy::PermissionDecisionKind::DenyOnce,
            matcher_id: None,
            expected_policy_revision: None,
            confirm_global: false,
        };
        assert!(config.record_decision("a", deny).is_err());

        let receipt = config
            .decision_receipt("a", "req", "generation-1")
            .expect("original receipt");
        let restored = PermissionConfig::new();
        assert_eq!(restored.record_decision_receipt(receipt.clone()), Ok(false));
        assert_eq!(
            restored.decision_receipt("a", "req", "generation-1"),
            Some(receipt)
        );
    }

    #[test]
    fn reused_request_id_keeps_generations_independent() {
        let config = PermissionConfig::new();
        let old_decision = PermissionDecision {
            request_id: "req".to_string(),
            request_generation: "generation-1".to_string(),
            decision: crate::policy::PermissionDecisionKind::AllowOnce,
            matcher_id: None,
            expected_policy_revision: None,
            confirm_global: false,
        };
        assert_eq!(config.record_decision("a", old_decision.clone()), Ok(false));
        config.grant_once(
            "a",
            "req",
            PermissionType::ExecuteCommand,
            "old-resource".to_string(),
        );

        let mut new_request = match config.evaluate(evaluation("a", "req", "new-resource", false)) {
            PermissionOutcome::Ask(request) => request,
            other => panic!("expected a new permission request, got {other:?}"),
        };
        new_request.request_generation = "generation-2".to_string();
        config.register_pending_request(new_request.clone());

        assert_eq!(
            config
                .pending_request("a", "req")
                .map(|request| request.request_generation),
            Some("generation-2".to_string())
        );
        assert!(config
            .decision_receipt("a", "req", "generation-2")
            .is_none());
        assert!(!config.consume_once("a", "req", PermissionType::ExecuteCommand, "old-resource"));
        assert_eq!(config.record_decision("a", old_decision), Ok(true));
        assert_eq!(
            config
                .pending_request("a", "req")
                .map(|request| request.request_generation),
            Some("generation-2".to_string())
        );
    }

    #[test]
    fn pending_registration_preserves_current_generation_grant_and_prunes_old_generation() {
        let config = PermissionConfig::new();
        for generation in ["generation-old", "generation-current"] {
            config
                .grant_once_for_generation(
                    "session-a",
                    "request-a",
                    generation,
                    PermissionType::ExecuteCommand,
                    "cargo test".to_string(),
                )
                .unwrap();
        }
        let mut request =
            match config.evaluate(evaluation("session-a", "request-a", "cargo test", false)) {
                PermissionOutcome::Ask(request) => request,
                other => panic!("expected a permission request, got {other:?}"),
            };
        request.request_generation = "generation-current".to_string();

        // Model the grant -> reconnect registration -> receipt interleaving.
        config.register_pending_request(request);

        assert!(!config.typed_one_shot_grants.contains_key(&(
            "session-a".to_string(),
            "request-a".to_string(),
            "generation-old".to_string(),
        )));
        assert!(config.typed_one_shot_grants.contains_key(&(
            "session-a".to_string(),
            "request-a".to_string(),
            "generation-current".to_string(),
        )));

        assert_eq!(
            config.record_decision(
                "session-a",
                PermissionDecision {
                    request_id: "request-a".to_string(),
                    request_generation: "generation-current".to_string(),
                    decision: crate::policy::PermissionDecisionKind::AllowOnce,
                    matcher_id: None,
                    expected_policy_revision: None,
                    confirm_global: false,
                },
            ),
            Ok(false)
        );
        assert!(config.pending_request("session-a", "request-a").is_none());
        assert!(config.consume_once_for_generation(
            "session-a",
            "request-a",
            "generation-current",
            PermissionType::ExecuteCommand,
            "cargo test",
        ));
    }

    #[test]
    fn pending_registration_removes_exact_generation_when_receipt_lands_before_insert() {
        let config = PermissionConfig::new();
        let mut request =
            match config.evaluate(evaluation("session-a", "request-a", "cargo test", false)) {
                PermissionOutcome::Ask(request) => request,
                other => panic!("expected a permission request, got {other:?}"),
            };
        request.request_generation = "generation-current".to_string();
        let decision = PermissionDecision {
            request_id: "request-a".to_string(),
            request_generation: "generation-current".to_string(),
            decision: crate::policy::PermissionDecisionKind::AllowOnce,
            matcher_id: None,
            expected_policy_revision: None,
            confirm_global: false,
        };

        config.register_pending_request_inner(request, || {
            assert_eq!(config.record_decision("session-a", decision), Ok(false));
        });

        assert!(config.pending_request("session-a", "request-a").is_none());
        assert!(config
            .decision_receipt("session-a", "request-a", "generation-current")
            .is_some());
    }

    #[tokio::test]
    async fn typed_one_shot_grant_requires_exact_replay_generation() {
        let config = PermissionConfig::new();
        config
            .grant_once_for_generation(
                "session-a",
                "reused-request",
                "generation-old",
                PermissionType::ExecuteCommand,
                "cargo test".to_string(),
            )
            .unwrap();

        assert!(matches!(
            config.evaluate(evaluation(
                "session-a",
                "reused-request",
                "cargo test",
                false,
            )),
            PermissionOutcome::Ask(_)
        ));
        crate::with_permission_replay_generation(
            "session-a",
            "reused-request",
            Some("generation-new"),
            async {
                assert!(matches!(
                    config.evaluate(evaluation(
                        "session-a",
                        "reused-request",
                        "cargo test",
                        false,
                    )),
                    PermissionOutcome::Ask(_)
                ));
            },
        )
        .await;
        crate::with_permission_replay_generation(
            "session-a",
            "reused-request",
            Some("generation-old"),
            async {
                assert!(matches!(
                    config.evaluate(evaluation(
                        "session-a",
                        "reused-request",
                        "cargo test",
                        false,
                    )),
                    PermissionOutcome::Allow {
                        source: PermissionDecisionSource::OneShot,
                        ..
                    }
                ));
            },
        )
        .await;

        assert!(matches!(
            config.evaluate(evaluation(
                "session-a",
                "reused-request",
                "cargo test",
                false,
            )),
            PermissionOutcome::Ask(_)
        ));
    }
}
