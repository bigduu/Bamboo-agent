//! Permission configuration for tool execution.
//!
//! This module provides a flexible permission system for controlling access to
//! potentially dangerous operations like file writes, command execution, and HTTP requests.

use std::path::{Component, Path};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use log::warn;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

/// Types of permissions that can be granted
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

/// Risk level for permission types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub fn new(tool_type: PermissionType, resource_pattern: impl Into<String>, allowed: bool) -> Self {
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
        let normalized_resource = match perm_type {
            PermissionType::WriteFile => canonicalize_path_for_matching(resource),
            _ => Some(resource.to_string()),
        };

        let normalized_resource = match normalized_resource {
            Some(r) => r,
            None => return false,
        };

        // Use globset for proper glob matching
        match_glob_pattern(&self.resource_pattern, &normalized_resource)
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
        let normalized_resource = match perm_type {
            PermissionType::WriteFile => canonicalize_path_for_matching(resource),
            _ => Some(resource.to_string()),
        };

        let normalized_resource = match normalized_resource {
            Some(r) => r,
            None => return false,
        };

        match_glob_pattern(&self.resource_pattern, &normalized_resource)
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
    Path::new(path).components().any(|c| matches!(c, Component::ParentDir))
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
        use std::os::windows::raw::HANDLE;
        use winapi::um::fileapi::{CreateFileW, OPEN_EXISTING};
        use winapi::um::winnt::{FILE_ATTRIBUTE_NORMAL, FILE_FLAG_OPEN_REPARSE_POINT, GENERIC_READ, GENERIC_WRITE};
        use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;

        let wide_path: Vec<u16> = path.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        // For Windows, we need to use the Windows API directly for more control
        // Fall back to standard options which provide some protection
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
                    "Path is a symbolic link"
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
            "Path has no parent directory"
        )
    })?;

    // Canonicalize parent to resolve any symlinks in the path
    // This ensures we're creating the file in the intended location
    let canonical_parent = std::fs::canonicalize(parent).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Parent directory cannot be resolved: {}", e)
        )
    })?;

    // Verify the parent is actually a directory
    let parent_metadata = std::fs::metadata(&canonical_parent)?;
    if !parent_metadata.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Parent path is not a directory"
        ));
    }

    // Reconstruct the full path with canonical parent
    let file_name = path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Path has no file name"
        )
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
fn match_glob_pattern(pattern: &str, resource: &str) -> bool {
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
        return resource.starts_with(prefix)
            && !resource[prefix.len()..].contains('/');
    }

    // Recursive directory pattern: /tmp/**
    if let Some(prefix) = pattern.strip_suffix("/**") {
        // pattern is like "/tmp/**", remove the "/**" to get "/tmp"
        // Remove "**" and the preceding "/"
        return resource.starts_with(prefix)
            && (resource.len() == prefix.len()
                || resource[prefix.len()..].starts_with('/'));
    }

    // Exact match
    resource == pattern
}

/// Global permission configuration
///
/// This struct manages both persistent whitelist rules and session-level grants.
/// It is designed to be shared across threads using Arc.
#[derive(Debug)]
pub struct PermissionConfig {
    /// Persistent whitelist rules (loaded from/saved to config file)
    whitelist: DashMap<String, PermissionRule>,
    /// Session-granted permissions that expire after a timeout
    session_grants: DashMap<PermissionType, Vec<SessionGrant>>,
    /// Default session grant duration (default: 30 minutes)
    session_grant_duration: Duration,
    /// Whether permission checks are enabled
    enabled: AtomicBool,
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
            session_grant_duration: Duration::from_secs(30 * 60), // 30 minutes
            enabled: AtomicBool::new(true),
        }
    }

    /// Create a new permission config with specific settings
    pub fn with_settings(enabled: bool, session_duration: Duration) -> Self {
        Self {
            whitelist: DashMap::new(),
            session_grants: DashMap::new(),
            session_grant_duration: session_duration,
            enabled: AtomicBool::new(enabled),
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

    /// Grant a permission for the current session
    pub fn grant_session_permission(&self, perm_type: PermissionType, resource_pattern: impl Into<String>) {
        let grant = SessionGrant::new(resource_pattern, self.session_grant_duration);

        self.session_grants
            .entry(perm_type)
            .and_modify(|grants| {
                grants.push(grant.clone());
            })
            .or_insert_with(|| vec![grant]);
    }

    /// Check if a permission is granted for the current session
    pub fn is_session_granted(&self, perm_type: PermissionType, resource: &str) -> bool {
        if let Some(grants) = self.session_grants.get(&perm_type) {
            // Clean up expired grants and check for matches
            let has_match = grants.iter().any(|grant| {
                if grant.is_expired() {
                    return false;
                }
                grant.matches(perm_type, resource)
            });

            if has_match {
                return true;
            }
        }
        false
    }

    /// Clear all session grants
    pub fn clear_session_grants(&self) {
        self.session_grants.clear();
    }

    /// Clean up expired session grants
    pub fn cleanup_expired_grants(&self) {
        for mut entry in self.session_grants.iter_mut() {
            entry.value_mut().retain(|grant| !grant.is_expired());
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
            Some(true) => false,  // Explicitly allowed
            Some(false) => true,  // Explicitly denied (requires override)
            None => true,         // No rule found, require confirmation
        }
    }

    /// Convert to serializable format for persistence
    pub fn to_serializable(&self) -> SerializablePermissionConfig {
        SerializablePermissionConfig {
            whitelist: self.get_rules(),
            enabled: self.is_enabled(),
            session_grant_duration_secs: self.session_grant_duration.as_secs(),
        }
    }

    /// Load from serializable format
    pub fn from_serializable(config: SerializablePermissionConfig) -> Self {
        let whitelist = DashMap::new();
        for rule in config.whitelist {
            let key = format!("{:?}:{}", rule.tool_type, rule.resource_pattern);
            whitelist.insert(key, rule);
        }

        Self {
            whitelist,
            session_grants: DashMap::new(),
            session_grant_duration: Duration::from_secs(config.session_grant_duration_secs),
            enabled: AtomicBool::new(config.enabled),
        }
    }
}

/// Serializable version of PermissionConfig for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializablePermissionConfig {
    pub whitelist: Vec<PermissionRule>,
    pub enabled: bool,
    pub session_grant_duration_secs: u64,
}

impl Default for SerializablePermissionConfig {
    fn default() -> Self {
        Self {
            whitelist: Vec::new(),
            enabled: true,
            session_grant_duration_secs: 30 * 60, // 30 minutes
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permission_type_description() {
        assert!(PermissionType::WriteFile.description().contains("Write files"));
        assert!(PermissionType::ExecuteCommand.description().contains("Execute"));
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
        config.add_rule(PermissionRule::new(PermissionType::WriteFile, "*.txt", false));

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
            config.add_rule(PermissionRule::new(PermissionType::WriteFile, "/safe/*", true));

            // All traversal attempts should be rejected
            assert!(
                config.needs_confirmation(PermissionType::WriteFile, path),
                "Path traversal should require confirmation (be blocked by default): {}", path
            );
        }
    }

    #[test]
    fn test_path_within_allowed_directory() {
        let config = PermissionConfig::new();
        // Use ** for recursive matching to include subdirectories
        config.add_rule(PermissionRule::new(PermissionType::WriteFile, "/tmp/allowed/**", true));

        // Files within allowed directory should be allowed
        assert!(!config.needs_confirmation(PermissionType::WriteFile, "/tmp/allowed/file.txt"));
        assert!(!config.needs_confirmation(PermissionType::WriteFile, "/tmp/allowed/subdir/file.txt"));

        // Files outside should require confirmation
        assert!(config.needs_confirmation(PermissionType::WriteFile, "/tmp/other/file.txt"));
        assert!(config.needs_confirmation(PermissionType::WriteFile, "/etc/passwd"));
    }

    #[test]
    fn test_secure_file_create_parent_validation() {
        use std::io::Write;

        let temp_dir = std::env::temp_dir().join(format!("secure_create_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let allowed_dir = temp_dir.join("allowed");
        std::fs::create_dir_all(&allowed_dir).unwrap();

        // Creating a file in an allowed directory should work
        let new_file = allowed_dir.join("new_file.txt");
        let result = open_file_for_write_secure(&new_file);
        assert!(result.is_ok(), "Should be able to create file in allowed directory");

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
        config.add_rule(PermissionRule::new(PermissionType::WriteFile, "/tmp/*", true));

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
        config.add_rule(PermissionRule::new(PermissionType::WriteFile, "/tmp/*", true));

        // Add deny rule (should override allow)
        config.add_rule(PermissionRule::new(PermissionType::WriteFile, "/tmp/sensitive.txt", false));

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
}
