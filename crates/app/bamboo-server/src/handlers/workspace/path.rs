use std::path::{Path, PathBuf};

use crate::error::AppError;

pub(super) const DEFAULT_MAX_DEPTH: usize = 6;
pub(super) const DEFAULT_MAX_ENTRIES: usize = 2000;
pub(super) const MAX_ALLOWED_ENTRIES: usize = 10000;

const IGNORED_DIRS: [&str; 10] = [
    ".git",
    "node_modules",
    "target",
    "dist",
    "build",
    ".next",
    ".turbo",
    ".cache",
    ".idea",
    ".vscode",
];

/// Validates and canonicalizes a workspace path to prevent directory traversal attacks.
pub(super) fn validate_workspace_path(input_path: &str) -> Result<PathBuf, AppError> {
    let trimmed = input_path.trim();

    if trimmed.is_empty() {
        return Err(AppError::BadRequest("Path cannot be empty".to_string()));
    }

    // Basic path traversal check before canonicalization.
    if trimmed.contains("..") {
        return Err(AppError::BadRequest(
            "Path cannot contain '..' sequences".to_string(),
        ));
    }

    let path = PathBuf::from(trimmed);
    let canonical = path.canonicalize().map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            AppError::NotFound(format!("Path does not exist: {trimmed}"))
        } else {
            AppError::BadRequest(format!("Invalid path: {error}"))
        }
    })?;

    Ok(canonical)
}

pub(super) fn home_dir() -> Result<PathBuf, AppError> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .ok_or_else(|| AppError::InternalError(anyhow::anyhow!("HOME not set")))?;
    Ok(PathBuf::from(home))
}

pub(super) fn should_skip_entry(name: &str, is_dir: bool, include_hidden: bool) -> bool {
    if !include_hidden && name.starts_with('.') {
        return true;
    }
    if is_dir && IGNORED_DIRS.iter().any(|ignored| ignored == &name) {
        return true;
    }
    false
}

pub(super) fn to_display_name(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
}
