/// Validates workflow names for security (prevents path traversal, etc.).
pub(crate) fn is_safe_workflow_name(name: &str) -> bool {
    // Check basic constraints.
    if name.is_empty() || name.len() > 255 {
        return false;
    }

    // Trim and check for whitespace issues.
    let trimmed = name.trim();
    if trimmed != name || trimmed.is_empty() {
        return false;
    }

    // Check for path separators and traversal patterns.
    if name.contains('/') || name.contains('\\') || name.contains("..") {
        return false;
    }

    // Check for null bytes and control characters.
    if name.chars().any(|ch| ch.is_control() || ch == '\0') {
        return false;
    }

    // Check for reserved Windows names.
    let upper = name.to_uppercase();
    let stem = upper.split('.').next().unwrap_or(&upper);
    let reserved = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    if reserved.contains(&stem) {
        return false;
    }

    // Only allow alphanumeric, dash, underscore, dot, and space.
    name.chars()
        .all(|ch| ch.is_alphanumeric() || ch == '-' || ch == '_' || ch == '.' || ch == ' ')
}
